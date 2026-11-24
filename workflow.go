// Package workflow runs durable workflows whose runners are ordinary Go
// functions.
//
// # Workflows and runners
//
// A workflow is a struct type that embeds Struct[Out], or has its own
// Workflow(Out) method, to declare the workflow's output. Its package path and
// type name are the workflow name, and a value of it is the input of a run, so
// code can Start a workflow from the type alone.
//
// A runner executes the workflow. It is a function func(context.Context, In)
// (Out, error) registered with Use, and it calls steps through Step,
// Background, Map, and All. Steps do the real work and side effects.
//
// # Runs
//
// Start begins a run of a workflow and returns a Run handle. Every run is
// recorded in a World, the backend that stores its history, queues its
// deliveries, and holds its streams.
//
// A run executes in one of two contexts. In workflow context, the runner's own
// code executes as a flow, which replays the runner from the beginning, records
// the operations it starts (steps, timers, hooks, webhooks, and child runs),
// queues the steps it needs, and records the run's result once the runner
// returns. In step context, a step function executes as a step, which replays
// the runner up to that step and calls the step function.
//
// # Replay
//
// Operations return promises. Once an operation is recorded, replay returns its
// recorded result instead of running it again, and awaiting a promise that has
// not settled suspends the run until it does.
//
// A runner must therefore be deterministic. It does side effects only through
// steps and reads time only through Now. Steps may retry and are executed at
// least once, so step functions should be idempotent. Package check reports
// runners that break these rules.
//
// # Clients
//
// Code outside a run uses a Client, carried in a context by NewContext or
// Middleware, to start, read, cancel, and resume runs.
package workflow

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/eriicafes/go-workflow/internal/devalue"
	"github.com/eriicafes/go-workflow/internal/envelope"
	"github.com/oklog/ulid/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/core"
)

type (
	// World is the backend a run is recorded in. It provides storage, a
	// delivery queue, and streams.
	World = worlds.World
	// Client is the API for starting, reading, cancelling, and resuming runs,
	// and for reading and writing their streams, from outside a run.
	Client = worlds.Client
)

// NewClient returns a Client that calls world directly, in the same process.
func NewClient(world World) Client { return worlds.NewClient(world) }

// NewHTTPClient returns a Client that calls a World served by
// worlds.HTTPHandler. endpoint is the absolute URL of the server followed by
// the handler's prefix, such as http://worker:8081/v1/workflow. It panics when
// endpoint is not an absolute HTTP URL. Requests use the last of clients, or
// http.DefaultClient when none is given.
func NewHTTPClient(endpoint string, clients ...*http.Client) Client {
	return worlds.NewHTTPClient(endpoint, clients...)
}

// Status is the state of a run or step.
type Status = worlds.Status

const (
	// StatusPending means the run or step is recorded but has not started.
	StatusPending = worlds.StatusPending
	// StatusRunning means the run or step has started and has not finished.
	StatusRunning = worlds.StatusRunning
	// StatusCompleted means the run or step finished with an output.
	StatusCompleted = worlds.StatusCompleted
	// StatusFailed means the run or step finished with an error.
	StatusFailed = worlds.StatusFailed
	// StatusCancelled means the run or step was cancelled before it finished.
	StatusCancelled = worlds.StatusCancelled
)

// clientWait bounds each blocking client call that Run.Value and stream
// readers make. It is long so that waiting is one blocking call rather than a
// series of short polls, which also lets testing/synctest advance its fake
// clock to the next timer. The HTTP client caps it per request.
const clientWait = time.Hour

// Struct declares a workflow's output type when embedded in the workflow
// struct, in place of writing its Workflow(Out) method:
//
//	type GenerateReport struct {
//		workflow.Struct[GenerateReportResult]
//		Topic string
//	}
//
// It has no fields, so it adds nothing to the workflow's JSON input.
type Struct[Out any] struct{}

// Workflow declares Out as the output of the workflow that embeds Struct.
func (Struct[Out]) Workflow(Out) {}

// Run is a handle to a run of a workflow whose output type is Out. Start and
// Get return one. A Run has an independent lifetime; use Cancel to cancel it.
//
// A Run started in workflow context is a child run. In workflow context, every
// Run method reads or changes the run once in an inline step and replay returns the
// recorded result, so a method does not wait for the run to finish. Value
// returns ErrRunNotFinished for a run that had not finished when it was called.
// These inline steps do not inherit managed context cancellation.
// To wait for a child run, create a hook and have the child resume it.
type Run[Out any] struct {
	id     worlds.RunID
	client Client
}

// ID returns the run ID.
func (r *Run[Out]) ID() string { return string(r.id) }

// Status returns the current status of the run. It returns
// worlds.ErrRunNotFound when the run does not exist.
func (r *Run[Out]) Status(ctx context.Context) (Status, error) {
	var status Status
	var err error
	if execution, ok := workflowExecutionFromContext(ctx); ok {
		status, err = InlineStep(execution.Context(), runStatus, r.id)
	} else {
		status, err = runStatus(NewContext(ctx, r.client), r.id)
	}
	if err == nil && status == "" {
		return "", worlds.ErrRunNotFound
	}
	return status, err
}

func runStatus(ctx context.Context, runID worlds.RunID) (Status, error) {
	client, err := clientFromContext(ctx)
	if err != nil {
		return "", err
	}
	run, err := client.RunsGet(ctx, worlds.RunsGetInput{RunID: runID})
	if errors.Is(err, worlds.ErrRunNotFound) {
		return "", nil
	}
	return run.Status, err
}

// Exists reports whether the run is recorded in its World.
func (r *Run[Out]) Exists(ctx context.Context) (bool, error) {
	if execution, ok := workflowExecutionFromContext(ctx); ok {
		return InlineStep(execution.Context(), runExists, r.id)
	}
	return runExists(NewContext(ctx, r.client), r.id)
}

func runExists(ctx context.Context, runID worlds.RunID) (bool, error) {
	client, err := clientFromContext(ctx)
	if err != nil {
		return false, err
	}
	_, err = client.RunsGet(ctx, worlds.RunsGetInput{RunID: runID})
	if errors.Is(err, worlds.ErrRunNotFound) {
		return false, nil
	}
	return err == nil, err
}

// WorkflowName returns the name of the workflow the run executes. It returns
// worlds.ErrRunNotFound when the run does not exist.
func (r *Run[Out]) WorkflowName(ctx context.Context) (string, error) {
	var name string
	var err error
	if execution, ok := workflowExecutionFromContext(ctx); ok {
		name, err = InlineStep(execution.Context(), runWorkflowName, r.id)
	} else {
		name, err = runWorkflowName(NewContext(ctx, r.client), r.id)
	}
	if err == nil && name == "" {
		return "", worlds.ErrRunNotFound
	}
	return name, err
}

func runWorkflowName(ctx context.Context, runID worlds.RunID) (string, error) {
	client, err := clientFromContext(ctx)
	if err != nil {
		return "", err
	}
	run, err := client.RunsGet(ctx, worlds.RunsGetInput{RunID: runID})
	if errors.Is(err, worlds.ErrRunNotFound) {
		return "", nil
	}
	return run.WorkflowName, err
}

// WakeOptions holds the options to WakeUp.
type WakeOptions struct {
	// CorrelationIDs limits WakeUp to the waits with these IDs. When empty,
	// WakeUp settles every pending wait in the run.
	CorrelationIDs []string
}

// WakeUp settles the run's pending timers early. It queues a delivery of the
// run that completes them before it resumes the run, and returns the number
// of timers that delivery completes. Waking a deadline timer triggers its
// cancellation continuation.
func (r *Run[Out]) WakeUp(ctx context.Context, options ...WakeOptions) (int, error) {
	option := WakeOptions{}
	if len(options) != 0 {
		option = options[len(options)-1]
	}
	if execution, ok := workflowExecutionFromContext(ctx); ok {
		// Record which waits to wake before waking them, so a retried wake
		// does not reach waits the run created since.
		waitIDs, err := InlineStep(execution.Context(), runPendingWaits, worlds.ClientRunsWakeInput{RunID: r.id, CorrelationIDs: option.CorrelationIDs})
		if err != nil || len(waitIDs) == 0 {
			return 0, err
		}
		return InlineStep(execution.Context(), runWakeUp, worlds.ClientRunsWakeInput{RunID: r.id, CorrelationIDs: waitIDs})
	}
	return runWakeUp(NewContext(ctx, r.client), worlds.ClientRunsWakeInput{RunID: r.id, CorrelationIDs: option.CorrelationIDs})
}

// runPendingWaits returns the pending waits of input.RunID that WakeUp would
// wake, in workflow context, where the run is in the execution's World.
func runPendingWaits(ctx context.Context, input worlds.ClientRunsWakeInput) ([]string, error) {
	execution, ok := core.ExecutionFromContext(ctx)
	if !ok {
		return nil, ErrNoWorkflow
	}
	pending, err := worlds.PendingWaits(ctx, execution.World(), input.RunID)
	if err != nil || len(input.CorrelationIDs) == 0 {
		return pending, err
	}
	selected := []string{}
	for _, waitID := range pending {
		if slices.Contains(input.CorrelationIDs, waitID) {
			selected = append(selected, waitID)
		}
	}
	return selected, nil
}

func runWakeUp(ctx context.Context, input worlds.ClientRunsWakeInput) (int, error) {
	client, err := clientFromContext(ctx)
	if err != nil {
		return 0, err
	}
	return client.RunsWake(ctx, input)
}

// Value returns the output of the run once it has finished. It returns a
// *RunFailedError, wrapping the run's error as an *Error, when the run
// failed, a *RunCancelledError, which matches ErrRunCancelled, when it was
// cancelled, and a *RunExpiredError when its data was deleted. A failed or
// cancelled run's error is fatal, so a step that awaits the run fails
// without retrying.
//
// In workflow context, Value reads the run once and returns ErrRunNotFinished
// when the run had not finished, so replay stays deterministic. In step context
// and outside a run, it blocks until the run finishes or ctx is done.
func (r *Run[Out]) Value(ctx context.Context) (Out, error) {
	var zero Out
	var run worlds.Run
	var err error
	if execution, ok := workflowExecutionFromContext(ctx); ok {
		run, err = InlineStep(execution.Context(), runValue, r.id)
	} else {
		for {
			run, err = r.client.RunsWaitForTerminalStatus(ctx, worlds.RunsWaitForTerminalStatusInput{RunID: r.id, Timeout: clientWait})
			if err != nil || run.Status.Terminal() {
				break
			}
			timer := time.NewTimer(10 * time.Millisecond)
			select {
			case <-ctx.Done():
				return zero, ctx.Err()
			case <-timer.C:
			}
		}
	}
	switch {
	case err != nil:
		return zero, err
	case run.RunID == "":
		return zero, worlds.ErrRunNotFound
	case !run.Status.Terminal():
		return zero, ErrRunNotFinished
	case run.Status != worlds.StatusCancelled && worlds.RunDataExpired(run, time.Now()):
		return zero, &RunExpiredError{RunID: run.RunID, RunStatus: run.Status, ExpiredAt: *run.ExpiredAt}
	}
	keys, err := r.client.RunKeys(ctx, worlds.RunKeysInput{RunID: run.RunID, DeploymentID: run.DeploymentID})
	if err != nil {
		return zero, err
	}
	switch {
	case run.Status == worlds.StatusCompleted:
	case len(run.Error) != 0:
		return zero, &RunFailedError{RunID: run.RunID, ErrorCode: run.ErrorCode, Cause: worlds.DecodeErrorPayload(run.Error, keys)}
	default:
		return zero, &RunCancelledError{RunID: run.RunID}
	}
	if err := worlds.DecodePayload(run.Output, keys, &zero); err != nil {
		return zero, fmt.Errorf("workflow: decode run output: %w", err)
	}
	return zero, nil
}

func runValue(ctx context.Context, runID worlds.RunID) (worlds.Run, error) {
	client, err := clientFromContext(ctx)
	if err != nil {
		return worlds.Run{}, err
	}
	run, err := client.RunsGet(ctx, worlds.RunsGetInput{RunID: runID})
	if errors.Is(err, worlds.ErrRunNotFound) {
		return worlds.Run{}, nil
	}
	run.Input = nil
	// The recorded run keeps ExpiredAt only once it has passed, so a replay
	// reaches the same answer later.
	if !worlds.RunDataExpired(run, time.Now()) {
		run.ExpiredAt = nil
	}
	return run, err
}

// CancelOptions configures Run.Cancel.
type CancelOptions struct {
	// Reason is recorded on the run's run_cancelled event as its
	// cancelReason, in plain text, so tools can show why the run was
	// cancelled. It holds at most worlds.MaxCancelReasonLength characters.
	Reason string
}

// Cancel cancels the run. Cancelling a run that has already finished is a
// no-op. If multiple options are passed, the last one applies.
func (r *Run[Out]) Cancel(ctx context.Context, options ...CancelOptions) error {
	var reason string
	if len(options) != 0 {
		reason = options[len(options)-1].Reason
	}
	input := runCancelInput{RunID: r.id, Reason: reason}
	if execution, ok := workflowExecutionFromContext(ctx); ok {
		_, err := InlineStep(execution.Context(), runCancel, input)
		return err
	}
	_, err := runCancel(NewContext(ctx, r.client), input)
	return err
}

// runCancelInput is the input of the step that cancels a run: the run and
// the reason to record, if any.
type runCancelInput struct {
	RunID  worlds.RunID
	Reason string
}

// runCancel cancels the run input names. A run that has already finished is
// left as it is.
func runCancel(ctx context.Context, input runCancelInput) (struct{}, error) {
	client, err := clientFromContext(ctx)
	if err != nil {
		return struct{}{}, err
	}
	err = client.RunsCancel(ctx, worlds.ClientRunsCancelInput{RunID: input.RunID, Reason: input.Reason})
	if _, ok := errors.AsType[*worlds.RunExpiredError](err); ok {
		return struct{}{}, nil
	}
	return struct{}{}, err
}

// Reader returns a reader for the run's stream from ReaderOption.StartIndex,
// for step context and for use outside a run, such as a route handler. Reads
// block until data is written and return io.EOF once the stream is closed. It
// returns ErrNotInStep in workflow context.
func (r *Run[Out]) Reader(ctx context.Context, options ...ReaderOption) (StreamReader, error) {
	if _, ok := workflowExecutionFromContext(ctx); ok {
		return nil, ErrNotInStep
	}
	return newStreamReader(ctx, r.client, r.id, readerOption(options)), nil
}

// Writer returns a writer for the run's stream, for step context and for use
// outside a run. Writers of a stream do not wait for each other. Close
// stores what is buffered and gives up the writer, leaving the stream open,
// while CloseStream closes the stream for every writer. Writer returns
// ErrNotInStep in workflow context, and an *EncryptionKeyLookupError, which
// is fatal, when the run's encryption key cannot be resolved for good, such
// as for a run that does not exist.
func (r *Run[Out]) Writer(ctx context.Context, options ...WriterOption) (StreamWriter, error) {
	if _, ok := workflowExecutionFromContext(ctx); ok {
		return nil, ErrNotInStep
	}
	frame, err := outsideStreamFrame(ctx, r.client, r.id)
	if err != nil {
		return nil, err
	}
	return newStreamWriter(ctx, r.client, r.id, writerOption(options).Name, frame)
}

// Use registers fn as the runner of workflow In. Pass the returned Runner to
// worlds.Runners so the runtime can execute the workflow. The workflow name is
// formed from the package path and type name of In (see worlds.WorkflowName). fn replays on every delivery, so it
// must be deterministic. It does side effects only through steps and reads
// time only through Now.
func Use[In worlds.Workflow[Out], Out any](fn func(context.Context, In) (Out, error)) worlds.Runner {
	name := worlds.WorkflowName[In]()
	return worlds.NewRunner(name, func(ctx context.Context, raw []byte) ([]byte, error) {
		execution, ok := core.ExecutionFromContext(ctx)
		if !ok {
			return nil, ErrNoWorkflow
		}
		// The input is the workflow's argument list.
		input, err := decodeArguments[In](raw, execution.DecodePayload)
		if err != nil {
			return nil, fmt.Errorf("workflow: decode %s input: %w", name, err)
		}
		output, err := fn(ctx, input)
		if err != nil {
			return nil, err
		}
		encoded, err := execution.EncodePayload(output)
		if err != nil {
			return nil, fmt.Errorf("workflow: encode %s output: %w", name, err)
		}
		return encoded, nil
	})
}

// Get returns a handle to the run with runID, using the Client in ctx. It does
// not check that the run exists (see Run.Exists). It returns ErrNoClient when
// ctx carries no Client and does not belong to a run.
func Get[Out any](ctx context.Context, runID string) (*Run[Out], error) {
	client, err := clientFromContext(ctx)
	if err != nil {
		return nil, err
	}
	return &Run[Out]{id: worlds.RunID(runID), client: client}, nil
}

// Retention is how long a finished run's data is kept, as the run's
// $retention attribute records it. Its zero value is RetainDefault.
type Retention = worlds.Retention

var (
	// RetainDefault leaves a run's data to the World's retention.
	RetainDefault = worlds.RetainDefault
	// RetainNone deletes a run's data as soon as the run finishes. Its
	// metadata, such as its ID, status, and attributes, is kept, and
	// Run.Value returns a *RunExpiredError.
	RetainNone = worlds.RetainNone
)

// Deployment is the deployment a new run starts on. Its zero value is the
// deployment that starts the run.
type Deployment = worlds.Deployment

// LatestDeployment starts a run on the World's latest deployment.
var LatestDeployment = worlds.LatestDeployment

// DeploymentWithID starts a run on the deployment id.
func DeploymentWithID(id string) Deployment { return worlds.DeploymentWithID(id) }

// Region is the region a run's data lives in. Its values belong to the World,
// and the empty Region leaves it to the World.
type Region = worlds.Region

// StartOptions holds the options to Start. The zero value starts a run as
// Start without options does.
type StartOptions struct {
	// Deployment is the deployment the run starts on and stays pinned to.
	// LatestDeployment starts it on the World's latest deployment, so a
	// workflow that starts its next run before it finishes, such as a daily
	// chain, moves to new code at that boundary instead of staying on the
	// deployment its first run started on. Use it only for runs nothing waits
	// on the output of, since a newer deployment may return a different
	// output. DeploymentWithID names one. On Worlds without deployment
	// affinity it changes nothing. The zero value is the deployment that
	// starts the run.
	Deployment Deployment
	// Retention is how long the run's data is kept after it finishes.
	// RetainNone deletes it as soon as the run finishes, recorded as the
	// $retention attribute, and wins over one in Attributes. The zero value,
	// RetainDefault, leaves it to the World.
	Retention Retention
	// Attributes are plain-text attributes set on the run as it is created,
	// within the limits worlds.CheckAttributes enforces.
	Attributes map[string]string
	// AllowReservedAttributes permits keys in Attributes that start with "$",
	// which are reserved for framework and library code.
	AllowReservedAttributes bool
	// NoArgumentCompression stores the run's input uncompressed, for input
	// that does not compress, such as media or encrypted bytes.
	NoArgumentCompression bool
	// Region is the region the run's data lives in and its deliveries are
	// routed to, one of the World's regions. Only a World that implements
	// worlds.RegionalRunIDs uses it, by tagging the run's ID; others ignore
	// it. The zero value leaves it to the World.
	Region Region
	// Hook creates a hook with the run, claiming its token. When another run
	// holds the token, Start creates no run and returns a
	// *worlds.HookConflictError naming it. The workflow opens the hook with
	// CreateHook and the same token. It needs a World with
	// Capabilities().StartHook.
	Hook *StartHook
	// ReplayedFromRunID is the ID of the run this run replays, wrun_ and a
	// ULID. It is recorded in the run's ExecutionContext, so tools such as
	// the Workflow web UI link the replay to its source.
	ReplayedFromRunID string
}

var errNoStartHooks = fmt.Errorf("workflow: StartOptions.Hook: the World does not create start hooks: %w", errors.ErrUnsupported)

// StartHook is a hook Start creates with its run. See StartOptions.Hook.
type StartHook struct {
	// Token is the hook's token. It must not be empty.
	Token string
	// MinRetention keeps the token unavailable for at least this long from
	// Start, even after the run ends. Disposing the hook releases it at
	// once.
	MinRetention time.Duration
	// Metadata is stored with the hook, as a payload of the run.
	Metadata any
}

func startOptions(values []StartOptions) StartOptions {
	if len(values) == 0 {
		return StartOptions{}
	}
	return values[len(values)-1]
}

// startSettings are StartOptions as a child run's start step records them.
type startSettings struct {
	DeploymentID  string        `json:"deploymentId,omitempty"`
	Latest        bool          `json:"latest,omitempty"`
	Retention     string        `json:"retention,omitempty"`
	ReplayedFrom  string        `json:"replayedFrom,omitempty"`
	Region        Region        `json:"region,omitempty"`
	HookToken     string        `json:"hookToken,omitempty"`
	HookRetention time.Duration `json:"hookRetention,omitempty"`
	// HookMetadata is the hook's metadata as devalue text, encrypted with
	// the run's keys once they are known.
	HookMetadata  []byte            `json:"hookMetadata,omitempty"`
	Attributes    map[string]string `json:"attributes,omitempty"`
	NoCompression bool              `json:"noCompression,omitempty"`
	// AllowReservedAttributes is StartOptions.AllowReservedAttributes.
	AllowReservedAttributes bool `json:"allowReservedAttributes,omitempty"`
}

// settings checks options and returns them as a start step records them.
func (options StartOptions) settings() (startSettings, error) {
	if err := worlds.CheckAttributes(options.Attributes, options.AllowReservedAttributes); err != nil {
		return startSettings{}, err
	}
	if id := options.ReplayedFromRunID; id != "" {
		// It names a run, so it must be shaped as a run ID.
		rest, ok := strings.CutPrefix(id, "wrun_")
		if _, err := ulid.ParseStrict(rest); !ok || err != nil {
			return startSettings{}, fmt.Errorf("%w: ReplayedFromRunID %.64q is not wrun_ and a ULID", ErrInvalidStartOptions, id)
		}
	}
	var hookToken string
	var hookRetention time.Duration
	var hookMetadata []byte
	if hook := options.Hook; hook != nil {
		if hook.Token == "" {
			return startSettings{}, fmt.Errorf("%w: Hook needs a token", ErrInvalidStartOptions)
		}
		hookToken, hookRetention = hook.Token, hook.MinRetention
		if hook.Metadata != nil {
			var err error
			if hookMetadata, err = devalue.Marshal(hook.Metadata); err != nil {
				return startSettings{}, fmt.Errorf("workflow: encode hook metadata: %w", err)
			}
		}
	}
	return startSettings{
		DeploymentID:            options.Deployment.ID(),
		Latest:                  options.Deployment.Latest(),
		Retention:               options.Retention.String(),
		ReplayedFrom:            options.ReplayedFromRunID,
		Region:                  options.Region,
		HookToken:               hookToken,
		HookRetention:           hookRetention,
		HookMetadata:            hookMetadata,
		Attributes:              options.Attributes,
		NoCompression:           options.NoArgumentCompression,
		AllowReservedAttributes: options.AllowReservedAttributes,
	}, nil
}

// startHook returns the run's start hook, its metadata encrypted with keys,
// or nil when it has none.
func (settings startSettings) startHook(keys *worlds.RunKeys) (*worlds.StartHook, error) {
	if settings.HookToken == "" {
		return nil, nil
	}
	hook := &worlds.StartHook{Token: settings.HookToken}
	if settings.HookRetention > 0 {
		until := time.Now().UTC().Add(settings.HookRetention)
		hook.TokenRetentionUntil = &until
	}
	if settings.HookMetadata != nil {
		metadata, err := worlds.EncodeDevaluePayload(settings.HookMetadata, worlds.PayloadOptions{Keys: keys})
		if err != nil {
			return nil, err
		}
		hook.Metadata = metadata
	}
	return hook, nil
}

// runExecutionContext returns the ExecutionContext of a run started from ctx
// whose payloads are encrypted or not: the release that started it, whether
// it is encrypted, the trace it was started in, and the run it replays.
func (settings startSettings) runExecutionContext(ctx context.Context, encrypted bool) map[string]any {
	// The carrier is the trace context of ctx as the configured propagator
	// writes it, and empty outside a trace.
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	trace := make(map[string]any, len(carrier))
	for key, value := range carrier {
		trace[key] = value
	}
	executionContext := map[string]any{
		worlds.ExecutionContextTraceCarrier:        trace,
		worlds.ExecutionContextWorkflowCoreVersion: Version,
		worlds.ExecutionContextFeatures:            map[string]any{"encryption": encrypted},
	}
	if settings.ReplayedFrom != "" {
		executionContext[worlds.ExecutionContextReplayedFromRunID] = settings.ReplayedFrom
	}
	return executionContext
}

// deployment returns the Deployment settings record.
func (settings startSettings) deployment() Deployment {
	if settings.DeploymentID != "" {
		return DeploymentWithID(settings.DeploymentID)
	}
	if settings.Latest {
		return LatestDeployment
	}
	return Deployment{}
}

// attributes returns the run's attributes: lineage, then the caller's, which
// may replace it with AllowReservedAttributes, then the retention.
func (settings startSettings) attributes(lineage map[string]string) map[string]string {
	if len(lineage) == 0 && len(settings.Attributes) == 0 && settings.Retention == "" {
		return nil
	}
	attributes := make(map[string]string, len(lineage)+len(settings.Attributes)+1)
	maps.Copy(attributes, lineage)
	maps.Copy(attributes, settings.Attributes)
	if settings.Retention != "" {
		attributes[worlds.RetentionAttribute] = settings.Retention
	}
	return attributes
}

// Start starts a run of workflow In with input and returns a handle to the run
// without waiting for it. It needs only the workflow type, not its runner.
// options set its retention, attributes, and how its input is stored.
// Attributes outside the limits of worlds.CheckAttributes return
// worlds.ErrAttributeInvalid.
//
// Called in workflow context, Start starts a child run in a step, so replay
// returns the same run. The child run carries
// worlds.ParentRunIDAttribute and worlds.RootRunIDAttribute. Cancellation of ctx
// does not cancel the child; use Run.Cancel explicitly. Called elsewhere,
// it uses the Client in ctx and returns ErrNoClient when ctx carries none.
func Start[In worlds.Workflow[Out], Out any](ctx context.Context, input In, options ...StartOptions) (*Run[Out], error) {
	name := worlds.WorkflowName[In]()
	client, err := clientFromContext(ctx)
	if err != nil {
		return nil, err
	}
	settings, err := startOptions(options).settings()
	if err != nil {
		return nil, err
	}
	if execution, ok := workflowExecutionFromContext(ctx); ok {
		if settings.HookToken != "" && !execution.World().Capabilities().StartHook {
			return nil, errNoStartHooks
		}
		id, err := InlineStep(execution.Context(), startChild[In], childStartInput[In]{WorkflowName: name, Input: input, Settings: settings})
		if err != nil {
			return nil, err
		}
		return &Run[Out]{id: id, client: client}, nil
	}
	// The client caches the World's capabilities, so this costs a request
	// once per client.
	spec, err := client.Spec(ctx)
	if err != nil {
		return nil, err
	}
	if settings.HookToken != "" && !spec.Capabilities.StartHook {
		return nil, errNoStartHooks
	}
	// The input is encrypted with the run's key, so the run's ID and
	// deployment are settled first. A World with regional run IDs mints the
	// ID, tagged with the run's region.
	runID := worlds.NewRunID()
	if spec.Capabilities.RegionalRunIDs {
		if runID, err = client.RunsNewID(ctx, worlds.ClientRunsNewIDInput{Region: settings.Region}); err != nil {
			return nil, err
		}
	}
	deploymentID, err := client.RunsStartDeployment(ctx, worlds.NewClientRunsStartDeploymentInput(settings.deployment()))
	if err != nil {
		return nil, err
	}
	keys, err := client.RunKeys(ctx, worlds.RunKeysInput{RunID: runID, DeploymentID: deploymentID})
	if err != nil {
		return nil, err
	}
	create, err := runCreateInput(runID, deploymentID, name, input, keys, settings)
	if err != nil {
		return nil, err
	}
	create.Attributes = settings.attributes(nil)
	// The retention is a reserved attribute the framework sets.
	create.AllowReservedAttributes = settings.AllowReservedAttributes || settings.Retention != ""
	create.ExecutionContext = settings.runExecutionContext(ctx, keys != nil)
	if create.StartHook, err = settings.startHook(keys); err != nil {
		return nil, err
	}
	id, err := client.RunsCreate(ctx, worlds.ClientRunsCreateInput{EventsCreateRunInput: create})
	if err != nil {
		return nil, err
	}
	return &Run[Out]{id: id, client: client}, nil
}

// runCreateInput returns the run_created input of a run of input, its
// argument list encrypted with keys when they are set.
func runCreateInput[In any](runID worlds.RunID, deploymentID, name string, input In, keys *worlds.RunKeys, settings startSettings) (worlds.EventsCreateRunInput, error) {
	encoded, err := worlds.EncodePayload(arguments(input), worlds.PayloadOptions{Keys: keys, NoCompression: settings.NoCompression})
	if err != nil {
		return worlds.EventsCreateRunInput{}, fmt.Errorf("workflow: encode %s input: %w", name, err)
	}
	create := worlds.EventsCreateRunInput{RunID: runID, WorkflowName: name, Input: encoded, DeploymentID: deploymentID}
	if keys != nil {
		create.EncryptionPublicKey = envelope.EncodePublicKey(keys.PublicKey)
	}
	return create, nil
}

type childStartInput[In any] struct {
	WorkflowName string `json:"workflowName"`
	Input        In     `json:"input"`
	// Settings are the child's StartOptions.
	Settings startSettings `json:"settings,omitzero"`
}

// startChild is the inline step Start uses in workflow context. It creates the
// child run, pinned to the deployment executing the parent, or to the one its
// StartOptions.Deployment names, and queues its first
// delivery. The child run ID comes from worlds.NewChildRunID, so a retried
// attempt finds the run an earlier attempt created and queues its first
// delivery again, to the deployment it was pinned to, while it has not
// started.
func startChild[In any](ctx context.Context, input childStartInput[In]) (worlds.RunID, error) {
	execution, ok := core.ExecutionFromContext(ctx)
	if !ok {
		return "", ErrNoWorkflow
	}
	parent := execution.Run()
	root := parent.Attributes[worlds.RootRunIDAttribute]
	if root == "" {
		root = string(parent.RunID)
	}
	// Tagging is deterministic, so a retried attempt keeps the same ID.
	id := worlds.RegionRunID(execution.World(), worlds.NewChildRunID(worlds.StepID(StepID(ctx))), input.Settings.Region)
	deploymentID, err := worlds.StartDeploymentID(ctx, execution.World(), input.Settings.deployment())
	if err != nil {
		return "", err
	}
	keys, err := worlds.RunKeysFor(ctx, execution.World(), id, deploymentID)
	if err != nil {
		return "", err
	}
	create, err := runCreateInput(id, deploymentID, input.WorkflowName, input.Input, keys, input.Settings)
	if err != nil {
		return "", err
	}
	create.Attributes = input.Settings.attributes(map[string]string{
		worlds.ParentRunIDAttribute: string(parent.RunID),
		worlds.RootRunIDAttribute:   root,
	})
	// The lineage is reserved attributes the framework sets.
	create.AllowReservedAttributes = true
	create.ExecutionContext = input.Settings.runExecutionContext(ctx, keys != nil)
	if create.StartHook, err = input.Settings.startHook(keys); err != nil {
		return "", err
	}
	if err := worlds.StartRun(ctx, execution.World(), create); err != nil {
		// A start hook's refusal is decided and a retry cannot change it, so
		// it fails the step at once, and the workflow reads the refusal.
		if _, conflict := errors.AsType[*worlds.HookConflictError](err); create.StartHook != nil && (conflict || errors.Is(err, worlds.ErrStartHookRejected)) {
			return "", FatalError(err)
		}
		return "", err
	}
	return id, nil
}

// DeleteRunsResult is the result of DeleteRuns: one result per run, in the
// order of the run IDs, and a count of each outcome.
type DeleteRunsResult = worlds.RunsDeleteDataManyResult

// Delays DeleteRuns waits in a step before sending retryable failures again,
// and before the step retries them.
const (
	deleteRunsResendDelay = time.Second
	deleteRunsRetryDelay  = 10 * time.Second
)

// DeleteRuns deletes the data of one to 100 finished runs, using the Client
// in ctx: their inputs, outputs, and errors, the payloads of their steps,
// events, and hooks, and their streams' chunks. It cannot be undone. The
// runs, steps, events, and hooks stay, with their IDs, names, statuses,
// timestamps, and attributes, and a run's Run.Value then returns
// a *RunExpiredError.
//
// A run that has not finished is skipped and left running. Each run's
// outcome is in the result. Called in a step, DeleteRuns sends retryable
// failures again once, and returns a RetryableError for those that still
// fail, so the step retries them; elsewhere they are returned for the caller
// to retry. It returns an error wrapping errors.ErrUnsupported when the
// World does not delete run data, and ErrNotInStep in workflow code.
func DeleteRuns(ctx context.Context, runIDs ...string) (DeleteRunsResult, error) {
	if _, ok := workflowExecutionFromContext(ctx); ok {
		return DeleteRunsResult{}, ErrNotInStep
	}
	_, inStep := stepExecutionFromContext(ctx)
	// Inside a step, a request that cannot succeed fails the step rather
	// than being retried.
	fatal := func(err error) error {
		if inStep {
			return FatalError(err)
		}
		return err
	}
	client, err := clientFromContext(ctx)
	if err != nil {
		return DeleteRunsResult{}, err
	}
	ids := make([]worlds.RunID, len(runIDs))
	for index, id := range runIDs {
		ids[index] = worlds.RunID(id)
	}
	if err := worlds.CheckDeleteDataInput(worlds.RunsDeleteDataManyInput{RunIDs: ids}); err != nil {
		return DeleteRunsResult{}, fatal(err)
	}
	spec, err := client.Spec(ctx)
	if err != nil {
		return DeleteRunsResult{}, err
	}
	if !spec.Capabilities.DeleteData {
		return DeleteRunsResult{}, fatal(fmt.Errorf("workflow: the World does not delete run data: %w", errors.ErrUnsupported))
	}
	results, err := deleteRunData(ctx, client, ids)
	failed := 0
	if err == nil && inStep {
		retryable := func(result worlds.DeleteDataResult) bool {
			return result.Outcome == worlds.DeleteDataFailed && result.Retryable
		}
		var retry []int
		for index, result := range results {
			if retryable(result) {
				retry = append(retry, index)
			}
		}
		if len(retry) > 0 {
			// Send only the failed runs again, once, since a step retry
			// sends the whole batch.
			timer := time.NewTimer(deleteRunsResendDelay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return DeleteRunsResult{}, ctx.Err()
			case <-timer.C:
			}
			again := make([]worlds.RunID, len(retry))
			for position, index := range retry {
				again[position] = results[index].RunID
			}
			var resent []worlds.DeleteDataResult
			if resent, err = deleteRunData(ctx, client, again); err == nil {
				for position, index := range retry {
					// deleteRunData answers in the order it was asked.
					if results[index] = resent[position]; retryable(results[index]) {
						failed++
					}
				}
			}
		}
	}
	if err != nil {
		// In a step, a World's rate limit retries the step after the wait
		// the World asked for, or deleteRunsRetryDelay when it did not say.
		var throttle *worlds.ThrottleError
		if inStep && errors.As(err, &throttle) {
			err = RetryableError(err, cmp.Or(throttle.After, deleteRunsRetryDelay))
		}
		return DeleteRunsResult{}, err
	}
	if failed > 0 {
		return worlds.NewDeleteDataResult(results), RetryableError(fmt.Errorf("workflow: deleting the data of %d runs failed and will be retried", failed), deleteRunsRetryDelay)
	}
	return worlds.NewDeleteDataResult(results), nil
}

// deleteRunData deletes the data of ids and checks the World answered each
// one, in order.
func deleteRunData(ctx context.Context, client Client, ids []worlds.RunID) ([]worlds.DeleteDataResult, error) {
	result, err := client.RunsDeleteDataMany(ctx, worlds.RunsDeleteDataManyInput{RunIDs: ids})
	if err != nil {
		return nil, err
	}
	if len(result.Results) != len(ids) {
		return nil, fmt.Errorf("workflow: delete run data returned %d results for %d runs", len(result.Results), len(ids))
	}
	for index, id := range ids {
		if result.Results[index].RunID != id {
			return nil, fmt.Errorf("workflow: delete run data returned run %s where %s was expected", result.Results[index].RunID, id)
		}
	}
	return result.Results, nil
}
