// Package workflow runs durable workflows whose runners are ordinary Go
// functions.
//
// # Workflows and runners
//
// A workflow is a struct type whose Workflow(Out) method declares the
// workflow's output. Its package path and type name are the workflow name, and
// a value of it is the input of a run, so code can Start a workflow from the
// type alone.
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
// A run executes in one of two contexts, as in the Workflow SDK. In workflow
// context, the runner's own code executes as a flow, which replays the runner
// from the beginning, records the operations it starts (steps, timers, hooks,
// webhooks, and child runs), queues the steps it needs, and records the run's
// result once the runner returns. In step context, a step function executes as
// a step, which replays the runner up to that step and calls the step function.
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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"runtime"
	"strings"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
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
// worlds.HTTPHandler at endpoint, an absolute HTTP URL. It panics when
// endpoint is not one. Requests use the last of clients, or
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

// workflow is satisfied by a workflow, a struct type whose Workflow(Out)
// method declares the workflow's output type.
type workflow[Out any] interface{ Workflow(Out) }

// Run is a handle to a run of a workflow whose output type is Out. Start and
// Get return one. Run implements Promise.
//
// A Run started in workflow context is a child run. In workflow context, every
// Run method reads or changes the run once in a step and replay returns the
// recorded result, so a method does not wait for the run to finish. Await
// returns ErrRunNotFinished for a run that had not finished when it was called.
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
	if _, ok := workflowExecutionFromContext(ctx); ok {
		status, err = Step(ctx, runStatus, r.id)
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
	if _, ok := workflowExecutionFromContext(ctx); ok {
		return Step(ctx, runExists, r.id)
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
	if _, ok := workflowExecutionFromContext(ctx); ok {
		name, err = Step(ctx, runWorkflowName, r.id)
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

// WakeUp settles the run's pending timers early. It queues a delivery so the
// run resumes, and returns the number of timers it settled. A settled Timeout
// does not cancel the run.
func (r *Run[Out]) WakeUp(ctx context.Context, options ...WakeOptions) (int, error) {
	option := WakeOptions{}
	if len(options) != 0 {
		option = options[len(options)-1]
	}
	if _, ok := workflowExecutionFromContext(ctx); ok {
		return Step(ctx, runWakeUp, worlds.ClientRunsWakeInput{RunID: r.id, CorrelationIDs: option.CorrelationIDs})
	}
	return runWakeUp(NewContext(ctx, r.client), worlds.ClientRunsWakeInput{RunID: r.id, CorrelationIDs: option.CorrelationIDs})
}

func runWakeUp(ctx context.Context, input worlds.ClientRunsWakeInput) (int, error) {
	client, err := clientFromContext(ctx)
	if err != nil {
		return 0, err
	}
	return client.RunsWake(ctx, input)
}

// Done reports whether the run has finished, whether it completed, failed, or
// was cancelled. It does not block and does not return the run's error, which
// Await returns.
func (r *Run[Out]) Done(ctx context.Context) (bool, error) {
	var run worlds.Run
	var err error
	if _, ok := workflowExecutionFromContext(ctx); ok {
		run, err = Step(ctx, runDone, r.id)
	} else {
		run, err = runDone(NewContext(ctx, r.client), r.id)
	}
	switch {
	case err != nil:
		return false, err
	case run.RunID == "":
		return false, worlds.ErrRunNotFound
	default:
		return run.Status.Terminal(), nil
	}
}

func runDone(ctx context.Context, runID worlds.RunID) (worlds.Run, error) {
	client, err := clientFromContext(ctx)
	if err != nil {
		return worlds.Run{}, err
	}
	run, err := client.RunsGet(ctx, worlds.RunsGetInput{RunID: runID, ResolveData: worlds.ResolveDataNone})
	if errors.Is(err, worlds.ErrRunNotFound) {
		return worlds.Run{}, nil
	}
	return run, err
}

// Await returns the output of the run once it has finished. It returns the
// run's error as an *Error when the run failed and ErrRunCancelled when it was
// cancelled.
//
// In a run, in workflow or step context, Await reads the run once and returns
// ErrRunNotFinished when the run had not finished. Outside a run, it blocks
// until the run finishes or ctx is done.
func (r *Run[Out]) Await(ctx context.Context) (Out, error) {
	var zero Out
	var run worlds.Run
	var err error
	if _, ok := workflowExecutionFromContext(ctx); ok {
		run, err = Step(ctx, runAwait, r.id)
	} else {
		run, err = runAwait(NewContext(ctx, r.client), r.id)
	}
	switch {
	case err != nil:
		return zero, err
	case run.RunID == "":
		return zero, worlds.ErrRunNotFound
	case !run.Status.Terminal():
		return zero, ErrRunNotFinished
	case run.Status == worlds.StatusCompleted:
	case len(run.Error) != 0:
		return zero, worlds.DecodeError(run.Error)
	default:
		return zero, ErrRunCancelled
	}
	if err := json.Unmarshal(run.Output, &zero); err != nil {
		return zero, fmt.Errorf("workflow: decode run output: %w", err)
	}
	return zero, nil
}

func runAwait(ctx context.Context, runID worlds.RunID) (worlds.Run, error) {
	client, err := clientFromContext(ctx)
	if err != nil {
		return worlds.Run{}, err
	}
	if _, ok := worlds.ExecutionFromContext(ctx); ok {
		run, err := client.RunsGet(ctx, worlds.RunsGetInput{RunID: runID})
		if errors.Is(err, worlds.ErrRunNotFound) {
			return worlds.Run{}, nil
		}
		run.Input = nil
		return run, err
	}
	for {
		run, err := client.RunsWaitForTerminalStatus(ctx, worlds.RunsWaitForTerminalStatusInput{RunID: runID, Timeout: 100 * time.Millisecond})
		if err != nil || run.Status.Terminal() {
			return run, err
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return worlds.Run{}, ctx.Err()
		case <-timer.C:
		}
	}
}

// Unwrap is like Await but panics if Await returns an error.
func (r *Run[Out]) Unwrap(ctx context.Context) Out {
	value, err := r.Await(ctx)
	if err != nil {
		panic(err)
	}
	return value
}

// Cancel cancels the run. Cancelling a run that has already finished is a
// no-op.
func (r *Run[Out]) Cancel(ctx context.Context) error {
	if _, ok := workflowExecutionFromContext(ctx); ok {
		_, err := Step(ctx, runCancel, r.id)
		return err
	}
	_, err := runCancel(NewContext(ctx, r.client), r.id)
	return err
}

func runCancel(ctx context.Context, runID worlds.RunID) (struct{}, error) {
	client, err := clientFromContext(ctx)
	if err != nil {
		return struct{}{}, err
	}
	err = client.RunsCancel(ctx, worlds.ClientRunsCancelInput{RunID: runID})
	if errors.Is(err, worlds.ErrRunTerminal) {
		return struct{}{}, nil
	}
	return struct{}{}, err
}

// Reader returns a reader for the run's stream from its beginning. Reads block
// until data is written and return io.EOF once the stream is closed. Reader is
// for step context and for use outside a run. It returns ErrNotInStep in
// workflow context.
func (r *Run[Out]) Reader(ctx context.Context, options ...ReaderOption) (io.Reader, error) {
	return r.ReaderAt(ctx, 0, options...)
}

// ReaderAt is like Reader but starts at byte offset. A negative offset is
// relative to the current end of the stream.
func (r *Run[Out]) ReaderAt(ctx context.Context, offset int64, options ...ReaderOption) (io.Reader, error) {
	if _, ok := workflowExecutionFromContext(ctx); ok {
		return nil, ErrNotInStep
	}
	opts := readerOption(options)
	return newStreamReader(ctx, r.client, r.id, opts.Name, offset), nil
}

// Writer returns a writer for the run's stream, for step context and for use
// outside a run. It waits until no other writer holds the stream name. Close
// closes the stream for all writers, while the returned release function only
// gives up this writer's hold so another writer can continue the stream.
// Writer returns ErrNotInStep in workflow context.
func (r *Run[Out]) Writer(ctx context.Context, options ...WriterOption) (io.WriteCloser, func() error, error) {
	if _, ok := workflowExecutionFromContext(ctx); ok {
		return nil, nil, ErrNotInStep
	}
	opts := writerOption(options)
	return newStreamWriter(ctx, r.client, r.id, opts.Name, string(worlds.NewStreamID()))
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

// Start starts a run of workflow In with input and returns a handle to the run
// without waiting for it. It needs only the workflow type, not its runner.
//
// Called in workflow context, Start starts a child run in a step, as in the
// Workflow SDK, so replay returns the same run. The child run carries
// worlds.ParentRunIDAttribute and worlds.RootRunIDAttribute. Called elsewhere,
// it uses the Client in ctx and returns ErrNoClient when ctx carries none.
func Start[In workflow[Out], Out any](ctx context.Context, input In) (*Run[Out], error) {
	name := typeIdentity[In]()
	client, err := clientFromContext(ctx)
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(input)
	if err != nil {
		return nil, fmt.Errorf("workflow: encode %s input: %w", name, err)
	}
	if _, ok := workflowExecutionFromContext(ctx); ok {
		id, err := Step(ctx, startChild, childStartInput{WorkflowName: name, Input: raw})
		if err != nil {
			return nil, err
		}
		return &Run[Out]{id: id, client: client}, nil
	}
	id, err := client.RunsCreate(ctx, worlds.ClientRunsCreateInput{WorkflowName: name, Input: raw})
	if err != nil {
		return nil, err
	}
	return &Run[Out]{id: id, client: client}, nil
}

type childStartInput struct {
	WorkflowName string
	Input        json.RawMessage
}

// startChild is the step Start uses in workflow context. It creates the child
// run and queues its first delivery. The child run ID is derived from the
// step ID, so a retried step finds the run an earlier attempt created.
func startChild(ctx context.Context, input childStartInput) (worlds.RunID, error) {
	execution, ok := worlds.ExecutionFromContext(ctx)
	if !ok {
		return "", ErrNoWorkflow
	}
	parent := execution.Run()
	root := parent.Attributes[worlds.RootRunIDAttribute]
	if root == "" {
		root = string(parent.RunID)
	}
	id := worlds.RunID("wrun_" + strings.TrimPrefix(StepID(ctx), "step_"))
	_, err := execution.World().EventsCreateRun(ctx, worlds.EventsCreateRunInput{
		RunID:        id,
		WorkflowName: input.WorkflowName,
		Input:        input.Input,
		Attributes: map[string]string{
			worlds.ParentRunIDAttribute: string(parent.RunID),
			worlds.RootRunIDAttribute:   root,
		},
	})
	if errors.Is(err, worlds.ErrRunAlreadyExists) {
		run, err := execution.World().RunsGet(ctx, worlds.RunsGetInput{RunID: id})
		if err != nil || run.Status != worlds.StatusPending {
			return id, err
		}
	} else if err != nil {
		return "", err
	}
	if _, err := execution.World().Queue(ctx, worlds.QueueInput{RunID: id, IdempotencyKey: string(id)}); err != nil {
		return "", err
	}
	return id, nil
}

// Use registers fn as the runner of workflow In. Pass the returned Runner to
// worlds.Runners so the runtime can execute the workflow. The workflow name is
// the package path and type name of In. fn replays on every delivery, so it
// must be deterministic. It does side effects only through steps and reads
// time only through Now.
func Use[In workflow[Out], Out any](fn func(context.Context, In) (Out, error)) worlds.Runner {
	name := typeIdentity[In]()
	return worlds.NewRunner(name, func(ctx context.Context, raw json.RawMessage) (json.RawMessage, error) {
		var input In
		if err := json.Unmarshal(raw, &input); err != nil {
			return nil, fmt.Errorf("workflow: decode %s input: %w", name, err)
		}
		output, err := fn(ctx, input)
		if err != nil {
			return nil, err
		}
		encoded, err := json.Marshal(output)
		if err != nil {
			return nil, fmt.Errorf("workflow: encode %s output: %w", name, err)
		}
		return encoded, nil
	})
}

func stepRetryAfter(cause error, options StepOptions, attempt int) *time.Time {
	var fatal fatalError
	if errors.As(cause, &fatal) || attempt > options.MaxRetries {
		return nil
	}
	var retryable retryableError
	due := time.Now()
	if errors.As(cause, &retryable) && retryable.after > 0 {
		due = due.Add(retryable.after)
	}
	return &due
}

func typeIdentity[In any]() string {
	typeOf := reflect.TypeFor[In]()
	for typeOf.Kind() == reflect.Pointer {
		typeOf = typeOf.Elem()
	}
	return typeOf.PkgPath() + "." + typeOf.Name()
}

func functionIdentity[In, Out any](fn func(context.Context, In) (Out, error)) string {
	return runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
}

func rollbackFunctionIdentity[In any](fn func(context.Context, In) error) string {
	return runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
}

func inlineFunctionIdentity[Out any](fn func(context.Context) Out) string {
	return runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
}
