# Package worlds

```go
import "github.com/eriicafes/go-workflow/worlds"
```

Package `worlds` defines the World contract that runs are recorded in, the
runtime that executes deliveries against it, the Client, and the HTTP server
that exposes a World to clients. Full godoc is on
[pkg.go.dev](https://pkg.go.dev/github.com/eriicafes/go-workflow/worlds).

Most applications only touch `worlds.Runners`. This page is for operators,
tool builders, and people [implementing a World](/worlds/custom).

## Runners and runtime

```go
func Runners(values ...Runner) RunnerSet
func NewRunner(name string, run func(context.Context, json.RawMessage) (json.RawMessage, error)) Runner

func NewRuntime(world World, runners RunnerSet) *Runtime
func (*Runtime) Execute(ctx context.Context, input QueueInput) error
```

`workflow.Use` builds a `Runner` from a typed runner function. `NewRunner`
builds one from a JSON function, for runners written without the `workflow`
package.

`Runtime.Execute` handles one delivery. With a nil `StepID` it executes a
flow; with `StepID` set it executes that step. A delivery for a terminal run or
step returns nil without running code. A non-nil error means the delivery
should be retried. `Execute` removes any Client put in the context with
`NewClientContext`, so a run never uses a Client it inherited.

A replay that diverges from the event log ends the delivery with an error
that runner code never sees. `Execute` queues an immediate recovery delivery
of the same flow or step with `ReplayDivergenceCount` incremented, and once
three recovery deliveries have diverged, fails the run with an error matching
`ErrCorruptedEventLog`, without running compensations.

When the World reports `DeploymentAffinity`, a delivery for a run pinned to
another deployment is re-routed to the run's deployment instead of replayed.
After three re-routes, or when the World returns `ErrDeploymentUnavailable`,
the run fails with an error matching `ErrDeploymentMismatch`. See
[Pinning runs to a deployment](/guide/replay#pinning-runs-to-a-deployment).

A flow whose write another flow superseded (`ErrInBandSuperseded`) queues one
follow-up flow delivery right away and acknowledges its own. Flows refused by
the same write share that follow-up. See
[Concurrent flows](/guide/replay#concurrent-flows).

A storage error while declaring a step, timer, or hook, or while cancelling a
promise or hook, ends the delivery so the queue retries it, instead of reaching
runner code.

When a flow replays past a step's declaration, it queues the step's delivery
again if the step hasn't started since it was declared or last scheduled for a
retry, in case a crash or queue error lost the first one. This covers steps
started with `Background` that nothing awaits. The re-queue uses the step ID as
its idempotency key. A step waiting for a retry that isn't due yet is
left alone, since its own queued retry runs it; once the retry time has passed
without a start, it is re-queued like any unstarted step. Steps declared in the
same replay are skipped, and inline steps never need it. If the re-queue fails,
the flow delivery fails so the queue retries it. Local and Postgres drop the
duplicate while the original delivery is still queued; AWS and Vercel don't
deduplicate, so a step waiting to start may be delivered twice, which its
at-least-once contract already allows. The Workflow SDK re-ensures pending
steps on replay the same way.

Timers settle the same way: a flow that replays past a timer's declaration
once it is due completes it, or, for a due `Timeout`, cancels the run.

## World

```go
type World interface {
	Storage
	Queue
	Streamer
	BaseURL() string
	Analytics() Analytics
	Capabilities() Capabilities
	SpecVersion() int
	DeploymentID() string
}

type Capabilities struct {
	Analytics          bool
	SetAttributes      bool
	DeploymentAffinity bool
}
```

A capability being false does not remove a method. Unsupported attribute writes
are successful no-ops, and `Analytics` returns nil.

`DeploymentID` returns the deployment whose code the World's deliveries
execute, or `""`. Runs created through the World's Client record it on
`Run.DeploymentID`. With `DeploymentAffinity`, the Runtime also enforces it:
deliveries are addressed to the run's deployment through
`QueueInput.DeploymentID`, and misdelivered ones are re-routed. Only the Vercel
World reports affinity.

## Storage

`Storage` is `StorageRuns`, `StorageSteps`, `StorageEvents`, and
`StorageHooks`.

| Method | Description |
| --- | --- |
| `RunsGet` | A run view. `ErrRunNotFound` if missing. |
| `RunsList` | Runs filtered by `WorkflowName` and `Status`, paginated. |
| `RunsWaitForTerminalStatus` | Blocks until the run is terminal, `Timeout` elapses, or `ctx` is done. |
| `RunsSetAttributes` | Applies attribute changes as one `attr_set` event. |
| `StepsGet`, `StepsList` | Step views of a run. |
| `EventsCreateRun` | Creates a run and its `run_created` event. `ErrRunAlreadyExists` if it exists. |
| `EventsCreate` | Appends one event, enforcing transition rules. Repeats return the earlier event. |
| `EventsCreateBatch` | Appends events atomically; none if any is rejected. |
| `EventsGet`, `EventsList`, `EventsListByCorrelationID` | Read the event log. |
| `HooksGet`, `HooksGetByToken`, `HooksList` | Hook views. |
| `HooksClaim` | Atomically claims a token and appends `hook_created`, optionally forcing. |

**In-band fence.** An append whose `EventsCreateOptions.InBandCount` (or
`EventsCreateBatchInput.InBandCount`) is set is an in-band write by the run's
orchestrator. The World appends it only when `Run.InBandCount` still equals that
count, advances the count by the events appended, and returns it as
`EventResult.InBandCount`. Otherwise it appends nothing and returns
`ErrInBandSuperseded`. Appends without a count are out-of-band: never refused,
and they leave the count alone. See
[Concurrent flows](/guide/replay#concurrent-flows).

Lists take `Pagination{Cursor, Limit, SortOrder}` and return
`Page[T]{Data, Cursor, HasMore}`. `ResolveData` selects whether payloads
(inputs, outputs, hook metadata) are returned.

### Views

```go
type Run struct {
	RunID        RunID
	WorkflowName string
	Status       Status
	Input        []byte // JSON workflow input
	Output       []byte // JSON workflow output
	Error        []byte // JSON Error; DecodeError decodes it
	Attributes   map[string]string
	DeploymentID string // the deployment the run is pinned to
	InBandCount  int    // events appended by the run's orchestrator, which fence its writes
	CreatedAt    time.Time
	UpdatedAt    time.Time
	StartedAt    *time.Time
	CompletedAt  *time.Time
}

type Step struct {
	RunID       RunID
	StepID      StepID
	StepName    string
	Status      Status // pending before the first attempt and while a retry is scheduled
	Input       []byte
	Output      []byte
	Error       []byte // last failed attempt
	Attempt     int
	CreatedAt   time.Time
	UpdatedAt   time.Time
	StartedAt   *time.Time
	CompletedAt *time.Time
	RetryAfter  *time.Time
}

type Hook struct {
	RunID           RunID
	HookID          HookID
	Token           string
	Metadata        []byte
	Webhook         *Webhook // set for webhooks
	ReceivedEventID EventID
	CreatedAt       time.Time
	UpdatedAt       time.Time
	ReceivedAt      *time.Time
	MinRetention    time.Duration
	RetainUntil     *time.Time
}
```

Run attributes `$parentRunId` (`ParentRunIDAttribute`) and `$rootRunId`
(`RootRunIDAttribute`) are set on child runs.

## Queue

```go
type Queue interface {
	Queue(context.Context, QueueInput) (QueueResult, error)
	QueueBatch(context.Context, QueueBatchInput) ([]QueueResult, error)
	QueueCancel(context.Context, QueueCancelInput) error
}

type QueueInput struct {
	QueueName      string     // empty selects the default queue
	RunID          RunID
	StepID         *StepID    // nil makes the delivery a flow
	NotBefore      *time.Time // nil allows immediate delivery
	IdempotencyKey string     // for implementations that deduplicate
	DeploymentID   string     // deployment to deliver to; empty uses the World's own

	DeploymentMismatchRetryCount int // re-routes so far, set by the Runtime
	ReplayDivergenceCount        int // recovery deliveries after divergence, set by the Runtime
}
```

Delivery is at least once. `QueueCancel` returns `ErrQueueMessageNotFound` for
an unknown message. A World with deployment affinity returns an error wrapping
`ErrDeploymentUnavailable` when the addressed deployment no longer exists.

## Streamer

| Method | Description |
| --- | --- |
| `StreamsCreateWriteSession` | Exclusive writer session for a stream. `ErrStreamLocked` if another holds it. |
| `StreamsWriteSession` | Appends chunks for a session at a sequence number. |
| `StreamsCloseWriteSession` | Releases a session, optionally closing the stream. |
| `StreamsWrite`, `StreamsWriteMulti` | Append when no session owns the stream. |
| `StreamsClose` | Marks a stream done when no session owns it. |
| `StreamsGetChunks` | Chunks after a cursor, waiting up to `Wait` for more. |
| `StreamsGetInfo` | Tail index and whether the stream is done. |
| `StreamsList` | Stream names of a run. |

## Analytics

`Analytics` is `AnalyticsRuns` (`RunsGet`, `RunsList` of run summaries),
`AnalyticsAttributes` (`AttributesList`, one aggregate per attribute key), and
`AnalyticsWaits` (`WaitsGet`, `WaitsList`). A World returns it only when
`Capabilities().Analytics` is true.

## Client

`Client` is the API outside a run. `workflow.Client` is an alias. Its first
methods are shared with the World:

| Method | Description |
| --- | --- |
| `RunsGet`, `RunsList`, `RunsWaitForTerminalStatus`, `RunsSetAttributes` | Read runs and change their attributes. |
| `StepsGet`, `StepsList` | Read steps. |
| `StreamsGetChunks`, `StreamsGetInfo`, `StreamsList` | Read streams. |
| `StreamsCreateWriteSession` | Open an exclusive writer session. |
| `HooksGetByToken` | Look up an active hook. |
| `Analytics` | As on World. |

The rest are the Client's own:

| Method | Description |
| --- | --- |
| `RunsCreate` | Starts a run and queues its first delivery; returns the run ID. |
| `RunsCancel` | Cancels a run; `ErrRunTerminal` if finished. |
| `RunsWake` | Settles pending waits early. |
| `StreamsWrite`, `StreamsWriteMulti`, `StreamsClose` | Write streams without a session. |
| `HooksResume` | Resumes the hook that owns a token. |
| `Spec` | The World's protocol version and capabilities, as a `ClientSpec`. |

```go
type ClientSpec struct {
	SpecVersion  int
	Capabilities Capabilities
}
```

`Spec` takes a context and returns an error, because the HTTP client asks the
World for it over RPC. The HTTP client caches a successful result.

```go
func NewClient(world World) Client
func NewHTTPClient(endpoint string, clients ...*http.Client) Client
func NewClientContext(ctx context.Context, client Client) context.Context
func ClientFromContext(ctx context.Context) (Client, bool)
```

## HTTP

```go
func HTTPHandler(world World, prefix string) http.Handler // empty prefix is /v1/workflow
func RPCHandler(world World) http.Handler
func WebhookHandler(world World) http.Handler
func NormalizeBaseURL(value string) (string, error)
```

See [HTTP Endpoints](/reference/http).

## Durable primitives

The `workflow` package is built on these functions, which read and append to
the `Execution` of the current delivery. They are exported for advanced
integrations. Each takes the runner's next position itself, with
`Execution.NextPosition`, so callers declare operations in order without passing
positions.

| Function | Description |
| --- | --- |
| `DurableStep` | Declares a step at the next position, or executes it in its step delivery. |
| `DurableInlineStep` | Runs an inline step's attempt in the flow and records its result, or queues the retry, records it, and suspends. |
| `DurableWait` | Declares a timer; with `Cancel` it cancels the run when it fires. |
| `DurableHook` | Creates a hook and claims its token, or matches its recorded declaration. |
| `Execution.Settled`, `Execution.SettledEvent` | Whether an operation's result has been delivered, and the event that delivered it. |
| `Execution.Select` | Walk the log until one of several operations has settled, or the log ends; reports which. |
| `Execution.CreateEvent` | Append one of this replay's writes, fenced for flows, and add it with any events appended since. A superseded write ends the replay with a follow-up queued; other World errors retry the delivery. A non-nil `mapError` sees the error first and can accept it. |
| `Execution.ClaimHook` | Claim a hook's token and append `hook_created`, fenced and handled like `CreateEvent`. A forced claim also queues a delivery that wakes the displaced run on its deployment. `ErrHookConflict` reaches `mapError`. |
| `StepOutcome`, `HookOutcome` | Decode a delivered step or hook result. |
| `Execution.InBandCount` | The in-band count a flow's writes are fenced with. |
| `WakeRun` | Completes pending waits in log order and queues a delivery. |
| `PendingWaits` | The IDs of a run's pending waits, in creation order. |
| `ResumeHook` | Appends a hook's payload and queues its run, addressed to the run's deployment. |
| `RunDeploymentID` | The deployment a run is pinned to, for addressing deliveries queued from outside it. |
| `ExecutionFromContext` | The replay state of the current delivery. |

## Errors

| Variable | Meaning |
| --- | --- |
| `ErrWorkflowNotFound` | No runner is registered for the run's workflow. |
| `ErrRunNotFound` | No run has the ID. |
| `ErrRunAlreadyExists` | `EventsCreateRun` with an existing ID. |
| `ErrRunTerminal` | Appending to or writing a stream of a finished run. |
| `ErrRunCancelled` | The run was cancelled. |
| `ErrStepNotFound` | No step has the ID. |
| `ErrStepCancelled` | The step was cancelled. |
| `ErrStepNotStartable` | Step transition not allowed. |
| `ErrEventNotFound` | No event (or wait) has the ID. |
| `ErrEventInvalid` | Malformed event, ID, or cursor, or an item over a backend limit. |
| `ErrHookNotFound` | No hook has the ID, or no active hook has the token. |
| `ErrHookNotActive` | The hook was resolved, disposed, or lost its token. |
| `ErrHookConflict` | Another active hook owns the token. |
| `ErrHookForceClaimed` | Another run took the token with `Force`. |
| `ErrStreamLocked` | Stream write conflicts with its writer session. |
| `ErrQueueMessageNotFound` | `QueueCancel` with an unknown message. |
| `ErrCorruptedEventLog` | The run's replay kept diverging from its event log. |
| `ErrDeploymentMismatch` | The run's deliveries kept reaching other deployments, or its deployment is gone. |
| `ErrDeploymentUnavailable` | `Queue` addressed a deployment that no longer exists. |
| `ErrInBandSuperseded` | An in-band write's count is stale because another flow wrote to the run first. |
| `ErrWebhookRequestTooLarge` | Webhook body over 128 KiB (`413`). |

```go
type Error struct{ /* ... */ }

func NewError(err error) *Error
func DecodeError(data []byte) *Error
```

`Error` keeps the type, message, and JSON encoding of every error in the chain
so `errors.Is` and `errors.As` work after replay.
