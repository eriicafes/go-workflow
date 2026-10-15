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

`Client` is the API outside a run. `workflow.Client` is an alias.

| Method | Description |
| --- | --- |
| `RunsCreate` | Starts a run and queues its first delivery; returns the run ID. |
| `RunsGet`, `RunsList`, `RunsWaitForTerminalStatus` | Read runs. |
| `RunsCancel` | Cancels a run; `ErrRunTerminal` if finished. |
| `RunsWake` | Settles pending waits early. |
| `RunsSetAttributes` | Changes attributes. |
| `StepsGet`, `StepsList` | Read steps. |
| `StreamsGetChunks`, `StreamsGetInfo`, `StreamsList` | Read streams. |
| `StreamsCreateWriteSession`, `StreamsWrite`, `StreamsWriteMulti`, `StreamsClose` | Write streams. |
| `HooksGetByToken`, `HooksResume` | Look up and resume hooks. |
| `Analytics`, `Capabilities` | As on World. |

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
integrations.

| Function | Description |
| --- | --- |
| `DurableStep` | Declares a step at a position; records it, replays its result, or executes it. |
| `DurableInlineStep` | Records an inline step's result. |
| `DurableWait` | Declares a timer; with `Cancel` it cancels the run when it fires. |
| `DurableHook` | Creates a hook and claims its token, or returns its recorded state. |
| `WakeRun` | Completes pending waits and queues a delivery. |
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
| `ErrWebhookRequestTooLarge` | Webhook body over 128 KiB (`413`). |

```go
type Error struct{ /* ... */ }

func NewError(err error) *Error
func DecodeError(data []byte) *Error
```

`Error` keeps the type, message, and JSON encoding of every error in the chain
so `errors.Is` and `errors.As` work after replay.
