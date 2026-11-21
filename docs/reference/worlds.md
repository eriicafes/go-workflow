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
func NewRunner(name string, run func(context.Context, []byte) ([]byte, error)) Runner

func NewRuntime(world World, runners RunnerSet) *Runtime
func (*Runtime) Execute(ctx context.Context, input QueueInput) error
```

`workflow.Use` builds a `Runner` from a typed runner function. `NewRunner`
builds one from a function of payloads, for runners written without the
`workflow` package: it receives the run's input payload and returns its output
payload, read and written with the `DecodePayload` and `EncodePayload` methods
of the run's `Execution`, which encrypt as the run does.

`Runtime.Execute` handles one delivery. With a nil `StepID` it executes a
flow. With `StepID` set it executes that step. A delivery for a terminal run
returns nil without running code, and one for a terminal step queues the flow
that consumes its result instead of running the step. A non-nil error means the delivery
should be retried. `Execute` filters managed cancellation bindings and removes any Client put in the context with
`NewClientContext`, so a run never uses a Client it inherited.

Set `QueueInput.MessageID` to the ID `Queue` returned for the message before
calling `Execute`, and keep it the same when the message is redelivered. A
flow records it as the owner of each step it starts inline, so the
redelivery of a flow that stopped runs those steps again at once.

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
follow-up flow delivery right away and acknowledges its own. On a World that
deduplicates, flows refused by the same write share that follow-up. See
[Concurrent flows](/guide/replay#concurrent-flows).

A storage error while declaring a step, timer, or hook, or while cancelling a
context or disposing a hook, ends the delivery so the queue retries it, instead of reaching
runner code.

When a flow replays past a step's declaration, it queues the step's delivery
again if the step hasn't started since it was declared or last scheduled for a
retry, in case a crash or queue error lost the first one. This covers steps
started with `Background` that nothing awaits. The re-queue uses the step ID as
its idempotency key, or the retry's key for a step scheduled for a retry. A step waiting for a retry that isn't due yet is
left alone until its queued retry is due. Once that time has passed
without a start, it is re-queued like any unstarted step. Steps declared in the
same replay are skipped, and inline steps never need it. If the re-queue fails,
the flow delivery fails so the queue retries it. Local and Postgres drop the
duplicate while the original delivery is still queued. AWS and Vercel don't
deduplicate, so a step waiting to start may be delivered twice, which its
at-least-once contract already allows. The Workflow SDK re-ensures pending
steps on replay the same way.

Timers settle the same way: a flow that replays past a timer's declaration
once it is due completes it. `WithTimeout` and `WithDeadline` use normal timers
and continuations to cancel contexts. Cancelling those contexts affects the
steps and timers that use them, not the run.

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
	// DeleteData: the optional RunsDeleteDataMany. Every World deletes a
	// run's data as it finishes when its $retention attribute is "0".
	DeleteData bool
	// StartHook: EventsCreateRun creates EventsCreateRunInput.StartHook
	// with the run, or returns a *HookConflictError and creates nothing.
	StartHook bool
	// RegionalRunIDs: the World implements RegionalRunIDs, and clients ask
	// it for new run IDs.
	RegionalRunIDs bool
}
```

A capability being false does not remove a method. Unsupported attribute writes
are successful no-ops, and `Analytics` returns nil.

`DeploymentID` returns the deployment whose code the World's deliveries
execute, or `""`. Runs created through the World's Client record it on
`Run.DeploymentID`. With `DeploymentAffinity`, the Runtime also enforces it:
deliveries are addressed to the run's deployment through
`QueueInput.DeploymentID`, and misdelivered ones are re-routed. Of the built-in Worlds, only Vercel reports
affinity, and only when it has a deployment ID.

A World with affinity can also implement `LatestDeployments`, whose
`LatestDeploymentID(ctx)` returns its newest deployment, or `""` when it knows
none. `workflow.Start` resolves its `StartOptions.Deployment` through
`StartDeploymentID(ctx, world, deployment)`, which pins a run started with
`LatestDeployment` to it: child runs directly, and other runs through the
Client's `RunsStartDeployment`, resolved on the World's side of a local or
HTTP Client.

A World can choose a delivery's queue from `QueueInput.WorkflowName`, which the
Runtime and Client set on every delivery. `ResumeHook` takes it from the hook's
`ResumeContext`, and other code that queues for a run it hasn't loaded, such as
wake-ups and cancellation, reads the run for it. `RunDelivery(ctx, world,
runID)` returns a delivery addressed that way.

## Storage

`Storage` is `StorageRuns`, `StorageSteps`, `StorageEvents`, and
`StorageHooks`.

| Method | Description |
| --- | --- |
| `RunsGet` | A run view. `ErrRunNotFound` if missing. |
| `RunsList` | Runs filtered by `WorkflowName` and `Status`, paginated. |
| `RunsWaitForTerminalStatus` | Blocks until the run is terminal, `Timeout` elapses, or `ctx` is done. |
| `RunsSetAttributes` | Applies attribute changes as one `attr_set` event. |
| `RunsDeleteDataMany` | Deletes the data of up to 100 finished runs, as the Workflow SDK's `runs.deleteDataMany`: their payloads, the payloads in their events, their hooks' metadata, and their streams' chunks. Records stay, and the run's `ExpiredAt` is set. Without `Capabilities().DeleteData`, it returns an error wrapping `errors.ErrUnsupported`. |
| `StepsGet`, `StepsList` | Step views of a run. |
| `EventsCreateRun` | Creates a run and its `run_created` event. `ErrRunAlreadyExists` if it exists. |
| `EventsCreate` | Appends one event, enforcing transition rules. Hook creation atomically claims or records conflict, returning committed event data and the created Hook. |
| `EventsCreateBatch` | Appends events in order. An invalid event appends none of them; a refused event may leave others appended, as in the Workflow SDK. |
| `EventsGet`, `EventsList`, `EventsListByCorrelationID` | Read the event log. |
| `HooksGet`, `HooksGetByToken`, `HooksList` | Hook views. |

**In-band fence.** An append whose `EventsCreateOptions.InBandCount` (or
`EventsCreateBatchInput.InBandCount`) is set is an in-band write by the run's
orchestrator. The World appends it only when `Run.InBandCount` still equals that
count, advances the count by the events appended, and returns it as
`EventResult.InBandCount`. Otherwise it appends nothing and returns
`ErrInBandSuperseded`. Appends without a count are out-of-band: never refused,
and they leave the count alone. See
[Concurrent flows](/guide/replay#concurrent-flows).

Hook token ownership is acquired through `EventsCreate(hook_created)`, with conflict and takeover metadata in the same commit. There is no separate public claim operation. A hook's `TokenRetentionUntil` comes from its `hook_created` data. Explicit disposal releases immediately. Run, step, and hook views retain their timestamps. Step `StartedAt` is the first attempt. Step metadata describes the current attempt.

Lists take a `Pagination` with `Cursor`, `Limit`, and `SortOrder`, and return
a `Page` with `Data`, `Cursor`, and `HasMore`. `ResolveData` selects whether payloads
(inputs, outputs, hook metadata) are returned.

### Views

```go
type Run struct {
	RunID        RunID
	WorkflowName string
	Status       Status
	Input        []byte // payload: the argument list, [input]
	Output       []byte // payload
	Error        []byte // payload; DecodeErrorPayload decodes it
	Attributes   map[string]string
	DeploymentID string // the deployment the run is pinned to
	EncryptionPublicKey string // base64, set when the run's payloads are encrypted
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
	RunID               RunID
	ResumeContext       HookResumeContext // copied from the run when the hook is created
	HookID              HookID
	Token               string
	Metadata            []byte // payload; a webhook's is its WebhookMetadata
	IsWebhook           bool
	CreatedAt           time.Time
	UpdatedAt           time.Time
	ReceivedAt          *time.Time
	TokenRetentionUntil *time.Time
	ClaimedFrom         *HookForceClaim
	ForceClaimedBy      *HookForceClaim
	DisposedAt          *time.Time
}

type HookResumeContext struct {
	DeploymentID        string
	WorkflowName        string
	EncryptionPublicKey string // the run's public key, which payloads sent to the hook are sealed to
}
```

A view's payloads are in the Workflow SDK's format and may be encrypted; a
World stores them as opaque bytes. See [Payloads](#payloads).

Run attributes `$parentRunId` (`ParentRunIDAttribute`) and `$rootRunId`
(`RootRunIDAttribute`) are set on child runs.

## Queue

```go
type Queue interface {
	Queue(context.Context, QueueInput) (QueueResult, error)
	QueueBatch(context.Context, QueueBatchInput) ([]QueueResult, error)
}

type QueueInput struct {
	QueueName      string // empty selects the default queue
	WorkflowName   string // workflow of the run, for Worlds that route by it
	RunID          RunID
	StepID         *StepID    // nil makes the delivery a flow
	NotBefore      *time.Time // nil allows immediate delivery
	IdempotencyKey string     // for implementations that deduplicate
	DeploymentID   string     // deployment to deliver to; empty uses the World's own

	DeploymentMismatchRetryCount int    // re-routes so far, set by the Runtime
	ReplayDivergenceCount        int    // recovery deliveries after divergence, set by the Runtime
	MessageID                    string // the delivered message's ID, set by the World before Execute; Queue ignores it
	Attempt                      int    // 1-based delivery attempt, set by the World before Execute; 0 is not counted
}
```

Delivery is at least once, and a queued delivery cannot be cancelled: one that
is no longer needed only replays its run. A World sets `Attempt` from its
backend's delivery count, and the Runtime gives up on a delivery past
`MaxDeliveries` (48) attempts, as the Workflow SDK does: a flow delivery fails
its run, and a step delivery fails its step, both with
`ErrMaxDeliveriesExceeded`. A World with deployment affinity returns an error wrapping
`ErrDeploymentUnavailable` when the addressed deployment no longer exists.

### Routing

```go
type Workflow[Out any] interface{ Workflow(Out) }

func WorkflowName[In Workflow[Out], Out any]() string
func Route[In Workflow[Out], Out any](queue string) QueueRoute
func Routes(routes ...QueueRoute) map[string]string
```

`Workflow` is satisfied by a workflow type, such as one embedding
`workflow.Struct`. `WorkflowName` returns the name its runs are recorded under,
its package path and type name. `Routes(Route[W](queue), ...)` builds the
`Config.Routes` of a World that routes deliveries by workflow.

### Retention

```go
type WorkflowRetention struct {
	Workflow  string
	Retention time.Duration
}

func Retain[In Workflow[Out], Out any](retention time.Duration) WorkflowRetention
func Retentions(retentions ...WorkflowRetention) map[string]time.Duration
func RetentionFor(fallback time.Duration, retentions map[string]time.Duration, workflow string) time.Duration
func MaxRetention(fallback time.Duration, retentions map[string]time.Duration) (longest time.Duration, negative bool)
```

`Retentions(Retain[W](retention), ...)` builds the `Config.Retentions` of a
World that supports [retention](/worlds/#retention). `RetentionFor` returns how
long finished runs of a workflow are kept, its entry or else the fallback, where
zero keeps them. `MaxRetention` returns the longest retention and whether any
is negative, for validating a configuration.

## Streamer

| Method | Description |
| --- | --- |
| `StreamsWrite`, `StreamsWriteMulti` | Append chunks. `ErrStreamClosed` once the stream is closed. |
| `StreamsClose` | Marks a stream done for every writer. Closing it again does nothing. |
| `StreamsGetChunks` | Chunks after a cursor, or from `StartIndex` when the cursor is empty, waiting up to `Wait` for more. A negative `StartIndex` counts back from the end; `worlds.StreamsStartIndex` resolves it. |
| `StreamsGetInfo` | Tail index and whether the stream is done. |
| `StreamsList` | Stream names of a run. |

Streams have no exclusive writers, as in the Workflow SDK: any number of
writers append to a stream at once and their chunks interleave.
`OpenStreamWriter` returns a `StreamWriter` for one writer, through the
optional `StreamWriterOpener` interface when the World implements it, and otherwise
through `StreamsWriteMulti` and `StreamsClose`:

```go
type StreamWriterOpener interface {
	StreamsOpenWriter(context.Context, StreamsOpenWriterInput) (StreamWriter, error)
}

type StreamsOpenWriterInput struct {
	RunID    RunID
	Name     string
	WriterID string // from NewWriterID, "wrtr_" and a ULID
}

type StreamWriter interface {
	Write(ctx context.Context, chunks [][]byte) error
	Release(ctx context.Context) error     // ends this writer, leaving the stream open
	CloseStream(ctx context.Context) error // closes the stream for every writer
}
```

A World implements `StreamWriterOpener` to hold a connection for a writer's
lifetime. `HTTPHandler` serves the Workflow SDK's stream socket protocol at
`<prefix>/websockets/v1/runs/{runId}/streams/{name}?writerId=`, behind the same
RPC protection, and `NewHTTPClient` writes over it, falling back to RPC writes
while it opens and when it is declined or fails. `OpenStreamSocketWriter` and
`StreamSocketHandler` are the two ends, for a World whose backend speaks the
protocol, as Vercel's does.

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
| `RunsGet`, `RunsList`, `RunsWaitForTerminalStatus`, `RunsSetAttributes`, `RunsDeleteDataMany` | Read runs, change their attributes, and delete their data. |
| `StepsGet`, `StepsList` | Read steps. |
| `StreamsGetChunks`, `StreamsGetInfo`, `StreamsList` | Read streams. |
| `StreamsOpenWriter` | Open a stream writer. |
| `HooksGet`, `HooksGetByToken` | Look up hook metadata by ID, or by the token of an active or retained hook. |
| `Analytics` | As on World. |

The rest are the Client's own:

| Method | Description |
| --- | --- |
| `RunsCreate` | Starts a run and queues its first delivery. Returns the run ID. |
| `RunsCancel` | Cancels a run. Returns `ErrRunTerminal` if finished. |
| `RunsWake` | Settles pending waits early. |
| `StreamsWrite`, `StreamsWriteMulti`, `StreamsClose` | Write streams one call at a time. |
| `HooksResume` | Resolves the token owner once, records a payload, and queues the run. Ownership races reach the caller. |
| `Spec` | The World's protocol version and capabilities, as a `ClientSpec`. |
| `RunKeys` | The keys of a run's payloads, or nil when they are not encrypted. |
| `RunsStartDeployment` | The deployment a new run starts on, so its input is encrypted for it. |

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

## Payloads

```go
func EncodePayload(value any, options PayloadOptions) ([]byte, error)
func DecodePayload(data []byte, keys *RunKeys, value any) error
func EncodeErrorPayload(err error, options PayloadOptions) ([]byte, error)
func DecodeErrorPayload(data []byte, keys *RunKeys) *Error

type PayloadOptions struct {
	Keys          *RunKeys // encrypt with the run's key (encr)
	SealTo        []byte   // seal to a run's public key (encp)
	NoCompression bool
}

type EncryptionKeys interface {
	EncryptionKeyForRun(ctx context.Context, runID RunID, deploymentID string) ([]byte, error)
}

func RunKeysFor(ctx context.Context, world any, runID RunID, deploymentID string) (*RunKeys, error)
func NewRunKeys(runKey []byte) (*RunKeys, error)
func DeriveRunKey(key []byte, scope string, runID RunID) ([]byte, error)
func ConfiguredRunKey(key []byte, runID RunID) ([]byte, error)
```

Payloads use the Workflow SDK's format: devalue text labelled `devl`,
optionally compressed (`gzip`, `zstd`) and encrypted (`encr`, `encp`). See
[Data Format and Encryption](/guide/encryption). `EncryptionKeys` is the
optional World interface the SDK calls `getEncryptionKeyForRun`. A World that
takes a `Config.EncryptionKey` returns `ConfiguredRunKey(key, runID)`.
`RunKeysFor` resolves and caches a run's keys. `EncodeStreamFrame` and
`SplitStreamFrames` write and read stream frames, and `PayloadMarshaler`,
`PayloadUnmarshaler`, and `PayloadReduced` let a type choose its encoding,
such as `WebhookRequest`, `WebhookResponse`, and `WebhookMetadata`, the SDK's
`Request`, `Response`, and `{respondWith}`.

## Durable primitives

The `workflow` package is built on these functions, which read and append to
the `Execution` of the current delivery. They are exported for advanced
integrations. Operations normally take the runner's next position with
`Execution.NextPosition`. Lazy hooks reserve a position at creation and pass it
to `DurableHook` when receiving, selecting, or explicitly checking ownership.

Each declaration's ID carries its position: `NewStepID`, `NewHookID`, and
`NewWaitID` mint ULIDs whose entropy holds it, and `OperationPosition`
recovers it. A World whose backend cannot store a declaration's data, as
Vercel's keeps only a wait's resume time, rebuilds the position from the ID.

| Function | Description |
| --- | --- |
| `DurableStep` | Declares a step at the next position, or executes it in its step delivery. |
| `DurableInlineStep` | Runs an inline step's attempt in the flow and records its result, or queues the retry, records it, and suspends. |
| `DurableWait` | Declares a durable timer. Context cancellation can end its wait early. |
| `DurableHook` | Creates a hook and claims its token, or matches its recorded declaration. Takes options only and uses the execution’s delivery context. |
| `Execution.Settled`, `Execution.SettledEvent` | Whether an operation's result has been delivered, and the event that delivered it. |
| `Execution.Advance` | Deliver the next recorded event and run continuations. Returns false at the end of the loaded log. |
| `Execution.IsSuspended` | Whether the runtime stopped replay before the workflow body finished. Deferred cleanup skips work in this case. |
| `Execution.CreateEvent` | Append one of this replay's writes, fenced for flows, and add it with any events appended since. A superseded write ends the replay with a follow-up queued. Other World errors retry the delivery. A non-nil `mapError` sees the error first and can accept it. |
| `StepOutcome`, `HookOutcome` | Decode a delivered step or hook result. |
| `Execution.InBandCount` | The in-band count a flow's writes are fenced with. |
| `WakeRun` | Completes pending waits in log order and queues a delivery. |
| `PendingWaits` | The IDs of a run's pending waits, in creation order. |
| `ResumeHook` | Appends a hook's payload and queues its run, addressed to the run's deployment. |
| `RunDeploymentID` | The deployment a run is pinned to, for addressing deliveries queued from outside it. |
| `NewRunHookToken` | Derives a stable token from a run and hook declaration position. |
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
| `ErrStepNotStartable` | Step transition not allowed. |
| `ErrEventNotFound` | No event (or wait) has the ID. |
| `ErrEventInvalid` | Malformed event, ID, or cursor, or an item over a backend limit. |
| `ErrHookNotFound` | No hook has the ID, or no hook owns the token. |
| `ErrHookNotActive` | The hook was resolved, disposed, or lost its token. |
| `ErrHookConflict` | Another active hook owns the token. |
| `ErrHookForceClaimed` | Another hook took the token with `Force`. |
| `ErrResumeNotQueued` | A hook payload was recorded but its run could not be queued. |
| `ErrStreamClosed` | Writing to a closed stream. |
| `ErrMaxDeliveriesExceeded` | A delivery went past `MaxDeliveries` attempts. |
| `ErrCorruptedEventLog` | The run's replay kept diverging from its event log. |
| `ErrDeploymentMismatch` | The run's deliveries kept reaching other deployments, or its deployment is gone. |
| `ErrDeploymentUnavailable` | `Queue` addressed a deployment that no longer exists. |
| `ErrInBandSuperseded` | An in-band write's count is stale because another flow wrote to the run first. |
| `ErrWebhookRequestTooLarge` | Webhook body over 128 KiB (`413`). |

```go
type Error struct {
	// Error chain fields omitted.
}

func NewError(err error) *Error
func DecodeError(data []byte) *Error
func DeliveryError(cause error) error
```

`DeliveryError` marks a World error that a step function hit while doing the
runtime's own work, such as recording a hook's disposal. The delivery is
retried, instead of the step recording a failed attempt.

`Error` keeps the type, message, and JSON encoding of every error in the chain
so `errors.Is` and `errors.As` work after replay.
