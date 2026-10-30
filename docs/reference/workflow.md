# Package workflow

```go
import "github.com/eriicafes/go-workflow"
```

The API that runners, steps, and applications use. Full godoc is on
[pkg.go.dev](https://pkg.go.dev/github.com/eriicafes/go-workflow).

The **Context** column says where a function can be called:
**W** workflow context (a runner), **S** step context (a step function),
**O** outside a run (with a Client in the context).

## Registration

```go
func Use[In worlds.Workflow[Out], Out any](fn func(context.Context, In) (Out, error)) worlds.Runner
```

| Function | Context | Description |
| --- | --- | --- |
| `Use` | — | Registers `fn` as the runner of workflow `In`. Pass the result to `worlds.Runners`. |

## Steps

```go
func Step[In, Out any](ctx context.Context, fn func(context.Context, In) (Out, error), input In, options ...StepOptions) (Out, error)
func Background[In, Out any](ctx context.Context, fn func(context.Context, In) (Out, error), input In, options ...StepOptions) *Result[Out]
func Map[In, Out any](ctx context.Context, step func(context.Context, In) (Out, error), inputs []In) []*Result[Out]
func MapResults[T any](results []*Result[T]) []promise
func All[In, Out any](ctx context.Context, step func(context.Context, In) (Out, error), inputs []In) ([]Out, error)
func InlineStep[In, Out any](ctx context.Context, fn func(context.Context, In) (Out, error), input In, options ...StepOptions) (Out, error)
func InlineBackground[In, Out any](ctx context.Context, fn func(context.Context, In) (Out, error), input In, options ...StepOptions) *Result[Out]
func SetMaxInlineSteps(ctx context.Context, n int) context.Context
func Rollback[In any](ctx context.Context, fn func(context.Context, In) error, input In, options ...StepOptions)
func FatalError(cause error) error
func RetryableError(cause error, after time.Duration) error
func StepID(ctx context.Context) string
func GetStepMetadata(ctx context.Context) StepMetadata
func GetWorkflowMetadata(ctx context.Context) WorkflowMetadata
```

| Function | Context | Description |
| --- | --- | --- |
| `Step` | W | Calls a step and waits for it. `Background` + `Value`. |
| `Background` | W | Starts a step without waiting. |
| `Map` | W | Starts one step per input. |
| `MapResults` | W, S, O | Converts result handles for combinators or group Add, preserving order. |
| `All` | W | Collects values in input order. Cancels remaining steps on any failure. |
| `InlineStep` | W | Calls an inline step, which the flow delivery runs, and waits for it. `InlineBackground` + `Value`. See [Inline Steps](/guide/inline-steps). |
| `InlineBackground` | W | Starts an inline step without waiting. The flow runs it when the runner waits. |
| `SetMaxInlineSteps` | — | Sets how many new steps a flow runs inline at once: 3 by default, 0 for none, at most 16. See [Running steps inline](/guide/steps#running-steps-inline). |
| `Rollback` | W | Defer after successful work to register compensation on exit. Runs only if the workflow fails; invalid context panics. |
| `FatalError` | S | Wraps an error a step returns so the step fails without retrying. Has no effect on a runner's error. |
| `RetryableError` | S | Wraps an error a step returns so the step retries no sooner than `after`. Has no effect on a runner's error. |
| `StepID` | S | Stable step ID, usable as an idempotency key. |
| `GetStepMetadata` | S | ID, name, start time, and attempt of the current step. |
| `GetWorkflowMetadata` | W, S | Workflow name, run ID, and start time. |

```go
type StepOptions struct {
	MaxRetries int // omitted options allow 3; the zero value allows none
}

type StepMetadata struct {
	ID        string
	Name      string
	StartedAt time.Time
	Attempt   int // starting at 1
}

type WorkflowMetadata struct {
	Name      string
	RunID     string
	StartedAt time.Time
}
```

## Results and promises

`Background`, timers, `NewResult`, and `Then` return a `Result`. Its `Value`
method waits for the value and error, and `Unwrap` waits for the value and
panics on failure.

The signatures below use `promise` for results, hooks, webhooks, and groups.
Selection returns the original input handle, which can be compared by identity.

```go
func (p *Result[T]) Value(ctx context.Context) (T, error)
func (p *Result[T]) Unwrap(ctx context.Context) T

func Join(ctx context.Context, results ...promise) error
func JoinOk(ctx context.Context, results ...promise) error
func Select(ctx context.Context, results ...promise) (promise, error)
func SelectOk(ctx context.Context, results ...promise) (promise, error)
func NewResult[T any](value T, err error) *Result[T]
func Then[T, U any](ctx context.Context, source *Result[T], fn func(T, error) *Result[U]) *Result[U]
```

| Function | Context | Description |
| --- | --- | --- |
| `Join` | W | Waits for all settlement, ignoring operation failures. |
| `JoinOk` | W | Waits for all successes. Returns the first recorded failure. |
| `Select` | W | First settlement and its error, in recorded order. |
| `SelectOk` | W | First success, or a nil winner and the first failed input's error if all fail. |
| `NewResult` | W, S, O | Creates an already-settled result. Reading it requires workflow context. |
| `Then` | W | Runs the callback after `source` settles and follows the result it returns. Return pending work instead of waiting inside the callback. |

An empty join succeeds. An empty selection returns a nil winner and no error.
None of these functions cancel inputs they stop waiting for.

## Groups

WaitGroup and ErrGroup wait for all members. Their zero values are ready to
use. WaitGroup ignores operation failures; ErrGroup returns the first recorded
failure. Groups begin observing members when first awaited.
Completed groups ignore new work: Add does nothing and Go returns nil without
starting a step.

```go
func Go[In, Out any](ctx context.Context, g group, step func(context.Context, In) (Out, error), input In, options ...StepOptions) *Result[Out]
func (g *WaitGroup) Add(members ...promise)
func (g *WaitGroup) Wait(ctx context.Context) error
func (g *ErrGroup) Add(members ...promise)
func (g *ErrGroup) Wait(ctx context.Context) error
func ErrGroupWithContext(ctx context.Context) (*ErrGroup, context.Context, error)
```

| Function or method | Context | Description |
| --- | --- | --- |
| `Go` | W | Starts a step and adds its result to either group. Returns nil if the group has completed. |
| `WaitGroup.Add`, `ErrGroup.Add` | W | Tracks existing handles without changing their cancellation contexts. |
| `WaitGroup.Wait` | W | Waits for all members, ignoring operation failures. |
| `ErrGroup.Wait` | W | Waits for all members and returns the first failure. Cancels its associated context on return. |
| `ErrGroupWithContext` | W | Creates an ErrGroup whose context cancels on the first failure or when Wait returns. |

A zero-value ErrGroup does not cancel work. Use ErrGroupWithContext and pass
its context to work that should stop on group failure. Cancellation is
cooperative.

## Cancellation

```go
func WithCancel(ctx context.Context) (context.Context, func() error, error)
func WithCancelCause(ctx context.Context) (context.Context, func(error) error, error)
func WithTimeout(ctx context.Context, duration time.Duration) (context.Context, func() error, error)
func WithDeadline(ctx context.Context, deadline time.Time) (context.Context, func() error, error)
func WithTimeoutCause(ctx context.Context, duration time.Duration, cause error) (context.Context, func() error, error)
func WithDeadlineCause(ctx context.Context, deadline time.Time, cause error) (context.Context, func() error, error)
func CancelAny(ctx context.Context, sources ...context.Context) context.Context
func CancelFunc(ctx context.Context) (func() error, bool)
```

| Function | Context | Description |
| --- | --- | --- |
| `WithCancel` | W | Creates a durable cancellation context. |
| `WithCancelCause` | W | Creates a cancellation context whose first committed cause wins. |
| `WithTimeout` | W | Cancels the returned context after a duration using a timer and `Then`. |
| `WithDeadline` | W | Cancels the returned context at an absolute deadline. |
| `WithTimeoutCause` | W | Like `WithTimeout`, with a custom expiry cause. |
| `WithDeadlineCause` | W | Like `WithDeadline`, with a custom expiry cause. |
| `CancelAny` | W, S, O | Preserves base values and cancels when base or any source cancels. Returns ctx unchanged outside workflow context. |
| `CancelFunc` | S | Finds the current context's controller for manual cancellation. |

See [Cancellation](/guide/cancellation) for cooperative step cancellation,
expiry causes, and cancelling entire runs. Standard context cancellation
constructors belong in steps or external code.

## Time

```go
func Now(ctx context.Context) (time.Time, error)
func Sleep(ctx context.Context, duration time.Duration) error
func Timer(ctx context.Context, duration time.Duration) *Result[struct{}]
```

| Function | Context | Description |
| --- | --- | --- |
| `Now` | W | Recorded current time in UTC. |
| `Sleep` | W | Suspends the run for `duration`. |
| `Timer` | W | Promise that settles after `duration`. |

## Runs

```go
func Start[In worlds.Workflow[Out], Out any](ctx context.Context, input In) (*Run[Out], error)
func Get[Out any](ctx context.Context, runID string) (*Run[Out], error)
```

| Function | Context | Description |
| --- | --- | --- |
| `Start` | W, S, O | Starts a run. In workflow context starts a child run. |
| `Get` | W, S, O | Handle to an existing run. |

### Run

```go
func (r *Run[Out]) ID() string
func (r *Run[Out]) Value(ctx context.Context) (Out, error)
func (r *Run[Out]) Cancel(ctx context.Context) error
func (r *Run[Out]) Status(ctx context.Context) (Status, error)
func (r *Run[Out]) Exists(ctx context.Context) (bool, error)
func (r *Run[Out]) WorkflowName(ctx context.Context) (string, error)
func (r *Run[Out]) WakeUp(ctx context.Context, options ...WakeOptions) (int, error)
func (r *Run[Out]) Reader(ctx context.Context, options ...ReaderOption) (StreamReader, error)
func (r *Run[Out]) Writer(ctx context.Context, options ...WriterOption) (io.WriteCloser, func() error, error)
```

| Method | Description |
| --- | --- |
| `ID` | Run ID. |
| `Value` | Output once finished. Blocks in step context and outside a run. In workflow context, returns `ErrRunNotFinished` for an unfinished run. |
| `Cancel` | Cancels the run. Uses an inline step in workflow context. |
| `Status` | Current status. |
| `Exists` | Whether the run is recorded. |
| `WorkflowName` | Workflow name. |
| `WakeUp` | Settles pending timers early. |
| `Reader` | Reads a stream from a chunk index (S, O). |
| `Writer` | Writes a stream (S, O). |

```go
type WakeOptions struct {
	CorrelationIDs []string // empty settles every pending wait
}
```

### Statuses

`Status` is an alias of `worlds.Status`.

| Constant | Value |
| --- | --- |
| `StatusPending` | `"pending"` |
| `StatusRunning` | `"running"` |
| `StatusCompleted` | `"completed"` |
| `StatusFailed` | `"failed"` |
| `StatusCancelled` | `"cancelled"` |

## Hooks

```go
func CreateHook[T any](ctx context.Context, options ...HookOption) *Hook[T]
func DefineHook[T any](options ...HookOption) HookDefinition[T]
func (d HookDefinition[T]) Create(ctx context.Context) *Hook[T]
func ResumeHook[T any](ctx context.Context, token string, value T) error
func ResumeHookWithRetries[T any](ctx context.Context, token string, value T, attempts int, interval time.Duration) error
func GetHook[T any](ctx context.Context, token string) (*Hook[T], bool, error)
func NewHookToken() string
```

| Function | Context | Description |
| --- | --- | --- |
| `CreateHook` | W | Creates a local handle. Receiving from or selecting this hook registers it. `GetConflict` claims ownership without awaiting payloads. |
| `DefineHook` | — | Declares a reusable hook. An empty token is generated when a handle is created. |
| `HookDefinition.Create` | W | Creates an unregistered handle for the declared hook. Invalid context errors are reported when used. |
| `ResumeHook` | S, O | Resumes the active hook with `token`. |
| `ResumeHookWithRetries` | S, O | Attempts resume immediately, then at fixed intervals while the hook is not found, up to `attempts` times. |
| `GetHook` | S, O | Looks up an active hook. The result cannot be awaited. |
| `NewHookToken` | — | Random 21-character token. |

```go
type HookOption struct {
	Token        string        // empty generates a replay-stable token
	MinRetention time.Duration // reserve until creation plus duration; Dispose releases immediately
	Force        bool          // take the token over from another run
}
```

### Hook

```go
func (h *Hook[T]) Next(ctx context.Context) (T, error)
func (h *Hook[T]) Values(ctx context.Context) iter.Seq2[T, error]
func (h *Hook[T]) Dispose(ctx context.Context)
func (h *Hook[T]) GetConflict(ctx context.Context) (*Run[any], error)
func (h *Hook[T]) ID() string
func (h *Hook[T]) Token() string
```

| Method | Context | Description |
| --- | --- | --- |
| `Next` | W | Registers automatically when waiting, then consumes the next payload. Conflicts return `ErrHookConflict`. |
| `Values` | W | Iterates with the same receive cursor. Stops after an error. |
| `Dispose` | W | Disposes durably and releases the token immediately. Panics on failure, including when deferred. |
| `GetConflict` | W | Registers and claims the token. Returns the conflicting owner, or nil. Repeated calls use the recorded result. |
| `ID`, `Token` | W, S, O | Reads identity without registering. An unregistered handle has an empty ID. |

A hook's `ClaimedFrom` field identifies the previous token owner through `RunID` and `HookID`. Reusable definitions belong beside workflow structs in a shared package. Directly defer `Dispose` unless relying on `MinRetention` after workflow completion.

## Webhooks

```go
func CreateWebhook[T any](ctx context.Context, options ...WebhookOption) (*Webhook[T], error)
```

| Function | Context | Description |
| --- | --- | --- |
| `CreateWebhook` | W | Creates a webhook with a random token. |

```go
type WebhookOption struct {
	Method   string         // empty accepts any method
	Response *http.Response // static response, body up to 128 KiB
	Manual   bool           // hold the request until RespondWith
}
```

### Webhook

```go
func (w *Webhook[T]) URL() string
func (w *Webhook[T]) Token() string
func (w *Webhook[T]) ID() string
func (w *Webhook[T]) Request(ctx context.Context) (*WebhookRequest[T], error)
func (w *Webhook[T]) Dispose(ctx context.Context)
```

| Method | Context | Description |
| --- | --- | --- |
| `URL` | W | Absolute endpoint URL. |
| `Token`, `ID` | W | Hook identity. |
| `Request` | W | Waits for the request. |
| `Dispose` | W | Durable disposal, deactivating the endpoint. Panics on failure, including when deferred. |

### WebhookRequest

```go
func (r *WebhookRequest[T]) JSON() (T, error)
func (r *WebhookRequest[T]) RespondWith(ctx context.Context, response *http.Response) error
```

Embeds the incoming HTTP request and implements JSON serialization for step input, preserving raw body and response origin.

| Method | Context | Description |
| --- | --- | --- |
| `JSON` | W, S | Parses the recorded body without consuming it. |
| `RespondWith` | S | Records a manual response, idempotently for the same content. |

## Streams

```go
func Writer(ctx context.Context, options ...WriterOption) (io.WriteCloser, func() error, error)
func Reader(ctx context.Context, options ...ReaderOption) (StreamReader, error)
```

| Function | Context | Description |
| --- | --- | --- |
| `Writer` | S | Writer for a stream of the current run, plus a release function. |
| `Reader` | S | Reader from `StartIndex`. Negative indexes count from the end. |

```go
type WriterOption struct{ Name string } // empty is the default stream
type ReaderOption struct {
	Name       string // empty is the default stream
	StartIndex int    // first chunk; negative counts back from the end
}
type StreamChunk struct {
	Index int // zero-based; each write is one chunk
	Data  []byte
}
type StreamReader interface {
	io.Reader
	Next() (StreamChunk, error)
	TailIndex() (int, error)
}
```

`StreamReader` reads bytes like any reader. `Next` returns one chunk at a
time, and `TailIndex` returns the index of the last chunk, or -1 for an empty
stream.

## Clients

```go
func NewClient(world World) Client
func NewHTTPClient(endpoint string, clients ...*http.Client) Client
func NewContext(ctx context.Context, client Client) context.Context
func Middleware(handler http.Handler, client Client) http.Handler
```

| Function | Description |
| --- | --- |
| `NewClient` | Client that calls a World in the same process. |
| `NewHTTPClient` | Client for a World served by `worlds.HTTPHandler`. `endpoint` includes the handler's prefix, such as `/v1/workflow`. |
| `NewContext` | Context carrying `client`. |
| `Middleware` | Adds `client` to every request context. |

`Client`, `World`, `Error`, and `Status` are aliases of the
[`worlds`](/reference/worlds) types.

## Errors

| Variable | Meaning |
| --- | --- |
| `ErrNoClient` | No Client in the context, outside a run. |
| `ErrNoWorkflow` | Workflow-context function called elsewhere, or stream function outside a run. |
| `ErrNotInStep` | I/O function called in workflow context. |
| `ErrRunCancelled` | Run was cancelled. |
| `ErrRunNotFinished` | `Run.Value` in workflow context on an unfinished run. |
| `ErrHookNotActive` | Hook was disposed and no longer receives. |
| `ErrHookNotFound` | No active hook has the token. |
| `ErrHookNotRegistered` | Receiving from a hook not created in this run. |
| `ErrHookConflict` | Another active hook owns the token. |
| `ErrHookForceClaimed` | Another run took the token with `Force`. |

`Error` (alias of `worlds.Error`) is the type of failed step and run errors.
See [Errors and Retries](/guide/errors).
