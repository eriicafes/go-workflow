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

| Function | Context | Description |
| --- | --- | --- |
| `Use[In workflow[Out], Out any](fn func(context.Context, In) (Out, error)) worlds.Runner` | — | Registers `fn` as the runner of workflow `In`. Pass the result to `worlds.Runners`. |

## Steps

| Function | Context | Description |
| --- | --- | --- |
| `Step(ctx, fn, input, ...StepOptions) (Out, error)` | W | Calls a step and waits for it. `Background` + `Value`. |
| `Background(ctx, fn, input, ...StepOptions) *Result[Out]` | W | Starts a step without waiting. |
| `Map(ctx, fn, inputs) []*Result[Out]` | W | Starts one step per input. |
| `All(ctx, fn, inputs) ([]Out, error)` | W | Collects values in input order. Cancels remaining steps on any failure. |
| `InlineStep(ctx, fn func(context.Context, In) (Out, error), input In, ...StepOptions) (Out, error)` | W, S | Records work in the flow delivery. See [Inline Steps](/guide/inline-steps). In step context calls `fn` directly. |
| `OnRollback(ctx, fn func(context.Context, In) error, input, ...StepOptions) error` | W | Registers a compensation that runs if the run fails. |
| `FatalError(err) error` | S | Wraps an error so the step fails without retrying. |
| `RetryableError(err, after) error` | S | Wraps an error so the step retries no sooner than `after`. |
| `StepID(ctx) string` | S | Stable step ID, usable as an idempotency key. |
| `GetStepMetadata(ctx) StepMetadata` | S | ID, name, start time, and attempt of the current step. |
| `GetWorkflowMetadata(ctx) WorkflowMetadata` | W, S | Workflow name, run ID, and start time. |

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

`Background`, timers, `NewResult`, and `Then` return `*Result[T]`, which exposes `Value(ctx) (T, error)` and `Unwrap(ctx) T`. The settlement method is private to the SDK and cannot be implemented independently by application types. There is no public readiness or cancellation method.

The combinators accept SDK waiting handles with mixed value types, including hooks and webhooks. Their unexported parameter and winner interface is shown as `promise` below. Callers pass handles directly and compare the winner by identity.

| Function | Context | Description |
| --- | --- | --- |
| `Join(ctx, ...promise) error` | W | Waits for all settlement, ignoring operation failures. |
| `JoinOk(ctx, ...promise) error` | W | Waits for all successes. Returns the first recorded failure without cancelling other inputs. |
| `Select(ctx, ...promise) (promise, error)` | W | First settlement and its error, in recorded order. |
| `SelectOk(ctx, ...promise) (promise, error)` | W | First success, or a nil winner if all fail. |
| `NewResult[T any](value T, err error) *Result[T]` | W, S, O | Creates an already-settled result. Reading it requires workflow context. |
| `Then[T, U any](ctx, source *Result[T], fn func(T, error) *Result[U]) *Result[U]` | W | Follows the callback's returned result without waiting inside the callback. |

An empty join succeeds. An empty selection returns a nil winner and no error.

## Cancellation

| Function | Context | Description |
| --- | --- | --- |
| `WithCancel(ctx) (context.Context, func() error, error)` | W | Creates a durable cancellation context. |
| `WithCancelCause(ctx) (context.Context, func(error) error, error)` | W | Creates a cancellation context whose first committed cause wins. |
| `WithTimeout(ctx, d) (context.Context, func() error, error)` | W | Cancels the returned context after a duration using a timer and `Then`. |
| `WithDeadline(ctx, deadline) (context.Context, func() error, error)` | W | Cancels the returned context at an absolute deadline. |
| `WithTimeoutCause(ctx, d, cause) (context.Context, func() error, error)` | W | Like `WithTimeout`, with a custom expiry cause. |
| `WithDeadlineCause(ctx, deadline, cause) (context.Context, func() error, error)` | W | Like `WithDeadline`, with a custom expiry cause. |
| `CancelAny(ctx, sources ...context.Context) context.Context` | W, S, O | Preserves base values and cancels when base or any source cancels. Returns ctx unchanged outside workflow context. |
| `CancelFunc(ctx) (func() error, bool)` | S | Finds the current context's controller for manual cancellation. |

See [Cancellation](/guide/cancellation) for cooperative step cancellation,
expiry causes, and cancelling entire runs. Standard context cancellation
constructors belong in steps or external code.

## Time

| Function | Context | Description |
| --- | --- | --- |
| `Now(ctx) (time.Time, error)` | W | Recorded current time in UTC. |
| `Sleep(ctx, d) error` | W | Suspends the run for `d`. |
| `Timer(ctx, d) *Result[struct{}]` | W | Promise that settles after `d`. |

## Runs

| Function | Context | Description |
| --- | --- | --- |
| `Start[In workflow[Out], Out any](ctx, input In) (*Run[Out], error)` | W, S, O | Starts a run. In workflow context starts a child run. |
| `Get[Out any](ctx, runID string) (*Run[Out], error)` | W, S, O | Handle to an existing run. |

### `Run[Out]`

| Method | Description |
| --- | --- |
| `ID() string` | Run ID. |
| `Value(ctx) (Out, error)` | Output once finished. Blocks outside a run. Returns `ErrRunNotFinished` inside one. |
| `Cancel(ctx) error` | Cancels the run. Uses an inline step in workflow context. |
| `Status(ctx) (Status, error)` | Current status. |
| `Exists(ctx) (bool, error)` | Whether the run is recorded. |
| `WorkflowName(ctx) (string, error)` | Workflow name. |
| `WakeUp(ctx, ...WakeOptions) (int, error)` | Settles pending timers early. |
| `Reader(ctx, ...ReaderOption) (io.Reader, error)` | Reads a stream from the start (S, O). |
| `ReaderAt(ctx, offset, ...ReaderOption) (io.Reader, error)` | Reads a stream from an offset (S, O). |
| `Writer(ctx, ...WriterOption) (io.WriteCloser, func() error, error)` | Writes a stream (S, O). |

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

| Function | Context | Description |
| --- | --- | --- |
| `CreateHook[T](ctx, ...HookOption) *Hook[T]` | W | Creates a local handle. Receiving from or selecting this hook registers it. `GetConflict` claims ownership without awaiting payloads. |
| `DefineHook[T](...HookOption) HookDefinition[T]` | — | Declares a reusable hook. An empty token is generated when a handle is created. |
| `(HookDefinition[T]) Create(ctx) *Hook[T]` | W | Creates an unregistered handle for the declared hook. Invalid context errors are reported when used. |
| `ResumeHook[T](ctx, token, value) error` | S, O | Resumes the active hook with `token`. |
| `ResumeHookWithRetries[T](ctx, token, value, attempts, interval) error` | S, O | Attempts resume immediately, then at fixed intervals while the hook is not found. |
| `GetHook[T](ctx, token) (*Hook[T], bool, error)` | S, O | Looks up an active hook. The result cannot be awaited. |
| `NewHookToken() string` | — | Random 21-character token. |

```go
type HookOption struct {
	Token        string        // empty generates a replay-stable token
	MinRetention time.Duration // reserve until creation plus duration; Dispose releases immediately
	Force        bool          // take the token over from another run
}
```

### `Hook[T]`

| Method | Context | Description |
| --- | --- | --- |
| `Next(ctx) (T, error)` | W | Registers automatically when waiting, then consumes the next payload. Conflicts return `ErrHookConflict`. |
| `Values(ctx) iter.Seq2[T, error]` | W | Iterates with the same receive cursor. Stops after an error. |
| `Dispose(ctx)` | W | Disposes durably and releases the token immediately. Panics on failure, including when deferred. |
| `GetConflict(ctx) (*Run[any], error)` | W | Registers and claims the token. Returns the conflicting owner, or nil. Repeated calls use the recorded result. |
| `ID() string`, `Token() string` | W, S, O | Reads identity without registering. An unregistered handle has an empty ID. |

`ClaimedFrom *worlds.HookForceClaim` identifies the previous token owner through `RunID` and `HookID`. Reusable definitions belong beside workflow structs in a shared package. Directly defer `Dispose` unless relying on `MinRetention` after workflow completion.

## Webhooks

| Function | Context | Description |
| --- | --- | --- |
| `CreateWebhook[T](ctx, ...WebhookOption) (*Webhook[T], error)` | W | Creates a webhook with a random token. |

```go
type WebhookOption struct {
	Method   string         // empty accepts any method
	Response *http.Response // static response, body up to 128 KiB
	Manual   bool           // hold the request until RespondWith
}
```

### `Webhook[T]`

| Method | Context | Description |
| --- | --- | --- |
| `URL() string` | W | Absolute endpoint URL. |
| `Token() string`, `ID() string` | W | Hook identity. |
| `Request(ctx) (*WebhookRequest[T], error)` | W | Waits for the request. |
| `Dispose(ctx)` | W | Durable disposal, deactivating the endpoint. Panics on failure, including when deferred. |

### `WebhookRequest[T]`

Embeds `*http.Request` and implements JSON serialization for step input, preserving raw body and response origin.

| Method | Context | Description |
| --- | --- | --- |
| `JSON() (T, error)` | W, S | Parses the recorded body without consuming it. |
| `RespondWith(ctx, *http.Response) error` | S | Records a manual response, idempotently for the same content. |

## Streams

| Function | Context | Description |
| --- | --- | --- |
| `Writer(ctx, ...WriterOption) (io.WriteCloser, func() error, error)` | S | Writer for a stream of the current run, plus a release function. |
| `Reader(ctx, ...ReaderOption) (io.Reader, error)` | S | Reader from the beginning. |
| `ReaderAt(ctx, offset, ...ReaderOption) (io.Reader, error)` | S | Reader from `offset`. Negative offsets count from the end. |

```go
type WriterOption struct{ Name string } // empty is the default stream
type ReaderOption struct{ Name string }
```

## Clients

| Function | Description |
| --- | --- |
| `NewClient(world World) Client` | Client that calls a World in the same process. |
| `NewHTTPClient(endpoint string, ...*http.Client) Client` | Client for a World served by `worlds.HTTPHandler`. `endpoint` includes `/v1/workflow`. |
| `NewContext(ctx, client) context.Context` | Context carrying `client`. |
| `Middleware(handler, client) http.Handler` | Adds `client` to every request context. |

`Client`, `World`, `Error`, and `Status` are aliases of the
[`worlds`](/reference/worlds) types.

## Errors

| Variable | Meaning |
| --- | --- |
| `ErrNoClient` | No Client in the context, outside a run. |
| `ErrNoWorkflow` | Workflow-context function called elsewhere, or stream function outside a run. |
| `ErrNotInStep` | I/O function called in workflow context. |
| `ErrRunCancelled` | Run was cancelled. |
| `ErrRunNotFinished` | `Run.Value` inside a run on an unfinished run. |
| `ErrHookNotActive` | Hook was disposed and no longer receives. |
| `ErrHookNotFound` | No active hook has the token. |
| `ErrHookNotRegistered` | Receiving from a hook not created in this run. |
| `ErrHookConflict` | Another active hook owns the token. |
| `ErrHookForceClaimed` | Another run took the token with `Force`. |

`*Error` (alias of `worlds.Error`) is the type of failed step and run errors.
See [Errors and Retries](/guide/errors).
