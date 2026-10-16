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
| `Step(ctx, fn, input, ...StepOptions) (Out, error)` | W | Calls a step and waits for it. `Background` + `Await`. |
| `Background(ctx, fn, input, ...StepOptions) Promise[Out]` | W | Starts a step without waiting. |
| `Map(ctx, fn, inputs) []Promise[Out]` | W | Starts one step per input. |
| `All(ctx, fn, inputs) ([]Out, error)` | W | `Map` + `Join`. |
| `InlineStep(ctx, fn func(context.Context, In) (Out, error), input In, ...StepOptions) (Out, error)` | W, S | Runs the step `fn` with `input` in the flow, without a step delivery, retrying failed attempts like a step. In step context calls `fn` without recording. |
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

## Promises

```go
type Promise[T any] interface {
	Done(context.Context) (bool, error) // settled as of delivered results; never the promise's error
	Await(context.Context) (T, error)
	Unwrap(context.Context) T
	Cancel(context.Context) error
}
```

| Function | Context | Description |
| --- | --- | --- |
| `Join(ctx, ...Promise[T]) ([]T, error)` | W | Waits for all; first error in argument order. |
| `JoinSettled(ctx, ...Promise[T]) ([]T, error)` | W | Waits for all to settle; zero values for failures. |
| `Select(ctx, ...Promise[T]) (Promise[T], error)` | W | First to settle, by the order results were recorded. |
| `SelectWithCancel(ctx, ...Promise[T]) (Promise[T], error)` | W | `Select`, cancelling the rest. |
| `SelectOk(ctx, ...Promise[T]) (Promise[T], error)` | W | First to succeed, by the order results were recorded. |
| `SelectOkWithCancel(ctx, ...Promise[T]) (Promise[T], error)` | W | `SelectOk`, cancelling the rest. |
| `Cancel(ctx, ...Promise[T]) error` | W | Cancels each promise in order. |

## Time

| Function | Context | Description |
| --- | --- | --- |
| `Now(ctx) (time.Time, error)` | W | Recorded current time in UTC. |
| `Sleep(ctx, d) error` | W | Suspends the run for `d`. |
| `Timer(ctx, d) Promise[struct{}]` | W | Promise that settles after `d`. |
| `Timeout(ctx, d) Promise[struct{}]` | W | Timer that cancels the run when it fires. |

## Runs

| Function | Context | Description |
| --- | --- | --- |
| `Start[In workflow[Out], Out any](ctx, input In) (*Run[Out], error)` | W, S, O | Starts a run. In workflow context starts a child run. |
| `Get[Out any](ctx, runID string) (*Run[Out], error)` | W, S, O | Handle to an existing run. |

### `Run[Out]`

| Method | Description |
| --- | --- |
| `ID() string` | Run ID. |
| `Await(ctx) (Out, error)` | Output once finished. Blocks outside a run; returns `ErrRunNotFinished` inside one. |
| `Done(ctx) (bool, error)` | Whether finished, without blocking. Does not return the run's error. |
| `Unwrap(ctx) Out` | `Await`, panicking on error. |
| `Cancel(ctx) error` | Cancels the run. |
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
| `CreateHook[T](ctx, ...HookOption) (*Hook[T], error)` | W | Creates a hook. `DefineHook` + `Create`. |
| `DefineHook[T](...HookOption) HookDefinition[T]` | — | Declares a hook; its token is fixed now. |
| `(HookDefinition[T]) Create(ctx) (*Hook[T], error)` | W | Creates the declared hook. |
| `ResumeHook[T](ctx, token, value) error` | S, O | Resumes the active hook with `token`. |
| `ResumeHookWithRetries[T](ctx, token, value, attempts) error` | S, O | Retries while the hook is not found. |
| `GetHook[T](ctx, token) (*Hook[T], bool, error)` | S, O | Looks up an active hook; the result cannot be awaited. |
| `NewHookToken() string` | — | Random 21-character token. |

```go
type HookOption struct {
	Token        string        // empty generates a random token
	MinRetention time.Duration // keep the token reserved after the hook settles
	Force        bool          // take the token over from another run
}
```

### `Hook[T]`

`Await`, `Done`, `Unwrap`, `Cancel` (the `Promise[T]` methods), plus `ID()`,
`Token()`, and `GetConflict(ctx) (worlds.RunID, bool, error)`.

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

| Method | Description |
| --- | --- |
| `URL() string` | Absolute URL under the World's base URL. |
| `Token() string`, `ID() string` | Hook token and ID. |
| `Await(ctx) (T, error)` | JSON body decoded as `T`. |
| `Request(ctx) (*http.Request, error)` | The request that resumed the webhook. |
| `RespondWith(ctx, *http.Response) error` | Responds to a manual webhook's request. |
| `Done`, `Unwrap`, `Cancel`, `GetConflict` | As on `Hook`. |
| `MarshalJSON`, `UnmarshalJSON` | Pass a webhook to or from a step. |

## Streams

| Function | Context | Description |
| --- | --- | --- |
| `Writer(ctx, ...WriterOption) (io.WriteCloser, func() error, error)` | S | Writer for a stream of the current run, plus a release function. |
| `Reader(ctx, ...ReaderOption) (io.Reader, error)` | S | Reader from the beginning. |
| `ReaderAt(ctx, offset, ...ReaderOption) (io.Reader, error)` | S | Reader from `offset`; negative counts from the end. |

```go
type WriterOption struct{ Name string } // empty is the default stream
type ReaderOption struct{ Name string }
```

## Clients

| Function | Description |
| --- | --- |
| `NewClient(world World) Client` | Client that calls a World in the same process. |
| `NewHTTPClient(endpoint string, ...*http.Client) Client` | Client for a World served by `worlds.HTTPHandler`; `endpoint` includes `/v1/workflow`. |
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
| `ErrRunNotFinished` | `Run.Await` inside a run on an unfinished run. |
| `ErrHookNotFound` | No active hook has the token. |
| `ErrHookNotRegistered` | Awaiting a hook not created in this run. |
| `ErrHookConflict` | Another active hook owns the token. |
| `ErrHookForceClaimed` | Another run took the token with `Force`. |

`*Error` (alias of `worlds.Error`) is the type of failed step and run errors;
see [Errors and Retries](/guide/errors).
