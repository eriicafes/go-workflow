# Errors and Retries

## Errors that survive replay

A failed step's error is recorded in the event log and read back on every
replay. Go errors are values with types, so go-workflow records more than the
message: it keeps the **type, message, and JSON encoding of every error in the
original chain**.

`workflow.Step` returns a failed step's error as a `*workflow.Error`, and
`Run.Await` returns a failed run's error the same way. Because the
chain is preserved, `errors.Is` and `errors.As` keep working after replay and
across child runs.

```go
var ErrCardDeclined = errors.New("card declined")

type LimitError struct{ Limit int }

func (e *LimitError) Error() string { return fmt.Sprintf("over limit %d", e.Limit) }
```

```go
_, err := workflow.Step(ctx, charge, order)
var limit *LimitError
switch {
case errors.Is(err, ErrCardDeclined):
	// ...
case errors.As(err, &limit):
	// limit.Limit is decoded from the recorded error.
}
```

How matching works:

- `errors.Is` matches a target with the **same type and message** as an error
  in the recorded chain. That is how sentinel errors made with `errors.New`
  match.
- `errors.As` matches a target whose **type** is the type of an error in the
  chain and decodes that error's **exported fields** from JSON into it. Give
  error types exported fields for anything callers need.

`worlds.NewError(err)` builds an `*Error` from any error, and
`worlds.DecodeError(data)` decodes the JSON stored in a run's or step's
`Error` field.

## Step retries

A step that returns an error is retried up to `StepOptions.MaxRetries` times,
3 by default. While a retry is scheduled the step's status is `pending` and its
`RetryAfter` field holds the earliest time of the next attempt. When retries run
out, the step fails with its last error.

| Return from a step | Effect |
| --- | --- |
| `nil` error | Step completes. |
| Any other error | Retried while retries remain, then fails. |
| `workflow.FatalError(err)` | Fails immediately, never retried. |
| `workflow.RetryableError(err, after)` | Retried no sooner than `after`, while retries remain. |

Both wrappers return `nil` when `err` is `nil`, so you can wrap
unconditionally.

```go
func sendInvoice(ctx context.Context, invoice Invoice) (struct{}, error) {
	err := mailer.Send(ctx, invoice)
	if errors.Is(err, mailer.ErrInvalidAddress) {
		return struct{}{}, workflow.FatalError(err)
	}

	return struct{}{}, err
}
```

## Failing a run

A runner fails its run by returning an error. Before the run is marked failed,
registered [rollbacks](/guide/rollbacks) run in reverse order. The run's error
is recorded as a `*workflow.Error`, so callers can inspect it with `errors.Is`
and `errors.As`:

```go
_, err := run.Await(ctx)
if errors.Is(err, ErrCardDeclined) {
	// ...
}
```

A runner may also handle a step's error and keep going, for example by
retrying with different input or taking another branch.

## Sentinel errors

| Error | Returned when |
| --- | --- |
| `workflow.ErrNoClient` | `Start`, `Get`, `GetHook`, or `ResumeHook` is called with no Client in the context and outside a run. |
| `workflow.ErrNoWorkflow` | A workflow-context function such as `Step`, `Join`, or `CreateHook` is called outside workflow context (including in step context), or a stream function is called outside a run. |
| `workflow.ErrNotInStep` | A function that does I/O, such as `Writer` or `Run.Reader`, is called in workflow context. |
| `workflow.ErrRunCancelled` | Awaiting a run that was cancelled. It is also the cause of the context of a cancelled run's runner and steps. |
| `workflow.ErrRunNotFinished` | `Run.Await` inside a run, when the awaited run had not finished. |
| `workflow.ErrHookNotFound` | Resuming a token that no active hook has. |
| `workflow.ErrHookNotRegistered` | Awaiting a hook that was not created in this run, such as one from `GetHook`. |
| `workflow.ErrHookConflict` | Creating a hook whose token another active hook already has. |
| `workflow.ErrHookForceClaimed` | Awaiting a hook whose token another run took over with `HookOption.Force`. |
| `worlds.ErrCorruptedEventLog` | `Run.Await` on a run whose replay kept diverging from its event log; see [Replay divergence](/guide/replay#replay-divergence). |

The [`worlds` reference](/reference/worlds#errors) lists the errors Worlds
return, such as `ErrRunNotFound` and `ErrStepCancelled`.
