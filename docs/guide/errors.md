# Errors and Retries

## Errors that survive replay

A failed step's error is recorded in the event log and read back on every
replay. Go errors are values with types, so go-workflow records more than the
message: it keeps the **type, message, and JSON encoding of every error in the
original chain**.

`workflow.Step` returns a failed step's error as a `*workflow.Error`, and
`Run.Value` returns a failed run's error the same way. Because the
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

`worlds.FatalError` and `worlds.RetryableError` are plain Go error wrappers.
Durable steps inspect them before encoding the result and record the retry
time separately. `worlds.Error` records them in its chain like other errors, so `errors.As`
still matches them after replay and rebuilds their `Cause` from the rest of
the recorded chain.

## Step retries

A failed step gets three retries after its first attempt by default. Set
`StepOptions.MaxRetries` to change the limit:

```go
receipt, err := workflow.Step(ctx, chargeCard, order, workflow.StepOptions{
	MaxRetries: 10,
})
if err != nil {
	return Result{}, err
}
```

Omitting options allows three retries. Passing `StepOptions{}` allows none.
If multiple options are passed, the last one applies.

While a retry is scheduled, the step's status is `pending` and `RetryAfter`
holds the earliest time of the next attempt. When retries run out, the step
fails with its last error. [Inline steps](/guide/inline-steps#retrying-failures)
use the same policy.

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

A runner fails its run by returning an error or panicking. Error panics preserve
the original error; other panic values become `workflow: panic: <value>` errors.
Before the run is marked failed, deferred [rollbacks](/guide/rollbacks) run in
Go's defer order, with the last defer first. The run's error
is recorded as a `*workflow.Error`, so callers can inspect it with `errors.Is`
and `errors.As`:

```go
_, err := run.Value(ctx)
if errors.Is(err, ErrCardDeclined) {
	// ...
}
```

A runner may also handle a step's error and keep going, for example by
retrying with different input or taking another branch.

## Sentinel errors

| Error | Returned when |
| --- | --- |
| `workflow.ErrNoClient` | A function that needs a Client, such as `Start`, is called with no Client in the context and outside a run. |
| `workflow.ErrNoWorkflow` | A workflow-context function such as `Step` is called outside workflow context (including in step context), a hook created outside workflow context is used, or a stream function is called outside a run. |
| `workflow.ErrNotInStep` | A function that does I/O, such as `Writer` or `Run.Reader`, is called in workflow context. |
| `workflow.ErrRunCancelled` | Reading a cancelled run. Running steps keep their contexts unless they use an explicitly cancelled context. |
| `workflow.ErrResumeNotQueued` | `ResumeHook` recorded the payload but could not queue its run. Don't resend the payload. |
| `workflow.ErrNilResult` | A nil `*Result` is returned from a `Then` callback or awaited. |
| `workflow.ErrRunNotFinished` | `Run.Value` in workflow context, when the awaited run had not finished. |
| `workflow.ErrHookNotFound` | Resuming a token that no active hook has. |
| `workflow.ErrHookNotRegistered` | Receiving or selecting a hook from another execution or a lookup. |
| `workflow.ErrHookConflict` | Receiving or selecting a hook whose token another active hook already has. The token is claimed on first use, not at creation. |
| `workflow.ErrHookForceClaimed` | Receiving from a hook whose token another run took over with `HookOption.Force`. |
| `worlds.ErrCorruptedEventLog` | `Run.Value` on a run whose replay kept diverging from its event log. See [Replay divergence](/guide/replay#replay-divergence). |

The [`worlds` reference](/reference/worlds#errors) lists the errors Worlds
return, such as `ErrRunNotFound` and `ErrHookNotActive`.
