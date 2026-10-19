# Concurrency and cancellation

## Waiting and racing

Pass results, hooks, webhooks, and groups directly to the combinators. Use Run
methods to inspect child workflows.

| API | Behavior |
| --- | --- |
| `Join(ctx, a, b)` | Waits for every settlement; ignores operation failures. Read each value afterward. |
| `JoinOk(ctx, a, b)` | Waits for all successes, or returns the first recorded failure. Leaves other work running. |
| `Select(ctx, a, b)` | Returns the first recorded settlement and its error. A failed winner is non-nil. |
| `SelectOk(ctx, a, b)` | Returns the first success; if all fail, returns nil and the first error in argument order. |
| `Map(ctx, fn, inputs)` | Starts steps and returns `[]*Result[T]` in input order. |
| `MapResults(results)` | Converts a result slice for combinators or group Add without waiting. |
| `All(ctx, fn, inputs)` | Returns values in input order, cancelling remaining work on failure. |

Select returns the original input handle, so compare it with `==` or use a
switch. Event order determines settlement order; argument order breaks ties.
An empty selection in valid workflow context returns `(nil, nil)`.
Neither Select variant cancels losers. Convert a result slice with
`MapResults`, for example `JoinOk(ctx, MapResults(results)...)`.

This function runs inside a workflow. `fetchPrimary` and `fetchReplica` are
named step functions with the signature
`func(context.Context, string) (string, error)`:

```go
func race(ctx context.Context, key string) (string, error) {
	workCtx, cancel, err := workflow.WithCancel(ctx)
	if err != nil {
		return "", err
	}
	a := workflow.Background(workCtx, fetchPrimary, key)
	b := workflow.Background(workCtx, fetchReplica, key)
	winner, err := workflow.SelectOk(ctx, a, b)
	if err := errors.Join(err, cancel()); err != nil {
		return "", err
	}
	switch winner {
	case a:
		return a.Value(ctx)
	default:
		return b.Value(ctx)
	}
}
```

Import `context`, `errors`, and `github.com/eriicafes/go-workflow`. Successful
step outcomes are already recorded when chosen; cancelling `workCtx` then
stops unfinished work without replacing those outcomes.

## Chaining

`Then(ctx, source, fn)` accepts `*Result[T]`. Its callback receives `(T, error)`
and returns only `*Result[U]`. The chain stays pending while the source or the
returned result is pending. Return `NewResult(value, err)` for an immediate
outcome, or return a background step, timer, or another chain.

```go
source := workflow.Background(ctx, fetchPrimary, key)
chain := workflow.Then(ctx, source, func(value string, err error) *workflow.Result[int] {
	if err != nil {
		return workflow.NewResult(0, err)
	}
	return workflow.Background(ctx, storeValue, value)
})
return chain.Value(ctx)
```

Here `storeValue` is a named `func(context.Context, string) (int, error)`.
The callback executes in workflow context. Return pending work rather than
calling `Step` or `Value` inside it when the chain must participate in a race.
Await or join the chain if the workflow must wait for it before returning.

## Groups

Declare `var group workflow.WaitGroup` to collect settlements while ignoring
operation failures. A zero-value `workflow.ErrGroup` waits for all members and
returns the first recorded failure, without cancelling other work.

`workflow.Go(ctx, &group, step, input, options...)` starts a step, adds its
result, and returns that result. Use
`group.Add(existingResult, timer, hook, otherGroup)` to include work created
elsewhere, or `group.Add(workflow.MapResults(results)...)` for a slice.

For cancellation on failure:

```go
group, groupCtx, err := workflow.ErrGroupWithContext(ctx)
if err != nil {
	return Output{}, err
}
profile := workflow.Go(groupCtx, group, fetchProfile, userID)
orders := workflow.Go(groupCtx, group, fetchOrders, userID)
if err := group.Wait(ctx); err != nil {
	return Output{}, err
}
return Output{Profile: profile.Unwrap(ctx), Orders: orders.Unwrap(ctx)}, nil
```

Groups begin observing members on their first Wait, Select, or Join. The first
observed failure cancels groupCtx. Wait still waits for every member and
returns the original failure; it cancels the context on successful return too.
Call Wait after adding the work. Pass groupCtx to work that should stop on
group failure, including work included through Add.

Groups also participate directly in Select and Join. They settle after all
members, even on failure. Add ignores new members after settlement. Go still
starts a step, but the completed group does not track it. Use a new group to
wait for later work.

## Managed contexts

- `WithCancel(ctx)` returns `(ctx, func() error, error)`.
- `WithCancelCause(ctx)` returns `(ctx, func(error) error, error)`.
- `WithTimeout(ctx, duration)` and `WithDeadline(ctx, deadline)` return
  `(ctx, func() error, error)`. The `Cause` variants add an expiry cause.
- These constructors require workflow context. Handle both constructor and
  cancellation errors; the returned cancel functions are durable operations.

Pass the returned context to steps and timers that should stop together. Pass
the step context into external I/O and observe `ctx.Done()` in step code.
Cancellation fails a step with a fatal error caused by `context.Canceled`,
without retries. `context.Cause(ctx)` carries a supplied reason. Deadline
expiry uses `context.DeadlineExceeded` unless customized; manual cancellation
uses `context.Canceled` and disarms the timer.

`CancelAny(ctx, sources...)` preserves the explicit base context's values and
cancels when it or any source cancels. It does not cancel its sources. Outside
workflow context it returns the base context unchanged. In a managed step,
`CancelFunc(ctx)` can return a controller for cancelling the shared context.

## Child runs

`Start` creates an independent run; passing a managed context does not arrange
child cancellation. Call `child.Cancel(ctx)` explicitly when that is desired.
Cancelling a run marks it cancelled and prevents new work; already admitted
running steps finish naturally. It does not automatically run compensations.

`Run.Value(ctx)` blocks in external or step context. In workflow context it
durably reads the current outcome and returns `ErrRunNotFinished` if pending;
it does not suspend to await a child. For durable parent/child coordination,
register a parent hook before passing its token to the child, have a child
step resume it, and wait with `hook.Next(ctx)` in the parent.
