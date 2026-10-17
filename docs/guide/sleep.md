# Sleep and Timers

A suspended run holds no process, goroutine, or connection. Timers are stored
in the World as deliveries due at a future time, so sleeping for days or weeks
is fine.

## Sleep

`workflow.Sleep` suspends the run for a duration. Use it instead of
`time.Sleep`.

```go
if err := workflow.Sleep(ctx, 24*time.Hour); err != nil {
	return Result{}, err
}
```

`Sleep` is `Timer` followed by `Value`.

## Timers

`workflow.Timer` returns a `*Result[struct{}]` that settles after a duration. Race a hook against a timer by comparing the winner:

```go
approved, err := workflow.CreateHook[struct{}](ctx, workflow.HookOption{Token: "approve:" + input.OrderID})
if err != nil {
    return Output{}, err
}
defer approved.Dispose(ctx)
reminder := workflow.Timer(ctx, 48*time.Hour)
winner, err := workflow.Select(ctx, approved, reminder)
if err != nil {
    return Output{}, err
}
if winner == reminder {
    if _, err := workflow.Step(ctx, sendReminder, input.OrderID); err != nil {
        return Output{}, err
    }
}
```

Timers fire when replay reaches their declaration after the due time, recording `wait_completed`. Recorded event order determines when waiting code sees the result. A managed cancellation context can end a timer wait early with a fatal cancellation error.

## Timeouts and deadlines

`workflow.WithTimeout(ctx, duration)` returns a durable cancellation context, a cancel function, and a creation error. `workflow.WithDeadline(ctx, deadline)` accepts an absolute deadline instead. Both attach a `Then` continuation to a normal timer; expiry cancels the returned context with `context.DeadlineExceeded` as its cause.

`workflow.WithTimeoutCause(ctx, duration, cause)` and `workflow.WithDeadlineCause(ctx, deadline, cause)` use a custom error as the expiry cause. A nil cause uses `context.DeadlineExceeded`. Their cancel functions still use `context.Canceled` for manual cancellation.

Pass the returned context to the work that should stop:

```go
cancelCtx, cancel, err := workflow.WithTimeout(ctx, time.Hour)
if err != nil {
    return Output{}, err
}
result, stepErr := workflow.Step(cancelCtx, longRunningJob, input)
if err := cancel(); err != nil {
    return Output{}, err
}
return result, stepErr
```

Calling `cancel()` cancels the context early with `context.Canceled` as its cause and disarms the timer. Disarming is durable: a replay after the deadline still sees the recorded cancellation and skips firing. Parent cancellation also stops work using the returned context. The first cancellation cause wins.

For an absolute deadline, derive the time from a durable value such as `workflow.Now`:

```go
now, err := workflow.Now(ctx)
if err != nil {
    return Output{}, err
}
cancelCtx, cancel, err := workflow.WithDeadline(ctx, now.Add(72*time.Hour))
```

The deadline is stored with the timer and does not restart on replay. `context.Cause(cancelCtx)` reports why cancellation happened; `cancelCtx.Err()` reports `context.Canceled`. Cancelled steps and pending timer results fail with fatal cancellation errors. A workflow can handle those errors and continue. Expiry does not cancel the run or any child runs; call `Run.Cancel` explicitly when that is required.

Run cancellation prevents new work and leaves already-running steps to finish naturally. `Run.Value` returns `workflow.ErrRunCancelled`. Ordinary `context.WithTimeout`, `WithDeadline`, and native cancellation constructors are prohibited in workflows; they remain valid inside steps and external handlers.

## Reading the time

Use `workflow.Now` instead of `time.Now` in a runner. It records the time the
first time it runs, so replay returns the same value.

```go
now, err := workflow.Now(ctx)
if err != nil {
	return Result{}, err
}

deadline := now.Add(72 * time.Hour)
```

`Now` returns UTC. It returns `ErrNoWorkflow` outside workflow context,
including in step context, where `time.Now` is safe to use.

## Waking a run early

`Run.WakeUp` settles a run's pending timers early and queues a delivery so the
run resumes. It returns the number of timers it settled. This is useful for
operators who want to skip a wait. In tests, run the World under
[synctest](/guide/testing#sleeps-and-timers) instead, which skips waits without
touching the run.

```go
run, err := workflow.Get[Result](ctx, runID)
if err != nil {
	return err
}

n, err := run.WakeUp(ctx)
```

`WakeOptions.CorrelationIDs` limits `WakeUp` to specific waits. Waking a timer used by `WithTimeout` or `WithDeadline` also runs its continuation and cancels the returned context.

## Delivery limits

Each World has a limit on how far ahead its queue can schedule a delivery
natively, and handles longer timers differently:

| World | Native delay limit | Longer timers |
| --- | --- | --- |
| Local | none | in-process timer heap |
| Postgres | none | rows with `not_before` |
| Vercel | 7 days | set `Config.QStash` |
| AWS | 15 minutes | set `Config.Scheduler` (EventBridge Scheduler) |

Without the optional component, queueing a delivery beyond the limit returns an
error. This applies to sleeps, timers, timeouts, and step retry delays.
