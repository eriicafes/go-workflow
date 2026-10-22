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

Use `workflow.Timer` when the runner needs to do other work during a wait.
For example, race an approval hook against a reminder timer:

```go
approved := workflow.CreateHook[struct{}](ctx, workflow.HookOption{Token: "approve:" + input.OrderID})
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

A timer returns a result you can wait on or use in
[selection](/guide/concurrency#racing-operations). It settles when replay
reaches its declaration after the due time. Waiting code sees the result in
recorded event order.

A [cancellation context](/guide/cancellation) can end the wait early.

## Timeouts and deadlines

To stop work after a duration or at an absolute deadline, pass it a context
created with `workflow.WithTimeout` or `workflow.WithDeadline`. See
[Cancellation](/guide/cancellation#timeouts-and-deadlines) for examples and
expiry behavior.

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

Set `WakeOptions.CorrelationIDs` to wake only specific waits. Waking a timer
used by a [timeout or deadline](/guide/cancellation#timeouts-and-deadlines)
also triggers its cancellation.

## Delivery limits

Each World has a limit on how far ahead its queue can schedule a delivery
natively, and handles longer timers differently:

| World | Native delay limit | Longer timers |
| --- | --- | --- |
| Local | none | in-process timer heap |
| Postgres | none | rows with `not_before` |
| Vercel | 6 days | set `Config.QStash` |
| AWS | 15 minutes | set `Config.Scheduler` (EventBridge Scheduler) |

Without the optional component, queueing a delivery beyond the limit returns an
error. This applies to sleeps, timers, timeouts, and step retry delays.
