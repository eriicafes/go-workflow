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

`Sleep` is `Timer` followed by `Await`.

## Timers

`workflow.Timer` returns a `Promise[struct{}]` that settles after a duration.
Combine it with other promises to wait for whichever comes first.

```go
approved, err := workflow.CreateHook[struct{}](ctx, workflow.HookOption{Token: "approve:" + input.OrderID})
if err != nil {
	return Result{}, err
}

reminder := workflow.Timer(ctx, 48*time.Hour)

// Suspend until the order is approved or 48 hours pass.
if _, err := workflow.Select(ctx, workflow.Promise[struct{}](approved), reminder); err != nil {
	return Result{}, err
}

if done, _ := approved.Done(ctx); !done {
	if _, err := workflow.Step(ctx, sendReminder, input.OrderID); err != nil {
		return Result{}, err
	}
}
```

Cancelling a timer settles it immediately.

## Timeouts

`workflow.Timeout` returns a timer that **cancels the run** when it fires.
Cancel the returned promise to disarm it.

```go
timeout := workflow.Timeout(ctx, time.Hour)

result, err := workflow.Step(ctx, longRunningJob, input)
if err != nil {
	return Result{}, err
}

if err := timeout.Cancel(ctx); err != nil {
	return Result{}, err
}
```

When the run is cancelled, its pending work is cancelled too: the contexts of
running steps are cancelled with `worlds.ErrRunCancelled` as the cause, and
`Run.Await` returns `workflow.ErrRunCancelled`.

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

`WakeOptions.CorrelationIDs` limits `WakeUp` to specific waits. A `Timeout`
settled by `WakeUp` does **not** cancel the run.

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
