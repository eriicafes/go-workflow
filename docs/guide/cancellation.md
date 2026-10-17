# Cancellation

Use a durable cancellation context to stop a group of steps or timers while
the workflow continues. Cancellation is recorded for replay and delivered to
running steps across workers.

Cancelling an entire run has a different scope: it prevents new work and lets
already-running steps finish. See [Cancelling a run](#cancelling-a-run).

## Cancelling work

Create a context with `workflow.WithCancel` and pass it to the work you want
to control. For example, cancel the remaining lookup after a race settles:

```go
workCtx, cancel, err := workflow.WithCancel(ctx)
if err != nil {
	return Output{}, err
}

primary := workflow.Background(workCtx, lookupPrimary, query)
replica := workflow.Background(workCtx, lookupReplica, query)

winner, err := workflow.Select(ctx, primary, replica)

// Stop pending work. The winner keeps its recorded result.
if cancelErr := cancel(); cancelErr != nil {
	return Output{}, errors.Join(err, cancelErr)
}

if err != nil {
	return Output{}, err
}

if winner == primary {
	return Output{Answer: primary.Unwrap(ctx)}, nil
}

return Output{Answer: replica.Unwrap(ctx)}, nil
```

Handle the creation error before using the context. The cancellation function
can also fail, for example if notifying a worker fails, so handle its error.
Use separate contexts when branches need independent cancellation.

## Stopping a step

Cancellation is cooperative. Pass the step's context to services that support
it:

```go
func fetchDocument(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	return io.ReadAll(resp.Body)
}
```

For work without a context-aware API, check `ctx.Done()` or `ctx.Err()` at
suitable points. Cancellation cannot forcibly stop a Go goroutine.

A cancelled attempt fails without retrying. Its error wraps `context.Canceled`
with `workflow.FatalError`, even if the step body returns nil after
cancellation. Pending timers using the context also fail with a fatal
cancellation error. The runner can handle these errors and continue.

A step can request cancellation through `workflow.CancelFunc(ctx)`. When a
controller is available, it cancels that managed context, including other work
sharing it:

```go
cancel, ok := workflow.CancelFunc(ctx)
if ok {
	if err := cancel(); err != nil {
		return Result{}, err
	}
}
```

## Cancellation causes

Use `workflow.WithCancelCause` when callers need a reason for cancellation:

```go
workCtx, cancel, err := workflow.WithCancelCause(ctx)
if err != nil {
	return Output{}, err
}

// Pass workCtx to steps that may become obsolete.
work := workflow.Background(workCtx, performWork, input)
replacement := workflow.Background(ctx, waitForReplacement, input)

winner, err := workflow.Select(ctx, work, replacement)
if err != nil {
	return Output{}, err
}

if winner == replacement {
	if err := cancel(errors.New("superseded")); err != nil {
		return Output{}, err
	}

	return Output{Superseded: true}, nil
}

return work.Value(ctx)
```

`context.Cause(workCtx)` reports the recorded cause. `workCtx.Err()` reports
`context.Canceled`. Passing nil to the cancellation function uses
`context.Canceled` as the cause. The first committed cause wins, including
concurrent requests and retries.

## Timeouts and deadlines

Use `workflow.WithTimeout` to limit how long work may take:

```go
workCtx, cancel, err := workflow.WithTimeout(ctx, time.Hour)
if err != nil {
	return Output{}, err
}

result, stepErr := workflow.Step(workCtx, longRunningJob, input)

// Disarm the timer once the work has settled.
if err := cancel(); err != nil {
	return Output{}, errors.Join(stepErr, err)
}

return result, stepErr
```

Expiry cancels the context with `context.DeadlineExceeded` as its cause.
Calling `cancel()` early uses `context.Canceled` and disarms the timer. Both
the deadline and cancellation survive replay, so the timeout does not restart
and a disarmed timer stays disarmed. Parent cancellation also stops work using
the context.

For an absolute deadline, use `workflow.WithDeadline` with a recorded time:

```go
now, err := workflow.Now(ctx)
if err != nil {
	return Output{}, err
}

workCtx, cancel, err := workflow.WithDeadline(ctx, now.Add(72*time.Hour))
if err != nil {
	return Output{}, err
}

result, stepErr := workflow.Step(workCtx, longRunningJob, input)
if err := cancel(); err != nil {
	return Output{}, errors.Join(stepErr, err)
}

return result, stepErr
```

`WithTimeoutCause` and `WithDeadlineCause` let you supply a custom expiry
cause. A nil cause uses `context.DeadlineExceeded`. Manual cancellation still
uses `context.Canceled`.

Timeouts use durable [timers](/guide/sleep#timers). Waking their timers with
`Run.WakeUp` also triggers cancellation.

## Combining contexts

Use `workflow.CancelAny` when work should stop if either of two managed
contexts is cancelled:

```go
combinedCtx := workflow.CancelAny(ctx, shutdownCtx, requestCtx)
result, err := workflow.Step(combinedCtx, performWork, input)
```

The base context supplies values and execution metadata. Cancellation of the
base or any source cancels the combined context without cancelling its
sources. Source values are not merged. With no sources, `CancelAny` returns
the base context unchanged.

## Cancelling a run

Call `Run.Cancel` to stop a run from doing further work:

```go
run, err := workflow.Get[Result](ctx, runID)
if err != nil {
	return err
}

if err := run.Cancel(ctx); err != nil {
	return err
}
```

The run records `run_cancelled`, and `Run.Value` returns
`workflow.ErrRunCancelled`. Cancelling an already-finished run is a no-op.
Already-running steps can finish and record their outcomes. Their contexts
remain active unless you also cancel a managed context passed to them.

A run does not inherit managed cancellation from the context passed to
`workflow.Start`. Call `child.Cancel(ctx)` explicitly when a child should
stop. In workflow context, `Run.Cancel` records a durable inline step. See
[Composing Workflows](/guide/composition#cancelling-a-child).

## Contexts in workflow code

Use the workflow package's cancellation constructors in runners. The
[analyzer](/tooling/analyzer) rejects the standard `context` package's
cancellation and deadline constructors there because their state is not
durable. Standard context operations remain available inside steps and
external handlers.
