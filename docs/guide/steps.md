# Steps

A step is a function that does real work: calling APIs, querying databases,
sending email. Its input and result are recorded in the run's event log, so
once a step completes it never runs again for that run.

```go
func findSources(ctx context.Context, topic string) ([]string, error) {
	return search(ctx, topic)
}
```

A step function has the shape `func(context.Context, In) (Out, error)`. `In`
and `Out` are encoded as JSON. Use `struct{}` for a step with no meaningful
input or output.

## Calling a step

`workflow.Step` calls a step from a runner and returns its result, suspending
the run until the step has settled.

```go
sources, err := workflow.Step(ctx, findSources, input.Topic)
if err != nil {
	return Result{}, err
}
```

The first time the runner reaches this call, the step is recorded and queued
as its own delivery. Once the step completes, the next replay returns its
recorded output immediately.

`Step` is `Background` followed by `Await`. To start a step without waiting for
it, see [Concurrency](/guide/concurrency).

### Rules

- **Pass everything the step needs as its input.** Steps should be named
  functions. A step that captures runner variables in a closure hides its real
  input from the log; the [analyzer](/tooling/analyzer) reports it.
- **Call steps only from workflow context.** Inside a step function, `Step`,
  `Background`, and other workflow-context functions return
  `workflow.ErrNoWorkflow`. A step does its work directly.
- **Make step functions idempotent.** A step runs at least once: it retries
  on error, and a step whose process dies before its result is recorded runs
  again.

## Retries

A step that returns an error is retried. By default it gets **3 retries** after
the first attempt. Set `StepOptions` to change that:

```go
// Up to 10 retries.
out, err := workflow.Step(ctx, callFlakyAPI, req, workflow.StepOptions{MaxRetries: 10})

// No retries.
out, err := workflow.Step(ctx, chargeCard, order, workflow.StepOptions{MaxRetries: 0})
```

Omitting `StepOptions` allows 3 retries, while the zero value of `StepOptions`
allows none. When several options are passed, the last one applies.

A step function controls its own retries with two error wrappers:

```go
func chargeCard(ctx context.Context, order Order) (Receipt, error) {
	receipt, err := payments.Charge(ctx, order, workflow.StepID(ctx))
	var declined *payments.DeclinedError
	switch {
	case errors.As(err, &declined):
		// Never retried.
		return Receipt{}, workflow.FatalError(err)
	case errors.Is(err, payments.ErrRateLimited):
		// Retried no sooner than 30 seconds from now.
		return Receipt{}, workflow.RetryableError(err, 30*time.Second)
	}
	return receipt, err
}
```

See [Errors and Retries](/guide/errors) for how failed steps are reported.

## Step metadata

Inside a step, `workflow.GetStepMetadata` describes the current attempt:

```go
meta := workflow.GetStepMetadata(ctx)
meta.ID        // step ID, stable across attempts
meta.Name      // step function name
meta.StartedAt // when this attempt started
meta.Attempt   // attempt number, starting at 1
```

`workflow.StepID(ctx)` is shorthand for `GetStepMetadata(ctx).ID`. It is a
good **idempotency key** for the services a step calls, because it is the same
on every attempt of the step.

Outside step context both return zero values.

## Step context

Inside a step function, the context belongs to the step:

- `time.Now`, I/O, and randomness are safe.
- [Streams](/guide/streams) can be written and read.
- [`workflow.Start`](/guide/runs), [`workflow.Get`](/guide/runs), and
  [`workflow.ResumeHook`](/guide/hooks) work, using the World that records the
  run, or a Client put in the context with `workflow.NewContext`.
- When the step or its run is cancelled, the context is cancelled with
  `worlds.ErrStepCancelled` or `worlds.ErrRunCancelled` as its cause.

## Inline steps

`workflow.InlineStep` records the result of a function that cannot fail, such as
a generated ID or a random choice, without a separate step delivery.

```go
orderID, err := workflow.InlineStep(ctx, newOrderID)
if err != nil {
	return Result{}, err
}

func newOrderID(ctx context.Context) string {
	return ulid.Make().String()
}
```

`fn` runs once in the flow and its result is recorded as a step, so replay
returns the recorded value. Unlike `Step`, `fn` cannot return an error. If the
result cannot be recorded, the delivery is retried and `fn` runs again. Use
`Step` for anything that can fail.

In step context, `InlineStep` calls `fn` without recording it.
