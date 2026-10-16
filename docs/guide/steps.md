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

`workflow.InlineStep` runs a short function as a step **inside the flow**,
without a separate step delivery, and records its result. Use it for quick work
such as generating an ID or reading another run. Its result is available right
where you call it, with no extra delivery round trip.

`InlineStep` takes the same arguments as `Step`: a named step function, its
input, and optional `StepOptions`. The input is recorded, and every attempt runs
with the recorded input.

This generates an order ID once; every replay returns the recorded ID:

```go
orderID, err := workflow.InlineStep(ctx, newOrderID, struct{}{})
if err != nil {
	return Result{}, err
}

func newOrderID(ctx context.Context, _ struct{}) (string, error) {
	return ulid.Make().String(), nil
}
```

`fn` can fail, and failures are retried like a step's: three retries by
default, `StepOptions` to change that, `FatalError` to fail at once, and
`RetryableError` to delay the next attempt. A failed attempt is recorded and
suspends the run, and a flow delivery is queued for when the retry is due; that
replay starts the next attempt. Retries never run back to back in the same
delivery. When retries run out, `InlineStep` returns
the error as a [`*workflow.Error`](/guide/errors).

This reads a price that may be briefly unavailable, retrying up to five times:

```go
price, err := workflow.InlineStep(ctx, currentPrice, input.SKU, workflow.StepOptions{MaxRetries: 5})
if err != nil {
	return Result{}, err
}

func currentPrice(ctx context.Context, sku string) (int, error) {
	return pricing.Current(ctx, sku)
}
```

Inline steps suit work that is short, rarely fails, and is idempotent or free
of side effects, like reading the time or another run. If a delivery stops
before an attempt's result is recorded, the next replay simply starts another
attempt, and if two flows run it at once, the first to record a result wins. Use `Step`
for anything long, failure-prone, or with side effects that must not repeat. If
the World fails to record the step, the whole delivery is retried.

Each attempt records `step_started` and then its outcome: `step_completed`,
`step_retrying`, or `step_failed`. On the happy path an inline step writes
`step_created`, `step_started`, and `step_completed`. `workflow.Now` is an inline
step too, with one attempt. A step delivery never runs
an inline step: if it reaches one that hasn't settled, it suspends.

In step context, `InlineStep` calls `fn` without recording it.
