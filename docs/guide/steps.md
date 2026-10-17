# Steps

A step does work such as calling an API or querying a database. Its input and
result are recorded in the run's event log. Once it completes, replay returns
the recorded result.

## Calling a step

Write a named function and call it from a runner with `workflow.Step`:

```go
func findSources(ctx context.Context, topic string) ([]string, error) {
	return search(ctx, topic)
}
```

```go
sources, err := workflow.Step(ctx, findSources, input.Topic)
if err != nil {
	return Result{}, err
}

return Result{Sources: sources}, nil
```

A step function has the shape `func(context.Context, In) (Out, error)`. Its
input and output must be JSON-encodable. Use `struct{}` when either is empty.

The first call queues a separate step delivery and suspends the runner. Once
the step finishes, replay continues with its recorded output. To start work
without waiting, use `workflow.Background`. See [Concurrency](/guide/concurrency).

## Writing reliable steps

Pass the state a step needs through its input. Capturing runner variables in a
closure hides that state from the event log. The [analyzer](/tooling/analyzer)
reports these captures.

Keep workflow operations in the runner. Calling `Step` from inside a step
returns `workflow.ErrNoWorkflow`. A step should call services and other Go
functions directly.

Make steps idempotent. A failed attempt can retry, and a process can stop
before its result is recorded. Use `workflow.StepID(ctx)` as an idempotency key
when the service supports one.

## Retries

A failed step gets three retries by default. Override that limit when needed:

```go
receipt, err := workflow.Step(ctx, chargeCard, order, workflow.StepOptions{
	MaxRetries: 0, // Do not retry a failed charge.
})
if err != nil {
	return Result{}, err
}
```

[Errors and Retries](/guide/errors) covers retry delays, fatal errors, and
handling errors after replay.

## Inline steps

Use `workflow.InlineStep` for short work that should finish in the current
flow delivery:

```go
orderID, err := workflow.InlineStep(ctx, newOrderID, struct{}{})
if err != nil {
	return Result{}, err
}
```

The result is recorded without a separate step delivery. See
[Inline Steps](/guide/inline-steps) for choosing suitable work and how retries
behave.

## Step metadata

Read the current attempt's metadata inside a step:

```go
meta := workflow.GetStepMetadata(ctx)
log.Printf("step %s (%s), attempt %d, started %s",
	meta.ID, meta.Name, meta.Attempt, meta.StartedAt)
```

`workflow.StepID(ctx)` is shorthand for `GetStepMetadata(ctx).ID`. The ID stays
the same across attempts. Outside step context, these functions return zero
values.

## Step context

A step can use ordinary Go I/O, `time.Now`, and randomness. It can also access
[streams](/guide/streams) and use [Client operations](/guide/runs), such as
starting a run or [resuming a hook](/guide/hooks). These operations use the
World recording the run unless `workflow.NewContext` supplies another Client.

Pass the step's context to I/O so work can stop cooperatively. See
[Cancellation](/guide/cancellation) for cancelling steps and how that differs
from cancelling a run.
