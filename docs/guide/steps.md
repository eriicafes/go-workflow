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

A step function takes a context and one input, and returns an output and an
error. Its input and output must be JSON-encodable. Use `struct{}` when either is empty.

The first call suspends the runner. The flow delivery then runs the step
itself, or queues a step delivery for it, and replay continues with its
recorded output once it finishes. To start work without waiting, use
`workflow.Background`. See [Concurrency](/guide/concurrency).

## Running steps inline

When the runner waits for a result and nothing else is recorded, the flow
delivery runs the new steps it declared itself, up to three at once by
default, and queues a step delivery for each of the rest. The waiting runner
does not stop: as each inline step finishes, its result is recorded and the
runner continues from where it waited, while the others keep running. A chain
of steps that each need the previous result finishes in one delivery, with no
queue round trip, and a `Select`, `All`, or group reacts to the first result
as it would to a step delivery's.

`workflow.SetMaxInlineSteps` sets how many steps run inline at once, from 1 to
16, and zero turns it off:

```go
// For every delivery the worker executes.
ctx = workflow.SetMaxInlineSteps(ctx, 0)

// In a runner, for the steps declared with the returned ctx.
ctx = workflow.SetMaxInlineSteps(ctx, 8)
```

A step that runs inline is recorded with one write that also starts it, so it
takes two writes instead of three, like the Workflow SDK's
[lazy inline steps](https://workflow-sdk.dev/docs/changelog/lazy-event-creation).
A retry is queued as a step delivery after its delay.

After two minutes, the delivery queues new steps instead of running them, so a
long-running flow hands its work to other deliveries.
`workflow.SetMaxInlineStepsDuration` sets that duration like
`SetMaxInlineSteps` sets the limit, and zero turns inline steps off. When the
ctx a delivery runs with has a deadline, such as a Lambda invocation's, the
delivery also queues new steps once it is halfway to the deadline, so the
steps it started can finish before it:

```go
ctx = workflow.SetMaxInlineStepsDuration(ctx, 30*time.Second)
```

The flow that starts a step inline records its queued message as the step's
owner. If the flow dies before the step finishes, the queue redelivers that
message, which runs the step again at once. Any other flow that finds the step
running leaves it to its owner, and only one that replays more than 15 minutes
after the step started hands it to a step delivery.

The delivery ends once every step it runs inline has finished, so a slow step
keeps it open. On workers whose queue limits how long a delivery may run, run
deliveries with a ctx deadline before that limit, or turn inline steps off.

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

Use `workflow.InlineStep` for short work that should always finish in the
current flow delivery:

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

## Tracing

Each step attempt runs in an OpenTelemetry span named after the step's
function, such as `step flows.sendEmail`, with the run ID, step ID, and
attempt as attributes. The span uses the global tracer provider, so it is
recorded once you set one with `otel.SetTracerProvider`, and a failed attempt
records its error.

A step runs with the context the runner passed to it. So a span context the runner puts in that context becomes the parent of
the step's span, and the step's own instrumented I/O nests under it. Rebuild
the span context the same way on every replay, for example from a trace
context recorded in an [inline step](#inline-steps).
