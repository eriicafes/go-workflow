# Inline Steps

`workflow.InlineStep` records a result in the current flow delivery. It never
uses the queue, unlike a regular [step](/guide/steps), which only runs inline
within [a limit](/guide/steps#running-steps-inline). That makes it useful for
quick work such as generating an ID or reading another run.

## Recording a value

Define the work in a named function:

```go
func newOrderID(ctx context.Context, _ struct{}) (string, error) {
	return ulid.Make().String(), nil
}
```

Call it from the runner:

```go
orderID, err := workflow.InlineStep(ctx, newOrderID, struct{}{})
if err != nil {
	return Result{}, err
}

return Result{OrderID: orderID}, nil
```

The first successful attempt records the ID. Every replay then returns that
ID without calling the function again. The input is recorded too, so retries
use the original input.

## Choosing suitable work

Keep inline steps short and idempotent or free of side effects. If a delivery
stops before recording the result, the queue redelivers its message, which
calls the function again. Another flow calls it only when it replays more
than 15 minutes after the attempt started.

Use `workflow.Step` for longer operations or work that needs a separate
worker delivery. Both forms can execute more than once, so protect external
side effects with an idempotency key.

## Running inline steps concurrently

`workflow.InlineStep` waits for its inline step. `workflow.InlineBackground`
starts one without waiting, like `workflow.Background`, and returns a
`*workflow.Result`:

```go
price := workflow.InlineBackground(ctx, currentPrice, input.SKU)
stock := workflow.InlineBackground(ctx, currentStock, input.SKU)
if err := workflow.JoinOk(ctx, price, stock); err != nil {
	return Result{}, err
}
```

When the runner waits, the flow delivery runs both at the same time, alongside
any steps it runs inline, and the runner continues as each one finishes.
It always runs inline, whatever `SetMaxInlineSteps` allows. Combine its result
with `Select`, `Join`, and groups like any other.

## Retrying failures

Inline steps follow the same [retry policy](/guide/errors#step-retries) as
regular steps. For example, allow five retries for a price lookup:

```go
func currentPrice(ctx context.Context, sku string) (int, error) {
	return pricing.Current(ctx, sku)
}
```

```go
price, err := workflow.InlineStep(ctx, currentPrice, input.SKU, workflow.StepOptions{
	MaxRetries: 5,
})
if err != nil {
	return Result{}, err
}
```

A failed attempt that can retry suspends the run. A later flow delivery
starts the next attempt when its retry is due. Retries never run back to back
in one delivery. After retries run out, or on a fatal error, the caller
receives a `*workflow.Error`.

## Replay behavior

An inline step records `step_created` and `step_started` in one write,
followed by the attempt's outcome. Storage failures retry the delivery. The
[event reference](/reference/events) describes the recorded events.

`workflow.Now` uses an inline step with one attempt. Most `Run` methods also
use inline steps when called in workflow context, so their reads and changes
are preserved across replay. See [Starting and Reading Runs](/guide/runs#in-workflow-context).

A step delivery cannot execute an unsettled inline step while replaying toward
its target. It suspends until a flow delivery records the outcome. Like
`Step`, `InlineStep` returns `workflow.ErrNoWorkflow` outside workflow
context, including inside a step function.
