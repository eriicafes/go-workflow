# Inline Steps

`workflow.InlineStep` records a result in the current flow delivery. It always
runs inline, unlike a regular [step](/guide/steps), which only runs inline
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
stops before recording the result, the next flow of the run calls the
function again.

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

A failed attempt that can retry is handed to a step delivery, which runs the
next attempt when its retry is due, and the runner waits for it like any
step's result. Retries never run back to back in one delivery. After retries
run out, or on a fatal error, the caller receives a `*workflow.Error`, which
after retries run out [keeps the last attempt's error as its cause](/guide/errors#step-retries).

## Replay behavior

An inline step is created by the `step_started` of its first attempt, which
carries its name and input, followed by the attempt's outcome. Storage
failures retry the delivery. The [event reference](/reference/events)
describes the recorded events.

Most `Run` methods use inline steps when called in workflow context, so their reads and changes
are preserved across replay. See [Starting and Reading Runs](/guide/runs#in-workflow-context).

A step delivery runs an inline step only once its retry was handed to one.
Like
`Step`, `InlineStep` returns `workflow.ErrNoWorkflow` outside workflow
context, including inside a step function.
