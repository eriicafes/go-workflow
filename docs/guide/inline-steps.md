# Inline Steps

`workflow.InlineStep` records a result in the current flow delivery. It avoids
the queue round trip of a regular [step](/guide/steps), making it useful for
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
stops before recording the result, the next replay calls the function again.
Concurrent flow deliveries can also execute the function, with the first
recorded result winning.

Use `workflow.Step` for longer operations or work that needs a separate
worker delivery. Both forms can execute more than once, so protect external
side effects with an idempotency key.

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

A failed attempt suspends the run. A later flow delivery starts the next
attempt when its retry is due. Retries never run back to back in one delivery.
After retries run out, the caller receives a `*workflow.Error`.

## Replay behavior

An inline step records `step_created`, followed by `step_started` and the
attempt's outcome. Storage failures retry the delivery. The
[event reference](/reference/events) describes the recorded events.

`workflow.Now` uses an inline step with one attempt. Run methods also use
inline steps when called in workflow context, so their reads and changes are
preserved across replay. See [Starting and Reading Runs](/guide/runs#in-workflow-context).

A step delivery cannot execute an unsettled inline step while replaying toward
its target. It suspends until a flow delivery records the outcome. Inside a
step function, `InlineStep` calls the function directly without recording a
nested step.
