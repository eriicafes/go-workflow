# Rollbacks

A rollback, or compensation, undoes work a run already did when the run later
fails. Defer `workflow.Rollback` right after the work it reverses.

```go
func PlaceOrder(ctx context.Context, input workflows.PlaceOrder) (workflows.PlaceOrderResult, error) {
	invoice, err := workflow.Step(ctx, createInvoice, input)
	if err != nil {
		return workflows.PlaceOrderResult{}, err
	}
	defer workflow.Rollback(ctx, voidInvoice, invoice.ID)

	shipment, err := workflow.Step(ctx, reserveShipment, input)
	if err != nil {
		// voidInvoice runs before the run is marked failed.
		return workflows.PlaceOrderResult{}, err
	}
	defer workflow.Rollback(ctx, releaseShipment, shipment.ID)

	if _, err := workflow.Step(ctx, chargeCard, input); err != nil {
		// releaseShipment runs, then voidInvoice.
		return workflows.PlaceOrderResult{}, err
	}

	return workflows.PlaceOrderResult{InvoiceID: invoice.ID}, nil
}

func voidInvoice(ctx context.Context, invoiceID string) error {
	return billing.Void(ctx, invoiceID)
}
```

## When rollbacks run

A compensation receives the input it was registered with and has the shape
`func(context.Context, In) error`. When the runner returns an error or panics,
compensations execute as durable steps before the run is marked failed. A
successful run never executes them. An exhausted compensation failure is
recorded without preventing the remaining compensations or replacing the
original workflow error.

`Rollback` returns nothing. It registers a compensation when called and does
not execute the callback immediately. Go invokes deferred registrations in
reverse order; the runtime executes them in the order received. In the example,
`releaseShipment` runs before `voidInvoice`. Direct calls also register work,
but do so immediately, so their registration order differs from deferred calls.

Compensations should be idempotent because they retry like other steps.
Override their retry limit when needed:

```go
defer workflow.Rollback(ctx, voidInvoice, invoice.ID, workflow.StepOptions{
	MaxRetries: 10,
})
```

`Rollback` requires workflow context and panics with `ErrNoWorkflow` elsewhere.

## Deferred cleanup

Use `defer hook.Dispose(ctx)` after creating a hook and defer `Rollback` after
successful work that needs compensation. `Dispose` acts immediately when
called; a direct call can release a hook before the workflow function exits.

Go runs registered defers in reverse order whenever their containing function
returns or panics. The SDK applies these rules:

| Exit | `Dispose` | Rollback callbacks |
| --- | --- | --- |
| Successful workflow return | Disposes the hook. | Do not run. |
| Error return or application panic | Disposes the hook. | Run as durable steps. |
| Internal replay exit | Skips disposal. | Do not execute during this exit. |

An internal replay exit includes waiting for a result, completing a targeted
step delivery, retrying a failed delivery, or recovering replay divergence.
`Execution.IsSuspended()` identifies these exits. Deferred `Rollback` calls
still register their callbacks during unwinding so later compensation
deliveries can reconstruct them. Registration itself performs no durable work.

Application panics carrying errors preserve those errors; other panic values
become errors with the message `workflow: panic: <value>`. A panic inside a step
is recorded as a step failure and follows that step's retry policy. The workflow
can handle the final step error without running compensations.

Arbitrary deferred functions still execute on every return or panic, including
internal replay exits. They follow the same restrictions as the workflow body:
put external side effects in steps. SDK cleanup skips work on internal exits;
custom cleanup intended only for final exit must check the execution state too.

Put defers in the workflow body when they should run as that body exits. A defer
inside a helper runs when the helper returns. Go evaluates deferred arguments
when the defer statement is reached. Process termination does not guarantee
that deferred functions run.
