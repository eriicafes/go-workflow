# Rollbacks

A rollback, or compensation, undoes work a run already did when the run later
fails. Register it right after the work it reverses with `workflow.OnRollback`.

```go
func PlaceOrder(ctx context.Context, input workflows.PlaceOrder) (workflows.PlaceOrderResult, error) {
	invoice, err := workflow.Step(ctx, createInvoice, input)
	if err != nil {
		return workflows.PlaceOrderResult{}, err
	}

	if err := workflow.OnRollback(ctx, voidInvoice, invoice.ID); err != nil {
		return workflows.PlaceOrderResult{}, err
	}

	shipment, err := workflow.Step(ctx, reserveShipment, input)
	if err != nil {
		// voidInvoice runs before the run is marked failed.
		return workflows.PlaceOrderResult{}, err
	}

	if err := workflow.OnRollback(ctx, releaseShipment, shipment.ID); err != nil {
		return workflows.PlaceOrderResult{}, err
	}

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
`func(context.Context, In) error`. When the runner returns an error,
compensations execute as steps in reverse registration order before the run
is marked failed. A successful run never executes them.

Compensations should be idempotent because they retry like other steps.
Override their retry limit when needed:

```go
if err := workflow.OnRollback(ctx, voidInvoice, invoice.ID, workflow.StepOptions{
	MaxRetries: 10,
}); err != nil {
	return workflows.PlaceOrderResult{}, err
}
```

`OnRollback` requires workflow context and returns `ErrNoWorkflow` elsewhere.
