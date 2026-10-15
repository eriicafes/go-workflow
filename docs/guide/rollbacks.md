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

## Behavior

- A compensation has the shape `func(context.Context, In) error` and receives
  the input you register it with.
- When the runner returns an error, the registered compensations run as
  **steps**, in **reverse registration order**, before the run is marked
  failed.
- Compensations retry like other steps. Pass `StepOptions` to change their
  retries:

  ```go
  workflow.OnRollback(ctx, voidInvoice, invoice.ID, workflow.StepOptions{MaxRetries: 10})
  ```

- A run that completes never runs its compensations.
- Compensations should be idempotent, like every step.

`OnRollback` returns `ErrNoWorkflow` when the context does not belong to a run.
