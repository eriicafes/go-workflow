package workflow

import (
	"context"

	"github.com/eriicafes/go-workflow/workflowops"
)

type clientContextKey struct{}
type stepContextKey struct{}
type idempotencyContextKey struct{}

func NewContext(ctx context.Context, client Client) context.Context {
	return context.WithValue(ctx, clientContextKey{}, client)
}

func clientFromContext(ctx context.Context) (Client, *workflowops.Execution, error) {
	if execution, active := workflowops.ExecutionFromContext(ctx); active {
		return execution.World(), execution, nil
	}
	if client, ok := ctx.Value(clientContextKey{}).(Client); ok {
		return client, nil, nil
	}
	return nil, nil, ErrNoClient
}

func executionFromContext(ctx context.Context) (*workflowops.Execution, bool) {
	if inStep, _ := ctx.Value(stepContextKey{}).(bool); inStep {
		return nil, false
	}
	return workflowops.ExecutionFromContext(ctx)
}

// IdempotencyKey returns the key assigned to the currently executing durable
// step. It is empty outside a step.
func IdempotencyKey(ctx context.Context) string {
	value, _ := ctx.Value(idempotencyContextKey{}).(string)
	return value
}
