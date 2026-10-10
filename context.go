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

func clientFromContext(ctx context.Context) (Client, error) {
	client, ok := ctx.Value(clientContextKey{}).(Client)
	if !ok || client == nil {
		return nil, ErrNoClient
	}
	return client, nil
}

func executionFromContext(ctx context.Context) (*workflowops.Execution, bool) {
	if inStep, _ := ctx.Value(stepContextKey{}).(bool); inStep {
		return nil, false
	}
	return workflowops.ExecutionFromContext(ctx)
}

func parentExecutionFromContext(ctx context.Context) (*workflowops.Execution, bool) {
	return workflowops.ExecutionFromContext(ctx)
}

// IdempotencyKey returns the key assigned to the currently executing durable
// step. It is empty outside a step.
func IdempotencyKey(ctx context.Context) string {
	value, _ := ctx.Value(idempotencyContextKey{}).(string)
	return value
}
