package workflow

import (
	"context"
	"net/http"

	"github.com/eriicafes/go-workflow/workflowops"
)

type clientContextKey struct{}
type stepContextKey struct{}
type idempotencyContextKey struct{}

// NewContext attaches client to ctx so application code can start, retrieve,
// and cancel durable runs.
func NewContext(ctx context.Context, client Client) context.Context {
	return context.WithValue(ctx, clientContextKey{}, client)
}

// Middleware attaches client to each request context so handlers can use
// Start, Get, and Cancel with the request's lifetime.
func Middleware(handler http.Handler, client Client) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r = r.WithContext(NewContext(r.Context(), client))
		handler.ServeHTTP(w, r)
	})
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
