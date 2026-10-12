package workflow

import (
	"context"
	"net/http"
	"time"

	"github.com/eriicafes/go-workflow/workflowops"
)

type clientContextKey struct{}
type stepContextKey struct{}
type stepMetadataContextKey struct{}
type stepStreamsContextKey struct{}

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

// WorkflowMetadata describes the active workflow run. Its zero value is
// returned outside workflow or step execution.
type WorkflowMetadata struct {
	// Name is the stable identity of the workflow function.
	Name string
	// RunID uniquely identifies this workflow execution.
	RunID string
	// StartedAt is when the run was initialized.
	StartedAt time.Time
}

// GetWorkflowMetadata returns metadata for the active workflow run. It is
// available in both workflow and durable step functions.
func GetWorkflowMetadata(ctx context.Context) WorkflowMetadata {
	execution, active := workflowops.ExecutionFromContext(ctx)
	if !active {
		return WorkflowMetadata{}
	}
	lease := execution.Lease()
	return WorkflowMetadata{
		Name:      execution.Name(),
		RunID:     lease.ID,
		StartedAt: lease.StartedAt,
	}
}

// StepMetadata describes the current durable step attempt. Its zero value is
// returned outside a step.
type StepMetadata struct {
	// ID is the step's durable idempotency key.
	ID string
	// Name is the stable identity of the executing step function.
	Name string
	// StartedAt is when this attempt began executing.
	StartedAt time.Time
	// Attempt starts at one and increases for every retry.
	Attempt int
}

// GetStepMetadata returns metadata for the currently executing step attempt.
// It returns the zero value outside a step.
func GetStepMetadata(ctx context.Context) StepMetadata {
	value, _ := ctx.Value(stepMetadataContextKey{}).(StepMetadata)
	return value
}

// StepID returns the key assigned to the currently executing durable step. It
// is empty outside a step.
func StepID(ctx context.Context) string {
	return GetStepMetadata(ctx).ID
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

func stepExecutionFromContext(ctx context.Context) (*workflowops.Execution, bool) {
	if inStep, _ := ctx.Value(stepContextKey{}).(bool); !inStep {
		return nil, false
	}
	return workflowops.ExecutionFromContext(ctx)
}

func workflowExecutionFromContext(ctx context.Context) (*workflowops.Execution, bool) {
	if inStep, _ := ctx.Value(stepContextKey{}).(bool); inStep {
		return nil, false
	}
	return workflowops.ExecutionFromContext(ctx)
}
