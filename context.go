package workflow

import (
	"context"
	"net/http"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// NewContext returns a copy of ctx that carries client. Start, Get, GetHook,
// ResumeHook, and Run methods use it, and in step context it takes precedence
// over the World that records the run.
func NewContext(ctx context.Context, client Client) context.Context {
	return worlds.NewClientContext(ctx, client)
}

// Middleware returns an http.Handler that serves each request with handler,
// adding client to the request context as NewContext does.
func Middleware(handler http.Handler, client Client) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler.ServeHTTP(w, r.WithContext(NewContext(r.Context(), client)))
	})
}

// WorkflowMetadata describes the run a workflow or step is executing in.
type WorkflowMetadata struct {
	// Name is the workflow name.
	Name string
	// RunID is the run's ID.
	RunID string
	// StartedAt is when the run started, or when it was created if it has
	// not started.
	StartedAt time.Time
}

// GetWorkflowMetadata returns metadata about the run ctx belongs to, or the
// zero WorkflowMetadata when ctx does not belong to a run.
func GetWorkflowMetadata(ctx context.Context) WorkflowMetadata {
	execution, ok := worlds.ExecutionFromContext(ctx)
	if !ok {
		return WorkflowMetadata{}
	}
	run := execution.Run()
	startedAt := run.CreatedAt
	if run.StartedAt != nil {
		startedAt = *run.StartedAt
	}
	return WorkflowMetadata{Name: run.WorkflowName, RunID: string(run.RunID), StartedAt: startedAt}
}

// StepMetadata describes the step attempt that step context belongs to.
type StepMetadata struct {
	// ID is the step ID, stable across attempts. It is suitable as an
	// idempotency key.
	ID string
	// Name is the step function's name.
	Name string
	// StartedAt is when the current attempt started.
	StartedAt time.Time
	// Attempt is the current attempt number, starting at 1.
	Attempt int
}

// GetStepMetadata returns metadata about the step ctx belongs to, or the zero
// StepMetadata when ctx does not belong to a step.
func GetStepMetadata(ctx context.Context) StepMetadata {
	metadata, ok := worlds.StepMetadataFromContext(ctx)
	if !ok {
		return StepMetadata{}
	}
	return StepMetadata{ID: string(metadata.ID), Name: metadata.Name, StartedAt: metadata.StartedAt, Attempt: metadata.Attempt}
}

// StepID returns the ID of the step ctx belongs to, or "" outside step context.
// It is shorthand for GetStepMetadata(ctx).ID.
func StepID(ctx context.Context) string { return GetStepMetadata(ctx).ID }

// clientFromContext returns the Client that operations started from ctx use,
// which is the Client NewContext or Middleware put in ctx. In a run without
// one, it is a Client for the World that records the run. It returns
// ErrNoClient outside a run when ctx carries none.
//
// The runtime removes the Client from a delivery's context, so in a run a
// Client in ctx is always one the run's own code set.
func clientFromContext(ctx context.Context) (Client, error) {
	if client, ok := worlds.ClientFromContext(ctx); ok {
		return client, nil
	}
	if execution, ok := worlds.ExecutionFromContext(ctx); ok {
		return NewClient(execution.World()), nil
	}
	return nil, ErrNoClient
}

// workflowExecutionFromContext returns the Execution ctx belongs to when ctx
// is workflow context. Code that runs in workflow or step context uses
// worlds.ExecutionFromContext instead.
func workflowExecutionFromContext(ctx context.Context) (*worlds.Execution, bool) {
	if _, inStep := worlds.StepMetadataFromContext(ctx); inStep {
		return nil, false
	}
	return worlds.ExecutionFromContext(ctx)
}

// stepExecutionFromContext returns the Execution ctx belongs to when ctx is
// step context. It reports false in workflow context and outside a run.
func stepExecutionFromContext(ctx context.Context) (*worlds.Execution, bool) {
	if _, ok := worlds.StepMetadataFromContext(ctx); !ok {
		return nil, false
	}
	return worlds.ExecutionFromContext(ctx)
}
