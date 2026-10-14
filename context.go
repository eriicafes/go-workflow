package workflow

import (
	"context"
	"net/http"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

type clientContextKey struct{}

func NewContext(ctx context.Context, client Client) context.Context {
	return context.WithValue(ctx, clientContextKey{}, client)
}

func Middleware(handler http.Handler, client Client) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler.ServeHTTP(w, r.WithContext(NewContext(r.Context(), client)))
	})
}

type WorkflowMetadata struct {
	Name      string
	RunID     string
	StartedAt time.Time
}

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

type StepMetadata struct {
	ID        string
	Name      string
	StartedAt time.Time
	Attempt   int
}

func GetStepMetadata(ctx context.Context) StepMetadata {
	metadata, ok := worlds.StepMetadataFromContext(ctx)
	if !ok {
		return StepMetadata{}
	}
	return StepMetadata{ID: string(metadata.ID), Name: metadata.Name, StartedAt: metadata.StartedAt, Attempt: metadata.Attempt}
}

func StepID(ctx context.Context) string { return GetStepMetadata(ctx).ID }

func clientFromContext(ctx context.Context) (Client, error) {
	if execution, ok := worlds.ExecutionFromContext(ctx); ok {
		return NewClient(execution.World()), nil
	}
	if client, ok := ctx.Value(clientContextKey{}).(Client); ok {
		return client, nil
	}
	return nil, ErrNoClient
}

func workflowExecutionFromContext(ctx context.Context) (*worlds.Execution, bool) {
	return worlds.ExecutionFromContext(ctx)
}

func stepExecutionFromContext(ctx context.Context) (*worlds.Execution, bool) {
	if _, ok := worlds.StepMetadataFromContext(ctx); !ok {
		return nil, false
	}
	return workflowExecutionFromContext(ctx)
}
