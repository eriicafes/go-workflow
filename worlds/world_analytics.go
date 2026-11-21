package worlds

import (
	"context"
	"maps"
	"time"
)

// Analytics answers reporting queries across runs. A World returns it from
// its Analytics method only when its Capabilities report Analytics as true.
type Analytics interface {
	AnalyticsRuns
	AnalyticsAttributes
	AnalyticsSteps
	AnalyticsEvents
	AnalyticsHooks
	AnalyticsWaits
}

// AnalyticsRuns reads run summaries.
type AnalyticsRuns interface {
	// RunsGet returns the summary of the run identified by RunID. It returns
	// ErrRunNotFound when the run does not exist.
	RunsGet(context.Context, AnalyticsRunsGetInput) (AnalyticsRun, error)
	// RunsList returns the summaries of runs that match the input filters. It
	// applies Pagination to the result.
	RunsList(context.Context, AnalyticsRunsListInput) (Page[AnalyticsRun], error)
}

// AnalyticsAttributes aggregates run attributes.
type AnalyticsAttributes interface {
	// AttributesList returns one aggregate per attribute key across the runs
	// that match the input filters. It applies Pagination to the result.
	AttributesList(context.Context, AnalyticsAttributesListInput) (Page[AnalyticsAttribute], error)
}

// AnalyticsWaits reads the waits of a run.
type AnalyticsWaits interface {
	// WaitsGet returns the wait identified by RunID and WaitID. It returns
	// ErrEventNotFound when the wait does not exist.
	WaitsGet(context.Context, AnalyticsWaitsGetInput) (AnalyticsWait, error)
	// WaitsList returns the waits in a run. It applies Pagination to the result.
	WaitsList(context.Context, AnalyticsWaitsListInput) (Page[AnalyticsWait], error)
}

type (
	// AnalyticsRunsGetInput holds the arguments to AnalyticsRuns.RunsGet.
	AnalyticsRunsGetInput struct {
		RunID RunID
	}
	// AnalyticsRunsListInput holds the arguments to AnalyticsRuns.RunsList.
	// Unset filters match every run.
	AnalyticsRunsListInput struct {
		WorkflowName string
		// Status selects runs in any of the listed statuses.
		Status []Status
		// StartTime and EndTime bound the run creation time, inclusive.
		StartTime *time.Time
		EndTime   *time.Time
		// Attributes selects runs that have every listed key with the
		// listed value.
		Attributes map[string]string
		Pagination Pagination
	}
	// AnalyticsAttributesListInput holds the arguments to AttributesList.
	// Unset filters match every run.
	AnalyticsAttributesListInput struct {
		WorkflowName string
		// StartTime and EndTime bound the run creation time, inclusive.
		StartTime  *time.Time
		EndTime    *time.Time
		Pagination Pagination
	}
	// AnalyticsWaitsGetInput holds the arguments to WaitsGet.
	AnalyticsWaitsGetInput struct {
		RunID  RunID
		WaitID WaitID
	}
	// AnalyticsWaitsListInput holds the arguments to WaitsList. An empty
	// Status matches every wait.
	AnalyticsWaitsListInput struct {
		RunID      RunID
		Status     WaitStatus
		Pagination Pagination
	}
)

// AnalyticsRun is the summary of one run, without its payloads.
type AnalyticsRun struct {
	RunID        RunID
	Status       Status
	DeploymentID string
	WorkflowName string
	SpecVersion  int
	Attributes   map[string]string
	CreatedAt    time.Time
	UpdatedAt    time.Time
	StartedAt    *time.Time
	CompletedAt  *time.Time
	// ErrorCode is the category of a failed run's error.
	ErrorCode string
}

// NewAnalyticsRun returns the summary of run.
func NewAnalyticsRun(run Run) AnalyticsRun {
	return AnalyticsRun{RunID: run.RunID, Status: run.Status, DeploymentID: run.DeploymentID, WorkflowName: run.WorkflowName, SpecVersion: run.SpecVersion, Attributes: maps.Clone(run.Attributes), CreatedAt: run.CreatedAt, UpdatedAt: run.UpdatedAt, StartedAt: run.StartedAt, CompletedAt: run.CompletedAt, ErrorCode: run.ErrorCode}
}

// AnalyticsAttribute aggregates one attribute key across runs.
type AnalyticsAttribute struct {
	Key string
	// RunCount is the number of matching runs that have the key.
	RunCount int
	// FirstSeenAt is the earliest creation time of those runs.
	FirstSeenAt time.Time
	// LastSeenAt is the latest update time of those runs.
	LastSeenAt time.Time
}

// AnalyticsWait describes one timer recorded by a wait_created event, read
// from its events, so a finished run's waits are still reported.
type AnalyticsWait struct {
	RunID  RunID
	WaitID WaitID
	Status WaitStatus
	// ResumeAt is when the timer fires.
	ResumeAt *time.Time
	// CreatedAt is the time of its wait_created, and UpdatedAt the time of
	// its latest event.
	CreatedAt   time.Time
	UpdatedAt   time.Time
	CompletedAt *time.Time
}
