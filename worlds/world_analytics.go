package worlds

import (
	"context"
	"time"
)

type Analytics interface {
	AnalyticsRuns
	AnalyticsAttributes
	AnalyticsWaits
}

type AnalyticsRuns interface {
	// RunsGet returns analytics for RunID. It returns ErrRunNotFound when the run
	// does not exist.
	RunsGet(context.Context, AnalyticsRunsGetInput) (AnalyticsRun, error)
	// RunsList returns analytics runs that match the input filters. It applies
	// Pagination to the result.
	RunsList(context.Context, AnalyticsRunsListInput) (Page[AnalyticsRun], error)
}

type AnalyticsAttributes interface {
	// AttributesList returns attribute aggregates that match the input filters.
	// It applies Pagination to the result.
	AttributesList(context.Context, AnalyticsAttributesListInput) (Page[AnalyticsAttribute], error)
}

type AnalyticsWaits interface {
	// WaitsGet returns the wait identified by RunID and WaitID. It returns
	// ErrEventNotFound when the wait does not exist.
	WaitsGet(context.Context, AnalyticsWaitsGetInput) (AnalyticsWait, error)
	// WaitsList returns the waits in a run. It applies Pagination to the result.
	WaitsList(context.Context, AnalyticsWaitsListInput) (Page[AnalyticsWait], error)
}

type (
	AnalyticsRunsGetInput struct {
		RunID RunID
	}
	AnalyticsRunsListInput struct {
		WorkflowName string
		Status       []Status
		StartTime    *time.Time
		EndTime      *time.Time
		Attributes   map[string]string
		Pagination   Pagination
	}
	AnalyticsAttributesListInput struct {
		WorkflowName string
		StartTime    *time.Time
		EndTime      *time.Time
		Pagination   Pagination
	}
	AnalyticsWaitsGetInput struct {
		RunID  RunID
		WaitID WaitID
	}
	AnalyticsWaitsListInput struct {
		RunID      RunID
		Pagination Pagination
	}
)

type AnalyticsRun struct {
	RunID        RunID
	WorkflowName string
	Status       Status
	Attributes   map[string]string
	CreatedAt    time.Time
	UpdatedAt    time.Time
	StartedAt    *time.Time
	CompletedAt  *time.Time
}

type AnalyticsAttribute struct {
	Key         string
	RunCount    int
	FirstSeenAt time.Time
	LastSeenAt  time.Time
}

type AnalyticsWait struct {
	RunID          RunID
	WaitID         WaitID
	DueAt          time.Time
	Cancel         bool
	QueueMessageID string
	CreatedAt      time.Time
	CompletedAt    *time.Time
}
