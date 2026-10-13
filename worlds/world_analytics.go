package worlds

import (
	"context"
	"time"
)

type Analytics interface {
	AnalyticsRuns
	AnalyticsAttributes
}

type AnalyticsRuns interface {
	RunsGet(context.Context, AnalyticsRunsGetInput) (AnalyticsRun, error)
	RunsList(context.Context, AnalyticsRunsListInput) (Page[AnalyticsRun], error)
}

type AnalyticsAttributes interface {
	AttributesList(context.Context, AnalyticsAttributesListInput) (Page[AnalyticsAttribute], error)
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
