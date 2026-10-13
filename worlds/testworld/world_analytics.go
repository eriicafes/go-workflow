package testworld

import (
	"context"
	"maps"

	"github.com/eriicafes/go-workflow/worlds"
)

// analytics is required only because worlds.Analytics.RunsGet/RunsList have the same
// names as StorageRuns methods but different input types.
type analytics struct{ world *World }

func (a analytics) RunsGet(ctx context.Context, input worlds.AnalyticsRunsGetInput) (worlds.AnalyticsRun, error) {
	return a.world.analyticsRunsGet(ctx, input)
}
func (a analytics) RunsList(ctx context.Context, input worlds.AnalyticsRunsListInput) (worlds.Page[worlds.AnalyticsRun], error) {
	return a.world.analyticsRunsList(ctx, input)
}
func (a analytics) AttributesList(ctx context.Context, input worlds.AnalyticsAttributesListInput) (worlds.Page[worlds.AnalyticsAttribute], error) {
	return a.world.analyticsAttributesList(ctx, input)
}

func analyticsRun(run worlds.Run) worlds.AnalyticsRun {
	return worlds.AnalyticsRun{RunID: run.RunID, WorkflowName: run.WorkflowName, Status: run.Status, Attributes: maps.Clone(run.Attributes), CreatedAt: run.CreatedAt, UpdatedAt: run.UpdatedAt, StartedAt: run.StartedAt, CompletedAt: run.CompletedAt}
}
