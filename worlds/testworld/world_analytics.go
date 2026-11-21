package testworld

import (
	"context"

	"github.com/eriicafes/go-workflow/worlds"
)

// analytics is required only because worlds.Analytics.RunsGet/RunsList have the
// same names as StorageRuns methods but different input types.
// Steps, events, and hooks are read from the event log, by
// worlds.LogAnalytics.
type analytics struct {
	worlds.LogAnalytics
	world *World
}

func (a analytics) RunsGet(ctx context.Context, input worlds.AnalyticsRunsGetInput) (worlds.AnalyticsRun, error) {
	return a.world.analyticsRunsGet(ctx, input)
}
func (a analytics) RunsList(ctx context.Context, input worlds.AnalyticsRunsListInput) (worlds.Page[worlds.AnalyticsRun], error) {
	return a.world.analyticsRunsList(ctx, input)
}
func (a analytics) AttributesList(ctx context.Context, input worlds.AnalyticsAttributesListInput) (worlds.Page[worlds.AnalyticsAttribute], error) {
	return a.world.analyticsAttributesList(ctx, input)
}
