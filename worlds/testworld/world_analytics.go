package testworld

import (
	"context"
	"encoding/json"
	"maps"
	"sort"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// analytics is required only because worlds.Analytics.RunsGet/RunsList have the
// same names as StorageRuns methods but different input types.
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
func (a analytics) WaitsGet(ctx context.Context, input worlds.AnalyticsWaitsGetInput) (worlds.AnalyticsWait, error) {
	page, err := a.WaitsList(ctx, worlds.AnalyticsWaitsListInput{RunID: input.RunID})
	if err != nil {
		return worlds.AnalyticsWait{}, err
	}
	for _, value := range page.Data {
		if value.WaitID == input.WaitID {
			return value, nil
		}
	}
	return worlds.AnalyticsWait{}, worlds.ErrEventNotFound
}
func (a analytics) WaitsList(ctx context.Context, input worlds.AnalyticsWaitsListInput) (worlds.Page[worlds.AnalyticsWait], error) {
	if err := ctx.Err(); err != nil {
		return worlds.Page[worlds.AnalyticsWait]{}, err
	}
	a.world.mu.RLock()
	defer a.world.mu.RUnlock()
	if _, ok := a.world.runs[input.RunID]; !ok {
		return worlds.Page[worlds.AnalyticsWait]{}, worlds.ErrRunNotFound
	}
	values := map[worlds.WaitID]worlds.AnalyticsWait{}
	for _, event := range a.world.events[input.RunID] {
		switch event.EventType {
		case worlds.EventTypeWaitCreated:
			var data struct {
				DueAt          time.Time `json:"dueAt"`
				Cancel         bool      `json:"cancel"`
				QueueMessageID string    `json:"queueMessageId"`
			}
			if json.Unmarshal(event.Data, &data) == nil {
				values[worlds.WaitID(event.CorrelationID)] = worlds.AnalyticsWait{RunID: input.RunID, WaitID: worlds.WaitID(event.CorrelationID), DueAt: data.DueAt, Cancel: data.Cancel, QueueMessageID: data.QueueMessageID, CreatedAt: event.CreatedAt}
			}
		case worlds.EventTypeWaitCompleted:
			id := worlds.WaitID(event.CorrelationID)
			if value, ok := values[id]; ok {
				at := event.CreatedAt
				value.CompletedAt = &at
				values[id] = value
			}
		}
	}
	result := make([]worlds.AnalyticsWait, 0, len(values))
	for _, value := range values {
		result = append(result, value)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt.Before(result[j].CreatedAt) })
	return page(result, input.Pagination), nil
}

func analyticsRun(run worlds.Run) worlds.AnalyticsRun {
	return worlds.AnalyticsRun{RunID: run.RunID, WorkflowName: run.WorkflowName, Status: run.Status, Attributes: maps.Clone(run.Attributes), CreatedAt: run.CreatedAt, UpdatedAt: run.UpdatedAt, StartedAt: run.StartedAt, CompletedAt: run.CompletedAt}
}
