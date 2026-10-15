package localworld

import (
	"context"
	"encoding/json"
	"maps"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// analytics exists because worlds.Analytics.RunsGet and RunsList share names
// with the storage methods but take different inputs.
type analytics struct{ world *World }

type waitCreatedData struct {
	DueAt          time.Time `json:"dueAt"`
	Cancel         bool      `json:"cancel"`
	QueueMessageID string    `json:"queueMessageId"`
}

func (a analytics) RunsGet(ctx context.Context, input worlds.AnalyticsRunsGetInput) (worlds.AnalyticsRun, error) {
	run, err := a.world.RunsGet(ctx, worlds.RunsGetInput{RunID: input.RunID, ResolveData: worlds.ResolveDataNone})
	if err != nil {
		return worlds.AnalyticsRun{}, err
	}
	return analyticsRun(run), nil
}

// matchingRuns returns runs that match the filters, ascending by CreatedAt.
func (a analytics) matchingRuns(ctx context.Context, workflowName string, statuses []worlds.Status, start, end *time.Time, attributes map[string]string) ([]worlds.AnalyticsRun, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	unlock, err := a.world.readLock()
	if err != nil {
		return nil, err
	}
	defer unlock()
	records, err := a.world.readRuns()
	if err != nil {
		return nil, err
	}
	runs := make([]worlds.AnalyticsRun, 0, len(records))
	for _, record := range records {
		run := record.Run
		if !matchesRun(run, workflowName, statuses) || start != nil && run.CreatedAt.Before(*start) || end != nil && run.CreatedAt.After(*end) {
			continue
		}
		matched := true
		for key, value := range attributes {
			if current, ok := run.Attributes[key]; !ok || current != value {
				matched = false
				break
			}
		}
		if matched {
			runs = append(runs, analyticsRun(run))
		}
	}
	return runs, nil
}

func (a analytics) RunsList(ctx context.Context, input worlds.AnalyticsRunsListInput) (worlds.Page[worlds.AnalyticsRun], error) {
	runs, err := a.matchingRuns(ctx, input.WorkflowName, input.Status, input.StartTime, input.EndTime, input.Attributes)
	if err != nil {
		return worlds.Page[worlds.AnalyticsRun]{}, err
	}
	pagination := input.Pagination
	pagination.SortOrder = worlds.SortOrderAscending
	return keyPage(runs, pagination, func(run worlds.AnalyticsRun) string { return sortKey(run.CreatedAt, string(run.RunID)) })
}

func (a analytics) AttributesList(ctx context.Context, input worlds.AnalyticsAttributesListInput) (worlds.Page[worlds.AnalyticsAttribute], error) {
	runs, err := a.matchingRuns(ctx, input.WorkflowName, nil, input.StartTime, input.EndTime, nil)
	if err != nil {
		return worlds.Page[worlds.AnalyticsAttribute]{}, err
	}
	items := map[string]worlds.AnalyticsAttribute{}
	for _, run := range runs {
		for key := range run.Attributes {
			item, ok := items[key]
			if !ok {
				item = worlds.AnalyticsAttribute{Key: key, FirstSeenAt: run.CreatedAt, LastSeenAt: run.UpdatedAt}
			}
			item.RunCount++
			if run.CreatedAt.Before(item.FirstSeenAt) {
				item.FirstSeenAt = run.CreatedAt
			}
			if run.UpdatedAt.After(item.LastSeenAt) {
				item.LastSeenAt = run.UpdatedAt
			}
			items[key] = item
		}
	}
	values := make([]worlds.AnalyticsAttribute, 0, len(items))
	for _, item := range items {
		values = append(values, item)
	}
	pagination := input.Pagination
	pagination.SortOrder = worlds.SortOrderAscending
	return keyPage(values, pagination, func(item worlds.AnalyticsAttribute) string { return item.Key })
}

func (a analytics) WaitsGet(ctx context.Context, input worlds.AnalyticsWaitsGetInput) (worlds.AnalyticsWait, error) {
	waits, err := a.waits(ctx, input.RunID)
	if err != nil {
		return worlds.AnalyticsWait{}, err
	}
	for _, wait := range waits {
		if wait.WaitID == input.WaitID {
			return wait, nil
		}
	}
	return worlds.AnalyticsWait{}, worlds.ErrEventNotFound
}

func (a analytics) WaitsList(ctx context.Context, input worlds.AnalyticsWaitsListInput) (worlds.Page[worlds.AnalyticsWait], error) {
	waits, err := a.waits(ctx, input.RunID)
	if err != nil {
		return worlds.Page[worlds.AnalyticsWait]{}, err
	}
	pagination := input.Pagination
	pagination.SortOrder = worlds.SortOrderAscending
	return keyPage(waits, pagination, func(wait worlds.AnalyticsWait) string { return sortKey(wait.CreatedAt, string(wait.WaitID)) })
}

// waits derives waits from the run's committed wait_created and
// wait_completed events.
func (a analytics) waits(ctx context.Context, runID worlds.RunID) ([]worlds.AnalyticsWait, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	unlock, err := a.world.readLock()
	if err != nil {
		return nil, err
	}
	defer unlock()
	run, err := a.world.readRun(runID)
	if err != nil {
		return nil, err
	}
	events, err := a.world.readEvents(runID, 1, run.EventCount)
	if err != nil {
		return nil, err
	}
	index := map[worlds.WaitID]int{}
	var waits []worlds.AnalyticsWait
	for _, event := range events {
		id := worlds.WaitID(event.CorrelationID)
		switch event.EventType {
		case worlds.EventTypeWaitCreated:
			var data waitCreatedData
			if json.Unmarshal(event.Data, &data) != nil {
				continue
			}
			index[id] = len(waits)
			waits = append(waits, worlds.AnalyticsWait{RunID: runID, WaitID: id, DueAt: data.DueAt, Cancel: data.Cancel, QueueMessageID: data.QueueMessageID, CreatedAt: event.CreatedAt})
		case worlds.EventTypeWaitCompleted:
			if position, ok := index[id]; ok {
				at := event.CreatedAt
				waits[position].CompletedAt = &at
			}
		}
	}
	return waits, nil
}

func analyticsRun(run worlds.Run) worlds.AnalyticsRun {
	return worlds.AnalyticsRun{RunID: run.RunID, WorkflowName: run.WorkflowName, Status: run.Status, Attributes: maps.Clone(run.Attributes), CreatedAt: run.CreatedAt, UpdatedAt: run.UpdatedAt, StartedAt: run.StartedAt, CompletedAt: run.CompletedAt}
}
