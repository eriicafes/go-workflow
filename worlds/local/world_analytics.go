package localworld

import (
	"context"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// analytics exists because worlds.Analytics.RunsGet and RunsList share names
// with the storage methods but take different inputs.
// Steps, events, and hooks are read from the event log, by
// worlds.LogAnalytics.
type analytics struct {
	worlds.LogAnalytics
	world *World
}

func (a analytics) RunsGet(ctx context.Context, input worlds.AnalyticsRunsGetInput) (worlds.AnalyticsRun, error) {
	// Analytics runs carry no payloads, so the run record alone answers.
	var run worlds.AnalyticsRun
	err := a.world.read(ctx, func(s *store) error {
		record, err := s.getRun(input.RunID)
		run = worlds.NewAnalyticsRun(record.Run)
		return err
	})
	return run, err
}

// matchingRuns returns runs that match the filters, ascending by CreatedAt.
func (a analytics) matchingRuns(ctx context.Context, workflowName string, statuses []worlds.Status, start, end *time.Time, attributes map[string]string) ([]worlds.AnalyticsRun, error) {
	var records []runRecord
	if err := a.world.read(ctx, func(s *store) error {
		var err error
		records, err = s.listRuns(runFilter{workflowName: workflowName, statuses: statuses})
		return err
	}); err != nil {
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
			runs = append(runs, worlds.NewAnalyticsRun(run))
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
