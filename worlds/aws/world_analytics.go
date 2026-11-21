package awsworld

import (
	"context"
	"sort"
	"time"

	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/eriicafes/go-workflow/worlds"
)

// analytics adapts World to worlds.Analytics, whose RunsGet and RunsList
// differ from the storage methods of the same names.
// Steps, events, and hooks are read from the event log, by
// worlds.LogAnalytics.
type analytics struct {
	worlds.LogAnalytics
	world *World
}

func (a analytics) RunsGet(ctx context.Context, input worlds.AnalyticsRunsGetInput) (worlds.AnalyticsRun, error) {
	// An analytics run has no payloads, so its events are not read.
	if err := ctx.Err(); err != nil {
		return worlds.AnalyticsRun{}, err
	}
	item, found, err := a.world.item(ctx, runKey(string(input.RunID)))
	if err != nil {
		return worlds.AnalyticsRun{}, err
	}
	if !found {
		return worlds.AnalyticsRun{}, worlds.ErrRunNotFound
	}
	return worlds.NewAnalyticsRun(runFromItem(item).Run), nil
}

// RunsList reads a global secondary index, which is eventually consistent. A
// run created a moment ago may be missing from the result. The runs it returns
// are read consistently.
func (a analytics) RunsList(ctx context.Context, input worlds.AnalyticsRunsListInput) (worlds.Page[worlds.AnalyticsRun], error) {
	if err := ctx.Err(); err != nil {
		return worlds.Page[worlds.AnalyticsRun]{}, err
	}
	match := func(item map[string]ddb.AttributeValue) bool {
		attributes := runFromItem(item).Attributes
		for key, value := range input.Attributes {
			if current, ok := attributes[key]; !ok || current != value {
				return false
			}
		}
		return true
	}
	pagination := worlds.Pagination{Cursor: input.Pagination.Cursor, Limit: input.Pagination.Limit}
	items, cursor, more, err := a.world.indexPage(ctx, timeBounded(runsIndex(input.WorkflowName, input.Status), input.StartTime, input.EndTime), pagination, match)
	if err != nil {
		return worlds.Page[worlds.AnalyticsRun]{}, err
	}
	runs := make([]worlds.AnalyticsRun, len(items))
	for index, item := range items {
		runs[index] = worlds.NewAnalyticsRun(runFromItem(item).Run)
	}
	return worlds.Page[worlds.AnalyticsRun]{Data: runs, Cursor: cursor, HasMore: more}, nil
}

func (a analytics) AttributesList(ctx context.Context, input worlds.AnalyticsAttributesListInput) (worlds.Page[worlds.AnalyticsAttribute], error) {
	if err := ctx.Err(); err != nil {
		return worlds.Page[worlds.AnalyticsAttribute]{}, err
	}
	items, _, _, err := a.world.indexPage(ctx, timeBounded(runsIndex(input.WorkflowName, nil), input.StartTime, input.EndTime), worlds.Pagination{}, nil)
	if err != nil {
		return worlds.Page[worlds.AnalyticsAttribute]{}, err
	}
	aggregates := map[string]worlds.AnalyticsAttribute{}
	for _, item := range items {
		run := runFromItem(item).Run
		for key := range run.Attributes {
			aggregate, ok := aggregates[key]
			if !ok {
				aggregate = worlds.AnalyticsAttribute{Key: key, FirstSeenAt: run.CreatedAt, LastSeenAt: run.UpdatedAt}
			}
			aggregate.RunCount++
			if run.CreatedAt.Before(aggregate.FirstSeenAt) {
				aggregate.FirstSeenAt = run.CreatedAt
			}
			if run.UpdatedAt.After(aggregate.LastSeenAt) {
				aggregate.LastSeenAt = run.UpdatedAt
			}
			aggregates[key] = aggregate
		}
	}
	values := make([]worlds.AnalyticsAttribute, 0, len(aggregates))
	for _, aggregate := range aggregates {
		values = append(values, aggregate)
	}
	sort.Slice(values, func(i, j int) bool { return values[i].Key < values[j].Key })
	return sortedPage(values, func(value worlds.AnalyticsAttribute) string { return value.Key }, worlds.Pagination{Cursor: input.Pagination.Cursor, Limit: input.Pagination.Limit})
}

// timeBounded limits an index query to creation times in [start, end]. The
// index sort key is "<time>#<id>", and '$' sorts after '#'.
func timeBounded(query indexQuery, start, end *time.Time) indexQuery {
	if start != nil {
		query.from = stamp(*start)
	}
	if end != nil {
		query.to = stamp(*end) + "$"
	}
	return query
}
