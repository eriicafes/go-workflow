package awsworld

import (
	"context"
	"encoding/json"
	"maps"
	"sort"
	"time"

	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/eriicafes/go-workflow/worlds"
)

// analytics adapts World to worlds.Analytics, whose RunsGet and RunsList
// differ from the storage methods of the same names.
type analytics struct{ world *World }

func (a analytics) RunsGet(ctx context.Context, input worlds.AnalyticsRunsGetInput) (worlds.AnalyticsRun, error) {
	run, err := a.world.RunsGet(ctx, worlds.RunsGetInput{RunID: input.RunID, ResolveData: worlds.ResolveDataNone})
	if err != nil {
		return worlds.AnalyticsRun{}, err
	}
	return analyticsRun(run), nil
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
		runs[index] = analyticsRun(runFromItem(item).Run)
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

func (a analytics) WaitsGet(ctx context.Context, input worlds.AnalyticsWaitsGetInput) (worlds.AnalyticsWait, error) {
	waits, err := a.world.waits(ctx, input.RunID)
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
	waits, err := a.world.waits(ctx, input.RunID)
	if err != nil {
		return worlds.Page[worlds.AnalyticsWait]{}, err
	}
	sortKey := func(wait worlds.AnalyticsWait) string { return indexSortKey(wait.CreatedAt, string(wait.WaitID)) }
	return sortedPage(waits, sortKey, worlds.Pagination{Cursor: input.Pagination.Cursor, Limit: input.Pagination.Limit})
}

// waits derives the run's waits from its wait_created and wait_completed
// events, sorted by creation time.
func (w *World) waits(ctx context.Context, runID worlds.RunID) ([]worlds.AnalyticsWait, error) {
	if err := w.runExists(ctx, runID); err != nil {
		return nil, err
	}
	waits := map[worlds.WaitID]*worlds.AnalyticsWait{}
	err := w.query(ctx, &dynamodb.QueryInput{
		KeyConditionExpression:    awsv2.String("pk = :pk AND begins_with(sk, :prefix)"),
		FilterExpression:          awsv2.String("eventType IN (:created, :completed)"),
		ExpressionAttributeValues: map[string]ddb.AttributeValue{":pk": avs(runPartitionKey(string(runID))), ":prefix": avs("EVENT#"), ":created": avs(string(worlds.EventTypeWaitCreated)), ":completed": avs(string(worlds.EventTypeWaitCompleted))},
		ConsistentRead:            awsv2.Bool(true),
	}, func(item map[string]ddb.AttributeValue) bool {
		event := eventFromItem(item)
		id := worlds.WaitID(event.CorrelationID)
		switch event.EventType {
		case worlds.EventTypeWaitCreated:
			var data waitCreatedData
			if json.Unmarshal(event.Data, &data) == nil {
				waits[id] = &worlds.AnalyticsWait{RunID: runID, WaitID: id, DueAt: data.DueAt, Cancel: data.Cancel, QueueMessageID: data.QueueMessageID, CreatedAt: event.CreatedAt}
			}
		case worlds.EventTypeWaitCompleted:
			if wait := waits[id]; wait != nil && wait.CompletedAt == nil {
				completed := event.CreatedAt
				wait.CompletedAt = &completed
			}
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	result := make([]worlds.AnalyticsWait, 0, len(waits))
	for _, wait := range waits {
		result = append(result, *wait)
	}
	sort.Slice(result, func(i, j int) bool {
		return indexSortKey(result[i].CreatedAt, string(result[i].WaitID)) < indexSortKey(result[j].CreatedAt, string(result[j].WaitID))
	})
	return result, nil
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

func analyticsRun(run worlds.Run) worlds.AnalyticsRun {
	return worlds.AnalyticsRun{RunID: run.RunID, WorkflowName: run.WorkflowName, Status: run.Status, Attributes: maps.Clone(run.Attributes), CreatedAt: run.CreatedAt, UpdatedAt: run.UpdatedAt, StartedAt: run.StartedAt, CompletedAt: run.CompletedAt}
}
