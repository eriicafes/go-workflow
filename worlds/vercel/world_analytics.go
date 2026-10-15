package vercelworld

import (
	"context"
	"encoding/json"
	"sort"
	"strconv"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// analytics adapts World to worlds.Analytics, whose RunsGet and RunsList
// names collide with StorageRuns.
type analytics struct{ world *World }

func (a analytics) RunsGet(ctx context.Context, input worlds.AnalyticsRunsGetInput) (worlds.AnalyticsRun, error) {
	run, err := a.world.RunsGet(ctx, worlds.RunsGetInput{RunID: input.RunID, ResolveData: worlds.ResolveDataNone})
	if err != nil {
		return worlds.AnalyticsRun{}, err
	}
	return analyticsRun(run), nil
}

func (a analytics) RunsList(ctx context.Context, input worlds.AnalyticsRunsListInput) (worlds.Page[worlds.AnalyticsRun], error) {
	// Scan the run index in creation order and filter runs by time window and
	// attributes. The cursor is the creation score of the last run.
	const batch = 200
	page := worlds.Page[worlds.AnalyticsRun]{Data: []worlds.AnalyticsRun{}}
	cursor := input.Pagination.Cursor
	lastCursor := ""
	for {
		members, next, hasMore, err := a.world.runIndexPage(ctx, input.WorkflowName, input.Status, worlds.Pagination{Cursor: cursor, Limit: batch, SortOrder: input.Pagination.SortOrder})
		if err != nil {
			return worlds.Page[worlds.AnalyticsRun]{}, err
		}
		ids := make([]string, len(members))
		for index, member := range members {
			ids[index] = member.Member
		}
		runs, err := a.world.loadRuns(ctx, ids, worlds.ResolveDataNone)
		if err != nil {
			return worlds.Page[worlds.AnalyticsRun]{}, err
		}
		for index, run := range runs {
			if run == nil || !matchesRun(*run, input.StartTime, input.EndTime, input.Attributes) {
				continue
			}
			if input.Pagination.Limit > 0 && len(page.Data) == input.Pagination.Limit {
				page.HasMore, page.Cursor = true, lastCursor
				return page, nil
			}
			page.Data = append(page.Data, analyticsRun(*run))
			lastCursor = formatScore(members[index].Score)
		}
		if !hasMore {
			return page, nil
		}
		cursor = next
	}
}

func (a analytics) AttributesList(ctx context.Context, input worlds.AnalyticsAttributesListInput) (worlds.Page[worlds.AnalyticsAttribute], error) {
	runs, err := a.RunsList(ctx, worlds.AnalyticsRunsListInput{WorkflowName: input.WorkflowName, StartTime: input.StartTime, EndTime: input.EndTime})
	if err != nil {
		return worlds.Page[worlds.AnalyticsAttribute]{}, err
	}
	items := map[string]worlds.AnalyticsAttribute{}
	for _, run := range runs.Data {
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
	sort.Slice(values, func(i, j int) bool { return values[i].Key < values[j].Key })
	return offsetPage(values, input.Pagination), nil
}

func (a analytics) WaitsGet(ctx context.Context, input worlds.AnalyticsWaitsGetInput) (worlds.AnalyticsWait, error) {
	page, err := a.WaitsList(ctx, worlds.AnalyticsWaitsListInput{RunID: input.RunID})
	if err != nil {
		return worlds.AnalyticsWait{}, err
	}
	for _, wait := range page.Data {
		if wait.WaitID == input.WaitID {
			return wait, nil
		}
	}
	return worlds.AnalyticsWait{}, worlds.ErrEventNotFound
}

func (a analytics) WaitsList(ctx context.Context, input worlds.AnalyticsWaitsListInput) (worlds.Page[worlds.AnalyticsWait], error) {
	// Waits are derived from the run's wait_created and wait_completed events.
	waits := map[worlds.WaitID]*worlds.AnalyticsWait{}
	var order []worlds.WaitID
	cursor := ""
	for {
		events, err := a.world.EventsList(ctx, worlds.EventsListInput{RunID: input.RunID, Pagination: worlds.Pagination{Cursor: cursor, Limit: 1000}})
		if err != nil {
			return worlds.Page[worlds.AnalyticsWait]{}, err
		}
		for _, event := range events.Data {
			id := worlds.WaitID(event.CorrelationID)
			switch event.EventType {
			case worlds.EventTypeWaitCreated:
				var data struct {
					DueAt          time.Time `json:"dueAt"`
					Cancel         bool      `json:"cancel"`
					QueueMessageID string    `json:"queueMessageId"`
				}
				if json.Unmarshal(event.Data, &data) != nil {
					continue
				}
				if _, ok := waits[id]; !ok {
					order = append(order, id)
				}
				waits[id] = &worlds.AnalyticsWait{RunID: input.RunID, WaitID: id, DueAt: data.DueAt, Cancel: data.Cancel, QueueMessageID: data.QueueMessageID, CreatedAt: event.CreatedAt}
			case worlds.EventTypeWaitCompleted:
				if wait, ok := waits[id]; ok {
					at := event.CreatedAt
					wait.CompletedAt = &at
				}
			}
		}
		if !events.HasMore {
			break
		}
		cursor = events.Cursor
	}
	values := make([]worlds.AnalyticsWait, 0, len(order))
	for _, id := range order {
		values = append(values, *waits[id])
	}
	sort.SliceStable(values, func(i, j int) bool { return values[i].CreatedAt.Before(values[j].CreatedAt) })
	return offsetPage(values, input.Pagination), nil
}

func analyticsRun(run worlds.Run) worlds.AnalyticsRun {
	return worlds.AnalyticsRun{RunID: run.RunID, WorkflowName: run.WorkflowName, Status: run.Status, Attributes: run.Attributes, CreatedAt: run.CreatedAt, UpdatedAt: run.UpdatedAt, StartedAt: run.StartedAt, CompletedAt: run.CompletedAt}
}

func matchesRun(run worlds.Run, start, end *time.Time, attributes map[string]string) bool {
	if start != nil && run.CreatedAt.Before(*start) {
		return false
	}
	if end != nil && run.CreatedAt.After(*end) {
		return false
	}
	for key, value := range attributes {
		if run.Attributes[key] != value {
			return false
		}
	}
	return true
}

// offsetPage pages an in-memory result with a decimal offset cursor.
func offsetPage[T any](values []T, pagination worlds.Pagination) worlds.Page[T] {
	start := 0
	if value, err := strconv.Atoi(pagination.Cursor); err == nil && value > 0 {
		start = min(value, len(values))
	}
	end := len(values)
	if pagination.Limit > 0 {
		end = min(start+pagination.Limit, len(values))
	}
	page := worlds.Page[T]{Data: values[start:end], HasMore: end < len(values)}
	if page.HasMore {
		page.Cursor = strconv.Itoa(end)
	}
	return page
}
