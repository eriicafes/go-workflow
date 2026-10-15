package postgresworld

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
	"github.com/jackc/pgx/v5"
)

// analytics adapts World to worlds.Analytics, whose RunsGet and RunsList
// collide with the storage methods of the same names.
type analytics struct{ world *World }

const analyticsRunColumns = "run_id, workflow_name, status, attributes, created_at, updated_at, started_at, completed_at"

func (a analytics) RunsGet(ctx context.Context, input worlds.AnalyticsRunsGetInput) (worlds.AnalyticsRun, error) {
	run, err := scanAnalyticsRun(a.world.pool.QueryRow(ctx, `SELECT `+analyticsRunColumns+` FROM go_workflow_runs WHERE run_id = $1`, input.RunID))
	if errors.Is(err, pgx.ErrNoRows) {
		return worlds.AnalyticsRun{}, worlds.ErrRunNotFound
	}
	return run, err
}

func (a analytics) RunsList(ctx context.Context, input worlds.AnalyticsRunsListInput) (worlds.Page[worlds.AnalyticsRun], error) {
	var q query
	if err := runFilter(&q, input.WorkflowName, input.Status, input.StartTime, input.EndTime, input.Attributes); err != nil {
		return worlds.Page[worlds.AnalyticsRun]{}, err
	}
	if err := q.keyset(input.Pagination.Cursor, "created_at", "run_id", false); err != nil {
		return worlds.Page[worlds.AnalyticsRun]{}, err
	}
	rows, err := a.world.pool.Query(ctx, `SELECT `+analyticsRunColumns+` FROM go_workflow_runs`+q.clause()+order("created_at", "run_id", false)+q.limit(input.Pagination.Limit), q.args...)
	if err != nil {
		return worlds.Page[worlds.AnalyticsRun]{}, err
	}
	runs, err := collect(rows, scanAnalyticsRun)
	if err != nil {
		return worlds.Page[worlds.AnalyticsRun]{}, err
	}
	return keysetPage(runs, input.Pagination.Limit, func(run worlds.AnalyticsRun) (time.Time, string) { return run.CreatedAt, string(run.RunID) }), nil
}

// AttributesList groups matching runs by attribute key. The cursor is the
// last returned key.
func (a analytics) AttributesList(ctx context.Context, input worlds.AnalyticsAttributesListInput) (worlds.Page[worlds.AnalyticsAttribute], error) {
	var q query
	if err := runFilter(&q, input.WorkflowName, nil, input.StartTime, input.EndTime, nil); err != nil {
		return worlds.Page[worlds.AnalyticsAttribute]{}, err
	}
	if input.Pagination.Cursor != "" {
		q.where(`key COLLATE "C" > ` + q.arg(input.Pagination.Cursor))
	}
	sql := `SELECT key, count(*), min(created_at), max(updated_at) FROM go_workflow_runs, jsonb_object_keys(attributes) AS key` +
		q.clause() + ` GROUP BY key ORDER BY key COLLATE "C"` + q.limit(input.Pagination.Limit)
	rows, err := a.world.pool.Query(ctx, sql, q.args...)
	if err != nil {
		return worlds.Page[worlds.AnalyticsAttribute]{}, err
	}
	values, err := collect(rows, func(row pgx.Row) (worlds.AnalyticsAttribute, error) {
		var value worlds.AnalyticsAttribute
		err := row.Scan(&value.Key, &value.RunCount, &value.FirstSeenAt, &value.LastSeenAt)
		value.FirstSeenAt, value.LastSeenAt = value.FirstSeenAt.UTC(), value.LastSeenAt.UTC()
		return value, err
	})
	if err != nil {
		return worlds.Page[worlds.AnalyticsAttribute]{}, err
	}
	if input.Pagination.Limit <= 0 || len(values) <= input.Pagination.Limit {
		return worlds.Page[worlds.AnalyticsAttribute]{Data: values}, nil
	}
	values = values[:input.Pagination.Limit]
	return worlds.Page[worlds.AnalyticsAttribute]{Data: values, HasMore: true, Cursor: values[len(values)-1].Key}, nil
}

type waitCreatedData struct {
	DueAt          time.Time `json:"dueAt"`
	Cancel         bool      `json:"cancel"`
	QueueMessageID string    `json:"queueMessageId"`
}

// waits derives waits from wait_created events, completed by the first
// wait_completed with the same correlation ID. Unparseable wait_created data
// is skipped.
func (a analytics) waits(ctx context.Context, runID worlds.RunID, waitID *worlds.WaitID) ([]worlds.AnalyticsWait, error) {
	if err := a.world.runExists(ctx, runID); err != nil {
		return nil, err
	}
	var q query
	q.where("c.run_id = " + q.arg(runID))
	q.where("c.type = " + q.arg(worlds.EventTypeWaitCreated))
	if waitID != nil {
		q.where("c.correlation_id = " + q.arg(*waitID))
	}
	completed := q.arg(worlds.EventTypeWaitCompleted)
	rows, err := a.world.pool.Query(ctx, `SELECT c.correlation_id, c.data, c.created_at,
			(SELECT min(d.created_at) FROM go_workflow_events d WHERE d.run_id = c.run_id AND d.correlation_id = c.correlation_id AND d.type = `+completed+`)
		FROM go_workflow_events c`+q.clause()+` ORDER BY c.created_at, c.slot`, q.args...)
	if err != nil {
		return nil, err
	}
	type row struct {
		wait worlds.AnalyticsWait
		data []byte
	}
	values, err := collect(rows, func(scan pgx.Row) (row, error) {
		var value row
		value.wait.RunID = runID
		err := scan.Scan(&value.wait.WaitID, &value.data, &value.wait.CreatedAt, &value.wait.CompletedAt)
		return value, err
	})
	if err != nil {
		return nil, err
	}
	// A later wait_created for the same ID replaces the earlier one.
	index := map[worlds.WaitID]int{}
	waits := []worlds.AnalyticsWait{}
	for _, value := range values {
		var data waitCreatedData
		if json.Unmarshal(value.data, &data) != nil {
			continue
		}
		wait := value.wait
		wait.DueAt, wait.Cancel, wait.QueueMessageID = data.DueAt.UTC(), data.Cancel, data.QueueMessageID
		wait.CreatedAt, wait.CompletedAt = wait.CreatedAt.UTC(), utc(wait.CompletedAt)
		if i, ok := index[wait.WaitID]; ok {
			waits[i] = wait
			continue
		}
		index[wait.WaitID] = len(waits)
		waits = append(waits, wait)
	}
	return waits, nil
}

func (a analytics) WaitsGet(ctx context.Context, input worlds.AnalyticsWaitsGetInput) (worlds.AnalyticsWait, error) {
	waits, err := a.waits(ctx, input.RunID, &input.WaitID)
	if err != nil {
		return worlds.AnalyticsWait{}, err
	}
	if len(waits) == 0 {
		return worlds.AnalyticsWait{}, worlds.ErrEventNotFound
	}
	return waits[0], nil
}

func (a analytics) WaitsList(ctx context.Context, input worlds.AnalyticsWaitsListInput) (worlds.Page[worlds.AnalyticsWait], error) {
	waits, err := a.waits(ctx, input.RunID, nil)
	if err != nil {
		return worlds.Page[worlds.AnalyticsWait]{}, err
	}
	return offsetPage(waits, input.Pagination)
}

func scanAnalyticsRun(row pgx.Row) (worlds.AnalyticsRun, error) {
	var run worlds.AnalyticsRun
	var attributes []byte
	if err := row.Scan(&run.RunID, &run.WorkflowName, &run.Status, &attributes, &run.CreatedAt, &run.UpdatedAt, &run.StartedAt, &run.CompletedAt); err != nil {
		return worlds.AnalyticsRun{}, err
	}
	run.Attributes = map[string]string{}
	if err := json.Unmarshal(attributes, &run.Attributes); err != nil {
		return worlds.AnalyticsRun{}, err
	}
	run.CreatedAt, run.UpdatedAt = run.CreatedAt.UTC(), run.UpdatedAt.UTC()
	run.StartedAt, run.CompletedAt = utc(run.StartedAt), utc(run.CompletedAt)
	return run, nil
}

// runFilter adds the shared analytics filters; StartTime and EndTime are
// inclusive bounds on CreatedAt.
func runFilter(q *query, workflowName string, status []worlds.Status, start, end *time.Time, attributes map[string]string) error {
	if workflowName != "" {
		q.where("workflow_name = " + q.arg(workflowName))
	}
	if len(status) > 0 {
		q.where("status = ANY(" + q.arg(statusStrings(status)) + ")")
	}
	if start != nil {
		q.where("created_at >= " + q.arg(*start))
	}
	if end != nil {
		q.where("created_at <= " + q.arg(*end))
	}
	if len(attributes) > 0 {
		encoded, err := json.Marshal(attributes)
		if err != nil {
			return err
		}
		q.where("attributes @> " + q.arg(encoded) + "::jsonb")
	}
	return nil
}
