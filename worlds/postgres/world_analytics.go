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
// Steps, events, and hooks are read from the event log, by
// worlds.LogAnalytics.
type analytics struct {
	worlds.LogAnalytics
	world *World
}

const analyticsRunColumns = "id, name, status::text, deployment_id, spec_version, coalesce(error_code, ''), attributes, created_at, updated_at, started_at, completed_at"

func (a analytics) RunsGet(ctx context.Context, input worlds.AnalyticsRunsGetInput) (worlds.AnalyticsRun, error) {
	run, err := scanAnalyticsRun(a.world.pool.QueryRow(ctx, a.world.q(`SELECT `+analyticsRunColumns+` FROM workflow.workflow_runs WHERE id = $1`), input.RunID))
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
	if err := q.keyset(input.Pagination.Cursor, "created_at", "id", false); err != nil {
		return worlds.Page[worlds.AnalyticsRun]{}, err
	}
	rows, err := a.world.pool.Query(ctx, a.world.q(`SELECT `+analyticsRunColumns+` FROM workflow.workflow_runs`+q.clause()+order("created_at", "id", false)+q.limit(input.Pagination.Limit)), q.args...)
	if err != nil {
		return worlds.Page[worlds.AnalyticsRun]{}, err
	}
	runs, err := collect(rows, scanAnalyticsRun)
	if err != nil {
		return worlds.Page[worlds.AnalyticsRun]{}, err
	}
	return keysetPage(runs, input.Pagination.Limit, func(run worlds.AnalyticsRun) (time.Time, string) { return run.CreatedAt, string(run.RunID) }), nil
}

func (a analytics) AttributesList(ctx context.Context, input worlds.AnalyticsAttributesListInput) (worlds.Page[worlds.AnalyticsAttribute], error) {
	// Matching runs are grouped by attribute key. The cursor is the last
	// returned key.
	var q query
	if err := runFilter(&q, input.WorkflowName, nil, input.StartTime, input.EndTime, nil); err != nil {
		return worlds.Page[worlds.AnalyticsAttribute]{}, err
	}
	if input.Pagination.Cursor != "" {
		q.where(`key COLLATE "C" > ` + q.arg(input.Pagination.Cursor))
	}
	sql := `SELECT key, count(*), min(created_at), max(updated_at) FROM workflow.workflow_runs, jsonb_object_keys(attributes) AS key` +
		q.clause() + ` GROUP BY key ORDER BY key COLLATE "C"` + q.limit(input.Pagination.Limit)
	rows, err := a.world.pool.Query(ctx, a.world.q(sql), q.args...)
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

func scanAnalyticsRun(row pgx.Row) (worlds.AnalyticsRun, error) {
	var run worlds.AnalyticsRun
	var attributes []byte
	if err := row.Scan(&run.RunID, &run.WorkflowName, &run.Status, &run.DeploymentID, &run.SpecVersion, &run.ErrorCode, &attributes, &run.CreatedAt, &run.UpdatedAt, &run.StartedAt, &run.CompletedAt); err != nil {
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

// runFilter adds the shared analytics filters. StartTime and EndTime are
// inclusive bounds on CreatedAt.
func runFilter(q *query, workflowName string, status []worlds.Status, start, end *time.Time, attributes map[string]string) error {
	if workflowName != "" {
		q.where("name = " + q.arg(workflowName))
	}
	if len(status) > 0 {
		q.where("status::text = ANY(" + q.arg(statusStrings(status)) + ")")
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
