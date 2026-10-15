package postgresworld

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
	"github.com/jackc/pgx/v5"
)

func (w *World) RunsGet(ctx context.Context, input worlds.RunsGetInput) (worlds.Run, error) {
	run, err := scanRun(w.pool.QueryRow(ctx, `SELECT `+runColumns(input.ResolveData)+` FROM go_workflow_runs WHERE run_id = $1`, input.RunID))
	if errors.Is(err, pgx.ErrNoRows) {
		return worlds.Run{}, worlds.ErrRunNotFound
	}
	return run, err
}

func (w *World) RunsList(ctx context.Context, input worlds.RunsListInput) (worlds.Page[worlds.Run], error) {
	var q query
	if input.WorkflowName != "" {
		q.where("workflow_name = " + q.arg(input.WorkflowName))
	}
	if len(input.Status) > 0 {
		q.where("status = ANY(" + q.arg(statusStrings(input.Status)) + ")")
	}
	descending := input.Pagination.SortOrder == worlds.SortOrderDescending
	if err := q.keyset(input.Pagination.Cursor, "created_at", "run_id", descending); err != nil {
		return worlds.Page[worlds.Run]{}, err
	}
	sql := `SELECT ` + runColumns(input.ResolveData) + ` FROM go_workflow_runs` + q.clause() + order("created_at", "run_id", descending) + q.limit(input.Pagination.Limit)
	rows, err := w.pool.Query(ctx, sql, q.args...)
	if err != nil {
		return worlds.Page[worlds.Run]{}, err
	}
	runs, err := collect(rows, scanRun)
	if err != nil {
		return worlds.Page[worlds.Run]{}, err
	}
	return keysetPage(runs, input.Pagination.Limit, func(run worlds.Run) (time.Time, string) { return run.CreatedAt, string(run.RunID) }), nil
}

// RunsWaitForTerminalStatus re-reads the run on every go_workflow_runs
// notification for it and at least every PollInterval.
func (w *World) RunsWaitForTerminalStatus(ctx context.Context, input worlds.RunsWaitForTerminalStatusInput) (worlds.Run, error) {
	deadline := time.Now().Add(input.Timeout)
	for {
		wake, release := w.notifier.wait(channelRuns, string(input.RunID))
		run, err := w.RunsGet(ctx, worlds.RunsGetInput{RunID: input.RunID, ResolveData: input.ResolveData})
		remaining := time.Until(deadline)
		if err != nil || run.Status.Terminal() || input.Timeout <= 0 || remaining <= 0 {
			release()
			return run, err
		}
		timer := time.NewTimer(min(remaining, w.interval))
		select {
		case <-ctx.Done():
			timer.Stop()
			release()
			return worlds.Run{}, ctx.Err()
		case <-wake:
		case <-timer.C:
		}
		timer.Stop()
		release()
	}
}

func (w *World) RunsSetAttributes(ctx context.Context, input worlds.RunsSetAttributesInput) error {
	data, err := json.Marshal(attrSetData{Changes: input.Changes})
	if err != nil {
		return err
	}
	_, err = w.EventsCreate(ctx, worlds.EventsCreateInput{RunID: input.RunID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeAttrSet, Data: data}})
	return err
}

func runColumns(resolve worlds.ResolveData) string {
	data := "input, output"
	if resolve == worlds.ResolveDataNone {
		data = "NULL::bytea, NULL::bytea"
	}
	return "run_id, workflow_name, status, " + data + ", error, attributes, created_at, updated_at, started_at, completed_at"
}

func scanRun(row pgx.Row) (worlds.Run, error) {
	var run worlds.Run
	var attributes []byte
	if err := row.Scan(&run.RunID, &run.WorkflowName, &run.Status, &run.Input, &run.Output, &run.Error, &attributes, &run.CreatedAt, &run.UpdatedAt, &run.StartedAt, &run.CompletedAt); err != nil {
		return worlds.Run{}, err
	}
	run.Attributes = map[string]string{}
	if err := json.Unmarshal(attributes, &run.Attributes); err != nil {
		return worlds.Run{}, err
	}
	run.CreatedAt, run.UpdatedAt = run.CreatedAt.UTC(), run.UpdatedAt.UTC()
	run.StartedAt, run.CompletedAt = utc(run.StartedAt), utc(run.CompletedAt)
	return run, nil
}

func statusStrings(values []worlds.Status) []string {
	result := make([]string, len(values))
	for i, value := range values {
		result[i] = string(value)
	}
	return result
}
