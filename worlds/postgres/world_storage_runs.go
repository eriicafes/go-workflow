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
	run, err := scanRun(w.pool.QueryRow(ctx, runSelect(input.ResolveData)+` WHERE r.run_id = $1`, input.RunID))
	if errors.Is(err, pgx.ErrNoRows) {
		return worlds.Run{}, worlds.ErrRunNotFound
	}
	return run, err
}

func (w *World) RunsList(ctx context.Context, input worlds.RunsListInput) (worlds.Page[worlds.Run], error) {
	var q query
	if input.WorkflowName != "" {
		q.where("r.workflow_name = " + q.arg(input.WorkflowName))
	}
	if len(input.Status) > 0 {
		q.where("r.status = ANY(" + q.arg(statusStrings(input.Status)) + ")")
	}
	descending := input.Pagination.SortOrder == worlds.SortOrderDescending
	if err := q.keyset(input.Pagination.Cursor, "r.created_at", "r.run_id", descending); err != nil {
		return worlds.Page[worlds.Run]{}, err
	}
	sql := runSelect(input.ResolveData) + q.clause() + order("r.created_at", "r.run_id", descending) + q.limit(input.Pagination.Limit)
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

func (w *World) RunsWaitForTerminalStatus(ctx context.Context, input worlds.RunsWaitForTerminalStatusInput) (worlds.Run, error) {
	// Re-read the run on every go_workflow_runs notification for it and at
	// least every PollInterval.
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
			release()
			return worlds.Run{}, ctx.Err()
		case <-wake:
		case <-timer.C:
		}
		release()
	}
}

func (w *World) RunsSetAttributes(ctx context.Context, input worlds.RunsSetAttributesInput) error {
	data, err := worlds.EncodeAttrSet(input)
	if err != nil {
		return err
	}
	_, err = w.EventsCreate(ctx, worlds.EventsCreateInput{RunID: input.RunID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeAttrSet, Data: data}})
	return err
}

// runSelect selects runs as r with their payloads joined from the event log:
// the input from run_created in slot 1 and the output or error from the
// result slot. ResolveDataNone joins only a run_failed result, for the error.
func runSelect(resolve worlds.ResolveData) string {
	// Payloads read as NULL once the run's expired_at has passed.
	live := runLive("r.run_id")
	input, result, join := "ie.data", "re.data", " LEFT JOIN go_workflow_events ie ON ie.run_id = r.run_id AND ie.slot = 1"+live
	if resolve == worlds.ResolveDataNone {
		input, result, join = "NULL::bytea", "CASE WHEN re.type = '"+string(worlds.EventTypeRunFailed)+"' THEN re.data END", ""
	}
	return "SELECT r.run_id, r.workflow_name, r.status, " + input + ", re.type, " + result + ", r.attributes, r.deployment_id, r.encryption_public_key, r.in_band_count, r.spec_version, r.created_at, r.updated_at, r.started_at, r.completed_at, r.expired_at, r.execution_context, r.start_hook" +
		" FROM go_workflow_runs r" + join + " LEFT JOIN go_workflow_events re ON re.run_id = r.run_id AND re.slot = r.result_slot" + live
}

func scanRun(row pgx.Row) (worlds.Run, error) {
	var run worlds.Run
	var attributes, input, result, executionContext, startHook []byte
	var resultType *worlds.EventType
	var inBandCount int64
	if err := row.Scan(&run.RunID, &run.WorkflowName, &run.Status, &input, &resultType, &result, &attributes, &run.DeploymentID, &run.EncryptionPublicKey, &inBandCount, &run.SpecVersion, &run.CreatedAt, &run.UpdatedAt, &run.StartedAt, &run.CompletedAt, &run.ExpiredAt, &executionContext, &startHook); err != nil {
		return worlds.Run{}, err
	}
	if len(startHook) > 0 {
		if err := json.Unmarshal(startHook, &run.StartHook); err != nil {
			return worlds.Run{}, err
		}
	}
	if input != nil {
		run.Input = worlds.EventPayload(worlds.Event{EventType: worlds.EventTypeRunCreated, Data: input})
	}
	if resultType != nil && result != nil {
		payload := worlds.EventPayload(worlds.Event{EventType: *resultType, Data: result})
		if *resultType == worlds.EventTypeRunCompleted {
			run.Output = payload
		} else {
			run.Error = payload
		}
	}
	run.InBandCount = int(inBandCount)
	run.Attributes = map[string]string{}
	if err := json.Unmarshal(attributes, &run.Attributes); err != nil {
		return worlds.Run{}, err
	}
	if len(executionContext) > 0 {
		if err := json.Unmarshal(executionContext, &run.ExecutionContext); err != nil {
			return worlds.Run{}, err
		}
	}
	run.CreatedAt, run.UpdatedAt = run.CreatedAt.UTC(), run.UpdatedAt.UTC()
	run.StartedAt, run.CompletedAt, run.ExpiredAt = utc(run.StartedAt), utc(run.CompletedAt), utc(run.ExpiredAt)
	return run, nil
}

func statusStrings(values []worlds.Status) []string {
	result := make([]string, len(values))
	for i, value := range values {
		result[i] = string(value)
	}
	return result
}

func (w *World) RunsDeleteDataMany(ctx context.Context, input worlds.RunsDeleteDataManyInput) (worlds.RunsDeleteDataManyResult, error) {
	if err := worlds.CheckDeleteDataInput(input); err != nil {
		return worlds.RunsDeleteDataManyResult{}, err
	}
	results := make([]worlds.DeleteDataResult, len(input.RunIDs))
	for index, id := range input.RunIDs {
		results[index] = w.deleteRunData(ctx, id)
	}
	return worlds.NewDeleteDataResult(results), nil
}

// deleteRunData deletes one run's data in its own transaction, so a failure
// leaves the other runs of the request deleted.
func (w *World) deleteRunData(ctx context.Context, id worlds.RunID) worlds.DeleteDataResult {
	result := worlds.DeleteDataResult{RunID: id}
	err := w.inTx(ctx, func(tx pgx.Tx) error {
		var status worlds.Status
		var expired, purged bool
		err := tx.QueryRow(ctx, `SELECT status, coalesce(expired_at <= now(), false), data_purged FROM go_workflow_runs WHERE run_id = $1 FOR UPDATE`, id).Scan(&status, &expired, &purged)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			result.Outcome = worlds.DeleteDataNotFound
			return nil
		case err != nil:
			return err
		case !status.Terminal():
			result.Outcome, result.Reason, result.Status = worlds.DeleteDataSkipped, worlds.DeleteDataRunNotTerminal, status
			return nil
		}
		result.Outcome = worlds.DeleteDataDeleted
		if expired {
			result.Outcome = worlds.DeleteDataAlreadyDeleted
		}
		return purgeRunData(ctx, tx, id)
	})
	if err != nil {
		return worlds.DeleteDataResult{RunID: id, Outcome: worlds.DeleteDataFailed, Code: "storage_error", Retryable: ctx.Err() == nil}
	}
	return result
}
