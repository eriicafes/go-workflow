package postgresworld

import (
	"context"
	"errors"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
	"github.com/jackc/pgx/v5"
)

func (w *World) StepsGet(ctx context.Context, input worlds.StepsGetInput) (worlds.Step, error) {
	step, err := scanStep(w.pool.QueryRow(ctx, stepSelect(input.ResolveData)+` WHERE s.run_id = $1 AND s.step_id = $2`, input.RunID, input.StepID))
	if errors.Is(err, pgx.ErrNoRows) {
		return worlds.Step{}, worlds.ErrStepNotFound
	}
	return step, err
}

func (w *World) StepsList(ctx context.Context, input worlds.StepsListInput) (worlds.Page[worlds.Step], error) {
	var q query
	q.where("s.run_id = " + q.arg(input.RunID))
	if err := q.keyset(input.Pagination.Cursor, "s.created_at", "s.step_id", false); err != nil {
		return worlds.Page[worlds.Step]{}, err
	}
	rows, err := w.pool.Query(ctx, stepSelect(input.ResolveData)+q.clause()+order("s.created_at", "s.step_id", false)+q.limit(input.Pagination.Limit), q.args...)
	if err != nil {
		return worlds.Page[worlds.Step]{}, err
	}
	steps, err := collect(rows, scanStep)
	if err != nil {
		return worlds.Page[worlds.Step]{}, err
	}
	return keysetPage(steps, input.Pagination.Limit, func(step worlds.Step) (time.Time, string) { return step.CreatedAt, string(step.StepID) }), nil
}

// stepSelect selects steps as s with their payloads joined from the event
// log: the input from step_created and the output or error from the latest
// result. ResolveDataNone joins only an error result.
func stepSelect(resolve worlds.ResolveData) string {
	input, result, join := "ie.data", "re.data", " LEFT JOIN go_workflow_events ie ON ie.run_id = s.run_id AND ie.slot = s.created_slot"
	if resolve == worlds.ResolveDataNone {
		input, result, join = "NULL::bytea", "CASE WHEN re.type <> '"+string(worlds.EventTypeStepCompleted)+"' THEN re.data END", ""
	}
	return "SELECT s.run_id, s.step_id, s.step_name, s.status, " + input + ", re.type, " + result + ", s.attempt, s.created_at, s.updated_at, s.started_at, s.completed_at, s.retry_after" +
		" FROM go_workflow_steps s" + join + " LEFT JOIN go_workflow_events re ON re.run_id = s.run_id AND re.slot = s.result_slot"
}

func scanStep(row pgx.Row) (worlds.Step, error) {
	var step worlds.Step
	var input, result []byte
	var resultType *worlds.EventType
	if err := row.Scan(&step.RunID, &step.StepID, &step.StepName, &step.Status, &input, &resultType, &result, &step.Attempt, &step.CreatedAt, &step.UpdatedAt, &step.StartedAt, &step.CompletedAt, &step.RetryAfter); err != nil {
		return worlds.Step{}, err
	}
	if input != nil {
		step.Input = worlds.EventPayload(worlds.Event{EventType: worlds.EventTypeStepCreated, Data: input})
	}
	if resultType != nil && result != nil {
		payload := worlds.EventPayload(worlds.Event{EventType: *resultType, Data: result})
		if *resultType == worlds.EventTypeStepCompleted {
			step.Output = payload
		} else {
			step.Error = payload
		}
	}
	step.CreatedAt, step.UpdatedAt = step.CreatedAt.UTC(), step.UpdatedAt.UTC()
	step.StartedAt, step.CompletedAt, step.RetryAfter = utc(step.StartedAt), utc(step.CompletedAt), utc(step.RetryAfter)
	return step, nil
}

func getStep(ctx context.Context, db pgx.Tx, runID worlds.RunID, stepID worlds.StepID) (worlds.Step, error) {
	step, err := scanStep(db.QueryRow(ctx, stepSelect(worlds.ResolveDataAll)+` WHERE s.run_id = $1 AND s.step_id = $2`, runID, stepID))
	if errors.Is(err, pgx.ErrNoRows) {
		return worlds.Step{}, worlds.ErrStepNotFound
	}
	return step, err
}
