package postgresworld

import (
	"context"
	"errors"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
	"github.com/jackc/pgx/v5"
)

func (w *World) StepsGet(ctx context.Context, input worlds.StepsGetInput) (worlds.Step, error) {
	step, err := scanStep(w.pool.QueryRow(ctx, w.q(stepSelect(input.ResolveData)+` WHERE s.run_id = $1 AND s.step_id = $2`), input.RunID, input.StepID))
	if errors.Is(err, pgx.ErrNoRows) {
		return worlds.Step{}, worlds.ErrStepNotFound
	}
	return step, err
}

func (w *World) StepsList(ctx context.Context, input worlds.StepsListInput) (worlds.Page[worlds.Step], error) {
	// The list's defaults are world-postgres's.
	input.Pagination = input.Pagination.WithDefaults(worlds.SortOrderDescending, 20)
	var q query
	q.where("s.run_id = " + q.arg(input.RunID))
	descending := input.Pagination.SortOrder == worlds.SortOrderDescending
	if err := q.keyset(input.Pagination.Cursor, "s.created_at", "s.step_id", descending); err != nil {
		return worlds.Page[worlds.Step]{}, err
	}
	rows, err := w.pool.Query(ctx, w.q(stepSelect(input.ResolveData)+q.clause()+order("s.created_at", "s.step_id", descending)+q.limit(input.Pagination.Limit)), q.args...)
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
// log: the input from the event that created the step, the output from its
// step_completed, and the error from its latest step_failed or
// step_retrying. ResolveDataNone joins only the error.
func stepSelect(resolve worlds.ResolveData) string {
	input, output := "ie.type, ie.payload_cbor", "oe.payload_cbor"
	join := " LEFT JOIN workflow.workflow_events ie ON ie.run_id = s.run_id AND ie.id = " + slotID("s.input_slot") + runLive("s.run_id") +
		" LEFT JOIN workflow.workflow_events oe ON oe.run_id = s.run_id AND oe.id = " + slotID("s.output_slot") + runLive("s.run_id")
	if resolve == worlds.ResolveDataNone {
		input, output, join = "NULL::varchar, NULL::bytea", "NULL::bytea", ""
	}
	return "SELECT s.run_id, s.step_id, s.step_name, s.status::text, " + input + ", " + output + ", ee.type, ee.payload_cbor, s.attempt, s.created_at, s.updated_at, s.started_at, s.completed_at, s.retry_after, coalesce(s.spec_version, 0)" +
		" FROM workflow.workflow_steps s" + join + " LEFT JOIN workflow.workflow_events ee ON ee.run_id = s.run_id AND ee.id = " + slotID("s.error_slot") + runLive("s.run_id")
}

func scanStep(row pgx.Row) (worlds.Step, error) {
	var step worlds.Step
	var input, output, failure []byte
	var inputType, errorType *worlds.EventType
	if err := row.Scan(&step.RunID, &step.StepID, &step.StepName, &step.Status, &inputType, &input, &output, &errorType, &failure, &step.Attempt, &step.CreatedAt, &step.UpdatedAt, &step.StartedAt, &step.CompletedAt, &step.RetryAfter, &step.SpecVersion); err != nil {
		return worlds.Step{}, err
	}
	if inputType != nil && input != nil {
		// The input slot holds a step_created, or the step_started that
		// created an inline step.
		data, err := eventData(*inputType, input)
		if err != nil {
			return worlds.Step{}, err
		}
		step.Input = worlds.EventPayload(worlds.Event{EventType: *inputType, Data: data})
	}
	if output != nil {
		data, err := eventData(worlds.EventTypeStepCompleted, output)
		if err != nil {
			return worlds.Step{}, err
		}
		step.Output = worlds.EventPayload(worlds.Event{EventType: worlds.EventTypeStepCompleted, Data: data})
	}
	if errorType != nil && failure != nil {
		data, err := eventData(*errorType, failure)
		if err != nil {
			return worlds.Step{}, err
		}
		step.Error = worlds.EventPayload(worlds.Event{EventType: *errorType, Data: data})
	}
	step.CreatedAt, step.UpdatedAt = step.CreatedAt.UTC(), step.UpdatedAt.UTC()
	step.StartedAt, step.CompletedAt, step.RetryAfter = utc(step.StartedAt), utc(step.CompletedAt), utc(step.RetryAfter)
	return step, nil
}

func (w *World) getStep(ctx context.Context, db pgx.Tx, runID worlds.RunID, stepID worlds.StepID) (worlds.Step, error) {
	step, err := scanStep(db.QueryRow(ctx, w.q(stepSelect(worlds.ResolveDataAll)+` WHERE s.run_id = $1 AND s.step_id = $2`), runID, stepID))
	if errors.Is(err, pgx.ErrNoRows) {
		return worlds.Step{}, worlds.ErrStepNotFound
	}
	return step, err
}
