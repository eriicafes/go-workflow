package postgresworld

import (
	"context"
	"errors"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
	"github.com/jackc/pgx/v5"
)

func (w *World) StepsGet(ctx context.Context, input worlds.StepsGetInput) (worlds.Step, error) {
	step, err := scanStep(w.pool.QueryRow(ctx, `SELECT `+stepColumns(input.ResolveData)+` FROM go_workflow_steps WHERE run_id = $1 AND step_id = $2`, input.RunID, input.StepID))
	if errors.Is(err, pgx.ErrNoRows) {
		return worlds.Step{}, worlds.ErrStepNotFound
	}
	return step, err
}

func (w *World) StepsList(ctx context.Context, input worlds.StepsListInput) (worlds.Page[worlds.Step], error) {
	var q query
	q.where("run_id = " + q.arg(input.RunID))
	if err := q.keyset(input.Pagination.Cursor, "created_at", "step_id", false); err != nil {
		return worlds.Page[worlds.Step]{}, err
	}
	rows, err := w.pool.Query(ctx, `SELECT `+stepColumns(input.ResolveData)+` FROM go_workflow_steps`+q.clause()+order("created_at", "step_id", false)+q.limit(input.Pagination.Limit), q.args...)
	if err != nil {
		return worlds.Page[worlds.Step]{}, err
	}
	steps, err := collect(rows, scanStep)
	if err != nil {
		return worlds.Page[worlds.Step]{}, err
	}
	return keysetPage(steps, input.Pagination.Limit, func(step worlds.Step) (time.Time, string) { return step.CreatedAt, string(step.StepID) }), nil
}

func stepColumns(resolve worlds.ResolveData) string {
	data := "input, output"
	if resolve == worlds.ResolveDataNone {
		data = "NULL::bytea, NULL::bytea"
	}
	return "run_id, step_id, step_name, status, " + data + ", error, attempt, created_at, updated_at, started_at, completed_at, retry_after"
}

func scanStep(row pgx.Row) (worlds.Step, error) {
	var step worlds.Step
	if err := row.Scan(&step.RunID, &step.StepID, &step.StepName, &step.Status, &step.Input, &step.Output, &step.Error, &step.Attempt, &step.CreatedAt, &step.UpdatedAt, &step.StartedAt, &step.CompletedAt, &step.RetryAfter); err != nil {
		return worlds.Step{}, err
	}
	step.CreatedAt, step.UpdatedAt = step.CreatedAt.UTC(), step.UpdatedAt.UTC()
	step.StartedAt, step.CompletedAt, step.RetryAfter = utc(step.StartedAt), utc(step.CompletedAt), utc(step.RetryAfter)
	return step, nil
}

func getStep(ctx context.Context, db pgx.Tx, runID worlds.RunID, stepID worlds.StepID) (worlds.Step, error) {
	step, err := scanStep(db.QueryRow(ctx, `SELECT `+stepColumns(worlds.ResolveDataAll)+` FROM go_workflow_steps WHERE run_id = $1 AND step_id = $2`, runID, stepID))
	if errors.Is(err, pgx.ErrNoRows) {
		return worlds.Step{}, worlds.ErrStepNotFound
	}
	return step, err
}
