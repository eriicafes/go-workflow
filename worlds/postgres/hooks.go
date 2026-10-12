package postgresworld

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/eriicafes/go-workflow/workflowops"
	"github.com/jackc/pgx/v5"
)

// GetHook looks up a claimed token until its retention expiry.
func (world *World) GetHook(ctx context.Context, token string) (workflowops.Hook, bool, error) {
	var record workflowops.Hook
	var expires *time.Time
	var webhook json.RawMessage
	err := world.pool.QueryRow(
		ctx,
		`SELECT run_id, sequence, expires_at, webhook FROM go_workflow_hooks WHERE token = $1`,
		token,
	).Scan(&record.Task.RunID, &record.Task.Sequence, &expires, &webhook)
	if errors.Is(err, pgx.ErrNoRows) {
		return workflowops.Hook{}, false, nil
	}
	if err != nil {
		return workflowops.Hook{}, false, err
	}
	if expires != nil {
		record.ExpiresAt = *expires
		if !time.Now().Before(*expires) {
			_, err = world.pool.Exec(ctx, `DELETE FROM go_workflow_hooks WHERE token = $1 AND expires_at <= now()`, token)
			return workflowops.Hook{}, false, err
		}
	}
	record.Token, record.Registered = token, true
	if len(webhook) != 0 {
		if err := json.Unmarshal(webhook, &record.Webhook); err != nil {
			return workflowops.Hook{}, false, err
		}
	}
	return record, true, nil
}

// RegisterHook atomically claims a token for the current root replay.
func (world *World) RegisterHook(ctx context.Context, lease *workflowops.Lease, request workflowops.HookRequest) (workflowops.Hook, error) {
	if lease == nil || request.Token == "" || request.Task.RunID != lease.ID {
		return workflowops.Hook{}, workflowops.ErrLeaseLost
	}
	tx, err := world.pool.Begin(ctx)
	if err != nil {
		return workflowops.Hook{}, err
	}
	defer tx.Rollback(ctx)
	if err := checkLease(ctx, tx, lease); err != nil {
		return workflowops.Hook{}, err
	}
	var owner workflowops.Task
	var expires *time.Time
	var webhook json.RawMessage
	err = tx.QueryRow(
		ctx,
		`SELECT run_id, sequence, expires_at, webhook FROM go_workflow_hooks WHERE token = $1 FOR UPDATE`,
		request.Token,
	).Scan(&owner.RunID, &owner.Sequence, &expires, &webhook)
	if err == nil && expires != nil && !time.Now().Before(*expires) {
		if _, err = tx.Exec(ctx, `DELETE FROM go_workflow_hooks WHERE token = $1`, request.Token); err != nil {
			return workflowops.Hook{}, err
		}
		err = pgx.ErrNoRows
	}
	if err == nil && owner.RunID == request.Task.RunID && owner.Sequence != request.Task.Sequence {
		var status workflowops.Status
		if err = tx.QueryRow(
			ctx,
			`SELECT status FROM workflow_tasks WHERE run_id = $1 AND sequence = $2`,
			owner.RunID,
			owner.Sequence,
		).Scan(&status); err != nil {
			return workflowops.Hook{}, err
		}
		if status == workflowops.StatusCompleted || status == workflowops.StatusFailed || status == workflowops.StatusCancelled {
			if _, err = tx.Exec(ctx, `DELETE FROM go_workflow_hooks WHERE token = $1`, request.Token); err != nil {
				return workflowops.Hook{}, err
			}
			err = pgx.ErrNoRows
		}
	}
	if err == nil && owner == request.Task {
		value := workflowops.Hook{Token: request.Token, Task: request.Task, Registered: true}
		if expires != nil {
			value.ExpiresAt = *expires
		}
		if len(webhook) != 0 {
			if err := json.Unmarshal(webhook, &value.Webhook); err != nil {
				return workflowops.Hook{}, err
			}
		}
		return value, tx.Commit(ctx)
	}
	if err == nil && !request.Force {
		return workflowops.Hook{Token: request.Token, Task: request.Task, Conflict: &owner, Registered: true}, tx.Commit(ctx)
	}
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return workflowops.Hook{}, err
	}
	if err == nil {
		if err = forceClaimHook(ctx, tx, owner); err != nil {
			return workflowops.Hook{}, err
		}
		if _, err = tx.Exec(ctx, `DELETE FROM go_workflow_hooks WHERE token = $1`, request.Token); err != nil {
			return workflowops.Hook{}, err
		}
	}
	encodedWebhook, err := json.Marshal(request.Webhook)
	if err != nil {
		return workflowops.Hook{}, err
	}
	_, err = tx.Exec(
		ctx,
		`INSERT INTO go_workflow_hooks (token, run_id, sequence, retention_ns, webhook)
		 VALUES ($1, $2, $3, $4, $5)`,
		request.Token,
		request.Task.RunID,
		request.Task.Sequence,
		request.MinRetention.Nanoseconds(),
		encodedWebhook,
	)
	if err != nil {
		return workflowops.Hook{}, err
	}
	return workflowops.Hook{Token: request.Token, Task: request.Task, Registered: true}, tx.Commit(ctx)
}

// ReplyWebhook records the response for a manual webhook.
func (world *World) ReplyWebhook(ctx context.Context, token string, response json.RawMessage) error {
	command, err := world.pool.Exec(
		ctx,
		`UPDATE go_workflow_hooks SET response = COALESCE(response, $2::jsonb)
		 WHERE token = $1 AND webhook->>'manual' = 'true'`,
		token,
		response,
	)
	if err != nil {
		return err
	}
	if command.RowsAffected() == 0 {
		return workflowops.ErrHookNotFound
	}
	return nil
}

// WaitWebhookResponse waits for a response recorded by ReplyWebhook.
func (world *World) WaitWebhookResponse(ctx context.Context, token string) (json.RawMessage, error) {
	for {
		var raw json.RawMessage
		var manual bool
		err := world.pool.QueryRow(
			ctx,
			`SELECT response, COALESCE((webhook->>'manual')::boolean, false)
			 FROM go_workflow_hooks WHERE token = $1`,
			token,
		).Scan(&raw, &manual)
		if errors.Is(err, pgx.ErrNoRows) || !manual {
			return nil, workflowops.ErrHookNotFound
		}
		if err != nil {
			return nil, err
		}
		if len(raw) != 0 && string(raw) != "null" {
			return raw, nil
		}
		timer := time.NewTimer(25 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

// ResumeWebhook completes the owning hook operation and queues its suspended root.
func (world *World) ResumeWebhook(ctx context.Context, token string, payload json.RawMessage) error {
	tx, err := world.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var task workflowops.Task
	var retention int64
	var expires *time.Time
	err = tx.QueryRow(
		ctx,
		`SELECT run_id, sequence, retention_ns, expires_at FROM go_workflow_hooks WHERE token = $1 FOR UPDATE`,
		token,
	).Scan(&task.RunID, &task.Sequence, &retention, &expires)
	if errors.Is(err, pgx.ErrNoRows) {
		return workflowops.ErrHookNotFound
	}
	if err != nil {
		return err
	}
	if expires != nil && !time.Now().Before(*expires) {
		_, _ = tx.Exec(ctx, `DELETE FROM go_workflow_hooks WHERE token = $1`, token)
		return workflowops.ErrHookNotFound
	}
	command, err := tx.Exec(
		ctx,
		`UPDATE workflow_tasks SET status = $3, output = $4, error = NULL, updated_at = now()
		 WHERE run_id = $1 AND sequence = $2 AND status = $5`,
		task.RunID,
		task.Sequence,
		string(workflowops.StatusCompleted),
		payload,
		string(workflowops.StatusWaiting),
	)
	if err != nil {
		return err
	}
	if command.RowsAffected() != 1 {
		var status workflowops.Status
		err = tx.QueryRow(
			ctx,
			`SELECT status FROM workflow_tasks WHERE run_id = $1 AND sequence = $2`,
			task.RunID,
			task.Sequence,
		).Scan(&status)
		if err != nil {
			return err
		}
		if status == workflowops.StatusCompleted {
			return tx.Commit(ctx)
		}
		return workflowops.ErrHookNotFound
	}
	if _, err = tx.Exec(
		ctx,
		`INSERT INTO workflow_completion_events (run_id, sequence) VALUES ($1, $2)
		 ON CONFLICT (run_id, sequence) DO NOTHING`,
		task.RunID,
		task.Sequence,
	); err != nil {
		return err
	}
	if retention > 0 {
		if _, err = tx.Exec(ctx, `UPDATE go_workflow_hooks SET expires_at = now() + ($2 * interval '1 nanosecond') WHERE token = $1`, token, retention); err != nil {
			return err
		}
	}
	if _, err = tx.Exec(
		ctx,
		`UPDATE workflow_tasks SET status = $2, updated_at = now()
		 WHERE run_id = $1 AND sequence = $3 AND status = $4`,
		task.RunID,
		string(workflowops.StatusQueued),
		workflowops.RootSequence,
		string(workflowops.StatusWaiting),
	); err != nil {
		return err
	}
	if _, err = tx.Exec(
		ctx,
		`UPDATE go_workflow_runs SET status = $2, updated_at = now()
		 WHERE id = $1 AND status = $3`,
		task.RunID,
		string(workflowops.StatusQueued),
		string(workflowops.StatusWaiting),
	); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `SELECT pg_notify('go_workflow_jobs', $1)`, task.RunID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func forceClaimHook(ctx context.Context, tx pgx.Tx, owner workflowops.Task) error {
	_, err := tx.Exec(
		ctx,
		`UPDATE workflow_tasks SET status = $3, error = $4, output = NULL, updated_at = now()
		 WHERE run_id = $1 AND sequence = $2 AND status = $5`,
		owner.RunID,
		owner.Sequence,
		string(workflowops.StatusFailed),
		workflowops.EncodeFailure(workflowops.ErrHookForceClaimed),
		string(workflowops.StatusWaiting),
	)
	if err != nil {
		return err
	}
	_, err = tx.Exec(
		ctx,
		`UPDATE workflow_tasks SET status = $2, updated_at = now()
		 WHERE run_id = $1 AND sequence = $3 AND status = $4`,
		owner.RunID,
		string(workflowops.StatusQueued),
		workflowops.RootSequence,
		string(workflowops.StatusWaiting),
	)
	if err != nil {
		return err
	}
	_, err = tx.Exec(
		ctx,
		`UPDATE go_workflow_runs SET status = $2, updated_at = now() WHERE id = $1 AND status = $3`,
		owner.RunID,
		string(workflowops.StatusQueued),
		string(workflowops.StatusWaiting),
	)
	return err
}
