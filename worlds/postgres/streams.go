package postgresworld

import (
	"context"
	"errors"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/workflowops"
	"github.com/jackc/pgx/v5"
)

// ReadStream reads bytes at offset from the stream's durable byte buffer.
func (world *World) ReadStream(ctx context.Context, reference workflowops.Stream, offset int64) (workflowops.StreamRead, error) {
	var exists bool
	if err := world.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM go_workflow_runs WHERE id = $1)`, reference.RunID).Scan(&exists); err != nil {
		return workflowops.StreamRead{}, err
	}
	if !exists {
		return workflowops.StreamRead{}, workflow.ErrRunNotFound
	}
	var data []byte
	var closed bool
	err := world.pool.QueryRow(
		ctx,
		`SELECT data, closed FROM go_workflow_streams WHERE run_id = $1 AND namespace = $2`,
		reference.RunID,
		reference.Namespace,
	).Scan(&data, &closed)
	if errors.Is(err, pgx.ErrNoRows) {
		return workflowops.StreamRead{Next: offset}, nil
	}
	if err != nil {
		return workflowops.StreamRead{}, err
	}
	if offset < 0 {
		offset = max(int64(len(data))+offset, 0)
	}
	if offset > int64(len(data)) {
		offset = int64(len(data))
	}
	return workflowops.StreamRead{
		Data:   append([]byte(nil), data[offset:]...),
		Next:   int64(len(data)),
		Closed: closed,
	}, nil
}

// LockStream acquires the stream's exclusive writer lock.
func (world *World) LockStream(ctx context.Context, reference workflowops.Stream, token string) (bool, error) {
	var closed bool
	err := world.pool.QueryRow(
		ctx,
		`SELECT closed FROM go_workflow_streams WHERE run_id = $1 AND namespace = $2`,
		reference.RunID,
		reference.Namespace,
	).Scan(&closed)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return false, err
	}
	if closed {
		return false, workflowops.ErrCancelled
	}
	var acquired bool
	err = world.pool.QueryRow(
		ctx,
		`INSERT INTO go_workflow_stream_locks (run_id, namespace, token)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (run_id, namespace) DO NOTHING
		 RETURNING true`,
		reference.RunID,
		reference.Namespace,
		token,
	).Scan(&acquired)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return acquired, err
}

// AppendStream atomically appends bytes held by token.
func (world *World) AppendStream(ctx context.Context, reference workflowops.Stream, token string, data []byte) error {
	var written bool
	err := world.pool.QueryRow(
		ctx,
		`INSERT INTO go_workflow_streams (run_id, namespace, data)
		 SELECT $1, $2, $3
		 WHERE EXISTS (
		   SELECT 1 FROM go_workflow_stream_locks
		   WHERE run_id = $1 AND namespace = $2 AND token = $4
		 )
		 ON CONFLICT (run_id, namespace) DO UPDATE
		 SET data = go_workflow_streams.data || EXCLUDED.data, updated_at = now()
		 WHERE NOT go_workflow_streams.closed
		   AND EXISTS (
		     SELECT 1 FROM go_workflow_stream_locks
		     WHERE run_id = $1 AND namespace = $2 AND token = $4
		   )
		 RETURNING true`,
		reference.RunID,
		reference.Namespace,
		data,
		token,
	).Scan(&written)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return workflowops.ErrCancelled
		}
		return err
	}
	return nil
}

// ReleaseStream releases token's writer lock and can make a stream terminal.
func (world *World) ReleaseStream(ctx context.Context, reference workflowops.Stream, token string, closeStream bool) error {
	tx, err := world.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var released bool
	err = tx.QueryRow(
		ctx,
		`DELETE FROM go_workflow_stream_locks
		 WHERE run_id = $1 AND namespace = $2 AND token = $3
		 RETURNING true`,
		reference.RunID,
		reference.Namespace,
		token,
	).Scan(&released)
	if errors.Is(err, pgx.ErrNoRows) {
		return workflowops.ErrCancelled
	}
	if err != nil {
		return err
	}
	if closeStream {
		if _, err := tx.Exec(
			ctx,
			`INSERT INTO go_workflow_streams (run_id, namespace, closed)
			 VALUES ($1, $2, true)
			 ON CONFLICT (run_id, namespace) DO UPDATE
			 SET closed = true, updated_at = now()`,
			reference.RunID,
			reference.Namespace,
		); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// CloseStreams closes all streams that belong to a terminal run.
func (world *World) CloseStreams(ctx context.Context, runID string) error {
	if _, err := world.pool.Exec(ctx, `DELETE FROM go_workflow_stream_locks WHERE run_id = $1`, runID); err != nil {
		return err
	}
	_, err := world.pool.Exec(
		ctx,
		`UPDATE go_workflow_streams SET closed = true, updated_at = now() WHERE run_id = $1`,
		runID,
	)
	return err
}

func closeStreamsTx(ctx context.Context, tx pgx.Tx, runID string) error {
	_, err := tx.Exec(
		ctx,
		`UPDATE go_workflow_streams SET closed = true, updated_at = now() WHERE run_id = $1`,
		runID,
	)
	return err
}
