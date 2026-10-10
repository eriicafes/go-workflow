// Package postgresworld provides the Postgres workflow world.
package postgresworld

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/workflowops"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Config struct {
	// URL is the PostgreSQL connection string used by Open. New accepts an
	// existing pool instead and ignores URL.
	URL string
	// PollInterval controls how often WaitRun, WaitOperation, and Run poll when
	// no task or terminal state is immediately available. Zero defaults to 250ms.
	PollInterval time.Duration
	// LeaseDuration is the time a worker owns a claimed run before another
	// worker may reclaim it. Zero defaults to 5 minutes.
	LeaseDuration time.Duration
}

type World struct {
	pool            *pgxpool.Pool
	ownsPool        bool
	interval, lease time.Duration
}

func Open(ctx context.Context, config Config) (*World, error) {
	if config.URL == "" {
		return nil, errors.New("workflow/postgres: URL is required")
	}
	pool, err := pgxpool.New(ctx, config.URL)
	if err != nil {
		return nil, fmt.Errorf("workflow/postgres: open: %w", err)
	}
	world, err := New(ctx, pool, config)
	if err != nil {
		pool.Close()
		return nil, err
	}
	world.ownsPool = true
	return world, nil
}

// New uses an existing pgx pool. The caller retains ownership of pool, so
// Close on the returned world does not close that shared connection pool.
func New(ctx context.Context, pool *pgxpool.Pool, config Config) (*World, error) {
	if pool == nil {
		return nil, errors.New("workflow/postgres: pool is required")
	}
	world := &World{
		pool:     pool,
		interval: config.PollInterval,
		lease:    config.LeaseDuration,
	}
	if world.interval <= 0 {
		world.interval = 250 * time.Millisecond
	}
	if world.lease <= 0 {
		world.lease = 5 * time.Minute
	}
	if err := world.migrate(ctx); err != nil {
		return nil, err
	}
	return world, nil
}

func (world *World) Close() {
	if world.ownsPool {
		world.pool.Close()
	}
}

func (world *World) PollInterval() time.Duration {
	return world.interval
}

func (world *World) LeaseDuration() time.Duration {
	return world.lease
}

var schema = fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS go_workflow_runs (
  id text PRIMARY KEY,
  workflow_identity text NOT NULL,
  input jsonb NOT NULL,
  output jsonb,
  status text NOT NULL CHECK (status IN (%s, %s, %s, %s, %s)),
  error text,
  lease_until timestamptz,
  lease_token text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS go_workflow_runs_queue_idx ON go_workflow_runs (created_at) WHERE status = %s;

CREATE TABLE IF NOT EXISTS workflow_commands (
  run_id text NOT NULL REFERENCES go_workflow_runs(id) ON DELETE CASCADE,
  sequence integer NOT NULL,
  kind text NOT NULL,
  identity text NOT NULL,
  fingerprint text NOT NULL,
  input jsonb NOT NULL,
  input_hash text NOT NULL,
  child_run_id text,
  due_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, sequence)
);

CREATE TABLE IF NOT EXISTS workflow_tasks (
  run_id text NOT NULL,
  sequence integer NOT NULL,
  status text NOT NULL CHECK (status IN (%s, %s, %s, %s, %s, %s, %s)),
  output jsonb,
  error text,
  due_at timestamptz,
  lease_until timestamptz,
  lease_token text,
  attempt integer NOT NULL DEFAULT 0,
  idempotency_key text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, sequence),
  FOREIGN KEY (run_id, sequence)
    REFERENCES workflow_commands(run_id, sequence) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS workflow_tasks_claim_idx
  ON workflow_tasks(status, due_at, lease_until);

CREATE TABLE IF NOT EXISTS workflow_wait_dependencies (
  wait_run_id text NOT NULL,
  wait_sequence integer NOT NULL,
  position integer NOT NULL,
  dependency_run_id text NOT NULL,
  dependency_sequence integer NOT NULL,
  PRIMARY KEY (wait_run_id, wait_sequence, position),
  FOREIGN KEY (wait_run_id, wait_sequence)
    REFERENCES workflow_tasks(run_id, sequence) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS workflow_wait_dependencies_source_idx
  ON workflow_wait_dependencies(dependency_run_id, dependency_sequence);

CREATE TABLE IF NOT EXISTS workflow_completion_events (
  ordinal bigserial PRIMARY KEY,
  run_id text NOT NULL,
  sequence integer NOT NULL,
  UNIQUE (run_id, sequence)
);

CREATE TABLE IF NOT EXISTS go_workflow_events (
  id bigserial PRIMARY KEY,
  run_id text NOT NULL REFERENCES go_workflow_runs(id) ON DELETE CASCADE,
  operation_sequence integer,
  data bytea NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS go_workflow_events_operation_idx
  ON go_workflow_events(run_id, operation_sequence)
  WHERE operation_sequence IS NOT NULL;
`,
	statusLiteral(workflowops.StatusQueued),
	statusLiteral(workflowops.StatusRunning),
	statusLiteral(workflowops.StatusWaiting),
	statusLiteral(workflowops.StatusCompleted),
	statusLiteral(workflowops.StatusFailed),
	statusLiteral(workflowops.StatusQueued),
	statusLiteral(workflowops.StatusQueued),
	statusLiteral(workflowops.StatusRunning),
	statusLiteral(workflowops.StatusWaiting),
	statusLiteral(workflowops.StatusRetryWait),
	statusLiteral(workflowops.StatusCompleted),
	statusLiteral(workflowops.StatusFailed),
	statusLiteral(workflowops.StatusCancelled),
)

func (world *World) migrate(ctx context.Context) error {
	_, err := world.pool.Exec(ctx, schema)
	return err
}

func (world *World) createRoot(ctx context.Context, request workflowops.RunRequest) (string, error) {
	if request.Sequence != workflowops.RootSequence {
		return "", fmt.Errorf("workflow/postgres: invalid root sequence %d", request.Sequence)
	}
	if request.Kind != "" && request.Kind != workflowops.KindRun {
		return "", fmt.Errorf("workflow/postgres: invalid root kind %q", request.Kind)
	}
	if request.Fingerprint == "" {
		request.Fingerprint = request.Identity
	}
	id, err := workflowops.NewRunID()
	if err != nil {
		return "", err
	}
	tx, err := world.pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)
	root := workflowops.CommandMetadata{
		Kind:        workflowops.KindRun,
		Identity:    request.Identity,
		Fingerprint: request.Identity,
		Input:       request.Input,
	}
	if _, err = tx.Exec(
		ctx,
		`INSERT INTO go_workflow_runs (id, workflow_identity, input, status)
		 VALUES ($1, $2, $3, $4)`,
		id,
		request.Identity,
		request.Input,
		string(workflowops.StatusQueued),
	); err != nil {
		return "", err
	}
	if _, err = tx.Exec(
		ctx,
		`INSERT INTO workflow_commands
			 (run_id, sequence, kind, identity, fingerprint, input, input_hash)
			 VALUES ($1, $2, $3, $4, $4, $5, md5($5::text))`,
		id,
		workflowops.RootSequence,
		string(root.Kind),
		root.Identity,
		root.Input,
	); err != nil {
		return "", err
	}
	if _, err = tx.Exec(
		ctx,
		`INSERT INTO workflow_tasks (run_id, sequence, status, idempotency_key)
		 VALUES ($1, -1, $2, $3)`,
		id,
		string(workflowops.StatusQueued),
		id+":"+strconv.Itoa(workflowops.RootSequence),
	); err != nil {
		return "", err
	}
	if _, err = tx.Exec(ctx, `SELECT pg_notify('go_workflow_jobs',$1)`, id); err != nil {
		return "", err
	}
	return id, tx.Commit(ctx)
}

func (world *World) CreateRun(
	ctx context.Context,
	lease *workflowops.LeasedRun,
	request workflowops.RunRequest,
) (string, error) {
	if lease == nil {
		return world.createRoot(ctx, request)
	}
	tx, err := world.pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)
	var kind, identity, fingerprint, childID string
	var input []byte
	err = tx.QueryRow(
		ctx,
		`SELECT kind, identity, fingerprint, input, child_run_id
		 FROM workflow_commands
		 WHERE run_id = $1 AND sequence = $2`,
		request.RunID,
		request.Sequence,
	).Scan(&kind, &identity, &fingerprint, &input, &childID)
	if err == nil {
		if !commandMetadata(kind, identity, fingerprint, input, childID).MatchesRun(request) || childID == "" {
			return "", workflowops.HistoryMismatchError("nested run", request.Sequence)
		}
		return childID, tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", err
	}
	if !allowsCreate(lease, request.RunID) {
		return "", workflowops.HistoryMissingError("nested run command", request.Sequence)
	}
	childID, err = workflowops.NewRunID()
	if err != nil {
		return "", err
	}
	childCommand := workflowops.CommandMetadata{
		Kind:        workflowops.KindRun,
		Identity:    request.Identity,
		Fingerprint: request.Fingerprint,
		Input:       request.Input,
		ChildRunID:  childID,
	}
	childRoot := workflowops.CommandMetadata{
		Kind:        workflowops.KindRun,
		Identity:    request.Identity,
		Fingerprint: request.Identity,
		Input:       request.Input,
	}
	if _, err = tx.Exec(
		ctx,
		`INSERT INTO workflow_commands
		 (run_id, sequence, kind, identity, fingerprint, input, input_hash, child_run_id)
		 VALUES ($1, $2, $3, $4, $5, $6, md5($6::text), $7)`,
		request.RunID,
		request.Sequence,
		string(childCommand.Kind),
		childCommand.Identity,
		childCommand.Fingerprint,
		childCommand.Input,
		childCommand.ChildRunID,
	); err != nil {
		return "", err
	}
	if _, err = tx.Exec(
		ctx,
		`INSERT INTO go_workflow_runs (id, workflow_identity, input, status)
		 VALUES ($1, $2, $3, $4)`,
		childID,
		request.Identity,
		request.Input,
		string(workflowops.StatusQueued),
	); err != nil {
		return "", err
	}
	if _, err = tx.Exec(
		ctx,
		`INSERT INTO workflow_commands
			 (run_id, sequence, kind, identity, fingerprint, input, input_hash)
			 VALUES ($1, $2, $3, $4, $4, $5, md5($5::text))`,
		childID,
		workflowops.RootSequence,
		string(childRoot.Kind),
		childRoot.Identity,
		childRoot.Input,
	); err != nil {
		return "", err
	}
	if _, err = tx.Exec(
		ctx,
		`INSERT INTO workflow_tasks (run_id, sequence, status, idempotency_key)
		 VALUES ($1, -1, $2, $3)`,
		childID,
		string(workflowops.StatusQueued),
		childID+":"+strconv.Itoa(workflowops.RootSequence),
	); err != nil {
		return "", err
	}
	if _, err = tx.Exec(ctx, `SELECT pg_notify('go_workflow_jobs',$1)`, childID); err != nil {
		return "", err
	}
	return childID, tx.Commit(ctx)
}

func (world *World) ClaimRun(ctx context.Context) (*workflowops.LeasedRun, error) {
	token, err := workflowops.NewRunID()
	if err != nil {
		return nil, err
	}
	row := world.pool.QueryRow(
		ctx,
		`WITH candidate AS (
			SELECT run_id, sequence
			FROM workflow_tasks
			WHERE (status = $3 AND (due_at IS NULL OR due_at <= now()))
			   OR (status = $4 AND lease_until < now())
			ORDER BY created_at
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		UPDATE workflow_tasks tasks
		SET status = $4,
			lease_token = $1,
			lease_until = now() + make_interval(secs => $2),
			attempt = attempt + 1,
			updated_at = now()
		FROM candidate
		JOIN go_workflow_runs runs ON runs.id = candidate.run_id
		WHERE tasks.run_id = candidate.run_id
		  AND tasks.sequence = candidate.sequence
		RETURNING runs.id, runs.workflow_identity, runs.input,
		          tasks.lease_token, tasks.sequence`,
		token,
		world.lease.Seconds(),
		string(workflowops.StatusQueued),
		string(workflowops.StatusRunning),
	)
	var run workflowops.LeasedRun
	if err := row.Scan(&run.ID, &run.Identity, &run.Input, &run.Token, &run.Sequence); errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &run, nil
}

// ClaimTask claims one known queue-delivered task. A completed or currently
// leased task returns nil so duplicate queue delivery is safe to acknowledge.
func (world *World) ClaimTask(ctx context.Context, task workflowops.Task) (*workflowops.LeasedRun, error) {
	token, err := workflowops.NewRunID()
	if err != nil {
		return nil, err
	}
	row := world.pool.QueryRow(
		ctx,
		`UPDATE workflow_tasks tasks
		SET status = $5,
			lease_token = $1,
			lease_until = now() + make_interval(secs => $4),
			attempt = attempt + 1,
			updated_at = now()
		FROM go_workflow_runs runs
		WHERE tasks.run_id = $2
		  AND tasks.sequence = $3
		  AND runs.id = tasks.run_id
		  AND (
			(tasks.status = $6 AND (tasks.due_at IS NULL OR tasks.due_at <= now()))
			OR (tasks.status = $5 AND tasks.lease_until < now())
		  )
		RETURNING runs.id, runs.workflow_identity, runs.input,
		          tasks.lease_token, tasks.sequence`,
		token,
		task.RunID,
		task.Sequence,
		world.lease.Seconds(),
		string(workflowops.StatusRunning),
		string(workflowops.StatusQueued),
	)
	var run workflowops.LeasedRun
	if err := row.Scan(&run.ID, &run.Identity, &run.Input, &run.Token, &run.Sequence); errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &run, nil
}

func (world *World) HeartbeatRun(ctx context.Context, run *workflowops.LeasedRun) error {
	_, err := world.pool.Exec(
		ctx,
		`UPDATE workflow_tasks
		 SET lease_until = now() + make_interval(secs => $4), updated_at = now()
		 WHERE run_id = $1 AND sequence = $2 AND lease_token = $3 AND status = $5`,
		run.ID,
		run.Sequence,
		run.Token,
		world.lease.Seconds(),
		string(workflowops.StatusRunning),
	)
	return err
}

func (world *World) CompleteRun(ctx context.Context, run *workflowops.LeasedRun, output json.RawMessage) error {
	tx, err := world.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	command, err := tx.Exec(
		ctx,
		`UPDATE workflow_tasks
		 SET status = $5, output = $4, lease_until = NULL,
		     lease_token = NULL, updated_at = now()
		 WHERE run_id = $1 AND sequence = $2 AND lease_token = $3`,
		run.ID,
		run.Sequence,
		run.Token,
		output,
		string(workflowops.StatusCompleted),
	)
	if err != nil {
		return err
	}
	if command.RowsAffected() != 1 {
		return workflowops.ErrLeaseLost
	}
	if _, err = tx.Exec(
		ctx,
		`INSERT INTO workflow_completion_events (run_id, sequence)
		 VALUES ($1, $2) ON CONFLICT (run_id, sequence) DO NOTHING`,
		run.ID,
		run.Sequence,
	); err != nil {
		return err
	}
	if run.Sequence == workflowops.RootSequence {
		if _, err := tx.Exec(
			ctx,
			`UPDATE go_workflow_runs
			 SET status = $3, output = $2, lease_until = NULL,
			     lease_token = NULL, updated_at = now()
			 WHERE id = $1`,
			run.ID,
			output,
			string(workflowops.StatusCompleted),
		); err != nil {
			return err
		}
	}
	if _, err = world.resolveWaits(ctx, tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (world *World) FailRun(ctx context.Context, run *workflowops.LeasedRun, cause error) error {
	tx, err := world.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	command, err := tx.Exec(
		ctx,
		`UPDATE workflow_tasks
		 SET status = $5, error = $4, lease_until = NULL,
		     lease_token = NULL, updated_at = now()
		 WHERE run_id = $1 AND sequence = $2 AND lease_token = $3`,
		run.ID,
		run.Sequence,
		run.Token,
		cause.Error(),
		string(workflowops.StatusFailed),
	)
	if err != nil {
		return err
	}
	if command.RowsAffected() != 1 {
		return workflowops.ErrLeaseLost
	}
	if run.Sequence == workflowops.RootSequence {
		_, err = tx.Exec(
			ctx,
			`UPDATE go_workflow_runs
			 SET status = $3, error = $2, updated_at = now()
			 WHERE id = $1`,
			run.ID,
			cause.Error(),
			string(workflowops.StatusFailed),
		)
	}
	if err != nil {
		return err
	}
	if _, err = tx.Exec(
		ctx,
		`INSERT INTO workflow_completion_events (run_id, sequence)
		 VALUES ($1, $2) ON CONFLICT (run_id, sequence) DO NOTHING`,
		run.ID,
		run.Sequence,
	); err != nil {
		return err
	}
	if _, err = world.resolveWaits(ctx, tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (world *World) WaitRun(ctx context.Context, runID string) (json.RawMessage, error) {
	ticker := time.NewTicker(world.interval)
	defer ticker.Stop()
	for {
		var status string
		var output []byte
		var message *string
		err := world.pool.QueryRow(
			ctx,
			`SELECT status, output, error
			 FROM go_workflow_runs
			 WHERE id = $1`,
			runID,
		).Scan(&status, &output, &message)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, workflowops.ErrRunNotFound
		}
		if err != nil {
			return nil, err
		}
		if workflowops.Status(status) == workflowops.StatusCompleted {
			return output, nil
		}
		if workflowops.Status(status) == workflowops.StatusFailed {
			if message == nil {
				return nil, errors.New("workflow: run failed")
			}
			return nil, errors.New(*message)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (world *World) EnsureOperation(
	ctx context.Context,
	lease *workflowops.LeasedRun,
	request workflowops.OperationRequest,
) (workflowops.Operation, error) {
	var op workflowops.Operation
	var kind, identity, fingerprint string
	var input []byte
	err := world.pool.QueryRow(
		ctx,
		`SELECT commands.kind, commands.identity, commands.fingerprint, commands.input,
		        tasks.status, COALESCE(tasks.lease_token, ''),
		        COALESCE(tasks.error, ''), COALESCE(commands.due_at, now()),
		        COALESCE(tasks.lease_until, now()), tasks.idempotency_key
		 FROM workflow_commands commands
		 JOIN workflow_tasks tasks USING (run_id, sequence)
		 WHERE commands.run_id = $1 AND commands.sequence = $2`,
		request.RunID,
		request.Sequence,
	).Scan(
		&kind,
		&identity,
		&fingerprint,
		&input,
		&op.Status,
		&op.Token,
		&op.Error,
		&op.DueAt,
		&op.LeaseUntil,
		&op.IdempotencyKey,
	)
	if err == nil {
		if !commandMetadata(kind, identity, fingerprint, input, "").MatchesOperation(request) {
			return op, workflowops.HistoryMismatchError("operation", request.Sequence)
		}
		return op, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return op, err
	}
	if !allowsCreate(lease, request.RunID) {
		return op, workflowops.HistoryMissingError("command", request.Sequence)
	}
	key := fmt.Sprintf("%s:%d", request.RunID, request.Sequence)
	var due any
	if request.DueAt != nil {
		due = *request.DueAt
	}
	tx, e := world.pool.Begin(ctx)
	if e != nil {
		return op, e
	}
	defer tx.Rollback(ctx)
	metadata := workflowops.CommandMetadata{
		Kind:        request.Kind,
		Identity:    request.Identity,
		Fingerprint: request.Fingerprint,
		Input:       request.Input,
	}
	command, e := tx.Exec(
		ctx,
		`INSERT INTO workflow_commands
		 (run_id, sequence, kind, identity, fingerprint, input, input_hash, due_at)
		 VALUES ($1, $2, $3, $4, $5, $6, md5($6::text), $7)
		 ON CONFLICT (run_id, sequence) DO NOTHING`,
		request.RunID,
		request.Sequence,
		string(metadata.Kind),
		metadata.Identity,
		metadata.Fingerprint,
		metadata.Input,
		due,
	)
	if e != nil {
		return op, e
	}
	if command.RowsAffected() == 0 {
		return world.EnsureOperation(ctx, lease, request)
	}
	if _, e = tx.Exec(
		ctx,
		`INSERT INTO workflow_tasks
		 (run_id, sequence, status, due_at, idempotency_key)
		 VALUES ($1, $2, $3, $4, $5)`,
		request.RunID,
		request.Sequence,
		string(workflowops.StatusQueued),
		due,
		key,
	); e != nil {
		return op, e
	}
	if e = tx.Commit(ctx); e != nil {
		return op, e
	}
	op = workflowops.Operation{
		RunID:          request.RunID,
		Sequence:       request.Sequence,
		Status:         workflowops.StatusQueued,
		IdempotencyKey: key,
	}
	if request.DueAt != nil {
		op.DueAt = *request.DueAt
	}
	return op, nil
}

func (world *World) EnsureWait(
	ctx context.Context,
	lease *workflowops.LeasedRun,
	request workflowops.WaitRequest,
) (workflowops.Operation, error) {
	waitInput, err := json.Marshal(request.Dependencies)
	if err != nil {
		return workflowops.Operation{}, err
	}
	metadata := workflowops.CommandMetadata{
		Kind:        request.Kind,
		Identity:    string(request.Kind),
		Fingerprint: request.Fingerprint,
		Input:       waitInput,
	}
	var op workflowops.Operation
	var kind, fingerprint string
	var input []byte
	err = world.pool.QueryRow(
		ctx,
		`SELECT commands.kind, commands.fingerprint, commands.input,
		        tasks.status, COALESCE(tasks.lease_token, ''), COALESCE(tasks.error, ''),
		        COALESCE(tasks.lease_until, now()), tasks.idempotency_key
		 FROM workflow_commands commands
		 JOIN workflow_tasks tasks USING (run_id, sequence)
		 WHERE commands.run_id = $1 AND commands.sequence = $2`,
		request.RunID,
		request.Sequence,
	).Scan(&kind, &fingerprint, &input, &op.Status, &op.Token, &op.Error, &op.LeaseUntil, &op.IdempotencyKey)
	if err == nil {
		matches := commandMetadata(
			kind,
			string(workflowops.Kind(kind)),
			fingerprint,
			input,
			"",
		).MatchesWait(request, waitInput)
		if !matches {
			return op, workflowops.HistoryMismatchError("wait", request.Sequence)
		}
		op.RunID = request.RunID
		op.Sequence = request.Sequence
		return op, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return op, err
	}
	if !allowsCreate(lease, request.RunID) {
		return op, workflowops.HistoryMissingError("wait command", request.Sequence)
	}
	tx, err := world.pool.Begin(ctx)
	if err != nil {
		return op, err
	}
	defer tx.Rollback(ctx)
	key := fmt.Sprintf("%s:%d", request.RunID, request.Sequence)
	command, err := tx.Exec(
		ctx,
		`INSERT INTO workflow_commands
		 (run_id, sequence, kind, identity, fingerprint, input, input_hash)
		 VALUES ($1, $2, $3, $3, $4, $5, md5($5::text))
		 ON CONFLICT (run_id, sequence) DO NOTHING`,
		request.RunID,
		request.Sequence,
		string(metadata.Kind),
		metadata.Fingerprint,
		metadata.Input,
	)
	if err != nil {
		return op, err
	}
	if command.RowsAffected() == 0 {
		if err = tx.Commit(ctx); err != nil {
			return op, err
		}
		return world.EnsureWait(ctx, lease, request)
	}
	if _, err = tx.Exec(
		ctx,
		`INSERT INTO workflow_tasks (run_id, sequence, status, idempotency_key)
		 VALUES ($1, $2, $3, $4)`,
		request.RunID,
		request.Sequence,
		string(workflowops.StatusWaiting),
		key,
	); err != nil {
		return op, err
	}
	for position, dependency := range request.Dependencies {
		if _, err = tx.Exec(
			ctx,
			`INSERT INTO workflow_wait_dependencies
			 (wait_run_id, wait_sequence, position, dependency_run_id, dependency_sequence)
			 VALUES ($1, $2, $3, $4, $5)`,
			request.RunID,
			request.Sequence,
			position,
			dependency.RunID,
			dependency.Sequence,
		); err != nil {
			return op, err
		}
	}
	if _, err = world.resolveWaits(ctx, tx); err != nil {
		return op, err
	}
	if err = tx.Commit(ctx); err != nil {
		return op, err
	}
	return world.waitOperation(ctx, request.RunID, request.Sequence)
}

func (world *World) SuspendRun(ctx context.Context, run *workflowops.LeasedRun) error {
	if run.Sequence != workflowops.RootSequence {
		return workflowops.ErrLeaseLost
	}
	tx, err := world.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	command, err := tx.Exec(
		ctx,
		`UPDATE workflow_tasks
		 SET status = $3, lease_until = NULL, lease_token = NULL, updated_at = now()
		 WHERE run_id = $1 AND sequence = -1 AND status = $4 AND lease_token = $2`,
		run.ID,
		run.Token,
		string(workflowops.StatusWaiting),
		string(workflowops.StatusRunning),
	)
	if err != nil {
		return err
	}
	if command.RowsAffected() != 1 {
		return workflowops.ErrLeaseLost
	}
	if _, err = tx.Exec(
		ctx,
		`UPDATE go_workflow_runs SET status = $2, lease_until = NULL, lease_token = NULL, updated_at = now() WHERE id = $1`,
		run.ID,
		string(workflowops.StatusWaiting),
	); err != nil {
		return err
	}
	if _, err = world.resolveWaits(ctx, tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

type waitDependency struct {
	position int
	task     workflowops.Task
	status   string
	output   []byte
	err      *string
	ordinal  int64
}

func (world *World) resolveWaits(ctx context.Context, tx pgx.Tx) (int, error) {
	resolved := 0
	for changed := true; changed; {
		changed = false
		rows, err := tx.Query(
			ctx,
			`SELECT tasks.run_id, tasks.sequence, commands.kind
			 FROM workflow_tasks tasks
			 JOIN workflow_commands commands USING (run_id, sequence)
			 WHERE tasks.status = $1
			 ORDER BY tasks.run_id, tasks.sequence`,
			string(workflowops.StatusWaiting),
		)
		if err != nil {
			return resolved, err
		}
		var waits []struct {
			runID    string
			sequence int
			kind     string
		}
		for rows.Next() {
			var wait struct {
				runID    string
				sequence int
				kind     string
			}
			if err = rows.Scan(&wait.runID, &wait.sequence, &wait.kind); err != nil {
				rows.Close()
				return resolved, err
			}
			waits = append(waits, wait)
		}
		if err = rows.Err(); err != nil {
			rows.Close()
			return resolved, err
		}
		rows.Close()

		for _, wait := range waits {
			kind := workflowops.Kind(wait.kind)
			if !workflowops.IsWaitKind(kind) {
				continue
			}
			dependencies, err := world.waitDependencies(ctx, tx, wait.runID, wait.sequence)
			if err != nil {
				return resolved, err
			}
			resolution := workflowops.ResolveWait(kind, dependencies)
			if resolution.Pending {
				continue
			}
			status := workflowops.StatusCompleted
			var message any
			if resolution.Err != nil {
				status = workflowops.StatusFailed
				message = resolution.Err.Error()
			}
			command, err := tx.Exec(
				ctx,
				`UPDATE workflow_tasks
				 SET status = $3, output = $4, error = $5, updated_at = now()
				 WHERE run_id = $1 AND sequence = $2 AND status = $6`,
				wait.runID,
				wait.sequence,
				string(status),
				resolution.Output,
				message,
				string(workflowops.StatusWaiting),
			)
			if err != nil {
				return resolved, err
			}
			if command.RowsAffected() != 1 {
				continue
			}
			if _, err = tx.Exec(
				ctx,
				`INSERT INTO workflow_completion_events (run_id, sequence)
				 VALUES ($1, $2) ON CONFLICT (run_id, sequence) DO NOTHING`,
				wait.runID,
				wait.sequence,
			); err != nil {
				return resolved, err
			}
			if _, err = tx.Exec(
				ctx,
				`UPDATE workflow_tasks
				 SET status = $2, updated_at = now()
				 WHERE run_id = $1 AND sequence = -1 AND status = $3`,
				wait.runID,
				string(workflowops.StatusQueued),
				string(workflowops.StatusWaiting),
			); err != nil {
				return resolved, err
			}
			if _, err = tx.Exec(
				ctx,
				`UPDATE go_workflow_runs SET status = $2, updated_at = now()
				 WHERE id = $1 AND status = $3`,
				wait.runID,
				string(workflowops.StatusQueued),
				string(workflowops.StatusWaiting),
			); err != nil {
				return resolved, err
			}
			if _, err = tx.Exec(ctx, `SELECT pg_notify('go_workflow_jobs', $1)`, wait.runID); err != nil {
				return resolved, err
			}
			resolved++
			changed = true
		}
	}
	return resolved, nil
}

func (world *World) waitDependencies(
	ctx context.Context,
	tx pgx.Tx,
	runID string,
	sequence int,
) ([]workflowops.Dependency, error) {
	rows, err := tx.Query(
		ctx,
		`SELECT dependencies.position, dependencies.dependency_run_id, dependencies.dependency_sequence,
		        COALESCE(tasks.status, 'missing'), tasks.output,
		        tasks.error, COALESCE(completions.ordinal, 0)
		 FROM workflow_wait_dependencies dependencies
		 LEFT JOIN workflow_tasks tasks
		   ON tasks.run_id = dependencies.dependency_run_id
		  AND tasks.sequence = dependencies.dependency_sequence
		 LEFT JOIN workflow_completion_events completions
		   ON completions.run_id = dependencies.dependency_run_id
		  AND completions.sequence = dependencies.dependency_sequence
		 WHERE dependencies.wait_run_id = $1 AND dependencies.wait_sequence = $2
		 ORDER BY dependencies.position`,
		runID,
		sequence,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var dependencies []workflowops.Dependency
	for rows.Next() {
		var dependency waitDependency
		if err = rows.Scan(
			&dependency.position,
			&dependency.task.RunID,
			&dependency.task.Sequence,
			&dependency.status,
			&dependency.output,
			&dependency.err,
			&dependency.ordinal,
		); err != nil {
			return nil, err
		}
		dependencies = append(dependencies, workflowops.Dependency{
			Task:       dependency.task,
			Status:     workflowops.Status(dependency.status),
			Output:     dependency.output,
			Completion: uint64(dependency.ordinal),
			Found:      dependency.status != "missing",
		})
		if dependency.err != nil {
			dependencies[len(dependencies)-1].Error = *dependency.err
		}
	}
	return dependencies, rows.Err()
}

func (world *World) waitOperation(ctx context.Context, runID string, sequence int) (workflowops.Operation, error) {
	var op workflowops.Operation
	err := world.pool.QueryRow(
		ctx,
		`SELECT status, COALESCE(lease_token, ''), COALESCE(error, ''),
		        COALESCE(due_at, now()), COALESCE(lease_until, now()), idempotency_key
		 FROM workflow_tasks WHERE run_id = $1 AND sequence = $2`,
		runID,
		sequence,
	).Scan(&op.Status, &op.Token, &op.Error, &op.DueAt, &op.LeaseUntil, &op.IdempotencyKey)
	if err != nil {
		return op, err
	}
	op.RunID, op.Sequence = runID, sequence
	return op, nil
}

func (world *World) FinishOperation(
	ctx context.Context,
	op workflowops.Operation,
	output json.RawMessage,
	cause error,
) error {
	status := workflowops.StatusCompleted
	var message any
	if cause != nil {
		status = workflowops.StatusFailed
		message = cause.Error()
		output = nil
	}
	tx, err := world.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	command, err := tx.Exec(
		ctx,
		`UPDATE workflow_tasks
		 SET status = $4, output = $5, error = $6, lease_until = NULL,
		     lease_token = NULL, updated_at = now()
		 WHERE run_id = $1 AND sequence = $2 AND lease_token = $3`,
		op.RunID,
		op.Sequence,
		op.Token,
		string(status),
		output,
		message,
	)
	if err != nil {
		return err
	}
	if command.RowsAffected() != 1 {
		return workflowops.ErrLeaseLost
	}
	if _, err = tx.Exec(
		ctx,
		`INSERT INTO workflow_completion_events (run_id, sequence)
		 VALUES ($1, $2) ON CONFLICT (run_id, sequence) DO NOTHING`,
		op.RunID,
		op.Sequence,
	); err != nil {
		return err
	}
	if _, err = world.resolveWaits(ctx, tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (world *World) WaitOperation(ctx context.Context, task workflowops.Task) (json.RawMessage, error) {
	ticker := time.NewTicker(world.interval)
	defer ticker.Stop()
	for {
		var status string
		var output []byte
		var message *string
		err := world.pool.QueryRow(
			ctx,
			`SELECT status, output, error
			 FROM workflow_tasks
			 WHERE run_id = $1 AND sequence = $2`,
			task.RunID,
			task.Sequence,
		).Scan(&status, &output, &message)
		if err != nil {
			return nil, err
		}
		if workflowops.Status(status) == workflowops.StatusCompleted {
			return output, nil
		}
		if workflowops.Status(status) == workflowops.StatusFailed {
			if message == nil {
				return nil, errors.New("workflow: operation failed")
			}
			return nil, errors.New(*message)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (world *World) AppendEvent(ctx context.Context, task workflowops.Task, data []byte) error {
	_, err := world.pool.Exec(
		ctx,
		`INSERT INTO go_workflow_events (run_id, operation_sequence, data)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (run_id, operation_sequence)
		 WHERE operation_sequence IS NOT NULL DO NOTHING`,
		task.RunID,
		task.Sequence,
		data,
	)
	return err
}

func (world *World) Events(ctx context.Context, runID string, after int64) ([]workflow.Event, error) {
	rows, err := world.pool.Query(
		ctx,
		`SELECT id, data, created_at
		 FROM go_workflow_events
		 WHERE run_id = $1 AND id > $2
		 ORDER BY id`,
		runID,
		after,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []workflow.Event
	for rows.Next() {
		var event workflow.Event
		if err := rows.Scan(&event.ID, &event.Data, &event.CreatedAt); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

func allowsCreate(lease *workflowops.LeasedRun, runID string) bool {
	return lease != nil && lease.ID == runID && lease.Sequence == workflowops.RootSequence
}

func commandMetadata(
	kind string,
	identity string,
	fingerprint string,
	input json.RawMessage,
	childRunID string,
) workflowops.CommandMetadata {
	return workflowops.CommandMetadata{
		Kind:        workflowops.Kind(kind),
		Identity:    identity,
		Fingerprint: fingerprint,
		Input:       input,
		ChildRunID:  childRunID,
	}
}

func statusLiteral(status workflowops.Status) string {
	return "'" + strings.ReplaceAll(string(status), "'", "''") + "'"
}
