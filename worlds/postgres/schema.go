package postgresworld

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// schemaLockKey serializes migrations across processes sharing a database.
const schemaLockKey int64 = 0x676f5f776f726b66

// migrations are applied in order, and the index plus one is the recorded
// version. Never edit a released migration, append a new one instead.
var migrations = []string{`
CREATE TABLE go_workflow_runs (
	run_id text PRIMARY KEY,
	workflow_name text NOT NULL,
	status text NOT NULL,
	input bytea,
	output bytea,
	error bytea,
	attributes jsonb NOT NULL DEFAULT '{}'::jsonb,
	event_count bigint NOT NULL,
	created_at timestamptz NOT NULL,
	updated_at timestamptz NOT NULL,
	started_at timestamptz,
	completed_at timestamptz,
	deployment_id text NOT NULL DEFAULT '',
	in_band_count bigint NOT NULL DEFAULT 0
);
CREATE INDEX go_workflow_runs_created_idx ON go_workflow_runs (created_at, run_id);
CREATE INDEX go_workflow_runs_workflow_idx ON go_workflow_runs (workflow_name, created_at, run_id);
CREATE INDEX go_workflow_runs_status_idx ON go_workflow_runs (status, created_at, run_id);

CREATE TABLE go_workflow_events (
	run_id text NOT NULL,
	slot bigint NOT NULL,
	type text NOT NULL,
	correlation_id text NOT NULL DEFAULT '',
	data bytea,
	created_at timestamptz NOT NULL,
	PRIMARY KEY (run_id, slot)
);
CREATE INDEX go_workflow_events_correlation_idx ON go_workflow_events (run_id, correlation_id, slot);

CREATE TABLE go_workflow_steps (
	run_id text NOT NULL,
	step_id text NOT NULL,
	step_name text NOT NULL,
	status text NOT NULL,
	input bytea,
	output bytea,
	error bytea,
	attempt integer NOT NULL DEFAULT 0,
	created_at timestamptz NOT NULL,
	updated_at timestamptz NOT NULL,
	started_at timestamptz,
	completed_at timestamptz,
	retry_after timestamptz,
	PRIMARY KEY (run_id, step_id)
);
CREATE INDEX go_workflow_steps_created_idx ON go_workflow_steps (run_id, created_at, step_id);

CREATE TABLE go_workflow_hooks (
	hook_id text PRIMARY KEY,
	run_id text NOT NULL,
	token text NOT NULL,
	metadata bytea,
	webhook jsonb,
	webhook_event_id text NOT NULL DEFAULT '',
	created_at timestamptz NOT NULL,
	updated_at timestamptz NOT NULL,
	received_at timestamptz,
	min_retention bigint NOT NULL DEFAULT 0,
	token_retention_until timestamptz,
 claimed_from jsonb,
 force_claimed_by jsonb,
 disposed_at timestamptz
);
CREATE INDEX go_workflow_hooks_run_idx ON go_workflow_hooks (run_id, created_at, hook_id);
CREATE INDEX go_workflow_hooks_created_idx ON go_workflow_hooks (created_at, hook_id);

CREATE TABLE go_workflow_hook_tokens (
	token text PRIMARY KEY,
	hook_id text NOT NULL
);

CREATE TABLE go_workflow_queue (
	id text PRIMARY KEY,
	queue_name text NOT NULL DEFAULT '',
	run_id text NOT NULL,
	step_id text,
	idempotency_key text NOT NULL DEFAULT '',
	dedupe_key text,
	not_before timestamptz NOT NULL,
	attempts integer NOT NULL DEFAULT 0,
	lease_until timestamptz,
	replay_divergence_count integer NOT NULL DEFAULT 0,
	created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX go_workflow_queue_claim_idx ON go_workflow_queue (not_before, id);
CREATE UNIQUE INDEX go_workflow_queue_dedupe_idx ON go_workflow_queue (run_id, dedupe_key) WHERE dedupe_key IS NOT NULL;

CREATE TABLE go_workflow_streams (
	run_id text NOT NULL,
	name text NOT NULL,
	closed boolean NOT NULL DEFAULT false,
	session text,
	sequence integer NOT NULL DEFAULT 0,
	chunk_count bigint NOT NULL DEFAULT 0,
	PRIMARY KEY (run_id, name)
);

CREATE TABLE go_workflow_stream_chunks (
	run_id text NOT NULL,
	name text NOT NULL,
	idx bigint NOT NULL,
	data bytea NOT NULL,
	PRIMARY KEY (run_id, name, idx)
);
`}

// migrate applies pending migrations in one transaction under a
// transaction-scoped advisory lock, so concurrent processes migrate once.
func migrate(ctx context.Context, pool *pgxpool.Pool) error {
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, schemaLockKey); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS go_workflow_schema_version (
			version integer PRIMARY KEY,
			applied_at timestamptz NOT NULL DEFAULT now()
		)`); err != nil {
			return err
		}
		var current int
		if err := tx.QueryRow(ctx, `SELECT coalesce(max(version), 0) FROM go_workflow_schema_version`).Scan(&current); err != nil {
			return err
		}
		if current > len(migrations) {
			return fmt.Errorf("database schema version %d is newer than supported version %d", current, len(migrations))
		}
		for version := current + 1; version <= len(migrations); version++ {
			if _, err := tx.Exec(ctx, migrations[version-1]); err != nil {
				return fmt.Errorf("migration %d: %w", version, err)
			}
			if _, err := tx.Exec(ctx, `INSERT INTO go_workflow_schema_version (version) VALUES ($1)`, version); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("workflow/postgres: migrate: %w", err)
	}
	return nil
}
