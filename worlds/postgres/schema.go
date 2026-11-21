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
//
// The run, step, and hook views hold no payloads. They name the slots of the
// events carrying them, and reads join go_workflow_events, which is the only
// copy. A run's input is always slot 1 (run_created).
//
// A queue row keeps its message across redeliveries: payload holds the whole
// delivery, deliveries counts every delivery and fences its lease, and
// handoffs counts the redeliveries that are not attempts. An idempotency key
// deduplicates while its message lives, running marks the one serialized flow
// of a run that executes, and expires_at ends a message's life.
var migrations = []string{`
CREATE TABLE go_workflow_runs (
	run_id text PRIMARY KEY,
	workflow_name text NOT NULL,
	status text NOT NULL,
	result_slot bigint,
	attributes jsonb NOT NULL DEFAULT '{}'::jsonb,
	event_count bigint NOT NULL,
	created_at timestamptz NOT NULL,
	updated_at timestamptz NOT NULL,
	started_at timestamptz,
	completed_at timestamptz,
	deployment_id text NOT NULL DEFAULT '',
	in_band_count bigint NOT NULL,
	spec_version integer NOT NULL,
	encryption_public_key text NOT NULL DEFAULT '',
	execution_context jsonb,
	expired_at timestamptz,
	data_purged boolean NOT NULL DEFAULT false,
	start_hook jsonb
);
CREATE INDEX go_workflow_runs_created_idx ON go_workflow_runs (created_at, run_id);
CREATE INDEX go_workflow_runs_workflow_idx ON go_workflow_runs (workflow_name, created_at, run_id);
CREATE INDEX go_workflow_runs_status_idx ON go_workflow_runs (status, created_at, run_id);
CREATE INDEX go_workflow_runs_completed_idx ON go_workflow_runs (completed_at) WHERE completed_at IS NOT NULL;
CREATE INDEX go_workflow_runs_expired_idx ON go_workflow_runs (expired_at) WHERE expired_at IS NOT NULL AND NOT data_purged;

CREATE TABLE go_workflow_events (
	run_id text NOT NULL,
	slot bigint NOT NULL,
	type text NOT NULL,
	correlation_id text NOT NULL DEFAULT '',
	data bytea,
	created_at timestamptz NOT NULL,
	spec_version integer NOT NULL,
	resume_id text,
	resume_payload_digest text,
	PRIMARY KEY (run_id, slot)
);
CREATE INDEX go_workflow_events_correlation_idx ON go_workflow_events (run_id, correlation_id, slot);
CREATE UNIQUE INDEX go_workflow_events_hook_resume_unique ON go_workflow_events (run_id, resume_id)
	WHERE type = 'hook_received' AND resume_id IS NOT NULL;

CREATE TABLE go_workflow_steps (
	run_id text NOT NULL,
	step_id text NOT NULL,
	step_name text NOT NULL,
	status text NOT NULL,
	created_slot bigint NOT NULL,
	result_slot bigint,
	attempt integer NOT NULL DEFAULT 0,
	created_at timestamptz NOT NULL,
	updated_at timestamptz NOT NULL,
	started_at timestamptz,
	completed_at timestamptz,
	retry_after timestamptz,
	spec_version integer NOT NULL,
	PRIMARY KEY (run_id, step_id)
);
CREATE INDEX go_workflow_steps_created_idx ON go_workflow_steps (run_id, created_at, step_id);

CREATE TABLE go_workflow_hooks (
	hook_id text PRIMARY KEY,
	run_id text NOT NULL,
	workflow_name text NOT NULL,
	deployment_id text NOT NULL,
	encryption_public_key text NOT NULL DEFAULT '',
	token text NOT NULL,
	created_slot bigint NOT NULL,
	is_webhook boolean NOT NULL DEFAULT false,
	created_at timestamptz NOT NULL,
	updated_at timestamptz NOT NULL,
	received_at timestamptz,
	token_retention_until timestamptz,
	claimed_from jsonb,
	force_claimed_by jsonb,
	disposed_at timestamptz,
	spec_version integer NOT NULL,
	run_spec_version integer NOT NULL
);
CREATE INDEX go_workflow_hooks_run_idx ON go_workflow_hooks (run_id, created_at, hook_id);
CREATE INDEX go_workflow_hooks_created_idx ON go_workflow_hooks (created_at, hook_id);

-- A start hook candidate's permanent decision: accepted, or rejected because
-- conflicting_run_id held its token.
CREATE TABLE go_workflow_start_hook_admissions (
	run_id text PRIMARY KEY,
	token text NOT NULL,
	decision text NOT NULL,
	conflicting_run_id text,
	created_at timestamptz NOT NULL
);

CREATE TABLE go_workflow_hook_tokens (
	token text PRIMARY KEY,
	hook_id text NOT NULL
);
CREATE INDEX go_workflow_hook_tokens_hook_idx ON go_workflow_hook_tokens (hook_id);

CREATE TABLE go_workflow_queue (
	id text PRIMARY KEY,
	queue_name text NOT NULL DEFAULT '',
	run_id text NOT NULL,
	step_id text,
	idempotency_key text NOT NULL DEFAULT '',
	payload bytea NOT NULL,
	serialized boolean NOT NULL DEFAULT false,
	not_before timestamptz NOT NULL,
	deliveries integer NOT NULL DEFAULT 0,
	handoffs integer NOT NULL DEFAULT 0,
	lease_until timestamptz,
	running boolean NOT NULL DEFAULT false,
	expires_at timestamptz,
	created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX go_workflow_queue_claim_idx ON go_workflow_queue (not_before, id);
CREATE INDEX go_workflow_queue_routed_claim_idx ON go_workflow_queue (queue_name, not_before, id);
CREATE INDEX go_workflow_queue_run_idx ON go_workflow_queue (run_id);
CREATE UNIQUE INDEX go_workflow_queue_key_idx ON go_workflow_queue (run_id, idempotency_key) WHERE idempotency_key <> '';
CREATE UNIQUE INDEX go_workflow_queue_running_flow_idx ON go_workflow_queue (run_id) WHERE running;
CREATE INDEX go_workflow_queue_expires_idx ON go_workflow_queue (expires_at) WHERE expires_at IS NOT NULL;

CREATE TABLE go_workflow_streams (
	run_id text NOT NULL,
	name text NOT NULL,
	closed boolean NOT NULL DEFAULT false,
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
