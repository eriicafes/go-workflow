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
	in_band_count bigint NOT NULL DEFAULT 0
);
CREATE INDEX go_workflow_runs_created_idx ON go_workflow_runs (created_at, run_id);
CREATE INDEX go_workflow_runs_workflow_idx ON go_workflow_runs (workflow_name, created_at, run_id);
CREATE INDEX go_workflow_runs_status_idx ON go_workflow_runs (status, created_at, run_id);
CREATE INDEX go_workflow_runs_completed_idx ON go_workflow_runs (completed_at) WHERE completed_at IS NOT NULL;

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
	created_slot bigint NOT NULL,
	result_slot bigint,
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
	workflow_name text NOT NULL,
	deployment_id text NOT NULL,
	token text NOT NULL,
	created_slot bigint NOT NULL,
	webhook jsonb,
	webhook_event_id text NOT NULL DEFAULT '',
	created_at timestamptz NOT NULL,
	updated_at timestamptz NOT NULL,
	received_at timestamptz,
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
CREATE INDEX go_workflow_hook_tokens_hook_idx ON go_workflow_hook_tokens (hook_id);

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
	deployment_id text NOT NULL DEFAULT '',
	deployment_mismatch_retry_count integer NOT NULL DEFAULT 0,
	created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX go_workflow_queue_claim_idx ON go_workflow_queue (not_before, id);
CREATE INDEX go_workflow_queue_routed_claim_idx ON go_workflow_queue (queue_name, not_before, id);
CREATE UNIQUE INDEX go_workflow_queue_dedupe_idx ON go_workflow_queue (run_id, dedupe_key) WHERE dedupe_key IS NOT NULL;
CREATE INDEX go_workflow_queue_run_idx ON go_workflow_queue (run_id);

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
`, `
-- Stream writers are no longer exclusive.
ALTER TABLE go_workflow_streams DROP COLUMN session, DROP COLUMN sequence;
-- Runs publish a public key that payloads sent from outside are sealed to,
-- and hooks carry it with the rest of their resume context.
ALTER TABLE go_workflow_runs ADD COLUMN encryption_public_key text NOT NULL DEFAULT '';
ALTER TABLE go_workflow_hooks ADD COLUMN encryption_public_key text NOT NULL DEFAULT '';
-- A webhook's settings are in its metadata, as in the Workflow SDK, and it
-- takes any number of requests.
ALTER TABLE go_workflow_hooks DROP COLUMN webhook, DROP COLUMN webhook_event_id,
	ADD COLUMN is_webhook boolean NOT NULL DEFAULT false;
`, `
-- A run's data is deleted, rather than the run, as the Workflow SDK does.
-- expired_at is when it was or will be deleted, and data_purged records that
-- its payloads are gone.
ALTER TABLE go_workflow_runs ADD COLUMN expired_at timestamptz,
	ADD COLUMN data_purged boolean NOT NULL DEFAULT false;
-- A run's executionContext, as the Workflow SDK records it.
ALTER TABLE go_workflow_runs ADD COLUMN execution_context jsonb;
CREATE INDEX go_workflow_runs_expired_idx ON go_workflow_runs (expired_at) WHERE expired_at IS NOT NULL AND NOT data_purged;
`, `
-- A run records the spec version it was created under.
ALTER TABLE go_workflow_runs ADD COLUMN spec_version integer NOT NULL DEFAULT 0;
`, `
-- A message keeps its row across redeliveries. payload holds the whole
-- delivery, deliveries counts every delivery and fences its lease, and
-- handoffs counts the redeliveries that are not attempts. An IdempotencyKey
-- deduplicates while its message lives, running marks the one serialized
-- flow of a run that executes, and expires_at ends a message's life.
ALTER TABLE go_workflow_queue ADD COLUMN payload bytea,
	ADD COLUMN serialized boolean NOT NULL DEFAULT false;
UPDATE go_workflow_queue SET payload = convert_to(json_build_object(
	'RunID', run_id, 'QueueName', queue_name, 'StepID', step_id,
	'ReplayDivergenceCount', replay_divergence_count, 'DeploymentID', deployment_id,
	'DeploymentMismatchRetryCount', deployment_mismatch_retry_count)::text, 'UTF8'),
	idempotency_key = '', serialized = step_id IS NULL;
DROP INDEX go_workflow_queue_dedupe_idx;
ALTER TABLE go_workflow_queue DROP COLUMN dedupe_key, DROP COLUMN replay_divergence_count,
	DROP COLUMN deployment_id, DROP COLUMN deployment_mismatch_retry_count,
	ALTER COLUMN payload SET NOT NULL,
	ADD COLUMN handoffs integer NOT NULL DEFAULT 0,
	ADD COLUMN running boolean NOT NULL DEFAULT false,
	ADD COLUMN expires_at timestamptz;
ALTER TABLE go_workflow_queue RENAME COLUMN attempts TO deliveries;
CREATE UNIQUE INDEX go_workflow_queue_key_idx ON go_workflow_queue (run_id, idempotency_key) WHERE idempotency_key <> '';
CREATE UNIQUE INDEX go_workflow_queue_running_flow_idx ON go_workflow_queue (run_id) WHERE running;
CREATE INDEX go_workflow_queue_expires_idx ON go_workflow_queue (expires_at) WHERE expires_at IS NOT NULL;
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
