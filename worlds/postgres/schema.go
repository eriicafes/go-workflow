package postgresworld

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// defaultSchema is the schema the World's tables live in, as world-postgres
// names it.
const defaultSchema = "workflow"

// schemaLockKey serializes migrations across processes sharing a database.
const schemaLockKey int64 = 0x676f5f776f726b66

// migrations are applied in order, and the index plus one is the recorded
// version. Never edit a released migration, append a new one instead.
//
// The tables are world-postgres's (drizzle/schema.js), in the World's
// schema, which every statement names as "workflow." and qualify rewrites.
// They differ from world-postgres's where Go needs it:
//   - Payloads are stored once, in the events. The run, step, and hook views
//     name the slots of the events carrying them, and reads join
//     workflow_events. A run's input is always slot 1 (run_created).
//   - An event's data is payload_cbor, Go's own CBOR (worlds.EncodeEventData),
//     with payloads as raw bytes. There are no jsonb twins.
//   - Steps are keyed by run and step, as world-local keys them, since a step
//     ID is unique only within its run.
//   - Hooks fill in resume_context, the run's workflow, deployment, public
//     key, and spec version, which wake the run, where world-postgres leaves
//     it empty, and have no owner_id, project_id, or environment. A hook's
//     row goes when it is disposed or done with its token, as in
//     world-postgres.
//   - workflow_runs.data_purged records that a run's rows no longer hold its
//     data, for Config.Retention's background purge.
//   - There are no snapshots, invocations, dynamic workflow code, or
//     graphile-worker tables. workflow_queue is Go's queue.
//
// A queue row keeps its message across redeliveries: payload holds the whole
// delivery with the delivery count its current job began at, deliveries
// counts every delivery and fences its lease, and handoffs counts the
// deliveries that are not attempts. An idempotency key
// deduplicates while its message lives, running marks the one serialized flow
// of a run that executes, and expires_at ends a message's life.
var migrations = []string{`
CREATE TYPE workflow.status AS ENUM ('pending', 'running', 'completed', 'failed', 'cancelled');
CREATE TYPE workflow.step_status AS ENUM ('pending', 'running', 'completed', 'failed', 'cancelled');
CREATE TYPE workflow.wait_status AS ENUM ('waiting', 'completed');

CREATE TABLE workflow.workflow_runs (
	id varchar PRIMARY KEY,
	deployment_id varchar NOT NULL DEFAULT '',
	status workflow.status NOT NULL,
	name varchar NOT NULL,
	spec_version integer NOT NULL,
	execution_context_cbor bytea,
	error_code varchar,
	attributes jsonb NOT NULL DEFAULT '{}'::jsonb,
	encryption_public_key varchar,
	start_hook_cbor bytea,
	result_slot bigint,
	data_purged boolean NOT NULL DEFAULT false,
	created_at timestamptz NOT NULL DEFAULT now(),
	updated_at timestamptz NOT NULL DEFAULT now(),
	completed_at timestamptz,
	started_at timestamptz,
	expired_at timestamptz
);
CREATE INDEX workflow_runs_name_idx ON workflow.workflow_runs (name);
CREATE INDEX workflow_runs_status_idx ON workflow.workflow_runs (status);
CREATE INDEX workflow_runs_created_idx ON workflow.workflow_runs (created_at, id);
CREATE INDEX workflow_runs_expired_idx ON workflow.workflow_runs (expired_at) WHERE expired_at IS NOT NULL AND NOT data_purged;

CREATE TABLE workflow.workflow_events (
	id varchar NOT NULL,
	type varchar NOT NULL,
	correlation_id varchar,
	created_at timestamptz NOT NULL DEFAULT now(),
	run_id varchar NOT NULL,
	payload_cbor bytea,
	spec_version integer,
	resume_id varchar,
	resume_payload_digest varchar,
	PRIMARY KEY (run_id, id)
);
CREATE INDEX workflow_events_correlation_id_idx ON workflow.workflow_events (correlation_id);
CREATE UNIQUE INDEX workflow_events_hook_resume_unique ON workflow.workflow_events (run_id, resume_id)
	WHERE type = 'hook_received' AND resume_id IS NOT NULL;
CREATE UNIQUE INDEX workflow_events_entity_creation_unique ON workflow.workflow_events (run_id, correlation_id, type)
	WHERE type IN ('step_created', 'hook_created', 'wait_created', 'attr_set');

-- The in-band writer fence of a run: positions accepted from in-band
-- writes, run_created included. A run's next position is its highest event
-- id plus one, read under this row's lock.
CREATE TABLE workflow.workflow_event_slots (
	run_id varchar PRIMARY KEY,
	seq_in_band integer NOT NULL DEFAULT 1
);

-- A start hook candidate's permanent decision: accepted, or rejected because
-- conflicting_run_id held its token.
CREATE TABLE workflow.workflow_start_hook_admissions (
	run_id varchar PRIMARY KEY,
	token varchar NOT NULL,
	decision varchar NOT NULL,
	conflicting_run_id varchar,
	created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX workflow_start_hook_admissions_created_idx ON workflow.workflow_start_hook_admissions (created_at);

CREATE TABLE workflow.workflow_steps (
	run_id varchar NOT NULL,
	step_id varchar NOT NULL,
	step_name varchar NOT NULL,
	status workflow.step_status NOT NULL,
	input_slot bigint NOT NULL,
	output_slot bigint,
	error_slot bigint,
	attempt integer NOT NULL DEFAULT 0,
	started_at timestamptz,
	completed_at timestamptz,
	created_at timestamptz NOT NULL DEFAULT now(),
	updated_at timestamptz NOT NULL DEFAULT now(),
	retry_after timestamptz,
	spec_version integer,
	PRIMARY KEY (run_id, step_id)
);
CREATE INDEX workflow_steps_status_idx ON workflow.workflow_steps (status);
CREATE INDEX workflow_steps_created_idx ON workflow.workflow_steps (run_id, created_at, step_id);

CREATE TABLE workflow.workflow_hooks (
	run_id varchar NOT NULL,
	hook_id varchar PRIMARY KEY,
	token varchar NOT NULL,
	created_at timestamptz NOT NULL DEFAULT now(),
	token_retention_until timestamptz,
	metadata_slot bigint NOT NULL,
	spec_version integer,
	is_webhook boolean DEFAULT true,
	is_system boolean DEFAULT false,
	claimed_from jsonb,
	claims_token boolean NOT NULL DEFAULT false,
	resume_context bytea NOT NULL
);
CREATE INDEX workflow_hooks_run_id_idx ON workflow.workflow_hooks (run_id, created_at, hook_id);
CREATE INDEX workflow_hooks_token_idx ON workflow.workflow_hooks (token);
CREATE INDEX workflow_hooks_created_idx ON workflow.workflow_hooks (created_at, hook_id);
CREATE UNIQUE INDEX workflow_hooks_token_claim_unique ON workflow.workflow_hooks (token) WHERE claims_token;

CREATE TABLE workflow.workflow_waits (
	wait_id varchar PRIMARY KEY,
	run_id varchar NOT NULL,
	status workflow.wait_status NOT NULL,
	resume_at timestamptz,
	completed_at timestamptz,
	created_at timestamptz NOT NULL DEFAULT now(),
	updated_at timestamptz NOT NULL DEFAULT now(),
	spec_version integer
);
CREATE INDEX workflow_waits_run_id_idx ON workflow.workflow_waits (run_id);

-- A stream's chunks are ordered by their chnk_<ULID> id. An eof chunk, with
-- empty data, closes the stream.
CREATE TABLE workflow.workflow_stream_chunks (
	id varchar NOT NULL,
	stream_id varchar NOT NULL,
	run_id varchar NOT NULL,
	data bytea NOT NULL,
	created_at timestamptz NOT NULL DEFAULT now(),
	eof boolean NOT NULL,
	PRIMARY KEY (stream_id, id)
);
CREATE INDEX workflow_stream_chunks_run_id_index ON workflow.workflow_stream_chunks (run_id);

CREATE TABLE workflow.workflow_queue (
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
CREATE INDEX workflow_queue_claim_idx ON workflow.workflow_queue (not_before, id);
CREATE INDEX workflow_queue_routed_claim_idx ON workflow.workflow_queue (queue_name, not_before, id);
CREATE INDEX workflow_queue_run_idx ON workflow.workflow_queue (run_id);
CREATE UNIQUE INDEX workflow_queue_key_idx ON workflow.workflow_queue (run_id, idempotency_key) WHERE idempotency_key <> '';
CREATE UNIQUE INDEX workflow_queue_running_flow_idx ON workflow.workflow_queue (run_id) WHERE running;
CREATE INDEX workflow_queue_expires_idx ON workflow.workflow_queue (expires_at) WHERE expires_at IS NOT NULL;
`}

// qualifier rewrites the "workflow." of the World's statements to its
// schema.
type qualifier struct{ prefix string }

func newQualifier(schema string) qualifier {
	return qualifier{prefix: pgx.Identifier{schema}.Sanitize() + "."}
}

// q returns statement naming the World's schema.
func (s qualifier) q(statement string) string {
	if s.prefix == `"`+defaultSchema+`".` {
		return statement
	}
	return strings.ReplaceAll(statement, "workflow.", s.prefix)
}

// migrate creates the schema and applies pending migrations in one
// transaction under a transaction-scoped advisory lock, so concurrent
// processes migrate once.
func migrate(ctx context.Context, pool *pgxpool.Pool, schema qualifier) error {
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, schemaLockKey); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+strings.TrimSuffix(schema.prefix, ".")); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, schema.q(`CREATE TABLE IF NOT EXISTS workflow.workflow_schema_version (
			version integer PRIMARY KEY,
			applied_at timestamptz NOT NULL DEFAULT now()
		)`)); err != nil {
			return err
		}
		var current int
		if err := tx.QueryRow(ctx, schema.q(`SELECT coalesce(max(version), 0) FROM workflow.workflow_schema_version`)).Scan(&current); err != nil {
			return err
		}
		if current > len(migrations) {
			return fmt.Errorf("database schema version %d is newer than supported version %d", current, len(migrations))
		}
		for version := current + 1; version <= len(migrations); version++ {
			if _, err := tx.Exec(ctx, schema.q(migrations[version-1])); err != nil {
				return fmt.Errorf("migration %d: %w", version, err)
			}
			if _, err := tx.Exec(ctx, schema.q(`INSERT INTO workflow.workflow_schema_version (version) VALUES ($1)`), version); err != nil {
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
