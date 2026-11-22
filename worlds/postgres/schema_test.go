package postgresworld

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/eriicafes/go-workflow/worlds"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestMigrate(t *testing.T) {
	t.Parallel()
	url, schema := testSchema(t)
	pool := testPool(t, url, schema)
	ctx := context.Background()
	for range 2 {
		if err := migrate(ctx, pool, newQualifier(schema)); err != nil {
			t.Fatal(err)
		}
	}
	var count, version int
	if err := pool.QueryRow(ctx, `SELECT count(*), max(version) FROM workflow_schema_version`).Scan(&count, &version); err != nil {
		t.Fatal(err)
	}
	if count != len(migrations) || version != len(migrations) {
		t.Fatalf("versions = %d max %d, want %d", count, version, len(migrations))
	}
	if _, err := pool.Exec(ctx, `INSERT INTO workflow_schema_version (version) VALUES ($1)`, len(migrations)+1); err != nil {
		t.Fatal(err)
	}
	if err := migrate(ctx, pool, newQualifier(schema)); err == nil || !strings.Contains(err.Error(), "newer") {
		t.Fatalf("migrate over newer schema err = %v", err)
	}
}

// TestSchemaQualifiesEveryTable uses a pool without the test schema on its
// search path, so a statement that does not name the World's schema fails.
func TestSchemaQualifiesEveryTable(t *testing.T) {
	t.Parallel()
	url, schema := testSchema(t)
	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	world := testWorld(t, pool, Config{Schema: schema})
	ctx := context.Background()
	runID := worlds.NewRunID()
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "qualified", Input: []byte(`"in"`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeHookCreated, CorrelationID: "hook_q", Data: []byte(`{"token":"qualified"}`)}}); err != nil {
		t.Fatal(err)
	}
	if err := world.StreamsWrite(ctx, worlds.StreamsWriteInput{RunID: runID, Name: "out", Chunk: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	if run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID}); err != nil || string(run.Input) != `"in"` {
		t.Fatalf("run = %+v err=%v", run, err)
	}
	if _, err := world.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: "qualified"}); err != nil {
		t.Fatal(err)
	}
	var tables int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM information_schema.tables WHERE table_schema = $1 AND table_name LIKE 'workflow_%'`, schema).Scan(&tables); err != nil || tables != 10 {
		t.Fatalf("tables in %s = %d err=%v", schema, tables, err)
	}
}

// TestSchemaIndexesAndColumns checks the indexes the migration creates, by
// name, and the columns whose shape the World relies on.
func TestSchemaIndexesAndColumns(t *testing.T) {
	t.Parallel()
	url, schema := testSchema(t)
	pool := testPool(t, url, schema)
	ctx := context.Background()
	if err := migrate(ctx, pool, newQualifier(schema)); err != nil {
		t.Fatal(err)
	}
	rows, err := pool.Query(ctx, `SELECT indexname FROM pg_indexes WHERE schemaname = $1 AND indexname NOT LIKE '%_pkey' ORDER BY indexname`, schema)
	if err != nil {
		t.Fatal(err)
	}
	got, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"workflow_events_correlation_id_index",
		"workflow_events_entity_creation_unique",
		"workflow_events_hook_resume_unique",
		"workflow_hooks_run_id_index",
		"workflow_hooks_token_claim_unique",
		"workflow_hooks_token_index",
		"workflow_queue_claim_index",
		"workflow_queue_idempotency_key_unique",
		"workflow_queue_routed_claim_index",
		"workflow_queue_run_id_index",
		"workflow_queue_running_unique",
		"workflow_runs_expired_index",
		"workflow_runs_name_index",
		"workflow_runs_status_index",
		"workflow_start_hook_admissions_created_at_index",
		"workflow_steps_status_index",
		"workflow_stream_chunks_run_id_index",
		"workflow_waits_run_id_index",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("indexes = %q\nwant %q", got, want)
	}
	column := func(table, name string) (dataType string, nullable bool, found bool) {
		var isNullable string
		err := pool.QueryRow(ctx, `SELECT data_type, is_nullable FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 AND column_name = $3`, schema, table, name).Scan(&dataType, &isNullable)
		if errors.Is(err, pgx.ErrNoRows) {
			return "", false, false
		}
		if err != nil {
			t.Fatal(err)
		}
		return dataType, isNullable == "YES", true
	}
	if dataType, nullable, _ := column("workflow_stream_chunks", "run_id"); dataType != "character varying" || !nullable {
		t.Errorf("workflow_stream_chunks.run_id is %s, nullable %v", dataType, nullable)
	}
	if dataType, _, _ := column("workflow_hooks", "claimed_from"); dataType != "bytea" {
		t.Errorf("workflow_hooks.claimed_from is %s", dataType)
	}
	// A queued message does not expire.
	if _, _, found := column("workflow_queue", "expires_at"); found {
		t.Error("workflow_queue has an expires_at column")
	}
}
