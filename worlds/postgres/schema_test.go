package postgresworld

import (
	"context"
	"strings"
	"testing"

	"github.com/eriicafes/go-workflow/worlds"
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
