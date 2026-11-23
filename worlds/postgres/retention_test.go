package postgresworld

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// retentionTables are the tables that hold rows of a run.
var retentionTables = []string{"workflow_runs", "workflow_events", "workflow_event_slots", "workflow_steps", "workflow_hooks", "workflow_queue", "workflow_stream_chunks"}

func TestExpireRuns(t *testing.T) {
	url, schema := testSchema(t)
	world := testWorld(t, testPool(t, url, schema), Config{Retention: time.Hour})
	ctx := context.Background()

	done := retentionRun(t, world, "done")
	// The hook's retained token keeps its row past the run's end.
	retained := time.Now().Add(time.Hour)
	writeEvent(t, world, done, worlds.EventTypeHookCreated, "hook_done", worlds.DataHookCreated{Token: "token_done", Metadata: []byte(`"metadata"`), TokenRetentionUntil: &retained})
	if err := world.StreamsWrite(ctx, worlds.StreamsWriteInput{RunID: done, Name: "out", Chunk: []byte("chunk")}); err != nil {
		t.Fatal(err)
	}
	complete(t, world, done)
	alive := retentionRun(t, world, "alive")

	// A finished run's data is kept for the hour.
	run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: done})
	if err != nil || run.ExpiredAt == nil || run.ExpiredAt.Sub(*run.CompletedAt) != time.Hour || string(run.Output) != `"ok"` {
		t.Fatalf("finished run = %+v, %v", run, err)
	}
	if n, err := world.expireRuns(ctx); err != nil || n != 0 {
		t.Fatalf("expireRuns before retention = %d, %v", n, err)
	}

	// Once the hour has passed, reads hide the run's data before Worker
	// deletes it.
	if _, err := world.pool.Exec(ctx, `UPDATE workflow_runs SET expired_at = now() - interval '1 second' WHERE id = $1`, done); err != nil {
		t.Fatal(err)
	}
	run, err = world.RunsGet(ctx, worlds.RunsGetInput{RunID: done})
	if err != nil || run.Input != nil || run.Output != nil {
		t.Fatalf("expired run before purge = %+v, %v", run, err)
	}
	if purgedData(t, world, done) {
		t.Fatal("the run was purged before Worker ran")
	}
	if n, err := world.expireRuns(ctx); err != nil || n != 1 {
		t.Fatalf("expireRuns = %d, %v; want the expired run only", n, err)
	}
	if !purgedData(t, world, done) {
		t.Fatal("expireRuns left the run's data")
	}
	// Its rows stay, and the chunks go.
	counts := rowCounts(t, world, done)
	for _, table := range []string{"workflow_runs", "workflow_events", "workflow_steps", "workflow_hooks", "workflow_stream_chunks"} {
		if counts[table] == 0 {
			t.Errorf("%s lost the expired run's rows", table)
		}
	}
	var chunks int
	if err := world.pool.QueryRow(ctx, `SELECT count(*) FROM workflow_stream_chunks WHERE run_id = $1 AND NOT eof`, done).Scan(&chunks); err != nil || chunks != 0 {
		t.Errorf("stream chunks of the expired run = %d, %v", chunks, err)
	}
	for _, event := range storedEvents(t, world, done) {
		if payload := worlds.EventPayload(event); payload != nil {
			t.Fatalf("%s of the expired run holds payload %s", event.EventType, payload)
		}
	}
	if run := rowCounts(t, world, alive); run["workflow_runs"] != 1 || purgedData(t, world, alive) {
		t.Error("a running run was expired")
	}
}

func TestExpireRunsByWorkflow(t *testing.T) {
	url, schema := testSchema(t)
	world := testWorld(t, testPool(t, url, schema), Config{
		Retention:  time.Hour,
		Retentions: map[string]time.Duration{"short": time.Minute, "kept": 0},
	})
	ctx := context.Background()
	for workflow, want := range map[string]time.Duration{"short": time.Minute, "kept": 0, "other": time.Hour} {
		runID := retentionRunOf(t, world, workflow+"_run", workflow)
		complete(t, world, runID)
		run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID})
		if err != nil {
			t.Fatal(err)
		}
		switch {
		case want == 0 && run.ExpiredAt != nil:
			t.Errorf("%s run expires at %v; want kept", workflow, run.ExpiredAt)
		case want > 0 && (run.ExpiredAt == nil || run.ExpiredAt.Sub(*run.CompletedAt) != want):
			t.Errorf("%s run expires at %v; want %v after it completed", workflow, run.ExpiredAt, want)
		}
	}
}

func TestWorkerExpiresRuns(t *testing.T) {
	url, schema := testSchema(t)
	world := testWorld(t, testPool(t, url, schema), Config{Retention: time.Millisecond})
	done := retentionRun(t, world, "worker")
	complete(t, world, done)
	// Let the retention pass, since Worker checks once at start and then
	// every minute.
	time.Sleep(20 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan error, 1)
	go func() { stopped <- Worker(ctx, world, worlds.Runners()) }()
	deadline := time.Now().Add(10 * time.Second)
	for !purgedData(t, world, done) {
		if time.Now().After(deadline) {
			t.Fatal("Worker did not delete the finished run's data")
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	if err := <-stopped; err != context.Canceled {
		t.Fatalf("Worker returned %v", err)
	}
}

// purgedData reports whether a run's rows no longer hold its data.
func purgedData(t *testing.T, world *World, runID worlds.RunID) bool {
	t.Helper()
	var purged bool
	if err := world.pool.QueryRow(context.Background(), `SELECT data_purged FROM workflow_runs WHERE id = $1`, runID).Scan(&purged); err != nil {
		t.Fatal(err)
	}
	return purged
}

// retentionRun creates a started run of the retention workflow with a
// completed step.
func retentionRun(t *testing.T, world *World, name string) worlds.RunID {
	t.Helper()
	return retentionRunOf(t, world, name, "retention")
}

func retentionRunOf(t *testing.T, world *World, name, workflow string) worlds.RunID {
	t.Helper()
	runID := worlds.NewRunID()
	if _, err := world.EventsCreateRun(context.Background(), worlds.EventsCreateRunInput{RunID: runID, WorkflowName: workflow, Input: []byte(`"input"`)}); err != nil {
		t.Fatal(err)
	}
	writeEvent(t, world, runID, worlds.EventTypeRunStarted, "", nil)
	writeEvent(t, world, runID, worlds.EventTypeStepCreated, "step_"+name, worlds.DataStepCreated{StepName: "step", Input: json.RawMessage(`1`)})
	writeEvent(t, world, runID, worlds.EventTypeStepStarted, "step_"+name, worlds.DataStepStarted{})
	writeEvent(t, world, runID, worlds.EventTypeStepCompleted, "step_"+name, worlds.DataStepResult{Result: json.RawMessage(`2`)})
	return runID
}

func complete(t *testing.T, world *World, runID worlds.RunID) {
	t.Helper()
	writeEvent(t, world, runID, worlds.EventTypeRunCompleted, "", worlds.DataRunCompleted{Output: json.RawMessage(`"ok"`)})
}

func writeEvent(t *testing.T, world *World, runID worlds.RunID, eventType worlds.EventType, correlationID string, data any) {
	t.Helper()
	event := worlds.EventsCreateEvent{EventType: eventType, CorrelationID: correlationID}
	if data != nil {
		encoded, err := json.Marshal(data)
		if err != nil {
			t.Fatal(err)
		}
		event.Data = encoded
	}
	if _, err := world.EventsCreate(context.Background(), worlds.EventsCreateInput{RunID: runID, Event: event}); err != nil {
		t.Fatalf("EventsCreate(%s %s): %v", eventType, correlationID, err)
	}
}

// rowCounts returns how many rows of runID each table holds.
func rowCounts(t *testing.T, world *World, runID worlds.RunID) map[string]int {
	t.Helper()
	counts := map[string]int{}
	for _, table := range retentionTables {
		var count int
		column := "run_id"
		if table == "workflow_runs" {
			column = "id"
		}
		if err := world.pool.QueryRow(context.Background(), `SELECT count(*) FROM `+table+` WHERE `+column+` = $1`, string(runID)).Scan(&count); err != nil {
			t.Fatal(err)
		}
		counts[table] = count
	}
	return counts
}

func TestPurgeDeletesStartHookMetadata(t *testing.T) {
	url, schema := testSchema(t)
	world := testWorld(t, testPool(t, url, schema), Config{})
	ctx := context.Background()
	runID := runRetentionStartHook
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "retention", StartHook: &worlds.StartHook{Token: "purged-start", Metadata: []byte(`"metadata"`)}, Attributes: map[string]string{worlds.RetentionAttribute: worlds.RetainNone.String()}, AllowReservedAttributes: true}); err != nil {
		t.Fatal(err)
	}
	writeEvent(t, world, runID, worlds.EventTypeRunCancelled, "", nil)
	created := storedEvents(t, world, runID)[0]
	start := worlds.RunStartHook(created.Data)
	if start == nil || start.Token != "purged-start" || start.Metadata != nil {
		t.Fatalf("run_created start hook after purge = %+v", start)
	}
	_ = ctx
}

// storedEvents returns the run's events as its rows hold them.
func storedEvents(t *testing.T, world *World, runID worlds.RunID) []worlds.Event {
	t.Helper()
	rows, err := world.pool.Query(context.Background(), world.q(`SELECT `+eventColumns+` FROM workflow.workflow_events WHERE run_id = $1 ORDER BY id`), runID)
	if err != nil {
		t.Fatal(err)
	}
	events, err := collect(rows, scanEvent)
	if err != nil {
		t.Fatal(err)
	}
	return events
}

// runRetentionStartHook is a run ID minted when the tests start, as a World requires
// of a new run.
var runRetentionStartHook = worlds.NewRunID()
