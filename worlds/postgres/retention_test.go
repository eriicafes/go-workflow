package postgresworld

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/core"
)

// retentionTables are the tables that hold rows of a run.
var retentionTables = []string{"go_workflow_runs", "go_workflow_events", "go_workflow_steps", "go_workflow_hooks", "go_workflow_queue", "go_workflow_streams", "go_workflow_stream_chunks"}

func TestExpireRuns(t *testing.T) {
	url, schema := testSchema(t)
	world := testWorld(t, testPool(t, url, schema), Config{Retention: time.Hour})
	ctx := context.Background()

	done := retentionRun(t, world, "done")
	writeEvent(t, world, done, worlds.EventTypeHookCreated, "hook_done", worlds.DataHookCreated{Token: "token_done", Metadata: []byte(`"metadata"`)})
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
	if _, err := world.pool.Exec(ctx, `UPDATE go_workflow_runs SET expired_at = now() - interval '1 second' WHERE run_id = $1`, done); err != nil {
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
	// Its rows stay, as in the Workflow SDK, and the chunks go.
	counts := rowCounts(t, world, done)
	for _, table := range []string{"go_workflow_runs", "go_workflow_events", "go_workflow_steps", "go_workflow_hooks", "go_workflow_streams"} {
		if counts[table] == 0 {
			t.Errorf("%s lost the expired run's rows", table)
		}
	}
	if counts["go_workflow_stream_chunks"] != 0 {
		t.Errorf("stream chunks of the expired run = %d", counts["go_workflow_stream_chunks"])
	}
	var payloads int
	if err := world.pool.QueryRow(ctx, `SELECT count(*) FROM go_workflow_events WHERE run_id = $1 AND (convert_from(data, 'UTF8')::jsonb ?| array['input', 'output', 'result', 'error', 'metadata', 'payload'])`, done).Scan(&payloads); err != nil || payloads != 0 {
		t.Fatalf("events of the expired run with payloads = %d, %v", payloads, err)
	}
	if run := rowCounts(t, world, alive); run["go_workflow_runs"] != 1 || purgedData(t, world, alive) {
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
	go func() { stopped <- Worker(ctx, world, core.Runners()) }()
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
	if err := world.pool.QueryRow(context.Background(), `SELECT data_purged FROM go_workflow_runs WHERE run_id = $1`, runID).Scan(&purged); err != nil {
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
	runID := worlds.RunID("wrun_retention_" + name)
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
		if err := world.pool.QueryRow(context.Background(), `SELECT count(*) FROM `+table+` WHERE run_id = $1`, string(runID)).Scan(&count); err != nil {
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
	runID := worlds.RunID("wrun_retention_start_hook")
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "retention", StartHook: &worlds.StartHook{Token: "purged-start", Metadata: []byte(`"metadata"`)}, Attributes: map[string]string{worlds.RetentionAttribute: worlds.RetainNone.String()}}); err != nil {
		t.Fatal(err)
	}
	writeEvent(t, world, runID, worlds.EventTypeRunCancelled, "", nil)
	var left int
	if err := world.pool.QueryRow(ctx, `SELECT count(*) FROM go_workflow_events WHERE run_id = $1 AND convert_from(data, 'UTF8')::jsonb #> '{startHook,metadata}' IS NOT NULL`, runID).Scan(&left); err != nil || left != 0 {
		t.Fatalf("run_created rows still holding start hook metadata = %d, %v", left, err)
	}
	var token string
	if err := world.pool.QueryRow(ctx, `SELECT convert_from(data, 'UTF8')::jsonb #>> '{startHook,token}' FROM go_workflow_events WHERE run_id = $1 AND slot = 1`, runID).Scan(&token); err != nil || token != "purged-start" {
		t.Fatalf("start hook token after purge = %q, %v", token, err)
	}
}
