package postgresworld

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// retentionTables are the tables that hold rows of a run.
var retentionTables = []string{"go_workflow_runs", "go_workflow_events", "go_workflow_steps", "go_workflow_hooks", "go_workflow_queue", "go_workflow_streams", "go_workflow_stream_chunks"}

func TestExpireRuns(t *testing.T) {
	url, schema := testSchema(t)
	world := testWorld(t, testPool(t, url, schema), Config{Retention: time.Hour})
	ctx := context.Background()

	done := retentionRun(t, world, "done")
	writeEvent(t, world, done, worlds.EventTypeHookCreated, "hook_done", worlds.DataHookCreated{Token: "token_done"})
	if err := world.StreamsWrite(ctx, worlds.StreamsWriteInput{RunID: done, Name: "out", Chunk: []byte("chunk")}); err != nil {
		t.Fatal(err)
	}
	later := time.Now().Add(time.Hour)
	if _, err := world.Queue(ctx, worlds.QueueInput{RunID: done, WorkflowName: "retention", NotBefore: &later}); err != nil {
		t.Fatal(err)
	}
	complete(t, world, done)

	alive := retentionRun(t, world, "alive")

	// A hook whose token retention has not ended keeps its run.
	retained := retentionRun(t, world, "retained")
	writeEvent(t, world, retained, worlds.EventTypeHookCreated, "hook_retained", worlds.DataHookCreated{Token: "token_retained", TokenRetentionUntil: &later})
	complete(t, world, retained)

	before := rowCounts(t, world, done)
	for _, table := range []string{"go_workflow_runs", "go_workflow_events", "go_workflow_steps", "go_workflow_hooks", "go_workflow_queue", "go_workflow_streams", "go_workflow_stream_chunks"} {
		if before[table] == 0 {
			t.Fatalf("the finished run has no rows in %s to expire", table)
		}
	}

	// Nothing has been kept for the hour yet.
	if n, err := world.expireRuns(ctx, time.Now()); err != nil || n != 0 {
		t.Fatalf("expireRuns before retention = %d, %v", n, err)
	}
	n, err := world.expireRuns(ctx, time.Now().Add(2*time.Hour))
	if err != nil || n != 1 {
		t.Fatalf("expireRuns = %d, %v; want the finished run only", n, err)
	}
	for table, count := range rowCounts(t, world, done) {
		if count != 0 {
			t.Errorf("%s still has %d rows of the expired run", table, count)
		}
	}
	var tokens int
	if err := world.pool.QueryRow(ctx, `SELECT count(*) FROM go_workflow_hook_tokens WHERE hook_id = 'hook_done'`).Scan(&tokens); err != nil || tokens != 0 {
		t.Errorf("hook tokens of the expired run = %d, %v", tokens, err)
	}
	for _, runID := range []worlds.RunID{alive, retained} {
		if rowCounts(t, world, runID)["go_workflow_runs"] != 1 {
			t.Errorf("%s was expired", runID)
		}
	}
	page, err := world.RunsList(ctx, worlds.RunsListInput{})
	if err != nil || len(page.Data) != 2 {
		t.Fatalf("RunsList = %d runs, %v; want the two kept", len(page.Data), err)
	}
}

func TestExpireRunsByWorkflow(t *testing.T) {
	url, schema := testSchema(t)
	world := testWorld(t, testPool(t, url, schema), Config{
		Retention:  time.Hour,
		Retentions: map[string]time.Duration{"short": time.Minute, "kept": 0},
	})
	short := retentionRunOf(t, world, "short_run", "short")
	complete(t, world, short)
	kept := retentionRunOf(t, world, "kept_run", "kept")
	complete(t, world, kept)
	other := retentionRunOf(t, world, "other_run", "other")
	complete(t, world, other)

	// Past the short workflow's retention, before the default's.
	n, err := world.expireRuns(context.Background(), time.Now().Add(10*time.Minute))
	if err != nil || n != 1 || rowCounts(t, world, short)["go_workflow_runs"] != 0 {
		t.Fatalf("expireRuns after a minute = %d, %v; want only the short run", n, err)
	}
	// Past the default; the kept workflow's runs stay forever.
	n, err = world.expireRuns(context.Background(), time.Now().Add(1000*time.Hour))
	if err != nil || n != 1 || rowCounts(t, world, other)["go_workflow_runs"] != 0 {
		t.Fatalf("expireRuns after the default = %d, %v; want only the other run", n, err)
	}
	if rowCounts(t, world, kept)["go_workflow_runs"] != 1 {
		t.Fatal("a run of a workflow kept forever was expired")
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
	for rowCounts(t, world, done)["go_workflow_runs"] != 0 {
		if time.Now().After(deadline) {
			t.Fatal("Worker did not expire the finished run")
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	if err := <-stopped; err != context.Canceled {
		t.Fatalf("Worker returned %v", err)
	}
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
