package localworld_test

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/workflowtest"
	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/core"
	localworld "github.com/eriicafes/go-workflow/worlds/local"
)

func TestConformance(t *testing.T) {
	t.Parallel()
	workflowtest.Conformance(t, func(t *testing.T) worlds.World {
		return startWorld(t, localworld.Config{Dir: t.TempDir()}, nil)
	})
}

// TestConformanceShared runs the concurrency contract against two Worlds on
// one data directory, as two processes would share it.
func TestConformanceShared(t *testing.T) {
	t.Parallel()
	workflowtest.ConformanceShared(t, func(t *testing.T) (worlds.World, worlds.World) {
		dir := t.TempDir()
		return startWorld(t, localworld.Config{Dir: dir}, nil), startWorld(t, localworld.Config{Dir: dir}, nil)
	})
}

func TestBaseURL(t *testing.T) {
	t.Parallel()
	world, err := localworld.Open(localworld.Config{Dir: t.TempDir()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer world.Close()
	if world.BaseURL() != "http://localhost:3000/v1/workflow" {
		t.Fatalf("default BaseURL = %q", world.BaseURL())
	}
	custom, err := localworld.Open(localworld.Config{Dir: t.TempDir(), BaseURL: "https://example.com/wf/"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer custom.Close()
	if custom.BaseURL() != "https://example.com/wf" {
		t.Fatalf("custom BaseURL = %q", custom.BaseURL())
	}
	if _, err := localworld.Open(localworld.Config{Dir: t.TempDir(), BaseURL: "localhost"}, nil); err == nil {
		t.Fatal("invalid BaseURL accepted")
	}
}

func TestRequiresPrepare(t *testing.T) {
	t.Parallel()
	world, err := localworld.Open(localworld.Config{Dir: t.TempDir()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runA}); !errors.Is(err, localworld.ErrNotPrepared) {
		t.Fatalf("read before Prepare = %v", err)
	}
	if _, err := world.Queue(ctx, worlds.QueueInput{RunID: runA}); !errors.Is(err, localworld.ErrNotPrepared) {
		t.Fatalf("queue before Prepare = %v", err)
	}
	if err := world.Close(); err != nil {
		t.Fatal(err)
	}
	if err := world.Prepare(ctx); !errors.Is(err, localworld.ErrClosed) {
		t.Fatalf("Prepare after Close = %v", err)
	}
	if _, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runA}); !errors.Is(err, localworld.ErrClosed) {
		t.Fatalf("read after Close = %v", err)
	}
}

func TestDataDirDefaultsToTheEnvironment(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("WORKFLOW_LOCAL_DATA_DIR", dir)
	world := startWorld(t, localworld.Config{}, nil)
	if _, err := world.EventsCreateRun(context.Background(), worlds.EventsCreateRunInput{RunID: runEnv, WorkflowName: "env"}); err != nil {
		t.Fatal(err)
	}
	if count := queryInt(t, dir, "SELECT count(*) FROM runs"); count != 1 {
		t.Fatalf("runs in %s = %d", dir, count)
	}
}

func TestPersistsAcrossReopen(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	world := startWorld(t, localworld.Config{Dir: dir}, nil)
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runPersist, WorkflowName: "persist", Input: []byte(`"in"`), Attributes: map[string]string{"k": "v"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := world.EventsCreateBatch(ctx, worlds.EventsCreateBatchInput{RunID: runPersist, Events: []worlds.EventsCreateEvent{
		{EventType: worlds.EventTypeRunStarted},
		{EventType: worlds.EventTypeStepCreated, CorrelationID: "step_a", Data: []byte(`{"stepName":"a"}`)},
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runPersist, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeHookCreated, CorrelationID: "hook_a", Data: []byte(`{"token":"Persist/Token"}`)}}); err != nil {
		t.Fatal(err)
	}
	if err := world.StreamsWriteMulti(ctx, worlds.StreamsWriteMultiInput{RunID: runPersist, Name: "out/1", Chunks: [][]byte{[]byte("x"), []byte("y")}}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"held", ""} {
		if err := world.StreamsClose(ctx, worlds.StreamsCloseInput{RunID: runPersist, Name: name}); err != nil {
			t.Fatal(err)
		}
	}
	if err := world.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := startWorld(t, localworld.Config{Dir: dir, SkipRecovery: true}, nil)
	run, err := reopened.RunsGet(ctx, worlds.RunsGetInput{RunID: runPersist})
	if err != nil || run.Status != worlds.StatusRunning || string(run.Input) != `"in"` || run.Attributes["k"] != "v" {
		t.Fatalf("run = %#v err=%v", run, err)
	}
	events, err := reopened.EventsList(ctx, worlds.EventsListInput{RunID: runPersist})
	if err != nil || len(events.Data) != 4 {
		t.Fatalf("events = %#v err=%v", events, err)
	}
	if step, err := reopened.StepsGet(ctx, worlds.StepsGetInput{RunID: runPersist, StepID: "step_a"}); err != nil || step.StepName != "a" {
		t.Fatalf("step = %#v err=%v", step, err)
	}
	if hook, err := reopened.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: "Persist/Token"}); err != nil || hook.HookID != "hook_a" {
		t.Fatalf("hook = %#v err=%v", hook, err)
	}
	if _, err := reopened.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: "persist/token"}); !errors.Is(err, worlds.ErrHookNotFound) {
		t.Fatalf("tokens differing only by case collided: %v", err)
	}
	chunks, err := reopened.StreamsGetChunks(ctx, worlds.StreamsGetChunksInput{RunID: runPersist, Name: "out/1"})
	if err != nil || len(chunks.Data) != 2 || string(chunks.Data[1].Data) != "y" || chunks.Data[1].Index != 1 {
		t.Fatalf("chunks = %#v err=%v", chunks, err)
	}
	names, err := reopened.StreamsList(ctx, worlds.StreamsListInput{RunID: runPersist})
	if err != nil || len(names) != 3 || names[0] != "" || names[1] != "held" || names[2] != "out/1" {
		t.Fatalf("stream names = %q err=%v", names, err)
	}
	for _, name := range []string{"held", ""} {
		if info, err := reopened.StreamsGetInfo(ctx, worlds.StreamsGetInfoInput{RunID: runPersist, Name: name}); err != nil || !info.Done {
			t.Fatalf("closed stream %q after restart = %#v err=%v", name, info, err)
		}
	}
	next, err := reopened.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runPersist, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeWaitCreated, CorrelationID: "w"}})
	if err != nil || next.EventID != worlds.EventIDForSlot(5) {
		t.Fatalf("append after reopen = %#v err=%v", next, err)
	}
}

func TestViewsHoldNoPayloadsAndEventsHoldCBOR(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	world := startWorld(t, localworld.Config{Dir: dir}, nil)
	runID := runPayloads
	// Payloads are bytes, base64 in event JSON.
	b64 := func(value string) string { return base64.StdEncoding.EncodeToString([]byte(value)) }
	metadata := b64(`"hook-metadata-bytes"`)
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "payloads", Input: []byte(`"run-input-bytes"`)}); err != nil {
		t.Fatal(err)
	}
	for _, event := range []worlds.EventsCreateEvent{
		{EventType: worlds.EventTypeRunStarted},
		{EventType: worlds.EventTypeStepCreated, CorrelationID: "step_a", Data: []byte(`{"stepName":"a","input":"` + b64(`"step-input-bytes"`) + `"}`)},
		{EventType: worlds.EventTypeStepStarted, CorrelationID: "step_a"},
		{EventType: worlds.EventTypeStepRetrying, CorrelationID: "step_a", Data: []byte(`{"error":"` + b64(`"step-error-bytes"`) + `"}`)},
		{EventType: worlds.EventTypeStepStarted, CorrelationID: "step_a"},
		{EventType: worlds.EventTypeStepCompleted, CorrelationID: "step_a", Data: []byte(`{"result":"` + b64(`"step-output-bytes"`) + `"}`)},
		// The hook's retained token keeps it readable after its run ends.
		{EventType: worlds.EventTypeHookCreated, CorrelationID: "hook_a", Data: []byte(`{"token":"payload-token","metadata":"` + metadata + `","tokenRetentionUntil":"` + time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano) + `"}`)},
		{EventType: worlds.EventTypeRunCompleted, Data: []byte(`{"output":"` + b64(`"run-output-bytes"`) + `"}`)},
	} {
		if _, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: event}); err != nil {
			t.Fatalf("%s: %v", event.EventType, err)
		}
	}
	payloads := []string{"run-input-bytes", "run-output-bytes", "step-input-bytes", "step-error-bytes", "step-output-bytes", "hook-metadata-bytes"}
	for _, query := range []string{"SELECT doc FROM runs", "SELECT doc FROM steps", "SELECT doc FROM hooks"} {
		for _, doc := range queryStrings(t, dir, query) {
			for _, payload := range payloads {
				if strings.Contains(doc, payload) || strings.Contains(doc, b64(`"`+payload+`"`)) {
					t.Fatalf("%s holds payload %s: %s", query, payload, doc)
				}
			}
		}
	}
	// Events hold their payloads as raw bytes, not base64.
	docs := strings.Join(queryStrings(t, dir, "SELECT doc FROM events"), "")
	for _, payload := range payloads {
		if !strings.Contains(docs, payload) || strings.Contains(docs, b64(`"`+payload+`"`)) {
			t.Fatalf("events do not hold %s as raw bytes", payload)
		}
	}
	run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID})
	if err != nil || string(run.Input) != `"run-input-bytes"` || string(run.Output) != `"run-output-bytes"` || run.Error != nil {
		t.Fatalf("run = %#v err=%v", run, err)
	}
	step, err := world.StepsGet(ctx, worlds.StepsGetInput{RunID: runID, StepID: "step_a"})
	if err != nil || string(step.Input) != `"step-input-bytes"` || string(step.Output) != `"step-output-bytes"` || step.Error != nil || step.Attempt != 2 {
		t.Fatalf("step = %#v err=%v", step, err)
	}
	hook, err := world.HooksGet(ctx, worlds.HooksGetInput{HookID: "hook_a"})
	if err != nil || string(hook.Metadata) != `"hook-metadata-bytes"` {
		t.Fatalf("hook = %#v err=%v", hook, err)
	}
	stripped, err := world.HooksGet(ctx, worlds.HooksGetInput{HookID: "hook_a", ResolveData: worlds.ResolveDataNone})
	if err != nil || stripped.Metadata != nil {
		t.Fatalf("ResolveDataNone hook = %#v err=%v", stripped, err)
	}
}

func TestZeroRetentionPurgesWithTheRunsEnd(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	world := startWorld(t, localworld.Config{Dir: dir}, nil)
	runID := runPurge
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "purge", Input: []byte(`"run-input-bytes"`), Attributes: map[string]string{"$retention": "0"}, AllowReservedAttributes: true}); err != nil {
		t.Fatal(err)
	}
	if err := world.StreamsWrite(ctx, worlds.StreamsWriteInput{RunID: runID, Name: "out", Chunk: []byte("chunk-bytes")}); err != nil {
		t.Fatal(err)
	}
	for _, eventType := range []worlds.EventType{worlds.EventTypeRunStarted, worlds.EventTypeRunCancelled} {
		if _, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: eventType}}); err != nil {
			t.Fatal(err)
		}
	}
	docs := strings.Join(queryStrings(t, dir, "SELECT doc FROM events"), "")
	if strings.Contains(docs, "run-input-bytes") {
		t.Fatal("run_created kept its input")
	}
	if count := queryInt(t, dir, "SELECT count(*) FROM stream_chunks WHERE eof = 0"); count != 0 {
		t.Fatalf("chunks left: %d", count)
	}
	run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID})
	if err != nil || run.ExpiredAt == nil {
		t.Fatalf("run = %#v err=%v", run, err)
	}
	chunks, err := world.StreamsGetChunks(ctx, worlds.StreamsGetChunksInput{RunID: runID, Name: "out"})
	if err != nil || len(chunks.Data) != 0 || !chunks.Done {
		t.Fatalf("chunks = %+v, err=%v", chunks, err)
	}
	if err := world.StreamsWrite(ctx, worlds.StreamsWriteInput{RunID: runID, Name: "new", Chunk: []byte("x")}); !errors.Is(err, worlds.ErrStreamClosed) {
		t.Fatalf("write to a purged run = %v", err)
	}
}

func TestRefusesANewerSchema(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	world := startWorld(t, localworld.Config{Dir: dir}, nil)
	if err := world.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(dir, "world.db"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("PRAGMA user_version = 2"); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	newer, err := localworld.Open(localworld.Config{Dir: dir}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer newer.Close()
	if err := newer.Prepare(context.Background()); err == nil || !strings.Contains(err.Error(), "newer release") {
		t.Fatalf("Prepare = %v, want the newer schema refused", err)
	}
}

func TestTagIsolation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	untagged := startWorld(t, localworld.Config{Dir: dir}, nil)
	a := startWorld(t, localworld.Config{Dir: dir, Tag: "a"}, nil)
	b := startWorld(t, localworld.Config{Dir: dir, Tag: "b"}, nil)
	for world, id := range map[*localworld.World]worlds.RunID{untagged: runShared, a: runA, b: runB} {
		if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: id, WorkflowName: "tags"}); err != nil {
			t.Fatal(err)
		}
		// A stream name carries its run, as the Workflow SDK names streams.
		if err := world.StreamsWrite(ctx, worlds.StreamsWriteInput{RunID: id, Name: worlds.UserStreamName(id, "out"), Chunk: []byte("x")}); err != nil {
			t.Fatal(err)
		}
	}
	// A tagged World sees its tag and untagged rows; an untagged World only
	// untagged ones.
	for _, check := range []struct {
		world   *localworld.World
		visible []worlds.RunID
		hidden  []worlds.RunID
	}{
		{untagged, []worlds.RunID{runShared}, []worlds.RunID{runA, runB}},
		{a, []worlds.RunID{runShared, runA}, []worlds.RunID{runB}},
		{b, []worlds.RunID{runShared, runB}, []worlds.RunID{runA}},
	} {
		for _, id := range check.visible {
			if _, err := check.world.RunsGet(ctx, worlds.RunsGetInput{RunID: id}); err != nil {
				t.Fatalf("visible run %s: %v", id, err)
			}
		}
		for _, id := range check.hidden {
			if _, err := check.world.RunsGet(ctx, worlds.RunsGetInput{RunID: id}); !errors.Is(err, worlds.ErrRunNotFound) {
				t.Fatalf("hidden run %s = %v", id, err)
			}
			if _, err := check.world.EventsList(ctx, worlds.EventsListInput{RunID: id}); !errors.Is(err, worlds.ErrRunNotFound) {
				t.Fatalf("hidden run %s events = %v", id, err)
			}
			if names, err := check.world.StreamsList(ctx, worlds.StreamsListInput{RunID: id}); err != nil || len(names) != 0 {
				t.Fatalf("hidden run %s streams = %q err=%v", id, names, err)
			}
		}
		page, err := check.world.RunsList(ctx, worlds.RunsListInput{})
		if err != nil || len(page.Data) != len(check.visible) {
			t.Fatalf("listed %d runs, want %d: %v", len(page.Data), len(check.visible), err)
		}
	}
	// Clearing a tag removes only its data.
	if err := a.Clear(ctx); err != nil {
		t.Fatal(err)
	}
	if count := queryInt(t, dir, "SELECT count(*) FROM runs"); count != 2 {
		t.Fatalf("runs after clearing a tag = %d", count)
	}
	if count := queryInt(t, dir, "SELECT count(*) FROM stream_chunks WHERE tag = 'a'"); count != 0 {
		t.Fatalf("chunks of the cleared tag = %d", count)
	}
	if _, err := b.RunsGet(ctx, worlds.RunsGetInput{RunID: runB}); err != nil {
		t.Fatalf("other tag's run after clear: %v", err)
	}
}

type doubleInput struct{ Value int }
type doubleOutput struct{ Value int }

func (doubleInput) Workflow(doubleOutput) {}

func TestRunsWorkflows(t *testing.T) {
	t.Parallel()
	world := startWorld(t, localworld.Config{Dir: t.TempDir()}, core.Runners(workflow.Use(double)))
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), 10*time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, doubleInput{Value: 21})
	if err != nil {
		t.Fatal(err)
	}
	output, err := run.Value(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if output.Value != 42 {
		t.Fatalf("output = %#v", output)
	}
}

func TestRecoveryQueuesActiveRunsAgain(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	runners := core.Runners(workflow.Use(double))
	world := startWorld(t, localworld.Config{Dir: dir, Tag: "mine"}, runners)
	input, err := worlds.EncodePayload([1]doubleInput{{Value: 4}}, worlds.PayloadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// A run whose flow was queued in memory when the process stopped, and
	// one of another tag, which this World leaves alone.
	runID := worlds.NewRunID()
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: workflow.Use(double).Name(), Input: input}); err != nil {
		t.Fatal(err)
	}
	later := time.Now().Add(time.Hour)
	if _, err := world.Queue(ctx, worlds.QueueInput{RunID: runID, NotBefore: &later}); err != nil {
		t.Fatal(err)
	}
	other := startWorld(t, localworld.Config{Dir: dir, Tag: "theirs", SkipRecovery: true}, nil)
	otherID := worlds.NewRunID()
	if _, err := other.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: otherID, WorkflowName: workflow.Use(double).Name(), Input: input}); err != nil {
		t.Fatal(err)
	}
	if err := world.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := startWorld(t, localworld.Config{Dir: dir, Tag: "mine"}, runners)
	run, err := reopened.RunsWaitForTerminalStatus(ctx, worlds.RunsWaitForTerminalStatusInput{RunID: runID, Timeout: 10 * time.Second})
	var output doubleInput
	if err != nil || run.Status != worlds.StatusCompleted || worlds.DecodePayload(run.Output, nil, &output) != nil || output.Value != 8 {
		t.Fatalf("run after restart = %#v err=%v", run, err)
	}
	if run, err := other.RunsGet(ctx, worlds.RunsGetInput{RunID: otherID}); err != nil || run.Status != worlds.StatusPending {
		t.Fatalf("other tag's run = %#v err=%v", run, err)
	}
}

func TestClear(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	world := startWorld(t, localworld.Config{Dir: dir}, nil)
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runClear, WorkflowName: "clear"}); err != nil {
		t.Fatal(err)
	}
	later := time.Now().Add(time.Hour)
	if _, err := world.Queue(ctx, worlds.QueueInput{RunID: runClear, NotBefore: &later}); err != nil {
		t.Fatal(err)
	}
	if err := world.Clear(ctx); err != nil {
		t.Fatal(err)
	}
	if count := queryInt(t, dir, "SELECT count(*) FROM runs") + queryInt(t, dir, "SELECT count(*) FROM events"); count != 0 {
		t.Fatalf("Clear left %d rows", count)
	}
	// The World stays open, as world-local's clear() leaves it.
	if _, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runClear}); !errors.Is(err, worlds.ErrRunNotFound) {
		t.Fatalf("run after Clear = %v", err)
	}
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runAfter, WorkflowName: "clear"}); err != nil {
		t.Fatalf("create after Clear: %v", err)
	}
	if _, err := world.Queue(ctx, worlds.QueueInput{RunID: runAfter}); err != nil {
		t.Fatalf("queue after Clear: %v", err)
	}
	if err := world.Clear(ctx); err != nil {
		t.Fatalf("second Clear = %v", err)
	}
	if err := world.Close(); err != nil {
		t.Fatal(err)
	}
	if err := world.Clear(ctx); !errors.Is(err, localworld.ErrClosed) {
		t.Fatalf("Clear after Close = %v", err)
	}

	unprepared, err := localworld.Open(localworld.Config{Dir: t.TempDir()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer unprepared.Close()
	if err := unprepared.Clear(ctx); !errors.Is(err, localworld.ErrNotPrepared) {
		t.Fatalf("Clear before Prepare = %v", err)
	}
}

func TestHookRetentionIsCapped(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	world := startWorld(t, localworld.Config{Dir: t.TempDir(), HookRetentionLimit: time.Hour}, nil)
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runRetain, WorkflowName: "retain"}); err != nil {
		t.Fatal(err)
	}
	until := time.Now().Add(2 * time.Hour)
	data, _ := json.Marshal(worlds.DataHookCreated{Token: "retained", TokenRetentionUntil: &until})
	if _, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runRetain, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeHookCreated, CorrelationID: "hook_retained", Data: data}}); !errors.Is(err, worlds.ErrEventInvalid) {
		t.Fatalf("hook past the retention limit = %v", err)
	}
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: worlds.NewRunID(), WorkflowName: "retain", StartHook: &worlds.StartHook{Token: "start", TokenRetentionUntil: &until}}); !errors.Is(err, worlds.ErrStartHookRejected) {
		t.Fatalf("start hook past the retention limit = %v", err)
	}
}

func startWorld(t *testing.T, config localworld.Config, runners core.RunnerSet) *localworld.World {
	t.Helper()
	world, err := localworld.Open(config, runners)
	if err != nil {
		t.Fatal(err)
	}
	if err := world.Prepare(context.Background()); err != nil {
		_ = world.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = world.Close() })
	return world
}

// queryStrings returns the single column of query's rows in dir's database.
func queryStrings(t *testing.T, dir, query string) []string {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(dir, "world.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var values []string
	for rows.Next() {
		var value []byte
		if err := rows.Scan(&value); err != nil {
			t.Fatal(err)
		}
		values = append(values, string(value))
	}
	return values
}

func queryInt(t *testing.T, dir, query string) int {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(dir, "world.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var value int
	if err := db.QueryRow(query).Scan(&value); err != nil {
		t.Fatal(err)
	}
	return value
}

func double(ctx context.Context, input doubleInput) (doubleOutput, error) {
	value, err := workflow.Step(ctx, func(_ context.Context, value int) (int, error) { return value * 2, nil }, input.Value)
	return doubleOutput{Value: value}, err
}

func TestQueueDeduplicatesByIdempotencyKey(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	world := startWorld(t, localworld.Config{Dir: t.TempDir()}, nil)
	later := time.Now().Add(time.Hour)
	first, err := world.Queue(ctx, worlds.QueueInput{RunID: runA, NotBefore: &later, IdempotencyKey: "k"})
	if err != nil {
		t.Fatal(err)
	}
	again, err := world.Queue(ctx, worlds.QueueInput{RunID: runA, NotBefore: &later, IdempotencyKey: "k"})
	if err != nil || again.MessageID != first.MessageID {
		t.Fatalf("duplicate key = %#v err=%v, want %s", again, err, first.MessageID)
	}
	if !strings.HasPrefix(first.MessageID, "msg_") {
		t.Fatalf("message ID %q", first.MessageID)
	}
	unkeyed, err := world.Queue(ctx, worlds.QueueInput{RunID: runA, NotBefore: &later})
	if err != nil || unkeyed.MessageID == first.MessageID {
		t.Fatalf("unkeyed = %#v err=%v", unkeyed, err)
	}
}

// runClear is a run ID minted when the tests start, as a World requires
// of a new run.
var runClear = worlds.NewRunID()

// runEnv is a run ID minted when the tests start, as a World requires
// of a new run.
var runEnv = worlds.NewRunID()

// runPayloads is a run ID minted when the tests start, as a World requires
// of a new run.
var runPayloads = worlds.NewRunID()

// runPersist is a run ID minted when the tests start, as a World requires
// of a new run.
var runPersist = worlds.NewRunID()

// runPurge is a run ID minted when the tests start, as a World requires
// of a new run.
var runPurge = worlds.NewRunID()

// runRetain is a run ID minted when the tests start, as a World requires
// of a new run.
var runRetain = worlds.NewRunID()

// runShared is a run ID minted when the tests start, as a World requires
// of a new run.
var runShared = worlds.NewRunID()

// runA is a run ID minted when the tests start, as a World requires
// of a new run.
var runA = worlds.NewRunID()

// runAfter is a run ID minted when the tests start, as a World requires
// of a new run.
var runAfter = worlds.NewRunID()

// runB is a run ID minted when the tests start, as a World requires
// of a new run.
var runB = worlds.NewRunID()
