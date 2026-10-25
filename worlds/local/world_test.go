package localworld_test

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/workflowtest"
	"github.com/eriicafes/go-workflow/worlds"
	localworld "github.com/eriicafes/go-workflow/worlds/local"
)

func TestConformance(t *testing.T) {
	t.Parallel()
	workflowtest.Conformance(t, func(t *testing.T) worlds.World {
		return startWorld(t, t.TempDir(), nil)
	})
}

// TestConformanceShared runs the concurrency contract against one World, the
// only configuration a directory supports.
func TestConformanceShared(t *testing.T) {
	t.Parallel()
	workflowtest.ConformanceShared(t, func(t *testing.T) (worlds.World, worlds.World) {
		world := startWorld(t, t.TempDir(), nil)
		return world, world
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
	if _, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: "wrun_a"}); !errors.Is(err, localworld.ErrNotPrepared) {
		t.Fatalf("read before Prepare = %v", err)
	}
	if _, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_a"}); !errors.Is(err, localworld.ErrNotPrepared) {
		t.Fatalf("queue before Prepare = %v", err)
	}
	if err := world.Close(); err != nil {
		t.Fatal(err)
	}
	if err := world.Prepare(ctx); !errors.Is(err, localworld.ErrClosed) {
		t.Fatalf("Prepare after Close = %v", err)
	}
	if _, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: "wrun_a"}); !errors.Is(err, localworld.ErrClosed) {
		t.Fatalf("read after Close = %v", err)
	}
}

func TestPersistsAcrossReopen(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	world := startWorld(t, dir, nil)
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: "wrun_persist", WorkflowName: "persist", Input: []byte(`"in"`), Attributes: map[string]string{"k": "v"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := world.EventsCreateBatch(ctx, worlds.EventsCreateBatchInput{RunID: "wrun_persist", Events: []worlds.EventsCreateEvent{
		{EventType: worlds.EventTypeRunStarted},
		{EventType: worlds.EventTypeStepCreated, CorrelationID: "step_a", Data: []byte(`{"stepName":"a"}`)},
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: "wrun_persist", Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeHookCreated, CorrelationID: "hook_a", Data: []byte(`{"token":"Persist/Token"}`)}}); err != nil {
		t.Fatal(err)
	}
	if err := world.StreamsWriteMulti(ctx, worlds.StreamsWriteMultiInput{RunID: "wrun_persist", Name: "out/1", Chunks: [][]byte{[]byte("x"), []byte("y")}}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"held", ""} {
		if _, err := world.StreamsCreateWriteSession(ctx, worlds.StreamsCreateWriteSessionInput{RunID: "wrun_persist", Name: name}); err != nil {
			t.Fatal(err)
		}
	}
	if err := world.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := startWorld(t, dir, nil)
	run, err := reopened.RunsGet(ctx, worlds.RunsGetInput{RunID: "wrun_persist"})
	if err != nil || run.Status != worlds.StatusRunning || string(run.Input) != `"in"` || run.Attributes["k"] != "v" {
		t.Fatalf("run = %#v err=%v", run, err)
	}
	events, err := reopened.EventsList(ctx, worlds.EventsListInput{RunID: "wrun_persist"})
	if err != nil || len(events.Data) != 4 {
		t.Fatalf("events = %#v err=%v", events, err)
	}
	if step, err := reopened.StepsGet(ctx, worlds.StepsGetInput{RunID: "wrun_persist", StepID: "step_a"}); err != nil || step.StepName != "a" {
		t.Fatalf("step = %#v err=%v", step, err)
	}
	if hook, err := reopened.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: "Persist/Token"}); err != nil || hook.HookID != "hook_a" {
		t.Fatalf("hook = %#v err=%v", hook, err)
	}
	if _, err := reopened.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: "persist/token"}); !errors.Is(err, worlds.ErrHookNotFound) {
		t.Fatalf("tokens differing only by case collided: %v", err)
	}
	chunks, err := reopened.StreamsGetChunks(ctx, worlds.StreamsGetChunksInput{RunID: "wrun_persist", Name: "out/1"})
	if err != nil || len(chunks.Data) != 2 || string(chunks.Data[1].Data) != "y" {
		t.Fatalf("chunks = %#v err=%v", chunks, err)
	}
	names, err := reopened.StreamsList(ctx, worlds.StreamsListInput{RunID: "wrun_persist"})
	if err != nil || len(names) != 3 || names[0] != "" || names[1] != "held" || names[2] != "out/1" {
		t.Fatalf("stream names = %q err=%v", names, err)
	}
	// Sessions belong to the process that opened them and are released at
	// Prepare, including the default stream's.
	for _, name := range []string{"held", ""} {
		if _, err := reopened.StreamsCreateWriteSession(ctx, worlds.StreamsCreateWriteSessionInput{RunID: "wrun_persist", Name: name}); err != nil {
			t.Fatalf("session on %q after restart: %v", name, err)
		}
	}
	next, err := reopened.EventsCreate(ctx, worlds.EventsCreateInput{RunID: "wrun_persist", Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeWaitCreated, CorrelationID: "w"}})
	if err != nil || next.EventID != worlds.EventIDForSlot(5) {
		t.Fatalf("append after reopen = %#v err=%v", next, err)
	}
}

func TestViewsHoldNoPayloads(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	world := startWorld(t, dir, nil)
	runID := worlds.RunID("wrun_payloads")
	metadata := base64.StdEncoding.EncodeToString([]byte(`"hook-metadata-bytes"`))
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "payloads", Input: []byte(`"run-input-bytes"`)}); err != nil {
		t.Fatal(err)
	}
	for _, event := range []worlds.EventsCreateEvent{
		{EventType: worlds.EventTypeRunStarted},
		{EventType: worlds.EventTypeStepCreated, CorrelationID: "step_a", Data: []byte(`{"stepName":"a","input":"step-input-bytes"}`)},
		{EventType: worlds.EventTypeStepStarted, CorrelationID: "step_a"},
		{EventType: worlds.EventTypeStepRetrying, CorrelationID: "step_a", Data: []byte(`{"error":{"message":"step-error-bytes"}}`)},
		{EventType: worlds.EventTypeStepStarted, CorrelationID: "step_a"},
		{EventType: worlds.EventTypeStepCompleted, CorrelationID: "step_a", Data: []byte(`{"result":"step-output-bytes"}`)},
		{EventType: worlds.EventTypeHookCreated, CorrelationID: "hook_a", Data: []byte(`{"token":"payload-token","metadata":"` + metadata + `"}`)},
		{EventType: worlds.EventTypeRunCompleted, Data: []byte(`{"output":"run-output-bytes"}`)},
	} {
		if _, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: event}); err != nil {
			t.Fatalf("%s: %v", event.EventType, err)
		}
	}
	if err := world.Close(); err != nil {
		t.Fatal(err)
	}
	payloads := []string{"run-input-bytes", "run-output-bytes", "step-input-bytes", "step-error-bytes", "step-output-bytes", metadata}
	assertNoPayloads := func() {
		t.Helper()
		for _, name := range []string{"runs/" + key(string(runID)) + ".json", "steps/" + key(string(runID)) + "/" + key("step_a") + ".json", "hooks/" + key("hook_a") + ".json"} {
			raw, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(name)))
			if err != nil {
				t.Fatal(err)
			}
			for _, payload := range payloads {
				if strings.Contains(string(raw), payload) {
					t.Fatalf("%s holds payload %s: %s", name, payload, raw)
				}
			}
		}
	}
	assertNoPayloads()

	// An uncommitted event file makes the next Prepare rebuild the run's views
	// from its committed events.
	writeRecord(t, dir, "events/"+key(string(runID))+"/"+string(worlds.EventIDForSlot(10))+".json", worlds.Event{RunID: runID, EventID: worlds.EventIDForSlot(10), EventType: worlds.EventTypeWaitCreated, CorrelationID: "w", CreatedAt: time.Now().UTC()})
	reopened := startWorld(t, dir, nil)
	assertNoPayloads()
	run, err := reopened.RunsGet(ctx, worlds.RunsGetInput{RunID: runID})
	if err != nil || string(run.Input) != `"run-input-bytes"` || string(run.Output) != `"run-output-bytes"` || run.Error != nil {
		t.Fatalf("run = %#v err=%v", run, err)
	}
	step, err := reopened.StepsGet(ctx, worlds.StepsGetInput{RunID: runID, StepID: "step_a"})
	if err != nil || string(step.Input) != `"step-input-bytes"` || string(step.Output) != `"step-output-bytes"` || step.Error != nil || step.Attempt != 2 {
		t.Fatalf("step = %#v err=%v", step, err)
	}
	hook, err := reopened.HooksGet(ctx, worlds.HooksGetInput{HookID: "hook_a"})
	if err != nil || string(hook.Metadata) != `"hook-metadata-bytes"` {
		t.Fatalf("hook = %#v err=%v", hook, err)
	}
	stripped, err := reopened.HooksGet(ctx, worlds.HooksGetInput{HookID: "hook_a", ResolveData: worlds.ResolveDataNone})
	if err != nil || stripped.Metadata != nil {
		t.Fatalf("ResolveDataNone hook = %#v err=%v", stripped, err)
	}
}

func TestSecondPrepareIsInUse(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	first := startWorld(t, dir, nil)
	second, err := localworld.Open(localworld.Config{Dir: dir}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if err := second.Prepare(context.Background()); !errors.Is(err, localworld.ErrInUse) {
		t.Fatalf("second Prepare = %v, want ErrInUse", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := second.Prepare(context.Background()); err != nil {
		t.Fatalf("Prepare after release: %v", err)
	}
}

func TestRejectsIncompatibleStorage(t *testing.T) {
	t.Parallel()
	for name, files := range map[string]map[string]string{
		"old version":     {"version.json": `{"version":1}`},
		"corrupt version": {"version.json": `{"version":`},
		"unknown field":   {"version.json": `{"version":2,"completion":3}`},
		"legacy snapshot": {"world.json": `{}`},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			for file, content := range files {
				if err := os.WriteFile(filepath.Join(dir, file), []byte(content), 0600); err != nil {
					t.Fatal(err)
				}
			}
			world, err := localworld.Open(localworld.Config{Dir: dir}, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer world.Close()
			if err := world.Prepare(context.Background()); err == nil || !strings.Contains(err.Error(), "workflow/local") {
				t.Fatalf("Prepare = %v, want storage error", err)
			}
			// A rejected Prepare releases the lock.
			again, err := localworld.Open(localworld.Config{Dir: dir}, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer again.Close()
			if err := again.Prepare(context.Background()); errors.Is(err, localworld.ErrInUse) {
				t.Fatal("rejected Prepare kept the lock")
			}
		})
	}
}

func TestRecoveryDiscardsPartialBatch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	world := startWorld(t, dir, nil)
	runID := worlds.RunID("wrun_partial")
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "partial"}); err != nil {
		t.Fatal(err)
	}
	if _, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeStepCreated, CorrelationID: "step_kept", Data: []byte(`{"stepName":"kept"}`)}}); err != nil {
		t.Fatal(err)
	}
	if err := world.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate a crash during a batch: its event files and some views were
	// written, but the run record was not.
	now := time.Now().UTC()
	runDir := key(string(runID))
	writeRecord(t, dir, "events/"+runDir+"/"+string(worlds.EventIDForSlot(3))+".json", worlds.Event{RunID: runID, EventID: worlds.EventIDForSlot(3), EventType: worlds.EventTypeStepStarted, CorrelationID: "step_kept", CreatedAt: now})
	writeRecord(t, dir, "events/"+runDir+"/"+string(worlds.EventIDForSlot(4))+".json", worlds.Event{RunID: runID, EventID: worlds.EventIDForSlot(4), EventType: worlds.EventTypeStepCreated, CorrelationID: "step_lost", Data: []byte(`{"stepName":"lost"}`), CreatedAt: now})
	writeRecord(t, dir, "events/"+runDir+"/"+string(worlds.EventIDForSlot(5))+".json", worlds.Event{RunID: runID, EventID: worlds.EventIDForSlot(5), EventType: worlds.EventTypeHookCreated, CorrelationID: "hook_lost", Data: []byte(`{"token":"lost-token"}`), CreatedAt: now})
	writeRecord(t, dir, "steps/"+runDir+"/"+key("step_kept")+".json", worlds.Step{RunID: runID, StepID: "step_kept", StepName: "kept", Status: worlds.StatusRunning, Attempt: 1, CreatedAt: now, UpdatedAt: now})
	writeRecord(t, dir, "steps/"+runDir+"/"+key("step_lost")+".json", worlds.Step{RunID: runID, StepID: "step_lost", StepName: "lost", Status: worlds.StatusPending, CreatedAt: now, UpdatedAt: now})
	writeRecord(t, dir, "hooks/"+key("hook_lost")+".json", worlds.Hook{RunID: runID, HookID: "hook_lost", Token: "lost-token", CreatedAt: now, UpdatedAt: now})
	writeRecord(t, dir, "tokens/"+key("lost-token")+".json", map[string]any{"hookId": "hook_lost"})
	// And a run whose creation never committed.
	writeRecord(t, dir, "events/"+key("wrun_orphan")+"/"+string(worlds.EventIDForSlot(1))+".json", worlds.Event{RunID: "wrun_orphan", EventID: worlds.EventIDForSlot(1), EventType: worlds.EventTypeRunCreated, CreatedAt: now})

	reopened := startWorld(t, dir, nil)
	events, err := reopened.EventsList(ctx, worlds.EventsListInput{RunID: runID})
	if err != nil || len(events.Data) != 2 {
		t.Fatalf("events after recovery = %#v err=%v", events, err)
	}
	step, err := reopened.StepsGet(ctx, worlds.StepsGetInput{RunID: runID, StepID: "step_kept"})
	if err != nil || step.Status != worlds.StatusPending || step.Attempt != 0 {
		t.Fatalf("kept step = %#v err=%v", step, err)
	}
	if _, err := reopened.StepsGet(ctx, worlds.StepsGetInput{RunID: runID, StepID: "step_lost"}); !errors.Is(err, worlds.ErrStepNotFound) {
		t.Fatalf("uncommitted step survived: %v", err)
	}
	if _, err := reopened.HooksGet(ctx, worlds.HooksGetInput{HookID: "hook_lost"}); !errors.Is(err, worlds.ErrHookNotFound) {
		t.Fatalf("uncommitted hook survived: %v", err)
	}
	if _, err := reopened.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: "lost-token"}); !errors.Is(err, worlds.ErrHookNotFound) {
		t.Fatalf("uncommitted token claim survived: %v", err)
	}
	for _, slot := range []uint64{3, 4, 5} {
		if _, err := os.Stat(filepath.Join(dir, "events", runDir, string(worlds.EventIDForSlot(slot))+".json")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("event file %d survived: %v", slot, err)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "events", key("wrun_orphan"))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("orphan run events survived: %v", err)
	}
	started, err := reopened.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeStepStarted, CorrelationID: "step_kept"}})
	if err != nil || started.EventID != worlds.EventIDForSlot(3) || started.AllocatedEventCount != 1 {
		t.Fatalf("append after recovery = %#v err=%v", started, err)
	}
	if _, err := reopened.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: "wrun_orphan", WorkflowName: "partial"}); err != nil {
		t.Fatalf("create after discarded create: %v", err)
	}
}

func TestRecoveryKeepsTokenClaimedByAnotherRun(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	world := startWorld(t, dir, nil)
	for _, runID := range []worlds.RunID{"wrun_old", "wrun_new"} {
		if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "tokens"}); err != nil {
			t.Fatal(err)
		}
	}
	claim := func(runID worlds.RunID, hookID worlds.HookID, force bool) {
		t.Helper()
		if _, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeHookCreated, CorrelationID: string(hookID), Data: []byte(fmt.Sprintf(`{"token":"shared","force":%t}`, force))}}); err != nil {
			t.Fatal(err)
		}
	}
	claim("wrun_old", "hook_old", false)
	claim("wrun_new", "hook_new", true)
	if err := world.Close(); err != nil {
		t.Fatal(err)
	}
	// An uncommitted event in the old run forces it to be rebuilt.
	writeRecord(t, dir, "events/"+key("wrun_old")+"/"+string(worlds.EventIDForSlot(4))+".json", worlds.Event{RunID: "wrun_old", EventID: worlds.EventIDForSlot(4), EventType: worlds.EventTypeWaitCreated, CorrelationID: "w", CreatedAt: time.Now().UTC()})

	reopened := startWorld(t, dir, nil)
	hook, err := reopened.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: "shared"})
	if err != nil || hook.HookID != "hook_new" {
		t.Fatalf("token owner after recovery = %#v err=%v", hook, err)
	}
	events, err := reopened.EventsList(ctx, worlds.EventsListInput{RunID: "wrun_old"})
	if err != nil || len(events.Data) != 3 || events.Data[2].EventType != worlds.EventTypeHookDisposed {
		t.Fatalf("old run events = %#v err=%v", events, err)
	}
}

func TestRecoveryCompletesForcedHookHandoffAtEveryCommitBoundary(t *testing.T) {
	for phase := range 3 {
		t.Run([]string{"decision", "victim disposed", "claim committed"}[phase], func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			world := startWorld(t, dir, nil)
			for _, id := range []worlds.RunID{"wrun_victim", "wrun_claimant"} {
				if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: id, WorkflowName: "handoff"}); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: "wrun_victim", Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeHookCreated, CorrelationID: "hook_old", Data: []byte(`{"token":"handoff"}`)}}); err != nil {
				t.Fatal(err)
			}
			previous := worlds.HookForceClaim{RunID: "wrun_victim", HookID: "hook_old"}
			data, _ := json.Marshal(worlds.DataHookCreated{Token: "handoff", Force: true, ForceClaimedFrom: &previous})
			event := worlds.EventsCreateEvent{EventType: worlds.EventTypeHookCreated, CorrelationID: "hook_new", Data: data}
			if phase >= 1 {
				disposed, _ := json.Marshal(worlds.HookDisposedEventData{ForceClaimedBy: &worlds.HookForceClaim{RunID: "wrun_claimant", HookID: "hook_new"}})
				if _, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: "wrun_victim", Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeHookDisposed, CorrelationID: "hook_old", Data: disposed}}); err != nil {
					t.Fatal(err)
				}
			}
			zero := 0
			if phase == 2 {
				if _, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: "wrun_claimant", Event: event, Options: worlds.EventsCreateOptions{InBandCount: &zero}}); err != nil {
					t.Fatal(err)
				}
			}
			if err := world.Close(); err != nil {
				t.Fatal(err)
			}
			writeRecord(t, dir, "hook-handoff.json", map[string]any{"RunID": "wrun_claimant", "Event": event, "InBandCount": 0, "Previous": previous, "At": time.Now().UTC()})
			for range 2 {
				reopened := startWorld(t, dir, nil)
				owner, err := reopened.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: "handoff"})
				if err != nil || owner.HookID != "hook_new" || owner.ClaimedFrom == nil || *owner.ClaimedFrom != previous {
					t.Fatalf("owner=%#v err=%v", owner, err)
				}
				victim, err := reopened.HooksGet(ctx, worlds.HooksGetInput{HookID: "hook_old"})
				if err != nil || victim.DisposedAt == nil || victim.ForceClaimedBy == nil || victim.ForceClaimedBy.HookID != "hook_new" {
					t.Fatalf("victim=%#v err=%v", victim, err)
				}
				for id, count := range map[worlds.RunID]int{"wrun_victim": 3, "wrun_claimant": 2} {
					page, err := reopened.EventsList(ctx, worlds.EventsListInput{RunID: id})
					if err != nil || len(page.Data) != count {
						t.Fatalf("run=%s events=%d err=%v", id, len(page.Data), err)
					}
				}
				run, err := reopened.RunsGet(ctx, worlds.RunsGetInput{RunID: "wrun_claimant"})
				if err != nil || run.InBandCount != 1 {
					t.Fatalf("in-band count=%d err=%v", run.InBandCount, err)
				}
				if err := reopened.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

type doubleInput struct{ Value int }
type doubleOutput struct{ Value int }

func (doubleInput) Workflow(doubleOutput) {}

func TestRunsWorkflows(t *testing.T) {
	t.Parallel()
	world := startWorld(t, t.TempDir(), worlds.Runners(workflow.Use(double)))
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

func TestQueuedWorkSurvivesRestart(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	runners := worlds.Runners(workflow.Use(double))
	world := startWorld(t, dir, runners)
	input, err := json.Marshal(doubleInput{Value: 4})
	if err != nil {
		t.Fatal(err)
	}
	created, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: "wrun_restart", WorkflowName: workflow.Use(double).Name(), Input: input})
	if err != nil {
		t.Fatal(err)
	}
	_ = created
	notBefore := time.Now().Add(300 * time.Millisecond)
	if _, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_restart", NotBefore: &notBefore}); err != nil {
		t.Fatal(err)
	}
	if err := world.Close(); err != nil {
		t.Fatal(err)
	}
	queued, err := os.ReadDir(filepath.Join(dir, "queue"))
	if err != nil || len(queued) != 1 {
		t.Fatalf("queue files after close = %v err=%v", queued, err)
	}

	reopened := startWorld(t, dir, runners)
	run, err := reopened.RunsWaitForTerminalStatus(ctx, worlds.RunsWaitForTerminalStatusInput{RunID: "wrun_restart", Timeout: 10 * time.Second})
	if err != nil || run.Status != worlds.StatusCompleted || string(run.Output) != `{"Value":8}` {
		t.Fatalf("run after restart = %#v err=%v", run, err)
	}
	if !time.Now().After(notBefore) {
		t.Fatal("delivered before NotBefore")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		entries, err := os.ReadDir(filepath.Join(dir, "queue"))
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("queue records left after delivery: %d", len(entries))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestQueueIdempotencyKey(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	world := startWorld(t, t.TempDir(), nil)
	later := time.Now().Add(time.Hour)
	first, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_a", NotBefore: &later, IdempotencyKey: "k"})
	if err != nil {
		t.Fatal(err)
	}
	again, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_a", NotBefore: &later, IdempotencyKey: "k"})
	if err != nil || again.MessageID != first.MessageID {
		t.Fatalf("duplicate key = %#v err=%v, want %s", again, err, first.MessageID)
	}
	other, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_b", NotBefore: &later, IdempotencyKey: "k"})
	if err != nil || other.MessageID == first.MessageID {
		t.Fatalf("same key on another run = %#v err=%v", other, err)
	}
	if err := world.QueueCancel(ctx, worlds.QueueCancelInput{MessageID: first.MessageID}); err != nil {
		t.Fatal(err)
	}
	fresh, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_a", NotBefore: &later, IdempotencyKey: "k"})
	if err != nil || fresh.MessageID == first.MessageID {
		t.Fatalf("key after cancel = %#v err=%v", fresh, err)
	}
	if err := world.QueueCancel(ctx, worlds.QueueCancelInput{MessageID: "../version"}); !errors.Is(err, worlds.ErrQueueMessageNotFound) {
		t.Fatalf("cancel path-like ID = %v", err)
	}
}

func TestClear(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	world := startWorld(t, dir, nil)
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: "wrun_clear", WorkflowName: "clear"}); err != nil {
		t.Fatal(err)
	}
	later := time.Now().Add(time.Hour)
	if _, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_clear", NotBefore: &later}); err != nil {
		t.Fatal(err)
	}
	if err := world.Clear(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: "wrun_clear"}); !errors.Is(err, localworld.ErrClosed) {
		t.Fatalf("read after Clear = %v", err)
	}
	if err := world.Clear(ctx); !errors.Is(err, localworld.ErrNotPrepared) {
		t.Fatalf("second Clear = %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Name() != "version.json" && entry.Name() != "locks" {
			t.Fatalf("Clear left %s", entry.Name())
		}
	}
	reopened := startWorld(t, dir, nil)
	if _, err := reopened.RunsGet(ctx, worlds.RunsGetInput{RunID: "wrun_clear"}); !errors.Is(err, worlds.ErrRunNotFound) {
		t.Fatalf("run after Clear = %v", err)
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

func TestNewBorrowsRoot(t *testing.T) {
	t.Parallel()
	root, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	world, err := localworld.New(root, localworld.Config{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := world.Prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := world.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := root.Stat("version.json"); err != nil {
		t.Fatalf("borrowed root closed or unused: %v", err)
	}
}

func startWorld(t *testing.T, dir string, runners worlds.RunnerSet) *localworld.World {
	t.Helper()
	world, err := localworld.Open(localworld.Config{Dir: dir}, runners)
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

func key(value string) string { return hex.EncodeToString([]byte(value)) }

func writeRecord(t *testing.T, dir string, name string, value any) {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
}

func double(ctx context.Context, input doubleInput) (doubleOutput, error) {
	value, err := workflow.Step(ctx, func(_ context.Context, value int) (int, error) { return value * 2, nil }, input.Value)
	return doubleOutput{Value: value}, err
}
