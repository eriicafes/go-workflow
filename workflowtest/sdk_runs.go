package workflowtest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
	"github.com/oklog/ulid/v2"
)

// SDK: packages/world-local/src/storage.test.ts > Storage > custom runId validation > should accept a client-provided runId with current timestamp
// SDK: packages/world-local/src/storage.test.ts > Storage > custom runId validation > should accept a runId within the threshold
// SDK: packages/world-local/src/storage.test.ts > Storage > custom runId validation > should accept a runId with a timestamp 10 minutes in the past
// SDK: packages/world-local/src/storage.test.ts > Storage > custom runId validation > should reject a runId with a timestamp too far in the past
// SDK: packages/world-local/src/storage.test.ts > Storage > custom runId validation > should reject a runId with a timestamp too far in the future
// SDK: packages/world-local/src/storage.test.ts > Storage > custom runId validation > should reject a runId that is not a valid ULID
// SDK: packages/world-local/src/storage.test.ts > Storage > custom runId validation > should not validate runId for non-run_created events
func conformSDKRunIDs(t *testing.T, world worlds.World) {
	ctx := context.Background()
	// minted returns a run ID of a ULID minted at, its random part zeros.
	minted := func(at time.Time) worlds.RunID {
		return worlds.RunID("wrun_" + ulid.MustNew(ulid.Timestamp(at), bytes.NewReader(make([]byte, 16))).String())
	}
	create := func(runID worlds.RunID) error {
		_, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "test-workflow", DeploymentID: "deployment-123", Input: []byte(`[]`)})
		return err
	}
	for _, accepted := range []struct {
		label string
		at    time.Time
	}{{"now", time.Now()}, {"4 minutes ago", time.Now().Add(-4 * time.Minute)}, {"10 minutes ago", time.Now().Add(-10 * time.Minute)}} {
		t.Run("a run ID minted "+accepted.label, func(t *testing.T) {
			runID := minted(accepted.at)
			if err := create(runID); err != nil {
				t.Fatal(err)
			}
			if run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID}); err != nil || run.RunID != runID {
				t.Fatalf("run = %+v err = %v", run, err)
			}
		})
	}
	for _, refused := range []struct {
		label string
		runID worlds.RunID
	}{
		{"minted 25 hours ago", minted(time.Now().Add(-25 * time.Hour))},
		{"minted 10 minutes ahead", minted(time.Now().Add(10 * time.Minute))},
		{"that is not a ULID", "wrun_not-a-valid-ulid!!!!!!!!"},
	} {
		t.Run("a run ID "+refused.label, func(t *testing.T) {
			for range 2 {
				conformExpect(t, create(refused.runID), worlds.ErrEventInvalid, "a run ID "+refused.label)
			}
		})
	}
	t.Run("other events", func(t *testing.T) {
		runID := sdkRun(t, world, "test-workflow", []byte(`[]`))
		conformAppend(t, world, runID, worlds.EventTypeRunStarted, "", nil)
		if run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID}); err != nil || run.Status != worlds.StatusRunning {
			t.Fatalf("run = %+v err = %v", run, err)
		}
	})
}

// SDK: packages/world-local/src/storage.test.ts > Storage > runs > create > should create a new workflow run
// SDK: packages/world-local/src/storage.test.ts > Storage > runs > create > should handle minimal run data
// SDK: packages/world-local/src/storage.test.ts > Storage > runs > create > should seed initial attributes from run_created
// SDK: packages/world-local/src/storage.test.ts > Storage > runs > get > should retrieve an existing run
// SDK: packages/world-local/src/storage.test.ts > Storage > runs > get > should throw error for non-existent run
// SDK: packages/world-local/src/storage.test.ts > Storage > runs > encryptionPublicKey > persists the public key from run_created
// SDK: packages/world-local/src/storage.test.ts > Storage > runs > encryptionPublicKey > carries the public key through resilient-start run creation
// SDK: packages/world-local/src/storage.test.ts > Storage > runs > encryptionPublicKey > leaves the field unset when the SDK does not supply it
func conformSDKRunCreate(t *testing.T, world worlds.World) {
	ctx := context.Background()
	get := func(t *testing.T, runID worlds.RunID) worlds.Run {
		t.Helper()
		run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID})
		if err != nil {
			t.Fatal(err)
		}
		return run
	}
	t.Run("a run", func(t *testing.T) {
		runID := worlds.NewRunID()
		if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "test-workflow", DeploymentID: "deployment-123", ExecutionContext: map[string]any{"userId": "user-1"}, Input: []byte(`[1,2]`)}); err != nil {
			t.Fatal(err)
		}
		run := get(t, runID)
		if !strings.HasPrefix(string(run.RunID), "wrun_") || run.DeploymentID != "deployment-123" || run.Status != worlds.StatusPending || run.WorkflowName != "test-workflow" || run.ExecutionContext["userId"] != "user-1" || string(run.Input) != `[1,2]` {
			t.Fatalf("run = %+v", run)
		}
		if run.Output != nil || run.Error != nil || run.StartedAt != nil || run.CompletedAt != nil || run.CreatedAt.IsZero() || run.UpdatedAt.IsZero() {
			t.Fatalf("new run = %+v", run)
		}
	})
	t.Run("minimal run data", func(t *testing.T) {
		runID := worlds.NewRunID()
		if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "minimal-workflow", DeploymentID: "deployment-123", Input: []byte(`[]`)}); err != nil {
			t.Fatal(err)
		}
		if run := get(t, runID); len(run.ExecutionContext) != 0 || string(run.Input) != `[]` {
			t.Fatalf("minimal run = %+v", run)
		}
	})
	t.Run("initial attributes", func(t *testing.T) {
		runID := worlds.NewRunID()
		if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "attributed-workflow", Attributes: map[string]string{"tenant": "t1", "phase": "created"}}); err != nil {
			t.Fatal(err)
		}
		if run := get(t, runID); len(run.Attributes) != 2 || run.Attributes["tenant"] != "t1" || run.Attributes["phase"] != "created" {
			t.Fatalf("attributes = %v", run.Attributes)
		}
	})
	t.Run("get", func(t *testing.T) {
		runID := sdkRun(t, world, "test-workflow", []byte(`[]`))
		first := get(t, runID)
		listed, err := world.RunsList(ctx, worlds.RunsListInput{WorkflowName: "test-workflow"})
		if err != nil {
			t.Fatal(err)
		}
		for _, run := range listed.Data {
			if run.RunID == runID && (run.Status != first.Status || !run.CreatedAt.Equal(first.CreatedAt) || string(run.Input) != string(first.Input)) {
				t.Fatalf("listed run %+v, got %+v", run, first)
			}
		}
		if again := get(t, runID); again.RunID != first.RunID || !again.UpdatedAt.Equal(first.UpdatedAt) || string(again.Input) != string(first.Input) {
			t.Fatalf("run read twice: %+v and %+v", first, again)
		}
	})
	t.Run("missing run", func(t *testing.T) {
		_, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: "wrun_nonexistent"})
		conformExpect(t, err, worlds.ErrRunNotFound, "missing run")
	})
	key := strings.Repeat("A", 43) + "="
	t.Run("encryption public key", func(t *testing.T) {
		runID := worlds.NewRunID()
		if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "test-workflow", DeploymentID: "deployment-123", Input: []byte(`[]`), EncryptionPublicKey: key}); err != nil {
			t.Fatal(err)
		}
		if run := get(t, runID); run.EncryptionPublicKey != key {
			t.Fatalf("run key = %q", run.EncryptionPublicKey)
		}
	})
	t.Run("encryption public key through a resilient start", func(t *testing.T) {
		runID := worlds.NewRunID()
		data, err := worlds.RunCreatedData(worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "resilient-test", DeploymentID: "deployment-123", Input: []byte(`[]`), EncryptionPublicKey: key})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeRunStarted, Data: data}}); err != nil {
			t.Fatal(err)
		}
		if run := get(t, runID); run.EncryptionPublicKey != key {
			t.Fatalf("run key = %q", run.EncryptionPublicKey)
		}
		// The run_created the start writes carries the key as the
		// World's run_created events do, so a replay of the log reads it.
		keyOf := func(runID worlds.RunID) string {
			var created worlds.DataRunCreated
			events := conformAllEvents(t, world, runID)
			if events[0].EventType != worlds.EventTypeRunCreated || json.Unmarshal(events[0].Data, &created) != nil {
				t.Fatalf("first event = %+v", events[0])
			}
			return created.EncryptionPublicKey
		}
		ordinary := worlds.NewRunID()
		if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: ordinary, WorkflowName: "test-workflow", Input: []byte(`[]`), EncryptionPublicKey: key}); err != nil {
			t.Fatal(err)
		}
		if want, got := keyOf(ordinary), keyOf(runID); got != want {
			t.Fatalf("run_created key = %q, want %q as a created run's", got, want)
		}
	})
	t.Run("no encryption public key", func(t *testing.T) {
		if run := get(t, sdkRun(t, world, "test-workflow", []byte(`[]`))); run.EncryptionPublicKey != "" {
			t.Fatalf("run key = %q", run.EncryptionPublicKey)
		}
	})
}

// SDK: packages/world-local/src/storage.test.ts > Storage > runs > update via events > should update run status to running via run_started event
// SDK: packages/world-local/src/storage.test.ts > Storage > runs > update via events > should reject run_started on a non-existent run
// SDK: packages/world-local/src/storage.test.ts > Storage > runs > update via events > should update run status to completed via run_completed event
// SDK: packages/world-local/src/storage.test.ts > Storage > runs > update via events > should update run status to failed via run_failed event
// SDK: packages/world-local/src/storage.test.ts > Storage > runs > update via events > should reject run_failed on non-existent run
// SDK: packages/world-local/src/storage.test.ts > Storage > runs > update via events > should materialize attr_set events and preserve event history
// SDK: packages/world-local/src/storage.test.ts > Storage > runs > update via events > should allow reserved native attributes only with the opt-in flag
// SDK: packages/world-local/src/storage.test.ts > Storage > runs > update via events > should enforce the per-run cap against existing attributes
// SDK: packages/world-local/src/storage.test.ts > Storage > runs > update via events > should reject oversized attribute values on attr_set
// SDK: packages/world-local/src/storage.test.ts > Storage > runs > update via events > should reject invalid initial attributes on run_created
func conformSDKRunUpdates(t *testing.T, world worlds.World) {
	ctx := context.Background()
	get := func(t *testing.T, runID worlds.RunID) worlds.Run {
		t.Helper()
		run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID})
		if err != nil {
			t.Fatal(err)
		}
		return run
	}
	attrSet := func(runID worlds.RunID, correlationID string, writer worlds.AttributeWriter, reserved bool, changes ...worlds.RunsAttributeChange) (worlds.EventResult, error) {
		return sdkTry(world, runID, worlds.EventTypeAttrSet, correlationID, worlds.DataAttrSet{Changes: changes, Writer: writer, AllowReservedAttributes: reserved})
	}
	workflow := worlds.AttributeWriter{Type: worlds.AttributeWriterWorkflow}
	value := func(s string) *string { return &s }
	t.Run("run_started", func(t *testing.T) {
		runID := sdkRun(t, world, "test-workflow", []byte(`[]`))
		created := get(t, runID)
		time.Sleep(time.Millisecond)
		conformAppend(t, world, runID, worlds.EventTypeRunStarted, "", nil)
		if run := get(t, runID); run.Status != worlds.StatusRunning || run.StartedAt == nil || run.UpdatedAt.Before(created.UpdatedAt) {
			t.Fatalf("started run = %+v", run)
		}
	})
	t.Run("run_started of a missing run", func(t *testing.T) {
		_, err := sdkTry(world, "wrun_nonexistent", worlds.EventTypeRunStarted, "", nil)
		conformExpect(t, err, worlds.ErrRunNotFound, "run_started of a missing run")
	})
	t.Run("run_completed", func(t *testing.T) {
		runID := sdkRun(t, world, "test-workflow", []byte(`[]`))
		conformAppend(t, world, runID, worlds.EventTypeRunCompleted, "", worlds.DataRunCompleted{Output: []byte(`[1]`)})
		if run := get(t, runID); run.Status != worlds.StatusCompleted || string(run.Output) != `[1]` || run.CompletedAt == nil {
			t.Fatalf("completed run = %+v", run)
		}
	})
	t.Run("run_failed", func(t *testing.T) {
		runID := sdkRun(t, world, "test-workflow", []byte(`[]`))
		serialized := []byte(`{"message":"opaque"}`)
		conformAppend(t, world, runID, worlds.EventTypeRunFailed, "", worlds.DataRunFailed{Error: serialized})
		if run := get(t, runID); run.Status != worlds.StatusFailed || string(run.Error) != string(serialized) || run.CompletedAt == nil {
			t.Fatalf("failed run = %+v", run)
		}
	})
	t.Run("run_failed of a missing run", func(t *testing.T) {
		_, err := sdkTry(world, "wrun_nonexistent", worlds.EventTypeRunFailed, "", worlds.DataRunFailed{Error: []byte(`"Something went wrong"`)})
		conformExpect(t, err, worlds.ErrRunNotFound, "run_failed of a missing run")
	})
	if !world.Capabilities().SetAttributes {
		return
	}
	t.Run("attr_set", func(t *testing.T) {
		runID := worlds.NewRunID()
		if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "test-workflow", Attributes: map[string]string{"stale": "remove"}}); err != nil {
			t.Fatal(err)
		}
		result, err := attrSet(runID, "attr_1", workflow, false, worlds.RunsAttributeChange{Key: "phase", Value: value("ready")}, worlds.RunsAttributeChange{Key: "stale"})
		if err != nil || result.EventType != worlds.EventTypeAttrSet {
			t.Fatalf("attr_set = %+v err = %v", result, err)
		}
		if run := get(t, runID); len(run.Attributes) != 1 || run.Attributes["phase"] != "ready" {
			t.Fatalf("attributes = %v", run.Attributes)
		}
	})
	t.Run("reserved attributes", func(t *testing.T) {
		runID := sdkRun(t, world, "test-workflow", []byte(`[]`))
		_, err := attrSet(runID, "", workflow, false, worlds.RunsAttributeChange{Key: "$system", Value: value("nope")})
		conformExpect(t, err, worlds.ErrAttributeInvalid, "reserved attribute without the opt-in")
		if _, err := attrSet(runID, "", workflow, true, worlds.RunsAttributeChange{Key: "$system", Value: value("ok")}); err != nil {
			t.Fatal(err)
		}
		if run := get(t, runID); len(run.Attributes) != 1 || run.Attributes["$system"] != "ok" {
			t.Fatalf("attributes = %v", run.Attributes)
		}
	})
	t.Run("per-run cap", func(t *testing.T) {
		initial := map[string]string{}
		for i := range 63 {
			initial[fmt.Sprintf("a%d", i)] = "v"
		}
		runID := worlds.NewRunID()
		if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "test-workflow", Attributes: initial}); err != nil {
			t.Fatal(err)
		}
		if _, err := attrSet(runID, "", workflow, false, worlds.RunsAttributeChange{Key: "a63", Value: value("v")}); err != nil {
			t.Fatal(err)
		}
		if run := get(t, runID); len(run.Attributes) != 64 {
			t.Fatalf("attributes at the cap = %d", len(run.Attributes))
		}
		_, err := attrSet(runID, "", workflow, false, worlds.RunsAttributeChange{Key: "a64", Value: value("v")})
		conformExpect(t, err, worlds.ErrAttributeInvalid, "a 65th attribute")
		if _, err := attrSet(runID, "", worlds.AttributeWriter{Type: worlds.AttributeWriterStep, StepID: "step_1", Attempt: 1}, false, worlds.RunsAttributeChange{Key: "a0", Value: value("updated")}); err != nil {
			t.Fatal(err)
		}
		if run := get(t, runID); run.Attributes["a0"] != "updated" {
			t.Fatalf("upserted attribute = %q", run.Attributes["a0"])
		}
		if _, err := attrSet(runID, "", workflow, false, worlds.RunsAttributeChange{Key: "a1"}, worlds.RunsAttributeChange{Key: "replacement", Value: value("v")}); err != nil {
			t.Fatal(err)
		}
		run := get(t, runID)
		if _, ok := run.Attributes["a1"]; ok || run.Attributes["replacement"] != "v" || len(run.Attributes) != 64 {
			t.Fatalf("swapped attributes = %v", run.Attributes)
		}
	})
	t.Run("oversized value", func(t *testing.T) {
		runID := sdkRun(t, world, "test-workflow", []byte(`[]`))
		_, err := attrSet(runID, "", workflow, false, worlds.RunsAttributeChange{Key: "note", Value: value(strings.Repeat("v", 257))})
		conformExpect(t, err, worlds.ErrAttributeInvalid, "a 257-byte value")
	})
	t.Run("invalid initial attributes", func(t *testing.T) {
		over := map[string]string{}
		for i := range 65 {
			over[fmt.Sprintf("a%d", i)] = "v"
		}
		_, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: worlds.NewRunID(), WorkflowName: "test-workflow", Attributes: over})
		conformExpect(t, err, worlds.ErrAttributeInvalid, "65 initial attributes")
		_, err = world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: worlds.NewRunID(), WorkflowName: "test-workflow", Attributes: map[string]string{"$reserved": "nope"}})
		conformExpect(t, err, worlds.ErrAttributeInvalid, "a reserved initial attribute")
	})
}

// SDK: packages/world-local/src/storage.test.ts > Storage > run terminal state validation > completed run > should reject run_started on completed run
// SDK: packages/world-local/src/storage.test.ts > Storage > run terminal state validation > completed run > should reject run_failed on completed run
// SDK: packages/world-local/src/storage.test.ts > Storage > run terminal state validation > completed run > should reject run_cancelled on completed run
// SDK: packages/world-local/src/storage.test.ts > Storage > run terminal state validation > failed run > should reject run_started on failed run
// SDK: packages/world-local/src/storage.test.ts > Storage > run terminal state validation > failed run > should reject run_completed on failed run
// SDK: packages/world-local/src/storage.test.ts > Storage > run terminal state validation > failed run > should reject run_cancelled on failed run
// SDK: packages/world-local/src/storage.test.ts > Storage > run terminal state validation > cancelled run > should reject run_started on cancelled run
// SDK: packages/world-local/src/storage.test.ts > Storage > run terminal state validation > cancelled run > should reject run_completed on cancelled run
// SDK: packages/world-local/src/storage.test.ts > Storage > run terminal state validation > cancelled run > should reject run_failed on cancelled run
// SDK: packages/world-local/src/storage.test.ts > Storage > idempotent operations > should allow run_cancelled on already cancelled run (idempotent)
func conformSDKRunTerminal(t *testing.T, world worlds.World) {
	ctx := context.Background()
	finish := map[worlds.EventType]any{
		worlds.EventTypeRunCompleted: worlds.DataRunCompleted{Output: []byte(`[3]`)},
		worlds.EventTypeRunFailed:    worlds.DataRunFailed{Error: []byte(`"Failed"`)},
		worlds.EventTypeRunCancelled: nil,
	}
	for _, ended := range []worlds.EventType{worlds.EventTypeRunCompleted, worlds.EventTypeRunFailed, worlds.EventTypeRunCancelled} {
		for _, next := range []worlds.EventType{worlds.EventTypeRunStarted, worlds.EventTypeRunCompleted, worlds.EventTypeRunFailed, worlds.EventTypeRunCancelled} {
			if next == ended {
				continue
			}
			t.Run(fmt.Sprintf("%s on a %s run", next, ended), func(t *testing.T) {
				runID := sdkRun(t, world, "test-workflow", []byte(`[]`))
				conformAppend(t, world, runID, ended, "", finish[ended])
				_, err := sdkTry(world, runID, next, "", finish[next])
				conformExpectType[*worlds.RunExpiredError](t, err, string(next)+" on a finished run")
			})
		}
	}
	t.Run("run_cancelled twice", func(t *testing.T) {
		runID := sdkRun(t, world, "test-workflow", []byte(`[]`))
		conformAppend(t, world, runID, worlds.EventTypeRunCancelled, "", nil)
		conformAppend(t, world, runID, worlds.EventTypeRunCancelled, "", nil)
		if run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID}); err != nil || run.Status != worlds.StatusCancelled {
			t.Fatalf("run cancelled twice = %+v err = %v", run, err)
		}
	})
}

// SDK: packages/world-local/src/storage.test.ts > Storage > runs > list > should list all runs
// SDK: packages/world-local/src/storage.test.ts > Storage > runs > list > should filter runs by workflowName
// SDK: packages/world-local/src/storage.test.ts > Storage > runs > list > should support pagination
// SDK: packages/world-local/src/storage.test.ts > Storage > runs > list > filters by a single status
// SDK: packages/world-local/src/storage.test.ts > Storage > runs > list > filters by an array of statuses (matches any)
// SDK: packages/world-local/src/storage.test.ts > Storage > runs > list > returns no runs when status is an empty array (matches SQL `IN ()`)
// SDK: packages/world-local/src/storage.test.ts > Storage > runs > list > leaves the filter unset when status field is omitted
//
// Each listing is scoped to workflow names of its own, so runs other cases
// create do not count.
func conformSDKRunsList(t *testing.T, world worlds.World) {
	ctx := context.Background()
	unique := func(name string) string { return fmt.Sprintf("%s-%d", name, time.Now().UnixNano()) }
	list := func(t *testing.T, input worlds.RunsListInput) worlds.Page[worlds.Run] {
		t.Helper()
		page, err := world.RunsList(ctx, input)
		if err != nil {
			t.Fatal(err)
		}
		return page
	}
	t.Run("newest first", func(t *testing.T) {
		name := unique("workflow")
		first := sdkRun(t, world, name, []byte(`[]`))
		time.Sleep(2 * time.Millisecond)
		second := sdkRun(t, world, name, []byte(`[]`))
		page := list(t, worlds.RunsListInput{WorkflowName: name})
		if len(page.Data) != 2 || page.Data[0].RunID != second || page.Data[1].RunID != first || !page.Data[0].CreatedAt.After(page.Data[1].CreatedAt) {
			t.Fatalf("runs = %+v", page.Data)
		}
	})
	t.Run("by workflow name", func(t *testing.T) {
		name := unique("workflow")
		sdkRun(t, world, name+"-1", []byte(`[]`))
		second := sdkRun(t, world, name+"-2", []byte(`[]`))
		if page := list(t, worlds.RunsListInput{WorkflowName: name + "-2"}); len(page.Data) != 1 || page.Data[0].RunID != second {
			t.Fatalf("runs = %+v", page.Data)
		}
	})
	t.Run("pagination", func(t *testing.T) {
		name := unique("workflow")
		for range 5 {
			sdkRun(t, world, name, []byte(`[]`))
		}
		first := list(t, worlds.RunsListInput{WorkflowName: name, Pagination: worlds.Pagination{Limit: 2}})
		if len(first.Data) != 2 || first.Cursor == "" {
			t.Fatalf("first page = %+v", first)
		}
		second := list(t, worlds.RunsListInput{WorkflowName: name, Pagination: worlds.Pagination{Limit: 2, Cursor: first.Cursor}})
		if len(second.Data) != 2 || second.Data[0].RunID == first.Data[0].RunID {
			t.Fatalf("second page = %+v", second)
		}
	})
	t.Run("status filters", func(t *testing.T) {
		name := unique("w1")
		sdkRun(t, world, name, []byte(`[]`))
		if page := list(t, worlds.RunsListInput{WorkflowName: name, Status: []worlds.Status{worlds.StatusPending}}); len(page.Data) != 1 || page.Data[0].Status != worlds.StatusPending {
			t.Fatalf("pending runs = %+v", page.Data)
		}
		// Vercel's backend filters runs by one status, and refuses several as
		// invalid.
		if page, err := world.RunsList(ctx, worlds.RunsListInput{WorkflowName: name, Status: []worlds.Status{worlds.StatusPending, worlds.StatusRunning}}); !errors.Is(err, worlds.ErrEventInvalid) && (err != nil || len(page.Data) != 1) {
			t.Fatalf("pending or running runs = %+v err = %v", page.Data, err)
		}
		if page := list(t, worlds.RunsListInput{WorkflowName: name, Status: []worlds.Status{}}); len(page.Data) != 0 {
			t.Fatalf("runs of no status = %+v", page.Data)
		}
		if page := list(t, worlds.RunsListInput{WorkflowName: name}); len(page.Data) != 1 {
			t.Fatalf("runs without a status filter = %+v", page.Data)
		}
	})
}

// SDK: packages/world-local/src/storage/run-status-wait.test.ts > runs.waitForTerminalStatus (world-local) > returns an already-terminal run without waiting
// SDK: packages/world-local/src/storage/run-status-wait.test.ts > runs.waitForTerminalStatus (world-local) > resolves as soon as the run completes
// SDK: packages/world-local/src/storage/run-status-wait.test.ts > runs.waitForTerminalStatus (world-local) > picks up a cancellation
// SDK: packages/world-local/src/storage/run-status-wait.test.ts > runs.waitForTerminalStatus (world-local) > returns the latest non-terminal snapshot when the budget expires
// SDK: packages/world-local/src/storage/run-status-wait.test.ts > runs.waitForTerminalStatus (world-local) > returns immediately without a budget
// SDK: packages/world-local/src/storage/run-status-wait.test.ts > runs.waitForTerminalStatus (world-local) > stops early when the caller aborts
// SDK: packages/world-local/src/storage/run-status-wait.test.ts > runs.waitForTerminalStatus (world-local) > fails like get for an unknown run
func conformSDKRunsWait(t *testing.T, world worlds.World) {
	ctx := context.Background()
	wait := func(ctx context.Context, runID worlds.RunID, timeout time.Duration) (worlds.Run, error) {
		return world.RunsWaitForTerminalStatus(ctx, worlds.RunsWaitForTerminalStatusInput{RunID: runID, Timeout: timeout})
	}
	t.Run("already terminal", func(t *testing.T) {
		runID := sdkStartedRun(t, world)
		conformAppend(t, world, runID, worlds.EventTypeRunCompleted, "", worlds.DataRunCompleted{Output: []byte(`[2]`)})
		started := time.Now()
		if run, err := wait(ctx, runID, 30*time.Second); err != nil || run.Status != worlds.StatusCompleted || time.Since(started) >= time.Second {
			t.Fatalf("wait = %+v err = %v after %v", run, err, time.Since(started))
		}
	})
	for _, end := range []worlds.EventType{worlds.EventTypeRunCompleted, worlds.EventTypeRunCancelled} {
		t.Run("ended by "+string(end), func(t *testing.T) {
			runID := sdkStartedRun(t, world)
			done := make(chan error, 1)
			go func() {
				time.Sleep(20 * time.Millisecond)
				var data any
				if end == worlds.EventTypeRunCompleted {
					data = worlds.DataRunCompleted{Output: []byte(`[2]`)}
				}
				_, err := sdkTry(world, runID, end, "", data)
				done <- err
			}()
			started := time.Now()
			run, err := wait(ctx, runID, 30*time.Second)
			if writeErr := <-done; writeErr != nil {
				t.Fatal(writeErr)
			}
			want := worlds.StatusCompleted
			if end == worlds.EventTypeRunCancelled {
				want = worlds.StatusCancelled
			}
			if err != nil || run.Status != want || time.Since(started) >= 5*time.Second {
				t.Fatalf("wait = %+v err = %v after %v", run, err, time.Since(started))
			}
		})
	}
	t.Run("budget expires", func(t *testing.T) {
		runID := sdkStartedRun(t, world)
		started := time.Now()
		if run, err := wait(ctx, runID, 150*time.Millisecond); err != nil || run.Status != worlds.StatusRunning || time.Since(started) < 140*time.Millisecond {
			t.Fatalf("wait = %+v err = %v after %v", run, err, time.Since(started))
		}
	})
	t.Run("no budget", func(t *testing.T) {
		runID := sdkStartedRun(t, world)
		started := time.Now()
		if run, err := wait(ctx, runID, 0); err != nil || run.Status != worlds.StatusRunning || time.Since(started) >= time.Second {
			t.Fatalf("wait = %+v err = %v after %v", run, err, time.Since(started))
		}
	})
	t.Run("caller aborts", func(t *testing.T) {
		// A Go World answers a wait the caller ends with the context's
		// error, as its contract says, where the SDK returns the run.
		runID := sdkStartedRun(t, world)
		aborted, cancel := context.WithCancel(ctx)
		time.AfterFunc(20*time.Millisecond, cancel)
		started := time.Now()
		if _, err := wait(aborted, runID, 30*time.Second); !errors.Is(err, context.Canceled) || time.Since(started) >= 5*time.Second {
			t.Fatalf("aborted wait err = %v after %v", err, time.Since(started))
		}
		if run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID}); err != nil || run.Status != worlds.StatusRunning {
			t.Fatalf("run = %+v err = %v", run, err)
		}
	})
	t.Run("unknown run", func(t *testing.T) {
		_, err := wait(ctx, "wrun_01JB0000000000000000000000", 30*time.Second)
		conformExpect(t, err, worlds.ErrRunNotFound, "wait for an unknown run")
	})
}

// SDK: packages/world-local/src/storage/runs-storage.test.ts > runs.experimentalSetAttributes (world-local) > upserts new keys
// SDK: packages/world-local/src/storage/runs-storage.test.ts > runs.experimentalSetAttributes (world-local) > updates existing keys (merge semantics)
// SDK: packages/world-local/src/storage/runs-storage.test.ts > runs.experimentalSetAttributes (world-local) > removes keys when value is null
// SDK: packages/world-local/src/storage/runs-storage.test.ts > runs.experimentalSetAttributes (world-local) > applies set and unset in a single call
// SDK: packages/world-local/src/storage/runs-storage.test.ts > runs.experimentalSetAttributes (world-local) > throws WorkflowRunNotFoundError for unknown run
// SDK: packages/world-local/src/storage/runs-storage.test.ts > runs.experimentalSetAttributes (world-local) > rejects keys starting with reserved prefix
// SDK: packages/world-local/src/storage/runs-storage.test.ts > runs.experimentalSetAttributes (world-local) > accepts reserved-prefix keys when allowReservedAttributes is set (framework escape hatch)
// SDK: packages/world-local/src/storage/runs-storage.test.ts > runs.experimentalSetAttributes (world-local) > rejects keys over the max length
// SDK: packages/world-local/src/storage/runs-storage.test.ts > runs.experimentalSetAttributes (world-local) > rejects values exceeding the byte cap
// SDK: packages/world-local/src/storage/runs-storage.test.ts > runs.experimentalSetAttributes (world-local) > updates at the cap boundary do not falsely trip the limit
// SDK: packages/world-local/src/storage/runs-storage.test.ts > runs.experimentalSetAttributes (world-local) > repeated identical calls are idempotent
// SDK: packages/world-local/src/storage/runs-storage.test.ts > runs.experimentalSetAttributes (world-local) > rejects when post-merge count exceeds limit
// SDK: packages/world-local/src/storage/runs-storage.test.ts > runs.experimentalSetAttributes (world-local) > serializes concurrent writes to the same run (no lost writes)
func conformSDKSetAttributes(t *testing.T, world worlds.World) {
	if !world.Capabilities().SetAttributes {
		t.Skip("the World does not store attributes")
	}
	ctx := context.Background()
	set := func(runID worlds.RunID, reserved bool, changes ...worlds.RunsAttributeChange) error {
		return world.RunsSetAttributes(ctx, worlds.RunsSetAttributesInput{RunID: runID, Changes: changes, AllowReservedAttributes: reserved})
	}
	change := func(key string, value ...string) worlds.RunsAttributeChange {
		if len(value) == 0 {
			return worlds.RunsAttributeChange{Key: key}
		}
		return worlds.RunsAttributeChange{Key: key, Value: &value[0]}
	}
	attributes := func(t *testing.T, runID worlds.RunID) map[string]string {
		t.Helper()
		run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID})
		if err != nil {
			t.Fatal(err)
		}
		return run.Attributes
	}
	expect := func(t *testing.T, runID worlds.RunID, want map[string]string) {
		t.Helper()
		got := attributes(t, runID)
		if len(got) != len(want) {
			t.Fatalf("attributes = %v, want %v", got, want)
		}
		for key, value := range want {
			if got[key] != value {
				t.Fatalf("attributes = %v, want %v", got, want)
			}
		}
	}
	newRun := func(t *testing.T) worlds.RunID { return sdkRun(t, world, "test-workflow", []byte(`[1]`)) }
	must := func(t *testing.T, err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Run("upsert", func(t *testing.T) {
		runID := newRun(t)
		must(t, set(runID, false, change("phase", "init"), change("tenant", "t1")))
		expect(t, runID, map[string]string{"phase": "init", "tenant": "t1"})
	})
	t.Run("merge", func(t *testing.T) {
		runID := newRun(t)
		must(t, set(runID, false, change("phase", "init"), change("tenant", "t1")))
		must(t, set(runID, false, change("phase", "done")))
		expect(t, runID, map[string]string{"phase": "done", "tenant": "t1"})
	})
	t.Run("remove", func(t *testing.T) {
		runID := newRun(t)
		must(t, set(runID, false, change("phase", "init"), change("orderId", "ord_123")))
		must(t, set(runID, false, change("orderId")))
		expect(t, runID, map[string]string{"phase": "init"})
	})
	t.Run("set and unset", func(t *testing.T) {
		runID := newRun(t)
		must(t, set(runID, false, change("stale", "yes")))
		must(t, set(runID, false, change("stale"), change("fresh", "yes")))
		expect(t, runID, map[string]string{"fresh": "yes"})
	})
	t.Run("unknown run", func(t *testing.T) {
		conformExpect(t, set("wrun_doesnotexist", false, change("phase", "init")), worlds.ErrRunNotFound, "attributes of an unknown run")
	})
	t.Run("reserved prefix", func(t *testing.T) {
		runID := newRun(t)
		conformExpect(t, set(runID, false, change(worlds.ReservedAttributePrefix+"sys", "x")), worlds.ErrAttributeInvalid, "a reserved key")
	})
	t.Run("reserved prefix allowed", func(t *testing.T) {
		runID := newRun(t)
		kind := worlds.ReservedAttributePrefix + "framework.kind"
		must(t, set(runID, true, change(kind, "agent"), change("phase", "init")))
		expect(t, runID, map[string]string{kind: "agent", "phase": "init"})
		conformExpect(t, set(runID, false, change(worlds.ReservedAttributePrefix+"other", "x")), worlds.ErrAttributeInvalid, "a reserved key without the opt-in")
	})
	t.Run("long key", func(t *testing.T) {
		runID := newRun(t)
		conformExpect(t, set(runID, false, change(strings.Repeat("k", worlds.MaxAttributeKeyLength+1), "x")), worlds.ErrAttributeInvalid, "a key over the limit")
	})
	t.Run("large value", func(t *testing.T) {
		runID := newRun(t)
		conformExpect(t, set(runID, false, change("big", strings.Repeat("a", 257))), worlds.ErrAttributeInvalid, "a value over the limit")
	})
	t.Run("update at the cap", func(t *testing.T) {
		runID := newRun(t)
		var initial []worlds.RunsAttributeChange
		for i := range 64 {
			initial = append(initial, change(fmt.Sprintf("k%d", i), "v"))
		}
		must(t, set(runID, false, initial...))
		must(t, set(runID, false, change("k0", "updated")))
		if got := attributes(t, runID); got["k0"] != "updated" || len(got) != 64 {
			t.Fatalf("attributes = %v", got)
		}
	})
	t.Run("idempotent", func(t *testing.T) {
		runID := newRun(t)
		for range 3 {
			must(t, set(runID, false, change("phase", "init"), change("tenant", "t1")))
			expect(t, runID, map[string]string{"phase": "init", "tenant": "t1"})
		}
	})
	t.Run("count over the limit", func(t *testing.T) {
		runID := newRun(t)
		var initial, overflow []worlds.RunsAttributeChange
		for i := range 60 {
			initial = append(initial, change(fmt.Sprintf("k%d", i), "v"))
		}
		for i := range 5 {
			overflow = append(overflow, change(fmt.Sprintf("extra%d", i), "v"))
		}
		must(t, set(runID, false, initial...))
		conformExpect(t, set(runID, false, overflow...), worlds.ErrAttributeInvalid, "attributes over the per-run limit")
	})
	t.Run("concurrent writes", func(t *testing.T) {
		runID := newRun(t)
		errs := make([]error, 20)
		var group sync.WaitGroup
		for i := range 20 {
			group.Go(func() { errs[i] = set(runID, false, change(fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))) })
		}
		group.Wait()
		must(t, errors.Join(errs...))
		if got := attributes(t, runID); len(got) != 20 {
			t.Fatalf("attributes = %v, want 20", got)
		}
	})
}

// Values the run-retention tests store, long enough to tell apart.
var (
	sdkRunInput    = []byte(`[1,2,3]`)
	sdkRunOutput   = []byte(`[4,5,6]`)
	sdkStepInput   = []byte(`[7,7,7]`)
	sdkStepOutput  = []byte(`[8,8,8]`)
	sdkHookMeta    = []byte(`[9,9,9]`)
	sdkStreamChunk = []byte("secret payload")
)

// sdkRetentionRun creates a run with attributes, starts it, completes a step
// of it, and writes and closes one of its streams, stream-a.
func sdkRetentionRun(t *testing.T, world worlds.World, attributes map[string]string, startHook *worlds.StartHook) worlds.RunID {
	t.Helper()
	ctx := context.Background()
	runID := worlds.NewRunID()
	input := worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "retention-workflow", DeploymentID: "dpl_test", Input: sdkRunInput, Attributes: attributes, AllowReservedAttributes: attributes != nil, StartHook: startHook}
	if _, err := world.EventsCreateRun(ctx, input); err != nil {
		t.Fatal(err)
	}
	conformAppend(t, world, runID, worlds.EventTypeRunStarted, "", nil)
	sdkStep(t, world, runID, "step_001", "step//./mod//doWork", sdkStepInput)
	conformAppend(t, world, runID, worlds.EventTypeStepCompleted, "step_001", worlds.DataStepResult{Result: sdkStepOutput})
	if err := world.StreamsWrite(ctx, worlds.StreamsWriteInput{RunID: runID, Name: sdkRunStream("stream-a", runID), Chunk: sdkStreamChunk}); err != nil {
		t.Fatal(err)
	}
	if err := world.StreamsClose(ctx, worlds.StreamsCloseInput{RunID: runID, Name: sdkRunStream("stream-a", runID)}); err != nil {
		t.Fatal(err)
	}
	return runID
}

func sdkComplete(t *testing.T, world worlds.World, runID worlds.RunID) {
	t.Helper()
	conformAppend(t, world, runID, worlds.EventTypeRunCompleted, "", worlds.DataRunCompleted{Output: sdkRunOutput})
}

// sdkRunStream returns a stream name made of name and the end of runID. A
// World's stream names are unique across its runs, as worlds.NewStreamName
// makes them.
func sdkRunStream(name string, runID worlds.RunID) string {
	return name + "-" + string(runID[len(runID)-8:])
}

// sdkExpectEmptyStreams checks that each named stream of the run holds
// nothing and is closed.
func sdkExpectEmptyStreams(t *testing.T, world worlds.World, runID worlds.RunID, names ...string) {
	t.Helper()
	for _, name := range names {
		chunks, err := world.StreamsGetChunks(context.Background(), worlds.StreamsGetChunksInput{RunID: runID, Name: name})
		if err != nil || len(chunks.Data) != 0 || !chunks.Done {
			t.Fatalf("stream %s = %+v err = %v, want empty and done", name, chunks, err)
		}
	}
}

// sdkRetainNone returns the attributes of a run whose data is deleted when it
// finishes.
func sdkRetainNone() map[string]string {
	return map[string]string{worlds.RetentionAttribute: worlds.RetainNone.String()}
}

// sdkLateStep creates and starts step_late, which is still running when the
// run ends.
func sdkLateStep(t *testing.T, world worlds.World, runID worlds.RunID, input []byte) {
	t.Helper()
	sdkStep(t, world, runID, "step_late", "step//./mod//slow", input)
	conformAppend(t, world, runID, worlds.EventTypeStepStarted, "step_late", nil)
}

// SDK: packages/world-local/src/storage/run-retention.test.ts > run retention (world-local) > $retention: '0' > drops the run payloads and stamps expiredAt
// SDK: packages/world-local/src/storage/run-retention.test.ts > run retention (world-local) > $retention: '0' > drops a start Hook's metadata, keeping its token and retention
// SDK: packages/world-local/src/storage/run-retention.test.ts > run retention (world-local) > $retention: '0' > keeps the run listable, with its metadata intact
// SDK: packages/world-local/src/storage/run-retention.test.ts > run retention (world-local) > $retention: '0' > drops the step payloads but keeps the step
// SDK: packages/world-local/src/storage/run-retention.test.ts > run retention (world-local) > $retention: '0' > drops every event payload but keeps the log
// SDK: packages/world-local/src/storage/run-retention.test.ts > run retention (world-local) > $retention: '0' > empties the run streams and reports them finished
// SDK: packages/world-local/src/storage/run-retention.test.ts > run retention (world-local) > $retention: '0' > empties streams that first wrote at the same time
// SDK: packages/world-local/src/storage/run-retention.test.ts > run retention (world-local) > $retention: '0' > scrubs the payload of a ${eventType} that lands after the purge
// SDK: packages/world-local/src/storage/run-retention.test.ts > run retention (world-local) > $retention: '0' > ends a stream first written after the purge, with nothing in it
// SDK: packages/world-local/src/storage/run-retention.test.ts > run retention (world-local) > $retention: '0' > scrubs the metadata of a hook that outlives the run
// SDK: packages/world-local/src/storage/run-retention.test.ts > run retention (world-local) > $retention: '0' > purges a failed run, error included
// SDK: packages/world-local/src/storage/run-retention.test.ts > run retention (world-local) > $retention: '0' > purges a cancelled run
// SDK: packages/world-local/src/storage/run-retention.test.ts > run retention (world-local) > $retention: '0' > reads the freshest attributes, not the pre-validation snapshot
// SDK: packages/world-local/src/storage/run-retention.test.ts > run retention (world-local) > $retention: '0' > leaves other runs alone
func conformSDKRetainNone(t *testing.T, world worlds.World) {
	ctx := context.Background()
	get := func(t *testing.T, runID worlds.RunID) worlds.Run {
		t.Helper()
		run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID})
		if err != nil {
			t.Fatal(err)
		}
		return run
	}
	noPayloads := func(t *testing.T, runID worlds.RunID) {
		t.Helper()
		if payloads := sdkPayloads(t, world, runID); len(payloads) != 0 {
			t.Fatalf("payloads left = %v", payloads)
		}
	}
	t.Run("run payloads and expiredAt", func(t *testing.T) {
		runID := sdkRetentionRun(t, world, sdkRetainNone(), nil)
		before := time.Now().Truncate(time.Millisecond)
		sdkComplete(t, world, runID)
		run := get(t, runID)
		if run.Input != nil || run.Output != nil || run.ExpiredAt == nil || run.ExpiredAt.Before(before) || run.ExpiredAt.After(time.Now()) {
			t.Fatalf("purged run = %+v", run)
		}
	})
	t.Run("start hook metadata", func(t *testing.T) {
		if !world.Capabilities().StartHook {
			t.Skip("the World creates no start hooks")
		}
		token := "order:purge:" + worlds.NewHookToken()
		until := time.Now().Add(time.Minute).UTC().Truncate(time.Millisecond)
		runID := sdkRetentionRun(t, world, sdkRetainNone(), &worlds.StartHook{Token: token, TokenRetentionUntil: &until, Metadata: sdkHookMeta})
		hookID := worlds.StartHookID(runID)
		sdkComplete(t, world, runID)
		start := get(t, runID).StartHook
		if start == nil || start.Token != token || start.HookID != hookID || start.Metadata != nil || start.TokenRetentionUntil == nil || !start.TokenRetentionUntil.Equal(until) {
			t.Fatalf("run start hook = %+v", start)
		}
		retained, err := world.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: token})
		if err != nil || retained.HookID != hookID || retained.RunID != runID || retained.Metadata != nil {
			t.Fatalf("retained start hook = %+v err = %v", retained, err)
		}
		noPayloads(t, runID)
		creations := 0
		for _, event := range conformAllEvents(t, world, runID) {
			if hook := worlds.RunStartHook(event.Data); hook != nil && (event.EventType == worlds.EventTypeRunCreated || event.EventType == worlds.EventTypeRunStarted) {
				creations++
				if hook.Metadata != nil || hook.Token != token {
					t.Fatalf("%s start hook = %+v", event.EventType, hook)
				}
			}
		}
		if creations == 0 {
			t.Fatal("no creation event carries the start hook")
		}
		_, err = world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: worlds.NewRunID(), WorkflowName: "retention-workflow", Input: sdkRunInput, StartHook: &worlds.StartHook{Token: token}})
		if conflict, ok := errors.AsType[*worlds.HookConflictError](err); !ok || conflict.RunID != runID {
			t.Fatalf("contender = %v, want a conflict naming %s", err, runID)
		}
	})
	t.Run("listable", func(t *testing.T) {
		runID := sdkRetentionRun(t, world, sdkRetainNone(), nil)
		sdkComplete(t, world, runID)
		page, err := world.RunsList(ctx, worlds.RunsListInput{WorkflowName: "retention-workflow", Status: []worlds.Status{worlds.StatusCompleted}})
		if err != nil {
			t.Fatal(err)
		}
		index := slices.IndexFunc(page.Data, func(run worlds.Run) bool { return run.RunID == runID })
		if index < 0 {
			t.Fatal("purged run is not listed")
		}
		if found := page.Data[index]; found.Status != worlds.StatusCompleted || found.WorkflowName != "retention-workflow" || found.Attributes[worlds.RetentionAttribute] != "0" {
			t.Fatalf("listed run = %+v", found)
		}
	})
	t.Run("steps", func(t *testing.T) {
		runID := sdkRetentionRun(t, world, sdkRetainNone(), nil)
		sdkComplete(t, world, runID)
		steps, err := world.StepsList(ctx, worlds.StepsListInput{RunID: runID})
		if err != nil || len(steps.Data) != 1 || steps.Data[0].StepID != "step_001" || steps.Data[0].Status != worlds.StatusCompleted || steps.Data[0].Input != nil || steps.Data[0].Output != nil {
			t.Fatalf("steps = %+v err = %v", steps.Data, err)
		}
	})
	t.Run("events", func(t *testing.T) {
		runID := sdkRetentionRun(t, world, sdkRetainNone(), nil)
		sdkComplete(t, world, runID)
		events := conformAllEvents(t, world, runID)
		want := []worlds.EventType{worlds.EventTypeRunCreated, worlds.EventTypeRunStarted, worlds.EventTypeStepCreated, worlds.EventTypeStepCompleted, worlds.EventTypeRunCompleted}
		if !slices.Equal(sdkTypes(events), want) {
			t.Fatalf("event types = %v", sdkTypes(events))
		}
		noPayloads(t, runID)
		var created worlds.DataStepCreated
		if json.Unmarshal(events[2].Data, &created) != nil || created.StepName != "step//./mod//doWork" {
			t.Fatalf("step_created data = %s", events[2].Data)
		}
	})
	t.Run("streams", func(t *testing.T) {
		runID := sdkRetentionRun(t, world, sdkRetainNone(), nil)
		sdkComplete(t, world, runID)
		if names, err := world.StreamsList(ctx, worlds.StreamsListInput{RunID: runID}); err != nil || !slices.Equal(names, []string{sdkRunStream("stream-a", runID)}) {
			t.Fatalf("streams = %v err = %v", names, err)
		}
		chunks, err := world.StreamsGetChunks(ctx, worlds.StreamsGetChunksInput{RunID: runID, Name: sdkRunStream("stream-a", runID)})
		if err != nil || len(chunks.Data) != 0 || !chunks.Done || chunks.HasMore {
			t.Fatalf("chunks = %+v err = %v", chunks, err)
		}
		if info, err := world.StreamsGetInfo(ctx, worlds.StreamsGetInfoInput{RunID: runID, Name: sdkRunStream("stream-a", runID)}); err != nil || info != (worlds.StreamInfo{TailIndex: -1, Done: true}) {
			t.Fatalf("info = %+v err = %v", info, err)
		}
	})
	t.Run("streams first written together", func(t *testing.T) {
		runID := sdkRetentionRun(t, world, sdkRetainNone(), nil)
		concurrent := []string{sdkRunStream("stream-b", runID), sdkRunStream("stream-c", runID), sdkRunStream("stream-d", runID)}
		errs := make([]error, len(concurrent))
		var group sync.WaitGroup
		for index, name := range concurrent {
			group.Go(func() {
				errs[index] = world.StreamsWrite(ctx, worlds.StreamsWriteInput{RunID: runID, Name: name, Chunk: sdkStreamChunk})
			})
		}
		group.Wait()
		if err := errors.Join(errs...); err != nil {
			t.Fatal(err)
		}
		sdkComplete(t, world, runID)
		sdkExpectEmptyStreams(t, world, runID, append([]string{sdkRunStream("stream-a", runID)}, concurrent...)...)
	})
	for _, late := range []struct {
		eventType worlds.EventType
		data      worlds.DataStepResult
	}{
		{worlds.EventTypeStepCompleted, worlds.DataStepResult{Result: sdkStepOutput}},
		{worlds.EventTypeStepFailed, worlds.DataStepResult{Error: sdkStepOutput}},
		{worlds.EventTypeStepRetrying, worlds.DataStepResult{Error: sdkStepOutput}},
	} {
		t.Run("late "+string(late.eventType), func(t *testing.T) {
			runID := sdkRetentionRun(t, world, sdkRetainNone(), nil)
			sdkLateStep(t, world, runID, sdkStepInput)
			sdkComplete(t, world, runID)
			conformAppend(t, world, runID, late.eventType, "step_late", late.data)
			step, err := world.StepsGet(ctx, worlds.StepsGetInput{RunID: runID, StepID: "step_late"})
			if err != nil || step.Output != nil || step.Error != nil || step.Input != nil {
				t.Fatalf("late step = %+v err = %v", step, err)
			}
			noPayloads(t, runID)
		})
	}
	t.Run("stream first written after the purge", func(t *testing.T) {
		runID := sdkRetentionRun(t, world, sdkRetainNone(), nil)
		sdkComplete(t, world, runID)
		for _, write := range []worlds.StreamsWriteInput{
			{RunID: runID, Name: sdkRunStream("stream-late", runID), Chunk: sdkStreamChunk},
			{RunID: runID, Name: sdkRunStream("stream-late", runID), Chunk: []byte("more")},
			{RunID: runID, Name: sdkRunStream("stream-a", runID), Chunk: sdkStreamChunk},
		} {
			conformExpectDropped(t, world.StreamsWrite(ctx, write), "write to "+write.Name+" after the purge")
		}
		sdkExpectEmptyStreams(t, world, runID, sdkRunStream("stream-late", runID), sdkRunStream("stream-a", runID))
	})
	t.Run("hook outliving the run", func(t *testing.T) {
		runID := sdkRetentionRun(t, world, sdkRetainNone(), nil)
		token := "reserved-token:" + string(runID)
		until := time.Now().Add(time.Minute).UTC()
		conformAppend(t, world, runID, worlds.EventTypeHookCreated, "hook_001", worlds.DataHookCreated{Token: token, TokenRetentionUntil: &until, Metadata: sdkHookMeta})
		sdkComplete(t, world, runID)
		hook, err := world.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: token})
		if err != nil || hook.HookID != "hook_001" || hook.Metadata != nil {
			t.Fatalf("retained hook = %+v err = %v", hook, err)
		}
	})
	t.Run("failed run", func(t *testing.T) {
		runID := sdkRetentionRun(t, world, sdkRetainNone(), nil)
		conformAppend(t, world, runID, worlds.EventTypeRunFailed, "", worlds.DataRunFailed{Error: []byte(`[255]`)})
		if run := get(t, runID); run.Status != worlds.StatusFailed || run.Error != nil || run.ExpiredAt == nil {
			t.Fatalf("failed run = %+v", run)
		}
		noPayloads(t, runID)
	})
	t.Run("cancelled run", func(t *testing.T) {
		runID := sdkRetentionRun(t, world, sdkRetainNone(), nil)
		conformAppend(t, world, runID, worlds.EventTypeRunCancelled, "", nil)
		if run := get(t, runID); run.Status != worlds.StatusCancelled || run.Input != nil || run.ExpiredAt == nil {
			t.Fatalf("cancelled run = %+v", run)
		}
		noPayloads(t, runID)
	})
	t.Run("freshest attributes", func(t *testing.T) {
		if !world.Capabilities().SetAttributes {
			t.Skip("the World does not store attributes")
		}
		runID := sdkRetentionRun(t, world, nil, nil)
		retainNone := worlds.RetainNone.String()
		if err := world.RunsSetAttributes(ctx, worlds.RunsSetAttributesInput{RunID: runID, Changes: []worlds.RunsAttributeChange{{Key: worlds.RetentionAttribute, Value: &retainNone}}, AllowReservedAttributes: true}); err != nil {
			t.Fatal(err)
		}
		sdkComplete(t, world, runID)
		if run := get(t, runID); run.ExpiredAt == nil || run.Output != nil {
			t.Fatalf("run = %+v", run)
		}
		noPayloads(t, runID)
	})
	t.Run("other runs", func(t *testing.T) {
		keep := sdkRetentionRun(t, world, nil, nil)
		purge := sdkRetentionRun(t, world, sdkRetainNone(), nil)
		sdkComplete(t, world, purge)
		sdkComplete(t, world, keep)
		if run := get(t, keep); string(run.Input) != string(sdkRunInput) || string(run.Output) != string(sdkRunOutput) || run.ExpiredAt != nil {
			t.Fatalf("kept run = %+v", run)
		}
		if steps, err := world.StepsList(ctx, worlds.StepsListInput{RunID: keep}); err != nil || string(steps.Data[0].Output) != string(sdkStepOutput) {
			t.Fatalf("kept steps = %+v err = %v", steps.Data, err)
		}
	})
}

// Payloads of a step that finishes after its run ended, long and distinct.
var (
	sdkLateInput  = []byte(`"` + strings.Repeat("late-step-input-", 40) + `"`)
	sdkLateResult = []byte(`"` + strings.Repeat("late-step-input-", 40) + strings.Repeat("late-step-result-", 40) + `"`)
	sdkLateError  = []byte(`"` + strings.Repeat("late-step-error-", 40) + `"`)
)

// SDK: packages/world-local/src/storage/run-retention.test.ts > run retention (world-local) > $retention: '0', a step that finishes after the run ended > scrubs a step_completed that lands after the run was cancelled
// SDK: packages/world-local/src/storage/run-retention.test.ts > run retention (world-local) > $retention: '0', a step that finishes after the run ended > scrubs a step_failed that lands after the run ended
// SDK: packages/world-local/src/storage/run-retention.test.ts > run retention (world-local) > $retention: '0', a step that finishes after the run ended > scrubs a step_retrying that lands after the run ended
// SDK: packages/world-local/src/storage/run-retention.test.ts > run retention (world-local) > $retention: '0', a step that finishes after the run ended > leaves no payload whichever write commits first (${order})
// SDK: packages/world-local/src/storage/run-retention.test.ts > run retention (world-local) > $retention: '0', a step that finishes after the run ended > still scrubs when both writes are in flight at once
// SDK: packages/world-local/src/storage/run-retention.test.ts > run retention (world-local) > $retention: '0', a step that finishes after the run ended > keeps a late result on a run without zero retention
func conformSDKRetainNoneLateSteps(t *testing.T, world worlds.World) {
	ctx := context.Background()
	running := func(t *testing.T, attributes map[string]string) worlds.RunID {
		t.Helper()
		runID := worlds.NewRunID()
		if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "retention-workflow", Input: sdkRunInput, Attributes: attributes, AllowReservedAttributes: attributes != nil}); err != nil {
			t.Fatal(err)
		}
		conformAppend(t, world, runID, worlds.EventTypeRunStarted, "", nil)
		sdkStep(t, world, runID, "step_late", "step//./mod//slowWork", sdkLateInput)
		conformAppend(t, world, runID, worlds.EventTypeStepStarted, "step_late", nil)
		return runID
	}
	cancel := func(runID worlds.RunID) error {
		_, err := sdkTry(world, runID, worlds.EventTypeRunCancelled, "", nil)
		return err
	}
	finish := func(runID worlds.RunID) (worlds.EventResult, error) {
		return sdkTry(world, runID, worlds.EventTypeStepCompleted, "step_late", worlds.DataStepResult{Result: sdkLateResult})
	}
	step := func(t *testing.T, runID worlds.RunID) worlds.Step {
		t.Helper()
		step, err := world.StepsGet(ctx, worlds.StepsGetInput{RunID: runID, StepID: "step_late"})
		if err != nil {
			t.Fatal(err)
		}
		return step
	}
	must := func(t *testing.T, err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	noPayloads := func(t *testing.T, runID worlds.RunID) {
		t.Helper()
		if payloads := sdkPayloads(t, world, runID); len(payloads) != 0 {
			t.Fatalf("payloads left = %v", payloads)
		}
	}
	t.Run("step_completed after a cancel", func(t *testing.T) {
		runID := running(t, sdkRetainNone())
		must(t, cancel(runID))
		if input := step(t, runID).Input; input != nil {
			t.Fatalf("step input after the cancel = %s", input)
		}
		late, err := finish(runID)
		must(t, err)
		event, err := world.EventsGet(ctx, worlds.EventsGetInput{RunID: runID, EventID: late.EventID})
		var data worlds.DataStepResult
		if err != nil || late.EventType != worlds.EventTypeStepCompleted || json.Unmarshal(event.Data, &data) != nil && event.Data != nil || data.Result != nil {
			t.Fatalf("late step_completed = %+v, stored %s err = %v", late, event.Data, err)
		}
		if got := step(t, runID); got.Status != worlds.StatusCompleted || got.Output != nil || got.Input != nil {
			t.Fatalf("late step = %+v", got)
		}
		want := []worlds.EventType{worlds.EventTypeRunCreated, worlds.EventTypeRunStarted, worlds.EventTypeStepCreated, worlds.EventTypeStepStarted, worlds.EventTypeRunCancelled, worlds.EventTypeStepCompleted}
		if types := sdkTypes(conformAllEvents(t, world, runID)); !slices.Equal(types, want) {
			t.Fatalf("event types = %v", types)
		}
		noPayloads(t, runID)
		if run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID}); err != nil || run.Status != worlds.StatusCancelled || run.Input != nil {
			t.Fatalf("run = %+v err = %v", run, err)
		}
	})
	for _, late := range []struct {
		eventType worlds.EventType
		status    worlds.Status
	}{{worlds.EventTypeStepFailed, worlds.StatusFailed}, {worlds.EventTypeStepRetrying, worlds.StatusPending}} {
		t.Run(string(late.eventType)+" after a cancel", func(t *testing.T) {
			runID := running(t, sdkRetainNone())
			must(t, cancel(runID))
			conformAppend(t, world, runID, late.eventType, "step_late", worlds.DataStepResult{Error: sdkLateError})
			if got := step(t, runID); got.Status != late.status || got.Error != nil {
				t.Fatalf("late step = %+v", got)
			}
			noPayloads(t, runID)
		})
	}
	for _, order := range []string{"cancel first", "step first"} {
		t.Run(order, func(t *testing.T) {
			runID := running(t, sdkRetainNone())
			if order == "cancel first" {
				must(t, cancel(runID))
				_, err := finish(runID)
				must(t, err)
			} else {
				_, err := finish(runID)
				must(t, err)
				must(t, cancel(runID))
			}
			if got := step(t, runID); got.Status != worlds.StatusCompleted || got.Input != nil || got.Output != nil {
				t.Fatalf("step = %+v", got)
			}
			if run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID}); err != nil || run.Status != worlds.StatusCancelled {
				t.Fatalf("run = %+v err = %v", run, err)
			}
			noPayloads(t, runID)
		})
	}
	t.Run("both in flight", func(t *testing.T) {
		runID := running(t, sdkRetainNone())
		var cancelErr, finishErr error
		var group sync.WaitGroup
		group.Go(func() { cancelErr = cancel(runID) })
		group.Go(func() { _, finishErr = finish(runID) })
		group.Wait()
		must(t, errors.Join(cancelErr, finishErr))
		if got := step(t, runID); got.Output != nil {
			t.Fatalf("step output = %s", got.Output)
		}
		noPayloads(t, runID)
	})
	t.Run("kept without zero retention", func(t *testing.T) {
		runID := running(t, nil)
		must(t, cancel(runID))
		late, err := finish(runID)
		must(t, err)
		if got := step(t, runID); string(got.Input) != string(sdkLateInput) || string(got.Output) != string(sdkLateResult) {
			t.Fatalf("step = %+v", got)
		}
		event, err := world.EventsGet(ctx, worlds.EventsGetInput{RunID: runID, EventID: late.EventID})
		var data worlds.DataStepResult
		if err != nil || json.Unmarshal(event.Data, &data) != nil || string(data.Result) != string(sdkLateResult) {
			t.Fatalf("step_completed = %s err = %v", event.Data, err)
		}
		if run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID}); err != nil || string(run.Input) != string(sdkRunInput) {
			t.Fatalf("run = %+v err = %v", run, err)
		}
	})
}

// SDK: packages/world-local/src/storage/run-retention.test.ts > run retention (world-local) > values this World does not implement > keeps everything for ${label}
// SDK: packages/world-local/src/storage/run-retention.test.ts > run retention (world-local) > values this World does not implement > keeps the output of a step that settles after the run ends
// SDK: packages/world-local/src/storage/run-retention.test.ts > run retention (world-local) > values this World does not implement > keeps the data for an unrelated reserved attribute
// SDK: packages/world-local/src/storage/run-retention.test.ts > run retention (world-local) > values this World does not implement > does not purge before the run is terminal
func conformSDKRetentionKeeps(t *testing.T, world worlds.World) {
	ctx := context.Background()
	get := func(t *testing.T, runID worlds.RunID) worlds.Run {
		t.Helper()
		run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID})
		if err != nil {
			t.Fatal(err)
		}
		return run
	}
	steps := func(t *testing.T, runID worlds.RunID) []worlds.Step {
		t.Helper()
		page, err := world.StepsList(ctx, worlds.StepsListInput{RunID: runID})
		if err != nil || len(page.Data) == 0 {
			t.Fatalf("steps = %+v err = %v", page.Data, err)
		}
		return page.Data
	}
	keepers := []struct {
		label      string
		attributes map[string]string
	}{
		{"absent", nil},
		{"default", map[string]string{worlds.RetentionAttribute: "default"}},
		{"a non-zero duration", map[string]string{worlds.RetentionAttribute: "7"}},
		{"a very large duration", map[string]string{worlds.RetentionAttribute: "999999999"}},
		{"a malformed value", map[string]string{worlds.RetentionAttribute: "none"}},
		{"a non-integer", map[string]string{worlds.RetentionAttribute: "0.0"}},
		{"a signed zero", map[string]string{worlds.RetentionAttribute: "-0"}},
		{"a padded zero", map[string]string{worlds.RetentionAttribute: "00"}},
		{"an empty value", map[string]string{worlds.RetentionAttribute: ""}},
	}
	for _, keeper := range keepers {
		t.Run("keeps everything for "+keeper.label, func(t *testing.T) {
			runID := sdkRetentionRun(t, world, keeper.attributes, nil)
			sdkComplete(t, world, runID)
			if run := get(t, runID); run.ExpiredAt != nil || string(run.Input) != string(sdkRunInput) || string(run.Output) != string(sdkRunOutput) {
				t.Fatalf("run = %+v", run)
			}
			if step := steps(t, runID)[0]; string(step.Input) != string(sdkStepInput) || string(step.Output) != string(sdkStepOutput) {
				t.Fatalf("step = %+v", step)
			}
			if payloads := sdkPayloads(t, world, runID); len(payloads) != 4 ||
				!strings.HasPrefix(payloads[0], "run_created input=") || !strings.HasPrefix(payloads[1], "step_created input=") ||
				!strings.HasPrefix(payloads[2], "step_completed result=") || !strings.HasPrefix(payloads[3], "run_completed output=") {
				t.Fatalf("payloads = %v", payloads)
			}
			chunks, err := world.StreamsGetChunks(ctx, worlds.StreamsGetChunksInput{RunID: runID, Name: sdkRunStream("stream-a", runID)})
			if err != nil || len(chunks.Data) != 1 || string(chunks.Data[0].Data) != string(sdkStreamChunk) {
				t.Fatalf("chunks = %+v err = %v", chunks, err)
			}
		})
	}
	t.Run("late step output", func(t *testing.T) {
		runID := sdkRetentionRun(t, world, nil, nil)
		sdkLateStep(t, world, runID, sdkStepInput)
		sdkComplete(t, world, runID)
		conformAppend(t, world, runID, worlds.EventTypeStepCompleted, "step_late", worlds.DataStepResult{Result: sdkStepOutput})
		if err := world.StreamsWrite(ctx, worlds.StreamsWriteInput{RunID: runID, Name: sdkRunStream("stream-late", runID), Chunk: []byte("kept")}); err != nil {
			t.Fatal(err)
		}
		if step, err := world.StepsGet(ctx, worlds.StepsGetInput{RunID: runID, StepID: "step_late"}); err != nil || string(step.Output) != string(sdkStepOutput) {
			t.Fatalf("late step = %+v err = %v", step, err)
		}
		if chunks, err := world.StreamsGetChunks(ctx, worlds.StreamsGetChunksInput{RunID: runID, Name: sdkRunStream("stream-late", runID)}); err != nil || len(chunks.Data) != 1 {
			t.Fatalf("late stream = %+v err = %v", chunks, err)
		}
	})
	t.Run("unrelated reserved attribute", func(t *testing.T) {
		runID := sdkRetentionRun(t, world, map[string]string{"$someOtherThing": "0"}, nil)
		sdkComplete(t, world, runID)
		if run := get(t, runID); run.ExpiredAt != nil || string(run.Output) != string(sdkRunOutput) {
			t.Fatalf("run = %+v", run)
		}
	})
	t.Run("not before the run is terminal", func(t *testing.T) {
		runID := sdkRetentionRun(t, world, sdkRetainNone(), nil)
		if run := get(t, runID); run.Status != worlds.StatusRunning || run.ExpiredAt != nil || string(run.Input) != string(sdkRunInput) {
			t.Fatalf("running run = %+v", run)
		}
		if step := steps(t, runID)[0]; string(step.Output) != string(sdkStepOutput) {
			t.Fatalf("step = %+v", step)
		}
	})
}

// SDK: packages/world-local/src/storage/delete-data-many.test.ts > runs.deleteDataMany (world-local) > deletes a finished run's data, keeping the run, its steps and its events listed
// SDK: packages/world-local/src/storage/delete-data-many.test.ts > runs.deleteDataMany (world-local) > scrubs the payload of a ${eventType} that lands after the deletion
// SDK: packages/world-local/src/storage/delete-data-many.test.ts > runs.deleteDataMany (world-local) > ends a stream first written after the deletion, with nothing in it
// SDK: packages/world-local/src/storage/delete-data-many.test.ts > runs.deleteDataMany (world-local) > answers already_deleted on a second call, keeping the first expiredAt
// SDK: packages/world-local/src/storage/delete-data-many.test.ts > runs.deleteDataMany (world-local) > skips a run that has not finished, leaving it running with its data
// SDK: packages/world-local/src/storage/delete-data-many.test.ts > runs.deleteDataMany (world-local) > answers every run in request order, with not_found for unknown IDs
// SDK: packages/world-local/src/storage/delete-data-many.test.ts > runs.deleteDataMany (world-local) > rejects an empty, duplicated or oversized request
func conformSDKDeleteDataMany(t *testing.T, world worlds.World) {
	if !world.Capabilities().DeleteData {
		t.Skip("the World does not delete data on demand")
	}
	ctx := context.Background()
	deleteData := func(t *testing.T, runIDs ...worlds.RunID) worlds.RunsDeleteDataManyResult {
		t.Helper()
		result, err := world.RunsDeleteDataMany(ctx, worlds.RunsDeleteDataManyInput{RunIDs: runIDs})
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	get := func(t *testing.T, runID worlds.RunID) worlds.Run {
		t.Helper()
		run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID})
		if err != nil {
			t.Fatal(err)
		}
		return run
	}
	finished := func(t *testing.T) worlds.RunID {
		t.Helper()
		runID := sdkRetentionRun(t, world, nil, nil)
		sdkComplete(t, world, runID)
		return runID
	}
	t.Run("a finished run", func(t *testing.T) {
		runID := finished(t)
		if len(sdkPayloads(t, world, runID)) == 0 {
			t.Fatal("the run holds no payloads to delete")
		}
		before := time.Now().Truncate(time.Millisecond)
		result := deleteData(t, runID)
		want := worlds.RunsDeleteDataManyResult{Summary: worlds.DeleteDataSummary{Requested: 1, Deleted: 1}, Results: []worlds.DeleteDataResult{{RunID: runID, Outcome: worlds.DeleteDataDeleted}}}
		if result.Summary != want.Summary || !slices.Equal(result.Results, want.Results) {
			t.Fatalf("result = %+v, want %+v", result, want)
		}
		if run := get(t, runID); run.Status != worlds.StatusCompleted || run.Input != nil || run.Output != nil || run.ExpiredAt == nil || run.ExpiredAt.Before(before) {
			t.Fatalf("run = %+v", run)
		}
		steps, err := world.StepsList(ctx, worlds.StepsListInput{RunID: runID})
		if err != nil || len(steps.Data) != 1 || steps.Data[0].Input != nil || steps.Data[0].Output != nil {
			t.Fatalf("steps = %+v err = %v", steps.Data, err)
		}
		if payloads := sdkPayloads(t, world, runID); len(payloads) != 0 {
			t.Fatalf("payloads left = %v", payloads)
		}
		if names, err := world.StreamsList(ctx, worlds.StreamsListInput{RunID: runID}); err != nil || !slices.Equal(names, []string{sdkRunStream("stream-a", runID)}) {
			t.Fatalf("streams = %v err = %v", names, err)
		}
		sdkExpectEmptyStreams(t, world, runID, sdkRunStream("stream-a", runID))
	})
	for _, late := range []struct {
		eventType worlds.EventType
		data      worlds.DataStepResult
	}{
		{worlds.EventTypeStepCompleted, worlds.DataStepResult{Result: sdkStepOutput}},
		{worlds.EventTypeStepFailed, worlds.DataStepResult{Error: sdkStepOutput}},
		{worlds.EventTypeStepRetrying, worlds.DataStepResult{Error: sdkStepOutput}},
	} {
		t.Run("late "+string(late.eventType), func(t *testing.T) {
			runID := sdkRetentionRun(t, world, nil, nil)
			sdkLateStep(t, world, runID, sdkStepInput)
			sdkComplete(t, world, runID)
			deleteData(t, runID)
			conformAppend(t, world, runID, late.eventType, "step_late", late.data)
			step, err := world.StepsGet(ctx, worlds.StepsGetInput{RunID: runID, StepID: "step_late"})
			if err != nil || step.Output != nil || step.Error != nil || step.Input != nil {
				t.Fatalf("late step = %+v err = %v", step, err)
			}
			if payloads := sdkPayloads(t, world, runID); len(payloads) != 0 {
				t.Fatalf("payloads left = %v", payloads)
			}
		})
	}
	t.Run("stream first written after the deletion", func(t *testing.T) {
		runID := finished(t)
		deleteData(t, runID)
		for _, write := range []worlds.StreamsWriteInput{
			{RunID: runID, Name: sdkRunStream("stream-late", runID), Chunk: sdkStreamChunk},
			{RunID: runID, Name: sdkRunStream("stream-late", runID), Chunk: []byte("more")},
			{RunID: runID, Name: sdkRunStream("stream-a", runID), Chunk: sdkStreamChunk},
		} {
			conformExpectDropped(t, world.StreamsWrite(ctx, write), "write to "+write.Name+" after the deletion")
		}
		sdkExpectEmptyStreams(t, world, runID, sdkRunStream("stream-late", runID), sdkRunStream("stream-a", runID))
	})
	t.Run("already deleted", func(t *testing.T) {
		runID := finished(t)
		deleteData(t, runID)
		expiredAt := get(t, runID).ExpiredAt
		again := deleteData(t, runID)
		if !slices.Equal(again.Results, []worlds.DeleteDataResult{{RunID: runID, Outcome: worlds.DeleteDataAlreadyDeleted}}) || again.Summary.AlreadyDeleted != 1 {
			t.Fatalf("second deletion = %+v", again)
		}
		if run := get(t, runID); run.ExpiredAt == nil || !run.ExpiredAt.Equal(*expiredAt) {
			t.Fatalf("ExpiredAt = %v, want %v", run.ExpiredAt, expiredAt)
		}
	})
	t.Run("unfinished run", func(t *testing.T) {
		runID := sdkRetentionRun(t, world, nil, nil)
		result := deleteData(t, runID)
		want := []worlds.DeleteDataResult{{RunID: runID, Outcome: worlds.DeleteDataSkipped, Reason: worlds.DeleteDataRunNotTerminal, Status: worlds.StatusRunning}}
		if !slices.Equal(result.Results, want) {
			t.Fatalf("results = %+v", result.Results)
		}
		if run := get(t, runID); run.Status != worlds.StatusRunning || string(run.Input) != string(sdkRunInput) || run.ExpiredAt != nil {
			t.Fatalf("run = %+v", run)
		}
		if steps, err := world.StepsList(ctx, worlds.StepsListInput{RunID: runID}); err != nil || string(steps.Data[0].Output) != string(sdkStepOutput) {
			t.Fatalf("steps = %+v err = %v", steps.Data, err)
		}
	})
	t.Run("request order", func(t *testing.T) {
		done := finished(t)
		running := sdkRetentionRun(t, world, nil, nil)
		result := deleteData(t, "wrun_missing", running, done, "../escape")
		outcomes := make([]string, len(result.Results))
		for index, item := range result.Results {
			outcomes[index] = fmt.Sprintf("%s %s", item.RunID, item.Outcome)
		}
		want := []string{"wrun_missing not_found", string(running) + " skipped", string(done) + " deleted", "../escape not_found"}
		if !slices.Equal(outcomes, want) {
			t.Fatalf("outcomes = %v, want %v", outcomes, want)
		}
		if result.Summary != (worlds.DeleteDataSummary{Requested: 4, Deleted: 1, Skipped: 1, NotFound: 2}) {
			t.Fatalf("summary = %+v", result.Summary)
		}
	})
	t.Run("invalid requests", func(t *testing.T) {
		oversized := make([]worlds.RunID, 101)
		for index := range oversized {
			oversized[index] = worlds.RunID(fmt.Sprintf("wrun_%d", index))
		}
		for _, runIDs := range [][]worlds.RunID{{}, {"wrun_a", "wrun_a"}, oversized} {
			_, err := world.RunsDeleteDataMany(ctx, worlds.RunsDeleteDataManyInput{RunIDs: runIDs})
			conformExpect(t, err, worlds.ErrDeleteDataInvalid, fmt.Sprintf("a request of %d run IDs", len(runIDs)))
		}
	})
}

// setSDKAttributes returns the error of RunsSetAttributes applying
// changes to a new run created with attributes.
func setSDKAttributes(t *testing.T, world worlds.World, attributes map[string]string, allowReserved bool, changes ...worlds.RunsAttributeChange) (worlds.RunID, error) {
	t.Helper()
	ctx := context.Background()
	runID := conformRunID("sdk_attributes")
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "conformance", Input: []byte(`"input"`), Attributes: attributes}); err != nil {
		t.Fatalf("EventsCreateRun: %v", err)
	}
	return runID, world.RunsSetAttributes(ctx, worlds.RunsSetAttributesInput{RunID: runID, Changes: changes, AllowReservedAttributes: allowReserved})
}

// conformSDKChange returns the change that sets key to value.
func conformSDKChange(key, value string) worlds.RunsAttributeChange {
	return worlds.RunsAttributeChange{Key: key, Value: &value}
}

// conformSDKKeys returns count changes that set the keys k0 to k<count-1>
// to "v".
func conformSDKKeys(count int) []worlds.RunsAttributeChange {
	changes := make([]worlds.RunsAttributeChange, count)
	for index := range changes {
		changes[index] = conformSDKChange(fmt.Sprintf("k%d", index), "v")
	}
	return changes
}

// conformSDKAttributeLimits checks the limits a World holds attribute
// writes to: a key's length, a value's UTF-8 bytes, keys set once per write,
// the attributes a run holds after a write, and reserved keys.
//
// SDK: packages/world/src/attributes.test.ts > attribute schemas > accepts a normal key
// SDK: packages/world/src/attributes.test.ts > attribute schemas > rejects empty keys
// SDK: packages/world/src/attributes.test.ts > attribute schemas > rejects keys over the length cap
// SDK: packages/world/src/attributes.test.ts > attribute schemas > accepts keys exactly at the length cap
// SDK: packages/world/src/attributes.test.ts > attribute schemas > accepts null (unset)
// SDK: packages/world/src/attributes.test.ts > attribute schemas > accepts a normal string
// SDK: packages/world/src/attributes.test.ts > attribute schemas > rejects values over the byte cap
// SDK: packages/world/src/attributes.test.ts > attribute schemas > counts UTF-8 bytes, not characters
// SDK: packages/world/src/attributes.test.ts > attribute schemas > validates complete changes and batches
// SDK: packages/world/src/attributes.test.ts > attribute schemas > rejects batches over the per-run cap
// SDK: packages/world/src/attributes.test.ts > validateAttributeChanges > accepts a small batch of valid changes
// SDK: packages/world/src/attributes.test.ts > validateAttributeChanges > rejects duplicate keys within a single batch
// SDK: packages/world/src/attributes.test.ts > validateAttributeChanges > rejects when post-merge count exceeds the per-run cap
// SDK: packages/world/src/attributes.test.ts > validateAttributeChanges > does not let an unknown deletion offset a new attribute
// SDK: packages/world/src/attributes.test.ts > validateAttributeChanges > does not count upserts on already-present keys against the cap
// SDK: packages/world/src/attributes.test.ts > validateAttributeChanges > rejects reserved-prefix keys in a batch by default
// SDK: packages/world/src/attributes.test.ts > validateAttributeChanges > accepts reserved-prefix keys when allowReservedAttributes is set
func conformSDKAttributeLimits(t *testing.T, world worlds.World) {
	if !world.Capabilities().SetAttributes {
		t.Skip("the World does not store attribute writes")
	}
	full := map[string]string{}
	for index := range worlds.MaxAttributesPerRun {
		full[fmt.Sprintf("k%d", index)] = "v"
	}
	for _, test := range []struct {
		name          string
		attributes    map[string]string
		allowReserved bool
		changes       []worlds.RunsAttributeChange
		valid         bool
	}{
		{"a normal key", nil, false, []worlds.RunsAttributeChange{conformSDKChange("phase", "running")}, true},
		{"an empty key", nil, false, []worlds.RunsAttributeChange{conformSDKChange("", "running")}, false},
		{"a key over the length cap", nil, false, []worlds.RunsAttributeChange{conformSDKChange(strings.Repeat("k", 257), "v")}, false},
		{"a key at the length cap", nil, false, []worlds.RunsAttributeChange{conformSDKChange(strings.Repeat("k", 256), "v")}, true},
		{"an unset", nil, false, []worlds.RunsAttributeChange{{Key: "phase"}}, true},
		{"a normal string", nil, false, []worlds.RunsAttributeChange{conformSDKChange("greeting", "hello")}, true},
		{"a value over the byte cap", nil, false, []worlds.RunsAttributeChange{conformSDKChange("big", strings.Repeat("a", 257))}, false},
		// A four-byte emoji: 64 of them are 256 bytes, the cap.
		{"a value at the byte cap in UTF-8", nil, false, []worlds.RunsAttributeChange{conformSDKChange("emoji", strings.Repeat("💥", 64))}, true},
		{"a value over the byte cap in UTF-8", nil, false, []worlds.RunsAttributeChange{conformSDKChange("emoji", strings.Repeat("💥", 65))}, false},
		{"a key set twice", nil, false, []worlds.RunsAttributeChange{conformSDKChange("phase", "running"), conformSDKChange("phase", "done")}, false},
		{"a batch over the per-run cap", nil, false, conformSDKKeys(worlds.MaxAttributesPerRun + 1), false},
		{"a small batch", nil, false, []worlds.RunsAttributeChange{conformSDKChange("phase", "init"), {Key: "stale"}}, true},
		{"a batch past the cap with the run's attribute", map[string]string{"preexisting": "v"}, false, conformSDKKeys(worlds.MaxAttributesPerRun), false},
		{"a batch past the cap with an unknown deletion", nil, false, append(conformSDKKeys(worlds.MaxAttributesPerRun+1), worlds.RunsAttributeChange{Key: "not-known-to-exist"}), false},
		{"an update of a full run", full, false, []worlds.RunsAttributeChange{conformSDKChange("k0", "updated")}, true},
		{"a reserved key", nil, false, []worlds.RunsAttributeChange{conformSDKChange("phase", "init"), conformSDKChange("$framework.kind", "agent")}, false},
		{"an allowed reserved key", nil, true, []worlds.RunsAttributeChange{conformSDKChange("phase", "init"), conformSDKChange("$framework.kind", "agent")}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := setSDKAttributes(t, world, test.attributes, test.allowReserved, test.changes...)
			if test.valid && err != nil {
				t.Fatalf("RunsSetAttributes: %v", err)
			}
			if !test.valid {
				conformExpect(t, err, worlds.ErrAttributeInvalid, test.name)
			}
		})
	}
}

// conformSDKAttributeChanges checks how an attribute write changes a run's
// attributes: it sets and overwrites keys, and removes the keys it unsets.
//
// SDK: packages/world/src/attributes.test.ts > applyAttributeChanges > upserts new keys
// SDK: packages/world/src/attributes.test.ts > applyAttributeChanges > overwrites existing keys
// SDK: packages/world/src/attributes.test.ts > applyAttributeChanges > removes keys when value is null
// SDK: packages/world/src/attributes.test.ts > applyAttributeChanges > applies set and unset in a single batch
// SDK: packages/world/src/attributes.test.ts > applyAttributeChanges > treats undefined existing as the empty record
func conformSDKAttributeChanges(t *testing.T, world worlds.World) {
	if !world.Capabilities().SetAttributes {
		t.Skip("the World does not store attribute writes")
	}
	for _, test := range []struct {
		name       string
		attributes map[string]string
		changes    []worlds.RunsAttributeChange
		want       map[string]string
	}{
		{"upsert", map[string]string{"a": "1"}, []worlds.RunsAttributeChange{conformSDKChange("b", "2")}, map[string]string{"a": "1", "b": "2"}},
		{"overwrite", map[string]string{"a": "1"}, []worlds.RunsAttributeChange{conformSDKChange("a", "2")}, map[string]string{"a": "2"}},
		{"remove", map[string]string{"a": "1", "b": "2"}, []worlds.RunsAttributeChange{{Key: "a"}}, map[string]string{"b": "2"}},
		{"set and unset", map[string]string{"a": "1", "stale": "x"}, []worlds.RunsAttributeChange{{Key: "stale"}, conformSDKChange("fresh", "yes")}, map[string]string{"a": "1", "fresh": "yes"}},
		{"no attributes", nil, []worlds.RunsAttributeChange{conformSDKChange("a", "1")}, map[string]string{"a": "1"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			runID, err := setSDKAttributes(t, world, test.attributes, false, test.changes...)
			if err != nil {
				t.Fatalf("RunsSetAttributes: %v", err)
			}
			run, err := world.RunsGet(context.Background(), worlds.RunsGetInput{RunID: runID})
			if err != nil || !maps.Equal(run.Attributes, test.want) {
				t.Fatalf("attributes = %v err = %v, want %v", run.Attributes, err, test.want)
			}
		})
	}
}

// conformSDKAttributeEventDataSize checks that a World takes an attr_set
// whose Data is MaxAttrSetDataBytes of JSON and refuses one a byte larger,
// counting UTF-8 bytes and JSON escapes, as the Workflow SDK's
// JSON.stringify writes them.
//
// SDK: packages/world/src/attributes.test.ts > validateAttributeEventDataSize > counts %s at the JSON byte boundary
func conformSDKAttributeEventDataSize(t *testing.T, world worlds.World) {
	if !world.Capabilities().SetAttributes {
		t.Skip("the World does not store attribute writes")
	}
	type change struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	type eventData struct {
		Changes []change `json:"changes"`
		Writer  struct {
			Type string `json:"type"`
		} `json:"writer"`
	}
	encode := func(data eventData) []byte {
		encoded, err := json.Marshal(data)
		if err != nil {
			t.Fatal(err)
		}
		return encoded
	}
	for _, test := range []struct{ label, key, value string }{
		{"envelope", "key", ""},
		{"long keys", strings.Repeat("k", 180), ""},
		{"UTF-8 keys", strings.Repeat("é", 90), ""},
		{"UTF-8 values", "key", strings.Repeat("é", 64)},
		{"JSON escapes", "\n\"\\", strings.Repeat("\n\"\\", 12)},
	} {
		t.Run(test.label, func(t *testing.T) {
			data := eventData{}
			data.Writer.Type = worlds.AttributeWriterWorkflow
			for index := range 32 {
				data.Changes = append(data.Changes, change{Key: fmt.Sprintf("%d%s", index, test.key), Value: test.value})
			}
			remaining := worlds.MaxAttrSetDataBytes - len(encode(data))
			for index := range data.Changes {
				padding := min(remaining, worlds.MaxAttributeValueBytes-len(data.Changes[index].Value))
				data.Changes[index].Value += strings.Repeat("x", padding)
				remaining -= padding
			}
			if size := len(encode(data)); remaining != 0 || size != worlds.MaxAttrSetDataBytes {
				t.Fatalf("attr_set data is %d bytes, %d short", size, remaining)
			}
			changes := func() []worlds.RunsAttributeChange {
				var changes []worlds.RunsAttributeChange
				for _, change := range data.Changes {
					changes = append(changes, conformSDKChange(change.Key, change.Value))
				}
				return changes
			}
			if err := worlds.CheckAttributeChanges(nil, changes(), false); err != nil {
				t.Fatalf("changes at the limit: %v", err)
			}
			atLimit := conformCreateRun(t, world, "sdk_attr_size")
			conformAppend(t, world, atLimit, worlds.EventTypeAttrSet, "attr_size", encode(data))

			data.Changes[0].Key += "x"
			if size := len(encode(data)); size != worlds.MaxAttrSetDataBytes+1 {
				t.Fatalf("attr_set data is %d bytes, want %d", size, worlds.MaxAttrSetDataBytes+1)
			}
			if err := worlds.CheckAttributeChanges(nil, changes(), false); err != nil {
				t.Fatalf("changes over the limit: %v", err)
			}
			overLimit := conformCreateRun(t, world, "sdk_attr_size")
			_, err := world.EventsCreate(context.Background(), worlds.EventsCreateInput{RunID: overLimit, Event: conformEvent(t, worlds.EventTypeAttrSet, "attr_size", encode(data))})
			conformExpect(t, err, worlds.ErrAttributeInvalid, "attr_set over the limit")
		})
	}
}

// conformSDKRetention checks that a World deletes a finished run's data
// only when its RetentionAttribute is exactly "0": an absent attribute,
// "default", another duration, and every value that only looks like zero
// keep it.
//
// SDK: packages/world/src/run-retention.test.ts > readRunRetention > purges only on the literal "0"
// SDK: packages/world/src/run-retention.test.ts > readRunRetention > keeps the data when the attribute is absent
// SDK: packages/world/src/run-retention.test.ts > readRunRetention > keeps the data for %s
// SDK: packages/world/src/run-retention.test.ts > purgesUserDataOnFinish > is true only for the literal "0"
// SDK: packages/world/src/run-retention.test.ts > purgesUserDataOnFinish > is false for %s
func conformSDKRetention(t *testing.T, world worlds.World) {
	ctx := context.Background()
	retention := func(value string) map[string]string { return map[string]string{worlds.RetentionAttribute: value} }
	finish := func(t *testing.T, attributes map[string]string) worlds.Run {
		t.Helper()
		runID := conformRunID("sdk_retention")
		if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "conformance", Input: []byte(`"input"`), Attributes: attributes, AllowReservedAttributes: true}); err != nil {
			t.Fatalf("EventsCreateRun: %v", err)
		}
		conformAppend(t, world, runID, worlds.EventTypeRunStarted, "", nil)
		conformAppend(t, world, runID, worlds.EventTypeRunCompleted, "", worlds.DataRunCompleted{Output: []byte(`"output"`)})
		run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID})
		if err != nil {
			t.Fatal(err)
		}
		return run
	}
	t.Run("zero", func(t *testing.T) {
		if run := finish(t, retention("0")); !worlds.RunDataExpired(run, time.Now()) || run.Output != nil {
			t.Fatalf("run with retention 0 = %+v, want its data deleted", run)
		}
	})
	for _, test := range []struct {
		label      string
		attributes map[string]string
	}{
		{"absent", nil},
		{"empty attributes", map[string]string{}},
		{"other attributes", map[string]string{"tenant": "t1"}},
		{"default", retention("default")},
		{"a non-zero duration", retention("7")},
		{"a large duration", retention("86400")},
		{"a malformed word", retention("none")},
		{"a negative number", retention("-1")},
		{"a float", retention("0.0")},
		{"a zero with a unit", retention("0s")},
		{"a padded zero", retention(" 0 ")},
		{"an empty string", retention("")},
		{"a leading-zero integer", retention("007")},
		{"a signed zero", retention("-0")},
		{"a padded-digit zero", retention("00")},
	} {
		t.Run(test.label, func(t *testing.T) {
			if run := finish(t, test.attributes); run.ExpiredAt != nil || string(run.Output) != `"output"` {
				t.Fatalf("run = %+v, want its data kept", run)
			}
		})
	}
}
