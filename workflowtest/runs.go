package workflowtest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

func conformRunCreate(t *testing.T, world worlds.World) {
	ctx := context.Background()
	runID := conformRunID("create")
	created, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "conformance", Input: []byte(`"input"`), Attributes: map[string]string{"team": "a"}})
	if err != nil {
		t.Fatal(err)
	}
	if created.EventID != worlds.EventIDForSlot(1) || created.EventType != worlds.EventTypeRunCreated || created.AllocatedEventCount != 1 || len(created.Events) != 0 {
		t.Fatalf("create result = %#v", created)
	}
	_, err = world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "conformance"})
	conformExpect(t, err, worlds.ErrRunAlreadyExists, "duplicate run")
	_, err = world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{WorkflowName: "conformance"})
	conformExpect(t, err, worlds.ErrEventInvalid, "empty run ID")

	run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID})
	if err != nil {
		t.Fatal(err)
	}
	if run.RunID != runID || run.WorkflowName != "conformance" || run.Status != worlds.StatusPending || string(run.Input) != `"input"` || run.Attributes["team"] != "a" || run.CreatedAt.IsZero() || run.StartedAt != nil {
		t.Fatalf("run = %#v", run)
	}
	stripped, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID, ResolveData: worlds.ResolveDataNone})
	if err != nil || stripped.Input != nil {
		t.Fatalf("ResolveDataNone run = %#v err=%v", stripped, err)
	}
	_, err = world.RunsGet(ctx, worlds.RunsGetInput{RunID: "wrun_missing"})
	conformExpect(t, err, worlds.ErrRunNotFound, "missing run")

	events := conformAllEvents(t, world, runID)
	if len(events) != 1 || events[0].EventType != worlds.EventTypeRunCreated || events[0].RunID != runID || events[0].CreatedAt.IsZero() {
		t.Fatalf("events = %#v", events)
	}
	var data struct {
		WorkflowName string `json:"workflowName"`
		Input        []byte `json:"input"`
	}
	if err := json.Unmarshal(events[0].Data, &data); err != nil || data.WorkflowName != "conformance" || string(data.Input) != `"input"` {
		t.Fatalf("run_created data = %s err=%v", events[0].Data, err)
	}

	empty := conformRunID("empty_attrs")
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: empty, WorkflowName: "conformance"}); err != nil {
		t.Fatal(err)
	}
	run, err = world.RunsGet(ctx, worlds.RunsGetInput{RunID: empty})
	if err != nil || run.Attributes == nil {
		t.Fatalf("run without attributes = %#v err=%v", run, err)
	}
}

func conformRunLifecycle(t *testing.T, world worlds.World) {
	ctx := context.Background()
	runID := conformCreateRun(t, world, "lifecycle")
	first := conformAppend(t, world, runID, worlds.EventTypeRunStarted, "", nil)
	one := 1
	replay, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeRunStarted}, Options: worlds.EventsCreateOptions{EventCount: &one}})
	if err != nil {
		t.Fatal(err)
	}
	if replay.EventID != first.EventID || replay.AllocatedEventCount != 0 || len(replay.Events) != 1 || replay.Events[0].EventID != first.EventID {
		t.Fatalf("run_started replay = %#v", replay)
	}
	run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID})
	if err != nil || run.Status != worlds.StatusRunning || run.StartedAt == nil {
		t.Fatalf("running run = %#v err=%v", run, err)
	}
	conformAppend(t, world, runID, worlds.EventTypeRunCompleted, "", worlds.DataRunCompleted{Output: json.RawMessage(`"output"`)})
	run, err = world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID})
	if err != nil || run.Status != worlds.StatusCompleted || string(run.Output) != `"output"` || run.CompletedAt == nil {
		t.Fatalf("completed run = %#v err=%v", run, err)
	}
	if !run.UpdatedAt.Equal(*run.CompletedAt) {
		t.Fatalf("UpdatedAt = %v, CompletedAt = %v", run.UpdatedAt, run.CompletedAt)
	}
	stripped, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID, ResolveData: worlds.ResolveDataNone})
	if err != nil || stripped.Output != nil || stripped.Input != nil {
		t.Fatalf("ResolveDataNone run = %#v err=%v", stripped, err)
	}
	_, err = world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeWaitCreated, CorrelationID: "wait"}})
	conformExpect(t, err, worlds.ErrRunTerminal, "append after completion")
	_, err = world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeRunCancelled}})
	conformExpect(t, err, worlds.ErrRunTerminal, "cancel completed run")

	failed := conformCreateRun(t, world, "failed")
	conformAppend(t, world, failed, worlds.EventTypeRunFailed, "", worlds.DataRunFailed{Error: json.RawMessage(`{"message":"boom"}`)})
	run, err = world.RunsGet(ctx, worlds.RunsGetInput{RunID: failed})
	if err != nil || run.Status != worlds.StatusFailed || string(run.Error) != `{"message":"boom"}` || run.CompletedAt == nil {
		t.Fatalf("failed run = %#v err=%v", run, err)
	}

	cancelled := conformCreateRun(t, world, "cancelled")
	cancel := conformAppend(t, world, cancelled, worlds.EventTypeRunCancelled, "", nil)
	again, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: cancelled, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeRunCancelled}})
	if err != nil || again.EventID != cancel.EventID || again.AllocatedEventCount != 0 {
		t.Fatalf("run_cancelled replay = %#v err=%v", again, err)
	}
	if events := conformAllEvents(t, world, cancelled); len(events) != 2 {
		t.Fatalf("cancelled run events = %d, want 2", len(events))
	}
}

func conformRunsWaitForTerminalStatus(t *testing.T, world worlds.World) {
	ctx := context.Background()
	runID := conformCreateRun(t, world, "wait_terminal")
	run, err := world.RunsWaitForTerminalStatus(ctx, worlds.RunsWaitForTerminalStatusInput{RunID: runID, Timeout: 50 * time.Millisecond})
	if err != nil || run.Status != worlds.StatusPending {
		t.Fatalf("timed out wait = %#v err=%v", run, err)
	}
	go func() {
		time.Sleep(100 * time.Millisecond)
		_, _ = world.EventsCreate(context.Background(), worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeRunCompleted, Data: []byte(`{"output":"ImRvbmUi"}`)}})
	}()
	run, err = world.RunsWaitForTerminalStatus(ctx, worlds.RunsWaitForTerminalStatusInput{RunID: runID, Timeout: 10 * time.Second})
	if err != nil || run.Status != worlds.StatusCompleted || string(run.Output) != `"done"` {
		t.Fatalf("terminal wait = %#v err=%v", run, err)
	}
	_, err = world.RunsWaitForTerminalStatus(ctx, worlds.RunsWaitForTerminalStatusInput{RunID: "wrun_missing", Timeout: time.Second})
	conformExpect(t, err, worlds.ErrRunNotFound, "wait for missing run")
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := world.RunsWaitForTerminalStatus(cancelled, worlds.RunsWaitForTerminalStatusInput{RunID: conformCreateRun(t, world, "wait_cancel"), Timeout: time.Second}); err == nil {
		t.Fatal("wait with a cancelled context succeeded")
	}
}

func conformRunsList(t *testing.T, world worlds.World) {
	ctx := context.Background()
	name := fmt.Sprintf("list_%d", time.Now().UnixNano())
	ids := make([]worlds.RunID, 3)
	for i := range ids {
		ids[i] = conformRunID("list")
		if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: ids[i], WorkflowName: name, Input: []byte(`"in"`)}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Millisecond)
	}
	conformAppend(t, world, ids[1], worlds.EventTypeRunStarted, "", nil)

	all, err := world.RunsList(ctx, worlds.RunsListInput{WorkflowName: name})
	if err != nil || len(all.Data) != 3 || all.Data[0].RunID != ids[0] || all.Data[2].RunID != ids[2] {
		t.Fatalf("ascending runs = %#v err=%v", all, err)
	}
	desc, err := world.RunsList(ctx, worlds.RunsListInput{WorkflowName: name, Pagination: worlds.Pagination{SortOrder: worlds.SortOrderDescending}})
	if err != nil || len(desc.Data) != 3 || desc.Data[0].RunID != ids[2] {
		t.Fatalf("descending runs = %#v err=%v", desc, err)
	}
	running, err := world.RunsList(ctx, worlds.RunsListInput{WorkflowName: name, Status: []worlds.Status{worlds.StatusRunning}})
	if err != nil || len(running.Data) != 1 || running.Data[0].RunID != ids[1] {
		t.Fatalf("running runs = %#v err=%v", running, err)
	}
	var seen []worlds.RunID
	cursor := ""
	for {
		page, err := world.RunsList(ctx, worlds.RunsListInput{WorkflowName: name, Pagination: worlds.Pagination{Limit: 2, Cursor: cursor}, ResolveData: worlds.ResolveDataNone})
		if err != nil {
			t.Fatal(err)
		}
		for _, run := range page.Data {
			if run.Input != nil {
				t.Fatalf("ResolveDataNone run input = %q", run.Input)
			}
			seen = append(seen, run.RunID)
		}
		if !page.HasMore {
			break
		}
		if page.Cursor == "" {
			t.Fatal("HasMore page without cursor")
		}
		cursor = page.Cursor
	}
	if len(seen) != 3 || seen[0] != ids[0] || seen[1] != ids[1] || seen[2] != ids[2] {
		t.Fatalf("paged runs = %v", seen)
	}
}

func conformAttributes(t *testing.T, world worlds.World) {
	ctx := context.Background()
	runID := conformCreateRun(t, world, "attrs")
	zone, tier := "eu", "gold"
	if err := world.RunsSetAttributes(ctx, worlds.RunsSetAttributesInput{RunID: runID, Changes: []worlds.RunsAttributeChange{{Key: "zone", Value: &zone}, {Key: "tier", Value: &tier}}}); err != nil {
		t.Fatal(err)
	}
	if err := world.RunsSetAttributes(ctx, worlds.RunsSetAttributesInput{RunID: runID, Changes: []worlds.RunsAttributeChange{{Key: "tier"}}}); err != nil {
		t.Fatal(err)
	}
	run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID})
	if err != nil {
		t.Fatal(err)
	}
	if world.Capabilities().SetAttributes {
		if run.Attributes["zone"] != "eu" || len(run.Attributes) != 1 {
			t.Fatalf("attributes = %#v", run.Attributes)
		}
	}
	conformAppend(t, world, runID, worlds.EventTypeRunCancelled, "", nil)
	if world.Capabilities().SetAttributes {
		err = world.RunsSetAttributes(ctx, worlds.RunsSetAttributesInput{RunID: runID, Changes: []worlds.RunsAttributeChange{{Key: "zone", Value: &tier}}})
		conformExpect(t, err, worlds.ErrRunTerminal, "attributes on terminal run")
	}
}

func conformDeleteData(t *testing.T, world worlds.World) {
	ctx := context.Background()
	if !world.Capabilities().DeleteData {
		_, err := world.RunsDeleteDataMany(ctx, worlds.RunsDeleteDataManyInput{RunIDs: []worlds.RunID{conformRunID("unsupported")}})
		conformExpect(t, err, errors.ErrUnsupported, "RunsDeleteDataMany without DeleteData")
		return
	}

	// A run created with $retention "0" loses its data as it finishes.
	purged := conformRunID("retain_none")
	attributes := map[string]string{worlds.RetentionAttribute: worlds.RetainNone.String(), "tier": "gold"}
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: purged, WorkflowName: "conformance", Input: []byte(`"input"`), Attributes: attributes}); err != nil {
		t.Fatal(err)
	}
	token := fmt.Sprintf("conformance-delete-%s", purged)
	conformAppend(t, world, purged, worlds.EventTypeRunStarted, "", nil)
	conformAppend(t, world, purged, worlds.EventTypeStepCreated, "step_a", worlds.DataStepCreated{StepName: "work", Input: []byte(`"step-input"`)})
	conformAppend(t, world, purged, worlds.EventTypeStepStarted, "step_a", nil)
	conformAppend(t, world, purged, worlds.EventTypeStepCompleted, "step_a", worlds.DataStepResult{Result: []byte(`"step-output"`)})
	conformAppend(t, world, purged, worlds.EventTypeStepCreated, "step_b", worlds.DataStepCreated{StepName: "late", Input: []byte(`"late-input"`)})
	conformAppend(t, world, purged, worlds.EventTypeStepStarted, "step_b", nil)
	conformAppend(t, world, purged, worlds.EventTypeHookCreated, "hook_a", worlds.DataHookCreated{Token: token, Metadata: []byte(`"metadata"`)})
	if err := world.StreamsWrite(ctx, worlds.StreamsWriteInput{RunID: purged, Name: "out", Chunk: []byte("chunk")}); err != nil {
		t.Fatal(err)
	}
	conformAppend(t, world, purged, worlds.EventTypeRunCompleted, "", worlds.DataRunCompleted{Output: []byte(`"output"`)})
	// A step still running when the run finished settles after it, and what
	// it brought is deleted too.
	conformAppend(t, world, purged, worlds.EventTypeStepCompleted, "step_b", worlds.DataStepResult{Result: []byte(`"late-output"`)})
	conformExpectDataDeleted(t, world, purged)
	if hook, err := world.HooksGet(ctx, worlds.HooksGetInput{HookID: "hook_a"}); err == nil && (hook.Metadata != nil || hook.Token != token) {
		t.Fatalf("deleted run's hook = %+v", hook)
	}
	err := world.StreamsWrite(ctx, worlds.StreamsWriteInput{RunID: purged, Name: "out", Chunk: []byte("more")})
	conformExpect(t, err, worlds.ErrStreamClosed, "write to a deleted run's stream")
	err = world.StreamsWrite(ctx, worlds.StreamsWriteInput{RunID: purged, Name: "new", Chunk: []byte("more")})
	conformExpect(t, err, worlds.ErrStreamClosed, "new stream of a deleted run")

	// A retention the SDK does not support keeps the data, until
	// RunsDeleteDataMany deletes it.
	kept := conformRunID("retain_kept")
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: kept, WorkflowName: "conformance", Input: []byte(`"input"`), Attributes: map[string]string{worlds.RetentionAttribute: "86400"}}); err != nil {
		t.Fatal(err)
	}
	conformAppend(t, world, kept, worlds.EventTypeRunStarted, "", nil)
	conformAppend(t, world, kept, worlds.EventTypeStepCreated, "step_a", worlds.DataStepCreated{StepName: "work", Input: []byte(`"step-input"`)})
	conformAppend(t, world, kept, worlds.EventTypeRunCompleted, "", worlds.DataRunCompleted{Output: []byte(`"output"`)})
	run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: kept})
	if err != nil || run.ExpiredAt != nil || string(run.Output) != `"output"` {
		t.Fatalf("kept run = %+v, err=%v", run, err)
	}
	running := conformCreateRun(t, world, "retain_running")
	conformAppend(t, world, running, worlds.EventTypeRunStarted, "", nil)
	missing := conformRunID("retain_missing")

	result, err := world.RunsDeleteDataMany(ctx, worlds.RunsDeleteDataManyInput{RunIDs: []worlds.RunID{kept, purged, running, missing}})
	if err != nil {
		t.Fatal(err)
	}
	want := []worlds.DeleteDataResult{
		{RunID: kept, Outcome: worlds.DeleteDataDeleted},
		{RunID: purged, Outcome: worlds.DeleteDataAlreadyDeleted},
		{RunID: running, Outcome: worlds.DeleteDataSkipped, Reason: worlds.DeleteDataRunNotTerminal, Status: worlds.StatusRunning},
		{RunID: missing, Outcome: worlds.DeleteDataNotFound},
	}
	wantSummary := worlds.DeleteDataSummary{Requested: 4, Deleted: 1, AlreadyDeleted: 1, Skipped: 1, NotFound: 1}
	if len(result.Results) != len(want) || result.Summary != wantSummary {
		t.Fatalf("RunsDeleteDataMany = %+v", result)
	}
	for index := range want {
		if result.Results[index] != want[index] {
			t.Fatalf("result %d = %+v, want %+v", index, result.Results[index], want[index])
		}
	}
	conformExpectDataDeleted(t, world, kept)
	if run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: running}); err != nil || run.ExpiredAt != nil {
		t.Fatalf("running run = %+v, err=%v", run, err)
	}

	_, err = world.RunsDeleteDataMany(ctx, worlds.RunsDeleteDataManyInput{})
	conformExpect(t, err, worlds.ErrDeleteDataInvalid, "RunsDeleteDataMany without run IDs")
	_, err = world.RunsDeleteDataMany(ctx, worlds.RunsDeleteDataManyInput{RunIDs: []worlds.RunID{kept, kept}})
	conformExpect(t, err, worlds.ErrDeleteDataInvalid, "RunsDeleteDataMany with a repeated run ID")
}

// conformExpectDataDeleted checks that a run's data is gone and its records
// stay: the run with its attributes, its steps with their names, its events
// with their metadata, and its streams, empty and closed.
func conformExpectDataDeleted(t *testing.T, world worlds.World, runID worlds.RunID) {
	t.Helper()
	ctx := context.Background()
	run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID})
	if err != nil || run.ExpiredAt == nil || run.Input != nil || run.Output != nil || run.Error != nil || !run.Status.Terminal() {
		t.Fatalf("deleted run = %+v, err=%v", run, err)
	}
	if !worlds.RunDataExpired(run, time.Now()) {
		t.Fatalf("deleted run expires at %v, after now", run.ExpiredAt)
	}
	if run.Attributes[worlds.RetentionAttribute] == worlds.RetainNone.String() && run.Attributes["tier"] != "gold" {
		t.Fatalf("deleted run attributes = %#v", run.Attributes)
	}
	steps, err := world.StepsList(ctx, worlds.StepsListInput{RunID: runID})
	if err != nil || len(steps.Data) == 0 {
		t.Fatalf("deleted run steps = %+v, err=%v", steps, err)
	}
	for _, step := range steps.Data {
		if step.StepName == "" || step.Input != nil || step.Output != nil || step.Error != nil {
			t.Fatalf("deleted run step = %+v", step)
		}
	}
	for _, event := range conformAllEvents(t, world, runID) {
		if payload := worlds.EventPayload(event); payload != nil {
			t.Fatalf("deleted run %s event payload = %s", event.EventType, payload)
		}
		if event.EventType == worlds.EventTypeStepCreated {
			var created worlds.DataStepCreated
			if json.Unmarshal(event.Data, &created) != nil || created.StepName == "" {
				t.Fatalf("deleted run step_created data = %s", event.Data)
			}
		}
	}
	names, err := world.StreamsList(ctx, worlds.StreamsListInput{RunID: runID})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		chunks, err := world.StreamsGetChunks(ctx, worlds.StreamsGetChunksInput{RunID: runID, Name: name})
		if err != nil || len(chunks.Data) != 0 || !chunks.Done {
			t.Fatalf("deleted run stream %q = %+v, err=%v", name, chunks, err)
		}
	}
}
