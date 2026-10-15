package workflowtest

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

func conformRunCreate(t *testing.T, world worlds.World) {
	ctx := context.Background()
	runID := conformRunID("create")
	created, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "conformance", Input: []byte("input"), Attributes: map[string]string{"team": "a"}})
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
	if run.RunID != runID || run.WorkflowName != "conformance" || run.Status != worlds.StatusPending || string(run.Input) != "input" || run.Attributes["team"] != "a" || run.CreatedAt.IsZero() || run.StartedAt != nil {
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
	if err := json.Unmarshal(events[0].Data, &data); err != nil || data.WorkflowName != "conformance" || string(data.Input) != "input" {
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
	conformAppend(t, world, runID, worlds.EventTypeRunCompleted, "", []byte("output"))
	run, err = world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID})
	if err != nil || run.Status != worlds.StatusCompleted || string(run.Output) != "output" || run.CompletedAt == nil {
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
	conformAppend(t, world, failed, worlds.EventTypeRunFailed, "", []byte("boom"))
	run, err = world.RunsGet(ctx, worlds.RunsGetInput{RunID: failed})
	if err != nil || run.Status != worlds.StatusFailed || string(run.Error) != "boom" || run.CompletedAt == nil {
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
		_, _ = world.EventsCreate(context.Background(), worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeRunCompleted, Data: []byte("done")}})
	}()
	run, err = world.RunsWaitForTerminalStatus(ctx, worlds.RunsWaitForTerminalStatusInput{RunID: runID, Timeout: 10 * time.Second})
	if err != nil || run.Status != worlds.StatusCompleted || string(run.Output) != "done" {
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
		if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: ids[i], WorkflowName: name, Input: []byte("in")}); err != nil {
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
