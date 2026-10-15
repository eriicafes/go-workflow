package workflowtest

import (
	"context"
	"errors"
	"testing"

	"github.com/eriicafes/go-workflow/worlds"
)

func conformEventDeltas(t *testing.T, world worlds.World) {
	ctx := context.Background()
	runID := conformCreateRun(t, world, "delta")
	zero := 0
	started, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeRunStarted}, Options: worlds.EventsCreateOptions{EventCount: &zero}})
	if err != nil {
		t.Fatal(err)
	}
	if started.EventID != worlds.EventIDForSlot(2) || started.AllocatedEventCount != 1 || len(started.Events) != 1 || started.Events[0].EventType != worlds.EventTypeRunCreated {
		t.Fatalf("started = %#v", started)
	}
	plain := conformAppend(t, world, runID, worlds.EventTypeWaitCreated, "wait_a", nil)
	if plain.EventID != worlds.EventIDForSlot(3) || len(plain.Events) != 0 {
		t.Fatalf("delta without options = %#v", plain)
	}
	since, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeWaitCreated, CorrelationID: "wait_b"}, Options: worlds.EventsCreateOptions{SinceCursor: string(worlds.EventIDForSlot(1))}})
	if err != nil {
		t.Fatal(err)
	}
	if len(since.Events) != 2 || since.Events[0].EventID != worlds.EventIDForSlot(2) || since.Events[1].EventID != worlds.EventIDForSlot(3) {
		t.Fatalf("SinceCursor delta = %#v", since.Events)
	}
	two := 2
	none, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeWaitCreated, CorrelationID: "wait_c", Data: []byte(`{"x":1}`)}, Options: worlds.EventsCreateOptions{EventCount: &two, ResolveData: worlds.EventsResolveDataNone}})
	if err != nil {
		t.Fatal(err)
	}
	if len(none.Events) != 2 || none.Events[0].EventID != worlds.EventIDForSlot(3) || none.Events[0].Data != nil {
		t.Fatalf("EventCount delta with ResolveDataNone = %#v", none.Events)
	}
	large := 100
	beyond, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeWaitCreated, CorrelationID: "wait_d"}, Options: worlds.EventsCreateOptions{EventCount: &large}})
	if err != nil || len(beyond.Events) != 0 {
		t.Fatalf("EventCount beyond log = %#v err=%v", beyond, err)
	}

	_, err = world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeRunCreated}})
	conformExpect(t, err, worlds.ErrEventInvalid, "run_created append")
	_, err = world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: "bogus"}})
	conformExpect(t, err, worlds.ErrEventInvalid, "unknown event type")
	_, err = world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: "wrun_missing", Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeRunStarted}})
	conformExpect(t, err, worlds.ErrRunNotFound, "missing run")
}

func conformBatch(t *testing.T, world worlds.World) {
	ctx := context.Background()
	runID := conformCreateRun(t, world, "batch")
	empty, err := world.EventsCreateBatch(ctx, worlds.EventsCreateBatchInput{RunID: runID})
	if err != nil || len(empty.Events) != 0 {
		t.Fatalf("empty batch = %#v err=%v", empty, err)
	}
	result, err := world.EventsCreateBatch(ctx, worlds.EventsCreateBatchInput{RunID: runID, Events: []worlds.EventsCreateEvent{
		{EventType: worlds.EventTypeRunStarted},
		conformEvent(t, worlds.EventTypeStepCreated, "step_a", map[string]any{"stepName": "a"}),
		{EventType: worlds.EventTypeStepStarted, CorrelationID: "step_a"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Events) != 3 || result.Events[0].EventID != worlds.EventIDForSlot(2) || result.Events[2].EventID != worlds.EventIDForSlot(4) || result.Events[2].EventType != worlds.EventTypeStepStarted {
		t.Fatalf("batch result = %#v", result)
	}
	for _, item := range result.Events {
		if item.AllocatedEventCount != 1 {
			t.Fatalf("batch item AllocatedEventCount = %d", item.AllocatedEventCount)
		}
	}
	if result.Events[1].Step == nil || !result.Events[1].StepCreated {
		t.Fatalf("batch step_created result = %#v", result.Events[1])
	}
	step, err := world.StepsGet(ctx, worlds.StepsGetInput{RunID: runID, StepID: "step_a"})
	if err != nil || step.Status != worlds.StatusRunning {
		t.Fatalf("batch step = %#v err=%v", step, err)
	}

	_, err = world.EventsCreateBatch(ctx, worlds.EventsCreateBatchInput{RunID: runID, Events: []worlds.EventsCreateEvent{
		conformEvent(t, worlds.EventTypeStepCreated, "step_b", map[string]any{"stepName": "b"}),
		{EventType: worlds.EventTypeStepStarted, CorrelationID: "step_missing"},
	}})
	conformExpect(t, err, worlds.ErrStepNotStartable, "batch with rejected event")
	if _, err := world.StepsGet(ctx, worlds.StepsGetInput{RunID: runID, StepID: "step_b"}); !errors.Is(err, worlds.ErrStepNotFound) {
		t.Fatalf("rejected batch leaked step_b: %v", err)
	}
	if events := conformAllEvents(t, world, runID); len(events) != 4 {
		t.Fatalf("events after rejected batch = %d, want 4", len(events))
	}
	_, err = world.EventsCreateBatch(ctx, worlds.EventsCreateBatchInput{RunID: runID, Events: []worlds.EventsCreateEvent{{EventType: worlds.EventTypeWaitCreated, CorrelationID: "w"}, {EventType: worlds.EventTypeRunCreated}}})
	conformExpect(t, err, worlds.ErrEventInvalid, "batch with run_created")
	_, err = world.EventsCreateBatch(ctx, worlds.EventsCreateBatchInput{RunID: runID, Events: []worlds.EventsCreateEvent{{EventType: worlds.EventTypeWaitCreated, CorrelationID: "w"}, {EventType: worlds.EventTypeRunStarted}}})
	conformExpect(t, err, worlds.ErrEventInvalid, "batch with replayed event")
	if events := conformAllEvents(t, world, runID); len(events) != 4 {
		t.Fatalf("events after invalid batches = %d, want 4", len(events))
	}
	_, err = world.EventsCreateBatch(ctx, worlds.EventsCreateBatchInput{RunID: "wrun_missing", Events: []worlds.EventsCreateEvent{{EventType: worlds.EventTypeRunStarted}}})
	conformExpect(t, err, worlds.ErrRunNotFound, "batch for missing run")
	completed, err := world.EventsCreateBatch(ctx, worlds.EventsCreateBatchInput{RunID: runID, Events: []worlds.EventsCreateEvent{
		{EventType: worlds.EventTypeStepCompleted, CorrelationID: "step_a"},
		{EventType: worlds.EventTypeRunCompleted, Data: []byte("out")},
		{EventType: worlds.EventTypeWaitCreated, CorrelationID: "late"},
	}})
	conformExpect(t, err, worlds.ErrRunTerminal, "batch appending after its own terminal event")
	if completed.Events != nil {
		t.Fatalf("failed batch returned results: %#v", completed)
	}
	run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID})
	if err != nil || run.Status != worlds.StatusRunning {
		t.Fatalf("run after rejected terminal batch = %#v err=%v", run, err)
	}
}

func conformEventsList(t *testing.T, world worlds.World) {
	ctx := context.Background()
	runID := conformCreateRun(t, world, "events")
	conformAppend(t, world, runID, worlds.EventTypeRunStarted, "", nil)
	conformAppend(t, world, runID, worlds.EventTypeWaitCreated, "wait_a", []byte(`{"dueAt":"2030-01-01T00:00:00Z"}`))
	conformAppend(t, world, runID, worlds.EventTypeWaitCreated, "wait_b", nil)
	conformAppend(t, world, runID, worlds.EventTypeWaitCompleted, "wait_a", nil)

	var ascending []worlds.EventID
	cursor := ""
	for {
		page, err := world.EventsList(ctx, worlds.EventsListInput{RunID: runID, Pagination: worlds.Pagination{Limit: 2, Cursor: cursor}})
		if err != nil {
			t.Fatal(err)
		}
		for _, event := range page.Data {
			ascending = append(ascending, event.EventID)
		}
		if !page.HasMore {
			break
		}
		cursor = page.Cursor
	}
	if len(ascending) != 5 {
		t.Fatalf("ascending events = %v", ascending)
	}
	for i, id := range ascending {
		if id != worlds.EventIDForSlot(uint64(i+1)) {
			t.Fatalf("ascending events = %v", ascending)
		}
	}
	var descending []worlds.EventID
	cursor = ""
	for {
		page, err := world.EventsList(ctx, worlds.EventsListInput{RunID: runID, Pagination: worlds.Pagination{Limit: 2, Cursor: cursor, SortOrder: worlds.SortOrderDescending}})
		if err != nil {
			t.Fatal(err)
		}
		for _, event := range page.Data {
			descending = append(descending, event.EventID)
		}
		if !page.HasMore {
			break
		}
		cursor = page.Cursor
	}
	if len(descending) != 5 || descending[0] != worlds.EventIDForSlot(5) || descending[4] != worlds.EventIDForSlot(1) {
		t.Fatalf("descending events = %v", descending)
	}

	event, err := world.EventsGet(ctx, worlds.EventsGetInput{RunID: runID, EventID: worlds.EventIDForSlot(3)})
	if err != nil || event.EventType != worlds.EventTypeWaitCreated || event.CorrelationID != "wait_a" || len(event.Data) == 0 {
		t.Fatalf("EventsGet = %#v err=%v", event, err)
	}
	event, err = world.EventsGet(ctx, worlds.EventsGetInput{RunID: runID, EventID: worlds.EventIDForSlot(3), ResolveData: worlds.EventsResolveDataNone})
	if err != nil || event.Data != nil {
		t.Fatalf("EventsGet ResolveDataNone = %#v err=%v", event, err)
	}
	_, err = world.EventsGet(ctx, worlds.EventsGetInput{RunID: runID, EventID: worlds.EventIDForSlot(99)})
	conformExpect(t, err, worlds.ErrEventNotFound, "missing event")
	_, err = world.EventsGet(ctx, worlds.EventsGetInput{RunID: "wrun_missing", EventID: worlds.EventIDForSlot(1)})
	conformExpect(t, err, worlds.ErrRunNotFound, "event of missing run")
	_, err = world.EventsList(ctx, worlds.EventsListInput{RunID: "wrun_missing"})
	conformExpect(t, err, worlds.ErrRunNotFound, "events of missing run")

	stripped, err := world.EventsList(ctx, worlds.EventsListInput{RunID: runID, ResolveData: worlds.EventsResolveDataNone})
	if err != nil || len(stripped.Data) != 5 || stripped.Data[2].Data != nil {
		t.Fatalf("EventsList ResolveDataNone = %#v err=%v", stripped, err)
	}
	correlated, err := world.EventsListByCorrelationID(ctx, worlds.EventsListByCorrelationIDInput{RunID: runID, CorrelationID: "wait_a"})
	if err != nil || len(correlated.Data) != 2 || correlated.Data[0].EventID != worlds.EventIDForSlot(3) || correlated.Data[1].EventID != worlds.EventIDForSlot(5) {
		t.Fatalf("EventsListByCorrelationID = %#v err=%v", correlated, err)
	}
	firstCorrelated, err := world.EventsListByCorrelationID(ctx, worlds.EventsListByCorrelationIDInput{RunID: runID, CorrelationID: "wait_a", Pagination: worlds.Pagination{Limit: 1}})
	if err != nil || len(firstCorrelated.Data) != 1 || !firstCorrelated.HasMore {
		t.Fatalf("first correlated page = %#v err=%v", firstCorrelated, err)
	}
	nextCorrelated, err := world.EventsListByCorrelationID(ctx, worlds.EventsListByCorrelationIDInput{RunID: runID, CorrelationID: "wait_a", Pagination: worlds.Pagination{Limit: 1, Cursor: firstCorrelated.Cursor}})
	if err != nil || len(nextCorrelated.Data) != 1 || nextCorrelated.Data[0].EventID != worlds.EventIDForSlot(5) || nextCorrelated.HasMore {
		t.Fatalf("second correlated page = %#v err=%v", nextCorrelated, err)
	}
}

func conformWaits(t *testing.T, world worlds.World) {
	ctx := context.Background()
	runID := conformCreateRun(t, world, "waits")
	completed := conformAppend(t, world, runID, worlds.EventTypeWaitCompleted, "wait_a", nil)
	replay, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeWaitCompleted, CorrelationID: "wait_a"}})
	if err != nil || replay.EventID != completed.EventID || replay.AllocatedEventCount != 0 {
		t.Fatalf("wait_completed replay = %#v err=%v", replay, err)
	}
	if _, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeWaitCompleted, CorrelationID: "wait_b"}}); err != nil {
		t.Fatal(err)
	}
	if events := conformAllEvents(t, world, runID); len(events) != 3 {
		t.Fatalf("events = %d, want 3", len(events))
	}
}
