package testworld_test

import (
	"context"
	"testing"

	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/testworld"
)

func TestSinceCursorPastTheLogDoesNotPanic(t *testing.T) {
	ctx := context.Background()
	world := testworld.NewManual()
	defer world.Close()
	runID := worlds.RunID("wrun_cursors")
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "w"}); err != nil {
		t.Fatal(err)
	}
	if _, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeRunStarted}, Options: worlds.EventsCreateOptions{SinceCursor: string(worlds.EventIDForSlot(50))}}); err != nil {
		t.Fatal(err)
	}
}

func TestEventsByCorrelationIDHonourDescendingOrder(t *testing.T) {
	ctx := context.Background()
	world := testworld.NewManual()
	defer world.Close()
	runID := worlds.RunID("wrun_correlation")
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "w"}); err != nil {
		t.Fatal(err)
	}
	for _, event := range []worlds.EventsCreateEvent{
		{EventType: worlds.EventTypeRunStarted},
		{EventType: worlds.EventTypeStepCreated, CorrelationID: "step_a", Data: []byte(`{}`)},
		{EventType: worlds.EventTypeStepStarted, CorrelationID: "step_a"},
		{EventType: worlds.EventTypeStepCompleted, CorrelationID: "step_a"},
	} {
		if _, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: event}); err != nil {
			t.Fatal(err)
		}
	}
	input := worlds.EventsListByCorrelationIDInput{RunID: runID, CorrelationID: "step_a", Pagination: worlds.Pagination{Limit: 2, SortOrder: worlds.SortOrderDescending}}
	first, err := world.EventsListByCorrelationID(ctx, input)
	if err != nil || len(first.Data) != 2 || first.Data[0].EventType != worlds.EventTypeStepCompleted || !first.HasMore {
		t.Fatalf("first page = %#v err=%v", first, err)
	}
	input.Pagination.Cursor = first.Cursor
	next, err := world.EventsListByCorrelationID(ctx, input)
	if err != nil || len(next.Data) != 1 || next.Data[0].EventType != worlds.EventTypeStepCreated || next.HasMore {
		t.Fatalf("second page = %#v err=%v", next, err)
	}
}
