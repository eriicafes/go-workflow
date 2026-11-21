package testworld_test

import (
	"context"
	"errors"
	"testing"

	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/testworld"
)

func TestSinceCursorPastTheLogDoesNotPanic(t *testing.T) {
	ctx := context.Background()
	world := testworld.NewManual()
	defer world.Close()
	runID := runCursors
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
	runID := runCorrelation
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

func TestRolledBackBatchKeepsAPurgedRunsStreams(t *testing.T) {
	ctx := context.Background()
	world := testworld.NewManual()
	defer world.Close()
	runID := runRollback
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "wf", Attributes: map[string]string{worlds.RetentionAttribute: worlds.RetainNone.String()}, AllowReservedAttributes: true}); err != nil {
		t.Fatal(err)
	}
	if err := world.StreamsWrite(ctx, worlds.StreamsWriteInput{RunID: runID, Name: "out", Chunk: []byte("chunk")}); err != nil {
		t.Fatal(err)
	}
	// The batch finishes the run, which purges its data, then fails.
	_, err := world.EventsCreateBatch(ctx, worlds.EventsCreateBatchInput{RunID: runID, Events: []worlds.EventsCreateEvent{{EventType: worlds.EventTypeRunCancelled}, {EventType: worlds.EventTypeRunCreated}}})
	if !errors.Is(err, worlds.ErrEventInvalid) {
		t.Fatalf("batch error = %v", err)
	}
	chunks, err := world.StreamsGetChunks(ctx, worlds.StreamsGetChunksInput{RunID: runID, Name: "out"})
	if err != nil || len(chunks.Data) != 1 || chunks.Done {
		t.Fatalf("stream after a rolled-back batch = %+v, %v", chunks, err)
	}
}

// runCorrelation is a run ID minted when the tests start, as a World requires
// of a new run.
var runCorrelation = worlds.NewRunID()

// runCursors is a run ID minted when the tests start, as a World requires
// of a new run.
var runCursors = worlds.NewRunID()

// runRollback is a run ID minted when the tests start, as a World requires
// of a new run.
var runRollback = worlds.NewRunID()
