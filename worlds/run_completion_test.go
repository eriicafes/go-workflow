package worlds_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/testworld"
)

func TestNotifyRunEndResumesTheParentsCompletionHook(t *testing.T) {
	ctx := context.Background()
	world := testworld.NewManual(nil)
	defer world.Close()
	parent, child := worlds.NewRunID(), worlds.NewRunID()
	for _, runID := range []worlds.RunID{parent, child} {
		if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "test"}); err != nil {
			t.Fatal(err)
		}
	}
	notifies := map[string]any{worlds.ExecutionContextCompletionParentRunID: string(parent)}

	// Without a hook, or without a parent, nothing is written.
	worlds.NotifyRunEnd(ctx, world, child, notifies, worlds.StatusCompleted)
	worlds.NotifyRunEnd(ctx, world, child, map[string]any{}, worlds.StatusCompleted)
	if received := hookPayloads(t, ctx, world, parent); len(received) != 0 {
		t.Fatalf("payloads = %v, want none", received)
	}

	created, _ := json.Marshal(worlds.DataHookCreated{Token: worlds.ChildRunCompletionToken(parent), IsSystem: true})
	if _, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: parent, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeHookCreated, CorrelationID: "hook_completion", Data: created}}); err != nil {
		t.Fatal(err)
	}
	worlds.NotifyRunEnd(ctx, world, child, notifies, worlds.StatusFailed)
	received := hookPayloads(t, ctx, world, parent)
	if len(received) != 1 || received[0] != (worlds.ChildRunCompletion{RunID: child, Status: worlds.StatusFailed}) {
		t.Fatalf("payloads = %v, want the child's completion", received)
	}
	if queued := world.QueuedMessages(); len(queued) == 0 || queued[len(queued)-1].Input.RunID != parent {
		t.Fatalf("queued = %#v, want a delivery of the parent", queued)
	}
}

// hookPayloads returns the completion payloads the run's hooks received.
func hookPayloads(t *testing.T, ctx context.Context, world worlds.World, runID worlds.RunID) []worlds.ChildRunCompletion {
	t.Helper()
	events, err := world.EventsList(ctx, worlds.EventsListInput{RunID: runID})
	if err != nil {
		t.Fatal(err)
	}
	var payloads []worlds.ChildRunCompletion
	for _, event := range events.Data {
		if event.EventType != worlds.EventTypeHookReceived {
			continue
		}
		var payload worlds.ChildRunCompletion
		if err := worlds.DecodePayload(worlds.DecodeHookReceived(event.Data), nil, &payload); err != nil {
			t.Fatal(err)
		}
		payloads = append(payloads, payload)
	}
	return payloads
}
