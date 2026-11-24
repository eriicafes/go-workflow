package workflowtest

import (
	"testing"

	"github.com/eriicafes/go-workflow/worlds"
)

// conformSDKEventData checks events a World takes without Data, a
// step_started and a run_cancelled, and that it keeps a run_cancelled's
// reason.
//
// SDK: packages/world/src/events.test.ts > step_started ownerMessageId > accepts a bare step_started with no eventData (legacy contract)
// SDK: packages/world/src/events.test.ts > run_cancelled cancelReason > retains cancelReason when reading back a stored run_cancelled event (not stripped)
func conformSDKEventData(t *testing.T, world worlds.World) {
	runID := conformCreateRun(t, world, "sdk_bare")
	conformAppend(t, world, runID, worlds.EventTypeRunStarted, "", nil)
	conformAppend(t, world, runID, worlds.EventTypeStepCreated, "step_bare", worlds.DataStepCreated{StepName: "bare", Input: []byte(`"input"`)})
	conformAppend(t, world, runID, worlds.EventTypeStepStarted, "step_bare", nil)
	conformAppend(t, world, runID, worlds.EventTypeRunCancelled, "", nil)

	reasoned := conformCreateRun(t, world, "sdk_cancel_reason")
	data, err := worlds.EncodeRunCancelled("operator cancelled")
	if err != nil {
		t.Fatal(err)
	}
	conformAppend(t, world, reasoned, worlds.EventTypeRunCancelled, "", data)
	events := conformAllEvents(t, world, reasoned)
	if last := events[len(events)-1]; last.EventType != worlds.EventTypeRunCancelled || worlds.RunCancelReason(last.Data) != "operator cancelled" {
		t.Fatalf("run_cancelled = %s %s, want its cancel reason", last.EventType, last.Data)
	}
}
