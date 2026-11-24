package core

import (
	"encoding/json"
	"testing"

	"github.com/eriicafes/go-workflow/worlds"
)

func TestRunAheadHazardClassifiesEventsBelowAWriteAhead(t *testing.T) {
	system, _ := json.Marshal(worlds.DataHookCreated{Token: "t", IsSystem: true})
	completion, _ := json.Marshal(worlds.DataHookCreated{Token: worlds.ChildRunCompletionToken("wrun_parent"), IsSystem: true})
	e := &Execution{
		events: []worlds.Event{
			{EventType: worlds.EventTypeHookCreated, CorrelationID: "hook_system", Data: system},
			{EventType: worlds.EventTypeHookCreated, CorrelationID: "hook_completion", Data: completion},
		},
		selfSteps: map[worlds.StepID]bool{"step_own": true},
	}
	for _, test := range []struct {
		event  worlds.Event
		hazard bool
	}{
		{worlds.Event{EventType: worlds.EventTypeHookReceived, CorrelationID: "hook_idle"}, false},
		{worlds.Event{EventType: worlds.EventTypeHookReceived, CorrelationID: "hook_observed"}, true},
		{worlds.Event{EventType: worlds.EventTypeHookDisposed, CorrelationID: "hook_observed"}, true},
		{worlds.Event{EventType: worlds.EventTypeHookReceived, CorrelationID: "hook_system"}, true},
		// A child-run completion hook is a hazard only while the runner
		// waits for its payloads, as a user hook is.
		{worlds.Event{EventType: worlds.EventTypeHookReceived, CorrelationID: "hook_completion"}, false},
		{worlds.Event{EventType: worlds.EventTypeStepCompleted, CorrelationID: "step_other"}, true},
		{worlds.Event{EventType: worlds.EventTypeStepStarted, CorrelationID: "step_other"}, true},
		{worlds.Event{EventType: worlds.EventTypeStepCompleted, CorrelationID: "step_own"}, false},
		{worlds.Event{EventType: worlds.EventTypeWaitCompleted, CorrelationID: "wait_a"}, true},
		{worlds.Event{EventType: worlds.EventTypeRunCancelled}, false},
		{worlds.Event{EventType: worlds.EventTypeAttrSet}, false},
	} {
		write := &aheadWrite{observed: map[worlds.HookID]bool{"hook_observed": true}}
		if got := e.runAheadHazard(test.event, write) != ""; got != test.hazard {
			t.Errorf("hazard(%s %s) = %v, want %v", test.event.EventType, test.event.CorrelationID, got, test.hazard)
		}
	}
}
