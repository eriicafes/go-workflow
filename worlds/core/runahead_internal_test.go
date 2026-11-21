package core

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

func TestRunAheadHazard(t *testing.T) {
	system, _ := json.Marshal(worlds.DataHookCreated{Token: "t", IsSystem: true})
	e := &Execution{
		events:    []worlds.Event{{EventType: worlds.EventTypeHookCreated, CorrelationID: "hook_system", Data: system}},
		ranInline: map[worlds.StepID]bool{"step_own": true},
	}
	for _, test := range []struct {
		event  worlds.Event
		hazard bool
	}{
		{worlds.Event{EventType: worlds.EventTypeHookReceived, CorrelationID: "hook_idle"}, false},
		{worlds.Event{EventType: worlds.EventTypeHookReceived, CorrelationID: "hook_observed"}, true},
		{worlds.Event{EventType: worlds.EventTypeHookDisposed, CorrelationID: "hook_observed"}, true},
		{worlds.Event{EventType: worlds.EventTypeHookReceived, CorrelationID: "hook_system"}, true},
		{worlds.Event{EventType: worlds.EventTypeStepCompleted, CorrelationID: "step_other"}, true},
		{worlds.Event{EventType: worlds.EventTypeStepStarted, CorrelationID: "step_other"}, true},
		{worlds.Event{EventType: worlds.EventTypeStepCompleted, CorrelationID: "step_own"}, false},
		{worlds.Event{EventType: worlds.EventTypeWaitCompleted, CorrelationID: "wait_a"}, true},
		{worlds.Event{EventType: worlds.EventTypeRunCancelled}, false},
		{worlds.Event{EventType: worlds.EventTypeAttrSet}, false},
	} {
		if got := e.runAheadHazard(test.event, map[worlds.HookID]bool{"hook_observed": true}) != ""; got != test.hazard {
			t.Errorf("hazard(%s %s) = %v, want %v", test.event.EventType, test.event.CorrelationID, got, test.hazard)
		}
	}
}

func TestHopDeadline(t *testing.T) {
	now := time.Date(2030, time.January, 1, 0, 0, 0, 0, time.UTC)
	hour := time.Hour
	for _, test := range []struct {
		name     string
		resumeAt time.Time
		after    *time.Time
		maxDelay time.Duration
		want     time.Time
	}{
		{"no limit", now.Add(100 * hour), nil, 0, now.Add(100 * hour)},
		{"within the limit", now.Add(10 * hour), nil, 23 * hour, now.Add(10 * hour)},
		{"anchored on the deadline", now.Add(50 * hour), nil, 23 * hour, now.Add(50*hour - 46*hour)},
		{"the last hop", now.Add(30 * hour), nil, 23 * hour, now.Add(30*hour - 23*hour)},
		{"after the first hop", now.Add(50 * hour), ptr(now.Add(4 * hour)), 23 * hour, now.Add(27 * hour)},
		{"after the hop before the last", now.Add(50 * hour), ptr(now.Add(27 * hour)), 23 * hour, now.Add(50 * hour)},
	} {
		if got := hopDeadline(test.resumeAt, now, test.after, test.maxDelay); !got.Equal(test.want) {
			t.Errorf("%s: hop at %v, want %v", test.name, got.Sub(now), test.want.Sub(now))
		}
	}
}

func ptr[T any](value T) *T { return &value }

func TestStepMessageKey(t *testing.T) {
	// The FNV-1a hash of the name's UTF-16 code units.
	if got := StepMessageKey("step_1", "a"); got != "step_1:e40c292c" {
		t.Fatalf("key = %q", got)
	}
	if got := StepMessageKey("step_1", ""); got != "step_1:811c9dc5" {
		t.Fatalf("key of an empty name = %q", got)
	}
}
