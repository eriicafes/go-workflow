package vercelworld

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/eriicafes/go-workflow/worlds"
)

func TestStepStartedNamesTheInstance(t *testing.T) {
	data, _ := json.Marshal(worlds.DataStepStarted{StepName: "step", Attempt: 1})
	meta, _, err := encodeEvent(worlds.EventsCreateEvent{EventType: worlds.EventTypeStepStarted, CorrelationID: "step_1", Data: data})
	if err != nil {
		t.Fatal(err)
	}
	if id, _ := meta["computeInstanceId"].(string); !strings.HasPrefix(id, "cinst_") || id != computeInstanceID() {
		t.Fatalf("step_started instance = %v, want this process's", meta["computeInstanceId"])
	}
	if meta["stepName"] != "step" || meta["attempt"] != 1 {
		t.Fatalf("step_started meta = %v", meta)
	}
}

func TestHookEventsCarryTheirToken(t *testing.T) {
	data, _ := json.Marshal(worlds.HookDisposedEventData{Token: "tok"})
	meta, _, err := encodeEvent(worlds.EventsCreateEvent{EventType: worlds.EventTypeHookDisposed, CorrelationID: "hook_1", Data: data})
	if err != nil || meta["hookToken"] != "tok" {
		t.Fatalf("hook_disposed meta = %v, %v, want its token as hookToken", meta, err)
	}
	// A payload names the token it was sent to the same way.
	received, _ := json.Marshal(worlds.DataHookReceived{Payload: []byte(`"x"`), Token: "tok"})
	meta, _, err = encodeEvent(worlds.EventsCreateEvent{EventType: worlds.EventTypeHookReceived, CorrelationID: "hook_1", Data: received})
	if err != nil || meta["hookToken"] != "tok" {
		t.Fatalf("hook_received meta = %v, %v, want its token as hookToken", meta, err)
	}
	back, err := eventData(wireEvent{EventType: string(worlds.EventTypeHookReceived), Data: map[string]any{"token": "tok", "payload": []byte(`"x"`)}})
	var payload worlds.DataHookReceived
	if err != nil || json.Unmarshal(back, &payload) != nil || payload.Token != "tok" {
		t.Fatalf("stored hook_received = %s, %v", back, err)
	}
	// One written without data names none.
	meta, _, err = encodeEvent(worlds.EventsCreateEvent{EventType: worlds.EventTypeHookDisposed, CorrelationID: "hook_1"})
	if _, named := meta["hookToken"]; err != nil || named {
		t.Fatalf("hook_disposed without data = %v, %v", meta, err)
	}
	// The backend returns the token it stored as the event's.
	stored, err := eventData(wireEvent{EventType: string(worlds.EventTypeHookDisposed), Data: map[string]any{"token": "tok"}})
	var disposed worlds.HookDisposedEventData
	if err != nil || json.Unmarshal(stored, &disposed) != nil || disposed.Token != "tok" {
		t.Fatalf("stored hook_disposed = %s, %v", stored, err)
	}
}

func TestStepResultCarriesItsLatencies(t *testing.T) {
	ttfs, stso := 12.5, 3.0
	data, _ := json.Marshal(worlds.DataStepResult{StepName: "step", Attempt: 1, TTFS: &ttfs, STSO: &stso, StepCount: 2, EventCount: 9, Optimizations: []string{"turbo"}})
	meta, _, err := encodeEvent(worlds.EventsCreateEvent{EventType: worlds.EventTypeStepCompleted, CorrelationID: "step_1", Data: data})
	if err != nil {
		t.Fatal(err)
	}
	if meta["ttfs"] != ttfs || meta["stso"] != stso || meta["stepCount"] != 2 || meta["eventCount"] != 9 || len(meta["optimizations"].([]string)) != 1 {
		t.Fatalf("step_completed meta = %v, want its latencies", meta)
	}
	// They are read back from an event that carries them.
	stored, err := eventData(wireEvent{EventType: string(worlds.EventTypeStepCompleted), Data: map[string]any{"stepName": "step", "ttfs": 12.5, "stso": uint64(3), "stepCount": uint64(2), "eventCount": uint64(9), "optimizations": []any{"turbo"}}})
	var back worlds.DataStepResult
	if err != nil || json.Unmarshal(stored, &back) != nil || back.TTFS == nil || *back.TTFS != ttfs || back.STSO == nil || *back.STSO != stso || back.StepCount != 2 || back.EventCount != 9 || len(back.Optimizations) != 1 || back.RSFS != nil {
		t.Fatalf("stored step_completed = %s, %v", stored, err)
	}
	// One without them names none.
	plain, _ := json.Marshal(worlds.DataStepResult{StepName: "step", Attempt: 1})
	meta, _, _ = encodeEvent(worlds.EventsCreateEvent{EventType: worlds.EventTypeStepCompleted, CorrelationID: "step_1", Data: plain})
	for _, key := range []string{"ttfs", "stso", "rsfs", "stepCount", "eventCount", "finalSchedulingReplay", "optimizations"} {
		if _, set := meta[key]; set {
			t.Errorf("step_completed without latencies has %s", key)
		}
	}
}

func TestRunCancelledCarriesItsReason(t *testing.T) {
	data, err := worlds.EncodeRunCancelled("superseded")
	if err != nil {
		t.Fatal(err)
	}
	meta, _, err := encodeEvent(worlds.EventsCreateEvent{EventType: worlds.EventTypeRunCancelled, Data: data})
	if err != nil || meta["cancelReason"] != "superseded" {
		t.Fatalf("run_cancelled meta = %v, %v, want its reason", meta, err)
	}
	stored, err := eventData(wireEvent{EventType: string(worlds.EventTypeRunCancelled), Data: map[string]any{"cancelReason": "superseded"}})
	if err != nil || worlds.RunCancelReason(stored) != "superseded" {
		t.Fatalf("stored run_cancelled = %s, %v", stored, err)
	}
	// A cancellation without a reason has no data either way.
	meta, _, _ = encodeEvent(worlds.EventsCreateEvent{EventType: worlds.EventTypeRunCancelled})
	stored, _ = eventData(wireEvent{EventType: string(worlds.EventTypeRunCancelled), Data: map[string]any{}})
	if _, set := meta["cancelReason"]; set || stored != nil {
		t.Fatalf("run_cancelled without a reason = %v, %s", meta, stored)
	}
}
