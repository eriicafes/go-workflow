// Package workflowtest provides conformance tests for worlds.World
// implementations. A World package runs Conformance from its own tests, and
// ConformanceShared when several instances can share one backend.
package workflowtest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// Conformance runs the World contract against Worlds created by newWorld. Each
// subtest receives a fresh World, so implementations backed by shared storage
// must isolate state per call (for example with a unique table, prefix, or
// directory).
func Conformance(t *testing.T, newWorld func(t *testing.T) worlds.World) {
	t.Helper()
	tests := []struct {
		name string
		run  func(*testing.T, worlds.World)
	}{
		{"Identity", conformIdentity},
		{"RunCreate", conformRunCreate},
		{"EventDeltas", conformEventDeltas},
		{"RunDeployment", conformRunDeployment},
		{"InBandFence", conformInBandFence},
		{"RunLifecycle", conformRunLifecycle},
		{"RunsWaitForTerminalStatus", conformRunsWaitForTerminalStatus},
		{"RunsList", conformRunsList},
		{"Steps", conformSteps},
		{"TerminalSteps", conformTerminalSteps},
		{"Batch", conformBatch},
		{"EventsList", conformEventsList},
		{"Attributes", conformAttributes},
		{"HookTokens", conformHookTokens},
		{"HooksForce", conformHooksForce},
		{"HooksRetention", conformHooksRetention},
		{"Webhooks", conformWebhooks},
		{"Waits", conformWaits},
		{"Streams", conformStreams},
		{"StreamsDirect", conformStreamsDirect},
		{"StreamsWait", conformStreamsWait},
		{"Queue", conformQueue},
		{"Analytics", conformAnalytics},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.run(t, newWorld(t))
		})
	}
}

// ConformanceShared runs the parts of the World contract that involve several
// instances, which are concurrent appends, hook claims, and stream writers.
// newPair returns two Worlds backed by the same storage, as two processes
// would be.
func ConformanceShared(t *testing.T, newPair func(t *testing.T) (worlds.World, worlds.World)) {
	t.Helper()
	t.Run("ConcurrentAppends", func(t *testing.T) {
		a, b := newPair(t)
		conformConcurrentAppends(t, a, b)
	})
	t.Run("ConcurrentHookClaims", func(t *testing.T) {
		a, b := newPair(t)
		conformConcurrentHookClaims(t, a, b)
	})
	t.Run("SharedStreamSessions", func(t *testing.T) {
		a, b := newPair(t)
		conformSharedStreamSessions(t, a, b)
	})
}

var conformanceRunSequence struct {
	mu    sync.Mutex
	value int
}

func conformRunID(prefix string) worlds.RunID {
	conformanceRunSequence.mu.Lock()
	defer conformanceRunSequence.mu.Unlock()
	conformanceRunSequence.value++
	return worlds.RunID(fmt.Sprintf("wrun_%s_%d_%d", prefix, time.Now().UnixNano(), conformanceRunSequence.value))
}

func conformCreateRun(t *testing.T, world worlds.World, prefix string) worlds.RunID {
	t.Helper()
	runID := conformRunID(prefix)
	if _, err := world.EventsCreateRun(context.Background(), worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "conformance", Input: []byte(`"input"`)}); err != nil {
		t.Fatalf("EventsCreateRun: %v", err)
	}
	return runID
}

func conformAppend(t *testing.T, world worlds.World, runID worlds.RunID, eventType worlds.EventType, correlationID string, data any) worlds.EventResult {
	t.Helper()
	result, err := world.EventsCreate(context.Background(), worlds.EventsCreateInput{RunID: runID, Event: conformEvent(t, eventType, correlationID, data)})
	if err != nil {
		t.Fatalf("EventsCreate(%s %s): %v", eventType, correlationID, err)
	}
	return result
}

// conformReceived returns hook_received Data carrying value as its payload.
// A []byte value is used as the JSON payload as is.
func conformReceived(t *testing.T, value any) worlds.DataHookReceived {
	t.Helper()
	payload, ok := value.([]byte)
	if !ok {
		var err error
		if payload, err = json.Marshal(value); err != nil {
			t.Fatal(err)
		}
	}
	return worlds.DataHookReceived{Payload: payload}
}

func conformEvent(t *testing.T, eventType worlds.EventType, correlationID string, data any) worlds.EventsCreateEvent {
	t.Helper()
	event := worlds.EventsCreateEvent{EventType: eventType, CorrelationID: correlationID}
	switch value := data.(type) {
	case nil:
	case []byte:
		event.Data = value
	default:
		encoded, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		event.Data = encoded
	}
	return event
}

func conformExpect(t *testing.T, err, want error, label string) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatalf("%s: error = %v, want %v", label, err, want)
	}
}

func conformAllEvents(t *testing.T, world worlds.World, runID worlds.RunID) []worlds.Event {
	t.Helper()
	page, err := world.EventsList(context.Background(), worlds.EventsListInput{RunID: runID})
	if err != nil {
		t.Fatalf("EventsList: %v", err)
	}
	return page.Data
}

func conformHookData(token string, webhook *worlds.Webhook, retention time.Duration) worlds.DataHookCreated {
	data := worlds.DataHookCreated{Token: token, Metadata: []byte("meta"), IsWebhook: webhook != nil, Webhook: webhook}
	if retention > 0 {
		until := time.Now().UTC().Add(retention)
		data.TokenRetentionUntil = &until
	}
	return data
}

func conformClaim(world worlds.World, runID worlds.RunID, hookID worlds.HookID, token string, force bool) (worlds.EventResult, error) {
	data, _ := json.Marshal(worlds.DataHookCreated{Token: token, Force: force, Metadata: []byte("meta")})
	return world.EventsCreate(context.Background(), worlds.EventsCreateInput{
		RunID: runID,
		Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeHookCreated, CorrelationID: string(hookID), Data: data},
	})
}

func conformIdentity(t *testing.T, world worlds.World) {
	if world.SpecVersion() != worlds.SpecVersion {
		t.Fatalf("SpecVersion = %d", world.SpecVersion())
	}
	normalized, err := worlds.NormalizeBaseURL(world.BaseURL())
	if err != nil || normalized != world.BaseURL() {
		t.Fatalf("BaseURL %q is not normalized: %v", world.BaseURL(), err)
	}
	if world.Capabilities().Analytics != (world.Analytics() != nil) {
		t.Fatalf("Capabilities().Analytics = %v but Analytics() = %v", world.Capabilities().Analytics, world.Analytics())
	}
}
