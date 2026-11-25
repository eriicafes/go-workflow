package worlds_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// SDK: packages/world/src/spec-version.test.ts > requiresNewerWorld > accepts runs at or below the current spec version
func TestSDKRunsAtOrBelowTheSDKsCurrentSpecVersionAreSupported(t *testing.T) {
	// 8 is the SDK's current spec version, 4 the one that added attributes,
	// 1 the legacy one, and 0 a run that records none.
	for _, version := range []int{8, 4, 1, 0} {
		if err := worlds.CheckRunSpecVersion("wrun_a", version); err != nil {
			t.Errorf("CheckRunSpecVersion(%d) = %v", version, err)
		}
	}
}

// SDK: packages/world/src/spec-version.test.ts > requiresNewerWorld > accepts a slot-identity run even though it is above the default
func TestSDKSlotIdentityRunsAreSupported(t *testing.T) {
	if err := worlds.CheckRunSpecVersion("wrun_a", 6); err != nil {
		t.Fatalf("CheckRunSpecVersion(6) = %v", err)
	}
}

// SDK: packages/world/src/spec-version.test.ts > spec version constants > the readable ceiling moves with the version we stamp
func TestSDKTheStampedSpecVersionIsTheHighestSupported(t *testing.T) {
	// 9 is the SDK's single-orchestrator version, its readable ceiling.
	if worlds.SpecVersion != 9 {
		t.Fatalf("SpecVersion = %d, want 9", worlds.SpecVersion)
	}
	if stamped := worlds.EventSpecVersion(0); stamped != worlds.SpecVersion || worlds.CheckRunSpecVersion("wrun_a", stamped) != nil {
		t.Fatalf("a World stamps spec version %d, which it does not read back", stamped)
	}
}

// SDK: packages/world/src/slot-identity.test.ts > time is never derived from a slot id > fails validation instead of reporting 56 years of drift
func TestSDKARunIDMadeOfASlotIsNotAULID(t *testing.T) {
	body := strings.TrimPrefix(string(worlds.EventIDForSlot(1)), "evnt_")
	err := worlds.CheckRunCreation(worlds.EventsCreateRunInput{RunID: worlds.RunID("wrun_" + body), WorkflowName: "w"}, time.Now())
	if !errors.Is(err, worlds.ErrEventInvalid) || !strings.Contains(err.Error(), "is not wrun_ and a ULID") {
		t.Fatalf("CheckRunCreation = %v, want the run ID refused as not a ULID", err)
	}
}

// SDK: packages/world/src/events.test.ts > hook_created token retention > coerces tokenRetentionUntil to a Date
func TestSDKHookCreatedReadsTokenRetentionUntilAsATime(t *testing.T) {
	var created worlds.DataHookCreated
	if err := json.Unmarshal([]byte(`{"token":"order:123","tokenRetentionUntil":"2026-08-01T00:00:00.000Z"}`), &created); err != nil {
		t.Fatal(err)
	}
	if want := time.Date(2026, time.August, 1, 0, 0, 0, 0, time.UTC); created.TokenRetentionUntil == nil || !created.TokenRetentionUntil.Equal(want) {
		t.Fatalf("tokenRetentionUntil = %v, want %v", created.TokenRetentionUntil, want)
	}
}

// SDK: packages/world/src/events.test.ts > run_cancelled cancelReason > accepts a run_cancelled create request with no eventData
func TestSDKRunCancelledWithoutAReasonHasNoData(t *testing.T) {
	if data, err := worlds.EncodeRunCancelled(""); err != nil || data != nil {
		t.Fatalf("EncodeRunCancelled(\"\") = %s, %v", data, err)
	}
}

// SDK: packages/world/src/events.test.ts > run_cancelled cancelReason > accepts an optional cancelReason on the create request
func TestSDKRunCancelledRecordsItsReason(t *testing.T) {
	data, err := worlds.EncodeRunCancelled("superseded by newer run")
	if err != nil || worlds.RunCancelReason(data) != "superseded by newer run" {
		t.Fatalf("EncodeRunCancelled = %s, %v", data, err)
	}
}

// SDK: packages/world/src/events.test.ts > run_cancelled cancelReason > rejects a cancelReason longer than 512 chars
func TestSDKRunCancelledRefusesAReasonOver512Characters(t *testing.T) {
	if _, err := worlds.EncodeRunCancelled(strings.Repeat("x", 513)); !errors.Is(err, worlds.ErrEventInvalid) {
		t.Fatalf("EncodeRunCancelled of 513 characters = %v, want ErrEventInvalid", err)
	}
	if _, err := worlds.EncodeRunCancelled(strings.Repeat("x", 512)); err != nil {
		t.Fatalf("EncodeRunCancelled of 512 characters = %v", err)
	}
}

// SDK: packages/world/src/event-metadata.test.ts > event metadata > uses one payload-field mapping for singular and plural lookups
func TestSDKEventPayloadReadsTheMappedField(t *testing.T) {
	fields := worlds.EventPayloadFields()
	if fields[worlds.EventTypeRunCreated] != "input" || fields[worlds.EventTypeStepCompleted] != "result" {
		t.Fatalf("payload fields = %v", fields)
	}
	created := worlds.Event{EventType: worlds.EventTypeRunCreated, Data: []byte(`{"workflowName":"w","input":"aW4="}`)}
	completed := worlds.Event{EventType: worlds.EventTypeStepCompleted, Data: []byte(`{"stepName":"s","result":"b3V0"}`)}
	if string(worlds.EventPayload(created)) != "in" || string(worlds.EventPayload(completed)) != "out" {
		t.Fatalf("payloads = %q, %q", worlds.EventPayload(created), worlds.EventPayload(completed))
	}
}

// SDK: packages/world/src/event-metadata.test.ts > event metadata > returns no metadata for unknown and inherited property names
func TestSDKUnknownEventTypesHaveNoPayloadField(t *testing.T) {
	fields := worlds.EventPayloadFields()
	for _, eventType := range []worlds.EventType{"unknown", "constructor", "toString"} {
		if field, ok := fields[eventType]; ok {
			t.Errorf("payload field of %s = %q", eventType, field)
		}
		if payload := worlds.EventPayload(worlds.Event{EventType: eventType, Data: []byte(`{"input":"aW4="}`)}); payload != nil {
			t.Errorf("payload of %s = %q", eventType, payload)
		}
	}
}
