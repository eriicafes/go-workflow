package worlds_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/eriicafes/go-workflow/worlds"
)

// SDK: packages/errors/src/corrupted-event-log-error.test.ts > CorruptedEventLogError > sets the name and extends WorkflowRuntimeError
func TestSDKCorruptedEventLogIsRecordedUnderItsClassName(t *testing.T) {
	for _, err := range []error{worlds.ErrCorruptedEventLog, fmt.Errorf("%w: event mismatch", worlds.ErrCorruptedEventLog)} {
		if recorded := roundTripError(t, err); recorded.Name != "CorruptedEventLogError" {
			t.Errorf("%v recorded as %+v", err, recorded)
		}
	}
}

// SDK: packages/errors/src/corrupted-event-log-error.test.ts > CorruptedEventLogError > preserves cause for debugging
func TestSDKCorruptedEventLogKeepsItsCause(t *testing.T) {
	cause := errors.New("underlying mismatch")
	recorded := roundTripError(t, fmt.Errorf("%w: event mismatch: %w", worlds.ErrCorruptedEventLog, cause))
	if !errors.Is(recorded, cause) || !errors.Is(recorded, worlds.ErrCorruptedEventLog) {
		t.Fatalf("recorded = %+v, want it to match its cause and the corrupted event log", recorded)
	}
}

// SDK: packages/errors/src/corrupted-event-log-error.test.ts > CorruptedEventLogError > CorruptedEventLogError.is discriminates by name
func TestSDKCorruptedEventLogMatchesOnlyItsOwnErrors(t *testing.T) {
	if recorded := roundTripError(t, fmt.Errorf("%w: event mismatch", worlds.ErrCorruptedEventLog)); !errors.Is(recorded, worlds.ErrCorruptedEventLog) {
		t.Fatalf("recorded = %+v, want it to match ErrCorruptedEventLog", recorded)
	}
	if other := roundTripError(t, errors.New("event mismatch")); errors.Is(other, worlds.ErrCorruptedEventLog) {
		t.Fatalf("a plain error matched ErrCorruptedEventLog: %+v", other)
	}
	if errors.Is(nil, worlds.ErrCorruptedEventLog) {
		t.Fatal("nil matched ErrCorruptedEventLog")
	}
}
