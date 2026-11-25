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

// SDK: packages/errors/src/serialization-error.test.ts > SerializationError > sets the name and extends WorkflowError
func TestSDKSerializationErrorIsRecordedUnderItsClassName(t *testing.T) {
	recorded := roundTripError(t, &worlds.SerializationError{Message: "boom"})
	var serialization *worlds.SerializationError
	if recorded.Name != "SerializationError" || !errors.As(recorded, &serialization) || serialization.Message != "boom" {
		t.Fatalf("recorded = %+v, want an Error named SerializationError", recorded)
	}
}

// SDK: packages/errors/src/serialization-error.test.ts > SerializationError > renders just the title when no hint is provided
func TestSDKSerializationErrorWithoutACauseIsItsMessage(t *testing.T) {
	if err := (&worlds.SerializationError{Message: "boom"}); err.Error() != "boom" {
		t.Fatalf("message = %q, want boom", err.Error())
	}
}

// SDK: packages/errors/src/serialization-error.test.ts > SerializationError > preserves cause for debugging
func TestSDKSerializationErrorKeepsItsCause(t *testing.T) {
	cause := errors.New("underlying")
	err := &worlds.SerializationError{Message: "boom", Cause: cause}
	if !errors.Is(err, cause) || err.Error() != "boom: underlying" {
		t.Fatalf("err = %v, want it to wrap its cause", err)
	}
	recorded := roundTripError(t, err)
	var serialization *worlds.SerializationError
	if !errors.As(recorded, &serialization) || serialization.Message != "boom" || serialization.Cause == nil || serialization.Cause.Error() != "underlying" {
		t.Fatalf("recorded = %+v, read back as %+v", recorded, serialization)
	}
}

// SDK: packages/errors/src/serialization-error.test.ts > SerializationError > SerializationError.is discriminates by name
func TestSDKSerializationErrorMatchesOnlyItsOwnErrors(t *testing.T) {
	var serialization *worlds.SerializationError
	if recorded := roundTripError(t, &worlds.SerializationError{Message: "boom"}); !errors.As(recorded, &serialization) {
		t.Fatalf("recorded = %+v, want a SerializationError", recorded)
	}
	if other := roundTripError(t, errors.New("boom")); errors.As(other, &serialization) {
		t.Fatalf("a plain error read back as a SerializationError: %+v", other)
	}
	if errors.As(nil, &serialization) {
		t.Fatal("nil read back as a SerializationError")
	}
}

// SDK: packages/errors/src/serialization-error.test.ts > SerializationError > is fatal — FatalError.is() short-circuits retry loop
func TestSDKSerializationErrorIsFatal(t *testing.T) {
	if !worlds.IsFatal(&worlds.SerializationError{Message: "boom"}) {
		t.Fatal("a SerializationError is not fatal")
	}
}

// SDK: packages/errors/src/encryption-key-lookup-error.test.ts > EncryptionKeyLookupError > sets the name and extends WorkflowRuntimeError
func TestSDKEncryptionKeyLookupErrorIsRecordedUnderItsClassName(t *testing.T) {
	recorded := roundTripError(t, &worlds.EncryptionKeyLookupError{RunID: "wrun_1", Cause: errors.New("404 Not Found")})
	var lookup *worlds.EncryptionKeyLookupError
	if recorded.Name != "EncryptionKeyLookupError" || !errors.As(recorded, &lookup) || lookup.RunID != "wrun_1" {
		t.Fatalf("recorded = %+v, want an Error named EncryptionKeyLookupError", recorded)
	}
}

// SDK: packages/errors/src/encryption-key-lookup-error.test.ts > EncryptionKeyLookupError > is fatal
func TestSDKEncryptionKeyLookupErrorIsFatal(t *testing.T) {
	if !worlds.IsFatal(&worlds.EncryptionKeyLookupError{RunID: "wrun_1"}) {
		t.Fatal("an EncryptionKeyLookupError is not fatal")
	}
}

// SDK: packages/errors/src/encryption-key-lookup-error.test.ts > EncryptionKeyLookupError > preserves cause for debugging
func TestSDKEncryptionKeyLookupErrorKeepsItsCause(t *testing.T) {
	cause := errors.New("404 Not Found")
	err := &worlds.EncryptionKeyLookupError{RunID: "wrun_1", Cause: cause}
	if !errors.Is(err, cause) || !errors.Is(roundTripError(t, err), cause) {
		t.Fatalf("err = %v, want it to wrap its cause", err)
	}
}

// SDK: packages/errors/src/terminal-run-error.test.ts > terminal run errors are non-retryable > marks WorkflowRunFailedError fatal
func TestSDKRunFailedErrorIsFatal(t *testing.T) {
	err := &worlds.RunFailedError{RunID: "wrun_1", ErrorCode: "USER_ERROR", Cause: errors.New("boom")}
	if !worlds.IsFatal(err) {
		t.Fatal("a RunFailedError is not fatal")
	}
	recorded := roundTripError(t, err)
	var failed *worlds.RunFailedError
	if recorded.Name != "WorkflowRunFailedError" || !errors.As(recorded, &failed) {
		t.Fatalf("recorded = %+v, want an Error named WorkflowRunFailedError", recorded)
	}
	if failed.RunID != "wrun_1" || failed.ErrorCode != "USER_ERROR" || failed.Cause == nil || failed.Cause.Error() != "boom" {
		t.Fatalf("read back as %+v", failed)
	}
}

// SDK: packages/errors/src/terminal-run-error.test.ts > terminal run errors are non-retryable > marks WorkflowRunCancelledError fatal
func TestSDKRunCancelledErrorIsFatal(t *testing.T) {
	err := &worlds.RunCancelledError{RunID: "wrun_1"}
	if !worlds.IsFatal(err) || !errors.Is(err, worlds.ErrRunCancelled) {
		t.Fatalf("err = %v, want a fatal error matching ErrRunCancelled", err)
	}
	recorded := roundTripError(t, err)
	var cancelled *worlds.RunCancelledError
	if recorded.Name != "WorkflowRunCancelledError" || !errors.As(recorded, &cancelled) || cancelled.RunID != "wrun_1" || !errors.Is(recorded, worlds.ErrRunCancelled) {
		t.Fatalf("recorded = %+v, read back as %+v", recorded, cancelled)
	}
}

// SDK: packages/errors/src/fatal-error.test.ts > FatalError.is > returns true for any error with fatal: true
func TestSDKAnErrorWithAFatalMethodIsFatal(t *testing.T) {
	if !worlds.IsFatal(fmt.Errorf("step: %w", contextViolation{fatal: true})) {
		t.Fatal("an error whose Fatal method returns true is not fatal")
	}
}

// SDK: packages/errors/src/fatal-error.test.ts > FatalError.is > returns false when fatal is not strictly true
func TestSDKAnErrorWhoseFatalMethodReturnsFalseIsNotFatal(t *testing.T) {
	if worlds.IsFatal(contextViolation{}) {
		t.Fatal("an error whose Fatal method returns false is fatal")
	}
}

// SDK: packages/errors/src/fatal-error.test.ts > FatalError.is > returns false for plain errors
func TestSDKARecordedNamedFatalErrorIsNotFatal(t *testing.T) {
	// The SDK revives an error recorded with a class name as a plain Error.
	if recorded := roundTripError(t, &worlds.SerializationError{Message: "boom"}); worlds.IsFatal(recorded) {
		t.Fatalf("recorded = %+v is fatal", recorded)
	}
	if recorded := roundTripError(t, worlds.FatalError{Cause: errors.New("boom")}); !worlds.IsFatal(recorded) {
		t.Fatalf("recorded = %+v is not fatal", recorded)
	}
}

type contextViolation struct{ fatal bool }

func (e contextViolation) Error() string { return "context violation" }
func (e contextViolation) Fatal() bool   { return e.fatal }
