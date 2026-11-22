package worlds_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

var errSentinel = errors.New("sentinel")

type valueError struct{ Code int }

func (e valueError) Error() string { return fmt.Sprintf("value %d", e.Code) }

type pointerError struct {
	Reason string
	hidden string
}

func (e *pointerError) Error() string { return e.Reason + e.hidden }

func roundTripError(t *testing.T, err error) *worlds.Error {
	t.Helper()
	data, encodeErr := worlds.EncodeErrorPayload(err, worlds.PayloadOptions{})
	if encodeErr != nil {
		t.Fatal(encodeErr)
	}
	return worlds.DecodeErrorPayload(data, nil)
}

func TestStepErrorMatchesChainAfterRoundTrip(t *testing.T) {
	original := fmt.Errorf("outer: %w", errors.Join(valueError{Code: 7}, &pointerError{Reason: "bad", hidden: "!"}, errSentinel))
	decoded := roundTripError(t, original)
	if decoded.Error() != original.Error() {
		t.Fatalf("message = %q, want %q", decoded.Error(), original.Error())
	}
	if !errors.Is(decoded, errSentinel) {
		t.Fatal("errors.Is did not match the sentinel")
	}
	if errors.Is(decoded, errors.New("other")) {
		t.Fatal("errors.Is matched an error with another message")
	}
	var value valueError
	if !errors.As(decoded, &value) || value.Code != 7 {
		t.Fatalf("value error = %#v", value)
	}
	var pointer *pointerError
	if !errors.As(decoded, &pointer) || pointer.Reason != "bad" || pointer.hidden != "" {
		t.Fatalf("pointer error = %#v", pointer)
	}
	var missing *json.SyntaxError
	if errors.As(decoded, &missing) {
		t.Fatal("errors.As matched a type that is not in the chain")
	}
}

func TestStepErrorKeepsRecordedChainWhenWrapped(t *testing.T) {
	inner := roundTripError(t, valueError{Code: 3})
	decoded := roundTripError(t, fmt.Errorf("run: %w", inner))
	var value valueError
	if decoded.Error() != "run: value 3" || !errors.As(decoded, &value) || value.Code != 3 {
		t.Fatalf("decoded = %v, value = %#v", decoded, value)
	}
}

func TestDecodeStepErrorReportsInvalidPayloads(t *testing.T) {
	if err := worlds.DecodeErrorPayload([]byte("boom"), nil); !strings.Contains(err.Error(), "invalid payload") {
		t.Fatalf("message = %q", err.Error())
	}
}

func TestFatalErrorRecordsMarkerAndCauseInErrorChain(t *testing.T) {
	for _, original := range []error{
		worlds.FatalError{Cause: errSentinel},
		&worlds.FatalError{Cause: errSentinel},
	} {
		decoded := roundTripError(t, fmt.Errorf("outer: %w", original))
		var matched bool
		switch original.(type) {
		case worlds.FatalError:
			var fatal worlds.FatalError
			matched = errors.As(decoded, &fatal)
		case *worlds.FatalError:
			var fatal *worlds.FatalError
			matched = errors.As(decoded, &fatal)
		}
		if !matched || !errors.Is(decoded, errSentinel) || decoded.Error() != "outer: sentinel" {
			t.Fatalf("fatal marker=%v decoded=%v", matched, decoded)
		}
	}
}

func TestRetryableErrorRecordsMarkerAndCauseInErrorChain(t *testing.T) {
	for _, original := range []error{
		worlds.RetryableError{Cause: errSentinel, After: time.Minute},
		&worlds.RetryableError{Cause: errSentinel, After: time.Minute},
	} {
		decoded := roundTripError(t, fmt.Errorf("outer: %w", original))
		var matched bool
		switch original.(type) {
		case worlds.RetryableError:
			var retryable worlds.RetryableError
			matched = errors.As(decoded, &retryable)
		case *worlds.RetryableError:
			var retryable *worlds.RetryableError
			matched = errors.As(decoded, &retryable)
		}
		if !matched || !errors.Is(decoded, errSentinel) || decoded.Error() != "outer: sentinel" {
			t.Fatalf("retryable marker=%v decoded=%v", matched, decoded)
		}
	}
}

func TestDeliveryRetryDelayWaitsForAThrottle(t *testing.T) {
	throttle := &worlds.ThrottleError{After: time.Minute}
	for _, test := range []struct {
		err  error
		want time.Duration
	}{
		{errors.New("failed"), 5 * time.Second},
		{throttle, time.Minute},
		{&worlds.ThrottleError{After: time.Second}, 5 * time.Second},
		{&worlds.ThrottleError{}, 5 * time.Second},
		{fmt.Errorf("deliver: %w", throttle), time.Minute},
	} {
		if got := worlds.DeliveryRetryDelay(test.err, 5*time.Second); got != test.want {
			t.Errorf("DeliveryRetryDelay(%v) = %v, want %v", test.err, got, test.want)
		}
	}
}

func TestSDKErrorsAreRecordedAsTheSDKsTypes(t *testing.T) {
	for _, test := range []struct {
		err  error
		want string
	}{
		{worlds.FatalError{Cause: errSentinel}, `["FatalError",`},
		{worlds.RetryableError{Cause: errSentinel, After: time.Minute}, `"retryAfter"`},
		// A pointer keeps its Go type, so errors.As finds it as a pointer.
		{&worlds.FatalError{Cause: errSentinel}, `"*github.com/eriicafes/go-workflow/worlds.FatalError"`},
	} {
		data, err := worlds.EncodeErrorPayload(test.err, worlds.PayloadOptions{NoCompression: true})
		if err != nil {
			t.Fatal(err)
		}
		if text := string(data); !strings.Contains(text, test.want) {
			t.Fatalf("%T recorded as %s, want %s", test.err, text, test.want)
		}
		// A decoded chain records the same type again.
		decoded := worlds.DecodeErrorPayload(data, nil)
		again, err := worlds.EncodeErrorPayload(decoded, worlds.PayloadOptions{NoCompression: true})
		if err != nil || !strings.Contains(string(again), test.want) {
			t.Fatalf("%T recorded again as %s err=%v", test.err, again, err)
		}
	}
	var retryable worlds.RetryableError
	decoded := roundTripError(t, worlds.RetryableError{Cause: errSentinel, After: time.Hour})
	if !errors.As(decoded, &retryable) || retryable.After < 59*time.Minute || retryable.After > time.Hour {
		t.Fatalf("retryable after = %s", retryable.After)
	}
}

func TestIsEventRefusalMatchesRefusalsOnly(t *testing.T) {
	for _, test := range []struct {
		err  error
		want bool
	}{
		{nil, false},
		{worlds.ErrEntityConflict, true},
		{fmt.Errorf("wrapped: %w", &worlds.RunExpiredError{}), true},
		{errors.Join(errors.New("storage"), worlds.ErrStepNotFound), true},
		{&worlds.HookConflictError{RunID: "wrun_1"}, true},
		{&worlds.TooEarlyError{RetryAfter: time.Second}, true},
		{errors.New("storage"), false},
		{worlds.ErrRunNotFound, false},
	} {
		if got := worlds.IsEventRefusal(test.err); got != test.want {
			t.Errorf("IsEventRefusal(%v) = %v, want %v", test.err, got, test.want)
		}
	}
}

func TestHookErrorsAreRecordedAsTheSDKsTypes(t *testing.T) {
	conflict := roundTripError(t, &worlds.HookConflictError{Token: "tok", RunID: "wrun_1", Start: true})
	var gotConflict *worlds.HookConflictError
	if conflict.Type != "HookConflictError" || !errors.As(conflict, &gotConflict) || *gotConflict != (worlds.HookConflictError{Token: "tok", RunID: "wrun_1", Start: true}) {
		t.Fatalf("conflict = %+v %+v", conflict, gotConflict)
	}
	if _, ok := conflict.Field("kind"); !ok {
		t.Fatal("a start hook conflict has no kind")
	}
	claimed := roundTripError(t, fmt.Errorf("wait: %w", &worlds.HookForceClaimedError{Token: "tok", RunID: "wrun_2", HookID: "hook_2"}))
	var gotClaimed *worlds.HookForceClaimedError
	if !errors.As(claimed, &gotClaimed) || *gotClaimed != (worlds.HookForceClaimedError{Token: "tok", RunID: "wrun_2", HookID: "hook_2"}) {
		t.Fatalf("claimed = %+v %+v", claimed, gotClaimed)
	}
}

func TestExpiryAndLookupErrorsAreRecordedAsNamedErrors(t *testing.T) {
	for _, test := range []struct {
		err   error
		name  string
		check func(error) bool
	}{
		{&worlds.HookNotFoundError{Token: "tok"}, "HookNotFoundError", func(err error) bool {
			got, ok := errors.AsType[*worlds.HookNotFoundError](err)
			return ok && *got == worlds.HookNotFoundError{Token: "tok"}
		}},
		{&worlds.RunExpiredError{RunID: "wrun_1", RunStatus: worlds.StatusCompleted}, "RunExpiredError", func(err error) bool {
			got, ok := errors.AsType[*worlds.RunExpiredError](err)
			return ok && *got == worlds.RunExpiredError{RunID: "wrun_1", RunStatus: worlds.StatusCompleted}
		}},
		{&worlds.StreamExpiredError{RunID: "wrun_1", StreamID: "strm_1"}, "StreamExpiredError", func(err error) bool {
			got, ok := errors.AsType[*worlds.StreamExpiredError](err)
			return ok && *got == worlds.StreamExpiredError{RunID: "wrun_1", StreamID: "strm_1"}
		}},
	} {
		recorded := roundTripError(t, fmt.Errorf("step: %w", test.err))
		if recorded.Cause == nil || recorded.Cause.Name != test.name || !test.check(recorded) {
			t.Errorf("%T recorded as %+v", test.err, recorded.Cause)
		}
	}
}

func TestStartErrorIsRecordedWithItsRunAndStage(t *testing.T) {
	disabled := fmt.Errorf("%w: off", worlds.ErrStartHookDisabled)
	failed := &worlds.StartError{RunID: "wrun_1", Stage: worlds.StartStageAdmission, Cause: disabled}
	if !errors.Is(failed, worlds.ErrStartHookDisabled) || !strings.Contains(failed.Error(), "run wrun_1 was admitted") {
		t.Fatalf("StartError = %v, want it to name the run and stage and wrap its cause", failed)
	}
	if queue := (&worlds.StartError{RunID: "wrun_1", Stage: worlds.StartStageQueue}); !strings.Contains(queue.Error(), "was queued") {
		t.Fatalf("StartError for the queue = %v", queue)
	}
	var read *worlds.StartError
	if recorded := worlds.NewError(failed); recorded.Name != "WorkflowStartError" || !errors.As(recorded, &read) || read.RunID != "wrun_1" || read.Stage != worlds.StartStageAdmission {
		t.Fatalf("recorded StartError = %+v, read back %+v", recorded, read)
	}
}
