package worlds_test

import (
	"encoding/json"
	"errors"
	"fmt"
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
	data, marshalErr := json.Marshal(worlds.NewError(err))
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	return worlds.DecodeError(data)
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

func TestDecodeStepErrorAcceptsPlainMessages(t *testing.T) {
	if err := worlds.DecodeError([]byte("boom")); err.Error() != "boom" {
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
