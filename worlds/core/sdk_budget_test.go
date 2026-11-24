package core

import (
	"errors"
	"testing"

	"github.com/eriicafes/go-workflow/worlds"
)

// The SDK's message also has a hint and a docs link below its title, which a
// Go error's message does not.
//
// SDK: packages/errors/src/max-events-exceeded-error.test.ts > MaxEventsExceededError > frames a QuickJS hint above the docs link
func TestSDKMaxEventsExceededNamesTheLimit(t *testing.T) {
	failure := maxEventsExceededError{count: 25_000, limit: 25_000}
	if got, want := failure.Error(), "Workflow exceeded the maximum of 25000 events per run"; got != want {
		t.Fatalf("message = %q, want %q", got, want)
	}
	recorded, ok := failure.recorded().(*worlds.Error)
	if !ok || recorded.Name != "MaxEventsExceededError" || recorded.Message != failure.Error() || !errors.Is(recorded, worlds.ErrMaxEventsExceeded) {
		t.Fatalf("recorded = %#v", failure.recorded())
	}
}

// SDK: packages/errors/src/max-events-exceeded-error.test.ts > MaxEventsExceededError > says the count is a lower bound when the log was not read to its end
func TestSDKMaxEventsExceededSaysTheCountIsALowerBound(t *testing.T) {
	failure := maxEventsExceededError{count: 12, limit: 10, atLeast: true}
	if got, want := failure.Error(), "Workflow exceeded the maximum of 10 events per run (its event log holds at least 12 events)"; got != want {
		t.Fatalf("message = %q, want %q", got, want)
	}
}
