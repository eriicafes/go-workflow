package workflowops

import (
	"errors"
	"testing"
)

func TestFailureRoundTripsKnownCode(t *testing.T) {
	err := DecodeFailure(EncodeFailure(ErrHookForceClaimed))
	if !errors.Is(err, ErrHookForceClaimed) {
		t.Fatalf("DecodeFailure() = %v, want ErrHookForceClaimed", err)
	}
}

func TestFailureDoesNotClassifyMatchingApplicationMessage(t *testing.T) {
	err := DecodeFailure(ErrHookForceClaimed.Error())
	if errors.Is(err, ErrHookForceClaimed) {
		t.Fatal("DecodeFailure() classified an application error by its message")
	}
}
