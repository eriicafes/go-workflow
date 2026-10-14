package workflow_test

import (
	"errors"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow"
)

func TestFatalErrorPreservesCauseAndAcceptsNil(t *testing.T) {
	cause := errors.New("stop")
	if got := workflow.FatalError(nil); got != nil {
		t.Fatalf("FatalError(nil) = %v", got)
	}
	if got := workflow.FatalError(cause); !errors.Is(got, cause) || got.Error() != cause.Error() {
		t.Fatalf("FatalError = %v", got)
	}
}

func TestRetryableErrorPreservesCauseAndAcceptsNil(t *testing.T) {
	cause := errors.New("retry")
	if got := workflow.RetryableError(nil, time.Second); got != nil {
		t.Fatalf("RetryableError(nil) = %v", got)
	}
	if got := workflow.RetryableError(cause, time.Second); !errors.Is(got, cause) || got.Error() != cause.Error() {
		t.Fatalf("RetryableError = %v", got)
	}
}
