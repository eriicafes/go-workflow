package workflow

import (
	"errors"
	"time"

	"github.com/eriicafes/go-workflow/workflowops"
)

var (
	// ErrNoClient means Start was called without workflow.NewContext. Start is
	// valid from application, workflow, and step contexts when that client is present.
	ErrNoClient = errors.New("workflow: context has no client")

	// ErrNoWorkflow means a durable primitive was called outside a workflow body.
	ErrNoWorkflow = errors.New("workflow: context has no active workflow")

	// ErrRunNotFound means the requested durable run does not exist.
	ErrRunNotFound = workflowops.ErrRunNotFound

	// ErrCancelled means the durable task was cancelled.
	ErrCancelled = workflowops.ErrCancelled
)

type fatalError struct{ cause error }

func (errorValue fatalError) Error() string { return errorValue.cause.Error() }
func (errorValue fatalError) Unwrap() error { return errorValue.cause }

// FatalError marks a step failure as terminal. It bypasses StepOptions retries.
func FatalError(cause error) error {
	if cause == nil {
		return nil
	}
	return fatalError{cause: cause}
}

type retryableError struct {
	cause error
	after time.Duration
}

func (errorValue retryableError) Error() string { return errorValue.cause.Error() }
func (errorValue retryableError) Unwrap() error { return errorValue.cause }

// RetryableError requests a durable retry after after. It still respects the
// step's MaxRetries limit.
func RetryableError(cause error, after time.Duration) error {
	if cause == nil {
		return nil
	}
	return retryableError{cause: cause, after: after}
}
