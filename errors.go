package workflow

import (
	"errors"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

var (
	ErrNoClient          = errors.New("workflow: context has no client")
	ErrNoWorkflow        = errors.New("workflow: context has no active workflow")
	ErrRunCancelled      = worlds.ErrRunCancelled
	ErrHookNotFound      = worlds.ErrHookNotFound
	ErrHookNotRegistered = errors.New("workflow: hook is not registered")
	ErrHookConflict      = worlds.ErrHookConflict
	ErrHookForceClaimed  = worlds.ErrHookForceClaimed
)

type fatalError struct{ cause error }

func (e fatalError) Error() string { return e.cause.Error() }
func (e fatalError) Unwrap() error { return e.cause }

// FatalError marks a step failure as terminal and bypasses retries.
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

func (e retryableError) Error() string { return e.cause.Error() }
func (e retryableError) Unwrap() error { return e.cause }

// RetryableError requests a retry after after, subject to StepOptions.
func RetryableError(cause error, after time.Duration) error {
	if cause == nil {
		return nil
	}
	return retryableError{cause: cause, after: after}
}
