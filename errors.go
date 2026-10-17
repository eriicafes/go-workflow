package workflow

import (
	"errors"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

var (
	// ErrNoClient is returned by Start, Get, GetHook, and ResumeHook when ctx
	// carries no Client and does not belong to a run.
	ErrNoClient = errors.New("workflow: context has no client")
	// ErrNoWorkflow is returned when a function that must be called in
	// workflow context, such as Step, Join, or CreateHook, is called outside
	// it, including in step context, and when a stream function is called
	// outside a run.
	ErrNoWorkflow = errors.New("workflow: context has no active workflow")
	// ErrNotInStep is returned when a function that does I/O, such as Writer
	// or Run.Reader, is called in workflow context.
	ErrNotInStep = errors.New("workflow: must be called inside a step")
	// ErrRunCancelled is returned when awaiting a run that was cancelled.
	ErrRunCancelled = worlds.ErrRunCancelled
	// ErrRunNotFinished is returned by Run.Value in workflow context when the
	// run it awaits had not finished.
	ErrRunNotFinished = errors.New("workflow: run has not finished")
	// ErrHookNotFound is returned when resuming a hook token that no active
	// hook has.
	ErrHookNotFound = worlds.ErrHookNotFound
	// ErrHookNotActive is returned when receiving from a disposed hook.
	ErrHookNotActive = worlds.ErrHookNotActive
	// ErrHookNotRegistered is returned when receiving or selecting an unbound
	// hook, or when GetConflict or Dispose uses a handle from another execution.
	ErrHookNotRegistered = errors.New("workflow: hook is not registered")
	// ErrHookConflict is returned when claiming a hook whose token another
	// active hook already has. Hook.GetConflict reports the run that owns it.
	ErrHookConflict = worlds.ErrHookConflict
	// ErrHookForceClaimed is returned when awaiting a hook whose token
	// another run took over with HookOption.Force.
	ErrHookForceClaimed = worlds.ErrHookForceClaimed
)

// Error is the error Step returns for a failed step and Run.Value returns
// for a failed run. It keeps the type, message, and exported
// fields of each error in the original chain, so errors.Is matches sentinel
// errors and errors.As decodes error types after replay.
type Error = worlds.Error

// FatalError wraps cause so that a step returning it fails without retrying.
// It returns nil when cause is nil.
func FatalError(cause error) error {
	if cause == nil {
		return nil
	}
	return worlds.FatalError{Cause: cause}
}

// RetryableError wraps cause so that a step returning it is retried no sooner
// than after, while StepOptions.MaxRetries allows another retry. It returns
// nil when cause is nil.
func RetryableError(cause error, after time.Duration) error {
	if cause == nil {
		return nil
	}
	return worlds.RetryableError{Cause: cause, After: after}
}
