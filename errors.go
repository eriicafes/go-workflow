package workflow

import (
	"errors"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

var (
	// ErrNoClient reports that the context has no client.
	ErrNoClient = errors.New("workflow: context has no client")
	// ErrNoWorkflow reports that an operation requires workflow context.
	ErrNoWorkflow = errors.New("workflow: context has no active workflow")
	// ErrNilResult reports a nil result where an operation's result was expected.
	ErrNilResult = errors.New("workflow: nil result")
	// ErrNotInStep reports that an operation cannot be called in workflow context.
	ErrNotInStep = errors.New("workflow: must be called inside a step")
	// ErrRunCancelled is returned when awaiting a run that was cancelled.
	ErrRunCancelled = worlds.ErrRunCancelled
	// ErrInvalidStartOptions is returned by Start for StartOptions it cannot
	// use, such as a ReplayedFromRunID that is not a run ID.
	ErrInvalidStartOptions = errors.New("workflow: invalid start options")
	// ErrStartHookOptions is the error of a hook that opens its run's start
	// hook with HookOption.MinRetention or Force, which only
	// StartOptions.Hook can set.
	ErrStartHookOptions = worlds.ErrStartHookOptions
	// ErrRunExpired is returned when awaiting a run whose data was deleted,
	// such as a run started with RetainNone.
	ErrRunExpired = worlds.ErrRunExpired
	// ErrStreamExpired is returned when reading a stream whose data the World
	// deleted.
	ErrStreamExpired = worlds.ErrStreamExpired
	// ErrResumeNotQueued reports that a hook payload was recorded but its run was not queued.
	ErrResumeNotQueued = worlds.ErrResumeNotQueued
	// ErrRunNotFinished reports that a run is still waiting or running.
	ErrRunNotFinished = errors.New("workflow: run has not finished")
	// ErrHookNotFound reports that no active hook owns the requested token.
	ErrHookNotFound = worlds.ErrHookNotFound
	// ErrHookNotActive reports that a hook can no longer receive payloads.
	ErrHookNotActive = worlds.ErrHookNotActive
	// ErrHookNotRegistered reports that a hook does not belong to the current
	// workflow execution.
	ErrHookNotRegistered = errors.New("workflow: hook is not registered")
	// ErrHookConflict is returned when claiming a hook whose token another
	// active hook already has. Hook.GetConflict reports the run that owns it.
	ErrHookConflict = worlds.ErrHookConflict
	// ErrHookForceClaimed reports that another hook took over this hook's token.
	ErrHookForceClaimed = worlds.ErrHookForceClaimed
)

// Error represents a recorded failure. It keeps the type, message, and exported
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
