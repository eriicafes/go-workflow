package workflowops

import "errors"

var (
	// ErrUnknownWorkflow identifies a queued run without a registered runner.
	ErrUnknownWorkflow = errors.New("workflow: unknown workflow")

	// ErrRunNotFound identifies a missing durable workflow run.
	ErrRunNotFound = errors.New("workflow: run not found")

	// ErrLeaseLost means another worker owns a newer task lease.
	ErrLeaseLost = errors.New("workflow: lease lost")

	// ErrCancelled identifies a durably cancelled workflow task.
	ErrCancelled = errors.New("workflow: cancelled")

	// ErrNotSupported identifies a world feature that has not been configured.
	ErrNotSupported = errors.New("workflow: operation is not supported by this world")

	// ErrHookNotFound identifies a token that has not been durably registered.
	ErrHookNotFound = errors.New("workflow: hook not found")

	// ErrHookConflict identifies an active owner of a non-forced hook token.
	ErrHookConflict = errors.New("workflow: hook token is already claimed")

	// ErrHookForceClaimed is persisted for the old owner when a newer hook
	// force-claims its token.
	ErrHookForceClaimed = errors.New("workflow: hook token was force claimed")
)
