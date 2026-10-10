package internal

import "errors"

var (
	// ErrReplayComplete stops a targeted replay after it persists its outcome.
	ErrReplayComplete = errors.New("workflow: targeted replay complete")

	// ErrReplaySuspended stops a root replay after it persists an unresolved dependency.
	ErrReplaySuspended = errors.New("workflow: replay suspended for durable dependency")
)
