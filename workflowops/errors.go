package workflowops

import "errors"

var (
	// ErrUnknownWorkflow identifies a queued run without a registered runner.
	ErrUnknownWorkflow = errors.New("workflow: unknown workflow")

	// ErrRunNotFound identifies a missing durable workflow run.
	ErrRunNotFound = errors.New("workflow: run not found")

	// ErrLeaseLost means another worker owns a newer task lease.
	ErrLeaseLost = errors.New("workflow: lease lost")
)
