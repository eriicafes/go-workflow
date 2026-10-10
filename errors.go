package workflow

import (
	"errors"

	"github.com/eriicafes/go-workflow/workflowops"
)

var (
	// ErrNoClient means Start was called without workflow.NewContext. Start is
	// valid from application, workflow, and step contexts when that client is present.
	ErrNoClient = errors.New("workflow: context has no client")

	// ErrNoWorkflow means a durable primitive was called outside a workflow body.
	ErrNoWorkflow = errors.New("workflow: context has no active workflow")

	// ErrUnknownWorkflow means the application has not installed that workflow.
	ErrUnknownWorkflow = workflowops.ErrUnknownWorkflow

	// ErrRunNotFound means the requested durable run does not exist.
	ErrRunNotFound = workflowops.ErrRunNotFound

	// ErrLeaseLost means another worker obtained a newer fencing token.
	ErrLeaseLost = workflowops.ErrLeaseLost
)
