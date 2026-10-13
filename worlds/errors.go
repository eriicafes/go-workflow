package worlds

import "errors"

var (
	ErrWorkflowNotFound       = errors.New("workflow: workflow not found")
	ErrRunTerminal            = errors.New("workflow: run is terminal")
	ErrRunCancelled           = errors.New("workflow: run cancelled")
	ErrStepNotFound           = errors.New("workflow: step not found")
	ErrStepCancelled          = errors.New("workflow: step cancelled")
	ErrStepNotStartable       = errors.New("workflow: step is not startable")
	ErrHookNotFound           = errors.New("workflow: hook not found")
	ErrHookNotActive          = errors.New("workflow: hook is not active")
	ErrEventInvalid           = errors.New("workflow: event is invalid")
	ErrQueueMessageNotFound   = errors.New("workflow: queue message not found")
	ErrWebhookRequestTooLarge = errors.New("workflow: webhook request is too large")
)
