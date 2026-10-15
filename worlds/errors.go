package worlds

import (
	"errors"
	"fmt"
)

var (
	// ErrWorkflowNotFound is returned when a run names a workflow that is not
	// registered with the Runtime. The runtime fails the run, or the step
	// being executed, with this error.
	ErrWorkflowNotFound = errors.New("workflow: workflow not found")
	// ErrRunNotFound is returned when no run has the requested run ID.
	ErrRunNotFound = errors.New("workflow: run not found")
	// ErrRunAlreadyExists is returned by EventsCreateRun when a run with the
	// requested run ID already exists.
	ErrRunAlreadyExists = errors.New("workflow: run already exists")
	// ErrRunTerminal is returned when an event is appended to, or a stream is
	// written for, a run that has already completed, failed, or been cancelled.
	ErrRunTerminal = errors.New("workflow: run is terminal")
	// ErrRunCancelled reports that a run was cancelled. It is returned by
	// operations that observe the cancellation during replay and is the cause
	// of the context passed to a cancelled run's runner and steps.
	ErrRunCancelled = errors.New("workflow: run cancelled")
	// ErrStepNotFound is returned when a run has no step with the requested
	// step ID.
	ErrStepNotFound = errors.New("workflow: step not found")
	// ErrStepCancelled reports that a step was cancelled. It is the result of
	// a cancelled step on replay and the cause of the context passed to the
	// step function when the step is cancelled while it executes.
	ErrStepCancelled = errors.New("workflow: step cancelled")
	// ErrStepNotStartable is returned when a step_started or step_cancelled
	// event names a step that does not exist or whose status does not allow
	// the transition, such as starting a step that has completed.
	ErrStepNotStartable = errors.New("workflow: step is not startable")
	// ErrEventNotFound is returned when a run has no event with the requested
	// event ID, or no wait with the requested wait ID.
	ErrEventNotFound = errors.New("workflow: event not found")
	// ErrHookNotFound is returned when no hook has the requested hook ID, or
	// no active hook owns the requested hook token.
	ErrHookNotFound = errors.New("workflow: hook not found")
	// ErrHookNotActive is returned when a hook_received event targets a hook
	// that has already been resolved, has been disposed, or no longer owns its
	// token. It is also the result of a disposed hook on replay.
	ErrHookNotActive = errors.New("workflow: hook is not active")
	// ErrHookConflict is returned when a hook token is already owned by
	// another active hook and the claim is not forced.
	ErrHookConflict = errors.New("workflow: hook token is already claimed")
	// ErrHookForceClaimed is the result of a hook whose token another run
	// claimed with Force.
	ErrHookForceClaimed = errors.New("workflow: hook token was force claimed")
	// ErrStreamLocked is returned when a stream write conflicts with its
	// writer session. This happens when another session owns the stream, the
	// session is no longer current, or the write sequence is not the next one
	// expected.
	ErrStreamLocked = errors.New("workflow: stream namespace is already being written")
	// ErrEventInvalid is returned when an event, event ID, or event cursor is
	// malformed or names an unknown event type.
	ErrEventInvalid = errors.New("workflow: event is invalid")
	// ErrQueueMessageNotFound is returned by QueueCancel when no pending
	// delivery has the requested message ID.
	ErrQueueMessageNotFound = errors.New("workflow: queue message not found")
	// ErrWebhookRequestTooLarge is reported by WebhookHandler, with status 413,
	// when a webhook request body exceeds 128 KiB.
	ErrWebhookRequestTooLarge = errors.New("workflow: webhook request is too large")
	// ErrDeploymentUnavailable is returned by Queue when the deployment a
	// delivery is addressed to no longer exists or cannot receive deliveries.
	ErrDeploymentUnavailable = errors.New("workflow: deployment is unavailable")
	// ErrDeploymentMismatch is the error a run fails with when its deliveries
	// keep arriving at a deployment other than the one it is pinned to, or
	// when that deployment is unavailable.
	ErrDeploymentMismatch = errors.New("workflow: deployment mismatch")
	// ErrCorruptedEventLog is the error a run fails with when its replay keeps
	// diverging from its event log, usually because the runner's code changed
	// while the run was in flight. The run fails without running its
	// compensations, since they would replay against the same log.
	ErrCorruptedEventLog = errors.New("workflow: corrupted event log")

	errReplayComplete  = errors.New("workflow: targeted replay complete")
	errReplaySuspended = errors.New("workflow: replay suspended")
)

// deliveryError ends a replay because the World failed to record an event.
// The Runtime returns its cause from Execute, so the queue retries the
// delivery, instead of treating it as the runner's failure.
type deliveryError struct{ cause error }

func (e deliveryError) Error() string { return e.cause.Error() }
func (e deliveryError) Unwrap() error { return e.cause }

// replayDivergenceError reports that a runner, replaying its run, did not reach
// the operations recorded in the run's event log: the operation at Position is
// of another kind, calls another step function, or uses another hook token, or
// the runner returned without reaching a recorded operation. It usually means
// the runner's code changed while the run was in flight.
//
// The Runtime never returns it to runner code. It ends the delivery and queues
// a recovery delivery that replays the run again from a fresh read of its
// event log, up to three times, and then fails the run with a
// corruptedEventLogError.
type replayDivergenceError struct {
	Position int
	Reason   string
}

func (e *replayDivergenceError) Error() string {
	return fmt.Sprintf("workflow: replay diverged at position %d: %s", e.Position, e.Reason)
}

// corruptedEventLogError is the error a run fails with when its replay keeps
// diverging from its event log. Position and Reason describe the last
// replayDivergenceError, and RecoveryReplays counts the recovery deliveries
// that diverged before it. It wraps ErrCorruptedEventLog, which errors.Is
// matches on the run's recorded error.
type corruptedEventLogError struct {
	Position        int
	Reason          string
	RecoveryReplays int
}

func (e *corruptedEventLogError) Error() string {
	return fmt.Sprintf("%s at position %d after %d recovery replays: %s", ErrCorruptedEventLog, e.Position, e.RecoveryReplays, e.Reason)
}

func (e *corruptedEventLogError) Unwrap() error { return ErrCorruptedEventLog }

// deploymentMismatchError is the error a run fails with when its deliveries
// keep arriving at a deployment other than RunDeploymentID, the one it is
// pinned to, or when that deployment is unavailable. DeploymentID is the
// deployment that received the last delivery, and RecoveryAttempts is how many
// times the delivery was re-routed first. It wraps ErrDeploymentMismatch, which
// errors.Is matches after the error is recorded.
type deploymentMismatchError struct {
	RunDeploymentID  string
	DeploymentID     string
	RecoveryAttempts int
}

func (e *deploymentMismatchError) Error() string {
	return fmt.Sprintf("%s: run is pinned to deployment %q but was delivered to %q after %d re-routes", ErrDeploymentMismatch, e.RunDeploymentID, e.DeploymentID, e.RecoveryAttempts)
}

func (e *deploymentMismatchError) Unwrap() error { return ErrDeploymentMismatch }
