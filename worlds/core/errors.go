package core

import (
	"errors"
	"fmt"

	"github.com/eriicafes/go-workflow/worlds"
)

var (
	errReplayComplete  = errors.New("workflow: targeted replay complete")
	errReplaySuspended = errors.New("workflow: replay suspended")
	// errStepAttemptsExhausted fails a step whose every attempt started
	// without recording a result.
	errStepAttemptsExhausted = errors.New("workflow: step attempts exhausted")
	// errReplaySuperseded ends a flow whose in-band write was refused
	// because another orchestrator wrote first. The Runtime replays again.
	errReplaySuperseded = errors.New("workflow: replay superseded")
)

// deliveryError ends a replay because the World failed to record an event.
// The Runtime returns its cause from Execute, so the queue retries the
// delivery, instead of treating it as the runner's failure.
type deliveryError struct{ cause error }

// DeliveryError marks cause, a World error a step function hit while doing
// the runtime's own work, so the delivery is retried instead of the step
// recording a failed attempt.
func DeliveryError(cause error) error { return deliveryError{cause: cause} }

func (e deliveryError) Error() string { return e.cause.Error() }
func (e deliveryError) Unwrap() error { return e.cause }

// RunErrorCode records a run failed by a World error as a World contract
// error.
func (deliveryError) RunErrorCode() string { return "WORLD_CONTRACT_ERROR" }

// replayDivergenceError identifies a declaration that does not match the event log.
type replayDivergenceError struct {
	CorrelationID string
	Reason        string
}

// RunErrorCode records a run failed by a divergence as one.
func (replayDivergenceError) RunErrorCode() string { return "REPLAY_DIVERGENCE" }

func (e replayDivergenceError) Error() string {
	return fmt.Sprintf("workflow: replay diverged at %s: %s", e.CorrelationID, e.Reason)
}

// corruptedEventLogError records replay divergence after recovery attempts fail.
type corruptedEventLogError struct {
	CorrelationID   string
	Reason          string
	RecoveryReplays int
}

func (e corruptedEventLogError) Error() string {
	return fmt.Sprintf("%s at %s after %d recovery replays: %s", worlds.ErrCorruptedEventLog, e.CorrelationID, e.RecoveryReplays, e.Reason)
}

func (e corruptedEventLogError) Unwrap() error { return worlds.ErrCorruptedEventLog }

// deploymentMismatchError records a delivery that could not reach its deployment.
type deploymentMismatchError struct {
	RunDeploymentID  string
	DeploymentID     string
	RecoveryAttempts int
}

func (e deploymentMismatchError) Error() string {
	return fmt.Sprintf("%s: run is pinned to deployment %q but was delivered to %q after %d re-routes", worlds.ErrDeploymentMismatch, e.RunDeploymentID, e.DeploymentID, e.RecoveryAttempts)
}

func (e deploymentMismatchError) Unwrap() error { return worlds.ErrDeploymentMismatch }

// is reports whether err's chain has an error of type E.
func is[E error](err error) bool {
	_, ok := errors.AsType[E](err)
	return ok
}
