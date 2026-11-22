package core

import (
	"errors"
	"fmt"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

var (
	errReplayComplete  = errors.New("workflow: targeted replay complete")
	errReplaySuspended = errors.New("workflow: replay suspended")
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

// isoTime returns at as error messages write a time: UTC, to the millisecond.
func isoTime(at time.Time) string { return at.UTC().Format("2006-01-02T15:04:05.000Z") }

// retries returns "1 retry" or "n retries".
func retries(n int) string {
	if n == 1 {
		return "1 retry"
	}
	return fmt.Sprintf("%d retries", n)
}

// retriesExhaustedError returns the error a step fails with when its last
// allowed attempt fails with cause, which it keeps as its cause.
func retriesExhaustedError(name string, maxRetries int, cause error) error {
	return fatalError(fmt.Sprintf("Step \"%s\" failed after %s: %s", name, retries(maxRetries), cause.Error()), cause)
}

// attemptsUsedError returns the error a step fails with when every attempt
// it is allowed started and the last, attempt, which started at startedAt,
// recorded no result. A zero startedAt leaves the attempt out.
func attemptsUsedError(name string, maxRetries, attempt int, startedAt time.Time) error {
	message := fmt.Sprintf("Step \"%s\" exceeded max retries (%s)", name, retries(maxRetries))
	if !startedAt.IsZero() {
		message += fmt.Sprintf(": attempt %d started at %s and never recorded a result. The invocation running it likely crashed, ran out of memory, hit its timeout, or was stopped before it could record the result. Check that invocation's logs for the cause.", attempt, isoTime(startedAt))
	}
	return fatalError(message, nil)
}

// deliveriesExceededStepError returns the error a step fails with when its
// message was delivered deliveries times, more than MaxDeliveries, without a
// delivery starting it.
func deliveriesExceededStepError(name string, deliveries int) error {
	return fatalError(fmt.Sprintf("Step \"%s\" failed with MAX_DELIVERIES_EXCEEDED: its queue message was delivered %d times (more than %d) without the step starting", name, deliveries, MaxDeliveries), worlds.ErrMaxDeliveriesExceeded)
}

// is reports whether err's chain has an error of type E.
func is[E error](err error) bool {
	_, ok := errors.AsType[E](err)
	return ok
}
