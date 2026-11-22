package worlds

import (
	"errors"
	"fmt"
	"time"

	"github.com/eriicafes/go-workflow/internal/sdkerror"
)

var (
	// ErrWorkflowNotRegistered reports that the run's workflow is not registered
	// with the Runtime, as the Workflow SDK's WorkflowNotRegisteredError does.
	ErrWorkflowNotRegistered = errors.New("workflow: workflow is not registered")
	// ErrRunNotFound is returned when no run has the requested run ID.
	ErrRunNotFound = errors.New("workflow: run not found")
	// ErrRunAlreadyExists is returned by EventsCreateRun when a run with the
	// requested run ID already exists.
	ErrRunAlreadyExists = errors.New("workflow: run already exists")
	// ErrRunCancelled reports that a run was cancelled.
	ErrRunCancelled = errors.New("workflow: run cancelled")
	// ErrResumeNotQueued reports that a hook payload was recorded but its run was not queued.
	ErrResumeNotQueued = errors.New("workflow: hook payload recorded but run not queued")
	// ErrStepNotFound is returned when a run has no step with the requested
	// step ID.
	ErrStepNotFound = errors.New("workflow: step not found")
	// ErrStepNotStartable reports that a step cannot be started in its current state.
	ErrStepNotStartable = errors.New("workflow: step is not startable")
	// ErrEventNotFound reports that the requested event does not exist.
	ErrEventNotFound = errors.New("workflow: event not found")
	// ErrHookNotActive reports that a hook can no longer receive payloads.
	ErrHookNotActive = errors.New("workflow: hook is not active")
	// ErrStartHookRejected is returned by EventsCreateRun for a start hook
	// candidate the World will never admit: one whose run ID is older than
	// StartHookCandidateMaxAge.
	ErrStartHookRejected = errors.New("workflow: start hook admission rejected")
	// ErrMaxDeliveriesExceeded is the error of a run or step whose delivery
	// was attempted more than MaxDeliveries times.
	ErrMaxDeliveriesExceeded = errors.New("workflow: delivery exceeded the maximum attempts")
	// ErrStreamClosed reports a write to a closed stream.
	ErrStreamClosed = errors.New("workflow: stream is closed")
	// ErrEventInvalid is returned when an event, event ID, or event cursor is
	// malformed or names an unknown event type.
	ErrEventInvalid = errors.New("workflow: event is invalid")
	// ErrEntityConflict reports an event that repeats one the run's log
	// already holds, such as a second step_created or wait_completed for
	// the same step or wait, as the Workflow SDK's EntityConflictError does.
	// What the event records is already in the log. A transition from, or
	// a creation on, a finished run is refused with it too, and then also
	// matches *RunExpiredError.
	ErrEntityConflict = errors.New("workflow: event conflicts with the run's log")
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
	// ErrStartHookOptions is the error of a hook that opens its run's start
	// hook (see Execution.OpenStartHook) with MinRetention or Force, which
	// only the start can set.
	ErrStartHookOptions = errors.New("workflow: a hook opening the run's start hook takes no MinRetention or Force; set the retention on StartOptions.Hook")
	// ErrAttributeInvalid is returned for run attributes outside the limits
	// CheckAttributes enforces.
	ErrAttributeInvalid = errors.New("workflow: invalid run attribute")
	// ErrDeleteDataInvalid is returned by RunsDeleteDataMany for a request
	// without one to MaxDeleteDataRunIDs unique run IDs.
	ErrDeleteDataInvalid = errors.New("workflow: invalid delete data request")
)

func init() {
	sdkerror.Register("FatalError",
		func(FatalError) []sdkerror.Field { return nil },
		func(e *FatalError, recorded *sdkerror.Error) {
			if e.Cause == nil {
				e.Cause = recorded.Unwrap()
			}
		})
	sdkerror.Register("RetryableError",
		func(e RetryableError) []sdkerror.Field {
			// retryAfter is milliseconds since the epoch.
			return []sdkerror.Field{sdkerror.NewField("retryAfter", time.Now().Add(e.After).UnixMilli())}
		},
		func(e *RetryableError, recorded *sdkerror.Error) {
			if e.Cause == nil {
				e.Cause = recorded.Unwrap()
			}
			var ms float64
			if recorded.Decode("retryAfter", &ms) {
				e.After = max(time.Until(time.UnixMilli(int64(ms))), 0)
			}
		})
	sdkerror.Register("HookConflictError",
		func(e *HookConflictError) []sdkerror.Field {
			fields := []sdkerror.Field{sdkerror.NewField("token", e.Token)}
			if e.RunID != "" {
				fields = append(fields, sdkerror.NewField("conflictingRunId", e.RunID))
			}
			if e.Start {
				fields = append(fields, sdkerror.NewField("kind", "start"))
			}
			return fields
		},
		func(e **HookConflictError, recorded *sdkerror.Error) {
			conflict := &HookConflictError{}
			recorded.Decode("token", &conflict.Token)
			recorded.Decode("conflictingRunId", &conflict.RunID)
			var kind string
			conflict.Start = recorded.Decode("kind", &kind) && kind == "start"
			*e = conflict
		})
	sdkerror.Register("HookForceClaimedError",
		func(e *HookForceClaimedError) []sdkerror.Field {
			fields := []sdkerror.Field{sdkerror.NewField("token", e.Token), sdkerror.NewField("claimedByRunId", e.RunID)}
			if e.HookID != "" {
				fields = append(fields, sdkerror.NewField("claimedByHookId", e.HookID))
			}
			return fields
		},
		func(e **HookForceClaimedError, recorded *sdkerror.Error) {
			claimed := &HookForceClaimedError{}
			recorded.Decode("token", &claimed.Token)
			recorded.Decode("claimedByRunId", &claimed.RunID)
			recorded.Decode("claimedByHookId", &claimed.HookID)
			*e = claimed
		})
	sdkerror.RegisterNamed("HookNotFoundError",
		func(e *HookNotFoundError) []sdkerror.Field {
			return []sdkerror.Field{sdkerror.NewField("token", e.Token)}
		},
		func(e **HookNotFoundError, recorded *sdkerror.Error) {
			notFound := &HookNotFoundError{}
			recorded.Decode("token", &notFound.Token)
			*e = notFound
		})
	// The SDK records these as Errors named after their class, without
	// their fields, which Go keeps to read back. expiredAt is a Date, which
	// a recorded field, as JSON, cannot hold, so it is not kept.
	sdkerror.RegisterNamed("RunExpiredError",
		func(e *RunExpiredError) []sdkerror.Field {
			var fields []sdkerror.Field
			if e.RunID != "" {
				fields = append(fields, sdkerror.NewField("runId", e.RunID))
			}
			if e.RunStatus != "" {
				fields = append(fields, sdkerror.NewField("runStatus", e.RunStatus))
			}
			return fields
		},
		func(e **RunExpiredError, recorded *sdkerror.Error) {
			expired := &RunExpiredError{}
			recorded.Decode("runId", &expired.RunID)
			recorded.Decode("runStatus", &expired.RunStatus)
			*e = expired
		})
	sdkerror.RegisterNamed("StreamExpiredError",
		func(e *StreamExpiredError) []sdkerror.Field {
			return []sdkerror.Field{sdkerror.NewField("runId", e.RunID), sdkerror.NewField("streamId", e.StreamID)}
		},
		func(e **StreamExpiredError, recorded *sdkerror.Error) {
			expired := &StreamExpiredError{}
			recorded.Decode("runId", &expired.RunID)
			recorded.Decode("streamId", &expired.StreamID)
			*e = expired
		})
}

// Error is the error a failed step or run returns after it is recorded and
// read back: the chain of errors the original error wrapped, recorded as
// the Workflow SDK records errors. Its message is the original error's
// message, and errors.Is and errors.As keep working across replay: Is
// matches a target with the type and message of an error in the chain, as
// sentinel errors made by errors.New are matched, and As decodes an error of
// the target's type from its JSON encoding.
type Error = sdkerror.Error

// NewError returns an Error that records err and its chain. An Error in the
// chain contributes the chain it recorded. It returns nil when err is nil.
func NewError(err error) *Error { return sdkerror.New(err) }

// FatalError marks an intentional failure that must not be retried. It is
// recorded as the Workflow SDK's FatalError.
type FatalError struct{ Cause error }

func (e FatalError) Error() string { return causeMessage(e.Cause) }
func (e FatalError) Unwrap() error { return e.Cause }

// RetryableError requests a delay before the next attempt, while retries
// remain. It is recorded as the Workflow SDK's RetryableError, with when the
// retry is due as retryAfter.
type RetryableError struct {
	Cause error
	After time.Duration
}

func (e RetryableError) Error() string { return causeMessage(e.Cause) }
func (e RetryableError) Unwrap() error { return e.Cause }

// causeMessage returns cause's message, or an empty one without a cause, as
// the SDK's FatalError and RetryableError have without a message.
func causeMessage(cause error) string {
	if cause == nil {
		return ""
	}
	return cause.Error()
}

// HookConflictError reports that another active hook holds a hook's token
// and the claim is not forced. It is recorded as the Workflow SDK's
// HookConflictError.
type HookConflictError struct {
	// Token is the hook token another run holds. Empty when the World did
	// not name it.
	Token string
	// RunID is the run that holds the token.
	RunID RunID
	// Start reports that the hook is a run's start hook, which the SDK
	// records as kind "start".
	Start bool
}

func (e *HookConflictError) Error() string {
	if e.RunID == "" {
		return "workflow: hook token is already claimed"
	}
	return fmt.Sprintf("workflow: hook token is already claimed by run %s", e.RunID)
}

// HookForceClaimedError reports that another hook took over a hook's token.
// It is recorded as the Workflow SDK's HookForceClaimedError.
type HookForceClaimedError struct {
	// Token is the hook token that was taken over.
	Token string
	// RunID and HookID are the run and hook that claimed it.
	RunID  RunID
	HookID HookID
}

func (e *HookForceClaimedError) Error() string {
	if e.RunID == "" {
		return "workflow: hook token was force claimed"
	}
	return fmt.Sprintf("workflow: hook token was force claimed by run %s", e.RunID)
}

// HookNotFoundError reports that no hook has the requested token or hook
// ID. It is recorded as an Error named HookNotFoundError, as the Workflow
// SDK records its HookNotFoundError.
type HookNotFoundError struct {
	// Token is the token looked up, or the hook ID, as the SDK's Worlds
	// name it. Empty when the World did not name it.
	Token string
}

func (e *HookNotFoundError) Error() string { return "workflow: hook not found" }

// RunExpiredError reports a run that is no longer available: it finished,
// so it takes no more events, or its data was deleted, as the Workflow SDK's
// RunExpiredError. It is recorded as an Error named RunExpiredError, as the
// SDK records it.
type RunExpiredError struct {
	// RunID is the run, when the World named it.
	RunID RunID
	// RunStatus is the run's terminal status, when the World named it.
	RunStatus Status
	// ExpiredAt is when the run's data was deleted, or zero when it was not.
	ExpiredAt time.Time
}

func (e *RunExpiredError) Error() string {
	message := "workflow: run"
	if e.RunID != "" {
		message += " " + string(e.RunID)
	}
	switch {
	case !e.ExpiredAt.IsZero():
		return message + " data expired"
	case e.RunStatus != "":
		return message + " is in terminal status " + string(e.RunStatus)
	}
	return message + " expired"
}

// StreamExpiredError reports a read of a stream whose data a World deleted.
// It is recorded as an Error named StreamExpiredError, as the Workflow SDK
// records its StreamExpiredError.
type StreamExpiredError struct {
	RunID    RunID
	StreamID string
	// ExpiredAt is when the stream's data was deleted, when the World said.
	ExpiredAt time.Time
}

func (e *StreamExpiredError) Error() string { return "workflow: stream expired" }

// RunNotSupportedError reports an event on a run created under a newer spec
// version than this release supports (SpecVersion), as the Workflow SDK's
// RunNotSupportedError does: a newer release runs it.
type RunNotSupportedError struct {
	RunSpecVersion   int
	WorldSpecVersion int
}

func (e *RunNotSupportedError) Error() string {
	return fmt.Sprintf("workflow: run requires spec version %d, but the World supports version %d", e.RunSpecVersion, e.WorldSpecVersion)
}

// ThrottleError reports that a World refused a request for its rate limit,
// with how long it asked the caller to wait, as a Retry-After header gives
// it. A zero After means it did not say.
type ThrottleError struct {
	Cause error `cbor:"-"`
	After time.Duration
}

func (e *ThrottleError) Error() string {
	if e.Cause == nil {
		return "workflow: rate limited"
	}
	return fmt.Sprintf("workflow: rate limited: %v", e.Cause)
}

func (e *ThrottleError) Unwrap() error { return e.Cause }

// TooEarlyError reports a step_started refused before the step's retry is
// due, as the Workflow SDK's TooEarlyError does.
type TooEarlyError struct {
	// RetryAfter is how long until the retry is due.
	RetryAfter time.Duration
}

func (e *TooEarlyError) Error() string {
	return fmt.Sprintf("workflow: step retry is not due yet: due in %s", e.RetryAfter.Round(time.Millisecond))
}

// DeliveryRetryDelay returns how long a World waits before retrying a
// delivery that failed with err: backoff, or longer when err is a
// *ThrottleError asking for more, so a retry does not run into the same rate
// limit.
func DeliveryRetryDelay(err error, backoff time.Duration) time.Duration {
	if throttle, ok := errors.AsType[*ThrottleError](err); ok && throttle.After > backoff {
		return throttle.After
	}
	return backoff
}

// InBandSupersededError is returned by an in-band write whose InBandCount no
// longer matches the run's, because another orchestrator replaying the run
// has written since; the writer reloads the run and replays. It holds the
// run's counters as the World read them when it refused the write:
// EventCount positions, InBandCount of them in-band.
type InBandSupersededError struct {
	EventCount  int
	InBandCount int
	Cause       error `cbor:"-"`
}

func (e *InBandSupersededError) Error() string {
	message := fmt.Sprintf("workflow: in-band write superseded: the run is at %d positions, %d in-band", e.EventCount, e.InBandCount)
	if e.Cause != nil {
		message += ": " + e.Cause.Error()
	}
	return message
}

func (e *InBandSupersededError) Unwrap() error { return e.Cause }

// IsEventRefusal reports whether err refuses an event by the transition
// rules, rather than failing to read or write it.
func IsEventRefusal(err error) bool {
	for _, target := range []error{ErrEventInvalid, ErrEntityConflict, ErrEventNotFound, ErrAttributeInvalid, ErrStepNotFound, ErrStepNotStartable, ErrHookNotActive} {
		if errors.Is(err, target) {
			return true
		}
	}
	return is[*RunNotSupportedError](err) || is[*TooEarlyError](err) || is[*RunExpiredError](err) ||
		is[*HookNotFoundError](err) || is[*HookForceClaimedError](err) || is[*HookConflictError](err)
}

// is reports whether err's chain has an error of type E.
func is[E error](err error) bool {
	_, ok := errors.AsType[E](err)
	return ok
}
