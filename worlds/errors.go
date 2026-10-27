package worlds

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"
)

var (
	// ErrWorkflowNotFound reports that the run's workflow is not registered
	// with the Runtime.
	ErrWorkflowNotFound = errors.New("workflow: workflow not found")
	// ErrRunNotFound is returned when no run has the requested run ID.
	ErrRunNotFound = errors.New("workflow: run not found")
	// ErrRunAlreadyExists is returned by EventsCreateRun when a run with the
	// requested run ID already exists.
	ErrRunAlreadyExists = errors.New("workflow: run already exists")
	// ErrRunTerminal reports that a run has already completed, failed, or
	// been cancelled.
	ErrRunTerminal = errors.New("workflow: run is terminal")
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
	// ErrHookNotFound reports that the requested hook could not be found.
	ErrHookNotFound = errors.New("workflow: hook not found")
	// ErrHookNotActive reports that a hook can no longer receive payloads.
	ErrHookNotActive = errors.New("workflow: hook is not active")
	// ErrHookConflict is returned when a hook token is already owned by
	// another active hook and the claim is not forced.
	ErrHookConflict = errors.New("workflow: hook token is already claimed")
	// ErrHookForceClaimed reports that another hook took over this hook's token.
	ErrHookForceClaimed = errors.New("workflow: hook token was force claimed")
	// ErrStreamLocked reports that a stream write conflicts with its writer session.
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
	// ErrInBandSuperseded is returned by an in-band write whose InBandCount
	// no longer matches the run's, because another orchestrator replaying
	// the run has written since. The writer reloads the run and replays.
	ErrInBandSuperseded = errors.New("workflow: in-band write superseded")
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

// replayDivergenceError identifies a declaration that does not match the event log.
type replayDivergenceError struct {
	Position int
	Reason   string
}

func (e replayDivergenceError) Error() string {
	return fmt.Sprintf("workflow: replay diverged at position %d: %s", e.Position, e.Reason)
}

// corruptedEventLogError records replay divergence after recovery attempts fail.
type corruptedEventLogError struct {
	Position        int
	Reason          string
	RecoveryReplays int
}

func (e corruptedEventLogError) Error() string {
	return fmt.Sprintf("%s at position %d after %d recovery replays: %s", ErrCorruptedEventLog, e.Position, e.RecoveryReplays, e.Reason)
}

func (e corruptedEventLogError) Unwrap() error { return ErrCorruptedEventLog }

// deploymentMismatchError records a delivery that could not reach its deployment.
type deploymentMismatchError struct {
	RunDeploymentID  string
	DeploymentID     string
	RecoveryAttempts int
}

func (e deploymentMismatchError) Error() string {
	return fmt.Sprintf("%s: run is pinned to deployment %q but was delivered to %q after %d re-routes", ErrDeploymentMismatch, e.RunDeploymentID, e.DeploymentID, e.RecoveryAttempts)
}

func (e deploymentMismatchError) Unwrap() error { return ErrDeploymentMismatch }

// FatalError marks an intentional failure that must not be retried.
type FatalError struct{ Cause error }

func (e FatalError) Error() string { return causeMessage(e.Cause) }
func (e FatalError) Unwrap() error { return e.Cause }

func (e *FatalError) restoreCause(cause error) {
	if e.Cause == nil {
		e.Cause = cause
	}
}

// RetryableError requests a delay before the next attempt, while retries remain.
type RetryableError struct {
	Cause error
	After time.Duration
}

func (e RetryableError) Error() string { return causeMessage(e.Cause) }
func (e RetryableError) Unwrap() error { return e.Cause }

func (e *RetryableError) restoreCause(cause error) {
	if e.Cause == nil {
		e.Cause = cause
	}
}

// causeRestorer is an error whose Cause field cannot be decoded from JSON, so
// As rebuilds it from the rest of the recorded chain.
type causeRestorer interface{ restoreCause(error) }

func causeMessage(cause error) string {
	if cause == nil {
		return "<nil>"
	}
	return cause.Error()
}

// Error is the error a failed step or run returns after it is recorded and read
// back. Its message is the original error's message, and it keeps the type and
// JSON encoding of every error in the original error's chain, so errors.Is and
// errors.As keep working across replay.
//
// errors.Is matches a target with the same type and message as an error in the
// chain, which is how sentinel errors such as one made by errors.New are
// matched. errors.As matches a target whose type is the type of an error in the
// chain, and decodes that error's exported fields from JSON into it.
type Error struct {
	message string
	chain   []errorCause
}

type errorCause struct {
	Type    string          `json:"type"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

// NewError returns an Error that records err and its chain. An Error in the
// chain contributes the chain it recorded. It returns nil when err is nil.
func NewError(err error) *Error {
	if err == nil {
		return nil
	}
	return &Error{message: err.Error(), chain: appendErrorChain(nil, err)}
}

// DecodeError decodes an Error encoded with json.Marshal. Data that is
// not an encoded Error becomes an Error whose message is data.
func DecodeError(data []byte) *Error {
	var value Error
	if err := json.Unmarshal(data, &value); err != nil || (value.message == "" && value.chain == nil) {
		return &Error{message: string(data)}
	}
	return &value
}

func encodeError(err error) []byte {
	// Encoding cannot fail, because NewError keeps only the JSON that an
	// error in the chain encoded successfully.
	data, _ := json.Marshal(NewError(err))
	return data
}

// Error returns the original error's message.
func (e *Error) Error() string { return e.message }

// Is reports whether an error in the chain has the type and message of
// target.
func (e *Error) Is(target error) bool {
	if target == nil {
		return false
	}
	name, message := errorTypeName(reflect.TypeOf(target)), target.Error()
	for _, cause := range e.chain {
		if cause.Type == name && cause.Message == message {
			return true
		}
	}
	return false
}

// As sets target to the first error in the chain whose type is the type
// target points to, decoded from its JSON encoding, and reports whether one
// was found. Fields the JSON encoding does not hold, such as unexported
// fields, keep their zero values.
func (e *Error) As(target any) bool {
	pointer := reflect.ValueOf(target)
	if pointer.Kind() != reflect.Pointer || pointer.IsNil() {
		return false
	}
	targetType := pointer.Type().Elem()
	if targetType.Kind() == reflect.Interface {
		return false
	}
	name := errorTypeName(targetType)
	for index, cause := range e.chain {
		if cause.Type != name {
			continue
		}
		value := reflect.New(targetType)
		if targetType.Kind() == reflect.Pointer {
			value.Elem().Set(reflect.New(targetType.Elem()))
		}
		if len(cause.Data) != 0 {
			var typeError *json.UnmarshalTypeError
			if err := json.Unmarshal(cause.Data, value.Interface()); err != nil && !errors.As(err, &typeError) {
				continue
			}
		}
		restorer, ok := value.Interface().(causeRestorer)
		if !ok && targetType.Kind() == reflect.Pointer {
			restorer, ok = value.Elem().Interface().(causeRestorer)
		}
		if ok && index+1 < len(e.chain) {
			rest := e.chain[index+1:]
			restorer.restoreCause(&Error{message: rest[0].Message, chain: rest})
		}
		pointer.Elem().Set(value.Elem())
		return true
	}
	return false
}

// MarshalJSON encodes the message and the chain.
func (e *Error) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Message string       `json:"message"`
		Chain   []errorCause `json:"chain,omitempty"`
	}{e.message, e.chain})
}

// UnmarshalJSON decodes an Error encoded by MarshalJSON.
func (e *Error) UnmarshalJSON(data []byte) error {
	var value struct {
		Message string       `json:"message"`
		Chain   []errorCause `json:"chain"`
	}
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	e.message, e.chain = value.Message, value.Chain
	return nil
}

func appendErrorChain(chain []errorCause, err error) []errorCause {
	if err == nil {
		return chain
	}
	if recorded, ok := err.(*Error); ok {
		return append(chain, recorded.chain...)
	}
	cause := errorCause{Type: errorTypeName(reflect.TypeOf(err)), Message: err.Error()}
	if data, marshalErr := json.Marshal(err); marshalErr == nil {
		cause.Data = data
	}
	chain = append(chain, cause)
	switch wrapped := err.(type) {
	case interface{ Unwrap() error }:
		chain = appendErrorChain(chain, wrapped.Unwrap())
	case interface{ Unwrap() []error }:
		for _, err := range wrapped.Unwrap() {
			chain = appendErrorChain(chain, err)
		}
	}
	return chain
}

func errorTypeName(t reflect.Type) string {
	prefix := ""
	for t.Kind() == reflect.Pointer {
		prefix += "*"
		t = t.Elem()
	}
	if t.PkgPath() == "" {
		return prefix + t.String()
	}
	return prefix + t.PkgPath() + "." + t.Name()
}
