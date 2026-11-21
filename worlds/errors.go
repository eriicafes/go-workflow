package worlds

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
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
	// ErrMaxDeliveriesExceeded is the error of a run or step whose delivery
	// was attempted more than MaxDeliveries times.
	ErrMaxDeliveriesExceeded = errors.New("workflow: delivery exceeded the maximum attempts")
	// ErrStreamClosed reports a write to a closed stream.
	ErrStreamClosed = errors.New("workflow: stream is closed")
	// ErrEventInvalid is returned when an event, event ID, or event cursor is
	// malformed or names an unknown event type.
	ErrEventInvalid = errors.New("workflow: event is invalid")
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
	// ErrRunExpired is returned when reading the result of a run whose data
	// was deleted.
	ErrRunExpired = errors.New("workflow: run data expired")
	// ErrThrottled is returned when a World refuses a request for its rate
	// limit. A *ThrottleError carries how long it asked to wait.
	ErrThrottled = errors.New("workflow: rate limited")
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
	CorrelationID string
	Reason        string
}

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
	return fmt.Sprintf("%s at %s after %d recovery replays: %s", ErrCorruptedEventLog, e.CorrelationID, e.RecoveryReplays, e.Reason)
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

var (
	fatalErrorTypeName     = errorTypeName(reflect.TypeFor[FatalError]())
	retryableErrorTypeName = errorTypeName(reflect.TypeFor[RetryableError]())
)

// encodeErrorNode returns err as an Error value: each error of its chain is
// an Error whose name is its Go type, with its JSON encoding as data and the
// next error as its cause. FatalError and RetryableError are encoded under
// those names.
func encodeErrorNode(err error) (any, error) {
	chain := NewError(err).chain
	var next any
	for i := len(chain) - 1; i >= 0; i-- {
		cause := chain[i]
		object := &dvObject{}
		typ := "Error"
		switch cause.Type {
		case fatalErrorTypeName:
			typ = "FatalError"
		case retryableErrorTypeName:
			typ = "RetryableError"
		default:
			object.set("name", cause.Type)
		}
		object.set("message", cause.Message)
		object.set("stack", dvUndefined{})
		if next != nil {
			object.set("cause", next)
		}
		switch {
		case typ == "RetryableError":
			var data struct{ After time.Duration }
			_ = json.Unmarshal(cause.Data, &data)
			object.set("retryAfter", dvNumber(strconv.FormatInt(time.Now().Add(data.After).UnixMilli(), 10)))
		case typ == "Error" && len(cause.Data) > 0:
			data, err := jsonNode(cause.Data)
			if err != nil {
				return nil, err
			}
			object.set("data", data)
		}
		next = dvTyped{typ, object}
	}
	return next, nil
}

// decodeErrorNode returns the Error an Error value records, reading its
// causes as its chain.
func decodeErrorNode(node any) (error, error) {
	if message, ok := node.(string); ok {
		return &Error{message: message, chain: []errorCause{{Type: "Error", Message: message}}}, nil
	}
	var chain []errorCause
	for node != nil {
		typed, ok := node.(dvTyped)
		if !ok || !isErrorType(typed.typ) {
			break
		}
		object, ok := typed.value.(*dvObject)
		if !ok {
			return nil, fmt.Errorf("%w: %s is not an object", ErrPayloadInvalid, typed.typ)
		}
		cause := errorCause{Type: typed.typ}
		if name, ok := object.get("name"); ok {
			if text, ok := name.(string); ok && text != "" {
				cause.Type = text
			}
		}
		switch typed.typ {
		case "FatalError":
			cause.Type = fatalErrorTypeName
		case "RetryableError":
			cause.Type = retryableErrorTypeName
			if at, ok := object.get("retryAfter"); ok {
				if text, ok := numberText(at); ok {
					if ms, err := strconv.ParseFloat(text, 64); err == nil {
						after := max(time.Until(time.UnixMilli(int64(ms))), 0)
						cause.Data, _ = json.Marshal(struct{ After time.Duration }{after})
					}
				}
			}
		}
		if message, ok := object.get("message"); ok {
			cause.Message, _ = message.(string)
		}
		if data, ok := object.get("data"); ok {
			encoded, err := nodeJSON(data)
			if err != nil {
				return nil, err
			}
			cause.Data = encoded
		}
		chain = append(chain, cause)
		node, _ = object.get("cause")
	}
	if len(chain) == 0 {
		return nil, fmt.Errorf("%w: cannot decode %s as an error", ErrPayloadInvalid, nodeKind(node))
	}
	return &Error{message: chain[0].Message, chain: chain}, nil
}

// HookConflictError is ErrHookConflict naming the run that holds the token.
type HookConflictError struct{ RunID RunID }

func (e *HookConflictError) Error() string {
	return fmt.Sprintf("%s by run %s", ErrHookConflict, e.RunID)
}

func (e *HookConflictError) Is(target error) bool { return target == ErrHookConflict }

// ThrottleError is ErrThrottled with how long the World asked the caller to
// wait, as a Retry-After header gives it. A zero After means it did not say.
type ThrottleError struct {
	Cause error
	After time.Duration
}

func (e *ThrottleError) Error() string {
	if e.Cause == nil {
		return ErrThrottled.Error()
	}
	return fmt.Sprintf("%s: %v", ErrThrottled, e.Cause)
}

func (e *ThrottleError) Unwrap() error { return e.Cause }

func (e *ThrottleError) Is(target error) bool { return target == ErrThrottled }

// DeliveryRetryDelay returns how long a World waits before retrying a
// delivery that failed with err: backoff, or longer when err is a
// *ThrottleError asking for more, so a retry does not run into the same rate
// limit.
func DeliveryRetryDelay(err error, backoff time.Duration) time.Duration {
	var throttle *ThrottleError
	if errors.As(err, &throttle) && throttle.After > backoff {
		return throttle.After
	}
	return backoff
}
