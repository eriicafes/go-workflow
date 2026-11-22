package workflow

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/core"
)

// HookOption holds the options to DefineHook and CreateHook.
type HookOption struct {
	// Token is the hook token callers pass to ResumeHook. When empty, a
	// token is generated at creation and kept across replays.
	Token string
	// MinRetention reserves the token until creation time plus this duration,
	// including after run completion. Explicit Dispose releases it immediately.
	MinRetention time.Duration
	// Force takes the token over from another run's active hook instead of
	// failing with a *HookConflictError. Awaiting the displaced hook returns
	// a *HookForceClaimedError.
	Force bool
}

// HookDefinition is a hook declared with DefineHook but not yet created in a
// run. Receiving or selecting registers it; GetConflict waits only for ownership.
type HookDefinition[T any] struct {
	token        string
	minRetention time.Duration
	force        bool
}

// DefineHook declares a hook that receives a payload of type T. The last of
// options applies. An empty token is generated when the handle is created.
func DefineHook[T any](options ...HookOption) HookDefinition[T] {
	option := HookOption{}
	if len(options) != 0 {
		option = options[len(options)-1]
	}
	return HookDefinition[T]{token: option.Token, minRetention: option.MinRetention, force: option.Force}
}

// Create creates a hook handle in workflow context without claiming its token.
// Receiving or selecting this hook registers it. Use GetConflict to claim
// ownership before starting work that depends on the token. Outside workflow
// context, the handle reports ErrNoWorkflow when used.
func (d HookDefinition[T]) Create(ctx context.Context) *Hook[T] {
	execution, ok := workflowExecutionFromContext(ctx)
	if !ok {
		return &Hook[T]{err: ErrNoWorkflow}
	}
	hook := &Hook[T]{
		token:          d.token,
		minRetention:   d.minRetention,
		force:          d.force,
		executionState: execution,
		declared:       execution.NextHookID(),
	}
	if hook.token == "" {
		hook.token = execution.NextHookToken()
	}
	if id, ok := execution.OpenStartHook(hook.token); ok {
		// The run's start hook was created with the run, so nothing is
		// registered for it, and options only a write could apply are
		// refused.
		hook.id, hook.registered = id, true
		if hook.minRetention > 0 || hook.force {
			hook.err = ErrStartHookOptions
		}
	}
	return hook
}

// CreateHook creates an unregistered hook handle for payloads of type T. It is
// DefineHook followed by Create.
func CreateHook[T any](ctx context.Context, options ...HookOption) *Hook[T] {
	return DefineHook[T](options...).Create(ctx)
}

// Hook receives repeated typed payloads addressed by its token.
type Hook[T any] struct {
	token      string
	id         worlds.HookID
	registered bool
	// declared is the ID drawn for the hook when it was created, which
	// registration records.
	declared worlds.HookID
	disposed bool
	// isWebhook and metadata make the hook a webhook with its settings.
	isWebhook      bool
	metadata       []byte
	minRetention   time.Duration
	force          bool
	received       worlds.EventID
	executionState *core.Execution
	err            error
	conflict       worlds.RunID
	// ClaimedFrom identifies the previous owner of a force-claimed token.
	ClaimedFrom *worlds.HookForceClaim
}

func (h *Hook[T]) create(execution *core.Execution, token string) {
	h.registered, h.executionState = true, execution
	result := core.DurableHook[T](core.DurableHookOptions{
		Execution:    execution,
		ID:           h.declared,
		Token:        token,
		IsWebhook:    h.isWebhook,
		Metadata:     h.metadata,
		MinRetention: h.minRetention,
		Force:        h.force,
	})
	h.id, h.token, h.err, h.conflict, h.ClaimedFrom = result.HookID, result.Token, result.Err, result.ConflictRunID, result.ClaimedFrom
}

// ID returns the hook ID after registration.
func (h *Hook[T]) ID() string {
	return string(h.id)
}

// Token returns the hook token.
func (h *Hook[T]) Token() string {
	return h.token
}

func (h *Hook[T]) settlementID() (string, bool) {
	if h.executionState == nil {
		return "", false
	}
	if h.conflict != "" {
		return string(h.id), true
	}
	event, ok := core.HookEvent(h.executionState, h.id, h.received)
	return string(event.EventID), ok
}

func (h *Hook[T]) done(ctx context.Context) (bool, error, error) {
	execution, ok := workflowExecutionFromContext(ctx)
	if !ok {
		return false, nil, ErrNoWorkflow
	}
	if h.executionState != execution {
		if h.err != nil {
			return false, nil, h.err
		}
		return false, nil, ErrHookNotRegistered
	}
	if h.disposed {
		return true, ErrHookNotActive, nil
	}
	if !h.registered {
		h.create(execution, h.token)
	}
	if h.err != nil {
		// Wait until replay reaches the conflict event so Select cannot choose
		// it ahead of an earlier step result.
		if h.conflict != "" && !execution.Settled(string(h.id)) {
			return false, nil, nil
		}
		return true, h.err, nil
	}
	outcome := core.HookOutcome[T](execution, h.id, h.received)
	if outcome.EventID == "" {
		return false, nil, nil
	}
	return true, outcome.Err, nil
}

// Next waits for and consumes one payload. A disposed or displaced hook
// returns an error; payloads received before takeover are delivered first.
// It registers this hook on first use; a conflicting token returns a *HookConflictError.
func (h *Hook[T]) Next(ctx context.Context) (T, error) {
	var zero T
	if _, err := Select(ctx, h); err != nil {
		// A payload that fails to decode is still consumed, so the next call
		// moves on to the payload after it.
		if execution, ok := workflowExecutionFromContext(ctx); ok && h.executionState == execution && h.err == nil && !h.disposed {
			if outcome := core.HookOutcome[T](h.executionState, h.id, h.received); outcome.Received {
				h.received = outcome.EventID
				if event, ok := h.executionState.SettledEvent(string(outcome.EventID)); ok {
					h.executionState.AdvanceClock(event.CreatedAt)
				}
			}
		}
		return zero, err
	}
	outcome := core.HookOutcome[T](h.executionState, h.id, h.received)
	h.received = outcome.EventID
	if event, ok := h.executionState.SettledEvent(string(outcome.EventID)); ok && outcome.Received {
		h.executionState.AdvanceClock(event.CreatedAt)
	}
	return outcome.Data, outcome.Err
}

// Values yields successive payloads, stopping after an error or a range break.
// Ending iteration does not dispose the hook.
func (h *Hook[T]) Values(ctx context.Context) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		for {
			value, err := h.Next(ctx)
			if !yield(value, err) || err != nil {
				return
			}
		}
	}
}

// GetConflict registers the hook and attempts to claim its token, without
// waiting for a payload. It returns the conflicting owner, or nil on success.
// Repeated calls return the recorded result without attempting another claim.
func (h *Hook[T]) GetConflict(ctx context.Context) (*Run[any], error) {
	execution, ok := workflowExecutionFromContext(ctx)
	if !ok {
		return nil, ErrNoWorkflow
	}
	if h.executionState != execution {
		if h.err != nil {
			return nil, h.err
		}
		return nil, ErrHookNotRegistered
	}
	if h.disposed {
		return nil, ErrHookNotActive
	}
	if !h.registered {
		h.create(execution, h.token)
	}
	if h.conflict == "" {
		return nil, h.err
	}
	return Get[any](ctx, string(h.conflict))
}

// Dispose durably deactivates the hook and releases its token.
// It may be deferred and panics on failure.
func (h *Hook[T]) Dispose(ctx context.Context) {
	execution, ok := workflowExecutionFromContext(ctx)
	if !ok {
		panic(ErrNoWorkflow)
	}
	if h.executionState != execution {
		if h.err != nil {
			panic(h.err)
		}
		panic(ErrHookNotRegistered)
	}
	if execution.IsSuspended() || h.disposed {
		return
	}
	if !h.registered {
		h.disposed = true
		return
	}
	if h.err != nil {
		return
	}
	core.DurableDisposeHook(execution, h.id)
	h.disposed = true
}

// GetHook looks up the active hook with token using the Client in ctx. It
// returns false when no active hook has the token. The returned Hook only
// reports identity and claim metadata, and it cannot be awaited. It is for
// steps and external callers; workflow context returns ErrNotInStep.
func GetHook[T any](ctx context.Context, token string) (*Hook[T], bool, error) {
	if _, ok := workflowExecutionFromContext(ctx); ok {
		return nil, false, ErrNotInStep
	}
	client, err := clientFromContext(ctx)
	if err != nil {
		return nil, false, err
	}
	hook, err := client.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: token})
	if err != nil {
		if _, ok := errors.AsType[*worlds.HookNotFoundError](err); ok {
			return nil, false, nil
		}
		return nil, false, err
	}
	return &Hook[T]{token: token, id: hook.HookID, ClaimedFrom: hook.ClaimedFrom}, true, nil
}

// ResumeHook resumes the active hook with token, sending value as its
// payload, and queues a delivery so its run resumes. The payload is sealed
// to the run's public key when its payloads are encrypted, so the caller
// cannot read the run back. It uses the Client in ctx and returns
// a *HookNotFoundError when no active hook has the token.
// It is for steps and external callers; workflow context returns ErrNotInStep.
func ResumeHook[T any](ctx context.Context, token string, value T) error {
	return resumeHook(ctx, token, value, worlds.NewResumeID())
}

// resumeHook resumes the hook with token as one logical resume, resumeID.
func resumeHook[T any](ctx context.Context, token string, value T, resumeID string) error {
	if _, ok := workflowExecutionFromContext(ctx); ok {
		return ErrNotInStep
	}
	client, err := clientFromContext(ctx)
	if err != nil {
		return err
	}
	hook, err := client.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: token})
	if err != nil {
		return err
	}
	options, err := worlds.HookPayloadOptions(ctx, client, hook)
	if err != nil {
		return err
	}
	payload, err := worlds.EncodePayload(value, options)
	if err != nil {
		return fmt.Errorf("workflow: encode hook payload: %w", err)
	}
	return client.HooksResume(ctx, worlds.ClientHooksResumeInput{Token: token, Data: payload, ResumeID: resumeID})
}

// ResumeHookWithRetries attempts ResumeHook up to attempts times, waiting
// interval between attempts while it returns a *HookNotFoundError. The first attempt
// is immediate. Use it when a run may not have registered its hook yet, such as
// right after Start. It stops when ctx is done and rejects workflow context
// with ErrNotInStep.
func ResumeHookWithRetries[T any](ctx context.Context, token string, value T, attempts int, interval time.Duration) error {
	if _, ok := workflowExecutionFromContext(ctx); ok {
		return ErrNotInStep
	}
	if attempts <= 0 {
		return &HookNotFoundError{Token: token}
	}
	// Every attempt is the same resume, so one that recorded its payload
	// without answering records nothing more.
	resumeID := worlds.NewResumeID()
	for attempt := range attempts {
		err := resumeHook(ctx, token, value, resumeID)
		if _, ok := errors.AsType[*HookNotFoundError](err); !ok {
			return err
		}
		if attempt == attempts-1 {
			return err
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
	return &HookNotFoundError{Token: token}
}

// NewHookToken returns a random 21-character hook token. DefineHook generates
// replay-stable tokens when HookOption.Token is empty.
func NewHookToken() string { return worlds.NewHookToken() }
