package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// HookOption holds the options to DefineHook and CreateHook.
type HookOption struct {
	// Token is the hook token callers pass to ResumeHook. When empty, a
	// random token is generated when the hook is created and kept across
	// replays.
	Token string
	// MinRetention keeps the token reserved for at least this long after the
	// hook settles, so no other hook can take it in the meantime.
	MinRetention time.Duration
	// Force takes the token over from another run's active hook instead of
	// failing with ErrHookConflict. Awaiting the displaced hook returns
	// ErrHookForceClaimed.
	Force bool
}

// HookDefinition is a hook declared with DefineHook but not yet created in a
// run. It records nothing until Create is called.
type HookDefinition[T any] struct {
	token        string
	minRetention time.Duration
	force        bool
}

// DefineHook declares a hook that receives a payload of type T. The last of
// options applies. Its token is fixed when DefineHook is called, so a token
// generated here is the same for every Create.
func DefineHook[T any](options ...HookOption) HookDefinition[T] {
	option := HookOption{}
	if len(options) != 0 {
		option = options[len(options)-1]
	}
	return HookDefinition[T]{token: option.Token, minRetention: option.MinRetention, force: option.Force}
}

// Create creates the hook in the run ctx belongs to and returns it. On replay
// it returns the same hook. It returns ErrNoWorkflow when ctx does not belong
// to a run and ErrHookConflict when another active hook has the token.
func (d HookDefinition[T]) Create(ctx context.Context) (*Hook[T], error) {
	hook := &Hook[T]{token: d.token, minRetention: d.minRetention, force: d.force}
	return hook, hook.create(ctx)
}

// CreateHook creates a hook that receives a payload of type T. It is
// DefineHook followed by Create.
func CreateHook[T any](ctx context.Context, options ...HookOption) (*Hook[T], error) {
	return DefineHook[T](options...).Create(ctx)
}

// Hook is a promise resolved from outside the workflow by ResumeHook with a
// payload of type T, addressed by its hook token.
type Hook[T any] struct {
	token        string
	id           worlds.HookID
	registered   bool
	webhook      *worlds.Webhook
	minRetention time.Duration
	force        bool
	received     worlds.EventID
	// err is the error creating the hook failed with.
	err error
	// conflict is the run that owns its token then.
	conflict worlds.RunID
}

// ID returns the hook ID, or "" before the hook is created.
func (h *Hook[T]) ID() string { return string(h.id) }

// Token returns the hook token.
func (h *Hook[T]) Token() string { return h.token }

func (h *Hook[T]) settlementID() (string, bool) {
	return string(h.id), h.registered && h.err == nil && h.id != ""
}

// Done reports whether the hook has settled, which means it received a
// payload, was cancelled, or failed to be created, as of the results delivered
// so far. It never suspends the run and does not return the hook's error,
// which Await returns.
func (h *Hook[T]) Done(ctx context.Context) (bool, error) {
	execution, err := h.execution(ctx)
	if err != nil {
		return false, err
	}
	return h.err != nil || execution.Settled(string(h.id)), nil
}

// Await returns the hook's payload, suspending the run until the hook is
// resumed. It returns ErrHookConflict when the hook could not be created,
// ErrHookForceClaimed when another run took over its token, and
// ErrHookNotRegistered when the hook was not created in this run.
func (h *Hook[T]) Await(ctx context.Context) (T, error) {
	var zero T
	execution, err := h.execution(ctx)
	if err != nil {
		return zero, err
	}
	if h.err != nil {
		return zero, h.err
	}
	if _, ok := execution.Select(string(h.id)); !ok {
		execution.Suspend()
	}
	outcome := worlds.HookOutcome[T](execution, h.id)
	h.received = outcome.ReceivedEventID
	return outcome.Data, outcome.Err
}

// Unwrap is like Await but panics if Await returns an error.
func (h *Hook[T]) Unwrap(ctx context.Context) T {
	value, err := h.Await(ctx)
	if err != nil {
		panic(err)
	}
	return value
}

// GetConflict reports the run whose active hook has this hook's token when
// creating the hook failed with ErrHookConflict. It returns false when there
// was no conflict.
func (h *Hook[T]) GetConflict(ctx context.Context) (worlds.RunID, bool, error) {
	if _, err := h.execution(ctx); err != nil {
		return "", false, err
	}
	return h.conflict, h.conflict != "", nil
}

// execution returns the Execution of the run ctx belongs to, for a hook
// created in it.
func (h *Hook[T]) execution(ctx context.Context) (*worlds.Execution, error) {
	execution, ok := workflowExecutionFromContext(ctx)
	if !ok {
		return nil, ErrNoWorkflow
	}
	if !h.registered {
		return nil, ErrHookNotRegistered
	}
	return execution, nil
}

func (h *Hook[T]) create(ctx context.Context) error {
	execution, ok := workflowExecutionFromContext(ctx)
	if !ok {
		return ErrNoWorkflow
	}
	result := worlds.DurableHook[T](ctx, worlds.DurableHookOptions{
		Execution:    execution,
		Token:        h.token,
		Webhook:      h.webhook,
		MinRetention: h.minRetention,
		Force:        h.force,
	})
	h.registered = true
	h.id, h.token, h.err, h.conflict = result.HookID, result.Token, result.Err, result.ConflictRunID
	return h.err
}

// Cancel disposes of the hook so it no longer accepts a payload. It returns
// ErrNoWorkflow when ctx does not belong to a run.
func (h *Hook[T]) Cancel(ctx context.Context) error {
	execution, ok := workflowExecutionFromContext(ctx)
	if !ok {
		return ErrNoWorkflow
	}
	if !h.registered || h.id == "" {
		return ErrHookNotRegistered
	}
	event := worlds.EventsCreateEvent{
		EventType:     worlds.EventTypeHookDisposed,
		CorrelationID: string(h.id),
	}
	execution.CreateEvent(ctx, event, nil)
	return nil
}

// GetHook looks up the active hook with token using the Client in ctx. It
// returns false when no active hook has the token. The returned Hook only
// reports its ID and Token, and it cannot be awaited.
func GetHook[T any](ctx context.Context, token string) (*Hook[T], bool, error) {
	client, err := clientFromContext(ctx)
	if err != nil {
		return nil, false, err
	}
	hook, err := client.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: token})
	if err != nil {
		if err == worlds.ErrHookNotFound {
			return nil, false, nil
		}
		return nil, false, err
	}
	return &Hook[T]{token: token, id: hook.HookID}, true, nil
}

// ResumeHook resumes the active hook with token, sending value as its payload
// encoded as JSON, and queues a delivery so its run resumes. It uses the
// Client in ctx and returns ErrHookNotFound when no active hook has the token.
func ResumeHook[T any](ctx context.Context, token string, value T) error {
	client, err := clientFromContext(ctx)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("workflow: encode hook payload: %w", err)
	}
	return client.HooksResume(ctx, worlds.ClientHooksResumeInput{Token: token, Data: payload})
}

// ResumeHookWithRetries is like ResumeHook but retries up to attempts times,
// with a growing delay, while it returns ErrHookNotFound. Use it when the run
// may not have created the hook yet, such as right after Start. It stops early
// when ctx is done.
func ResumeHookWithRetries[T any](ctx context.Context, token string, value T, attempts int) error {
	if attempts <= 0 {
		return ErrHookNotFound
	}
	for attempt := range attempts {
		err := ResumeHook(ctx, token, value)
		if !errors.Is(err, ErrHookNotFound) {
			return err
		}
		if attempt == attempts-1 {
			return err
		}
		timer := time.NewTimer(time.Duration(attempt+1) * 100 * time.Millisecond)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
	return ErrHookNotFound
}

// NewHookToken returns a random 21-character hook token, the kind DefineHook
// generates when HookOption.Token is empty.
func NewHookToken() string { return worlds.NewHookToken() }
