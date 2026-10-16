package worlds

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type (
	// DurableHookOptions holds the arguments to DurableHook. An empty Token is
	// generated on first execution and recorded. Force takes the token from a
	// hook that owns it instead of failing with ErrHookConflict.
	DurableHookOptions struct {
		Execution    *Execution
		Token        string
		Webhook      *Webhook
		MinRetention time.Duration
		Force        bool
	}

	// DurableHookResult is the state of a hook. DurableHook reports the hook's
	// declaration, or Err ErrHookConflict when creating it failed, with
	// ConflictRunID naming the run that owns the token. HookOutcome
	// reports its settlement: Data holds the payload and ReceivedEventID the
	// hook_received event, or Err is ErrHookNotActive, ErrHookForceClaimed, or
	// a decoding error.
	DurableHookResult[T any] struct {
		HookID          HookID
		Token           string
		Data            T
		Err             error
		ConflictRunID   RunID
		ReceivedEventID EventID
	}
)

// DurableHook creates the hook declared at the next position, claiming its
// token, or on replay matches its recorded declaration. It does not block: the
// hook settles when the walk reaches its payload, and HookOutcome reads it.
// When the operation recorded at the
// position is not a hook with the same token, it ends the replay with a panic
// carrying a *replayDivergenceError.
func DurableHook[T any](ctx context.Context, options DurableHookOptions) DurableHookResult[T] {
	position := options.Execution.NextPosition()
	creationIndex, exists := options.Execution.index[position]
	if !exists {
		// On first execution, persist the token that will resume this replay.
		// An empty token is generated here once, and replay reads it from the
		// recorded event.
		if options.Token == "" {
			options.Token = NewHookToken()
		}
		hookID := NewHookID()
		data, err := json.Marshal(hookCreatedData{
			Position:     position,
			Token:        options.Token,
			Webhook:      options.Webhook,
			MinRetention: options.MinRetention,
		})
		if err != nil {
			return DurableHookResult[T]{HookID: hookID, Token: options.Token, Err: err}
		}
		event := EventsCreateEvent{
			EventType:     EventTypeHookCreated,
			CorrelationID: string(hookID),
			Data:          data,
		}
		conflict := false
		options.Execution.ClaimHook(ctx, event, options.Force, func(err error) error {
			conflict = errors.Is(err, ErrHookConflict)
			if conflict {
				return nil
			}
			return err
		})
		if conflict {
			return createHookConflict[T](ctx, options, position, hookID)
		}
		return DurableHookResult[T]{HookID: hookID, Token: options.Token}
	}

	// Replay the declaration at this logical workflow position.
	event := options.Execution.events[creationIndex]
	if event.EventType == EventTypeHookConflict {
		var conflictData hookConflictData
		if err := json.Unmarshal(event.Data, &conflictData); err != nil {
			return DurableHookResult[T]{Err: fmt.Errorf("workflow: decode hook conflict: %w", err)}
		}
		if conflictData.Token != options.Token {
			panic(&replayDivergenceError{Position: position, Reason: fmt.Sprintf("recorded hook token %q, replay reached hook token %q", conflictData.Token, options.Token)})
		}
		return DurableHookResult[T]{HookID: HookID(event.CorrelationID), Token: conflictData.Token, Err: ErrHookConflict, ConflictRunID: conflictData.RunID}
	}
	if event.EventType != EventTypeHookCreated {
		panic(&replayDivergenceError{Position: position, Reason: fmt.Sprintf("recorded %s, replay reached a hook", event.EventType)})
	}
	var createdData hookCreatedData
	if err := json.Unmarshal(event.Data, &createdData); err != nil {
		return DurableHookResult[T]{Err: fmt.Errorf("workflow: decode hook: %w", err)}
	}
	if options.Webhook == nil && options.Token != "" && createdData.Token != options.Token {
		panic(&replayDivergenceError{Position: position, Reason: fmt.Sprintf("recorded hook token %q, replay reached hook token %q", createdData.Token, options.Token)})
	}
	return DurableHookResult[T]{HookID: HookID(event.CorrelationID), Token: createdData.Token}
}

func createHookConflict[T any](ctx context.Context, options DurableHookOptions, position int, hookID HookID) DurableHookResult[T] {
	owner, err := options.Execution.World().HooksGetByToken(ctx, HooksGetByTokenInput{Token: options.Token})
	if err != nil {
		// The owner may have released the token since the claim, and any other
		// error is storage: either way the queue retries the delivery, which
		// claims again.
		panic(deliveryError{cause: err})
	}
	data, err := json.Marshal(hookConflictData{
		Position: position,
		Token:    options.Token,
		RunID:    owner.RunID,
	})
	if err != nil {
		return DurableHookResult[T]{HookID: hookID, Token: options.Token, Err: err}
	}
	options.Execution.CreateEvent(ctx, EventsCreateEvent{EventType: EventTypeHookConflict, CorrelationID: string(hookID), Data: data}, nil)
	return DurableHookResult[T]{HookID: hookID, Token: options.Token, Err: ErrHookConflict, ConflictRunID: owner.RunID}
}

// HookOutcome decodes how the hook hookID settled, which must have happened
// for the walk (Execution.Settled): its first payload, or ErrHookForceClaimed
// or ErrHookNotActive when it was disposed of first.
func HookOutcome[T any](execution *Execution, hookID HookID) DurableHookResult[T] {
	result := DurableHookResult[T]{HookID: hookID}
	event, ok := execution.SettledEvent(string(hookID))
	if !ok {
		result.Err = fmt.Errorf("workflow: hook %s has not settled", hookID)
		return result
	}
	if event.EventType == EventTypeHookDisposed {
		var disposed HookDisposedEventData
		if len(event.Data) > 0 && json.Unmarshal(event.Data, &disposed) == nil && disposed.ForceClaimedBy != nil {
			result.Err = ErrHookForceClaimed
		} else {
			result.Err = ErrHookNotActive
		}
		return result
	}
	if err := json.Unmarshal(event.Data, &result.Data); err != nil {
		result.Err = fmt.Errorf("workflow: decode hook value: %w", err)
		return result
	}
	result.ReceivedEventID = event.EventID
	return result
}

type hookCreatedData struct {
	Position     int           `json:"position"`
	Token        string        `json:"token"`
	Webhook      *Webhook      `json:"webhook,omitempty"`
	MinRetention time.Duration `json:"minRetention,omitempty"`
}

type hookConflictData struct {
	Position int    `json:"position"`
	Token    string `json:"token"`
	RunID    RunID  `json:"runId"`
}
