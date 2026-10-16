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
		Position     int
		Token        string
		Webhook      *Webhook
		MinRetention time.Duration
		Force        bool
	}

	// DurableHookResult is the state of a hook at one replay. Done reports
	// whether the hook has settled. On success Data holds the payload and
	// ReceivedEventID the hook_received event. Err is ErrHookConflict (with
	// ConflictRunID naming the run that owns the token), ErrHookNotActive,
	// ErrHookForceClaimed, or a storage or decoding error.
	DurableHookResult[T any] struct {
		HookID          HookID
		Token           string
		Data            T
		Done            bool
		Err             error
		ConflictRunID   RunID
		ReceivedEventID EventID
	}
)

// DurableHook creates the hook declared at options.Position, claiming its
// token, or on replay returns its recorded state. It does not block, so the
// caller suspends while Done is false. When the operation recorded at the
// position is not a hook with the same token, it ends the replay with a panic
// carrying a *replayDivergenceError.
func DurableHook[T any](ctx context.Context, options DurableHookOptions) DurableHookResult[T] {
	creationIndex, exists := options.Execution.index[options.Position]
	if !exists {
		// On first execution, persist the token that will resume this replay.
		// An empty token is generated here once, and replay reads it from the
		// recorded event.
		if options.Token == "" {
			options.Token = NewHookToken()
		}
		hookID := NewHookID()
		data, err := json.Marshal(hookCreatedData{
			Position:     options.Position,
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
		eventCount := len(options.Execution.events)
		claim, err := options.Execution.World().HooksClaim(ctx, HooksClaimInput{
			RunID:   options.Execution.Run().RunID,
			Event:   event,
			Options: EventsCreateOptions{EventCount: &eventCount},
			Force:   options.Force,
		})
		if err != nil {
			if errors.Is(err, ErrHookConflict) {
				return createHookConflict[T](ctx, options, hookID)
			}
			return DurableHookResult[T]{HookID: hookID, Token: options.Token, Err: err}
		}
		result := claim.EventResult
		if claim.ForcedRunID != "" {
			// Wake the displaced run on its own deployment. A failed wake is
			// harmless: the run reads the takeover on its next delivery.
			if deploymentID, err := RunDeploymentID(ctx, options.Execution.World(), claim.ForcedRunID); err == nil {
				_, _ = options.Execution.World().Queue(ctx, QueueInput{RunID: claim.ForcedRunID, IdempotencyKey: "hook-force:" + string(hookID), DeploymentID: deploymentID})
			}
		}
		options.Execution.Merge(result, Event{
			RunID:         options.Execution.Run().RunID,
			EventID:       result.EventID,
			EventType:     event.EventType,
			CorrelationID: event.CorrelationID,
			Data:          event.Data,
		})
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
			panic(&replayDivergenceError{Position: options.Position, Reason: fmt.Sprintf("recorded hook token %q, replay reached hook token %q", conflictData.Token, options.Token)})
		}
		return DurableHookResult[T]{HookID: HookID(event.CorrelationID), Token: conflictData.Token, Done: true, Err: ErrHookConflict, ConflictRunID: conflictData.RunID}
	}
	if event.EventType != EventTypeHookCreated {
		panic(&replayDivergenceError{Position: options.Position, Reason: fmt.Sprintf("recorded %s, replay reached a hook", event.EventType)})
	}
	var createdData hookCreatedData
	if err := json.Unmarshal(event.Data, &createdData); err != nil {
		return DurableHookResult[T]{Err: fmt.Errorf("workflow: decode hook: %w", err)}
	}
	if options.Webhook == nil && options.Token != "" && createdData.Token != options.Token {
		panic(&replayDivergenceError{Position: options.Position, Reason: fmt.Sprintf("recorded hook token %q, replay reached hook token %q", createdData.Token, options.Token)})
	}
	hookID := HookID(event.CorrelationID)

	// A received event after the declaration supplies the payload.
	for _, event := range options.Execution.events[creationIndex+1:] {
		if event.CorrelationID != string(hookID) {
			continue
		}
		switch event.EventType {
		case EventTypeHookReceived:
			var data T
			if err := json.Unmarshal(event.Data, &data); err != nil {
				return DurableHookResult[T]{HookID: hookID, Token: createdData.Token, Done: true, Err: fmt.Errorf("workflow: decode hook value: %w", err)}
			}
			return DurableHookResult[T]{HookID: hookID, Token: createdData.Token, Data: data, Done: true, ReceivedEventID: event.EventID}
		case EventTypeHookDisposed:
			var disposed HookDisposedEventData
			if len(event.Data) > 0 && json.Unmarshal(event.Data, &disposed) == nil && disposed.ForceClaimedBy != nil {
				return DurableHookResult[T]{HookID: hookID, Token: createdData.Token, Done: true, Err: ErrHookForceClaimed}
			}
			return DurableHookResult[T]{HookID: hookID, Token: createdData.Token, Done: true, Err: ErrHookNotActive}
		}
	}
	return DurableHookResult[T]{HookID: hookID, Token: createdData.Token}
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

func createHookConflict[T any](ctx context.Context, options DurableHookOptions, hookID HookID) DurableHookResult[T] {
	owner, err := options.Execution.World().HooksGetByToken(ctx, HooksGetByTokenInput{Token: options.Token})
	if err != nil {
		return DurableHookResult[T]{HookID: hookID, Token: options.Token, Err: err}
	}
	data, err := json.Marshal(hookConflictData{
		Position: options.Position,
		Token:    options.Token,
		RunID:    owner.RunID,
	})
	if err != nil {
		return DurableHookResult[T]{HookID: hookID, Token: options.Token, Err: err}
	}
	event := EventsCreateEvent{
		EventType:     EventTypeHookConflict,
		CorrelationID: string(hookID),
		Data:          data,
	}
	eventCount := len(options.Execution.events)
	result, err := options.Execution.World().EventsCreate(ctx, EventsCreateInput{
		RunID:   options.Execution.Run().RunID,
		Event:   event,
		Options: EventsCreateOptions{EventCount: &eventCount},
	})
	if err != nil {
		return DurableHookResult[T]{HookID: hookID, Token: options.Token, Err: err}
	}
	options.Execution.Merge(result, Event{
		RunID:         options.Execution.Run().RunID,
		EventID:       result.EventID,
		EventType:     event.EventType,
		CorrelationID: event.CorrelationID,
		Data:          event.Data,
	})
	return DurableHookResult[T]{HookID: hookID, Token: options.Token, Done: true, Err: ErrHookConflict, ConflictRunID: owner.RunID}
}
