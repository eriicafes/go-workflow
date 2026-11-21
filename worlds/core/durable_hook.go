package core

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

type (
	// DurableHookOptions holds the arguments to DurableHook. An empty Token is
	// drawn from the run's seed after the hook's ID. Force takes the token from a
	// hook that owns it instead of failing with ErrHookConflict.
	DurableHookOptions struct {
		Execution *Execution
		// ID is a hook ID drawn earlier by Execution.NextHookID. When empty,
		// DurableHook draws the next one.
		ID    worlds.HookID
		Token string
		// Metadata is the hook's metadata payload. A webhook's is its
		// WebhookMetadata.
		Metadata []byte
		// IsWebhook makes the hook a webhook, resumed by HTTP requests to its
		// URL. A webhook's token is always drawn from the run's seed.
		IsWebhook    bool
		MinRetention time.Duration
		Force        bool
		// System marks a hook the runtime creates for itself, such as a
		// cancellation hook, so observability can hide it.
		System bool
	}

	// DurableHookResult is the state of a hook. DurableHook reports the hook's
	// declaration, or Err ErrHookConflict when creating it failed, with
	// ConflictRunID naming the run that owns the token. HookOutcome
	// reports its settlement: EventID identifies the payload or disposal event,
	// Data holds a decoded payload, and Err reports disposal, takeover, or
	// decoding failures. An unsettled outcome has an empty EventID and an Err.
	DurableHookResult[T any] struct {
		HookID        worlds.HookID
		Token         string
		Data          T
		Err           error
		ConflictRunID worlds.RunID
		EventID       worlds.EventID
		ClaimedFrom   *worlds.HookForceClaim
		// Received reports that EventID is a delivered payload, which a
		// consumer advances past even when Data fails to decode.
		Received bool
	}
)

// DurableHook creates the next hook declared, claiming its
// token, or on replay matches its recorded declaration. It does not block: the
// hook settles when the walk reaches its payload, and HookOutcome reads it.
// Registration and takeover delivery use the Execution's delivery context;
// a hook does not inherit the caller's cancellation context.
// When the operation recorded with its ID
// is not a hook with the same token, it ends the replay with a panic
// carrying a replayDivergenceError.
func DurableHook[T any](options DurableHookOptions) DurableHookResult[T] {
	hookID := options.ID
	if hookID == "" {
		hookID = options.Execution.NextHookID()
	}
	if options.Token == "" {
		// Every replay draws the token, so later draws stay aligned.
		options.Token = options.Execution.NextHookToken()
	}
	creationIndex, exists := options.Execution.declared(string(hookID))
	if !exists {
		created := worlds.DataHookCreated{Token: options.Token, Metadata: options.Metadata, IsWebhook: options.IsWebhook, IsSystem: options.System, Force: options.Force}
		if options.MinRetention > 0 {
			until := options.Execution.Now().Add(options.MinRetention)
			created.TokenRetentionUntil = &until
		}
		data, err := json.Marshal(created)
		if err != nil {
			return DurableHookResult[T]{Err: err}
		}
		options.Execution.CreateEvent(worlds.EventsCreateEvent{EventType: worlds.EventTypeHookCreated, CorrelationID: string(hookID), Data: data}, nil)
		creationIndex, _ = options.Execution.declared(string(hookID))
	}
	event := options.Execution.events[creationIndex]
	if event.EventType != worlds.EventTypeHookCreated && event.EventType != worlds.EventTypeHookConflict {
		panic(options.Execution.exit(replayDivergenceError{CorrelationID: string(hookID), Reason: fmt.Sprintf("recorded %s, replay reached a hook", event.EventType)}))
	}
	var created worlds.DataHookCreated
	if err := json.Unmarshal(event.Data, &created); err != nil {
		return DurableHookResult[T]{Err: fmt.Errorf("workflow: decode hook: %w", err)}
	}
	if created.Token != options.Token {
		panic(options.Execution.exit(replayDivergenceError{CorrelationID: string(hookID), Reason: fmt.Sprintf("recorded hook token %q, replay reached hook token %q", created.Token, options.Token)}))
	}
	result := DurableHookResult[T]{HookID: worlds.HookID(event.CorrelationID), Token: created.Token, ClaimedFrom: created.ForceClaimedFrom}
	if event.EventType == worlds.EventTypeHookConflict {
		result.Err, result.ConflictRunID = worlds.ErrHookConflict, created.ConflictingRunID
	}
	// The run the token was taken from is woken to read its disposal while it
	// runs, which the World says by naming its workflow, as the Workflow
	// SDK's runtime wakes it.
	if from := created.ForceClaimedFrom; from != nil && from.RunID != options.Execution.run.RunID && from.WorkflowName != "" {
		queue, err := worlds.RunDelivery(options.Execution.ctx, options.Execution.World(), created.ForceClaimedFrom.RunID)
		if err != nil {
			panic(options.Execution.exit(deliveryError{cause: err}))
		}
		_, err = options.Execution.World().Queue(options.Execution.ctx, queue)
		if err != nil {
			panic(options.Execution.exit(deliveryError{cause: err}))
		}
	}
	return result
}

// HookEvent reports the next delivered payload or disposal after cursor. A
// hook's disposal is its last event: anything recorded for it after is
// ignored.
func HookEvent(execution *Execution, id worlds.HookID, cursor worlds.EventID) (worlds.Event, bool) {
	// A runner that waits on a hook's next payload may decide differently
	// when one arrives, and one that took the payload it looked for waits no
	// more.
	event, found := hookEvent(execution, id, cursor)
	if found {
		delete(execution.observedHooks, id)
	} else {
		execution.observedHooks[id] = true
	}
	return event, found
}

func hookEvent(execution *Execution, id worlds.HookID, cursor worlds.EventID) (worlds.Event, bool) {
	for _, event := range execution.events[:execution.horizon] {
		if event.CorrelationID != string(id) || event.EventType != worlds.EventTypeHookDisposed && event.EventType != worlds.EventTypeHookReceived {
			continue
		}
		if event.EventID > cursor {
			return event, true
		}
		if event.EventType == worlds.EventTypeHookDisposed {
			break
		}
	}
	return worlds.Event{}, false
}

// HookOutcome decodes the next delivered payload or disposal after the cursor.
// If no such event is visible to replay, EventID is empty and Err reports that
// the hook has not settled. A settled outcome includes EventID even on failure.
func HookOutcome[T any](execution *Execution, hookID worlds.HookID, after worlds.EventID) DurableHookResult[T] {
	result := DurableHookResult[T]{HookID: hookID}
	event, ok := HookEvent(execution, hookID, after)
	if !ok {
		result.Err = fmt.Errorf("workflow: hook %s has not settled", hookID)
		return result
	}
	result.EventID = event.EventID
	if event.EventType == worlds.EventTypeHookDisposed {
		var disposed worlds.HookDisposedEventData
		if len(event.Data) > 0 && json.Unmarshal(event.Data, &disposed) == nil && disposed.ForceClaimedBy != nil {
			result.Err = worlds.ErrHookForceClaimed
		} else {
			result.Err = worlds.ErrHookNotActive
		}
		return result
	}
	result.Received = true
	if err := execution.DecodePayload(worlds.DecodeHookReceived(event.Data), &result.Data); err != nil {
		result.Err = fmt.Errorf("workflow: decode hook value: %w", err)
		return result
	}
	return result
}

// DurableDisposeHook records that the run's hook hookID released its token,
// as a write of the flow, unless the log already records it. A step delivery
// reaches a disposal the flow recorded before it declared the step, so it
// records nothing.
func DurableDisposeHook(execution *Execution, hookID worlds.HookID) {
	for _, event := range execution.events {
		if event.EventType == worlds.EventTypeHookDisposed && event.CorrelationID == string(hookID) {
			return
		}
	}
	if execution.target != nil {
		return
	}
	execution.CreateEvent(worlds.EventsCreateEvent{EventType: worlds.EventTypeHookDisposed, CorrelationID: string(hookID)}, func(err error) error {
		// A run that ended, a hook that is gone, and a concurrent disposal
		// leave nothing to dispose, as the Workflow SDK's runtime skips them.
		if errors.Is(err, worlds.ErrRunTerminal) || errors.Is(err, worlds.ErrHookNotFound) || errors.Is(err, worlds.ErrHookNotActive) {
			return nil
		}
		return err
	})
}
