package worlds

import (
	"encoding/json"
	"fmt"
	"time"
)

type (
	// DurableHookOptions holds the arguments to DurableHook. An empty Token is
	// generated on first execution and recorded. Force takes the token from a
	// hook that owns it instead of failing with ErrHookConflict.
	DurableHookOptions struct {
		Execution *Execution
		// Position is a declaration position reserved by NextPosition. When nil,
		// DurableHook declares and registers at the next position immediately.
		Position     *int
		Token        string
		Webhook      *Webhook
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
		HookID        HookID
		Token         string
		Data          T
		Err           error
		ConflictRunID RunID
		EventID       EventID
		ClaimedFrom   *HookForceClaim
		// Received reports that EventID is a delivered payload, which a
		// consumer advances past even when Data fails to decode.
		Received bool
	}
)

// DurableHook creates the hook declared at the next position, claiming its
// token, or on replay matches its recorded declaration. It does not block: the
// hook settles when the walk reaches its payload, and HookOutcome reads it.
// Registration and takeover delivery use the Execution's delivery context;
// a hook does not inherit the caller's cancellation context.
// When the operation recorded at the
// position is not a hook with the same token, it ends the replay with a panic
// carrying a replayDivergenceError.
func DurableHook[T any](options DurableHookOptions) DurableHookResult[T] {
	var position int
	if options.Position != nil {
		position = *options.Position
	} else {
		position = options.Execution.NextPosition()
	}
	creationIndex, exists := options.Execution.index[position]
	if !exists {
		if options.Token == "" {
			options.Token = NewHookToken()
		}
		hookID := NewHookID()
		created := DataHookCreated{Position: position, Token: options.Token, IsWebhook: options.Webhook != nil, IsSystem: options.System, Webhook: options.Webhook, Force: options.Force}
		if options.MinRetention > 0 {
			until := time.Now().UTC().Add(options.MinRetention)
			created.TokenRetentionUntil = &until
		}
		data, err := json.Marshal(created)
		if err != nil {
			return DurableHookResult[T]{Err: err}
		}
		options.Execution.CreateEvent(EventsCreateEvent{EventType: EventTypeHookCreated, CorrelationID: string(hookID), Data: data}, nil)
		creationIndex = options.Execution.index[position]
	}
	event := options.Execution.events[creationIndex]
	if event.EventType != EventTypeHookCreated && event.EventType != EventTypeHookConflict {
		panic(options.Execution.exit(replayDivergenceError{Position: position, Reason: fmt.Sprintf("recorded %s, replay reached a hook", event.EventType)}))
	}
	var created DataHookCreated
	if err := json.Unmarshal(event.Data, &created); err != nil {
		return DurableHookResult[T]{Err: fmt.Errorf("workflow: decode hook: %w", err)}
	}
	if options.Webhook == nil && options.Token != "" && created.Token != options.Token {
		panic(options.Execution.exit(replayDivergenceError{Position: position, Reason: fmt.Sprintf("recorded hook token %q, replay reached hook token %q", created.Token, options.Token)}))
	}
	result := DurableHookResult[T]{HookID: HookID(event.CorrelationID), Token: created.Token, ClaimedFrom: created.ForceClaimedFrom}
	if event.EventType == EventTypeHookConflict {
		result.Err, result.ConflictRunID = ErrHookConflict, created.ConflictingRunID
	}
	if created.ForceClaimedFrom != nil {
		deployment, err := RunDeploymentID(options.Execution.ctx, options.Execution.World(), created.ForceClaimedFrom.RunID)
		if err != nil {
			panic(options.Execution.exit(deliveryError{cause: err}))
		}
		_, err = options.Execution.World().Queue(options.Execution.ctx, QueueInput{RunID: created.ForceClaimedFrom.RunID, DeploymentID: deployment, IdempotencyKey: "hook-force:" + string(result.HookID)})
		if err != nil {
			panic(options.Execution.exit(deliveryError{cause: err}))
		}
	}
	return result
}

// HookEvent reports the next delivered payload or disposal after cursor.
// For webhooks, it returns incoming requests and skips hook_received events
// that record manual responses sent by the workflow.
func HookEvent(execution *Execution, id HookID, cursor EventID) (Event, bool) {
	webhook := false
	for _, event := range execution.events[:execution.horizon] {
		if event.CorrelationID != string(id) {
			continue
		}
		if event.EventType == EventTypeHookCreated {
			var created DataHookCreated
			_ = json.Unmarshal(event.Data, &created)
			webhook = created.Webhook != nil
		}
		if event.EventID <= cursor {
			continue
		}
		if event.EventType == EventTypeHookDisposed {
			return event, true
		}
		if event.EventType == EventTypeHookReceived {
			// Manual responses also use hook_received; skip them so they are
			// not delivered as another incoming webhook request.
			if received, ok := DecodeWebhookReceived(event.Data); webhook && ok && received.Response != nil {
				continue
			}
			return event, true
		}
	}
	return Event{}, false
}

// HookOutcome decodes the next delivered payload or disposal after the cursor.
// If no such event is visible to replay, EventID is empty and Err reports that
// the hook has not settled. A settled outcome includes EventID even on failure.
func HookOutcome[T any](execution *Execution, hookID HookID, after EventID) DurableHookResult[T] {
	result := DurableHookResult[T]{HookID: hookID}
	event, ok := HookEvent(execution, hookID, after)
	if !ok {
		result.Err = fmt.Errorf("workflow: hook %s has not settled", hookID)
		return result
	}
	result.EventID = event.EventID
	if event.EventType == EventTypeHookDisposed {
		var disposed HookDisposedEventData
		if len(event.Data) > 0 && json.Unmarshal(event.Data, &disposed) == nil && disposed.ForceClaimedBy != nil {
			result.Err = ErrHookForceClaimed
		} else {
			result.Err = ErrHookNotActive
		}
		return result
	}
	result.Received = true
	if err := json.Unmarshal(DecodeHookReceived(event.Data), &result.Data); err != nil {
		result.Err = fmt.Errorf("workflow: decode hook value: %w", err)
		return result
	}
	return result
}
