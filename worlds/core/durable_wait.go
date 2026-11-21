package core

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

type (
	// DurableWaitOptions holds the arguments to DurableWait. DueAt is when the
	// timer fires.
	DurableWaitOptions struct {
		Execution *Execution
		DueAt     time.Time
	}

	// DurableWaitResult is a declared timer: its ID and any error from
	// declaring it.
	DurableWaitResult struct {
		WaitID worlds.WaitID
		Err    error
	}
)

// DurableWait declares the next timer. On first execution it
// queues a delivery for DueAt and records wait_created. On replay it matches
// the recorded declaration, and a flow that reaches the declaration once the
// timer is due completes it. The timer settles when the walk reaches its
// wait_completed, which Execution.Settled reports. When the operation recorded
// with its ID is not a timer, it ends the replay with a panic carrying a
// replayDivergenceError.
func DurableWait(ctx context.Context, options DurableWaitOptions) DurableWaitResult {
	waitID := options.Execution.NextWaitID()
	creationIndex, exists := options.Execution.declared(string(waitID))
	if !exists {
		// On first execution, schedule the timer, then persist its
		// declaration.
		data := worlds.DataWaitCreated{ResumeAt: options.DueAt}
		options.Execution.queue(worlds.QueueInput{NotBefore: &data.ResumeAt, IdempotencyKey: string(waitID)})
		encoded, err := json.Marshal(data)
		if err != nil {
			return DurableWaitResult{WaitID: waitID, Err: err}
		}
		// A write that fails or is refused leaves the queued delivery behind.
		// Queues do not recall deliveries, and the orphan only replays the run
		// once more.
		options.Execution.CreateEvent(worlds.EventsCreateEvent{EventType: worlds.EventTypeWaitCreated, CorrelationID: string(waitID), Data: encoded}, nil)
		return DurableWaitResult{WaitID: waitID}
	}

	// Replay the recorded declaration.
	event := options.Execution.events[creationIndex]
	if event.EventType != worlds.EventTypeWaitCreated {
		panic(options.Execution.exit(replayDivergenceError{CorrelationID: string(waitID), Reason: fmt.Sprintf("recorded %s, replay reached a wait", event.EventType)}))
	}
	var data worlds.DataWaitCreated
	if err := json.Unmarshal(event.Data, &data); err != nil {
		return DurableWaitResult{Err: fmt.Errorf("workflow: decode wait: %w", err)}
	}

	// A flow completes a due timer where it reaches its declaration, which
	// every replay does, so the timer's own delivery or any later flow
	// completes it.
	_, completed := options.Execution.settles[string(waitID)]
	if completed || ctx.Err() != nil || options.Execution.target != nil {
		return DurableWaitResult{WaitID: waitID}
	}
	if time.Now().Before(data.ResumeAt) {
		// The timer's own delivery can arrive early when clocks disagree. It
		// is consumed by this replay, so it queues its replacement.
		if options.Execution.delivery.IdempotencyKey == string(waitID) {
			options.Execution.queue(worlds.QueueInput{NotBefore: &data.ResumeAt, IdempotencyKey: string(waitID)})
		}
		return DurableWaitResult{WaitID: waitID}
	}
	// Replay reaches this declaration before later calls that disarmed it.
	// Inspect the recorded cancellation contexts before admitting a timer firing.
	// The recorded cancellation contexts are the ones replay reaches the
	// declaration in, which a World may not store.
	for _, ref := range CancellationReferences(ctx) {
		state, err := readCancellation(options.Execution.Context(), options.Execution, ref)
		if err != nil {
			panic(options.Execution.exit(deliveryError{cause: err}))
		}
		if state.Cause != nil {
			return DurableWaitResult{WaitID: waitID}
		}
	}
	options.Execution.CreateEvent(worlds.EventsCreateEvent{EventType: worlds.EventTypeWaitCompleted, CorrelationID: string(waitID)}, nil)
	return DurableWaitResult{WaitID: waitID}
}
