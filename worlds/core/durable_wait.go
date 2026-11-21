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

// DurableWait declares the next timer. On first execution a flow holds it
// back until the runner waits, and then records wait_created and queues a
// delivery for DueAt. On replay it matches
// the recorded declaration, and a flow that reaches the declaration once the
// timer is due completes it. The timer settles when the walk reaches its
// wait_completed, which Execution.Settled reports. When the operation recorded
// with its ID is not a timer, it ends the replay with a panic carrying a
// replayDivergenceError.
func DurableWait(ctx context.Context, options DurableWaitOptions) DurableWaitResult {
	waitID := options.Execution.NextWaitID()
	creationIndex, exists := options.Execution.declared(string(waitID))
	if !exists {
		if options.Execution.target != nil {
			// A step delivery records only its own step's events.
			panic(options.Execution.exit(errReplaySuspended))
		}
		// A flow holds the timer back until the runner waits, and then
		// records it with the steps it declared and queues its delivery.
		options.Execution.holdWait(pendingWait{waitID: waitID, dueAt: options.DueAt, refs: CancellationReferences(ctx)})
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
		// A flow that waits for an inline step completes the timer when it
		// is due. A timer that came early is armed again as the flow ends.
		options.Execution.openWaits[waitID] = pendingWait{waitID: waitID, dueAt: data.ResumeAt, refs: CancellationReferences(ctx)}
		return DurableWaitResult{WaitID: waitID}
	}
	options.Execution.completeWait(pendingWait{waitID: waitID, dueAt: data.ResumeAt, refs: CancellationReferences(ctx)})
	return DurableWaitResult{WaitID: waitID}
}

// completeWait records that the due timer wait completed, unless a
// cancellation context the replay declared it in was cancelled first.
// Replay reaches a declaration before later calls that disarmed it, so the
// recorded cancellation contexts, which a World may not store, are inspected
// before the timer fires.
func (e *Execution) completeWait(wait pendingWait) {
	delete(e.openWaits, wait.waitID)
	for _, ref := range wait.refs {
		state, err := readCancellation(e.Context(), e, ref)
		if err != nil {
			panic(e.exit(deliveryError{cause: err}))
		}
		if state.Cause != nil {
			return
		}
	}
	e.armChainBefore(string(wait.waitID))
	resumeAt := wait.dueAt
	data, _ := json.Marshal(worlds.DataWaitCompleted{ResumeAt: &resumeAt})
	e.CreateEvent(worlds.EventsCreateEvent{EventType: worlds.EventTypeWaitCompleted, CorrelationID: string(wait.waitID), Data: data}, nil)
}
