package worlds

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type (
	// DurableWaitOptions holds the arguments to DurableWait. DueAt is when the
	// timer fires.
	DurableWaitOptions struct {
		Execution *Execution
		DueAt     time.Time
	}

	// DurableWaitResult is a declared timer: its ID, the queue message that
	// fires it, and any error from declaring it.
	DurableWaitResult struct {
		WaitID         WaitID
		QueueMessageID string
		Err            error
	}
)

// DurableWait declares the timer at the next position. On first execution it
// queues a delivery for DueAt and records wait_created. On replay it matches
// the recorded declaration, and a flow that reaches the declaration once the
// timer is due completes it. The timer settles when the walk reaches its
// wait_completed, which Execution.Settled reports. When the operation recorded
// at the position is not a timer, it ends the replay with a panic carrying a
// replayDivergenceError.
func DurableWait(ctx context.Context, options DurableWaitOptions) DurableWaitResult {
	position := options.Execution.NextPosition()
	creationIndex, exists := options.Execution.index[position]
	if !exists {
		// On first execution, schedule the timer, then persist the queue message
		// ID with its durable declaration so a later replay can cancel that schedule.
		waitID := NewWaitID()
		data := DataWaitCreated{Position: position, ResumeAt: options.DueAt, Cancellation: CancellationReferences(ctx)}
		queued := options.Execution.queue(QueueInput{NotBefore: &data.ResumeAt, IdempotencyKey: string(waitID)})
		data.QueueMessageID = queued.MessageID
		encoded, err := json.Marshal(data)
		if err != nil {
			_ = options.Execution.World().QueueCancel(ctx, QueueCancelInput{MessageID: queued.MessageID})
			return DurableWaitResult{WaitID: waitID, Err: err}
		}
		// A write that fails or is refused leaves the queued delivery behind.
		// Cancelling it is best-effort, since an orphan delivery only replays
		// the run once more.
		options.Execution.CreateEvent(EventsCreateEvent{EventType: EventTypeWaitCreated, CorrelationID: string(waitID), Data: encoded}, func(err error) error {
			_ = options.Execution.World().QueueCancel(ctx, QueueCancelInput{MessageID: queued.MessageID})
			return err
		})
		return DurableWaitResult{WaitID: waitID, QueueMessageID: queued.MessageID}
	}

	// Replay the declaration at this logical workflow position.
	event := options.Execution.events[creationIndex]
	if event.EventType != EventTypeWaitCreated {
		panic(options.Execution.exit(replayDivergenceError{Position: position, Reason: fmt.Sprintf("recorded %s, replay reached a wait", event.EventType)}))
	}
	var data DataWaitCreated
	if err := json.Unmarshal(event.Data, &data); err != nil {
		return DurableWaitResult{Err: fmt.Errorf("workflow: decode wait: %w", err)}
	}
	waitID := WaitID(event.CorrelationID)

	// A flow completes a due timer where it reaches its declaration, which
	// every replay does, so the timer's own delivery or any later flow
	// completes it.
	_, completed := options.Execution.settles[string(waitID)]
	if completed || ctx.Err() != nil || options.Execution.target != nil || time.Now().Before(data.ResumeAt) {
		return DurableWaitResult{WaitID: waitID, QueueMessageID: data.QueueMessageID}
	}
	// Replay reaches this declaration before later calls that disarmed it.
	// Inspect the recorded cancellation contexts before admitting a timer firing.
	for _, ref := range data.Cancellation {
		state, err := readCancellation(options.Execution.Context(), options.Execution, ref)
		if err != nil {
			panic(options.Execution.exit(deliveryError{cause: err}))
		}
		if len(state.Cause) != 0 {
			return DurableWaitResult{WaitID: waitID, QueueMessageID: data.QueueMessageID}
		}
	}
	options.Execution.CreateEvent(EventsCreateEvent{EventType: EventTypeWaitCompleted, CorrelationID: string(waitID)}, nil)
	return DurableWaitResult{WaitID: waitID, QueueMessageID: data.QueueMessageID}
}
