package worlds

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type (
	// DurableWaitOptions holds the arguments to DurableWait. DueAt is when the
	// timer fires, and Cancel makes the timer cancel the run when it fires.
	DurableWaitOptions struct {
		Execution *Execution
		DueAt     time.Time
		Cancel    bool
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
// timer is due completes it, or cancels the run when Cancel is set. The timer
// settles when the walk reaches its wait_completed, which Execution.Settled
// reports. When the operation recorded at the position is not a timer, it ends
// the replay with a panic carrying a *replayDivergenceError.
func DurableWait(ctx context.Context, options DurableWaitOptions) DurableWaitResult {
	position := options.Execution.NextPosition()
	creationIndex, exists := options.Execution.index[position]
	if !exists {
		// On first execution, schedule the timer, then persist the queue message
		// ID with its durable declaration so a later replay can cancel that schedule.
		waitID := NewWaitID()
		data := waitCreatedData{Position: position, DueAt: options.DueAt, Cancel: options.Cancel}
		queued := options.Execution.queue(ctx, QueueInput{NotBefore: &data.DueAt, IdempotencyKey: string(waitID)})
		data.QueueMessageID = queued.MessageID
		encoded, err := json.Marshal(data)
		if err != nil {
			_ = options.Execution.World().QueueCancel(ctx, QueueCancelInput{MessageID: queued.MessageID})
			return DurableWaitResult{WaitID: waitID, Err: err}
		}
		// A write that fails or is refused leaves the queued delivery behind.
		// Cancelling it is best-effort, since an orphan delivery only replays
		// the run once more.
		options.Execution.CreateEvent(ctx, EventsCreateEvent{EventType: EventTypeWaitCreated, CorrelationID: string(waitID), Data: encoded}, func(err error) error {
			_ = options.Execution.World().QueueCancel(ctx, QueueCancelInput{MessageID: queued.MessageID})
			return err
		})
		return DurableWaitResult{WaitID: waitID, QueueMessageID: queued.MessageID}
	}

	// Replay the declaration at this logical workflow position.
	event := options.Execution.events[creationIndex]
	if event.EventType != EventTypeWaitCreated {
		panic(&replayDivergenceError{Position: position, Reason: fmt.Sprintf("recorded %s, replay reached a wait", event.EventType)})
	}
	var data waitCreatedData
	if err := json.Unmarshal(event.Data, &data); err != nil {
		return DurableWaitResult{Err: fmt.Errorf("workflow: decode wait: %w", err)}
	}
	waitID := WaitID(event.CorrelationID)

	// A flow completes a due timer where it reaches its declaration, which
	// every replay does, so the timer's own delivery or any later flow
	// completes it. A due Timeout cancels the run instead.
	_, completed := options.Execution.settles[string(waitID)]
	if completed || options.Execution.target != nil || time.Now().Before(data.DueAt) {
		return DurableWaitResult{WaitID: waitID, QueueMessageID: data.QueueMessageID}
	}
	if data.Cancel {
		_, err := options.Execution.World().EventsCreateBatch(ctx, EventsCreateBatchInput{RunID: options.Execution.Run().RunID, InBandCount: options.Execution.InBandCount(), Events: []EventsCreateEvent{
			{EventType: EventTypeWaitCompleted, CorrelationID: string(waitID)},
			{EventType: EventTypeRunCancelled},
		}})
		switch {
		case errors.Is(err, ErrInBandSuperseded):
			panic(errReplaySuperseded)
		case err != nil && !errors.Is(err, ErrRunTerminal):
			panic(deliveryError{cause: err})
		}
		panic(ErrRunCancelled)
	}
	options.Execution.CreateEvent(ctx, EventsCreateEvent{EventType: EventTypeWaitCompleted, CorrelationID: string(waitID)}, nil)
	return DurableWaitResult{WaitID: waitID, QueueMessageID: data.QueueMessageID}
}

type waitCreatedData struct {
	Position       int       `json:"position"`
	DueAt          time.Time `json:"dueAt"`
	Cancel         bool      `json:"cancel"`
	QueueMessageID string    `json:"queueMessageId"`
}
