package worlds

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type (
	DurableWaitOptions struct {
		Execution *Execution
		Position  int
		DueAt     time.Time
		Cancel    bool
	}

	// DurableWaitResult is the replay state of one durable timer.
	DurableWaitResult struct {
		WaitID         WaitID
		QueueMessageID string
		Done           bool
		Err            error
	}
)

// DurableWait replays or creates one durable timer. It returns true only once
// the timer completion is present in the event prefix.
func DurableWait(ctx context.Context, options DurableWaitOptions) DurableWaitResult {
	creationIndex, exists := options.Execution.index[options.Position]
	if !exists {
		// First execution: schedule the timer, then persist the queue message ID
		// with its durable declaration so a later replay can cancel that schedule.
		waitID := NewWaitID()
		data := waitCreatedData{Position: options.Position, DueAt: options.DueAt, Cancel: options.Cancel}
		queued, err := options.Execution.World().Queue(ctx, QueueInput{
			RunID:          options.Execution.Run().RunID,
			NotBefore:      &data.DueAt,
			IdempotencyKey: string(waitID),
		})
		if err != nil {
			return DurableWaitResult{WaitID: waitID, Err: err}
		}
		data.QueueMessageID = queued.MessageID
		encoded, err := json.Marshal(data)
		if err != nil {
			_ = options.Execution.World().QueueCancel(ctx, QueueCancelInput{MessageID: queued.MessageID})
			return DurableWaitResult{WaitID: waitID, Err: err}
		}
		event := EventsCreateEvent{
			EventType:     EventTypeWaitCreated,
			CorrelationID: string(waitID),
			Data:          encoded,
		}
		eventCount := len(options.Execution.events)
		result, err := options.Execution.World().EventsCreate(ctx, EventsCreateInput{
			RunID:   options.Execution.Run().RunID,
			Event:   event,
			Options: EventsCreateOptions{EventCount: &eventCount},
		})
		if err != nil {
			_ = options.Execution.World().QueueCancel(ctx, QueueCancelInput{MessageID: queued.MessageID})
			return DurableWaitResult{WaitID: waitID, Err: err}
		}
		options.Execution.Merge(result, Event{
			RunID:         options.Execution.Run().RunID,
			EventID:       result.EventID,
			EventType:     event.EventType,
			CorrelationID: event.CorrelationID,
			Data:          event.Data,
		})
		return DurableWaitResult{WaitID: waitID, QueueMessageID: queued.MessageID}
	}

	// Replay the declaration at this logical workflow position.
	event := options.Execution.events[creationIndex]
	if event.EventType != EventTypeWaitCreated {
		return DurableWaitResult{Err: fmt.Errorf("workflow: replay position %d is not a wait", options.Position)}
	}
	var data waitCreatedData
	if err := json.Unmarshal(event.Data, &data); err != nil {
		return DurableWaitResult{Err: fmt.Errorf("workflow: decode wait: %w", err)}
	}
	waitID := WaitID(event.CorrelationID)

	// A recorded completion makes the wait immediately available on replay.
	for _, event := range options.Execution.events[creationIndex+1:] {
		if event.EventType == EventTypeWaitCompleted && event.CorrelationID == string(waitID) {
			return DurableWaitResult{WaitID: waitID, QueueMessageID: data.QueueMessageID, Done: true}
		}
	}

	// The scheduled delivery arrived early; keep the wait unresolved.
	if time.Now().Before(data.DueAt) {
		return DurableWaitResult{WaitID: waitID, QueueMessageID: data.QueueMessageID}
	}

	// A cancelling wait completes and cancels its run atomically.
	if data.Cancel {
		_, err := options.Execution.World().EventsCreateBatch(ctx, EventsCreateBatchInput{RunID: options.Execution.Run().RunID, Events: []EventsCreateEvent{
			{EventType: EventTypeWaitCompleted, CorrelationID: string(waitID)},
			{EventType: EventTypeRunCancelled},
		}})
		if err != nil {
			return DurableWaitResult{WaitID: waitID, Err: err}
		}
		return DurableWaitResult{WaitID: waitID, QueueMessageID: data.QueueMessageID, Err: ErrRunCancelled}
	}

	// Otherwise this root delivery records completion and continues replay.
	writeEvent := EventsCreateEvent{
		EventType:     EventTypeWaitCompleted,
		CorrelationID: string(waitID),
	}
	eventCount := len(options.Execution.events)
	result, err := options.Execution.World().EventsCreate(ctx, EventsCreateInput{
		RunID:   options.Execution.Run().RunID,
		Event:   writeEvent,
		Options: EventsCreateOptions{EventCount: &eventCount},
	})
	if err != nil {
		return DurableWaitResult{WaitID: waitID, Err: err}
	}
	options.Execution.Merge(result, Event{
		RunID:         options.Execution.Run().RunID,
		EventID:       result.EventID,
		EventType:     writeEvent.EventType,
		CorrelationID: writeEvent.CorrelationID,
	})
	return DurableWaitResult{WaitID: waitID, QueueMessageID: data.QueueMessageID, Done: true}
}

type waitCreatedData struct {
	Position       int       `json:"position"`
	DueAt          time.Time `json:"dueAt"`
	Cancel         bool      `json:"cancel"`
	QueueMessageID string    `json:"queueMessageId"`
}
