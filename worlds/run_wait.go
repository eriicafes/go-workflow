package worlds

import (
	"context"
	"encoding/json"
	"errors"
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

// DurableRunWait records a parent workflow's dependency on another run. The
// child runtime completes this wait and queues the parent when it reaches a
// terminal state.
type DurableRunWaitOptions struct {
	Execution *Execution
	Position  int
	RunID     RunID
}

type DurableRunWaitResult struct {
	Done bool
	Err  error
}

// DurableRunStart records a child workflow declaration before creating its
// run. Replays therefore return the same child identity instead of starting a
// duplicate workflow.
type DurableRunStartOptions struct {
	Execution    *Execution
	Position     int
	WorkflowName string
	Input        []byte
}

func DurableRunStart(ctx context.Context, options DurableRunStartOptions) (RunID, error) {
	creationIndex, exists := options.Execution.index[options.Position]
	var data runSpawnedData
	if !exists {
		data = runSpawnedData{Position: options.Position, RunID: NewRunID(), WorkflowName: options.WorkflowName, Input: options.Input}
		encoded, err := json.Marshal(data)
		if err != nil {
			return "", err
		}
		event := EventsCreateEvent{EventType: EventTypeRunSpawned, CorrelationID: string(data.RunID), Data: encoded}
		eventCount := len(options.Execution.events)
		result, err := options.Execution.World().EventsCreate(ctx, EventsCreateInput{RunID: options.Execution.Run().RunID, Event: event, Options: EventsCreateOptions{EventCount: &eventCount}})
		if err != nil {
			return "", err
		}
		options.Execution.Merge(result, Event{RunID: options.Execution.Run().RunID, EventID: result.EventID, EventType: event.EventType, CorrelationID: event.CorrelationID, Data: event.Data})
	} else {
		event := options.Execution.events[creationIndex]
		if event.EventType != EventTypeRunSpawned {
			return "", fmt.Errorf("workflow: replay position %d is not a child run", options.Position)
		}
		if err := json.Unmarshal(event.Data, &data); err != nil || data.RunID == "" {
			return "", fmt.Errorf("workflow: decode child run: %w", err)
		}
	}
	if _, err := options.Execution.World().RunsGet(ctx, RunsGetInput{RunID: data.RunID}); err == nil {
		return data.RunID, nil
	} else if !errors.Is(err, ErrRunNotFound) {
		return "", err
	}
	if _, err := options.Execution.World().EventsCreateRun(ctx, EventsCreateRunInput{RunID: data.RunID, WorkflowName: data.WorkflowName, Input: data.Input}); err != nil {
		return "", err
	}
	if _, err := options.Execution.World().Queue(ctx, QueueInput{RunID: data.RunID, IdempotencyKey: string(data.RunID)}); err != nil {
		return "", err
	}
	return data.RunID, nil
}

func DurableRunWait(ctx context.Context, options DurableRunWaitOptions) DurableRunWaitResult {
	creationIndex, exists := options.Execution.index[options.Position]
	if !exists {
		run, err := options.Execution.World().RunsGet(ctx, RunsGetInput{RunID: options.RunID})
		if err != nil {
			return DurableRunWaitResult{Err: err}
		}
		if run.Status.Terminal() {
			return DurableRunWaitResult{Done: true}
		}
		data, err := json.Marshal(runWaitData{Position: options.Position, RunID: options.RunID})
		if err != nil {
			return DurableRunWaitResult{Err: err}
		}
		event := EventsCreateEvent{EventType: EventTypeWaitCreated, CorrelationID: string(NewWaitID()), Data: data}
		eventCount := len(options.Execution.events)
		result, err := options.Execution.World().EventsCreate(ctx, EventsCreateInput{RunID: options.Execution.Run().RunID, Event: event, Options: EventsCreateOptions{EventCount: &eventCount}})
		if err != nil {
			return DurableRunWaitResult{Err: err}
		}
		options.Execution.Merge(result, Event{RunID: options.Execution.Run().RunID, EventID: result.EventID, EventType: event.EventType, CorrelationID: event.CorrelationID, Data: event.Data})
		return DurableRunWaitResult{}
	}

	event := options.Execution.events[creationIndex]
	if event.EventType != EventTypeWaitCreated {
		return DurableRunWaitResult{Err: fmt.Errorf("workflow: replay position %d is not a run wait", options.Position)}
	}
	var data runWaitData
	if err := json.Unmarshal(event.Data, &data); err != nil || data.RunID == "" {
		return DurableRunWaitResult{Err: fmt.Errorf("workflow: decode run wait: %w", err)}
	}
	for _, later := range options.Execution.events[creationIndex+1:] {
		if later.EventType == EventTypeWaitCompleted && later.CorrelationID == event.CorrelationID {
			return DurableRunWaitResult{Done: true}
		}
	}
	run, err := options.Execution.World().RunsGet(ctx, RunsGetInput{RunID: data.RunID})
	if err != nil {
		return DurableRunWaitResult{Err: err}
	}
	return DurableRunWaitResult{Done: run.Status.Terminal()}
}

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

type runWaitData struct {
	Position int   `json:"position"`
	RunID    RunID `json:"runId"`
}

type runSpawnedData struct {
	Position     int    `json:"position"`
	RunID        RunID  `json:"runId"`
	WorkflowName string `json:"workflowName"`
	Input        []byte `json:"input"`
}
