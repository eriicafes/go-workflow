package worlds

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type (
	// DurableStepOptions holds the arguments to DurableStep. A failed attempt
	// is retried while the attempt number is at most MaxRetries and
	// RetryAfter returns a non-nil time for the next attempt. Otherwise the
	// step fails. Rollback marks a step that runs a compensation.
	DurableStepOptions struct {
		Execution  *Execution
		Position   int
		Name       string
		MaxRetries int
		RetryAfter func(error, int) *time.Time
		Rollback   bool
	}

	// DurableStepResult is the state of a step at one replay. Done reports
	// whether the step has settled. Data holds its output on success and Err
	// holds its error.
	DurableStepResult[T any] struct {
		StepID StepID
		Data   T
		Done   bool
		Err    error
	}

	// DurableInlineStepOptions holds the arguments to DurableInlineStep.
	DurableInlineStepOptions struct {
		Execution *Execution
		Position  int
		Name      string
	}
)

// DurableStep declares the step at options.Position. On first execution it
// records step_created with input and queues a step delivery. On replay it
// returns the recorded result. During the step delivery for this step it
// calls fn with the recorded input, records the result in the Execution, and
// ends the replay with a panic the Runtime recovers, so it does not return.
// fn is executed at least once.
func DurableStep[In, Out any](ctx context.Context, options DurableStepOptions, fn func(context.Context, In) (Out, error), input In) DurableStepResult[Out] {
	creationIndex, exists := options.Execution.index[options.Position]
	if !exists {
		// First execution persists the declaration and dispatches its target work.
		stepID := NewStepID()
		encoded, err := json.Marshal(input)
		if err != nil {
			return DurableStepResult[Out]{StepID: stepID, Err: fmt.Errorf("workflow: encode step input: %w", err)}
		}
		data, err := json.Marshal(stepCreatedData{
			Position:   options.Position,
			StepName:   options.Name,
			Input:      encoded,
			MaxRetries: options.MaxRetries,
			Rollback:   options.Rollback,
		})
		if err != nil {
			return DurableStepResult[Out]{StepID: stepID, Err: err}
		}
		event := EventsCreateEvent{
			EventType:     EventTypeStepCreated,
			CorrelationID: string(stepID),
			Data:          data,
		}
		eventCount := len(options.Execution.events)
		result, err := options.Execution.World().EventsCreate(ctx, EventsCreateInput{
			RunID:   options.Execution.Run().RunID,
			Event:   event,
			Options: EventsCreateOptions{EventCount: &eventCount},
		})
		if err != nil {
			return DurableStepResult[Out]{StepID: stepID, Err: err}
		}
		options.Execution.Merge(result, Event{
			RunID:         options.Execution.Run().RunID,
			EventID:       result.EventID,
			EventType:     event.EventType,
			CorrelationID: event.CorrelationID,
			Data:          event.Data,
		})
		if _, err := options.Execution.World().Queue(ctx, QueueInput{RunID: options.Execution.Run().RunID, StepID: &stepID, IdempotencyKey: string(stepID)}); err != nil {
			return DurableStepResult[Out]{StepID: stepID, Err: err}
		}
		return DurableStepResult[Out]{StepID: stepID}
	}

	// Replay the declaration at this logical workflow position.
	event := options.Execution.events[creationIndex]
	if event.EventType != EventTypeStepCreated {
		return DurableStepResult[Out]{Err: fmt.Errorf("workflow: replay position %d is not a step", options.Position)}
	}
	var createdData stepCreatedData
	if err := json.Unmarshal(event.Data, &createdData); err != nil {
		return DurableStepResult[Out]{Err: fmt.Errorf("workflow: decode step: %w", err)}
	}
	if createdData.StepName != options.Name {
		return DurableStepResult[Out]{Err: fmt.Errorf("workflow: replay step %d is %q, expected %q", options.Position, createdData.StepName, options.Name)}
	}
	stepID := StepID(event.CorrelationID)

	// A terminal event after the declaration records the step's result.
	for _, event := range options.Execution.events[creationIndex+1:] {
		if event.CorrelationID != string(stepID) {
			continue
		}
		switch event.EventType {
		case EventTypeStepCompleted:
			var result stepResultData
			if err := json.Unmarshal(event.Data, &result); err != nil {
				return DurableStepResult[Out]{StepID: stepID, Done: true, Err: fmt.Errorf("workflow: decode step result: %w", err)}
			}
			var data Out
			if err := json.Unmarshal(result.Output, &data); err != nil {
				return DurableStepResult[Out]{StepID: stepID, Done: true, Err: fmt.Errorf("workflow: decode step output: %w", err)}
			}
			return DurableStepResult[Out]{StepID: stepID, Data: data, Done: true}
		case EventTypeStepCancelled:
			return DurableStepResult[Out]{StepID: stepID, Done: true, Err: ErrStepCancelled}
		case EventTypeStepFailed:
			var result stepResultData
			if err := json.Unmarshal(event.Data, &result); err != nil {
				return DurableStepResult[Out]{StepID: stepID, Done: true, Err: fmt.Errorf("workflow: decode step result: %w", err)}
			}
			return DurableStepResult[Out]{StepID: stepID, Done: true, Err: DecodeError(result.Error)}
		}
	}

	// Root replays only declare pending steps, and their targeted delivery
	// invokes fn.
	if target, ok := options.Execution.TargetStepID(); !ok || target != stepID {
		return DurableStepResult[Out]{StepID: stepID}
	}

	// Targeted execution always uses the input captured by step_created.
	if err := json.Unmarshal(createdData.Input, &input); err != nil {
		return DurableStepResult[Out]{StepID: stepID, Err: fmt.Errorf("workflow: decode step input: %w", err)}
	}
	attempt := durableStepAttempt(options.Execution, stepID)
	ctx = context.WithValue(ctx, stepContextKey{}, StepMetadata{ID: stepID, Name: options.Name, StartedAt: time.Now(), Attempt: attempt})
	value, callErr := fn(ctx, input)

	// Report the target result to the engine, which appends the lifecycle
	// event.
	if callErr != nil {
		result := stepResultData{Error: encodeError(callErr)}
		if options.RetryAfter != nil && attempt <= options.MaxRetries {
			result.RetryAfter = options.RetryAfter(callErr, attempt)
		}
		options.Execution.SetTargetResult(result)
		panic(errReplayComplete)
	}

	output, err := json.Marshal(value)
	if err != nil {
		options.Execution.SetTargetResult(stepResultData{Error: encodeError(err)})
	} else {
		options.Execution.SetTargetResult(stepResultData{Output: output})
	}
	panic(errReplayComplete)
}

// DurableInlineStep records the result of fn as a step while a flow replays
// the runner, without a step delivery. fn cannot fail, so it is for results
// such as the current time or a generated ID. Work that can fail belongs in a
// step executed by DurableStep. On first execution it appends step_created,
// calls fn, and appends step_completed. A World error while appending ends the
// replay, and the Runtime returns it from Execute so the delivery is retried.
// A replay that finds step_created without step_completed calls fn again and
// completes the step. Later replays return the recorded result without
// calling fn.
func DurableInlineStep[Out any](ctx context.Context, options DurableInlineStepOptions, fn func(context.Context) Out) DurableStepResult[Out] {
	creationIndex, exists := options.Execution.index[options.Position]
	if !exists {
		// First execution persist the declaration, then call fn and record its result.
		stepID := NewStepID()
		created, err := json.Marshal(stepCreatedData{Position: options.Position, StepName: options.Name})
		if err != nil {
			return DurableStepResult[Out]{StepID: stepID, Err: err}
		}
		createdEvent := EventsCreateEvent{EventType: EventTypeStepCreated, CorrelationID: string(stepID), Data: created}
		eventCount := len(options.Execution.events)
		createdResult, err := options.Execution.World().EventsCreate(ctx, EventsCreateInput{
			RunID:   options.Execution.Run().RunID,
			Event:   createdEvent,
			Options: EventsCreateOptions{EventCount: &eventCount},
		})
		if err != nil {
			panic(deliveryError{cause: err})
		}
		options.Execution.Merge(createdResult, Event{
			RunID:         options.Execution.Run().RunID,
			EventID:       createdResult.EventID,
			EventType:     createdEvent.EventType,
			CorrelationID: createdEvent.CorrelationID,
			Data:          createdEvent.Data,
		})

		value := fn(ctx)
		output, err := json.Marshal(value)
		if err != nil {
			return DurableStepResult[Out]{StepID: stepID, Err: fmt.Errorf("workflow: encode inline step output: %w", err)}
		}
		completed, err := json.Marshal(stepResultData{Output: output})
		if err != nil {
			return DurableStepResult[Out]{StepID: stepID, Err: err}
		}
		completedEvent := EventsCreateEvent{EventType: EventTypeStepCompleted, CorrelationID: string(stepID), Data: completed}
		eventCount = len(options.Execution.events)
		completedResult, err := options.Execution.World().EventsCreate(ctx, EventsCreateInput{
			RunID:   options.Execution.Run().RunID,
			Event:   completedEvent,
			Options: EventsCreateOptions{EventCount: &eventCount},
		})
		if err != nil {
			panic(deliveryError{cause: err})
		}
		options.Execution.Merge(completedResult, Event{
			RunID:         options.Execution.Run().RunID,
			EventID:       completedResult.EventID,
			EventType:     completedEvent.EventType,
			CorrelationID: completedEvent.CorrelationID,
			Data:          completedEvent.Data,
		})
		return DurableStepResult[Out]{StepID: stepID, Data: value, Done: true}
	}

	// Replay validates the declaration at this position, then returns the
	// recorded result without calling fn again.
	event := options.Execution.events[creationIndex]
	if event.EventType != EventTypeStepCreated {
		return DurableStepResult[Out]{Err: fmt.Errorf("workflow: replay position %d is not a step", options.Position)}
	}
	var created stepCreatedData
	if err := json.Unmarshal(event.Data, &created); err != nil {
		return DurableStepResult[Out]{Err: fmt.Errorf("workflow: decode inline step: %w", err)}
	}
	if created.StepName != options.Name {
		return DurableStepResult[Out]{Err: fmt.Errorf("workflow: replay step %d is %q, expected %q", options.Position, created.StepName, options.Name)}
	}
	stepID := StepID(event.CorrelationID)
	for _, event := range options.Execution.events[creationIndex+1:] {
		if event.EventType != EventTypeStepCompleted || event.CorrelationID != string(stepID) {
			continue
		}
		var result stepResultData
		var value Out
		if err := json.Unmarshal(event.Data, &result); err != nil {
			return DurableStepResult[Out]{StepID: stepID, Done: true, Err: fmt.Errorf("workflow: decode inline step result: %w", err)}
		}
		if err := json.Unmarshal(result.Output, &value); err != nil {
			return DurableStepResult[Out]{StepID: stepID, Done: true, Err: fmt.Errorf("workflow: decode inline step output: %w", err)}
		}
		return DurableStepResult[Out]{StepID: stepID, Data: value, Done: true}
	}

	// The delivery that recorded step_created ended before recording the
	// result, so complete the step now.
	value := fn(ctx)
	output, err := json.Marshal(value)
	if err != nil {
		return DurableStepResult[Out]{StepID: stepID, Err: fmt.Errorf("workflow: encode inline step output: %w", err)}
	}
	completed, err := json.Marshal(stepResultData{Output: output})
	if err != nil {
		return DurableStepResult[Out]{StepID: stepID, Err: err}
	}
	completedEvent := EventsCreateEvent{EventType: EventTypeStepCompleted, CorrelationID: string(stepID), Data: completed}
	eventCount := len(options.Execution.events)
	completedResult, err := options.Execution.World().EventsCreate(ctx, EventsCreateInput{
		RunID:   options.Execution.Run().RunID,
		Event:   completedEvent,
		Options: EventsCreateOptions{EventCount: &eventCount},
	})
	if err != nil {
		panic(deliveryError{cause: err})
	}
	options.Execution.Merge(completedResult, Event{
		RunID:         options.Execution.Run().RunID,
		EventID:       completedResult.EventID,
		EventType:     completedEvent.EventType,
		CorrelationID: completedEvent.CorrelationID,
		Data:          completedEvent.Data,
	})
	return DurableStepResult[Out]{StepID: stepID, Data: value, Done: true}
}

type stepCreatedData struct {
	Position   int             `json:"position"`
	StepName   string          `json:"stepName"`
	Input      json.RawMessage `json:"input"`
	MaxRetries int             `json:"maxRetries"`
	Rollback   bool            `json:"rollback,omitempty"`
}

type stepResultData struct {
	Output     json.RawMessage `json:"output,omitempty"`
	Error      []byte          `json:"error,omitempty"`
	RetryAfter *time.Time      `json:"retryAfter,omitempty"`
}

func durableStepAttempt(execution *Execution, stepID StepID) int {
	attempt := 0
	for _, event := range execution.events {
		if event.EventType == EventTypeStepStarted && event.CorrelationID == string(stepID) {
			attempt++
		}
	}
	if attempt == 0 {
		return 1
	}
	return attempt
}
