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
		Name       string
		MaxRetries int
		RetryAfter func(error, int) *time.Time
		Rollback   bool
	}

	// DurableStepResult is the state of a step at one replay. DurableStep
	// reports only the StepID, or an Err from declaring the step, and the
	// step's result is read with StepOutcome once Execution.Select reaches
	// it. DurableInlineStep reports the step's result: Data holds the output
	// on success and Err holds the error.
	DurableStepResult[T any] struct {
		StepID StepID
		Data   T
		Err    error
	}

	// DurableInlineStepOptions holds the arguments to DurableInlineStep. A
	// failed attempt is retried as in DurableStepOptions.
	DurableInlineStepOptions struct {
		Execution  *Execution
		Name       string
		MaxRetries int
		RetryAfter func(error, int) *time.Time
	}
)

// DurableStep declares the step at the next position. On first execution it
// records step_created with input and queues a step delivery. On replay it
// matches the recorded declaration and returns the step's ID, whose result
// StepOutcome reads once the walk reaches it. During the step delivery for
// this step it
// calls fn with the recorded input, records the result in the Execution, and
// ends the replay with a panic the Runtime recovers, so it does not return.
// fn is executed at least once. When the operation recorded at the position is
// not a step with options.Name, it ends the replay with a panic carrying a
// *replayDivergenceError.
func DurableStep[In, Out any](ctx context.Context, options DurableStepOptions, fn func(context.Context, In) (Out, error), input In) DurableStepResult[Out] {
	position := options.Execution.NextPosition()
	creationIndex, exists := options.Execution.index[position]
	if !exists {
		// First execution persists the declaration and dispatches its target work.
		stepID := NewStepID()
		encoded, err := json.Marshal(input)
		if err != nil {
			return DurableStepResult[Out]{StepID: stepID, Err: fmt.Errorf("workflow: encode step input: %w", err)}
		}
		data, err := json.Marshal(stepCreatedData{
			Position:   position,
			StepName:   options.Name,
			Input:      encoded,
			MaxRetries: options.MaxRetries,
			Rollback:   options.Rollback,
		})
		if err != nil {
			return DurableStepResult[Out]{StepID: stepID, Err: err}
		}
		options.Execution.CreateEvent(ctx, EventsCreateEvent{EventType: EventTypeStepCreated, CorrelationID: string(stepID), Data: data}, nil)
		options.Execution.queue(ctx, QueueInput{StepID: &stepID, IdempotencyKey: string(stepID)})
		return DurableStepResult[Out]{StepID: stepID}
	}

	// Replay the declaration at this logical workflow position.
	event := options.Execution.events[creationIndex]
	if event.EventType != EventTypeStepCreated {
		panic(&replayDivergenceError{Position: position, Reason: fmt.Sprintf("recorded %s, replay reached a step", event.EventType)})
	}
	var createdData stepCreatedData
	if err := json.Unmarshal(event.Data, &createdData); err != nil {
		return DurableStepResult[Out]{Err: fmt.Errorf("workflow: decode step: %w", err)}
	}
	if createdData.StepName != options.Name {
		panic(&replayDivergenceError{Position: position, Reason: fmt.Sprintf("recorded step %q, replay reached step %q", createdData.StepName, options.Name)})
	}
	stepID := StepID(event.CorrelationID)

	// Root replays only declare steps, and the targeted delivery of a step
	// that has not settled invokes fn.
	if _, settled := options.Execution.settles[string(stepID)]; settled {
		return DurableStepResult[Out]{StepID: stepID}
	}
	if options.Execution.target == nil {
		// A delivery that recorded the step, or its retry, can stop before
		// queueing it, and its redelivery finds the step recorded. So a flow
		// queues a step that has not started since again. The key is the
		// step ID, so a World that deduplicates drops it while an earlier
		// delivery is queued, and otherwise the step may be delivered twice. A
		// retry that is not yet due is expected to be unstarted.
		last := event
		for _, later := range options.Execution.events[creationIndex+1:] {
			if later.CorrelationID == string(stepID) && (later.EventType == EventTypeStepStarted || later.EventType == EventTypeStepRetrying) {
				last = later
			}
		}
		input := QueueInput{StepID: &stepID, IdempotencyKey: string(stepID)}
		if last.EventType == EventTypeStepRetrying {
			var result stepResultData
			if json.Unmarshal(last.Data, &result) == nil {
				input.NotBefore = result.RetryAfter
			}
		}
		if last.EventType != EventTypeStepStarted && (input.NotBefore == nil || !time.Now().Before(*input.NotBefore)) {
			options.Execution.queue(ctx, input)
		}
		return DurableStepResult[Out]{StepID: stepID}
	}
	if *options.Execution.target != stepID {
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

// DurableInlineStep calls fn with input as a step while a flow replays the
// runner, without a step delivery, and returns its result at once. Like
// DurableStep, it records input in step_created, and every attempt uses the
// recorded input. It is for short work, such as reading the time or another
// run, that rarely fails.
//
// On first execution it appends step_created. Each attempt appends
// step_started, calls fn, and appends step_completed with the output. When fn
// fails and options allow a retry, it queues a flow for the retry time, then
// appends step_retrying and suspends the run, so a recorded retry always has a
// flow to run it, and the replay that reaches the step at that time calls fn
// again. Otherwise it appends step_failed and returns the error as an *Error.
// A replay that finds an attempt without a result, because the delivery that
// started it stopped, starts another. Concurrent flows cannot both record a
// result, since the in-band fence refuses the second. A step delivery never
// calls fn: it suspends at an inline step that has not settled. Later replays
// return the recorded result without calling fn. A World error while recording
// ends the replay so the queue retries the delivery.
func DurableInlineStep[In, Out any](ctx context.Context, options DurableInlineStepOptions, fn func(context.Context, In) (Out, error), input In) DurableStepResult[Out] {
	position := options.Execution.NextPosition()
	creationIndex, exists := options.Execution.index[position]
	if !exists {
		// First execution persists the declaration and calls fn.
		if options.Execution.target != nil {
			options.Execution.Suspend()
		}
		stepID := NewStepID()
		encoded, err := json.Marshal(input)
		if err != nil {
			return DurableStepResult[Out]{StepID: stepID, Err: fmt.Errorf("workflow: encode inline step input: %w", err)}
		}
		data, err := json.Marshal(stepCreatedData{Position: position, StepName: options.Name, Input: encoded, MaxRetries: options.MaxRetries, Inline: true})
		if err != nil {
			return DurableStepResult[Out]{StepID: stepID, Err: err}
		}
		options.Execution.CreateEvent(ctx, EventsCreateEvent{EventType: EventTypeStepCreated, CorrelationID: string(stepID), Data: data}, nil)
		return inlineStepAttempt(ctx, options, stepID, 1, fn, input)
	}

	// Replay the declaration at this logical workflow position.
	event := options.Execution.events[creationIndex]
	if event.EventType != EventTypeStepCreated {
		panic(&replayDivergenceError{Position: position, Reason: fmt.Sprintf("recorded %s, replay reached a step", event.EventType)})
	}
	var createdData stepCreatedData
	if err := json.Unmarshal(event.Data, &createdData); err != nil {
		return DurableStepResult[Out]{Err: fmt.Errorf("workflow: decode inline step: %w", err)}
	}
	if createdData.StepName != options.Name {
		panic(&replayDivergenceError{Position: position, Reason: fmt.Sprintf("recorded step %q, replay reached step %q", createdData.StepName, options.Name)})
	}
	stepID := StepID(event.CorrelationID)

	// A settled step returns its recorded result, and otherwise its last
	// retry decides when the next attempt runs.
	attempts := 0
	var retryAfter *time.Time
	for _, event := range options.Execution.events[creationIndex+1:] {
		if event.CorrelationID != string(stepID) {
			continue
		}
		var result stepResultData
		switch event.EventType {
		case EventTypeStepCompleted:
			var value Out
			if err := json.Unmarshal(event.Data, &result); err != nil {
				return DurableStepResult[Out]{StepID: stepID, Err: fmt.Errorf("workflow: decode inline step result: %w", err)}
			}
			if err := json.Unmarshal(result.Output, &value); err != nil {
				return DurableStepResult[Out]{StepID: stepID, Err: fmt.Errorf("workflow: decode inline step output: %w", err)}
			}
			return DurableStepResult[Out]{StepID: stepID, Data: value}
		case EventTypeStepFailed:
			if err := json.Unmarshal(event.Data, &result); err != nil {
				return DurableStepResult[Out]{StepID: stepID, Err: fmt.Errorf("workflow: decode inline step result: %w", err)}
			}
			return DurableStepResult[Out]{StepID: stepID, Err: DecodeError(result.Error)}
		case EventTypeStepCancelled:
			return DurableStepResult[Out]{StepID: stepID, Err: ErrStepCancelled}
		case EventTypeStepStarted:
			attempts++
			retryAfter = nil
		case EventTypeStepRetrying:
			if json.Unmarshal(event.Data, &result) == nil {
				retryAfter = result.RetryAfter
			}
		}
	}
	if options.Execution.target != nil {
		options.Execution.Suspend()
	}
	// A replay can reach the step before its retry is due, such as one
	// queued by a hook payload, and leaves the retry to the flow queued for
	// it.
	if retryAfter != nil && time.Now().Before(*retryAfter) {
		options.Execution.Suspend()
	}

	// Later attempts always use the input captured by step_created.
	if err := json.Unmarshal(createdData.Input, &input); err != nil {
		return DurableStepResult[Out]{StepID: stepID, Err: fmt.Errorf("workflow: decode inline step input: %w", err)}
	}
	return inlineStepAttempt(ctx, options, stepID, attempts+1, fn, input)
}

// inlineStepAttempt starts attempt of the inline step stepID, calls fn, and
// records its result, or queues a flow for a retry, records it, and suspends.
func inlineStepAttempt[In, Out any](ctx context.Context, options DurableInlineStepOptions, stepID StepID, attempt int, fn func(context.Context, In) (Out, error), input In) DurableStepResult[Out] {
	options.Execution.CreateEvent(ctx, EventsCreateEvent{EventType: EventTypeStepStarted, CorrelationID: string(stepID)}, nil)
	stepCtx := context.WithValue(ctx, stepContextKey{}, StepMetadata{ID: stepID, Name: options.Name, StartedAt: time.Now(), Attempt: attempt})
	value, callErr := fn(stepCtx, input)
	if callErr == nil {
		output, err := json.Marshal(value)
		if err != nil {
			return inlineStepFail[Out](ctx, options.Execution, stepID, fmt.Errorf("workflow: encode inline step output: %w", err))
		}
		completed, err := json.Marshal(stepResultData{Output: output})
		if err != nil {
			return inlineStepFail[Out](ctx, options.Execution, stepID, err)
		}
		options.Execution.CreateEvent(ctx, EventsCreateEvent{EventType: EventTypeStepCompleted, CorrelationID: string(stepID), Data: completed}, nil)
		return DurableStepResult[Out]{StepID: stepID, Data: value}
	}
	if options.RetryAfter != nil && attempt <= options.MaxRetries {
		if retryAfter := options.RetryAfter(callErr, attempt); retryAfter != nil {
			data, err := json.Marshal(stepResultData{Error: encodeError(callErr), RetryAfter: retryAfter})
			if err != nil {
				return inlineStepFail[Out](ctx, options.Execution, stepID, err)
			}
			// The flow is queued first, so step_retrying never exists without
			// one. When recording fails after it, the flow replays the step
			// and starts another attempt.
			options.Execution.queue(ctx, QueueInput{NotBefore: retryAfter, IdempotencyKey: fmt.Sprintf("retry:%s:%d", stepID, attempt)})
			options.Execution.CreateEvent(ctx, EventsCreateEvent{EventType: EventTypeStepRetrying, CorrelationID: string(stepID), Data: data}, nil)
			options.Execution.Suspend()
		}
	}
	return inlineStepFail[Out](ctx, options.Execution, stepID, callErr)
}

// inlineStepFail records that the inline step stepID failed with cause.
func inlineStepFail[Out any](ctx context.Context, execution *Execution, stepID StepID, cause error) DurableStepResult[Out] {
	encoded := encodeError(cause)
	data, err := json.Marshal(stepResultData{Error: encoded})
	if err != nil {
		return DurableStepResult[Out]{StepID: stepID, Err: err}
	}
	execution.CreateEvent(ctx, EventsCreateEvent{EventType: EventTypeStepFailed, CorrelationID: string(stepID), Data: data}, nil)
	return DurableStepResult[Out]{StepID: stepID, Err: DecodeError(encoded)}
}

// StepOutcome decodes the result of the step stepID, which must have settled
// for the walk (Execution.Settled). A completed step returns its output, a
// failed one its error as an *Error, and a cancelled one ErrStepCancelled.
func StepOutcome[Out any](execution *Execution, stepID StepID) (Out, error) {
	var data Out
	event, ok := execution.SettledEvent(string(stepID))
	if !ok {
		return data, fmt.Errorf("workflow: step %s has not settled", stepID)
	}
	if event.EventType == EventTypeStepCancelled {
		return data, ErrStepCancelled
	}
	var result stepResultData
	if err := json.Unmarshal(event.Data, &result); err != nil {
		return data, fmt.Errorf("workflow: decode step result: %w", err)
	}
	if event.EventType == EventTypeStepFailed {
		return data, DecodeError(result.Error)
	}
	if err := json.Unmarshal(result.Output, &data); err != nil {
		return data, fmt.Errorf("workflow: decode step output: %w", err)
	}
	return data, nil
}

type stepCreatedData struct {
	Position   int             `json:"position"`
	StepName   string          `json:"stepName"`
	Input      json.RawMessage `json:"input"`
	MaxRetries int             `json:"maxRetries"`
	Rollback   bool            `json:"rollback,omitempty"`
	// Inline marks a step whose attempts run in the flow, which has no step
	// delivery.
	Inline bool `json:"inline,omitempty"`
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
