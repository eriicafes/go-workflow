package worlds

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type (
	// DurableStepOptions holds the arguments to DurableStep. A failed attempt
	// is retried while the attempt number is at most MaxRetries, unless its
	// error is fatal. RetryableError delays the next attempt. Rollback marks
	// a step that runs a compensation.
	DurableStepOptions struct {
		Execution  *Execution
		Name       string
		MaxRetries int
		Rollback   bool
	}

	// DurableStepResult is the state of a step at one replay. DurableStep
	// reports only the StepID, or an Err from declaring the step, and the
	// step's result is read with StepOutcome once replay delivers its settling
	// event. DurableInlineStep reports the step's result: Data holds the output
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
// replayDivergenceError.
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
		data, err := json.Marshal(DataStepCreated{
			Position:     position,
			StepName:     options.Name,
			Input:        encoded,
			MaxRetries:   options.MaxRetries,
			Rollback:     options.Rollback,
			Cancellation: CancellationReferences(ctx),
		})
		if err != nil {
			return DurableStepResult[Out]{StepID: stepID, Err: err}
		}
		options.Execution.CreateEvent(EventsCreateEvent{EventType: EventTypeStepCreated, CorrelationID: string(stepID), Data: data}, nil)
		options.Execution.queue(QueueInput{StepID: &stepID, IdempotencyKey: string(stepID)})
		return DurableStepResult[Out]{StepID: stepID}
	}

	// Replay the declaration at this logical workflow position.
	event := options.Execution.events[creationIndex]
	if event.EventType != EventTypeStepCreated {
		panic(options.Execution.exit(replayDivergenceError{Position: position, Reason: fmt.Sprintf("recorded %s, replay reached a step", event.EventType)}))
	}
	var createdData DataStepCreated
	if err := json.Unmarshal(event.Data, &createdData); err != nil {
		return DurableStepResult[Out]{Err: fmt.Errorf("workflow: decode step: %w", err)}
	}
	if createdData.StepName != options.Name {
		panic(options.Execution.exit(replayDivergenceError{Position: position, Reason: fmt.Sprintf("recorded step %q, replay reached step %q", createdData.StepName, options.Name)}))
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
		// step ID, or the retry's key that the runtime queued it under, so a World that deduplicates drops it while an earlier
		// delivery is queued, and otherwise the step may be delivered twice. A
		// retry that is not yet due is expected to be unstarted.
		last, attempts := event, 0
		for _, later := range options.Execution.events[creationIndex+1:] {
			if later.CorrelationID == string(stepID) && (later.EventType == EventTypeStepStarted || later.EventType == EventTypeStepRetrying) {
				last = later
				if later.EventType == EventTypeStepStarted {
					attempts++
				}
			}
		}
		input := QueueInput{StepID: &stepID, IdempotencyKey: string(stepID)}
		if last.EventType == EventTypeStepRetrying {
			input.IdempotencyKey = stepRetryKey(stepID, attempts)
			var result DataStepResult
			if json.Unmarshal(last.Data, &result) == nil {
				input.NotBefore = result.RetryAfter
			}
		}
		if last.EventType != EventTypeStepStarted && (input.NotBefore == nil || !time.Now().Before(*input.NotBefore)) {
			options.Execution.queue(input)
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
	attempt := stepAttempt(options.Execution, stepID)
	ctx = context.WithValue(ctx, stepContextKey{}, StepMetadata{ID: stepID, Name: options.Name, StartedAt: time.Now(), Attempt: attempt})
	stepCtx, stop := StepCancelContext(ctx, options.Execution, createdData.Cancellation)
	defer stop()
	value, callErr := func() (value Out, err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				if options.Execution.IsSuspended() {
					panic(recovered)
				}
				var ok bool
				if err, ok = recovered.(error); !ok {
					err = fmt.Errorf("workflow: panic: %v", recovered)
				}
			}
		}()
		return fn(stepCtx, input)
	}()
	if stepCtx.Err() != nil && options.Execution.ctx.Err() == nil {
		callErr = FatalError{Cause: context.Canceled}
	}

	// Report the target result to the engine, which appends the lifecycle
	// event.
	if callErr != nil {
		result := DataStepResult{Error: encodeError(callErr), RetryAfter: stepRetryAfter(callErr, options.MaxRetries, attempt)}
		options.Execution.SetTargetResult(result)
		panic(options.Execution.exit(errReplayComplete))
	}

	output, err := json.Marshal(value)
	if err != nil {
		options.Execution.SetTargetResult(DataStepResult{Error: encodeError(err)})
	} else {
		options.Execution.SetTargetResult(DataStepResult{Result: output})
	}
	panic(options.Execution.exit(errReplayComplete))
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
			panic(options.Execution.exit(errReplaySuspended))
		}
		stepID := NewStepID()
		encoded, err := json.Marshal(input)
		if err != nil {
			return DurableStepResult[Out]{StepID: stepID, Err: fmt.Errorf("workflow: encode inline step input: %w", err)}
		}
		data, err := json.Marshal(DataStepCreated{Position: position, StepName: options.Name, Input: encoded, MaxRetries: options.MaxRetries, Cancellation: CancellationReferences(ctx)})
		if err != nil {
			return DurableStepResult[Out]{StepID: stepID, Err: err}
		}
		options.Execution.CreateEvent(EventsCreateEvent{EventType: EventTypeStepCreated, CorrelationID: string(stepID), Data: data}, nil)
		return inlineStepAttempt(ctx, options, stepID, 1, fn, input)
	}

	// Replay the declaration at this logical workflow position.
	event := options.Execution.events[creationIndex]
	if event.EventType != EventTypeStepCreated {
		panic(options.Execution.exit(replayDivergenceError{Position: position, Reason: fmt.Sprintf("recorded %s, replay reached a step", event.EventType)}))
	}
	var createdData DataStepCreated
	if err := json.Unmarshal(event.Data, &createdData); err != nil {
		return DurableStepResult[Out]{Err: fmt.Errorf("workflow: decode inline step: %w", err)}
	}
	if createdData.StepName != options.Name {
		panic(options.Execution.exit(replayDivergenceError{Position: position, Reason: fmt.Sprintf("recorded step %q, replay reached step %q", createdData.StepName, options.Name)}))
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
		var result DataStepResult
		switch event.EventType {
		case EventTypeStepCompleted:
			var value Out
			if err := json.Unmarshal(event.Data, &result); err != nil {
				return DurableStepResult[Out]{StepID: stepID, Err: fmt.Errorf("workflow: decode inline step result: %w", err)}
			}
			if err := json.Unmarshal(result.Result, &value); err != nil {
				return DurableStepResult[Out]{StepID: stepID, Err: fmt.Errorf("workflow: decode inline step output: %w", err)}
			}
			return DurableStepResult[Out]{StepID: stepID, Data: value}
		case EventTypeStepFailed:
			if err := json.Unmarshal(event.Data, &result); err != nil {
				return DurableStepResult[Out]{StepID: stepID, Err: fmt.Errorf("workflow: decode inline step result: %w", err)}
			}
			return DurableStepResult[Out]{StepID: stepID, Err: DecodeError(result.Error)}
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
		panic(options.Execution.exit(errReplaySuspended))
	}
	// A replay can reach the step before its retry is due, such as one
	// queued by a hook payload, and leaves the retry to the flow queued for
	// it.
	if retryAfter != nil && time.Now().Before(*retryAfter) {
		// The retry's own flow can arrive early when clocks disagree. It is
		// consumed by this replay, so it queues its replacement.
		key := stepRetryKey(stepID, attempts)
		if options.Execution.delivery.IdempotencyKey == key {
			options.Execution.queue(QueueInput{NotBefore: retryAfter, IdempotencyKey: key})
		}
		panic(options.Execution.exit(errReplaySuspended))
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
	options.Execution.CreateEvent(EventsCreateEvent{EventType: EventTypeStepStarted, CorrelationID: string(stepID)}, nil)
	stepCtx := context.WithValue(ctx, stepContextKey{}, StepMetadata{ID: stepID, Name: options.Name, StartedAt: time.Now(), Attempt: attempt})
	cancelCtx, stop := StepCancelContext(stepCtx, options.Execution, CancellationReferences(ctx))
	defer stop()
	value, callErr := func() (value Out, err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				if options.Execution.IsSuspended() {
					panic(recovered)
				}
				var ok bool
				if err, ok = recovered.(error); !ok {
					err = fmt.Errorf("workflow: panic: %v", recovered)
				}
			}
		}()
		return fn(cancelCtx, input)
	}()
	if cancelCtx.Err() != nil && options.Execution.ctx.Err() == nil {
		callErr = FatalError{Cause: context.Canceled}
	}
	if callErr == nil {
		output, err := json.Marshal(value)
		if err != nil {
			return inlineStepFail[Out](options.Execution, stepID, fmt.Errorf("workflow: encode inline step output: %w", err))
		}
		completed, err := json.Marshal(DataStepResult{Result: output})
		if err != nil {
			return inlineStepFail[Out](options.Execution, stepID, err)
		}
		options.Execution.CreateEvent(EventsCreateEvent{EventType: EventTypeStepCompleted, CorrelationID: string(stepID), Data: completed}, nil)
		return DurableStepResult[Out]{StepID: stepID, Data: value}
	}
	if retryAfter := stepRetryAfter(callErr, options.MaxRetries, attempt); retryAfter != nil {
		data, err := json.Marshal(DataStepResult{Error: encodeError(callErr), RetryAfter: retryAfter})
		if err != nil {
			return inlineStepFail[Out](options.Execution, stepID, err)
		}
		// The flow is queued first, so step_retrying never exists without
		// one. When recording fails after it, the flow replays the step
		// and starts another attempt.
		options.Execution.queue(QueueInput{NotBefore: retryAfter, IdempotencyKey: stepRetryKey(stepID, attempt)})
		options.Execution.CreateEvent(EventsCreateEvent{EventType: EventTypeStepRetrying, CorrelationID: string(stepID), Data: data}, nil)
		panic(options.Execution.exit(errReplaySuspended))
	}
	return inlineStepFail[Out](options.Execution, stepID, callErr)
}

// inlineStepFail records that the inline step stepID failed with cause.
func inlineStepFail[Out any](execution *Execution, stepID StepID, cause error) DurableStepResult[Out] {
	encoded := encodeError(cause)
	data, err := json.Marshal(DataStepResult{Error: encoded})
	if err != nil {
		return DurableStepResult[Out]{StepID: stepID, Err: err}
	}
	execution.CreateEvent(EventsCreateEvent{EventType: EventTypeStepFailed, CorrelationID: string(stepID), Data: data}, nil)
	return DurableStepResult[Out]{StepID: stepID, Err: DecodeError(encoded)}
}

// StepOutcome decodes the result of the step stepID, which must have settled
// for the walk (Execution.Settled). A completed step returns its output, a
// failed one its error as an *Error.
func StepOutcome[Out any](execution *Execution, stepID StepID) (Out, error) {
	var data Out
	event, ok := execution.SettledEvent(string(stepID))
	if !ok {
		return data, fmt.Errorf("workflow: step %s has not settled", stepID)
	}
	var result DataStepResult
	if err := json.Unmarshal(event.Data, &result); err != nil {
		return data, fmt.Errorf("workflow: decode step result: %w", err)
	}
	if event.EventType == EventTypeStepFailed {
		return data, DecodeError(result.Error)
	}
	if err := json.Unmarshal(result.Result, &data); err != nil {
		return data, fmt.Errorf("workflow: decode step output: %w", err)
	}
	return data, nil
}

func stepAttempt(execution *Execution, stepID StepID) int {
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

func stepRetryAfter(cause error, maxRetries int, attempt int) *time.Time {
	var fatal FatalError
	if errors.As(cause, &fatal) || attempt > maxRetries {
		return nil
	}
	var retryable RetryableError
	due := time.Now()
	if errors.As(cause, &retryable) && retryable.After > 0 {
		due = due.Add(retryable.After)
	}
	return &due
}

// stepRetryKey is the idempotency key of the delivery that runs the attempt
// after attempt of stepID, so every queue call for one retry shares it.
func stepRetryKey(stepID StepID, attempt int) string {
	return fmt.Sprintf("retry:%s:%d", stepID, attempt)
}
