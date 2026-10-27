package worlds

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
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
	// and DurableInlineStep report only the StepID, or an Err from declaring
	// the step, and the step's result is read with StepOutcome once replay
	// delivers its settling event.
	DurableStepResult[T any] struct {
		StepID StepID
		Data   T
		Err    error
	}
)

// DurableStep declares the step at the next position. On first execution in a
// flow it holds the step back until the runner waits, when Execution.Advance
// runs it inline or records step_created with input and queues a step
// delivery, which a step delivery or compensation does at once. On replay it
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
		created := DataStepCreated{
			Position:     position,
			StepName:     options.Name,
			Input:        encoded,
			MaxRetries:   options.MaxRetries,
			Rollback:     options.Rollback,
			Cancellation: CancellationReferences(ctx),
		}
		if options.Execution.target == nil && !options.Rollback {
			// A flow holds the step back until the runner waits, and then
			// runs it inline or queues it.
			options.Execution.hold(pendingStep{
				stepID:  stepID,
				created: &created,
				limit:   MaxInlineSteps(ctx),
				run:     pendingRun(ctx, options.Execution, stepID, options.Name, options.MaxRetries, created.Cancellation, fn, input),
			})
			return DurableStepResult[Out]{StepID: stepID}
		}
		data, err := json.Marshal(created)
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
		if last.EventType != EventTypeStepStarted {
			if input.NotBefore == nil || !time.Now().Before(*input.NotBefore) {
				options.Execution.queue(input)
			}
			return DurableStepResult[Out]{StepID: stepID}
		}

		// A started step without a result is running, or its delivery
		// stopped, and that delivery's queued message is redelivered when it
		// stops.
		owner, leased := attemptOwner(options.Execution, last)
		switch {
		case owner == "" || leased:
		case owner == options.Execution.delivery.MessageID:
			// The flow that ran the step stopped, and this is its message
			// redelivered, so it runs the step again.
			var stepInput In
			if err := json.Unmarshal(createdData.Input, &stepInput); err != nil {
				return DurableStepResult[Out]{StepID: stepID, Err: fmt.Errorf("workflow: decode step input: %w", err)}
			}
			options.Execution.hold(pendingStep{
				stepID:    stepID,
				recovered: true,
				attempt:   attempts + 1,
				run:       pendingRun(ctx, options.Execution, stepID, options.Name, createdData.MaxRetries, createdData.Cancellation, fn, stepInput),
			})
		default:
			// The owning flow's lease passed, so a step delivery takes the
			// step over.
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
	stepCtx, span := startStepSpan(stepCtx, options.Execution, stepID, options.Name, attempt)
	defer span.End()
	value, callErr := invoke(stepCtx, options.Execution, true, fn, input)
	var delivery deliveryError
	if errors.As(callErr, &delivery) {
		panic(options.Execution.exit(delivery))
	}
	if callErr != nil {
		span.RecordError(callErr)
		span.SetStatus(codes.Error, callErr.Error())
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

// invoke calls fn with input in stepCtx. A panic in fn becomes its error,
// except, when inReplay is set, the panic that ends a suspended replay, which
// passes on. A cancelled stepCtx makes the error fatal, unless the delivery
// itself stopped.
func invoke[In, Out any](stepCtx context.Context, execution *Execution, inReplay bool, fn func(context.Context, In) (Out, error), input In) (value Out, err error) {
	func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				if inReplay && execution.IsSuspended() {
					panic(recovered)
				}
				var ok bool
				if err, ok = recovered.(error); !ok {
					err = fmt.Errorf("workflow: panic: %v", recovered)
				}
			}
		}()
		value, err = fn(stepCtx, input)
	}()
	if stepCtx.Err() != nil && execution.ctx.Err() == nil {
		err = FatalError{Cause: context.Canceled}
	}
	return value, err
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

// startStepSpan starts the span of a step attempt with the global
// OpenTelemetry tracer, as a child of the span in ctx. A runner that puts a
// span context in the ctx it passes to a step parents the step's span under
// it, since the step runs in the replay that delivers it.
func startStepSpan(ctx context.Context, execution *Execution, stepID StepID, name string, attempt int) (context.Context, trace.Span) {
	short := strings.TrimSuffix(name[strings.LastIndex(name, "/")+1:], "-fm")
	run := execution.Run()
	return otel.Tracer("github.com/eriicafes/go-workflow").Start(ctx, "step "+short, trace.WithAttributes(
		attribute.String("workflow.name", run.WorkflowName),
		attribute.String("workflow.run.id", string(run.RunID)),
		attribute.String("workflow.step.id", string(stepID)),
		attribute.String("workflow.step.name", name),
		attribute.Int("workflow.step.attempt", attempt),
	))
}
