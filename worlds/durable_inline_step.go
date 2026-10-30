package worlds

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/codes"
)

const (
	// DefaultMaxInlineSteps is how many new steps a flow runs inline at once,
	// unless WithMaxInlineSteps sets another limit.
	DefaultMaxInlineSteps = 3
	// maxInlineStepsLimit caps WithMaxInlineSteps.
	maxInlineStepsLimit = 16
	// inlineStepLease is how long an attempt a flow started inline belongs
	// to that flow's queued message, whose redelivery runs it again. A flow
	// that replays after the lease without its result takes it over.
	inlineStepLease = 15 * time.Minute
	// retryRecheckDelay is how long a retry's flow that arrived while its
	// attempt was still being recorded waits before it replays again.
	retryRecheckDelay = 50 * time.Millisecond
	// DefaultMaxInlineStepsDuration is how long a flow delivery starts new
	// steps inline, unless WithMaxInlineStepsDuration sets another duration.
	// After it, the flow queues them, so a long-running flow hands its steps
	// to other deliveries.
	DefaultMaxInlineStepsDuration = 2 * time.Minute
)

type maxInlineStepsKey struct{}

type maxInlineStepsDurationKey struct{}

// WithMaxInlineSteps returns a copy of ctx that sets how many new steps a
// flow runs inline at once. Zero runs none inline, and other
// values are clamped to 1 through 16. Steps declared with the returned ctx
// use the limit, and a ctx passed to Runtime.Execute sets it for every step
// of the delivery.
func WithMaxInlineSteps(ctx context.Context, n int) context.Context {
	if n != 0 {
		n = min(max(n, 1), maxInlineStepsLimit)
	}
	return context.WithValue(ctx, maxInlineStepsKey{}, n)
}

// MaxInlineSteps returns the limit WithMaxInlineSteps set in ctx, or
// DefaultMaxInlineSteps.
func MaxInlineSteps(ctx context.Context) int {
	if n, ok := ctx.Value(maxInlineStepsKey{}).(int); ok {
		return n
	}
	return DefaultMaxInlineSteps
}

// WithMaxInlineStepsDuration returns a copy of ctx that sets how long after a
// flow delivery starts it may start new steps inline. Zero or less starts none
// inline. Steps declared with the returned ctx use the duration, and a ctx
// passed to Runtime.Execute sets it for every step of the delivery.
func WithMaxInlineStepsDuration(ctx context.Context, d time.Duration) context.Context {
	return context.WithValue(ctx, maxInlineStepsDurationKey{}, max(d, 0))
}

// MaxInlineStepsDuration returns the duration WithMaxInlineStepsDuration set
// in ctx, or DefaultMaxInlineStepsDuration.
func MaxInlineStepsDuration(ctx context.Context) time.Duration {
	if d, ok := ctx.Value(maxInlineStepsDurationKey{}).(time.Duration); ok {
		return d
	}
	return DefaultMaxInlineStepsDuration
}

// DurableInlineStepOptions holds the arguments to DurableInlineStep. A failed
// attempt is retried as in DurableStepOptions.
type DurableInlineStepOptions struct {
	Execution  *Execution
	Name       string
	MaxRetries int
}

// DurableInlineStep declares the inline step at the next position, which runs
// fn with input in the flow, never as a step delivery. It reports the step's
// ID, whose result StepOutcome reads once the walk reaches it. When the runner
// waits with nothing left to deliver, Execution.Wait records step_created
// with input and step_started in one write, calls fn alongside the other steps
// it runs inline, records the result, and delivers it. Every attempt uses the
// recorded input.
//
// A failed attempt that may retry queues a flow for the retry time before
// step_retrying is recorded, and the replay that reaches the step at that time
// runs the next attempt. An attempt without a result belongs to the queued
// message of the flow that started it. That message's redelivery runs another
// attempt at once, and any other flow only after inlineStepLease. A step
// delivery never runs it: it suspends at an inline step that is not recorded.
// When the operation recorded at the position is not a step with
// options.Name, it ends the replay with a panic carrying a
// replayDivergenceError.
func DurableInlineStep[In, Out any](ctx context.Context, options DurableInlineStepOptions, fn func(context.Context, In) (Out, error), input In) DurableStepResult[Out] {
	position := options.Execution.NextPosition()
	creationIndex, exists := options.Execution.index[position]
	if !exists {
		if options.Execution.target != nil {
			panic(options.Execution.exit(errReplaySuspended))
		}
		stepID := NewStepID()
		encoded, err := json.Marshal(input)
		if err != nil {
			return DurableStepResult[Out]{StepID: stepID, Err: fmt.Errorf("workflow: encode inline step input: %w", err)}
		}
		created := DataStepCreated{Position: position, StepName: options.Name, Input: encoded, MaxRetries: options.MaxRetries, Cancellation: CancellationReferences(ctx)}
		options.Execution.hold(pendingStep{
			stepID:   stepID,
			created:  &created,
			explicit: true,
			attempt:  1,
			run:      pendingRun(ctx, options.Execution, stepID, options.Name, options.MaxRetries, created.Cancellation, fn, input),
		})
		return DurableStepResult[Out]{StepID: stepID}
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
	if _, settled := options.Execution.settles[string(stepID)]; settled || options.Execution.target != nil {
		return DurableStepResult[Out]{StepID: stepID}
	}

	// The last retry decides when the next attempt runs, and an attempt
	// without a result belongs to the flow that started it until its lease
	// passes.
	attempts := 0
	var retryAfter *time.Time
	var running *Event
	for _, event := range options.Execution.events[creationIndex+1:] {
		if event.CorrelationID != string(stepID) {
			continue
		}
		switch event.EventType {
		case EventTypeStepStarted:
			attempts++
			retryAfter, running = nil, &event
		case EventTypeStepRetrying:
			running = nil
			var result DataStepResult
			if json.Unmarshal(event.Data, &result) == nil {
				retryAfter = result.RetryAfter
			}
		}
	}
	if running != nil {
		// The owner queues its retry's flow before it records the retry, so
		// that flow takes the attempt over too.
		if _, leased := attemptOwner(options.Execution, *running); leased && options.Execution.delivery.IdempotencyKey != stepRetryKey(stepID, attempts) {
			return DurableStepResult[Out]{StepID: stepID}
		}
	}
	if retryAfter != nil && time.Now().Before(*retryAfter) {
		// The retry's own flow can arrive early when clocks disagree. It is
		// consumed by this replay, so it queues its replacement.
		key := stepRetryKey(stepID, attempts)
		if options.Execution.delivery.IdempotencyKey == key {
			options.Execution.queue(QueueInput{NotBefore: retryAfter, IdempotencyKey: key})
		}
		return DurableStepResult[Out]{StepID: stepID}
	}

	// Later attempts always use the input captured by step_created.
	if err := json.Unmarshal(createdData.Input, &input); err != nil {
		return DurableStepResult[Out]{StepID: stepID, Err: fmt.Errorf("workflow: decode inline step input: %w", err)}
	}
	options.Execution.hold(pendingStep{
		stepID:   stepID,
		explicit: true,
		attempt:  attempts + 1,
		run:      pendingRun(ctx, options.Execution, stepID, options.Name, options.MaxRetries, CancellationReferences(ctx), fn, input),
	})
	return DurableStepResult[Out]{StepID: stepID}
}

// attemptOwner returns the queued message that owns the started attempt, and
// whether its lease still keeps this flow from taking it over. An attempt a
// step delivery started has no owner. A start with no recorded time keeps its
// lease, since its age is unknown.
func attemptOwner(execution *Execution, started Event) (string, bool) {
	var data DataStepStarted
	_ = json.Unmarshal(started.Data, &data)
	if data.OwnerMessageID == "" || data.OwnerMessageID == execution.delivery.MessageID {
		return data.OwnerMessageID, false
	}
	return data.OwnerMessageID, started.CreatedAt.IsZero() || time.Now().Before(started.CreatedAt.Add(inlineStepLease))
}

// pendingStep is a step a flow declared, or must attempt again, that
// Execution.Wait starts once every recorded event is delivered: inline when it
// is explicit, recovered, or within its limit, and otherwise queued as a step
// delivery.
type pendingStep struct {
	stepID StepID
	// created is the step's declaration, which is not yet recorded. It is
	// nil for a step an earlier delivery recorded.
	created *DataStepCreated
	// explicit marks an inline step, which always runs inline and is
	// retried by a flow.
	explicit bool
	// recovered marks a step this flow's message started inline in a
	// delivery that stopped, which always runs inline again.
	recovered bool
	// limit is the number of steps the flow may promote to run inline.
	limit int
	// duration is how long after the delivery started the flow may promote
	// the step to run inline.
	duration time.Duration
	// attempt is the attempt that runs.
	attempt int
	// run calls the step function as attempt, and returns its result to
	// record, or a World error that stopped it.
	run func(attempt int) (DataStepResult, error)
}

type inlineOutcome struct {
	step   pendingStep
	result DataStepResult
	err    error
}

// pendingRun returns the function that calls fn with input as an attempt of
// the step stepID outside the replay, in step context.
func pendingRun[In, Out any](ctx context.Context, execution *Execution, stepID StepID, name string, maxRetries int, refs []cancellationReference, fn func(context.Context, In) (Out, error), input In) func(int) (DataStepResult, error) {
	return func(attempt int) (DataStepResult, error) {
		stepCtx := context.WithValue(ctx, stepContextKey{}, StepMetadata{ID: stepID, Name: name, StartedAt: time.Now(), Attempt: attempt})
		stepCtx, stop, err := stepCancelContext(stepCtx, execution, refs)
		if err != nil {
			return DataStepResult{}, err
		}
		defer stop()
		stepCtx, span := startStepSpan(stepCtx, execution, stepID, name, attempt)
		defer span.End()
		value, callErr := invoke(stepCtx, execution, false, fn, input)
		var delivery deliveryError
		if errors.As(callErr, &delivery) {
			return DataStepResult{}, delivery.cause
		}
		if callErr != nil {
			span.RecordError(callErr)
			span.SetStatus(codes.Error, callErr.Error())
			return DataStepResult{Error: encodeError(callErr), RetryAfter: stepRetryAfter(callErr, maxRetries, attempt)}, nil
		}
		output, err := json.Marshal(value)
		if err != nil {
			return DataStepResult{Error: encodeError(fmt.Errorf("workflow: encode step output: %w", err))}, nil
		}
		return DataStepResult{Result: output}, nil
	}
}
