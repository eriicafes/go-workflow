package core

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
	"go.opentelemetry.io/otel/codes"
)

const (
	// DefaultMaxInlineSteps is how many new steps a flow runs inline at once,
	// unless WithMaxInlineSteps sets another limit.
	DefaultMaxInlineSteps = 3
	// maxInlineStepsLimit caps WithMaxInlineSteps.
	maxInlineStepsLimit = 16
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

// DurableInlineStep declares the next inline step, which is created inline
// whatever the flow's limit, and otherwise behaves as DurableStep: the flow
// that creates it runs its first attempt, a flow that finds an attempt
// without a result runs another, and a retry is handed to a step delivery.
func DurableInlineStep[In, Out any](ctx context.Context, options DurableInlineStepOptions, fn func(context.Context, In) (Out, error), input In) DurableStepResult[Out] {
	return durableStep(ctx, DurableStepOptions{Execution: options.Execution, Name: options.Name, MaxRetries: options.MaxRetries}, true, fn, input)
}

// pendingStep is a step a flow declared, or must attempt again, that
// Execution.Wait creates or starts once every recorded event is delivered:
// inline when it is explicit, recovered, or within its limit, and otherwise
// as a step delivery.
type pendingStep struct {
	stepID worlds.StepID
	name   string
	// input is the step's encoded arguments, for a step not yet recorded.
	input []byte
	// failure is why the step's input did not encode. The step is recorded
	// failed.
	failure error
	// explicit marks an inline step, which is created inline whatever the
	// limit.
	explicit bool
	// recovered marks an inline step whose attempt started in a delivery
	// that stopped, which runs again as attempt.
	recovered bool
	// exhausted marks a recovered step whose attempts all started, which
	// fails without another.
	exhausted bool
	// limit is the number of steps the flow may run inline at once.
	limit int
	// duration is how long after the delivery started the flow may create
	// the step inline.
	duration time.Duration
	// attempt is the attempt that runs, of at most maxRetries+1.
	attempt    int
	maxRetries int
	// run calls the step function as attempt, and returns its result to
	// record, or a World error that stopped it.
	run func(attempt int) (worlds.DataStepResult, error)
}

// pendingWait is a timer a flow declared, which Execution.Wait records once
// every recorded event is delivered.
type pendingWait struct {
	waitID worlds.WaitID
	dueAt  time.Time
	// refs are the cancellation contexts the replay declared the timer in.
	refs []cancellationReference
}

type inlineOutcome struct {
	step   pendingStep
	result worlds.DataStepResult
	err    error
}

// pendingRun returns the function that calls fn with input as an attempt of
// the step stepID outside the replay, in step context.
func pendingRun[In, Out any](ctx context.Context, execution *Execution, stepID worlds.StepID, name string, maxRetries int, refs []cancellationReference, fn func(context.Context, In) (Out, error), input In) func(int) (worlds.DataStepResult, error) {
	return func(attempt int) (worlds.DataStepResult, error) {
		stepCtx := context.WithValue(ctx, stepContextKey{}, StepMetadata{ID: stepID, Name: name, StartedAt: time.Now(), Attempt: attempt})
		stepCtx, stop, err := stepCancelContext(stepCtx, execution, refs)
		if err != nil {
			return worlds.DataStepResult{}, err
		}
		defer stop()
		stepCtx, span := startStepSpan(stepCtx, execution, stepID, name, attempt)
		defer span.End()
		value, callErr := invoke(stepCtx, execution, false, fn, input)
		var delivery deliveryError
		if errors.As(callErr, &delivery) {
			return worlds.DataStepResult{}, delivery.cause
		}
		if callErr != nil {
			span.RecordError(callErr)
			span.SetStatus(codes.Error, callErr.Error())
			return worlds.DataStepResult{Error: execution.encodeError(callErr), RetryAfter: stepRetryAfter(callErr, maxRetries, attempt)}, nil
		}
		output, err := execution.EncodePayload(value)
		if err != nil {
			return worlds.DataStepResult{Error: execution.encodeError(fmt.Errorf("workflow: encode step output: %w", err))}, nil
		}
		return worlds.DataStepResult{Result: output}, nil
	}
}
