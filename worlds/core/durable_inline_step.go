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
	// latency is the latency telemetry the step's result carries, for the
	// first new step of a batch the flow runs inline.
	latency *stepLatency
	// run calls the step function as attempt, and returns its result to
	// record, with latency's telemetry on a first attempt, or a World error
	// that stopped it.
	run func(attempt int, latency *stepLatency) (worlds.DataStepResult, error)
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
func pendingRun[In, Out any](ctx context.Context, execution *Execution, stepID worlds.StepID, name string, maxRetries int, refs []cancellationReference, fn func(context.Context, In) (Out, error), input In) func(int, *stepLatency) (worlds.DataStepResult, error) {
	return func(attempt int, latency *stepLatency) (result worlds.DataStepResult, err error) {
		stepCtx := context.WithValue(ctx, stepContextKey{}, StepMetadata{ID: stepID, Name: name, StartedAt: time.Now(), Attempt: attempt})
		stepCtx, stop, err := stepCancelContext(stepCtx, execution, refs)
		if err != nil {
			return worlds.DataStepResult{}, err
		}
		defer stop()
		stepCtx, span := startStepSpan(stepCtx, execution, stepID, name, attempt)
		defer span.End()
		if attempt == 1 {
			codeStarted := time.Now()
			defer func() { latency.record(&result, codeStarted) }()
		}
		value, callErr := invoke(stepCtx, execution, false, fn, input)
		var delivery deliveryError
		if errors.As(callErr, &delivery) {
			return worlds.DataStepResult{}, delivery.cause
		}
		if callErr != nil {
			span.RecordError(callErr)
			span.SetStatus(codes.Error, callErr.Error())
			// An inline step's retry is handed to a message sent now.
			failure, retryAfter := stepRetry(callErr, name, maxRetries, attempt, time.Now())
			return worlds.DataStepResult{Error: execution.encodeError(failure), RetryAfter: retryAfter}, nil
		}
		output, err := execution.EncodePayload(value)
		if err != nil {
			return worlds.DataStepResult{Error: execution.encodeError(fmt.Errorf("workflow: encode step output: %w", err))}, nil
		}
		return worlds.DataStepResult{Result: output}, nil
	}
}

// maxLatencyClockSkew is the most a latency measured across machines may be
// negative and still be taken as clock skew, and recorded as zero. A more
// negative one has a wrong start and is left out.
const maxLatencyClockSkew = time.Minute

// stepLatency is what a flow knows, as it schedules a batch of steps to run
// inline, about how long the batch's first step waited to start. The step's
// final event records it once its function has started.
type stepLatency struct {
	// ttfsAnchor is when the run was created, set when the step is the
	// run's first and nothing but the runtime delayed it. ttfsEnd, when
	// set, ends that measurement at the workflow's first attribute write,
	// and blocking is the time spent creating hooks inside it.
	ttfsAnchor time.Time
	ttfsEnd    time.Time
	blocking   time.Duration
	// rsfsAnchor is when the delivery's run_started was written, set with
	// ttfsAnchor, and startSent when the step's start was sent.
	rsfsAnchor time.Time
	startSent  time.Time
	// prevStepEnd is when the previous step finished, set when the log's
	// last event is a step's result, with the log's counts then.
	prevStepEnd           time.Time
	stepCount, eventCount int
	// replay is how long the runner ran before it scheduled the batch.
	replay        time.Duration
	optimizations []string
}

// ttfsDisqualifying holds the event types that, in the log when a run's
// first batch of steps is scheduled, mean the time to it was not the
// runtime's alone: an earlier step, a hook's payload, or a timer.
var ttfsDisqualifying = map[worlds.EventType]bool{
	worlds.EventTypeStepCreated: true, worlds.EventTypeStepStarted: true, worlds.EventTypeStepCompleted: true,
	worlds.EventTypeStepFailed: true, worlds.EventTypeStepRetrying: true, worlds.EventTypeHookReceived: true,
	worlds.EventTypeWaitCreated: true, worlds.EventTypeWaitCompleted: true,
}

// stepLatency returns what the flow knows about the wait of the first step
// of the batch it is about to run inline, or nil when the batch qualifies for
// no measurement. hasWaits reports that the batch creates timers too.
func (e *Execution) stepLatency(hasWaits bool) *stepLatency {
	latency := &stepLatency{replay: time.Since(e.resumedAt)}
	if e.turbo {
		latency.optimizations = append(latency.optimizations, "turbo")
	}
	if e.startedClean && !hasWaits {
		latency.ttfsAnchor, latency.blocking = time.UnixMilli(int64(e.seed.time)), e.hookBlocking
		for _, event := range e.events {
			if ttfsDisqualifying[event.EventType] {
				latency.ttfsAnchor, latency.ttfsEnd = time.Time{}, time.Time{}
				break
			}
			if event.EventType == worlds.EventTypeAttrSet {
				if at := eventTime(event); latency.ttfsEnd.IsZero() || at.Before(latency.ttfsEnd) {
					// Hook time is not known to fall before the write.
					latency.ttfsEnd, latency.blocking = at, 0
				}
			}
		}
		if !latency.ttfsAnchor.IsZero() {
			latency.rsfsAnchor = e.runStartedAt
		}
	}
	if last := len(e.events) - 1; !hasWaits && last >= 0 && (e.events[last].EventType == worlds.EventTypeStepCompleted || e.events[last].EventType == worlds.EventTypeStepFailed) {
		latency.prevStepEnd, latency.eventCount = eventTime(e.events[last]), len(e.events)
		finished := map[string]bool{}
		for _, event := range e.events {
			if event.EventType == worlds.EventTypeStepCompleted || event.EventType == worlds.EventTypeStepFailed {
				finished[event.CorrelationID] = true
			}
		}
		latency.stepCount = len(finished)
	}
	if latency.ttfsAnchor.IsZero() && latency.prevStepEnd.IsZero() {
		return nil
	}
	return latency
}

// eventTime returns when event's writer decided it, or when it was stored.
func eventTime(event worlds.Event) time.Time {
	if event.OccurredAt != nil {
		return *event.OccurredAt
	}
	return event.CreatedAt
}

// sent notes that the step's start is being sent now, in the way
// optimization names.
func (l *stepLatency) sent(optimization string) {
	if l != nil {
		l.startSent, l.optimizations = time.Now(), append(l.optimizations, optimization)
	}
}

// record sets the latency telemetry of result, the result of the step's
// first attempt, whose function started at codeStarted.
func (l *stepLatency) record(result *worlds.DataStepResult, codeStarted time.Time) {
	if l == nil {
		return
	}
	measure := func(from, to time.Time, less time.Duration) *float64 {
		if from.IsZero() {
			return nil
		}
		elapsed := to.Sub(from) - less
		if elapsed < -maxLatencyClockSkew {
			return nil
		}
		ms := float64(max(elapsed, 0).Milliseconds())
		return &ms
	}
	end := codeStarted
	if !l.ttfsEnd.IsZero() {
		end = l.ttfsEnd
	}
	ttfs, stso := measure(l.ttfsAnchor, end, l.blocking), measure(l.prevStepEnd, codeStarted, 0)
	if ttfs == nil && stso == nil {
		return
	}
	result.TTFS, result.STSO = ttfs, stso
	if stso != nil {
		result.StepCount, result.EventCount = l.stepCount, l.eventCount
	}
	if !l.rsfsAnchor.IsZero() && !l.startSent.IsZero() {
		ms := float64(max(l.startSent.Sub(l.rsfsAnchor), 0).Milliseconds())
		result.RSFS = &ms
	}
	replay := float64(max(l.replay, 0).Milliseconds())
	result.FinalSchedulingReplay, result.Optimizations = &replay, l.optimizations
}

// clearStepLatency removes the latency telemetry from result.
func clearStepLatency(result *worlds.DataStepResult) {
	result.TTFS, result.STSO, result.RSFS, result.FinalSchedulingReplay = nil, nil, nil, nil
	result.StepCount, result.EventCount, result.Optimizations = 0, 0, nil
}
