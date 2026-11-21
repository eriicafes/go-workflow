package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type (
	// DurableStepOptions holds the arguments to DurableStep. A failed attempt
	// is retried while the attempt number is at most MaxRetries, unless its
	// error is fatal. RetryableError delays the next attempt.
	DurableStepOptions struct {
		Execution  *Execution
		Name       string
		MaxRetries int
	}

	// DurableStepResult is the state of a step at one replay. DurableStep
	// and DurableInlineStep report only the StepID, or an Err from declaring
	// the step, and the step's result is read with StepOutcome once replay
	// delivers its settling event.
	DurableStepResult[T any] struct {
		StepID worlds.StepID
		Data   T
		Err    error
	}
)

// DurableStep declares the next step. A flow holds a new step back until the
// runner waits, when Execution.Wait creates it: inline, by a step_started
// that starts its first attempt in the flow, while fewer steps than the
// limit WithMaxInlineSteps sets run inline, and otherwise by a step_created
// and a step delivery queued once it commits. A step's mode never changes
// after: a flow that finds an inline step's attempt without a result, which
// a delivery that stopped started, runs another, and a failed inline attempt
// that may retry is handed to a step delivery.
//
// On replay it matches the recorded declaration and returns the step's ID,
// whose result StepOutcome reads once the walk reaches it. During the step
// delivery for this step it starts the next attempt, calls fn with the
// recorded input, records the result in the Execution, and ends the replay
// with a panic the Runtime recovers, so it does not return. A step whose
// attempts are all used fails without another. fn is executed at least once.
// When the operation recorded with its ID is not a step with options.Name, it
// ends the replay with a panic carrying a replayDivergenceError.
func DurableStep[In, Out any](ctx context.Context, options DurableStepOptions, fn func(context.Context, In) (Out, error), input In) DurableStepResult[Out] {
	return durableStep(ctx, options, false, fn, input)
}

// durableStep declares the next step, inline whatever the flow's limit when
// explicit is set.
func durableStep[In, Out any](ctx context.Context, options DurableStepOptions, explicit bool, fn func(context.Context, In) (Out, error), input In) DurableStepResult[Out] {
	execution := options.Execution
	stepID := execution.NextStepID()
	if _, exists := execution.declared(string(stepID)); !exists {
		if execution.target != nil {
			// A step delivery records only its own step's events.
			panic(execution.exit(errReplaySuspended))
		}
		step := pendingStep{
			stepID:     stepID,
			name:       options.Name,
			explicit:   explicit,
			limit:      MaxInlineSteps(ctx),
			duration:   MaxInlineStepsDuration(ctx),
			attempt:    1,
			maxRetries: options.MaxRetries,
			run:        pendingRun(ctx, execution, stepID, options.Name, options.MaxRetries, CancellationReferences(ctx), fn, input),
		}
		encoded, err := execution.EncodePayload(stepArgs[In]{Args: [1]In{input}})
		if err != nil {
			step.failure = fmt.Errorf("workflow: encode step input: %w", err)
		}
		step.input = encoded
		execution.hold(step)
		return DurableStepResult[Out]{StepID: stepID}
	}

	// Replay the recorded declaration.
	creation, ok := stepCreation(execution.events, stepID)
	if !ok {
		index, _ := execution.declared(string(stepID))
		panic(execution.exit(replayDivergenceError{CorrelationID: string(stepID), Reason: fmt.Sprintf("recorded %s, replay reached a step", execution.events[index].EventType)}))
	}
	if creation.name != options.Name {
		panic(execution.exit(replayDivergenceError{CorrelationID: string(stepID), Reason: fmt.Sprintf("recorded step %q, replay reached step %q", creation.name, options.Name)}))
	}
	if _, settled := execution.settles[string(stepID)]; settled {
		return DurableStepResult[Out]{StepID: stepID}
	}
	starts, retried := stepAttempts(execution.events, stepID)
	if execution.target == nil {
		// A step delivery runs a step created as one, and an inline step
		// whose retry was handed to one. The attempts of an inline step
		// this delivery runs are its own.
		if !creation.inline || retried || execution.ranInline[stepID] {
			return DurableStepResult[Out]{StepID: stepID}
		}
		// An inline attempt without a result was started by a delivery that
		// stopped, so this flow runs the next.
		var args stepArgs[In]
		if err := execution.DecodePayload(creation.input, &args); err != nil {
			return DurableStepResult[Out]{StepID: stepID, Err: fmt.Errorf("workflow: decode step input: %w", err)}
		}
		attempt := starts + 1
		execution.hold(pendingStep{
			stepID:    stepID,
			name:      options.Name,
			recovered: true,
			exhausted: attempt > options.MaxRetries+1,
			attempt:   attempt,
			run:       pendingRun(ctx, execution, stepID, options.Name, options.MaxRetries, CancellationReferences(ctx), fn, args.Args[0]),
		})
		return DurableStepResult[Out]{StepID: stepID}
	}
	if *execution.target != stepID {
		return DurableStepResult[Out]{StepID: stepID}
	}

	// The step delivery runs the next attempt with the recorded input, or
	// fails a step whose attempts are all used.
	attempt := starts + 1
	execution.targetAttempt = attempt
	if attempt > options.MaxRetries+1 {
		execution.SetTargetResult(worlds.DataStepResult{Error: execution.encodeError(worlds.FatalError{Cause: errStepAttemptsExhausted})})
		panic(execution.exit(errReplayComplete))
	}
	var args stepArgs[In]
	if err := execution.DecodePayload(creation.input, &args); err != nil {
		return DurableStepResult[Out]{StepID: stepID, Err: fmt.Errorf("workflow: decode step input: %w", err)}
	}
	input = args.Args[0]
	started, err := json.Marshal(worlds.DataStepStarted{StepName: options.Name, Attempt: attempt})
	if err != nil {
		panic(execution.exit(deliveryError{cause: err}))
	}
	result := execution.CreateEvent(worlds.EventsCreateEvent{EventType: worlds.EventTypeStepStarted, CorrelationID: string(stepID), Data: started}, func(err error) error {
		if errors.Is(err, worlds.ErrEventConflict) || errors.Is(err, worlds.ErrStepNotStartable) {
			// The step already has its result.
			return nil
		}
		return err
	})
	if result.EventID == "" {
		panic(execution.exit(errReplaySuspended))
	}
	ctx = context.WithValue(ctx, stepContextKey{}, StepMetadata{ID: stepID, Name: options.Name, StartedAt: time.Now(), Attempt: attempt})
	stepCtx, stop := StepCancelContext(ctx, execution, CancellationReferences(ctx))
	defer stop()
	stepCtx, span := startStepSpan(stepCtx, execution, stepID, options.Name, attempt)
	defer span.End()
	value, callErr := invoke(stepCtx, execution, true, fn, input)
	var delivery deliveryError
	if errors.As(callErr, &delivery) {
		panic(execution.exit(delivery))
	}
	if callErr != nil {
		span.RecordError(callErr)
		span.SetStatus(codes.Error, callErr.Error())
	}

	// Report the target result to the engine, which appends the lifecycle
	// event.
	if callErr != nil {
		result := worlds.DataStepResult{Error: execution.encodeError(callErr), RetryAfter: stepRetryAfter(callErr, options.MaxRetries, attempt)}
		execution.SetTargetResult(result)
		panic(execution.exit(errReplayComplete))
	}

	output, err := execution.EncodePayload(value)
	if err != nil {
		execution.SetTargetResult(worlds.DataStepResult{Error: execution.encodeError(fmt.Errorf("workflow: encode step output: %w", err))})
	} else {
		execution.SetTargetResult(worlds.DataStepResult{Result: output})
	}
	panic(execution.exit(errReplayComplete))
}

// stepAttempts returns the number of attempts of step stepID among events
// that started, and whether an attempt failed with a retry, which moves the
// step to a step delivery for good.
func stepAttempts(events []worlds.Event, stepID worlds.StepID) (int, bool) {
	starts, retried := 0, false
	for _, event := range events {
		if event.CorrelationID != string(stepID) {
			continue
		}
		switch event.EventType {
		case worlds.EventTypeStepStarted:
			starts++
		case worlds.EventTypeStepRetrying:
			retried = true
		}
	}
	return starts, retried
}

// invoke calls fn with input in stepCtx. A panic in fn becomes its error,
// except, when inReplay is set, the panic that ends a suspended replay, which
// passes on. A cancelled stepCtx makes the error fatal, unless the delivery
// itself stopped.
func invoke[In, Out any](stepCtx context.Context, execution *Execution, inReplay bool, fn func(context.Context, In) (Out, error), input In) (value Out, err error) {
	returned := &stepReturn{}
	stepCtx = context.WithValue(stepCtx, stepReturnKey{}, returned)
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
	// What the step left to finish, such as buffered stream writes, is done
	// before its result is recorded, so a completed step's writes are stored.
	if returnErr := returned.run(); returnErr != nil && err == nil {
		err = returnErr
	}
	if stepCtx.Err() != nil && execution.ctx.Err() == nil {
		err = worlds.FatalError{Cause: context.Canceled}
	}
	return value, err
}

type stepReturnKey struct{}

// stepReturn holds the functions to run when a step function returns.
type stepReturn struct {
	mu    sync.Mutex
	funcs []func() error
	done  bool
}

func (r *stepReturn) run() error {
	r.mu.Lock()
	funcs := r.funcs
	r.funcs, r.done = nil, true
	r.mu.Unlock()
	var errs []error
	for _, fn := range funcs {
		if err := fn(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// OnStepReturn registers fn to run when the step function ctx belongs to
// returns, in registration order and before the step's result is recorded.
// An error from fn fails a step that would otherwise succeed. It reports
// false, and does not register fn, when ctx is not a running step
// function's context.
func OnStepReturn(ctx context.Context, fn func() error) bool {
	returned, ok := ctx.Value(stepReturnKey{}).(*stepReturn)
	if !ok {
		return false
	}
	returned.mu.Lock()
	defer returned.mu.Unlock()
	if returned.done {
		return false
	}
	returned.funcs = append(returned.funcs, fn)
	return true
}

// StepOutcome decodes the result of the step stepID, which must have settled
// for the walk (Execution.Settled). A completed step returns its output, a
// failed one its error as an *Error.
func StepOutcome[Out any](execution *Execution, stepID worlds.StepID) (Out, error) {
	var data Out
	event, ok := execution.SettledEvent(string(stepID))
	if !ok {
		return data, fmt.Errorf("workflow: step %s has not settled", stepID)
	}
	var result worlds.DataStepResult
	if err := json.Unmarshal(event.Data, &result); err != nil {
		return data, fmt.Errorf("workflow: decode step result: %w", err)
	}
	if event.EventType == worlds.EventTypeStepFailed {
		return data, execution.decodeError(result.Error)
	}
	if err := execution.DecodePayload(result.Result, &data); err != nil {
		return data, fmt.Errorf("workflow: decode step output: %w", err)
	}
	return data, nil
}

func stepRetryAfter(cause error, maxRetries int, attempt int) *time.Time {
	var fatal worlds.FatalError
	if errors.As(cause, &fatal) || attempt > maxRetries {
		return nil
	}
	var retryable worlds.RetryableError
	due := time.Now()
	if errors.As(cause, &retryable) && retryable.After > 0 {
		due = due.Add(retryable.After)
	}
	return &due
}

// startStepSpan starts the span of a step attempt with the global
// OpenTelemetry tracer, as a child of the span in ctx. A runner that puts a
// span context in the ctx it passes to a step parents the step's span under
// it, since the step runs in the replay that delivers it.
func startStepSpan(ctx context.Context, execution *Execution, stepID worlds.StepID, name string, attempt int) (context.Context, trace.Span) {
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
