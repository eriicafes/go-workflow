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
	"go.opentelemetry.io/otel/propagation"
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
		encoded, err := execution.EncodePayload(stepArgs[In]{Args: [1]In{input}})
		if err != nil {
			err = &worlds.SerializationError{Message: "Failed to serialize step arguments", Cause: err}
		}
		// An inline attempt runs with the input decoded from what is recorded,
		// as a step delivery does, not with the workflow's own value.
		var args stepArgs[In]
		if err == nil {
			if decodeErr := execution.DecodePayload(encoded, &args); decodeErr != nil {
				err = fmt.Errorf("workflow: decode step input: %w", decodeErr)
			}
		}
		step := pendingStep{
			stepID:     stepID,
			name:       options.Name,
			input:      encoded,
			failure:    err,
			explicit:   explicit,
			limit:      MaxInlineSteps(ctx),
			duration:   MaxInlineStepsDuration(ctx),
			attempt:    1,
			maxRetries: options.MaxRetries,
			run:        pendingRun(ctx, execution, stepID, options.Name, options.MaxRetries, CancellationReferences(ctx), fn, args.Args[0]),
		}
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
		execution.SetTargetResult(worlds.DataStepResult{Error: execution.encodeError(attemptsUsedError(options.Name, options.MaxRetries, starts, lastStepStart(execution.events, stepID)))})
		panic(execution.exit(errReplayComplete))
	}
	if execution.stalled {
		// No delivery of the step's message got as far as starting it, and
		// this one is past the most a message is delivered.
		execution.SetTargetResult(worlds.DataStepResult{Error: execution.encodeError(deliveriesExceededStepError(options.Name, execution.delivery.Attempt))})
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
	// The first attempt of a step created with a step_created tells the
	// World when the step was created: the time its message carries, or the
	// one the log holds.
	var startOptions worlds.EventsCreateOptions
	if attempt == 1 && !creation.inline {
		startOptions.StepCreatedAt = execution.delivery.StepCreatedAt
		if startOptions.StepCreatedAt.IsZero() {
			startOptions.StepCreatedAt = stepCreatedAt(execution.events, stepID)
		}
	}
	result := execution.createEvent(worlds.EventsCreateEvent{EventType: worlds.EventTypeStepStarted, CorrelationID: string(stepID), Data: started}, startOptions, func(err error) error {
		if errors.Is(err, worlds.ErrEntityConflict) || errors.Is(err, worlds.ErrStepNotStartable) {
			// The step already has its result.
			return nil
		}
		return err
	})
	if result.EventID == "" {
		panic(execution.exit(errReplaySuspended))
	}
	ctx = context.WithValue(ctx, stepContextKey{}, StepMetadata{ID: stepID, Name: options.Name, StartedAt: stepStartedAt(execution.events, stepID, time.Now()), Attempt: attempt})
	stepCtx, stop := StepCancelContext(ctx, execution, CancellationReferences(ctx))
	defer stop()
	stepCtx, span := startStepSpan(stepCtx, execution, stepID, options.Name, attempt)
	defer span.End()
	execution.targetRan = true
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
		failure, retryAfter := stepRetry(callErr, options.Name, options.MaxRetries, attempt, stepMessageSent(execution.events, stepID))
		execution.SetTargetResult(worlds.DataStepResult{Error: execution.encodeError(failure), RetryAfter: retryAfter})
		panic(execution.exit(errReplayComplete))
	}

	output, err := execution.EncodePayload(value)
	if err != nil {
		execution.SetTargetResult(worlds.DataStepResult{Error: execution.encodeError(stepReturnSerializationError(err))})
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

// lastStepStart returns when the last attempt of step stepID among events
// started, or the zero time when none did.
func lastStepStart(events []worlds.Event, stepID worlds.StepID) time.Time {
	var started time.Time
	for _, event := range events {
		if event.CorrelationID == string(stepID) && event.EventType == worlds.EventTypeStepStarted {
			started = event.CreatedAt
		}
	}
	return started
}

// invoke calls fn with input in stepCtx. A panic in fn becomes its error,
// except, when inReplay is set, the panic that ends a suspended replay, which
// passes on. What fn returns is its result whether or not stepCtx is
// cancelled, except that an aborted error becomes a FatalError "Aborted:
// ...", unless the delivery itself stopped. A World that records step
// executions is told right before fn is called.
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
		if recorder, ok := execution.world.(worlds.StepExecutions); ok {
			metadata, _ := StepMetadataFromContext(stepCtx)
			recorder.RecordStepExecution(stepCtx, metadata.ID)
		}
		value, err = fn(stepCtx, input)
	}()
	// What the step left to finish, such as buffered stream writes, is done
	// before its result is recorded, so a completed step's writes are stored.
	if returnErr := returned.run(); returnErr != nil && err == nil {
		err = returnErr
	}
	if aborted(stepCtx, err) && execution.ctx.Err() == nil {
		err = worlds.FatalError{Cause: fmt.Errorf("Aborted: %w", err)}
	}
	return value, err
}

// aborted reports whether err, a step's error, is it giving up on a
// cancellation: an error matching context.Canceled, as the Workflow SDK
// promotes an AbortError, or once stepCtx is done, one matching its cause,
// such as an HTTP request's error after a durable timeout.
func aborted(stepCtx context.Context, err error) bool {
	if err == nil || errors.As(err, new(worlds.FatalError)) {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return true
	}
	return stepCtx.Err() != nil && errors.Is(err, context.Cause(stepCtx))
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

// stepRetryDelay is how long a failed attempt waits for its retry when its
// error names no delay.
var stepRetryDelay = time.Second

// stepRetryExpirySlack is how long before its message expires a step's retry
// must be due, so the message is still there to deliver it.
const stepRetryExpirySlack = time.Minute

// stepRetry decides what follows attempt of the step named name failing with
// cause: the time its retry is due, or nil when it fails for good. It returns
// the error the attempt records. A fatal cause is recorded as it is. A cause
// that fails the step's last allowed attempt is recorded inside a FatalError
// that says the retries are used up, and so is one whose retry would be due
// after the step's message, sent at sent, expires: such a retry would strand
// the step without an outcome, so the step fails instead. Either keeps cause
// as its cause. A zero sent is a message whose age is not known.
func stepRetry(cause error, name string, maxRetries, attempt int, sent time.Time) (error, *time.Time) {
	if worlds.IsFatal(cause) {
		return cause, nil
	}
	if attempt > maxRetries {
		return retriesExhaustedError(name, maxRetries, cause), nil
	}
	delay := stepRetryDelay
	var retryable worlds.RetryableError
	if errors.As(cause, &retryable) && retryable.After > 0 {
		delay = retryable.After
	}
	due := time.Now().Add(delay)
	if !sent.IsZero() && due.After(sent.Add(worlds.StepRetention-stepRetryExpirySlack)) {
		return fatalError(fmt.Sprintf("Step \"%s\" could not be retried: its next attempt at %s is past the retention of its queue message: %s", name, isoTime(due), cause.Error()), cause), nil
	}
	return cause, &due
}

// stepMessageSent returns when the message that delivers step stepID was
// sent: when the step was created as a step delivery, or when an inline
// step's first retry was handed to one. It is zero for a step without one.
func stepMessageSent(events []worlds.Event, stepID worlds.StepID) time.Time {
	for _, event := range events {
		if event.CorrelationID != string(stepID) {
			continue
		}
		switch event.EventType {
		case worlds.EventTypeStepCreated, worlds.EventTypeStepRetrying:
			return event.CreatedAt
		}
	}
	return time.Time{}
}

// startStepSpan starts the span of a step attempt with the global
// OpenTelemetry tracer, as a child of the span in ctx. A runner that puts a
// span context in the ctx it passes to a step parents the step's span under
// it, since the step runs in the replay that delivers it. With no span in
// ctx, the step's span continues the trace the run was started in, which the
// run's execution context carries.
func startStepSpan(ctx context.Context, execution *Execution, stepID worlds.StepID, name string, attempt int) (context.Context, trace.Span) {
	short := strings.TrimSuffix(name[strings.LastIndex(name, "//")+2:], "-fm")
	run := execution.Run()
	if !trace.SpanContextFromContext(ctx).IsValid() {
		if recorded, ok := run.ExecutionContext[worlds.ExecutionContextTraceCarrier].(map[string]any); ok {
			carrier := propagation.MapCarrier{}
			for key, value := range recorded {
				if text, ok := value.(string); ok {
					carrier[key] = text
				}
			}
			ctx = otel.GetTextMapPropagator().Extract(ctx, carrier)
		}
	}
	return otel.Tracer("github.com/eriicafes/go-workflow").Start(ctx, "step "+short, trace.WithAttributes(
		attribute.String("workflow.name", run.WorkflowName),
		attribute.String("workflow.run.id", string(run.RunID)),
		attribute.String("workflow.step.id", string(stepID)),
		attribute.String("workflow.step.name", name),
		attribute.Int("workflow.step.attempt", attempt),
	))
}

// stepReturnSerializationError returns the error of a step whose return
// value did not encode with err.
func stepReturnSerializationError(err error) error {
	return &worlds.SerializationError{Message: "Failed to serialize step return value", Cause: err}
}
