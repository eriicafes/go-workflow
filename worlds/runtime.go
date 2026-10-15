package worlds

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"
)

// Runtime executes deliveries. It replays the runners in its RunnerSet
// against a World as flows and steps and records their results as events.
// Whether a delivery may run is decided by appending events, so duplicate and
// stale deliveries are safe. A delivery that ends before recording its result
// is redelivered by the queue, which re-runs the step as a new attempt.
type Runtime struct {
	world   World
	runners RunnerSet
}

// NewRuntime returns a Runtime that executes deliveries for runs recorded in
// world using runners. It copies runners.
func NewRuntime(world World, runners RunnerSet) *Runtime {
	return &Runtime{world: world, runners: maps.Clone(runners)}
}

// Execute handles one delivery. With a nil StepID it executes a flow, which
// replays the run's runner and, when the runner returns instead of suspending,
// appends run_completed or run_failed. With StepID set it executes a step,
// which replays the runner up to that step, executes the step function, and
// records the step's result. A delivery for a terminal run or step returns nil
// without running runner or step code. A non-nil error means the delivery
// should be retried. Execute removes any Client NewClientContext put in ctx, so
// a run never uses a Client it inherited from the context its delivery runs in.
func (e *Runtime) Execute(ctx context.Context, input QueueInput) error {
	ctx = context.WithValue(ctx, clientContextKey{}, nil)
	if input.StepID != nil {
		return e.executeStep(ctx, input)
	}
	return e.executeFlow(ctx, input)
}

// executeFlow admits root execution, replays its complete event prefix, and
// turns the runner's result into a terminal run event when it did not suspend.
func (e *Runtime) executeFlow(ctx context.Context, input QueueInput) error {
	// Load the materialized run and its complete replay prefix before any
	// runner code is called.
	run, events, err := e.loadRun(ctx, input.RunID)
	if err != nil {
		return err
	}
	if run.Status.Terminal() {
		return nil
	}

	// Atomically admit this root delivery. Duplicate deliveries receive the
	// existing run_started event and replay from the same event history.
	eventsCount := len(events)
	startedEvent := EventsCreateEvent{
		EventType: EventTypeRunStarted,
	}
	started, err := e.world.EventsCreate(ctx, EventsCreateInput{
		RunID:   input.RunID,
		Event:   startedEvent,
		Options: EventsCreateOptions{EventCount: &eventsCount},
	})
	if errors.Is(err, ErrRunTerminal) {
		return nil
	}
	if err != nil {
		return err
	}

	// Build the replay context and merge the admission append without a
	// follow-up storage read.
	execution := newExecution(e.world, run, events, nil)
	execution.Merge(started, Event{
		RunID:     input.RunID,
		EventID:   started.EventID,
		EventType: startedEvent.EventType,
	})

	// Execute the workflow while observing cancellation events appended
	// after the loaded prefix.
	runner, ok := e.runners[run.WorkflowName]
	if !ok {
		return e.failRun(ctx, execution, fmt.Errorf("%w: %s", ErrWorkflowNotFound, run.WorkflowName))
	}
	ctx, stop := e.observeCancellation(ctx, run.RunID, nil, len(execution.events))
	defer stop()
	ctx = context.WithValue(ctx, executionContextKey{}, execution)
	output, err := e.run(runner, ctx, json.RawMessage(run.Input))

	// Interpret replay control flow or materialize the root outcome.
	switch {
	case errors.Is(context.Cause(ctx), ErrRunCancelled), errors.Is(context.Cause(ctx), ErrStepCancelled):
		return nil
	case context.Cause(ctx) != nil:
		return context.Cause(ctx)
	case errors.Is(err, errReplayComplete), errors.Is(err, errReplaySuspended):
		return nil
	case errors.As(err, &deliveryError{}):
		// The World failed to record an event, so retry the delivery. A run
		// that finished meanwhile needs nothing more.
		if errors.Is(err, ErrRunTerminal) {
			return nil
		}
		return errors.Unwrap(err)
	case errors.Is(err, ErrRunCancelled), errors.Is(err, ErrStepCancelled):
		return nil
	case err != nil:
		failure := e.failRun(ctx, execution, err)
		if errors.Is(failure, errReplayComplete) || errors.Is(failure, errReplaySuspended) {
			return nil
		}
		return failure
	case run.Status.Terminal():
		return nil
	default:
		return e.completeRun(ctx, run.RunID, len(execution.events), output)
	}
}

// executeStep admits one targeted delivery, replays to its declaration, and
// persists the result reported by DurableStep.
func (e *Runtime) executeStep(ctx context.Context, input QueueInput) error {
	if input.StepID == nil {
		return nil
	}
	// Load the root replay prefix and target projection before admitting the
	// targeted delivery.
	run, events, err := e.loadRun(ctx, input.RunID)
	if err != nil || run.Status.Terminal() {
		return err
	}
	step, err := e.world.StepsGet(ctx, StepsGetInput{RunID: input.RunID, StepID: *input.StepID})
	if errors.Is(err, ErrStepNotFound) || step.Status.Terminal() {
		return nil
	}
	if err != nil {
		return err
	}
	// Atomically admit the target. A terminal or stale target is inert and
	// never executes user code again. Starting a running step records a new
	// attempt, which is how a redelivery re-runs a step whose delivery died.
	eventsCount := len(events)
	startedEvent := EventsCreateEvent{
		EventType:     EventTypeStepStarted,
		CorrelationID: string(*input.StepID),
	}
	started, err := e.world.EventsCreate(ctx, EventsCreateInput{
		RunID:   input.RunID,
		Event:   startedEvent,
		Options: EventsCreateOptions{EventCount: &eventsCount},
	})
	if errors.Is(err, ErrRunTerminal) || errors.Is(err, ErrStepNotStartable) {
		return nil
	}
	if err != nil {
		return err
	}

	// Replay the root workflow with the target selected. DurableStep runs
	// only when replay reaches the matching step declaration.
	execution := newExecution(e.world, run, events, input.StepID)
	execution.Merge(started, Event{
		RunID:         input.RunID,
		EventID:       started.EventID,
		EventType:     startedEvent.EventType,
		CorrelationID: startedEvent.CorrelationID,
	})
	runner, ok := e.runners[run.WorkflowName]
	if !ok {
		return e.failStep(ctx, execution, *input.StepID, fmt.Errorf("%w: %s", ErrWorkflowNotFound, run.WorkflowName))
	}
	ctx, stop := e.observeCancellation(ctx, input.RunID, input.StepID, len(execution.events))
	defer stop()
	ctx = context.WithValue(ctx, executionContextKey{}, execution)
	_, err = e.run(runner, ctx, json.RawMessage(run.Input))

	// Persist the reported target result. A failed target is reported to the
	// root replay, which decides whether the workflow itself should fail and
	// therefore whether to run compensations.
	switch {
	case errors.Is(context.Cause(ctx), ErrRunCancelled), errors.Is(context.Cause(ctx), ErrStepCancelled):
		return nil
	case context.Cause(ctx) != nil:
		return context.Cause(ctx)
	case errors.Is(err, errReplayComplete):
		result, ok := execution.GetTargetResult()
		if !ok {
			return e.failStep(ctx, execution, *input.StepID, errors.New("workflow: targeted step completed without a result"))
		}
		return e.finishStep(ctx, execution, *input.StepID, result)
	case errors.Is(err, errReplaySuspended):
		return nil
	case errors.As(err, &deliveryError{}):
		if errors.Is(err, ErrRunTerminal) {
			return nil
		}
		return errors.Unwrap(err)
	case err != nil:
		return e.failStep(ctx, execution, *input.StepID, err)
	default:
		return e.failStep(ctx, execution, *input.StepID, fmt.Errorf("workflow: targeted replay did not reach step %s", *input.StepID))
	}
}

// run converts replay-control panics from durable primitives back into errors
// while allowing unrelated panics to retain their normal Go behavior.
func (e *Runtime) run(runner Runner, ctx context.Context, input json.RawMessage) (output json.RawMessage, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			if recoveredError, ok := recovered.(error); ok {
				err = recoveredError
				return
			}
			panic(recovered)
		}
	}()
	return runner.Run(ctx, input)
}

// loadRun reads the projection and one complete event prefix before user code
// runs. Replaying primitives then use only this in-memory prefix.
func (e *Runtime) loadRun(ctx context.Context, runID RunID) (Run, []Event, error) {
	run, err := e.world.RunsGet(ctx, RunsGetInput{RunID: runID})
	if err != nil {
		return Run{}, nil, err
	}
	// Worlds must return the full current prefix when no page size is requested.
	// A bounded event source can still page internally here, but the workflow
	// receives one fully preloaded slice.
	page, err := e.world.EventsList(ctx, EventsListInput{RunID: runID})
	if err != nil {
		return Run{}, nil, err
	}
	events := page.Data
	for page.HasMore {
		page, err = e.world.EventsList(ctx, EventsListInput{RunID: runID, Pagination: Pagination{Cursor: page.Cursor}})
		if err != nil {
			return Run{}, nil, err
		}
		events = append(events, page.Data...)
	}
	return run, events, nil
}

// completeRun records successful root output unless another delivery already
// made the run terminal.
func (e *Runtime) completeRun(ctx context.Context, runID RunID, eventCount int, data []byte) error {
	_, err := e.world.EventsCreate(ctx, EventsCreateInput{
		RunID:   runID,
		Event:   EventsCreateEvent{EventType: EventTypeRunCompleted, Data: data},
		Options: EventsCreateOptions{EventCount: &eventCount},
	})
	if errors.Is(err, ErrRunTerminal) {
		return nil
	}
	return err
}

// failRun executes registered compensations before recording the terminal root
// failure. A compensation records its own failed step result, so only
// infrastructure failures prevent the terminal root event from being appended.
func (e *Runtime) failRun(ctx context.Context, execution *Execution, failure error) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			if recoveredError, ok := recovered.(error); ok {
				err = recoveredError
				return
			}
			panic(recovered)
		}
	}()
	for _, rollback := range slices.Backward(execution.rollbacks) {
		if err := rollback(ctx, execution); err != nil {
			return err
		}
	}
	eventCount := len(execution.events)
	_, err = e.world.EventsCreate(ctx, EventsCreateInput{
		RunID: execution.run.RunID,
		Event: EventsCreateEvent{
			EventType: EventTypeRunFailed,
			Data:      encodeError(failure),
		},
		Options: EventsCreateOptions{EventCount: &eventCount},
	})
	if errors.Is(err, ErrRunTerminal) {
		return nil
	}
	return err
}

// finishStep records a targeted step result and queues either its retry or a
// root replay that can consume the terminal result.
func (e *Runtime) finishStep(ctx context.Context, execution *Execution, stepID StepID, result stepResultData) error {
	eventType := EventTypeStepCompleted
	if len(result.Error) != 0 {
		eventType = EventTypeStepFailed
		if result.RetryAfter != nil {
			eventType = EventTypeStepRetrying
		}
	}
	payload, err := json.Marshal(result)
	if err != nil {
		return err
	}
	eventCount := len(execution.events)
	eventResult, err := e.world.EventsCreate(ctx, EventsCreateInput{
		RunID:   execution.run.RunID,
		Event:   EventsCreateEvent{EventType: eventType, CorrelationID: string(stepID), Data: payload},
		Options: EventsCreateOptions{EventCount: &eventCount},
	})
	if errors.Is(err, ErrRunTerminal) || errors.Is(err, ErrStepNotStartable) {
		return nil
	}
	if err != nil {
		return err
	}
	execution.Merge(eventResult, Event{
		RunID:         execution.run.RunID,
		EventID:       eventResult.EventID,
		EventType:     eventType,
		CorrelationID: string(stepID),
		Data:          payload,
	})
	queue := QueueInput{RunID: execution.run.RunID, IdempotencyKey: string(eventResult.EventID)}
	if eventType == EventTypeStepRetrying {
		queue.StepID = &stepID
		queue.NotBefore = result.RetryAfter
		_, err = e.world.Queue(ctx, queue)
		return err
	}
	_, err = e.world.Queue(ctx, queue)
	return err
}

func (e *Runtime) failStep(ctx context.Context, execution *Execution, stepID StepID, failure error) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			recoveredError, ok := recovered.(error)
			if !ok {
				panic(recovered)
			}
			switch {
			case errors.Is(recoveredError, errReplayComplete):
				result, ok := execution.GetTargetResult()
				if !ok {
					err = errors.New("workflow: targeted rollback completed without a result")
					return
				}
				err = e.finishStep(ctx, execution, stepID, result)
			case errors.Is(recoveredError, errReplaySuspended):
				err = nil
			default:
				err = recoveredError
			}
		}
	}()
	for _, rollback := range slices.Backward(execution.rollbacks) {
		if err := rollback(ctx, execution); err != nil {
			return err
		}
	}
	return e.finishStep(ctx, execution, stepID, stepResultData{Error: encodeError(failure)})
}

// observeCancellation polls events appended after the loaded prefix and
// cooperatively cancels the root or targeted step context when requested.
func (e *Runtime) observeCancellation(parent context.Context, runID RunID, stepID *StepID, count int) (context.Context, func()) {
	ctx, cancel := context.WithCancelCause(parent)
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		defer close(done)
		var cursor string
		if count > 0 {
			cursor = string(EventIDForSlot(uint64(count)))
		}
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				page, err := e.world.EventsList(ctx, EventsListInput{RunID: runID, Pagination: Pagination{Cursor: cursor}})
				if err != nil {
					continue
				}
				for _, event := range page.Data {
					if event.EventType == EventTypeRunCancelled {
						cancel(ErrRunCancelled)
						return
					}
					if stepID != nil && event.EventType == EventTypeStepCancelled && event.CorrelationID == string(*stepID) {
						cancel(ErrStepCancelled)
						return
					}
				}
				if len(page.Data) > 0 {
					cursor = string(page.Data[len(page.Data)-1].EventID)
				} else if page.HasMore {
					cursor = page.Cursor
				}
			}
		}
	}()
	return ctx, func() { cancel(nil); <-done }
}
