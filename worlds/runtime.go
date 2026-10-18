package worlds

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"time"
)

const (
	// maxReplayDivergenceRetries is how many recovery deliveries a diverged
	// replay gets, each counted in QueueInput.ReplayDivergenceCount, before
	// the run fails.
	maxReplayDivergenceRetries = 3
	// maxDeploymentMismatchRetries is how many times a delivery that reached a
	// deployment other than its run's is re-routed before the run fails.
	maxDeploymentMismatchRetries = 3
	// maxRerouteDelay caps the backoff between re-routes.
	maxRerouteDelay = 8 * time.Second
)

// Runtime executes deliveries. It replays the runners in its RunnerSet
// against a World as flows and steps and records their results as events.
// Whether a delivery may run is decided by appending events, so duplicate and
// stale deliveries are safe. A delivery that ends before recording its result
// is redelivered by the queue, which re-runs the step as a new attempt.
//
// A replay that diverges from the run's event log ends with a
// replayDivergenceError that runner code never sees. The Runtime queues a
// recovery delivery that replays the run again from a fresh read of its log,
// and fails the run with a corruptedEventLogError once maxReplayDivergenceRetries
// recovery deliveries have diverged too.
//
// When the World reports DeploymentAffinity, a run executes only on the
// deployment it is pinned to. A delivery that reaches another deployment is
// re-routed to the run's own, and after maxDeploymentMismatchRetries re-routes,
// or when that deployment is unavailable, the run fails with a
// deploymentMismatchError.
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
// records the step's result.
//
// A delivery for a terminal run or step returns nil without running runner or
// step code, and so does a delivery that Execute re-routes to the deployment
// its run is pinned to. A non-nil error means the delivery should be retried.
//
// Execute removes any Client NewClientContext put in ctx, so a run never uses a
// Client it inherited from the context its delivery runs in.
func (r *Runtime) Execute(ctx context.Context, input QueueInput) error {
	ctx, stop := context.WithCancel(ctx)
	defer stop()
	ctx = context.WithValue(ctx, clientContextKey{}, nil)
	ctx = context.WithValue(ctx, cancelContextKey{}, nil)
	if input.StepID != nil {
		return r.executeStep(ctx, input)
	}
	err := r.executeFlow(ctx, input)
	if errors.Is(err, errReplaySuperseded) {
		return r.supersede(ctx, input)
	}
	return err
}

// executeFlow admits root execution, replays its complete event prefix, and
// turns the runner's result into a terminal run event when it did not suspend.
func (r *Runtime) executeFlow(ctx context.Context, input QueueInput) error {
	// Load the materialized run and its complete replay prefix before any
	// runner code is called.
	run, events, err := r.loadRun(ctx, input.RunID)
	if err != nil {
		return err
	}
	if run.Status.Terminal() {
		return nil
	}
	if r.misrouted(run) {
		return r.reroute(ctx, run, input)
	}

	// Atomically admit this root delivery. Duplicate deliveries receive the
	// existing run_started event and replay from the same event history.
	eventsCount := len(events)
	startedEvent := EventsCreateEvent{
		EventType: EventTypeRunStarted,
	}
	started, err := r.world.EventsCreate(ctx, EventsCreateInput{
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
	startedLatest := Event{
		RunID:     input.RunID,
		EventID:   started.EventID,
		EventType: startedEvent.EventType,
	}

	// Build the replay context and merge the admission append without a
	// follow-up storage read.
	execution := newExecution(ctx, r.world, run, events, nil)
	execution.merge(started, startedLatest)

	// A run whose workflow is not registered fails without replaying, so it
	// has no compensations to run and no replay to check.
	runner, ok := r.runners[run.WorkflowName]
	if !ok {
		_, err := r.world.EventsCreate(ctx, EventsCreateInput{
			RunID: run.RunID,
			Event: EventsCreateEvent{EventType: EventTypeRunFailed, Data: encodeError(fmt.Errorf("%w: %s", ErrWorkflowNotFound, run.WorkflowName))},
		})
		if errors.Is(err, ErrRunTerminal) {
			return nil
		}
		return err
	}
	return r.replayFlow(input, runner, run, execution)
}

// replayFlow replays the runner once against execution and records the run's
// outcome when the runner returns. A replay that diverges from the event log
// is handed to recoverDivergence.
func (r *Runtime) replayFlow(input QueueInput, runner Runner, run Run, execution *Execution) error {
	ctx := execution.Context()
	output, err := r.run(runner, ctx, json.RawMessage(run.Input))

	// Interpret replay control flow or materialize the root outcome.
	var divergenceErr replayDivergenceError
	var deliveryErr deliveryError
	switch {
	case context.Cause(ctx) != nil:
		return context.Cause(ctx)
	case errors.Is(err, errReplayComplete), errors.Is(err, errReplaySuspended):
		return nil
	case errors.Is(err, errReplaySuperseded):
		return errReplaySuperseded
	case errors.As(err, &divergenceErr):
		return r.recoverDivergence(ctx, input, divergenceErr)
	case errors.As(err, &deliveryErr):
		if errors.Is(deliveryErr.cause, ErrRunTerminal) {
			return nil
		}
		return deliveryErr.cause
	case errors.Is(err, ErrRunCancelled):
		return nil
	case err != nil:
		failure := r.failRun(ctx, input, execution, err)
		if errors.Is(failure, errReplayComplete) || errors.Is(failure, errReplaySuspended) {
			return nil
		}
		return failure
	case run.Status.Terminal():
		return nil
	default:
		return r.completeRun(ctx, input, execution, output)
	}
}

// loadRun reads the projection and one complete event prefix before user code
// runs. Replaying primitives then use only this in-memory prefix.
func (r *Runtime) loadRun(ctx context.Context, runID RunID) (Run, []Event, error) {
	run, err := r.world.RunsGet(ctx, RunsGetInput{RunID: runID})
	if err != nil {
		return Run{}, nil, err
	}
	// Worlds must return the full current prefix when no page size is requested.
	// A bounded event source can still page internally here, but the workflow
	// receives one fully preloaded slice.
	page, err := r.world.EventsList(ctx, EventsListInput{RunID: runID})
	if err != nil {
		return Run{}, nil, err
	}
	events := page.Data
	for page.HasMore {
		page, err = r.world.EventsList(ctx, EventsListInput{RunID: runID, Pagination: Pagination{Cursor: page.Cursor}})
		if err != nil {
			return Run{}, nil, err
		}
		events = append(events, page.Data...)
	}
	return run, events, nil
}

// completeRun records successful root output unless another delivery already
// made the run terminal. A recorded operation the runner returned without
// reaching is a divergence, handed to recoverDivergence instead.
func (r *Runtime) completeRun(ctx context.Context, input QueueInput, execution *Execution, data []byte) error {
	if position, ok := execution.unreachedPosition(); ok {
		return r.recoverDivergence(ctx, input, replayDivergenceError{Position: position, Reason: "runner returned without reaching the recorded operation"})
	}
	eventCount := len(execution.events)
	_, err := r.world.EventsCreate(ctx, EventsCreateInput{
		RunID:   execution.run.RunID,
		Event:   EventsCreateEvent{EventType: EventTypeRunCompleted, Data: data},
		Options: EventsCreateOptions{EventCount: &eventCount, InBandCount: execution.InBandCount()},
	})
	if errors.Is(err, ErrRunTerminal) {
		return nil
	}
	if errors.Is(err, ErrInBandSuperseded) {
		return errReplaySuperseded
	}
	return err
}

// failRun executes registered compensations of a replayed runner that
// returned failure, then records the terminal root failure. A compensation
// records its own failed step result, so only infrastructure failures prevent
// the terminal root event from being appended. A compensation or a recorded
// operation the runner did not reach that diverges from the event log is
// handed to recoverDivergence.
func (r *Runtime) failRun(ctx context.Context, input QueueInput, execution *Execution, failure error) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			recoveredError, ok := recovered.(error)
			if !ok {
				recoveredError = fmt.Errorf("workflow: panic: %v", recovered)
			}
			var divergenceErr replayDivergenceError
			var deliveryErr deliveryError
			switch {
			case errors.As(recoveredError, &divergenceErr):
				err = r.recoverDivergence(ctx, input, divergenceErr)
			case errors.As(recoveredError, &deliveryErr):
				err = deliveryErr.cause
				if errors.Is(err, ErrRunTerminal) {
					err = nil
				}
			default:
				err = recoveredError
			}
		}
	}()
	for _, rollback := range execution.rollbacks {
		if err := rollback(ctx, execution); err != nil {
			return err
		}
	}
	if position, ok := execution.unreachedPosition(); ok {
		return r.recoverDivergence(ctx, input, replayDivergenceError{Position: position, Reason: "runner returned without reaching the recorded operation"})
	}
	_, err = r.world.EventsCreate(ctx, EventsCreateInput{
		RunID:   execution.run.RunID,
		Event:   EventsCreateEvent{EventType: EventTypeRunFailed, Data: encodeError(failure)},
		Options: EventsCreateOptions{InBandCount: execution.InBandCount()},
	})
	if errors.Is(err, ErrRunTerminal) {
		return nil
	}
	if errors.Is(err, ErrInBandSuperseded) {
		return errReplaySuperseded
	}
	return err
}

// executeStep admits one targeted delivery, replays to its declaration, and
// persists the result reported by DurableStep.
func (r *Runtime) executeStep(ctx context.Context, input QueueInput) error {
	if input.StepID == nil {
		return nil
	}
	// Load the root replay prefix and target projection before admitting the
	// targeted delivery.
	run, events, err := r.loadRun(ctx, input.RunID)
	if err != nil || run.Status.Terminal() {
		return err
	}
	if r.misrouted(run) {
		return r.reroute(ctx, run, input)
	}
	step, err := r.world.StepsGet(ctx, StepsGetInput{RunID: input.RunID, StepID: *input.StepID})
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
	started, err := r.world.EventsCreate(ctx, EventsCreateInput{
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
	startedLatest := Event{
		RunID:         input.RunID,
		EventID:       started.EventID,
		EventType:     startedEvent.EventType,
		CorrelationID: startedEvent.CorrelationID,
	}

	// Replay the root workflow with the target selected. DurableStep runs
	// only when replay reaches the matching step declaration.
	execution := newExecution(ctx, r.world, run, events, input.StepID)
	execution.merge(started, startedLatest)
	runner, ok := r.runners[run.WorkflowName]
	if !ok {
		return r.failStep(ctx, input, execution, *input.StepID, fmt.Errorf("%w: %s", ErrWorkflowNotFound, run.WorkflowName))
	}
	return r.replayStep(input, runner, run, execution, *input.StepID)
}

// replayStep replays the runner once up to the target step and records the
// step's result. A replay that diverges from the event log is handed to
// recoverDivergence.
func (r *Runtime) replayStep(input QueueInput, runner Runner, run Run, execution *Execution, stepID StepID) error {
	ctx := execution.Context()
	_, err := r.run(runner, ctx, json.RawMessage(run.Input))

	// Persist the reported target result. A failed target is reported to the
	// root replay, which decides whether the workflow itself should fail and
	// therefore whether to run compensations.
	var divergenceErr replayDivergenceError
	var deliveryErr deliveryError
	switch {
	case context.Cause(ctx) != nil:
		return context.Cause(ctx)
	case errors.Is(err, errReplayComplete):
		result, ok := execution.GetTargetResult()
		if !ok {
			return r.failStep(ctx, input, execution, stepID, errors.New("workflow: targeted step completed without a result"))
		}
		return r.completeStep(ctx, execution, stepID, result)
	case errors.Is(err, errReplaySuspended):
		return nil
	case errors.As(err, &divergenceErr):
		return r.recoverDivergence(ctx, input, divergenceErr)
	case errors.As(err, &deliveryErr):
		if errors.Is(deliveryErr.cause, ErrRunTerminal) {
			return nil
		}
		return deliveryErr.cause
	case err != nil:
		return r.failStep(ctx, input, execution, stepID, err)
	default:
		return r.failStep(ctx, input, execution, stepID, fmt.Errorf("workflow: targeted replay did not reach step %s", stepID))
	}
}

// completeStep records a targeted step result and queues either its retry or a
// root replay that can consume the terminal result.
func (r *Runtime) completeStep(ctx context.Context, execution *Execution, stepID StepID, result DataStepResult) error {
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
	input := EventsCreateInput{
		RunID:   execution.run.RunID,
		Event:   EventsCreateEvent{EventType: eventType, CorrelationID: string(stepID), Data: payload},
		Options: EventsCreateOptions{EventCount: &eventCount},
	}
	eventResult, err := r.world.EventsCreate(ctx, input)
	if errors.Is(err, ErrRunTerminal) && eventType == EventTypeStepRetrying {
		// A terminal run permits the admitted step's final failure, but no retry.
		result.RetryAfter = nil
		payload, err = json.Marshal(result)
		if err != nil {
			return err
		}
		eventType = EventTypeStepFailed
		input.Event.EventType, input.Event.Data = eventType, payload
		eventResult, err = r.world.EventsCreate(ctx, input)
	}
	if errors.Is(err, ErrRunTerminal) || errors.Is(err, ErrStepNotStartable) {
		return nil
	}
	if err != nil {
		return err
	}
	execution.merge(eventResult, Event{
		RunID:         execution.run.RunID,
		EventID:       eventResult.EventID,
		EventType:     eventType,
		CorrelationID: string(stepID),
		Data:          payload,
	})
	queue := QueueInput{RunID: execution.run.RunID, IdempotencyKey: string(eventResult.EventID), DeploymentID: execution.run.DeploymentID}
	if eventType == EventTypeStepRetrying {
		queue.StepID = &stepID
		queue.NotBefore = result.RetryAfter
	}
	_, err = r.world.Queue(ctx, queue)
	return err
}

// failStep runs the registered compensations up to a targeted compensation
// step, or records the target's failure. A compensation that diverges from the
// event log is handed to recoverDivergence.
func (r *Runtime) failStep(ctx context.Context, input QueueInput, execution *Execution, stepID StepID, failure error) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			recoveredError, ok := recovered.(error)
			if !ok {
				recoveredError = fmt.Errorf("workflow: panic: %v", recovered)
			}
			var divergenceErr replayDivergenceError
			var deliveryErr deliveryError
			switch {
			case errors.As(recoveredError, &divergenceErr):
				err = r.recoverDivergence(ctx, input, divergenceErr)
			case errors.Is(recoveredError, errReplayComplete):
				result, ok := execution.GetTargetResult()
				if !ok {
					err = errors.New("workflow: targeted rollback completed without a result")
					return
				}
				err = r.completeStep(ctx, execution, stepID, result)
			case errors.Is(recoveredError, errReplaySuspended):
				err = nil
			case errors.As(recoveredError, &deliveryErr):
				err = deliveryErr.cause
				if errors.Is(err, ErrRunTerminal) {
					err = nil
				}
			default:
				if execution.IsSuspended() {
					err = recoveredError
				} else {
					err = r.completeStep(ctx, execution, stepID, DataStepResult{Error: encodeError(recoveredError)})
				}
			}
		}
	}()
	for _, rollback := range execution.rollbacks {
		if err := rollback(ctx, execution); err != nil {
			return err
		}
	}
	return r.completeStep(ctx, execution, stepID, DataStepResult{Error: encodeError(failure)})
}

// run preserves error panics, including replay control, and converts other
// application panic values into workflow errors.
func (r *Runtime) run(runner Runner, ctx context.Context, input json.RawMessage) (output json.RawMessage, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			if recoveredError, ok := recovered.(error); ok {
				err = recoveredError
				return
			}
			err = fmt.Errorf("workflow: panic: %v", recovered)
		}
	}()
	output, err = runner.Run(ctx, input)
	return output, err
}

// supersede queues a follow-up of a flow whose in-band write was refused,
// because another flow wrote to the run first, and acknowledges the flow.
// Acknowledging it without a follow-up could lose the result that queued it,
// which the other flow may have loaded the run too early to see. The
// follow-up's idempotency key names the run's in-band count, so flows
// superseded by the same write share one follow-up on a World that
// deduplicates, instead of superseding each other again.
func (r *Runtime) supersede(ctx context.Context, input QueueInput) error {
	run, err := r.world.RunsGet(ctx, RunsGetInput{RunID: input.RunID, ResolveData: ResolveDataNone})
	if err != nil {
		return err
	}
	if run.Status.Terminal() {
		return nil
	}
	followUp := input
	followUp.NotBefore = nil
	followUp.IdempotencyKey = fmt.Sprintf("superseded:%s:%d", run.RunID, run.InBandCount)
	followUp.DeploymentID = run.DeploymentID
	_, err = r.world.Queue(ctx, followUp)
	return err
}

// recoverDivergence queues input again as a fresh, immediate delivery of the
// same flow or step that counts the divergence, and acknowledges this
// delivery, so the recovery replay reads the event log anew and may run on
// another worker. Once maxReplayDivergenceRetries recovery deliveries have
// diverged too, it fails the run with a corruptedEventLogError, without
// running compensations, since they would replay against the same diverging
// log.
func (r *Runtime) recoverDivergence(ctx context.Context, input QueueInput, divergenceErr replayDivergenceError) error {
	if input.ReplayDivergenceCount >= maxReplayDivergenceRetries {
		corrupted := corruptedEventLogError{Position: divergenceErr.Position, Reason: divergenceErr.Reason, RecoveryReplays: input.ReplayDivergenceCount}
		_, err := r.world.EventsCreate(ctx, EventsCreateInput{
			RunID: input.RunID,
			Event: EventsCreateEvent{EventType: EventTypeRunFailed, Data: encodeError(corrupted)},
		})
		if errors.Is(err, ErrRunTerminal) {
			return nil
		}
		return err
	}
	input.NotBefore = nil
	input.ReplayDivergenceCount++
	_, err := r.world.Queue(ctx, input)
	return err
}

// misrouted reports whether run is pinned to a deployment other than the
// World's own, in a World that enforces deployment affinity.
func (r *Runtime) misrouted(run Run) bool {
	return run.DeploymentID != "" && r.world.Capabilities().DeploymentAffinity && run.DeploymentID != r.world.DeploymentID()
}

// reroute queues input again, addressed to the deployment run is pinned to,
// with a growing delay, and acknowledges this delivery. Once input has been
// re-routed maxDeploymentMismatchRetries times, or the World reports the
// deployment unavailable, it fails the run with a deploymentMismatchError.
func (r *Runtime) reroute(ctx context.Context, run Run, input QueueInput) error {
	attempts := input.DeploymentMismatchRetryCount
	if attempts < maxDeploymentMismatchRetries {
		notBefore := time.Now().Add(min(time.Second<<attempts, maxRerouteDelay))
		input.NotBefore = &notBefore
		input.DeploymentID = run.DeploymentID
		input.DeploymentMismatchRetryCount = attempts + 1
		_, err := r.world.Queue(ctx, input)
		if !errors.Is(err, ErrDeploymentUnavailable) {
			return err
		}
	}
	mismatch := deploymentMismatchError{RunDeploymentID: run.DeploymentID, DeploymentID: r.world.DeploymentID(), RecoveryAttempts: attempts}
	_, err := r.world.EventsCreate(ctx, EventsCreateInput{
		RunID: run.RunID,
		Event: EventsCreateEvent{EventType: EventTypeRunFailed, Data: encodeError(mismatch)},
	})
	if errors.Is(err, ErrRunTerminal) {
		return nil
	}
	return err
}
