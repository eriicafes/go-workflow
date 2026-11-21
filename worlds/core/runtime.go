package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"maps"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
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
//
// A flow is the run's orchestrator: it makes every decision, and its writes
// are in-band, fenced by the run's in-band count, so when two deliveries of
// a run's flow overlap, the World refuses the writes of the one that read
// the log first, which asks for its message again a second later. A step
// delivery runs one attempt of a step and records it out-of-band, and then
// queues a flow to consume it. Whether a delivery may run is decided from the
// event log alone, so duplicate and stale deliveries are safe.
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
	world   worlds.World
	runners RunnerSet
	// runAheadDepth is how many inline steps' results a flow hands to the
	// runner ahead of their writes.
	runAheadDepth int
}

// fenceRefusalRedelivery is how long a flow whose in-band write the World
// refused waits for its message again: long enough for the delivery that
// wrote first to finish, short enough that a refusal costs about a second.
const fenceRefusalRedelivery = time.Second

// NewRuntime returns a Runtime that executes deliveries for runs recorded in
// world using runners. It copies runners.
func NewRuntime(world worlds.World, runners RunnerSet, options ...RuntimeOption) *Runtime {
	runtime := &Runtime{world: world, runners: maps.Clone(runners), runAheadDepth: DefaultRunAheadDepth}
	for _, option := range options {
		option(runtime)
	}
	return runtime
}

// Execute handles one delivery. With a nil StepID it executes a flow, which
// replays the run's runner and, when the runner returns instead of suspending,
// appends run_completed or run_failed. With StepID set it executes a step,
// which replays the runner up to that step, executes the step function, and
// records the step's result.
//
// A delivery for a terminal run or step returns nil without running runner or
// step code, and so does a delivery that Execute re-routes to the deployment
// its run is pinned to. A *worlds.Redelivery asks the World for the same
// message again, and any other non-nil error means the delivery should be
// retried.
//
// Execute removes any Client NewClientContext put in ctx, so a run never uses a
// Client it inherited from the context its delivery runs in.
func (r *Runtime) Execute(ctx context.Context, input worlds.QueueInput) error {
	ctx, stop := context.WithCancel(ctx)
	defer stop()
	ctx = worlds.NewClientContext(ctx, nil)
	ctx = context.WithValue(ctx, cancelContextKey{}, nil)
	if input.Attempt > MaxDeliveries {
		if input.StepID == nil || r.stepStalled(ctx, input) {
			return r.exceedDeliveries(ctx, input)
		}
	}
	if input.StepID != nil {
		return r.executeStep(ctx, input)
	}
	if input.Intermediate() {
		return r.continueWaitHop(ctx, input)
	}
	// A pass whose run-ahead stopped starts again from a fresh read of the
	// log, with run-ahead off for the rest of the delivery, once, and then
	// hands its message on. A pass that has a repair due replays again from
	// the log, with run-ahead still on.
	// A pass that suspended lingers while the run waits on work done
	// elsewhere, and runs again when another writer appends an event or a
	// wait falls due (see linger.go).
	restarts, lingered := 0, false
	var pass *Execution
	err := r.executeFlow(ctx, input, flowPass{runAhead: true, first: true}, &pass)
	for {
		var stop errRunAheadStop
		if errors.As(err, &stop) {
			if restarts == maxRunAheadRestarts {
				// The next delivery decides from the log.
				log.Printf("workflow: run %s handed off after its run-ahead stopped again after a restart: %s", input.RunID, stop.reason)
				return worlds.Redeliver(0)
			}
			restarts++
			err = r.executeFlow(ctx, input, flowPass{lingered: lingered}, &pass)
			continue
		}
		if errors.As(err, new(errRunAheadRepair)) {
			err = r.executeFlow(ctx, input, flowPass{runAhead: restarts == 0, lingered: lingered}, &pass)
			continue
		}
		if err == nil && pass != nil && pass.parked && r.linger(ctx, pass) {
			lingered = true
			err = r.executeFlow(ctx, input, flowPass{runAhead: restarts == 0, lingered: lingered}, &pass)
			continue
		}
		break
	}
	if errors.Is(err, errStartRefused) {
		return nil
	}
	if errors.Is(err, errReplaySuperseded) {
		if count, warn := fenceRefusals.note(input.RunID); warn {
			log.Printf("workflow: warning: run %s had its writes refused by the in-band fence %d times in a row", input.RunID, count)
		}
		return worlds.Redeliver(fenceRefusalRedelivery)
	}
	fenceRefusals.clear(input.RunID)
	return err
}

// flowPass is how one pass of a flow delivery runs: whether it may run ahead,
// whether the delivery lingered before it, and whether it is the delivery's
// first.
type flowPass struct {
	runAhead, lingered, first bool
}

// TurboFirstDeliveries turns on the turbo path of a run's first flow: the
// first delivery of its first message replays without reading the log,
// writes run_started without waiting for it, and starts its inline steps
// before their writes commit, as the Workflow SDK's turbo mode does.
var TurboFirstDeliveries = true

// isTurbo reports whether the pass takes the turbo path: the first pass of the
// first delivery of a run's first message, which carries the run's creation,
// and not a start hook's candidate, which its run_started may refuse.
func isTurbo(input worlds.QueueInput, pass flowPass) bool {
	return TurboFirstDeliveries && pass.first && input.RunInput != nil && input.RunInput.StartHook == nil &&
		input.DeliveryCount == 1 && input.Attempt <= 1 && input.WaitContinuation == nil && len(input.CompleteWaits) == 0
}

// executeFlow loads the run's log, records run_started on the run's first
// flow, replays the runner, and turns the runner's result into a terminal run
// event when it did not suspend.
func (r *Runtime) executeFlow(ctx context.Context, input worlds.QueueInput, pass flowPass, current **Execution) error {
	*current = nil
	if isTurbo(input, pass) {
		return r.executeTurbo(ctx, input, pass, current)
	}
	run, events, err := r.loadRun(ctx, input.RunID)
	if errors.Is(err, worlds.ErrRunNotFound) && input.RunInput != nil {
		// The start's run_created never landed: its first flow creates the
		// run, or exits when its start hook is refused.
		if err := r.createRunForStart(ctx, input); err != nil {
			return err
		}
		run, events, err = r.loadRun(ctx, input.RunID)
	}
	if err != nil {
		return err
	}
	if run.Status.Terminal() {
		return nil
	}
	if r.misrouted(run) {
		return r.reroute(ctx, run, input)
	}
	execution, err := newExecution(ctx, r.world, run, events, nil)
	if err != nil {
		return err
	}
	execution.delivery = input
	execution.lingered = pass.lingered
	*current = execution
	if pass.runAhead {
		execution.aheadDepth = r.runAheadDepth
	}
	if execution.keys, err = worlds.RunKeysFor(ctx, r.world, run.RunID, run.DeploymentID); err != nil {
		return err
	}
	if run.Status == worlds.StatusPending {
		if err := r.startRun(execution); err != nil {
			return err
		}
	}

	// A run whose workflow is not registered fails without replaying, so it
	// has no compensations to run and no replay to check.
	runner, ok := r.runners[run.WorkflowName]
	if !ok {
		return r.recordRunEnd(execution, worlds.EventTypeRunFailed, execution.runFailedData(fmt.Errorf("%w: %s", worlds.ErrWorkflowNotFound, run.WorkflowName)))
	}
	return r.replayFlow(input, runner, run, execution)
}

// startRun records run_started as an in-band write of the run's first flow,
// carrying the run's creation when its message does.
func (r *Runtime) startRun(execution *Execution) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = replayExit(recovered)
		}
	}()
	execution.CreateEvent(worlds.EventsCreateEvent{EventType: worlds.EventTypeRunStarted, Data: runStartedData(execution.delivery)}, nil)
	return nil
}

// runStartedData returns the Data of a flow's run_started: the run's
// creation from its first message, which creates the run when its
// run_created never landed, or nil.
func runStartedData(input worlds.QueueInput) []byte {
	if input.RunInput == nil {
		return nil
	}
	data, _ := worlds.RunCreatedData(*input.RunInput)
	return data
}

// createRunForStart creates the run of a first flow whose start's run_created
// never landed. A start hook the World refuses ends the delivery.
func (r *Runtime) createRunForStart(ctx context.Context, input worlds.QueueInput) error {
	err := worlds.CreateRunForStart(ctx, r.world, input.RunID, runStartedData(input))
	var conflict *worlds.HookConflictError
	if errors.As(err, &conflict) {
		return errStartRefused
	}
	return err
}

// errStartRefused ends a first flow whose start hook the World refused: it
// acknowledges its message without creating a run.
var errStartRefused = errors.New("workflow: the run's start was refused")

// executeTurbo runs the turbo pass of a run's first flow. The log is taken to
// hold only run_created, as the run's creation in the message records it, and
// run_started is written ahead of the replay, as a write ahead the flow's
// other writes queue behind. When the run had already started, as on a
// platform's replay of a first delivery, the delivery replays again from the
// log.
func (r *Runtime) executeTurbo(ctx context.Context, input worlds.QueueInput, pass flowPass, current **Execution) error {
	create := *input.RunInput
	create.RunID = input.RunID
	created, err := worlds.RunCreatedData(create)
	if err != nil {
		return err
	}
	createdAt, err := runCreatedAt(input.RunID)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	attributes := create.Attributes
	if attributes == nil {
		attributes = map[string]string{}
	}
	run := worlds.Run{RunID: input.RunID, WorkflowName: create.WorkflowName, Status: worlds.StatusRunning, Input: create.Input, Attributes: attributes, DeploymentID: create.DeploymentID,
		EncryptionPublicKey: create.EncryptionPublicKey, ExecutionContext: create.ExecutionContext, InBandCount: 1, SpecVersion: worlds.SpecVersion, CreatedAt: now, UpdatedAt: now, StartedAt: &now}
	events := []worlds.Event{{RunID: input.RunID, EventID: worlds.EventIDForSlot(1), EventType: worlds.EventTypeRunCreated, Data: created, CreatedAt: time.UnixMilli(int64(createdAt)).UTC(), SpecVersion: worlds.SpecVersion}}
	if r.misrouted(run) {
		return r.reroute(ctx, run, input)
	}
	execution, err := newExecution(ctx, r.world, run, events, nil)
	if err != nil {
		return err
	}
	execution.delivery = input
	*current = execution
	if pass.runAhead {
		execution.aheadDepth = r.runAheadDepth
	}
	if execution.keys, err = worlds.RunKeysFor(ctx, r.world, run.RunID, run.DeploymentID); err != nil {
		return err
	}
	execution.writeStartAhead(created)
	runner, ok := r.runners[run.WorkflowName]
	if !ok {
		return r.recordRunEnd(execution, worlds.EventTypeRunFailed, execution.runFailedData(fmt.Errorf("%w: %s", worlds.ErrWorkflowNotFound, run.WorkflowName)))
	}
	return r.replayFlow(input, runner, run, execution)
}

// replayExit turns the panic that ended a replay outside runner code into
// the error the delivery returns.
func replayExit(recovered any) error {
	err, ok := recovered.(error)
	if !ok {
		return fmt.Errorf("workflow: panic: %v", recovered)
	}
	var delivery deliveryError
	if errors.As(err, &delivery) {
		if errors.Is(delivery.cause, worlds.ErrRunTerminal) {
			return nil
		}
		return delivery.cause
	}
	return err
}

// replayFlow replays the runner once against execution and records the run's
// outcome when the runner returns. Steps the flow still runs inline record
// their results before the delivery ends. A replay that diverges from the
// event log is handed to recoverDivergence.
func (r *Runtime) replayFlow(input worlds.QueueInput, runner Runner, run worlds.Run, execution *Execution) (err error) {
	ctx := execution.Context()
	if resendErr := r.resend(execution); resendErr != nil {
		return resendErr
	}
	output, err := r.run(runner, ctx, run.Input)
	// Steps and timers the runner declared and no longer waits for are not
	// created.
	execution.pending, execution.pendingWaits = nil, nil
	defer func() {
		if finishErr := execution.finishSteps(); finishErr != nil {
			if errors.As(finishErr, new(errRunAheadStop)) && !errors.As(err, new(errRunAheadStop)) {
				// Found while the flow finished its steps.
				if armErr := r.armCreatedWaitTimers(execution); armErr != nil {
					finishErr = errors.Join(finishErr, armErr)
				}
			}
			err = errors.Join(err, finishErr)
		}
		if execution.superseded && err == nil {
			err = errReplaySuperseded
		}
		if err == nil {
			err = r.armTimers(execution)
		}
	}()

	// Interpret replay control flow or materialize the root outcome.
	var divergenceErr replayDivergenceError
	var deliveryErr deliveryError
	switch {
	case context.Cause(ctx) != nil:
		return context.Cause(ctx)
	case errors.Is(err, errReplayComplete):
		return nil
	case errors.Is(err, errReplaySuspended):
		execution.parked = true
		return nil
	case errors.Is(err, errReplaySuperseded):
		return errReplaySuperseded
	case errors.As(err, new(errRunAheadStop)):
		// The flow decides nothing more: it writes the results of the steps
		// whose start committed, and starts again from the log. The delivery
		// may end in a handoff, which a World may continue on a new message
		// that does not own this message's chain.
		if armErr := r.armCreatedWaitTimers(execution); armErr != nil {
			return armErr
		}
		return err
	case errors.As(err, new(errRunAheadRepair)):
		// The flow writes the results of the steps it runs, and replays
		// again from the log.
		return err
	case errors.As(err, &divergenceErr):
		return r.recoverDivergence(ctx, input, divergenceErr)
	case errors.As(err, &deliveryErr):
		if errors.Is(deliveryErr.cause, worlds.ErrRunTerminal) {
			return nil
		}
		// A step started before its retry was due, by a clock behind the
		// World's: deliver it again when the retry is due.
		var early *worlds.TooEarlyError
		if errors.As(deliveryErr.cause, &early) {
			return worlds.Redeliver(early.RetryAfter)
		}
		return deliveryErr.cause
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

// resend queues again the step deliveries a delivery that stopped may have
// left unqueued, and completes the waits the delivery wakes the run for.
func (r *Runtime) resend(execution *Execution) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = replayExit(recovered)
		}
	}()
	// A pass after the delivery lingered finds what the first sent.
	if !execution.lingered {
		execution.resendSteps()
	}
	execution.completeWaitsOnWake()
	return nil
}

// armCreatedWaitTimers sends the timers a handoff owes.
func (r *Runtime) armCreatedWaitTimers(execution *Execution) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = replayExit(recovered)
		}
	}()
	execution.armCreatedWaitTimers()
	return nil
}

// armTimers queues the timers the flow owns as it ends.
func (r *Runtime) armTimers(execution *Execution) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = replayExit(recovered)
		}
	}()
	execution.armTimers()
	return nil
}

// loadRun reads the run and its complete event log before user code runs,
// and takes the in-band count that fences the flow's writes from the
// snapshot the log was listed under. Replaying primitives then use only this
// in-memory log.
func (r *Runtime) loadRun(ctx context.Context, runID worlds.RunID) (worlds.Run, []worlds.Event, error) {
	run, err := r.world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID})
	if err != nil {
		return worlds.Run{}, nil, err
	}
	page, err := r.world.EventsList(ctx, worlds.EventsListInput{RunID: runID})
	if err != nil {
		return worlds.Run{}, nil, err
	}
	// The flow fences its writes with the in-band count of the snapshot the
	// listing was read under, which covers every event it returns.
	run.InBandCount = page.Snapshot.InBandCount
	events := page.Data
	for page.HasMore {
		page, err = r.world.EventsList(ctx, worlds.EventsListInput{RunID: runID, Pagination: worlds.Pagination{Cursor: page.Cursor}})
		if err != nil {
			return worlds.Run{}, nil, err
		}
		events = append(events, page.Data...)
	}
	return run, events, nil
}

// completeRun records successful root output unless another delivery already
// made the run terminal. A recorded operation the runner returned without
// reaching is a divergence, handed to recoverDivergence instead.
func (r *Runtime) completeRun(ctx context.Context, input worlds.QueueInput, execution *Execution, data []byte) error {
	if id, ok := execution.unreached(); ok {
		return r.recoverDivergence(ctx, input, replayDivergenceError{CorrelationID: id, Reason: "runner returned without reaching the recorded operation"})
	}
	completed, err := json.Marshal(worlds.DataRunCompleted{Output: data})
	if err != nil {
		return r.failRun(ctx, input, execution, fmt.Errorf("workflow: encode run output: %w", err))
	}
	return r.recordRunEnd(execution, worlds.EventTypeRunCompleted, completed)
}

// recordRunEnd records the run's end as an in-band write of the flow, unless
// another delivery already made the run terminal.
func (r *Runtime) recordRunEnd(execution *Execution, eventType worlds.EventType, data []byte) (err error) {
	if execution.superseded {
		return errReplaySuperseded
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			err = replayExit(recovered)
		}
	}()
	execution.CreateEvent(worlds.EventsCreateEvent{EventType: eventType, Data: data}, nil)
	return nil
}

// failRun executes registered compensations of a replayed runner that
// returned failure, then records the terminal root failure. A compensation
// records its own failed step result, so only infrastructure failures prevent
// the terminal root event from being appended. A compensation or a recorded
// operation the runner did not reach that diverges from the event log is
// handed to recoverDivergence.
func (r *Runtime) failRun(ctx context.Context, input worlds.QueueInput, execution *Execution, failure error) (err error) {
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
				if errors.Is(err, worlds.ErrRunTerminal) {
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
	if id, ok := execution.unreached(); ok {
		return r.recoverDivergence(ctx, input, replayDivergenceError{CorrelationID: id, Reason: "runner returned without reaching the recorded operation"})
	}
	return r.recordRunEnd(execution, worlds.EventTypeRunFailed, execution.runFailedData(failure))
}

// executeStep runs the attempt of a step delivery. It decides from the log:
// a finished run or step is acknowledged, a retry not yet due asks for the
// message again when it is, and otherwise the runner replays to the step,
// whose attempt DurableStep starts, runs, or fails when its attempts are all
// used. Every write is out-of-band, and once the step's result is final the
// delivery queues a flow to consume it.
func (r *Runtime) executeStep(ctx context.Context, input worlds.QueueInput) error {
	if input.StepID == nil {
		return nil
	}
	if due, ok := r.pendingRetry(ctx, input); ok {
		return worlds.Redeliver(time.Until(due))
	}
	run, events, err := r.loadRun(ctx, input.RunID)
	if err != nil || run.Status.Terminal() {
		return err
	}
	if r.misrouted(run) {
		return r.reroute(ctx, run, input)
	}
	stepID := *input.StepID
	if _, ok := stepCreation(events, stepID); !ok {
		return nil
	}
	if _, ok := stepFinal(events, stepID); ok {
		// The delivery that recorded the result may have stopped before it
		// queued the flow that consumes it.
		return r.wake(ctx, run)
	}
	if due, ok := stepRetryDue(events, stepID); ok && time.Now().Before(due) {
		return worlds.Redeliver(time.Until(due))
	}
	execution, err := newExecution(ctx, r.world, run, events, input.StepID)
	if err != nil {
		return err
	}
	execution.delivery = input
	if execution.keys, err = worlds.RunKeysFor(ctx, r.world, run.RunID, run.DeploymentID); err != nil {
		return err
	}
	runner, ok := r.runners[run.WorkflowName]
	if !ok {
		execution.targetAttempt = 1
		return r.completeStep(ctx, execution, stepID, worlds.DataStepResult{Error: execution.encodeError(fmt.Errorf("%w: %s", worlds.ErrWorkflowNotFound, run.WorkflowName))})
	}
	return r.replayStep(input, runner, run, execution, stepID)
}

// stepFinal returns the first final event of step stepID among events.
func stepFinal(events []worlds.Event, stepID worlds.StepID) (worlds.Event, bool) {
	for _, event := range events {
		if event.CorrelationID == string(stepID) && (event.EventType == worlds.EventTypeStepCompleted || event.EventType == worlds.EventTypeStepFailed) {
			return event, true
		}
	}
	return worlds.Event{}, false
}

// stepRetryDue returns when the retry of step stepID is due, when its last
// event is a step_retrying.
func stepRetryDue(events []worlds.Event, stepID worlds.StepID) (time.Time, bool) {
	var due *time.Time
	for _, event := range events {
		if event.CorrelationID != string(stepID) {
			continue
		}
		switch event.EventType {
		case worlds.EventTypeStepStarted:
			due = nil
		case worlds.EventTypeStepRetrying:
			var data worlds.DataStepResult
			if json.Unmarshal(event.Data, &data) == nil {
				due = data.RetryAfter
			}
		}
	}
	if due == nil {
		return time.Time{}, false
	}
	return *due, true
}

// wake queues a flow of run, without an idempotency key, so the flow
// consumes what the delivery recorded.
func (r *Runtime) wake(ctx context.Context, run worlds.Run) error {
	_, err := r.world.Queue(ctx, worlds.QueueInput{RunID: run.RunID, WorkflowName: run.WorkflowName, DeploymentID: run.DeploymentID, RunSpecVersion: worlds.SpecVersion})
	return err
}

// replayStep replays the runner once up to the target step and records the
// step's result. A replay that diverges from the event log is handed to
// recoverDivergence.
func (r *Runtime) replayStep(input worlds.QueueInput, runner Runner, run worlds.Run, execution *Execution, stepID worlds.StepID) error {
	ctx := execution.Context()
	_, err := r.run(runner, ctx, run.Input)

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
		if errors.Is(deliveryErr.cause, worlds.ErrRunTerminal) {
			return nil
		}
		// A step started before its retry was due, by a clock behind the
		// World's: deliver it again when the retry is due.
		var early *worlds.TooEarlyError
		if errors.As(deliveryErr.cause, &early) {
			return worlds.Redeliver(early.RetryAfter)
		}
		return deliveryErr.cause
	case err != nil:
		return r.failStep(ctx, input, execution, stepID, err)
	default:
		return r.failStep(ctx, input, execution, stepID, fmt.Errorf("workflow: targeted replay did not reach step %s", stepID))
	}
}

// completeStep records a targeted step's result out-of-band. A retry asks for
// the step's message again once it is due, and a final result queues a flow
// that consumes it.
func (r *Runtime) completeStep(ctx context.Context, execution *Execution, stepID worlds.StepID, result worlds.DataStepResult) error {
	event, err := execution.recordStepResult(stepID, max(execution.targetAttempt, 1), result, false)
	if err != nil || event.EventID == "" {
		return err
	}
	if event.EventType == worlds.EventTypeStepRetrying && result.RetryAfter != nil {
		return worlds.Redeliver(time.Until(*result.RetryAfter))
	}
	return r.wake(ctx, execution.run)
}

// failStep runs the registered compensations up to a targeted compensation
// step, or records the target's failure. A compensation that diverges from the
// event log is handed to recoverDivergence.
func (r *Runtime) failStep(ctx context.Context, input worlds.QueueInput, execution *Execution, stepID worlds.StepID, failure error) (err error) {
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
				if errors.Is(err, worlds.ErrRunTerminal) {
					err = nil
				}
			default:
				if execution.IsSuspended() {
					err = recoveredError
				} else {
					err = r.completeStep(ctx, execution, stepID, worlds.DataStepResult{Error: execution.encodeError(recoveredError)})
				}
			}
		}
	}()
	for _, rollback := range execution.rollbacks {
		if err := rollback(ctx, execution); err != nil {
			return err
		}
	}
	return r.completeStep(ctx, execution, stepID, worlds.DataStepResult{Error: execution.encodeError(failure)})
}

// run preserves error panics, including replay control, and converts other
// application panic values into workflow errors.
func (r *Runtime) run(runner Runner, ctx context.Context, input []byte) (output []byte, err error) {
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

// recoverDivergence queues input again as a fresh, immediate delivery of the
// same flow or step that counts the divergence, and acknowledges this
// delivery, so the recovery replay reads the event log anew and may run on
// another worker. Once maxReplayDivergenceRetries recovery deliveries have
// diverged too, it fails the run with a corruptedEventLogError, without
// running compensations, since they would replay against the same diverging
// log.
func (r *Runtime) recoverDivergence(ctx context.Context, input worlds.QueueInput, divergenceErr replayDivergenceError) error {
	if input.ReplayDivergenceCount >= maxReplayDivergenceRetries {
		corrupted := corruptedEventLogError{CorrelationID: divergenceErr.CorrelationID, Reason: divergenceErr.Reason, RecoveryReplays: input.ReplayDivergenceCount}
		data, err := r.runFailedData(ctx, input.RunID, input.DeploymentID, corrupted)
		if err != nil {
			return err
		}
		_, err = r.world.EventsCreate(ctx, worlds.EventsCreateInput{
			RunID: input.RunID,
			Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeRunFailed, Data: data},
		})
		if errors.Is(err, worlds.ErrRunTerminal) {
			return nil
		}
		return err
	}
	// The recovery is a new message, which the key of the one being
	// delivered, still live, would absorb.
	input.NotBefore, input.IdempotencyKey = nil, ""
	input.ReplayDivergenceCount++
	_, err := r.world.Queue(ctx, input)
	return err
}

// misrouted reports whether run is pinned to a deployment other than the
// World's own, in a World that enforces deployment affinity.
func (r *Runtime) misrouted(run worlds.Run) bool {
	return run.DeploymentID != "" && r.world.Capabilities().DeploymentAffinity && run.DeploymentID != r.world.DeploymentID()
}

// reroute queues input again, addressed to the deployment run is pinned to,
// with a growing delay, and acknowledges this delivery. Once input has been
// re-routed maxDeploymentMismatchRetries times, or the World reports the
// deployment unavailable, it fails the run with a deploymentMismatchError.
func (r *Runtime) reroute(ctx context.Context, run worlds.Run, input worlds.QueueInput) error {
	attempts := input.DeploymentMismatchRetryCount
	if attempts < maxDeploymentMismatchRetries {
		notBefore := time.Now().Add(min(time.Second<<attempts, maxRerouteDelay))
		input.NotBefore = &notBefore
		input.WorkflowName = run.WorkflowName
		input.DeploymentID = run.DeploymentID
		input.DeploymentMismatchRetryCount = attempts + 1
		// The re-route is a new message, which the key of the one being
		// delivered, still live, would absorb.
		input.IdempotencyKey = ""
		_, err := r.world.Queue(ctx, input)
		if !errors.Is(err, worlds.ErrDeploymentUnavailable) {
			return err
		}
	}
	mismatch := deploymentMismatchError{RunDeploymentID: run.DeploymentID, DeploymentID: r.world.DeploymentID(), RecoveryAttempts: attempts}
	data, err := r.runFailedData(ctx, run.RunID, run.DeploymentID, mismatch)
	if err != nil {
		return err
	}
	_, err = r.world.EventsCreate(ctx, worlds.EventsCreateInput{
		RunID: run.RunID,
		Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeRunFailed, Data: data},
	})
	if errors.Is(err, worlds.ErrRunTerminal) {
		return nil
	}
	return err
}

// stepStallWindow is how long the log of a step whose message is delivered
// past MaxDeliveries must show no activity before the step is failed: its
// last creation, start, or retry, or a retry's due time, is older than that.
// A delivery that runs the step writes its start first, and one that defers
// follows a recent write or a pending retry, so only a step none of whose
// deliveries got as far as writing anything is failed. Every World keeps
// redelivering a failing step message at least this long.
const stepStallWindow = time.Hour

// stepStalled reports whether the step of input, delivered past
// MaxDeliveries, has shown no activity for stepStallWindow. A step whose log
// cannot be read is taken as stalled.
// pendingRetry returns when the retry a step delivery waits for is due, read
// from the step's latest event alone before the delivery reads the whole
// log, when that event is a step_retrying not yet due. Only a delivery that
// may be a retry's early redelivery reads it: a redelivery of the message, or
// one of an inline step's retry handed to a step delivery. A failed read
// leaves the decision to the whole log.
func (r *Runtime) pendingRetry(ctx context.Context, input worlds.QueueInput) (time.Time, bool) {
	if input.DeliveryCount == 1 && input.StepAttempt <= 1 {
		return time.Time{}, false
	}
	page, err := r.world.EventsListByCorrelationID(ctx, worlds.EventsListByCorrelationIDInput{
		RunID: input.RunID, CorrelationID: string(*input.StepID),
		Pagination: worlds.Pagination{Limit: 1, SortOrder: worlds.SortOrderDescending},
	})
	if err != nil || len(page.Data) == 0 || page.Data[0].EventType != worlds.EventTypeStepRetrying {
		return time.Time{}, false
	}
	var data worlds.DataStepResult
	if json.Unmarshal(page.Data[0].Data, &data) != nil || data.RetryAfter == nil || !time.Now().Before(*data.RetryAfter) {
		return time.Time{}, false
	}
	return *data.RetryAfter, true
}

func (r *Runtime) stepStalled(ctx context.Context, input worlds.QueueInput) bool {
	var events []worlds.Event
	pagination := worlds.Pagination{Limit: -1}
	for {
		page, err := r.world.EventsListByCorrelationID(ctx, worlds.EventsListByCorrelationIDInput{RunID: input.RunID, CorrelationID: string(*input.StepID), Pagination: pagination})
		if err != nil {
			return true
		}
		events = append(events, page.Data...)
		if !page.HasMore {
			break
		}
		pagination.Cursor = page.Cursor
	}
	since := time.Now().Add(-stepStallWindow)
	for _, event := range events {
		switch event.EventType {
		case worlds.EventTypeStepCreated, worlds.EventTypeStepStarted:
			if event.CreatedAt.After(since) {
				return false
			}
		case worlds.EventTypeStepRetrying:
			var data worlds.DataStepResult
			_ = json.Unmarshal(event.Data, &data)
			if event.CreatedAt.After(since) || data.RetryAfter != nil && data.RetryAfter.After(since) {
				return false
			}
		}
	}
	return true
}

// MaxDeliveries is the most attempts of one delivery Runtime.Execute makes.
// With a World's retry backoff it spans hours, short of a queue's retention,
// so a delivery that keeps failing fails its run or step instead of expiring
// unseen.
const MaxDeliveries = 48

// exceedDeliveries fails the run, or the step, of a delivery attempted more
// than MaxDeliveries times. A failed step is handed to a flow, which decides
// whether the run fails. It returns nil once the failure is recorded or
// another delivery has finished the run or step, so the delivery is
// acknowledged.
func (r *Runtime) exceedDeliveries(ctx context.Context, input worlds.QueueInput) error {
	cause := worlds.FatalError{Cause: fmt.Errorf("%w (%d attempts)", worlds.ErrMaxDeliveriesExceeded, input.Attempt-1)}
	log.Printf("workflow: delivery %s for run %s exceeded %d attempts", input.MessageID, input.RunID, MaxDeliveries)
	run, err := r.world.RunsGet(ctx, worlds.RunsGetInput{RunID: input.RunID, ResolveData: worlds.ResolveDataNone})
	if errors.Is(err, worlds.ErrRunNotFound) {
		return nil
	}
	if err != nil || run.Status.Terminal() {
		return err
	}
	if input.StepID == nil {
		data, err := r.runFailedData(ctx, run.RunID, run.DeploymentID, cause)
		if err != nil {
			return err
		}
		_, err = r.world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: input.RunID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeRunFailed, Data: data}})
		if errors.Is(err, worlds.ErrRunTerminal) {
			return nil
		}
		return err
	}
	_, events, err := r.loadRun(ctx, input.RunID)
	if err != nil {
		return err
	}
	stepID := *input.StepID
	if _, ok := stepCreation(events, stepID); !ok {
		return nil
	}
	if _, final := stepFinal(events, stepID); !final {
		keys, err := worlds.RunKeysFor(ctx, r.world, run.RunID, run.DeploymentID)
		if err != nil {
			return err
		}
		payload, err := worlds.EncodeErrorPayload(cause, worlds.PayloadOptions{Keys: keys})
		if err != nil {
			return err
		}
		starts, _ := stepAttempts(events, stepID)
		data, err := json.Marshal(worlds.DataStepResult{StepName: stepNameIn(events, stepID), Attempt: max(starts, 1), Error: payload})
		if err != nil {
			return err
		}
		_, err = r.world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: input.RunID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeStepFailed, CorrelationID: string(stepID), Data: data}})
		if errors.Is(err, worlds.ErrRunTerminal) {
			return nil
		}
		if err != nil && !errors.Is(err, worlds.ErrEventConflict) && !errors.Is(err, worlds.ErrStepNotStartable) {
			return err
		}
	}
	return r.wake(ctx, run)
}

// runFailedData returns the Data of a run_failed event for err, encrypted
// with the run's keys.
func (r *Runtime) runFailedData(ctx context.Context, runID worlds.RunID, deploymentID string, err error) ([]byte, error) {
	keys, keysErr := worlds.RunKeysFor(ctx, r.world, runID, deploymentID)
	if keysErr != nil {
		return nil, keysErr
	}
	return worlds.EncodeRunFailed(err, worlds.PayloadOptions{Keys: keys})
}
