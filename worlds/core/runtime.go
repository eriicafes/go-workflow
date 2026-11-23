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

// Runtime executes deliveries. It replays the runners in its worlds.RunnerSet
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
	runners worlds.RunnerSet
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
func NewRuntime(world worlds.World, runners worlds.RunnerSet, options ...RuntimeOption) *Runtime {
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
	if input.StepID != nil {
		// A step's message is delivered again while the step retries, so
		// past MaxDeliveries its delivery decides from the log.
		return r.executeStep(ctx, input)
	}
	if input.Attempt > MaxDeliveries {
		return r.exceedDeliveries(ctx, input)
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
	budget := newReplayBudget()
	err := r.executeFlow(ctx, input, flowPass{runAhead: true, first: true, budget: budget}, &pass)
	for {
		var stop errRunAheadStop
		if errors.As(err, &stop) {
			if restarts == maxRunAheadRestarts {
				// The next delivery decides from the log.
				log.Printf("workflow: run %s handed off after its run-ahead stopped again after a restart: %s", input.RunID, stop.reason)
				return worlds.Redeliver(0)
			}
			restarts++
			err = r.executeFlow(ctx, input, flowPass{lingered: lingered, budget: budget}, &pass)
			continue
		}
		if errors.As(err, new(errRunAheadRepair)) {
			err = r.executeFlow(ctx, input, flowPass{runAhead: restarts == 0, lingered: lingered, budget: budget}, &pass)
			continue
		}
		if err == nil && pass != nil && pass.parked {
			// Lingering is not replay.
			budget.pause()
			again := r.linger(ctx, pass)
			budget.resume()
			if again {
				lingered = true
				err = r.executeFlow(ctx, input, flowPass{runAhead: restarts == 0, lingered: lingered, budget: budget}, &pass)
				continue
			}
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
// whether the delivery lingered before it, whether it is the delivery's
// first, and the delivery's replay time.
type flowPass struct {
	runAhead, lingered, first bool
	budget                    *replayBudget
}

// TurboFirstDeliveries turns on the turbo path of a run's first delivery: the
// first delivery of its first message replays without reading the log,
// writes run_started without waiting for it, and starts its inline steps
// before their writes commit.
var TurboFirstDeliveries = true

// TurboFirstDeliveryMaxAge is how old a run's first message may be for its
// first delivery to take the turbo path. A platform that runs a delivery
// again after it crashed sends the same request, which counts as a first
// delivery too, while the crashed one may have run the run for minutes. A
// first delivery arrives within seconds of the start, so an older message is
// taken as such a repeat and reads the log.
var TurboFirstDeliveryMaxAge = 15 * time.Second

// isTurbo reports whether the pass takes the turbo path: the first pass of the
// first delivery of a run's first message, which carries the run's creation,
// and not a start hook's candidate, which its run_started may refuse, nor a
// message older than TurboFirstDeliveryMaxAge. A message whose World does not
// report when it was queued is taken as young enough.
func isTurbo(input worlds.QueueInput, pass flowPass) bool {
	return TurboFirstDeliveries && pass.first && input.RunInput != nil && input.RunInput.StartHook == nil &&
		input.DeliveryCount == 1 && input.Attempt <= 1 && input.WaitContinuation == nil && len(input.CompleteWaits) == 0 &&
		(input.CreatedAt.IsZero() || time.Since(input.CreatedAt) <= TurboFirstDeliveryMaxAge)
}

// executeFlow loads the run's log, records run_started on the run's first
// flow, replays the runner, and turns the runner's result into a terminal run
// event when it did not suspend.
func (r *Runtime) executeFlow(ctx context.Context, input worlds.QueueInput, pass flowPass, current **Execution) error {
	*current = nil
	if isTurbo(input, pass) {
		return r.executeTurbo(ctx, input, pass, current)
	}
	loaded, err := r.loadLog(ctx, input.RunID, true)
	if errors.Is(err, worlds.ErrRunNotFound) && input.RunInput != nil {
		// The start's run_created never landed: its first delivery creates the
		// run, or exits when its start hook is refused.
		if err := r.createRunForStart(ctx, input); err != nil {
			return err
		}
		loaded, err = r.loadLog(ctx, input.RunID, true)
	}
	if err != nil {
		return err
	}
	run, events := loaded.run, loaded.events
	if run.Status.Terminal() {
		return nil
	}
	if r.misrouted(run) {
		return r.reroute(ctx, run, input)
	}
	execution, err := newExecution(ctx, r.world, run, events, nil)
	if err != nil {
		// A run the runtime cannot replay at all fails at once: every
		// delivery would stop here.
		return r.failSetup(ctx, run, err)
	}
	execution.delivery = input
	execution.lingered = pass.lingered
	execution.budget, execution.maxEvents = pass.budget, loaded.maxEvents
	*current = execution
	if pass.runAhead {
		execution.aheadDepth = r.runAheadDepth
	}
	if execution.keys, err = worlds.RunKeysFor(ctx, r.world, run.RunID, run.DeploymentID); err != nil {
		return err
	}
	if loaded.truncated {
		// The run is past the most events it may hold, and fails on the
		// part of its log read so far.
		failure := maxEventsExceededError{count: countEvents(events), limit: loaded.maxEvents, atLeast: true}
		return r.recordRunEnd(execution, worlds.EventTypeRunFailed, execution.runFailedData(failure.recorded()))
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
		return r.recordRunEnd(execution, worlds.EventTypeRunFailed, execution.runFailedData(fmt.Errorf("%w: %s", worlds.ErrWorkflowNotRegistered, run.WorkflowName)))
	}
	return r.replayFlow(input, runner, run, execution)
}

// startRun records run_started as an in-band write of the run's first delivery,
// carrying the run's creation when its message does.
func (r *Runtime) startRun(execution *Execution) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = replayExit(recovered)
		}
	}()
	result := execution.CreateEvent(worlds.EventsCreateEvent{EventType: worlds.EventTypeRunStarted, Data: runStartedData(execution.delivery)}, nil)
	if result.EventID != "" {
		execution.runStartedAt = time.Now()
	}
	return nil
}

// failSetup fails a run whose replay could not be set up, for cause, with
// the error code RUNTIME_ERROR, as an out-of-band write.
func (r *Runtime) failSetup(ctx context.Context, run worlds.Run, cause error) error {
	keys, err := worlds.RunKeysFor(ctx, r.world, run.RunID, run.DeploymentID)
	if err != nil {
		return err
	}
	payload, err := worlds.EncodeErrorPayload(&worlds.Error{Name: "WorkflowRuntimeError", Message: cause.Error()}, worlds.PayloadOptions{Keys: keys})
	if err != nil {
		return err
	}
	data, err := json.Marshal(worlds.DataRunFailed{Error: payload, ErrorCode: "RUNTIME_ERROR"})
	if err != nil {
		return err
	}
	log.Printf("workflow: run %s cannot be replayed: %v", run.RunID, cause)
	_, err = r.world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: run.RunID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeRunFailed, Data: data}})
	if is[*worlds.RunExpiredError](err) || errors.Is(err, worlds.ErrEntityConflict) {
		return nil
	}
	return err
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

// createRunForStart creates the run of a first delivery whose start's run_created
// never landed. A start hook the World refuses ends the delivery.
func (r *Runtime) createRunForStart(ctx context.Context, input worlds.QueueInput) error {
	err := worlds.CreateRunForStart(ctx, r.world, input.RunID, runStartedData(input))
	var conflict *worlds.HookConflictError
	// A candidate the World refused, for a run that holds its token or for
	// good, never becomes a run. One answered while admission is switched
	// off is not decided, and its message is delivered again.
	if errors.As(err, &conflict) || errors.Is(err, worlds.ErrStartHookRejected) {
		return errStartRefused
	}
	return err
}

// errStartRefused ends a first delivery whose start hook the World refused: it
// acknowledges its message without creating a run.
var errStartRefused = errors.New("workflow: the run's start was refused")

// executeTurbo runs the turbo pass of a run's first delivery. The log is taken to
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
	execution.budget, execution.maxEvents = pass.budget, maxEventsFor(0)
	execution.turbo, execution.runStartedAt = true, now
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
		return r.recordRunEnd(execution, worlds.EventTypeRunFailed, execution.runFailedData(fmt.Errorf("%w: %s", worlds.ErrWorkflowNotRegistered, run.WorkflowName)))
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
		if _, ok := errors.AsType[*worlds.RunExpiredError](delivery.cause); ok {
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
func (r *Runtime) replayFlow(input worlds.QueueInput, runner worlds.Runner, run worlds.Run, execution *Execution) (err error) {
	ctx := execution.Context()
	var output []byte
	if err = r.checkLimits(execution); err == nil {
		if resendErr := r.resend(execution); resendErr != nil {
			return resendErr
		}
		output, err = r.run(runner, ctx, run.Input)
	}
	// Steps and timers the runner declared and no longer waits for are not
	// queued or armed. A run that ends records them as created.
	execution.unawaited, execution.unawaitedWaits = execution.pending, execution.pendingWaits
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
	var eventsErr maxEventsExceededError
	switch {
	case context.Cause(ctx) != nil:
		return context.Cause(ctx)
	case errors.Is(err, errReplayComplete):
		return nil
	case errors.Is(err, errReplaySuspended):
		// Every recorded event is delivered and the runner waits, so an
		// operation the log records and the runner never declared is one
		// this replay will not reach.
		if id, ok := execution.orphaned(); ok {
			return r.recoverDivergence(ctx, input, replayDivergenceError{CorrelationID: id, Reason: "replay suspended without reaching the recorded operation"})
		}
		execution.parked = true
		return nil
	case errors.Is(err, errReplayBudgetExhausted):
		return r.exhaustReplayBudget(input, execution)
	case errors.As(err, &eventsErr):
		// The run fails without running its compensations, which would
		// add to a log that is full.
		return r.recordRunEnd(execution, worlds.EventTypeRunFailed, execution.runFailedData(eventsErr.recorded()))
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
		if _, ok := errors.AsType[*worlds.RunExpiredError](deliveryErr.cause); ok {
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

// checkLimits returns the error that ends a pass before it replays: the
// delivery has replayed for its limit, or the run's log is full.
func (r *Runtime) checkLimits(execution *Execution) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			var ok bool
			if err, ok = recovered.(error); !ok {
				err = fmt.Errorf("workflow: panic: %v", recovered)
			}
		}
	}()
	execution.checkLimits()
	return nil
}

// exhaustReplayBudget ends a flow delivery that has replayed for its limit.
// While the message has been delivered no more than the retries allowed, it
// returns an error, so the queue delivers the message again. After that it
// fails the run with the error code REPLAY_TIMEOUT, as an in-band write.
func (r *Runtime) exhaustReplayBudget(input worlds.QueueInput, execution *Execution) error {
	limit, retries := execution.budget.seconds(), replayTimeoutRetries()
	if input.Attempt <= retries {
		log.Printf("workflow: warning: replay of run %s exceeded its maximum duration (%ss); retrying the delivery (attempt %d of %d)", input.RunID, limit, input.Attempt, retries+1)
		return fmt.Errorf("workflow: replay exceeded maximum duration (%ss)", limit)
	}
	log.Printf("workflow: replay of run %s exceeded its maximum duration (%ss) on %d attempts; failing the run", input.RunID, limit, input.Attempt)
	failure := fatalError(fmt.Sprintf("Workflow replay exceeded maximum duration (%ss) after %d attempts", limit, input.Attempt), worlds.ErrReplayTimeout)
	return r.recordRunEnd(execution, worlds.EventTypeRunFailed, execution.runFailedData(failure))
}

// recordUnawaited records the steps and timers the runner never waited for
// as the run ends. It returns an error only when the flow was superseded.
func (r *Runtime) recordUnawaited(execution *Execution) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = replayExit(recovered)
		}
	}()
	execution.recordUnawaited()
	return nil
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

// loadedRun is a run and its event log as a delivery read them.
type loadedRun struct {
	run    worlds.Run
	events []worlds.Event
	// maxEvents is the most events the run may hold, and truncated reports
	// that the read stopped there with more of the log left.
	maxEvents int
	truncated bool
}

// loadRun reads the run and its complete event log before user code runs,
// and takes the in-band count that fences the flow's writes from the
// snapshot the log was listed under. Replaying primitives then use only this
// in-memory log.
func (r *Runtime) loadRun(ctx context.Context, runID worlds.RunID) (worlds.Run, []worlds.Event, error) {
	loaded, err := r.loadLog(ctx, runID, false)
	return loaded.run, loaded.events, err
}

// loadLog reads the run and its event log as loadRun does, taking the run's
// input from the log. With stopAtLimit, a flow's read, it stops once it holds
// the most events the run may, when more follow: the run fails on what was
// read, so the rest is not.
func (r *Runtime) loadLog(ctx context.Context, runID worlds.RunID, stopAtLimit bool) (loadedRun, error) {
	// The run's input is read from its run_created, the log's first event,
	// so the run itself is read without payloads.
	run, err := r.world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID, ResolveData: worlds.ResolveDataNone})
	if err != nil {
		return loadedRun{}, err
	}
	page, err := r.world.EventsList(ctx, worlds.EventsListInput{RunID: runID})
	if err != nil {
		return loadedRun{}, err
	}
	// A finished run is not replayed, and its log may have been deleted.
	if !run.Status.Terminal() {
		if len(page.Data) == 0 || page.Data[0].EventType != worlds.EventTypeRunCreated {
			return loadedRun{}, fmt.Errorf("workflow: event log of run %s does not start with run_created", runID)
		}
		run.Input = worlds.EventPayload(page.Data[0])
	}
	// The flow fences its writes with the in-band count of the snapshot the
	// listing was read under, which covers every event it returns.
	run.InBandCount = page.Snapshot.InBandCount
	loaded := loadedRun{run: run, events: page.Data, maxEvents: maxEventsFor(page.MaxEvents)}
	for page.HasMore {
		if stopAtLimit && countEvents(loaded.events) >= loaded.maxEvents {
			loaded.truncated = true
			break
		}
		page, err = r.world.EventsList(ctx, worlds.EventsListInput{RunID: runID, Pagination: worlds.Pagination{Cursor: page.Cursor}})
		if err != nil {
			return loadedRun{}, err
		}
		loaded.events = append(loaded.events, page.Data...)
		if page.MaxEvents > 0 {
			loaded.maxEvents = maxEventsFor(page.MaxEvents)
		}
	}
	return loaded, nil
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
	if err := r.recordUnawaited(execution); err != nil {
		return err
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
				if _, ok := errors.AsType[*worlds.RunExpiredError](err); ok {
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
	if err := r.recordUnawaited(execution); err != nil {
		return err
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
	pastCap := input.Attempt > MaxDeliveries
	if past := input.Attempt - MaxDeliveries; pastCap && past&(past-1) == 0 {
		// At the first delivery past the cap, and then at every power of
		// two past it.
		log.Printf("workflow: warning: step delivery %s for run %s was delivered %d times, more than %d", input.MessageID, input.RunID, input.Attempt, MaxDeliveries)
	}
	run, events, err := r.loadRun(ctx, input.RunID)
	if pastCap && errors.Is(err, worlds.ErrRunNotFound) {
		return nil
	}
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
	// Past MaxDeliveries, a step no delivery got as far as starting for a
	// long while fails instead of starting.
	execution.stalled = pastCap && stepStalled(events, stepID, time.Now())
	if execution.keys, err = worlds.RunKeysFor(ctx, r.world, run.RunID, run.DeploymentID); err != nil {
		return err
	}
	runner, ok := r.runners[run.WorkflowName]
	if !ok && execution.stalled {
		return r.failStalledStep(ctx, execution, stepID)
	}
	if !ok {
		execution.targetAttempt = 1
		return r.completeStep(ctx, execution, stepID, worlds.DataStepResult{Error: execution.encodeError(fmt.Errorf("%w: %s", worlds.ErrWorkflowNotRegistered, run.WorkflowName))})
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
func (r *Runtime) replayStep(input worlds.QueueInput, runner worlds.Runner, run worlds.Run, execution *Execution, stepID worlds.StepID) error {
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
	case execution.stalled:
		// The replay that reached the step chose its failure. One that did
		// not reach it is why no delivery started the step.
		if result, ok := execution.GetTargetResult(); ok && errors.Is(err, errReplayComplete) {
			return r.completeStep(ctx, execution, stepID, result)
		}
		return r.failStalledStep(ctx, execution, stepID)
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
		if _, ok := errors.AsType[*worlds.RunExpiredError](deliveryErr.cause); ok {
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

// failStalledStep fails the step of a delivery made past MaxDeliveries that
// no delivery got as far as starting, as the attempt that would have run.
func (r *Runtime) failStalledStep(ctx context.Context, execution *Execution, stepID worlds.StepID) error {
	starts, _ := stepAttempts(execution.events, stepID)
	execution.targetAttempt = starts + 1
	failure := deliveriesExceededStepError(stepNameIn(execution.events, stepID), execution.delivery.Attempt)
	return r.completeStep(ctx, execution, stepID, worlds.DataStepResult{Error: execution.encodeError(failure)})
}

// completeStep records a targeted step's result out-of-band, and queues a
// flow, which consumes a final result. A retry also asks for the step's
// message again once it is due.
func (r *Runtime) completeStep(ctx context.Context, execution *Execution, stepID worlds.StepID, result worlds.DataStepResult) error {
	event, err := execution.recordStepResult(stepID, max(execution.targetAttempt, 1), result, false, execution.targetRan)
	if err != nil || event.EventID == "" {
		return err
	}
	if event.EventType == worlds.EventTypeStepRetrying && result.RetryAfter != nil {
		if err := r.wake(ctx, execution.run); err != nil {
			return err
		}
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
				if _, ok := errors.AsType[*worlds.RunExpiredError](err); ok {
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
func (r *Runtime) run(runner worlds.Runner, ctx context.Context, input []byte) (output []byte, err error) {
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
		if _, ok := errors.AsType[*worlds.RunExpiredError](err); ok {
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
	if _, ok := errors.AsType[*worlds.RunExpiredError](err); ok {
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

// stepStalled reports whether the step stepID, whose message is delivered
// past MaxDeliveries, has shown no activity among events for stepStallWindow
// before now.
func stepStalled(events []worlds.Event, stepID worlds.StepID, now time.Time) bool {
	since := now.Add(-stepStallWindow)
	for _, event := range events {
		if event.CorrelationID != string(stepID) {
			continue
		}
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

// MaxDeliveries is the most attempts of one flow delivery Runtime.Execute
// makes. With a World's retry backoff it spans hours, short of a queue's
// retention, so a delivery that keeps failing fails its run instead of
// expiring unseen. A step's message is delivered again on purpose while the
// step retries, so past MaxDeliveries its step fails only when no delivery
// got as far as starting it for stepStallWindow.
const MaxDeliveries = 48

// exceedDeliveries fails the run of a flow delivery attempted more than
// MaxDeliveries times, as an in-band write decided from a fresh read of the
// log. It returns nil once the failure is recorded or another delivery has
// finished the run, so the delivery is acknowledged. When the fence refuses
// the write it returns an error, so the queue delivers the message again,
// which tries the write again from a new read.
func (r *Runtime) exceedDeliveries(ctx context.Context, input worlds.QueueInput) error {
	log.Printf("workflow: delivery %s for run %s exceeded max deliveries (%d/%d)", input.MessageID, input.RunID, input.Attempt, MaxDeliveries)
	run, events, err := r.loadRun(ctx, input.RunID)
	if errors.Is(err, worlds.ErrRunNotFound) {
		return nil
	}
	if err != nil || run.Status.Terminal() {
		return err
	}
	execution, err := newExecution(ctx, r.world, run, events, nil)
	if err != nil {
		return r.failSetup(ctx, run, err)
	}
	execution.delivery = input
	if execution.keys, err = worlds.RunKeysFor(ctx, r.world, run.RunID, run.DeploymentID); err != nil {
		return err
	}
	failure := fatalError(fmt.Sprintf("Workflow exceeded maximum queue deliveries (%d/%d)", input.Attempt, MaxDeliveries), worlds.ErrMaxDeliveriesExceeded)
	err = r.recordRunEnd(execution, worlds.EventTypeRunFailed, execution.runFailedData(failure))
	if errors.Is(err, errReplaySuperseded) {
		return fmt.Errorf("workflow: the failure of run %s for exceeding max deliveries was refused by the in-band fence; the delivery is retried", input.RunID)
	}
	return err
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
