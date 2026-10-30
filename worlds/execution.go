package worlds

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"
)

type executionContextKey struct{}
type stepContextKey struct{}

// StepMetadata describes the step whose function is executing. Attempt
// starts at 1 and StartedAt is the start of the current attempt.
type StepMetadata struct {
	ID        StepID
	Name      string
	StartedAt time.Time
	Attempt   int
}

// StepMetadataFromContext returns the metadata of the step whose function is
// executing. It reports false outside step context.
func StepMetadataFromContext(ctx context.Context) (StepMetadata, bool) {
	metadata, ok := ctx.Value(stepContextKey{}).(StepMetadata)
	return metadata, ok
}

// ExecutionFromContext returns the Execution of the delivery ctx belongs to. It
// reports false outside a flow or step executed by the Runtime. Application
// code does not normally need it.
func ExecutionFromContext(ctx context.Context) (*Execution, bool) {
	execution, ok := ctx.Value(executionContextKey{}).(*Execution)
	return execution, ok && execution != nil
}

// Execution holds a delivery's run, event log, and target step. Durable operations
// match declarations by position and read recorded outcomes. Results become
// visible as Wait walks the event log.
type Execution struct {
	world  World
	ctx    context.Context
	run    Run
	events []Event
	// target identifies the step this delivery executes; nil marks a flow delivery.
	target *StepID
	// delivery is the queue message being executed.
	delivery QueueInput
	// result holds the target step's outcome for the runtime to persist.
	result *DataStepResult
	// index maps declaration positions to indexes in events.
	index map[int]int
	// rollbacks holds compensations in registration order.
	rollbacks []func(context.Context, *Execution) error
	// next is the next declaration position to reserve.
	next int
	// horizon is the number of events the walk has passed. A result at a
	// smaller index is visible to the runner.
	horizon int
	// settles maps operation IDs to their settlement event's index, and exact
	// event IDs to their own index for ordering and replay visibility.
	settles map[string]int
	// inBand is the run's InBandCount as of the last write this replay saw,
	// which fences a flow's writes.
	inBand int
	// continuations holds callbacks that react as replay delivers events.
	continuations []func()
	// suspended marks a runtime exit before the workflow body finishes.
	suspended bool
	// pending holds the steps a flow declared or must attempt again, in
	// declaration order, which advance starts once every recorded event is
	// delivered.
	pending []pendingStep
	// outcomes receives the result of each step the flow runs inline.
	outcomes chan inlineOutcome
	// inline counts the steps the flow runs inline that have not recorded
	// their results, and promoted the new steps among them.
	inline, promoted int
	// running holds the steps the Runtime's process runs inline. It is nil
	// outside a flow.
	running *sync.Map
	// startedAt is when the delivery started. A flow stops running new steps
	// inline after each step's duration.
	startedAt time.Time
	// inlineUntil is halfway from startedAt to the deadline of the
	// delivery's ctx, after which a flow stops running new steps inline, so
	// they and the steps already running can finish before it. It is zero
	// when ctx has no deadline.
	inlineUntil time.Time
}

func newExecution(ctx context.Context, world World, run Run, events []Event, target *StepID) *Execution {
	execution := &Execution{
		world:     world,
		ctx:       ctx,
		run:       run,
		events:    events,
		target:    target,
		index:     make(map[int]int),
		settles:   make(map[string]int),
		inBand:    run.InBandCount,
		startedAt: time.Now(),
	}
	if deadline, ok := ctx.Deadline(); ok {
		execution.inlineUntil = execution.startedAt.Add(deadline.Sub(execution.startedAt) / 2)
	}
	for index, event := range events {
		execution.trackEvent(index, event)
	}
	return execution
}

func (e *Execution) trackEvent(index int, event Event) {
	// Indexing an event ID does not settle its correlated operation. The
	// cases below index operation IDs only when their outcome is recorded.
	e.settles[string(event.EventID)] = index
	switch event.EventType {
	case EventTypeStepCompleted, EventTypeStepFailed, EventTypeWaitCompleted, EventTypeHookReceived, EventTypeHookDisposed:
		if _, settled := e.settles[event.CorrelationID]; !settled && event.CorrelationID != "" {
			e.settles[event.CorrelationID] = index
		}
		return
	case EventTypeHookConflict:
		e.settles[event.CorrelationID] = index
		// A conflict also records the declaration, so index its position below.
	case EventTypeStepCreated, EventTypeWaitCreated, EventTypeHookCreated:
	default:
		return
	}
	var data struct {
		Position *int `json:"position"`
	}
	if json.Unmarshal(event.Data, &data) == nil && data.Position != nil {
		e.index[*data.Position] = index
	}
}

// World returns the World the run is recorded in.
func (e *Execution) World() World {
	return e.world
}

// Context returns the delivery context with execution metadata, without a
// workflow cancellation context. Internal control steps use it to record outcomes.
func (e *Execution) Context() context.Context {
	return context.WithValue(e.ctx, executionContextKey{}, e)
}

// Run returns the run as loaded at the start of the delivery.
func (e *Execution) Run() Run {
	return e.run
}

// TargetStepID returns the step a step delivery executes. It reports false
// for a flow, which replays the runner without executing a step.
func (e *Execution) TargetStepID() (StepID, bool) {
	if e.target == nil {
		return "", false
	}
	return *e.target, true
}

// SetTargetResult records the result of the step a step delivery executes,
// for the Runtime to append once the replay ends. It is meaningful only during
// a step delivery.
func (e *Execution) SetTargetResult(result DataStepResult) {
	var retryAt *time.Time
	if result.RetryAfter != nil {
		value := *result.RetryAfter
		retryAt = &value
	}
	e.result = &DataStepResult{Result: bytes.Clone(result.Result), Error: bytes.Clone(result.Error), RetryAfter: retryAt}
}

// GetTargetResult returns the step result recorded by SetTargetResult. It
// reports false when none has been recorded.
func (e *Execution) GetTargetResult() (DataStepResult, bool) {
	if e.result == nil {
		return DataStepResult{}, false
	}
	result := *e.result
	result.Result = bytes.Clone(result.Result)
	result.Error = bytes.Clone(result.Error)
	if result.RetryAfter != nil {
		value := *result.RetryAfter
		result.RetryAfter = &value
	}
	return result, true
}

// NextPosition returns the position of the next operation the runner declares
// and advances the counter. Positions are recorded in event data so each replay
// matches an operation to its recorded events. This is why a runner must
// declare its operations in the same order on every replay.
func (e *Execution) NextPosition() int {
	position := e.next
	e.next++
	return position
}

// unreachedPosition reports the lowest position of a recorded operation that the
// replay has not reached. A runner that returns with such an operation left
// over has diverged from its event log.
func (e *Execution) unreachedPosition() (int, bool) {
	lowest, found := 0, false
	for position := range e.index {
		if position >= e.next && (!found || position < lowest) {
			lowest, found = position, true
		}
	}
	return lowest, found
}

// InBandCount returns the count that fences a write made by this replay, for
// EventsCreateOptions.InBandCount. It is nil in a step delivery, whose writes
// are out-of-band, so only a flow, the run's orchestrator, fences its writes.
func (e *Execution) InBandCount() *int {
	if e.target != nil {
		return nil
	}
	count := e.inBand
	return &count
}

// Settled reports whether the step, timer, or hook with id has settled as far
// as the replay has walked the event log. It never advances the walk, so the
// answer is the same at the same point of every replay.
func (e *Execution) Settled(id string) bool {
	index, ok := e.settles[id]
	return ok && index < e.horizon
}

// SettledEvent returns the event that settled id, when Settled reports true.
func (e *Execution) SettledEvent(id string) (Event, bool) {
	if !e.Settled(id) {
		return Event{}, false
	}
	return e.events[e.settles[id]], true
}

// DeliveredEventID identifies the last event delivered to workflow code.
func (e *Execution) DeliveredEventID() EventID {
	if e.horizon == 0 {
		return ""
	}
	return e.events[e.horizon-1].EventID
}

// exit marks replay as suspended and returns cause for the caller to panic with.
func (e *Execution) exit(cause error) error {
	e.suspended = true
	return cause
}

// Suspend marks replay as suspended and panics to return control to the runtime.
func (e *Execution) Suspend() {
	panic(e.exit(errReplaySuspended))
}

// IsSuspended reports whether the runtime stopped this replay before
// the workflow body finished. Deferred cleanup should be skipped.
func (e *Execution) IsSuspended() bool { return e.suspended }

// AddRollback registers fn for workflow failure. Callbacks run in registration
// order, and registrations are rebuilt on every replay.
func (e *Execution) AddRollback(fn func(context.Context, *Execution) error) {
	e.rollbacks = append(e.rollbacks, fn)
}

// queue queues input for this run, addressed to its deployment. A storage
// error ends the replay so the queue retries the delivery.
func (e *Execution) queue(input QueueInput) QueueResult {
	input.RunID = e.run.RunID
	input.WorkflowName = e.run.WorkflowName
	input.DeploymentID = e.run.DeploymentID
	result, err := e.world.Queue(e.ctx, input)
	if err != nil {
		panic(e.exit(deliveryError{cause: err}))
	}
	return result
}

// merge adds result.Events and latest, the event just appended, to the events
// the Execution holds, skipping latest when it is already among them. It does
// not read storage.
func (e *Execution) merge(result EventResult, latest Event) {
	e.inBand = result.InBandCount
	for index, event := range result.Events {
		e.trackEvent(len(e.events)+index, event)
	}
	e.events = append(e.events, result.Events...)
	for _, event := range e.events {
		if event.EventID == latest.EventID {
			return
		}
	}
	e.trackEvent(len(e.events), latest)
	e.events = append(e.events, latest)
}

// CreateEvent appends event to the run as a write of this replay and adds it,
// with the events others appended since, to the events the Execution holds. A
// flow's write is fenced by the run's in-band count: when another flow wrote
// first, the World refuses it and CreateEvent ends the replay so the Runtime
// replays the run again. Any other World error ends the replay so the queue
// retries the delivery, instead of reaching runner code.
//
// mapError, when not nil, sees the World's error first. It may act on it and
// return it, return another error, or return nil to accept it, and CreateEvent
// then adds nothing.
func (e *Execution) CreateEvent(event EventsCreateEvent, mapError func(error) error) EventResult {
	eventCount := len(e.events)
	result, err := e.world.EventsCreate(e.ctx, EventsCreateInput{
		RunID:   e.run.RunID,
		Event:   event,
		Options: EventsCreateOptions{EventCount: &eventCount, InBandCount: e.InBandCount()},
	})
	if err != nil {
		if mapError != nil {
			err = mapError(err)
		}
		switch {
		case err == nil:
			return EventResult{}
		case errors.Is(err, ErrInBandSuperseded):
			panic(e.exit(errReplaySuperseded))
		default:
			panic(e.exit(deliveryError{cause: err}))
		}
	}
	data := event.Data
	if result.Data != nil {
		data = result.Data
	}
	e.merge(result, Event{RunID: e.run.RunID, EventID: result.EventID, EventType: result.EventType, CorrelationID: event.CorrelationID, Data: data})
	return result
}

// OnAdvance registers workflow work that becomes runnable as results arrive.
func (e *Execution) OnAdvance(fn func()) {
	e.continuations = append(e.continuations, fn)
	fn()
}

// advance delivers one recorded event and runs newly ready continuations.
// Once every recorded event is delivered, a flow starts the steps the runner
// declared, and waits for a step it runs inline to record its result, which
// it then delivers, so the runner continues without replaying. advance
// reports false when no event can arrive in this delivery.
func (e *Execution) advance() bool {
	if e.horizon == len(e.events) {
		e.startSteps()
	}
	for e.horizon == len(e.events) {
		if e.inline == 0 {
			return false
		}
		if err := e.recordNext(); err != nil {
			panic(e.exit(deliveryError{cause: err}))
		}
	}
	e.horizon++
	for _, fn := range e.continuations {
		fn()
	}
	return true
}

// Wait advances the walk by one event, waiting for a step the flow runs
// inline when every recorded event is delivered, and suspends the replay when
// no event can arrive in this delivery.
func (e *Execution) Wait() {
	if !e.advance() {
		e.Suspend()
	}
}

// hold adds step to the steps advance starts once every recorded event is
// delivered.
func (e *Execution) hold(step pendingStep) {
	e.pending = append(e.pending, step)
}

// startSteps starts the steps the runner declared, in declaration order.
// Inline steps, steps this flow's message must run again, and new steps while
// fewer than their limit run inline, and the rest are queued as step
// deliveries. One write, fenced like every write of a flow so two flows never
// both start a step, records every new step and starts the inline ones with
// this flow's message as their owner. The inline steps then run concurrently,
// each sending its result to e.outcomes.
func (e *Execution) startSteps() {
	if len(e.pending) == 0 {
		return
	}
	pending := e.pending
	e.pending = nil
	started, err := json.Marshal(DataStepStarted{OwnerMessageID: e.delivery.MessageID})
	if err != nil {
		panic(e.exit(deliveryError{cause: err}))
	}
	var events []EventsCreateEvent
	var inline, queued []pendingStep
	var claimed, rechecks []string
	for _, step := range pending {
		runInline := step.explicit || step.recovered
		if step.created != nil && !step.explicit {
			// A new step runs inline only when this flow's message, whose
			// redelivery runs it again, owns it.
			runInline = e.delivery.MessageID != "" && e.promoted < step.limit && time.Since(e.startedAt) < step.duration &&
				(e.inlineUntil.IsZero() || time.Now().Before(e.inlineUntil))
			if runInline {
				e.promoted++
				step.attempt = 1
			}
		}
		if step.created != nil {
			data, err := json.Marshal(step.created)
			if err != nil {
				panic(e.exit(deliveryError{cause: err}))
			}
			events = append(events, EventsCreateEvent{EventType: EventTypeStepCreated, CorrelationID: string(step.stepID), Data: data})
		}
		if runInline && step.created == nil {
			// Another delivery of this flow's message in this process may
			// already run the attempt again, and records its result.
			if _, running := e.running.LoadOrStore(e.stepKey(step.stepID), true); running {
				// A retry's flow can arrive while the delivery that queued
				// it still records the failed attempt, and checks again once
				// it is recorded.
				if key := stepRetryKey(step.stepID, step.attempt-1); e.delivery.IdempotencyKey == key {
					rechecks = append(rechecks, key)
				}
				continue
			}
			claimed = append(claimed, e.stepKey(step.stepID))
		}
		if runInline {
			events = append(events, EventsCreateEvent{EventType: EventTypeStepStarted, CorrelationID: string(step.stepID), Data: started})
			inline = append(inline, step)
		} else {
			queued = append(queued, step)
		}
	}
	// The events are added to the log with the next write's delta.
	result, err := e.world.EventsCreateBatch(e.ctx, EventsCreateBatchInput{RunID: e.run.RunID, InBandCount: e.InBandCount(), Events: events})
	if err != nil {
		// The attempts did not start, so a later delivery may run them.
		for _, key := range claimed {
			e.running.Delete(key)
		}
	}
	switch {
	case errors.Is(err, ErrInBandSuperseded):
		panic(e.exit(errReplaySuperseded))
	case err != nil:
		panic(e.exit(deliveryError{cause: err}))
	}
	e.inBand = result.InBandCount
	for _, step := range queued {
		e.queue(QueueInput{StepID: &step.stepID, IdempotencyKey: string(step.stepID)})
	}
	for _, key := range rechecks {
		notBefore := time.Now().Add(retryRecheckDelay)
		e.queue(QueueInput{NotBefore: &notBefore, IdempotencyKey: key})
	}
	if e.outcomes == nil {
		e.outcomes = make(chan inlineOutcome)
	}
	for _, step := range inline {
		e.inline++
		e.running.Store(e.stepKey(step.stepID), true)
		go func() {
			result, err := step.run(step.attempt)
			e.outcomes <- inlineOutcome{step: step, result: result, err: err}
		}()
	}
}

// recordNext waits for a step running inline to finish and records its
// result.
func (e *Execution) recordNext() error {
	outcome := <-e.outcomes
	e.inline--
	if outcome.step.created != nil && !outcome.step.explicit {
		e.promoted--
	}
	defer e.running.Delete(e.stepKey(outcome.step.stepID))
	if outcome.err != nil {
		return outcome.err
	}
	return e.completeInlineStep(outcome.step, outcome.result)
}

// completeInlineStep records the result of a step that ran inline, as a step
// delivery does. A retry of a new step is queued as a step delivery, and a
// retry of an inline step as a flow that runs it again, queued before the
// retry is recorded.
func (e *Execution) completeInlineStep(step pendingStep, result DataStepResult) error {
	retry := QueueInput{RunID: e.run.RunID, WorkflowName: e.run.WorkflowName, NotBefore: result.RetryAfter, IdempotencyKey: stepRetryKey(step.stepID, step.attempt), DeploymentID: e.run.DeploymentID}
	if result.RetryAfter != nil && step.explicit {
		if _, err := e.world.Queue(e.ctx, retry); err != nil {
			return err
		}
	}
	event, err := e.recordStepResult(step.stepID, result)
	if err != nil {
		return err
	}
	if event.EventType == EventTypeStepRetrying && !step.explicit {
		retry.StepID = &step.stepID
		_, err = e.world.Queue(e.ctx, retry)
	}
	return err
}

// recordStepResult appends the result of the started step stepID out of band,
// and adds it with the events others appended since to the log. The flow keeps
// its in-band count, so its next write is still refused when another flow wrote
// first. A terminal run records a retry as the step's final failure. It returns
// the recorded event, or the zero Event when the step already settled or the
// run ended before it started.
func (e *Execution) recordStepResult(stepID StepID, result DataStepResult) (Event, error) {
	eventType := EventTypeStepCompleted
	if len(result.Error) != 0 {
		eventType = EventTypeStepFailed
		if result.RetryAfter != nil {
			eventType = EventTypeStepRetrying
		}
	}
	payload, err := json.Marshal(result)
	if err != nil {
		return Event{}, err
	}
	eventCount := len(e.events)
	input := EventsCreateInput{
		RunID:   e.run.RunID,
		Event:   EventsCreateEvent{EventType: eventType, CorrelationID: string(stepID), Data: payload},
		Options: EventsCreateOptions{EventCount: &eventCount},
	}
	created, err := e.world.EventsCreate(e.ctx, input)
	if errors.Is(err, ErrRunTerminal) && eventType == EventTypeStepRetrying {
		// A terminal run permits the started step's final failure, but no retry.
		result.RetryAfter = nil
		if input.Event.Data, err = json.Marshal(result); err != nil {
			return Event{}, err
		}
		input.Event.EventType = EventTypeStepFailed
		created, err = e.world.EventsCreate(e.ctx, input)
	}
	if errors.Is(err, ErrRunTerminal) || errors.Is(err, ErrStepNotStartable) {
		return Event{}, nil
	}
	if err != nil {
		return Event{}, err
	}
	event := Event{RunID: e.run.RunID, EventID: created.EventID, EventType: input.Event.EventType, CorrelationID: string(stepID), Data: input.Event.Data}
	inBand := e.inBand
	e.merge(created, event)
	e.inBand = inBand
	return event, nil
}

// finishSteps waits for every step the flow runs inline to finish and records
// their results, once the runner has returned or stopped.
func (e *Execution) finishSteps() error {
	var errs []error
	for e.inline > 0 {
		errs = append(errs, e.recordNext())
	}
	return errors.Join(errs...)
}

// stepKey identifies stepID of the run in Runtime.running.
func (e *Execution) stepKey(stepID StepID) string {
	return string(e.run.RunID) + "/" + string(stepID)
}
