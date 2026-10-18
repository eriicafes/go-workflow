package worlds

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
// visible as Advance walks the event log.
type Execution struct {
	world  World
	ctx    context.Context
	run    Run
	events []Event
	// target identifies the step this delivery executes; nil marks a flow delivery.
	target *StepID
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
}

func newExecution(ctx context.Context, world World, run Run, events []Event, target *StepID) *Execution {
	execution := &Execution{
		world:   world,
		ctx:     ctx,
		run:     run,
		events:  events,
		target:  target,
		index:   make(map[int]int),
		settles: make(map[string]int),
		inBand:  run.InBandCount,
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
	e.result = &DataStepResult{Output: bytes.Clone(result.Output), Error: bytes.Clone(result.Error), RetryAfter: retryAt}
}

// GetTargetResult returns the step result recorded by SetTargetResult. It
// reports false when none has been recorded.
func (e *Execution) GetTargetResult() (DataStepResult, bool) {
	if e.result == nil {
		return DataStepResult{}, false
	}
	result := *e.result
	result.Output = bytes.Clone(result.Output)
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

// Advance delivers one recorded event and runs newly ready continuations.
func (e *Execution) Advance() bool {
	if e.horizon == len(e.events) {
		return false
	}
	e.horizon++
	for _, fn := range e.continuations {
		fn()
	}
	return true
}
