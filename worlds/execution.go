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

// Execution is the replay state of one delivery. It holds the run, the events
// loaded for it, and the step the delivery executes, if any. The Durable*
// functions read recorded results from it instead of reading storage for each
// operation the runner replays, and add the events they append to it.
//
// Operations are matched to their declarations by position. Their results are
// delivered in log order: the replay walks the event log, and a result is
// visible only once the walk has passed it. The walk advances only where the
// runner waits, so what the runner can observe between two waits, and which of
// several promises settles first, is the same on every replay.
type Execution struct {
	world     World
	run       Run
	events    []Event
	index     map[int]int
	target    *StepID
	result    *stepResultData
	rollbacks []func(context.Context, *Execution) error
	next      int
	// horizon is the number of events the walk has passed. A result at a
	// smaller index is visible to the runner.
	horizon int
	// settles maps a step, wait, or hook ID to the index of the event that
	// settled it.
	settles map[string]int
	// inBand is the run's InBandCount as of the last write this replay saw,
	// which fences a flow's writes.
	inBand int
}

func newExecution(world World, run Run, events []Event, target *StepID) *Execution {
	execution := &Execution{
		world:   world,
		run:     run,
		events:  events,
		index:   make(map[int]int),
		settles: make(map[string]int),
		target:  target,
		inBand:  run.InBandCount,
	}
	for index, event := range events {
		execution.trackEvent(index, event)
	}
	return execution
}

func (e *Execution) trackEvent(index int, event Event) {
	switch event.EventType {
	case EventTypeStepCompleted, EventTypeStepFailed, EventTypeStepCancelled, EventTypeWaitCompleted, EventTypeHookReceived, EventTypeHookDisposed:
		if _, settled := e.settles[event.CorrelationID]; !settled && event.CorrelationID != "" {
			e.settles[event.CorrelationID] = index
		}
		return
	case EventTypeStepCreated, EventTypeWaitCreated, EventTypeHookCreated, EventTypeHookConflict:
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
func (e *Execution) SetTargetResult(result stepResultData) {
	var retryAt *time.Time
	if result.RetryAfter != nil {
		value := *result.RetryAfter
		retryAt = &value
	}
	e.result = &stepResultData{Output: bytes.Clone(result.Output), Error: bytes.Clone(result.Error), RetryAfter: retryAt}
}

// GetTargetResult returns the step result recorded by SetTargetResult. It
// reports false when none has been recorded.
func (e *Execution) GetTargetResult() (stepResultData, bool) {
	if e.result == nil {
		return stepResultData{}, false
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

// Suspend ends the current replay so the run suspends until a later delivery.
// It panics with a value the Runtime recovers and does not return. It is for
// operations that await a promise that is not ready, not for runner code.
func (e *Execution) Suspend() { panic(errReplaySuspended) }

// AddRollback registers fn to run when this delivery fails the run, or fails
// the step it executes. Registered functions run in reverse order. They are
// not recorded, so the runner registers them again on each replay.
func (e *Execution) AddRollback(fn func(context.Context, *Execution) error) {
	e.rollbacks = append(e.rollbacks, fn)
}

// queue queues input for this run, addressed to its deployment. A storage
// error ends the replay so the queue retries the delivery.
func (e *Execution) queue(ctx context.Context, input QueueInput) QueueResult {
	input.RunID = e.run.RunID
	input.DeploymentID = e.run.DeploymentID
	result, err := e.world.Queue(ctx, input)
	if err != nil {
		panic(deliveryError{cause: err})
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
func (e *Execution) CreateEvent(ctx context.Context, event EventsCreateEvent, mapError func(error) error) {
	eventCount := len(e.events)
	result, err := e.world.EventsCreate(ctx, EventsCreateInput{
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
			return
		case errors.Is(err, ErrInBandSuperseded):
			panic(errReplaySuperseded)
		default:
			panic(deliveryError{cause: err})
		}
	}
	e.merge(result, Event{RunID: e.run.RunID, EventID: result.EventID, EventType: event.EventType, CorrelationID: event.CorrelationID, Data: event.Data})
}

// ClaimHook claims the token of the hook event declares and appends event, as
// CreateEvent does. With force it takes the token from another run's active
// hook, and queues a delivery that wakes that run on its own deployment. The
// wake is best-effort, since the run reads the takeover on its next delivery.
// A token another active hook owns without force is ErrHookConflict, which
// mapError sees like any other error.
func (e *Execution) ClaimHook(ctx context.Context, event EventsCreateEvent, force bool, mapError func(error) error) {
	eventCount := len(e.events)
	claim, err := e.world.HooksClaim(ctx, HooksClaimInput{
		RunID:   e.run.RunID,
		Event:   event,
		Options: EventsCreateOptions{EventCount: &eventCount, InBandCount: e.InBandCount()},
		Force:   force,
	})
	if err != nil {
		if mapError != nil {
			err = mapError(err)
		}
		switch {
		case err == nil:
			return
		case errors.Is(err, ErrInBandSuperseded):
			panic(errReplaySuperseded)
		default:
			panic(deliveryError{cause: err})
		}
	}
	if claim.ForcedRunID != "" {
		if deploymentID, err := RunDeploymentID(ctx, e.world, claim.ForcedRunID); err == nil {
			_, _ = e.world.Queue(ctx, QueueInput{RunID: claim.ForcedRunID, IdempotencyKey: "hook-force:" + event.CorrelationID, DeploymentID: deploymentID})
		}
	}
	e.merge(claim.EventResult, Event{RunID: e.run.RunID, EventID: claim.EventID, EventType: event.EventType, CorrelationID: event.CorrelationID, Data: event.Data})
}

// Select walks the event log until one of ids has settled and returns the
// one whose settling event comes first in the log. It reports false when the
// log ends first, and the caller then suspends the run.
func (e *Execution) Select(ids ...string) (string, bool) {
	for {
		winner, winnerIndex := "", -1
		for _, id := range ids {
			if e.Settled(id) && (winnerIndex < 0 || e.settles[id] < winnerIndex) {
				winner, winnerIndex = id, e.settles[id]
			}
		}
		if winnerIndex >= 0 {
			return winner, true
		}
		if e.horizon == len(e.events) {
			return "", false
		}
		e.horizon++
	}
}
