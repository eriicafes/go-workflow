package worlds

import (
	"bytes"
	"context"
	"encoding/json"
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

// Execution is the replay state of one delivery. It holds the run, the events
// loaded for it, and the step the delivery executes, if any. The Durable*
// functions read recorded results from it instead of reading storage for each
// operation the runner replays, and add the events they append to it.
type Execution struct {
	world     World
	run       Run
	events    []Event
	index     map[int]int
	target    *StepID
	result    *stepResultData
	rollbacks []func(context.Context, *Execution) error
	next      int
}

func newExecution(world World, run Run, events []Event, target *StepID) *Execution {
	execution := &Execution{world: world, run: run, events: events, index: make(map[int]int), target: target}
	for index, event := range events {
		execution.indexEvent(index, event)
	}
	return execution
}

func (e *Execution) indexEvent(index int, event Event) {
	if event.EventType != EventTypeStepCreated && event.EventType != EventTypeWaitCreated && event.EventType != EventTypeHookCreated && event.EventType != EventTypeHookConflict {
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

// AddRollback registers fn to run when this delivery fails the run, or fails
// the step it executes. Registered functions run in reverse order. They are
// not recorded, so the runner registers them again on each replay.
func (e *Execution) AddRollback(fn func(context.Context, *Execution) error) {
	e.rollbacks = append(e.rollbacks, fn)
}

// EventCount returns the number of events the Execution holds.
func (e *Execution) EventCount() int { return len(e.events) }

// Merge adds result.Events and latest, the event just appended, to the events
// the Execution holds, skipping latest when it is already among them. It does
// not read storage.
func (e *Execution) Merge(result EventResult, latest Event) {
	for index, event := range result.Events {
		e.indexEvent(len(e.events)+index, event)
	}
	e.events = append(e.events, result.Events...)
	for _, event := range e.events {
		if event.EventID == latest.EventID {
			return
		}
	}
	e.indexEvent(len(e.events), latest)
	e.events = append(e.events, latest)
}

// Suspend ends the current replay so the run suspends until a later delivery.
// It panics with a value the Runtime recovers and does not return. It is for
// operations that await a promise that is not ready, not for runner code.
func (e *Execution) Suspend() { panic(errReplaySuspended) }

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
