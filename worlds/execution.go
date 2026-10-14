package worlds

import (
	"bytes"
	"context"
	"encoding/json"
	"time"
)

type executionContextKey struct{}
type stepContextKey struct{}

// StepMetadata identifies the durable step currently executing its user
// function. It is absent while the engine is only replaying declarations.
type StepMetadata struct {
	ID        StepID
	Name      string
	StartedAt time.Time
	Attempt   int
}

// Execution is immutable replay context plus an in-memory copy of the event
// prefix used to reconstruct a workflow. Workflow primitives consume this copy;
// they do not perform a storage round trip for each primitive replayed.
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
	if event.EventType != EventTypeRunSpawned && event.EventType != EventTypeStepCreated && event.EventType != EventTypeWaitCreated && event.EventType != EventTypeHookCreated && event.EventType != EventTypeHookConflict {
		return
	}
	var data struct {
		Position *int `json:"position"`
	}
	if json.Unmarshal(event.Data, &data) == nil && data.Position != nil {
		e.index[*data.Position] = index
	}
}

func (e *Execution) World() World {
	return e.world
}

func (e *Execution) Run() Run {
	return e.run
}

// TargetStepID returns the durable step being executed by a targeted queue
// delivery. It returns false during a root replay.
func (e *Execution) TargetStepID() (StepID, bool) {
	if e.target == nil {
		return "", false
	}
	return *e.target, true
}

// SetTargetResult reports a targeted step's result to the engine. It is valid
// only while executing a targeted queue delivery.
func (e *Execution) SetTargetResult(result stepResultData) {
	var retryAt *time.Time
	if result.RetryAfter != nil {
		value := *result.RetryAfter
		retryAt = &value
	}
	e.result = &stepResultData{Output: bytes.Clone(result.Output), Error: bytes.Clone(result.Error), RetryAfter: retryAt}
}

// GetTargetResult returns the terminal or retry result reported by a
// targeted durable-step replay.
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

// NextPosition reserves the next replay-local primitive position. Positions
// are stored in event payloads so a fresh replay resolves the same primitive.
func (e *Execution) NextPosition() int {
	position := e.next
	e.next++
	return position
}

// AddRollback registers durable compensation work for execution if the root
// workflow fails. Registrations are replay-local and run in reverse order.
func (e *Execution) AddRollback(fn func(context.Context, *Execution) error) {
	e.rollbacks = append(e.rollbacks, fn)
}

// EventCount returns the current number of events in the preloaded prefix.
func (e *Execution) EventCount() int { return len(e.events) }

// Merge adds the skipped events and the event just written to the in-memory
// prefix. It intentionally never fetches storage during replay.
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

// Suspend stops the current replay until a durable dependency supplies a
// later queue delivery. It is intended for workflow primitives, not user code.
func (e *Execution) Suspend() { panic(errReplaySuspended) }

// StepMetadataFromContext returns metadata only while a targeted durable-step
// function is executing.
func StepMetadataFromContext(ctx context.Context) (StepMetadata, bool) {
	metadata, ok := ctx.Value(stepContextKey{}).(StepMetadata)
	return metadata, ok
}

// ExecutionFromContext exposes replay state to the workflow runtime. It is not
// normally needed by application code.
func ExecutionFromContext(ctx context.Context) (*Execution, bool) {
	execution, ok := ctx.Value(executionContextKey{}).(*Execution)
	return execution, ok && execution != nil
}
