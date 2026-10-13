package worlds

import (
	"context"
	"sync"
)

type executionContextKey struct{}

// Execution is immutable replay context plus an in-memory copy of the event
// prefix used to reconstruct a workflow. Workflow primitives consume this copy;
// they do not perform a storage round trip for each primitive replayed.
type Execution struct {
	world  World
	run    Run
	events []Event
	mu     sync.RWMutex
}

func executionWithEvents(world World, run Run, events []Event) *Execution {
	copyEvents := make([]Event, len(events))
	copy(copyEvents, events)
	return &Execution{world: world, run: run, events: copyEvents}
}

// ExecutionFromContext exposes replay state to the workflow runtime. It is not
// normally needed by application code.
func ExecutionFromContext(ctx context.Context) (*Execution, bool) {
	execution, ok := ctx.Value(executionContextKey{}).(*Execution)
	return execution, ok && execution != nil
}

func (execution *Execution) World() World {
	return execution.world
}

func (execution *Execution) Run() Run {
	return execution.run
}

// Events returns the current preloaded prefix. The returned slice is safe for
// callers to inspect but cannot mutate replay state.
func (execution *Execution) Events() []Event {
	execution.mu.RLock()
	defer execution.mu.RUnlock()
	result := make([]Event, len(execution.events))
	copy(result, execution.events)
	return result
}

// Merge adds the stale-writer delta returned by EventResult to the in-memory
// prefix. It intentionally never fetches storage during replay.
func (execution *Execution) Merge(result EventResult) {
	execution.mu.Lock()
	defer execution.mu.Unlock()
	execution.events = append(execution.events, result.Events...)
}
