package workflowops

import (
	"context"
	"sync"
)

type executionContextKey struct{}

// Execution is replay-local state.
type Execution struct {
	world World
	lease *Lease
	name  string
	mu    sync.Mutex
	next  int
}

// ExecutionFromContext returns the active replay state.
func ExecutionFromContext(ctx context.Context) (*Execution, bool) {
	execution, ok := ctx.Value(executionContextKey{}).(*Execution)
	return execution, ok && execution != nil
}

// Sequence allocates the next deterministic command position.
func (execution *Execution) Sequence() int {
	execution.mu.Lock()
	defer execution.mu.Unlock()
	result := execution.next
	execution.next++
	return result
}

// World returns the execution's storage and dispatch protocol.
func (execution *Execution) World() World { return execution.world }

// RunID returns the root workflow run being replayed.
func (execution *Execution) RunID() string { return execution.lease.ID }

// Name returns the active workflow function's fully qualified name.
func (execution *Execution) Name() string { return execution.name }

// Target returns the targeted task sequence, or a negative value for root replay.
func (execution *Execution) Target() int { return execution.lease.Sequence }

// Lease returns the task lease being executed.
func (execution *Execution) Lease() *Lease { return execution.lease }
