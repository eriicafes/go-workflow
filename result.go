package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/eriicafes/go-workflow/internal"
	"github.com/eriicafes/go-workflow/workflowops"
)

// Task is a durable workflow or operation handle.
type Task interface {
	Wait(context.Context) error
	Task() (*workflowops.Task, error)
}

// Tasks converts a homogeneous slice into durable task handles.
func Tasks[T Task](values []T) []Task {
	result := make([]Task, len(values))
	for index := range values {
		result[index] = values[index]
	}
	return result
}

// Result is a comparable, pointer-backed handle to a durable operation.
type Result[T any] struct{ state *resultState[T] }

type resultState[T any] struct {
	once  sync.Once
	done  chan struct{}
	value T
	err   error
	world World
	task  workflowops.Task
	await bool
}

func suspendingOperationResult[T any](world World, runID string, sequence int) Result[T] {
	return Result[T]{state: &resultState[T]{
		done:  make(chan struct{}),
		task:  workflowops.Task{RunID: runID, Sequence: sequence},
		world: world,
		await: true,
	}}
}

func waitingOperationResult[T any](world World, runID string, sequence int) Result[T] {
	return Result[T]{state: &resultState[T]{
		done:  make(chan struct{}),
		task:  workflowops.Task{RunID: runID, Sequence: sequence},
		world: world,
	}}
}

func errorResult[T any](err error) Result[T] {
	result := Result[T]{state: &resultState[T]{
		done: make(chan struct{}),
		err:  err,
	}}
	result.state.once.Do(func() { close(result.state.done) })
	return result
}

func (state *resultState[T]) wait(ctx context.Context) (T, error) {
	state.once.Do(func() {
		defer close(state.done)
		if execution, active := workflowExecutionFromContext(ctx); active && state.await {
			if err := awaitSuspendingTask(ctx, execution, &state.task); err != nil {
				state.err = err
				return
			}
		}
		raw, err := state.world.WaitOperation(ctx, state.task)
		if err != nil {
			state.err = err
			return
		}
		if err := json.Unmarshal(raw, &state.value); err != nil {
			state.err = fmt.Errorf("workflow: decode operation output: %w", err)
			return
		}
	})
	select {
	case <-state.done:
		return state.value, state.err
	case <-ctx.Done():
		return state.value, ctx.Err()
	}
}

func (result Result[T]) Wait(ctx context.Context) error {
	_, err := result.Value(ctx)
	return err
}

func (result Result[T]) Value(ctx context.Context) (T, error) {
	return result.state.wait(ctx)
}

func (result Result[T]) Unwrap(ctx context.Context) T {
	value, err := result.Value(ctx)
	if err != nil {
		panic(err)
	}
	return value
}

func (result Result[T]) Task() (*workflowops.Task, error) {
	return &result.state.task, nil
}

func awaitSuspendingTask(ctx context.Context, execution *workflowops.Execution, task *workflowops.Task) error {
	if task == nil {
		return fmt.Errorf("workflow: task is not durable")
	}
	sequence := execution.Sequence()
	op, err := execution.World().EnsureWait(ctx, execution.Lease(), workflowops.WaitRequest{
		RunID:        execution.RunID(),
		Sequence:     sequence,
		Kind:         workflowops.KindAwait,
		Fingerprint:  fingerprint(string(workflowops.KindAwait), 2),
		Dependencies: []workflowops.Task{*task},
	})
	if err != nil {
		return err
	}
	if op.Status == workflowops.StatusCompleted || op.Status == workflowops.StatusFailed {
		return nil
	}
	if err := execution.World().SuspendRun(ctx, execution.Lease()); err != nil {
		return err
	}
	panic(internal.ErrReplaySuspended)
}
