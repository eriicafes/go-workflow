package workflow

import (
	"context"
	"fmt"
	"sync"

	"github.com/eriicafes/go-workflow/internal"
	"github.com/eriicafes/go-workflow/workflowops"
)

// Waiter is implemented by both Run and Result, enabling Join and Race.
type Waiter interface {
	Wait(context.Context) error
}

// Waiters converts a homogeneous slice into heterogeneous waiters for Join,
// JoinSettled, or Race.
func Waiters[T Waiter](values []T) []Waiter {
	result := make([]Waiter, len(values))
	for index := range values {
		result[index] = values[index]
	}
	return result
}

type durableWaiter interface {
	Waiter
	task() *workflowops.Task
}

// Result is a comparable, pointer-backed handle to a durable operation.
type Result[T any] struct{ state *resultState[T] }

type resultState[T any] struct {
	once  sync.Once
	done  chan struct{}
	value T
	err   error
	wait  func(context.Context) (T, error)
	task  *workflowops.Task
	await bool
}

func operationResult[T any](
	runID string,
	sequence int,
	wait func(context.Context) (T, error),
) Result[T] {
	return Result[T]{state: &resultState[T]{
		done:  make(chan struct{}),
		wait:  wait,
		task:  &workflowops.Task{RunID: runID, Sequence: sequence},
		await: true,
	}}
}

func resolvedResult[T any](value T, err error) Result[T] {
	return Result[T]{state: &resultState[T]{
		done: make(chan struct{}),
		wait: func(ctx context.Context) (T, error) {
			return value, err
		},
	}}
}

func awaitTask(ctx context.Context, execution *workflowops.Execution, task *workflowops.Task) error {
	if task == nil {
		return fmt.Errorf("workflow: waiter is not durable")
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

func (result Result[T]) resolve(ctx context.Context) (T, error) {
	var zero T
	if result.state == nil {
		return zero, fmt.Errorf("workflow: zero Result")
	}
	result.state.once.Do(func() {
		if execution, active := executionFromContext(ctx); active && result.state.await {
			if err := awaitTask(ctx, execution, result.state.task); err != nil {
				result.state.err = err
				close(result.state.done)
				return
			}
		}
		result.state.value, result.state.err = result.state.wait(ctx)
		close(result.state.done)
	})
	select {
	case <-result.state.done:
		return result.state.value, result.state.err
	case <-ctx.Done():
		return zero, ctx.Err()
	}
}

func (result Result[T]) Wait(ctx context.Context) error {
	_, err := result.resolve(ctx)
	return err
}

func (result Result[T]) Value(ctx context.Context) (T, error) {
	return result.resolve(ctx)
}

func (result Result[T]) Unwrap(ctx context.Context) T {
	value, err := result.resolve(ctx)
	if err != nil {
		panic(err)
	}
	return value
}

func (result Result[T]) task() *workflowops.Task {
	if result.state == nil {
		return nil
	}
	return result.state.task
}
