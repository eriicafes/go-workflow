package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/eriicafes/go-workflow/internal"
	"github.com/eriicafes/go-workflow/workflowops"
)

// Map starts one durable background step for each input without waiting for
// any result.
func Map[In, Out any](
	ctx context.Context,
	step func(context.Context, In) (Out, error),
	inputs []In,
) []Result[Out] {
	results := make([]Result[Out], len(inputs))
	for index, input := range inputs {
		results[index] = Background(ctx, step, input)
	}
	return results
}

// All maps inputs to durable background steps, then waits for their first
// failure or for every result to complete. On success, it returns every output
// in input order. On any error, it returns a nil output slice. Unfinished
// steps continue running after a fail-fast dependency error.
func All[In, Out any](
	ctx context.Context,
	step func(context.Context, In) (Out, error),
	inputs []In,
) ([]Out, error) {
	results := Map(ctx, step, inputs)
	if err := join(ctx, Waiters(results), false, fingerprint(string(workflowops.KindJoin), 2)); err != nil {
		return nil, err
	}
	values := make([]Out, len(results))
	for index := range results {
		value, err := results[index].Value(ctx)
		if err != nil {
			return nil, err
		}
		values[index] = value
	}
	return values, nil
}

// Join waits for every supplied durable waiter. It returns the first terminal
// failure in completion order, without waiting for unfinished waiters.
func Join(ctx context.Context, waiters ...Waiter) error {
	return join(ctx, waiters, false, fingerprint(string(workflowops.KindJoin), 2))
}

// JoinSettled waits until every supplied durable waiter has completed. Unlike
// Join, failures in the supplied waiters do not cause the join to fail.
func JoinSettled(ctx context.Context, waiters ...Waiter) error {
	return join(ctx, waiters, true, fingerprint(string(workflowops.KindJoinSettled), 2))
}

func join(ctx context.Context, waiters []Waiter, settled bool, fingerprint string) error {
	execution, active := executionFromContext(ctx)
	if !active {
		return ErrNoWorkflow
	}
	dependencies, err := dependenciesFor(waiters)
	if err != nil {
		return err
	}
	kind := workflowops.KindJoin
	if settled {
		kind = workflowops.KindJoinSettled
	}
	sequence := execution.Sequence()
	op, err := execution.World().EnsureWait(ctx, execution.Lease(), workflowops.WaitRequest{
		RunID:        execution.RunID(),
		Sequence:     sequence,
		Kind:         kind,
		Fingerprint:  fingerprint,
		Dependencies: dependencies,
	})
	if err != nil {
		return err
	}
	if op.Status == workflowops.StatusCompleted || op.Status == workflowops.StatusFailed {
		return decodeWaitOperation[struct{}](ctx, execution.World(), execution.RunID(), sequence).Wait(ctx)
	}
	if err := execution.World().SuspendRun(ctx, execution.Lease()); err != nil {
		return err
	}
	panic(internal.ErrReplaySuspended)
}

// Race waits for the first durable waiter to complete and returns that exact
// waiter with its terminal error. It does not synthesize a replacement waiter
// when the winner failed.
func Race(ctx context.Context, waiters ...Waiter) (Waiter, error) {
	execution, active := executionFromContext(ctx)
	if !active {
		return nil, ErrNoWorkflow
	}
	if len(waiters) == 0 {
		return nil, errors.New("workflow: Race requires a waiter")
	}
	dependencies, err := dependenciesFor(waiters)
	if err != nil {
		return nil, err
	}
	sequence := execution.Sequence()
	op, err := execution.World().EnsureWait(ctx, execution.Lease(), workflowops.WaitRequest{
		RunID:        execution.RunID(),
		Sequence:     sequence,
		Kind:         workflowops.KindRace,
		Fingerprint:  fingerprint(string(workflowops.KindRace), 2),
		Dependencies: dependencies,
	})
	if err != nil {
		return nil, err
	}
	if op.Status != workflowops.StatusCompleted && op.Status != workflowops.StatusFailed {
		if err := execution.World().SuspendRun(ctx, execution.Lease()); err != nil {
			return nil, err
		}
		panic(internal.ErrReplaySuspended)
	}
	outcome, err := decodeWaitOperation[workflowops.RaceOutcome](ctx, execution.World(), execution.RunID(), sequence).Value(ctx)
	if err != nil {
		return nil, err
	}
	if outcome.Index < 0 || outcome.Index >= len(waiters) {
		return nil, fmt.Errorf("workflow: Race recorded invalid winner %d", outcome.Index)
	}
	return waiters[outcome.Index], dependencyError(ctx, execution.World(), waiters[outcome.Index])
}

func timeout(execution *workflowops.Execution, ctx context.Context, duration time.Duration, callerSkip int) Result[struct{}] {
	sequence := execution.Sequence()
	raw, _ := json.Marshal(duration.String())
	due := time.Now().Add(duration)
	op, err := execution.World().EnsureOperation(ctx, execution.Lease(), workflowops.OperationRequest{
		RunID:       execution.RunID(),
		Sequence:    sequence,
		Kind:        workflowops.KindSleep,
		Identity:    string(workflowops.KindSleep),
		Fingerprint: fingerprint(string(workflowops.KindSleep), callerSkip),
		Input:       raw,
		DueAt:       &due,
	})
	if err != nil {
		return resolvedResult(struct{}{}, err)
	}
	if op.Status == workflowops.StatusCompleted {
		return decodeAwaitOperation[struct{}](ctx, execution.World(), execution.RunID(), sequence)
	}
	if sequence == execution.Target() {
		if remain := time.Until(op.DueAt); remain > 0 {
			time.Sleep(remain)
		}
		output, _ := json.Marshal(struct{}{})
		if err := execution.World().FinishOperation(context.Background(), op, output, nil); err != nil {
			panic(err)
		}
		panic(internal.ErrReplayComplete)
	}
	return decodeAwaitOperation[struct{}](ctx, execution.World(), execution.RunID(), sequence)
}

func background[In, Out any](
	execution *workflowops.Execution,
	ctx context.Context,
	identity string,
	fn func(context.Context, In) (Out, error),
	input In,
) Result[Out] {
	sequence := execution.Sequence()
	raw, err := json.Marshal(input)
	if err != nil {
		var zero Out
		return resolvedResult(zero, err)
	}
	if execution.Target() >= 0 && sequence > execution.Target() {
		panic(fmt.Errorf("workflow: history mismatch: replay passed command %d", execution.Target()))
	}
	op, err := execution.World().EnsureOperation(ctx, execution.Lease(), workflowops.OperationRequest{
		RunID:       execution.RunID(),
		Sequence:    sequence,
		Kind:        workflowops.KindStep,
		Identity:    identity,
		Fingerprint: fingerprint(identity, 3),
		Input:       raw,
	})
	if err != nil {
		var zero Out
		return resolvedResult(zero, err)
	}
	if op.Status == workflowops.StatusCompleted {
		return decodeAwaitOperation[Out](ctx, execution.World(), execution.RunID(), sequence)
	}
	if op.Status == workflowops.StatusFailed {
		return decodeAwaitOperation[Out](ctx, execution.World(), execution.RunID(), sequence)
	}
	if sequence == execution.Target() {
		stepCtx := context.WithValue(ctx, stepContextKey{}, true)
		stepCtx = context.WithValue(stepCtx, idempotencyContextKey{}, op.IdempotencyKey)
		value, callErr := fn(stepCtx, input)
		if callErr != nil {
			if err := execution.World().FinishOperation(context.Background(), op, nil, callErr); err != nil {
				panic(err)
			}
			panic(internal.ErrReplayComplete)
		}
		output, encodeErr := json.Marshal(value)
		if err := execution.World().FinishOperation(context.Background(), op, output, encodeErr); err != nil {
			panic(err)
		}
		panic(internal.ErrReplayComplete)
	}
	return decodeAwaitOperation[Out](ctx, execution.World(), execution.RunID(), sequence)
}

// Writer returns replay-safe workflow output. It returns a writer that fails
// with ErrNoWorkflow outside a workflow body.
func Writer(ctx context.Context) io.Writer {
	execution, ok := executionFromContext(ctx)
	if !ok {
		return errorWriter{}
	}
	return eventWriter{execution: execution, ctx: ctx}
}

type errorWriter struct{}

func (errorWriter) Write([]byte) (int, error) { return 0, ErrNoWorkflow }

type eventWriter struct {
	execution *workflowops.Execution
	ctx       context.Context
}

func (writer eventWriter) Write(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	sequence := writer.execution.Sequence()
	raw, _ := json.Marshal(data)
	op, err := writer.execution.World().EnsureOperation(writer.ctx, writer.execution.Lease(), workflowops.OperationRequest{
		RunID:       writer.execution.RunID(),
		Sequence:    sequence,
		Kind:        workflowops.KindLog,
		Identity:    string(workflowops.KindLog),
		Fingerprint: fingerprint(string(workflowops.KindLog), 2),
		Input:       raw,
	})
	if err != nil {
		return 0, err
	}
	if sequence == writer.execution.Target() {
		if err := writer.execution.World().AppendEvent(writer.ctx, workflowops.Task{RunID: writer.execution.RunID(), Sequence: sequence}, data); err != nil {
			return 0, err
		}
		if err := writer.execution.World().FinishOperation(writer.ctx, op, raw, nil); err != nil {
			panic(err)
		}
		panic(internal.ErrReplayComplete)
	}
	return len(data), nil
}

func decodeAwaitOperation[Out any](ctx context.Context, world World, runID string, sequence int) Result[Out] {
	return operationResult(runID, sequence, func(ctx context.Context) (Out, error) {
		var zero Out
		raw, err := world.WaitOperation(ctx, workflowops.Task{RunID: runID, Sequence: sequence})
		if err != nil {
			return zero, err
		}
		if err := json.Unmarshal(raw, &zero); err != nil {
			return zero, fmt.Errorf("workflow: decode operation output: %w", err)
		}
		return zero, nil
	})
}

func decodeWaitOperation[Out any](ctx context.Context, world World, runID string, sequence int) Result[Out] {
	result := decodeAwaitOperation[Out](ctx, world, runID, sequence)
	result.state.await = false
	return result
}

func dependenciesFor(waiters []Waiter) ([]workflowops.Task, error) {
	dependencies := make([]workflowops.Task, len(waiters))
	for index, waiter := range waiters {
		durable, ok := waiter.(durableWaiter)
		if !ok {
			return nil, fmt.Errorf("workflow: waiter %d is not durable", index)
		}
		task := durable.task()
		if task == nil {
			return nil, fmt.Errorf("workflow: waiter %d is not durable", index)
		}
		dependencies[index] = *task
	}
	return dependencies, nil
}

func dependencyError(ctx context.Context, world World, waiter Waiter) error {
	durable, ok := waiter.(durableWaiter)
	if !ok {
		return fmt.Errorf("workflow: race winner is not durable")
	}
	task := durable.task()
	if task == nil {
		return fmt.Errorf("workflow: race winner is not durable")
	}
	if task.Sequence == workflowops.RootSequence {
		_, err := world.WaitRun(ctx, task.RunID)
		return err
	}
	_, err := world.WaitOperation(ctx, *task)
	return err
}
