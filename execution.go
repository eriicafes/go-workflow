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

// StepMetadata describes the current durable step attempt. Its zero value is
// returned outside a step.
type StepMetadata struct {
	// ID is the step's durable idempotency key.
	ID string
	// Name is the stable identity of the executing step function.
	Name string
	// StartedAt is when this attempt began executing.
	StartedAt time.Time
	// Attempt starts at one and increases for every retry.
	Attempt int
}

// GetStepMetadata returns metadata for the currently executing step attempt.
// It returns the zero value outside a step.
func GetStepMetadata(ctx context.Context) StepMetadata {
	value, _ := ctx.Value(stepMetadataContextKey{}).(StepMetadata)
	return value
}

// StepID returns the key assigned to the currently executing durable step. It
// is empty outside a step.
func StepID(ctx context.Context) string {
	return GetStepMetadata(ctx).ID
}

// StepOptions configures one durable step. When omitted, a step durably retries
// an arbitrary error up to three times. When several values are supplied, the
// last value wins.
type StepOptions struct {
	// MaxRetries is the number of retries after the initial attempt.
	MaxRetries int
}

func stepOptions(values []StepOptions) StepOptions {
	if len(values) == 0 {
		return StepOptions{MaxRetries: 3}
	}
	return values[len(values)-1]
}

// Cancel durably cancels unfinished tasks. Cancelling an already cancelled
// task is a successful no-op.
func Cancel(ctx context.Context, tasks ...Task) error {
	dependencies, err := dependenciesFor(tasks)
	if err != nil {
		return err
	}
	if execution, active := executionFromContext(ctx); active {
		return cancel(execution, ctx, dependencies)
	}
	client, _, err := clientFromContext(ctx)
	if err != nil {
		return err
	}
	return client.CancelOperations(ctx, dependencies...)
}

func cancel(
	execution *workflowops.Execution,
	ctx context.Context,
	dependencies []workflowops.Task,
) error {
	sequence := execution.Sequence()
	input, err := json.Marshal(dependencies)
	if err != nil {
		return err
	}
	op, err := execution.World().EnsureOperation(ctx, execution.Lease(), workflowops.OperationRequest{
		RunID:       execution.RunID(),
		Sequence:    sequence,
		Kind:        workflowops.KindCancel,
		Identity:    string(workflowops.KindCancel),
		Fingerprint: fingerprint(string(workflowops.KindCancel), 3),
		Input:       input,
	})
	if err != nil {
		return err
	}
	if op.Status == workflowops.StatusCompleted || op.Status == workflowops.StatusFailed {
		return waitingOperationResult[struct{}](execution.World(), execution.RunID(), sequence).Wait(ctx)
	}
	if sequence != execution.Target() {
		return suspendingOperationResult[struct{}](execution.World(), execution.RunID(), sequence).Wait(ctx)
	}
	if err := execution.World().CancelOperations(ctx, dependencies...); err != nil {
		return err
	}
	for _, dependency := range dependencies {
		if dependency.RunID == execution.RunID() && dependency.Sequence == workflowops.RootSequence {
			panic(internal.ErrReplayComplete)
		}
	}
	output, _ := json.Marshal(struct{}{})
	if err := execution.World().FinishOperation(ctx, op, output, nil); err != nil {
		panic(err)
	}
	panic(internal.ErrReplayComplete)
}

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
	if err := join(ctx, Tasks(results), false, fingerprint(string(workflowops.KindJoin), 2)); err != nil {
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

// Join waits for every supplied durable task. It returns the first terminal
// failure in completion order, without waiting for unfinished tasks.
func Join(ctx context.Context, tasks ...Task) error {
	return join(ctx, tasks, false, fingerprint(string(workflowops.KindJoin), 2))
}

// JoinSettled waits until every supplied durable task has completed. Unlike
// Join, failures in the supplied tasks do not cause the join to fail.
func JoinSettled(ctx context.Context, tasks ...Task) error {
	return join(ctx, tasks, true, fingerprint(string(workflowops.KindJoinSettled), 2))
}

func join(ctx context.Context, tasks []Task, settled bool, fingerprint string) error {
	execution, active := executionFromContext(ctx)
	if !active {
		return ErrNoWorkflow
	}
	dependencies, err := dependenciesFor(tasks)
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
		return waitingOperationResult[struct{}](execution.World(), execution.RunID(), sequence).Wait(ctx)
	}
	if err := execution.World().SuspendRun(ctx, execution.Lease()); err != nil {
		return err
	}
	panic(internal.ErrReplaySuspended)
}

// Select waits for the first durable task to complete and returns that exact
// task with its terminal error.
func Select(ctx context.Context, tasks ...Task) (Task, error) {
	return selectTask(ctx, workflowops.KindSelect, tasks...)
}

// SelectOk waits for the first successfully completed durable task. It ignores
// failures while another task can still succeed, and returns the earliest
// durable failure only when every task has failed.
func SelectOk(ctx context.Context, tasks ...Task) (Task, error) {
	return selectTask(ctx, workflowops.KindSelectOk, tasks...)
}

func selectTask(ctx context.Context, kind workflowops.Kind, tasks ...Task) (Task, error) {
	execution, active := executionFromContext(ctx)
	if !active {
		return nil, ErrNoWorkflow
	}
	if len(tasks) == 0 {
		return nil, fmt.Errorf("workflow: %s requires a task", kind)
	}
	dependencies, err := dependenciesFor(tasks)
	if err != nil {
		return nil, err
	}
	sequence := execution.Sequence()
	op, err := execution.World().EnsureWait(ctx, execution.Lease(), workflowops.WaitRequest{
		RunID:        execution.RunID(),
		Sequence:     sequence,
		Kind:         kind,
		Fingerprint:  fingerprint(string(kind), 3),
		Dependencies: dependencies,
	})
	if err != nil {
		return nil, err
	}
	if op.Status == workflowops.StatusCompleted || op.Status == workflowops.StatusFailed {
		outcome, err := waitingOperationResult[workflowops.SelectOutcome](execution.World(), execution.RunID(), sequence).Value(ctx)
		if err != nil {
			return nil, err
		}
		if outcome.Index < 0 || outcome.Index >= len(dependencies) {
			return nil, fmt.Errorf("workflow: %s recorded invalid winner %d", kind, outcome.Index)
		}
		dependency := dependencies[outcome.Index]
		if dependency.Sequence == workflowops.RootSequence {
			_, err = execution.World().WaitRun(ctx, dependency.RunID)
		} else {
			_, err = execution.World().WaitOperation(ctx, dependency)
		}
		return tasks[outcome.Index], err
	}
	if err := execution.World().SuspendRun(ctx, execution.Lease()); err != nil {
		return nil, err
	}
	panic(internal.ErrReplaySuspended)
}

// SelectWithCancel returns the task resolved by Select and cancels every
// other supplied durable task. It also cancels the losers when the winner
// failed.
func SelectWithCancel(ctx context.Context, tasks ...Task) (Task, error) {
	return selectWithCancel(ctx, workflowops.KindSelect, tasks...)
}

// SelectOkWithCancel returns the task resolved by SelectOk and cancels every
// other supplied durable task.
func SelectOkWithCancel(ctx context.Context, tasks ...Task) (Task, error) {
	return selectWithCancel(ctx, workflowops.KindSelectOk, tasks...)
}

func selectWithCancel(ctx context.Context, kind workflowops.Kind, tasks ...Task) (Task, error) {
	winner, selectErr := selectTask(ctx, kind, tasks...)
	if winner == nil {
		return nil, selectErr
	}
	winnerTask := winner.Task()
	losers := make([]Task, 0, len(tasks)-1)
	for _, task := range tasks {
		value := task.Task()
		if value == nil || (value.RunID == winnerTask.RunID && value.Sequence == winnerTask.Sequence) {
			continue
		}
		losers = append(losers, task)
	}
	if err := Cancel(ctx, losers...); err != nil {
		return winner, err
	}
	return winner, selectErr
}

func timer(
	execution *workflowops.Execution,
	ctx context.Context,
	duration time.Duration,
	kind workflowops.Kind,
	callerSkip int,
) Result[struct{}] {
	sequence := execution.Sequence()
	raw, _ := json.Marshal(duration.String())
	due := time.Now().Add(duration)
	op, err := execution.World().EnsureOperation(ctx, execution.Lease(), workflowops.OperationRequest{
		RunID:       execution.RunID(),
		Sequence:    sequence,
		Kind:        kind,
		Identity:    string(kind),
		Fingerprint: fingerprint(string(kind), callerSkip),
		Input:       raw,
		DueAt:       &due,
	})
	if err != nil {
		return errorResult[struct{}](err)
	}
	if op.Status == workflowops.StatusCompleted {
		return suspendingOperationResult[struct{}](execution.World(), execution.RunID(), sequence)
	}
	if sequence == execution.Target() {
		if remain := time.Until(op.DueAt); remain > 0 {
			timer := time.NewTimer(remain)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return errorResult[struct{}](ctx.Err())
			case <-timer.C:
			}
		}
		if kind == workflowops.KindTimeout {
			task := workflowops.Task{RunID: execution.RunID(), Sequence: workflowops.RootSequence}
			if err := execution.World().CancelOperations(ctx, task); err != nil {
				panic(err)
			}
			panic(internal.ErrReplayComplete)
		}
		output, _ := json.Marshal(struct{}{})
		if err := execution.World().FinishOperation(ctx, op, output, nil); err != nil {
			panic(err)
		}
		panic(internal.ErrReplayComplete)
	}
	return suspendingOperationResult[struct{}](execution.World(), execution.RunID(), sequence)
}

func background[In, Out any](
	execution *workflowops.Execution,
	ctx context.Context,
	identity string,
	fn func(context.Context, In) (Out, error),
	input In,
	options StepOptions,
) Result[Out] {
	sequence := execution.Sequence()
	raw, err := json.Marshal(input)
	if err != nil {
		return errorResult[Out](err)
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
		return errorResult[Out](err)
	}
	if op.Status == workflowops.StatusCompleted {
		return suspendingOperationResult[Out](execution.World(), execution.RunID(), sequence)
	}
	if op.Status == workflowops.StatusFailed {
		return suspendingOperationResult[Out](execution.World(), execution.RunID(), sequence)
	}
	if sequence == execution.Target() {
		stepCtx := context.WithValue(ctx, stepContextKey{}, true)
		attemptCtx := context.WithValue(stepCtx, stepMetadataContextKey{}, StepMetadata{
			ID:        op.StepID,
			Name:      identity,
			StartedAt: time.Now(),
			Attempt:   max(op.Attempt, 1),
		})
		value, callErr := fn(attemptCtx, input)
		if callErr != nil {
			if due, retry := retryAt(callErr, options, op.Attempt); retry {
				if err := execution.World().RetryOperation(ctx, op, callErr, due); err != nil {
					panic(err)
				}
				panic(internal.ErrReplayComplete)
			}
			if err := execution.World().FinishOperation(ctx, op, nil, callErr); err != nil {
				panic(err)
			}
			panic(internal.ErrReplayComplete)
		}
		output, encodeErr := json.Marshal(value)
		if err := execution.World().FinishOperation(ctx, op, output, encodeErr); err != nil {
			panic(err)
		}
		panic(internal.ErrReplayComplete)
	}
	return suspendingOperationResult[Out](execution.World(), execution.RunID(), sequence)
}

func retryAt(cause error, options StepOptions, attempt int) (time.Time, bool) {
	var fatal fatalError
	if errors.As(cause, &fatal) {
		return time.Time{}, false
	}
	attempt = max(attempt, 1)
	if attempt > options.MaxRetries {
		return time.Time{}, false
	}
	var retryable retryableError
	if errors.As(cause, &retryable) {
		return time.Now().Add(max(retryable.after, 0)), true
	}
	return time.Now(), true
}

func now(execution *workflowops.Execution, ctx context.Context) Result[time.Time] {
	sequence := execution.Sequence()
	if execution.Target() >= 0 && sequence > execution.Target() {
		panic(fmt.Errorf("workflow: history mismatch: replay passed command %d", execution.Target()))
	}
	input := json.RawMessage("null")
	op, err := execution.World().EnsureOperation(ctx, execution.Lease(), workflowops.OperationRequest{
		RunID:       execution.RunID(),
		Sequence:    sequence,
		Kind:        workflowops.KindNow,
		Identity:    string(workflowops.KindNow),
		Fingerprint: fingerprint(string(workflowops.KindNow), 3),
		Input:       input,
	})
	if err != nil {
		return errorResult[time.Time](err)
	}
	if op.Status == workflowops.StatusCompleted || op.Status == workflowops.StatusFailed {
		return suspendingOperationResult[time.Time](execution.World(), execution.RunID(), sequence)
	}
	if sequence != execution.Target() {
		return suspendingOperationResult[time.Time](execution.World(), execution.RunID(), sequence)
	}
	output, err := json.Marshal(time.Now())
	if err != nil {
		return errorResult[time.Time](err)
	}
	if err := execution.World().FinishOperation(ctx, op, output, nil); err != nil {
		panic(err)
	}
	panic(internal.ErrReplayComplete)
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
		task := workflowops.Task{RunID: writer.execution.RunID(), Sequence: sequence}
		if err := writer.execution.World().AppendEvent(writer.ctx, task, data); err != nil {
			return 0, err
		}
		if err := writer.execution.World().FinishOperation(writer.ctx, op, raw, nil); err != nil {
			panic(err)
		}
		panic(internal.ErrReplayComplete)
	}
	return len(data), nil
}

func dependenciesFor(tasks []Task) ([]workflowops.Task, error) {
	dependencies := make([]workflowops.Task, len(tasks))
	for index, value := range tasks {
		task := value.Task()
		if task == nil {
			return nil, fmt.Errorf("workflow: task %d is not durable", index)
		}
		dependencies[index] = *task
	}
	return dependencies, nil
}
