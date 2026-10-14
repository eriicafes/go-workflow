// Package workflow makes ordinary named Go functions durable workflows.
package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"runtime"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

type (
	// World is the worker storage, queue, and stream protocol.
	World = worlds.World
	// Status identifies a materialized run or step state.
	Status = worlds.Status
)

const (
	StatusPending   = worlds.StatusPending
	StatusRunning   = worlds.StatusRunning
	StatusCompleted = worlds.StatusCompleted
	StatusFailed    = worlds.StatusFailed
	StatusCancelled = worlds.StatusCancelled
)

// workflow is a marker for a Go type that can start a workflow.
type workflow[Out any] interface{ Workflow(Out) }

// Run names a root workflow execution.
type Run[Out any] struct {
	id     worlds.RunID
	client Client
}

func (r *Run[Out]) ID() string { return string(r.id) }

func (r *Run[Out]) Status(ctx context.Context) (Status, error) {
	run, err := r.client.RunsGet(ctx, worlds.RunsGetInput{RunID: r.id})
	return run.Status, err
}

// Exists reports whether this run is present in its World.
func (r *Run[Out]) Exists(ctx context.Context) (bool, error) {
	_, err := r.client.RunsGet(ctx, worlds.RunsGetInput{RunID: r.id})
	if errors.Is(err, worlds.ErrRunNotFound) {
		return false, nil
	}
	return err == nil, err
}

// WorkflowName returns the workflow identity recorded when this run started.
func (r *Run[Out]) WorkflowName(ctx context.Context) (string, error) {
	run, err := r.client.RunsGet(ctx, worlds.RunsGetInput{RunID: r.id})
	return run.WorkflowName, err
}

// WakeOptions narrows Wake to the durable waits identified by correlation ID.
// With no IDs, Wake completes every pending wait in the run.
type WakeOptions struct {
	CorrelationIDs []string
}

// Wake completes pending durable waits in this run. When CorrelationIDs are
// supplied, it completes only those wait IDs.
func (r *Run[Out]) WakeUp(ctx context.Context, options ...WakeOptions) (int, error) {
	option := WakeOptions{}
	if len(options) != 0 {
		option = options[len(options)-1]
	}
	return r.client.RunsWake(ctx, worlds.ClientRunsWakeInput{RunID: r.id, CorrelationIDs: option.CorrelationIDs})
}

func (r *Run[Out]) Done(ctx context.Context) (bool, error) {
	run, err := r.client.RunsGet(ctx, worlds.RunsGetInput{RunID: r.id})
	if err != nil {
		return false, err
	}
	if !run.Status.Terminal() {
		return false, nil
	}
	if run.Status == worlds.StatusCompleted {
		return true, nil
	}
	if len(run.Error) != 0 {
		return true, fmt.Errorf("workflow: run failed: %s", run.Error)
	}
	return true, ErrRunCancelled
}

func (r *Run[Out]) Await(ctx context.Context) (Out, error) {
	var zero Out
	if execution, ok := workflowExecutionFromContext(ctx); ok {
		wait := worlds.DurableRunWait(ctx, worlds.DurableRunWaitOptions{Execution: execution, Position: execution.NextPosition(), RunID: r.id})
		if wait.Err != nil {
			return zero, wait.Err
		}
		if !wait.Done {
			execution.Suspend()
		}
		run, err := r.client.RunsGet(ctx, worlds.RunsGetInput{RunID: r.id})
		if err != nil {
			return zero, err
		}
		if run.Status == worlds.StatusCompleted {
			if err := json.Unmarshal(run.Output, &zero); err != nil {
				return zero, fmt.Errorf("workflow: decode run output: %w", err)
			}
			return zero, nil
		}
		if len(run.Error) != 0 {
			return zero, fmt.Errorf("workflow: run failed: %s", run.Error)
		}
		return zero, ErrRunCancelled
	}
	raw, err := r.blockingAwait(ctx)
	if err != nil {
		return zero, err
	}
	if err := json.Unmarshal(raw, &zero); err != nil {
		return zero, fmt.Errorf("workflow: decode run output: %w", err)
	}
	return zero, nil
}

func (r *Run[Out]) Unwrap(ctx context.Context) Out {
	value, err := r.Await(ctx)
	if err != nil {
		panic(err)
	}
	return value
}

func (r *Run[Out]) blockingAwait(ctx context.Context) ([]byte, error) {
	var run worlds.Run
	for !run.Status.Terminal() {
		value, err := r.client.RunsWaitForTerminalStatus(ctx, worlds.RunsWaitForTerminalStatusInput{RunID: r.id, Timeout: 100 * time.Millisecond})
		if err != nil {
			return nil, err
		}
		run = value
		if run.Status.Terminal() {
			break
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	if run.Status == worlds.StatusCompleted {
		return run.Output, nil
	}
	if len(run.Error) != 0 {
		return nil, fmt.Errorf("workflow: run failed: %s", run.Error)
	}
	return nil, ErrRunCancelled
}

func (r *Run[Out]) Cancel(ctx context.Context) error {
	err := r.client.RunsCancel(ctx, worlds.ClientRunsCancelInput{RunID: r.id})
	if errors.Is(err, worlds.ErrRunTerminal) {
		return nil
	}
	return err
}

// Reader opens this run's stream from application or durable-step code.
func (r *Run[Out]) Reader(ctx context.Context, options ...ReaderOption) (io.Reader, error) {
	return r.ReaderAt(ctx, 0, options...)
}

// ReaderAt opens this run's stream at a byte offset. Negative offsets are
// relative to the current end of the stream.
func (r *Run[Out]) ReaderAt(ctx context.Context, offset int64, options ...ReaderOption) (io.Reader, error) {
	if _, ok := workflowExecutionFromContext(ctx); ok {
		return nil, ErrNoWorkflow
	}
	opts := readerOption(options)
	return newStreamReader(ctx, r.client, r.id, opts.Namespace, offset), nil
}

// Writer obtains a writer for this run's stream from application code. Close
// closes the stream; the returned release function only releases this writer.
func (r *Run[Out]) Writer(ctx context.Context, options ...WriterOption) (io.WriteCloser, func() error, error) {
	if _, ok := workflowExecutionFromContext(ctx); ok {
		return nil, nil, ErrNoWorkflow
	}
	opts := writerOption(options)
	return newStreamWriter(ctx, r.client, r.id, opts.Namespace, string(worlds.NewStreamID()))
}

// Get returns a handle for an existing workflow. The handle is bound to the
// client in ctx.
func Get[Out any](ctx context.Context, runID string) (*Run[Out], error) {
	client, err := clientFromContext(ctx)
	if err != nil {
		return nil, err
	}
	return &Run[Out]{id: worlds.RunID(runID), client: client}, nil
}

// Start creates and queues a new root workflow.
func Start[In workflow[Out], Out any](ctx context.Context, input In) (*Run[Out], error) {
	name := typeIdentity[In]()
	client, err := clientFromContext(ctx)
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(input)
	if err != nil {
		return nil, fmt.Errorf("workflow: encode %s input: %w", name, err)
	}
	if execution, ok := workflowExecutionFromContext(ctx); ok {
		id, err := worlds.DurableRunStart(ctx, worlds.DurableRunStartOptions{Execution: execution, Position: execution.NextPosition(), WorkflowName: name, Input: raw})
		if err != nil {
			return nil, err
		}
		return &Run[Out]{id: id, client: client}, nil
	}
	id, err := client.RunsCreate(ctx, worlds.ClientRunsCreateInput{WorkflowName: name, Input: raw})
	if err != nil {
		return nil, err
	}
	return &Run[Out]{id: id, client: client}, nil
}

// Use converts a typed workflow function into a runner.
func Use[In workflow[Out], Out any](fn func(context.Context, In) (Out, error)) worlds.Runner {
	name := typeIdentity[In]()
	return worlds.NewRunner(name, func(ctx context.Context, raw json.RawMessage) (json.RawMessage, error) {
		var input In
		if err := json.Unmarshal(raw, &input); err != nil {
			return nil, fmt.Errorf("workflow: decode %s input: %w", name, err)
		}
		output, err := fn(ctx, input)
		if err != nil {
			return nil, err
		}
		encoded, err := json.Marshal(output)
		if err != nil {
			return nil, fmt.Errorf("workflow: encode %s output: %w", name, err)
		}
		return encoded, nil
	})
}

func Step[In, Out any](ctx context.Context, fn func(context.Context, In) (Out, error), input In, options ...StepOptions) (Out, error) {
	return Background(ctx, fn, input, options...).Await(ctx)
}

func Background[In, Out any](ctx context.Context, fn func(context.Context, In) (Out, error), input In, options ...StepOptions) Promise[Out] {
	execution, ok := workflowExecutionFromContext(ctx)
	if !ok {
		return errorPromise[Out](ErrNoWorkflow)
	}
	option := stepOptions(options)
	durable := worlds.DurableStep(ctx, worlds.DurableStepOptions{
		Execution:  execution,
		Position:   execution.NextPosition(),
		Name:       functionIdentity(fn),
		MaxRetries: option.MaxRetries,
		RetryAfter: func(cause error, attempt int) *time.Time {
			return stepRetryAfter(cause, option, attempt)
		},
	}, fn, input)
	return &promise[Out]{
		data:      durable.Data,
		done:      durable.Done,
		err:       durable.Err,
		execution: execution,
		stepID:    durable.StepID,
	}
}

func Sleep(ctx context.Context, duration time.Duration) error {
	_, err := Timer(ctx, duration).Await(ctx)
	return err
}

// Timer returns a durable result that becomes ready after duration.
func Timer(ctx context.Context, duration time.Duration) Promise[struct{}] {
	return timer(ctx, duration, false)
}

// Timeout returns a durable timer that cancels its containing run when it
// becomes ready.
func Timeout(ctx context.Context, duration time.Duration) Promise[struct{}] {
	return timer(ctx, duration, true)
}

func timer(ctx context.Context, duration time.Duration, cancel bool) Promise[struct{}] {
	execution, ok := workflowExecutionFromContext(ctx)
	if !ok {
		return errorPromise[struct{}](ErrNoWorkflow)
	}
	wait := worlds.DurableWait(ctx, worlds.DurableWaitOptions{
		Execution: execution,
		Position:  execution.NextPosition(),
		DueAt:     time.Now().Add(duration),
		Cancel:    cancel,
	})
	return &promise[struct{}]{
		done:      wait.Done,
		err:       wait.Err,
		execution: execution,
		waitID:    wait.WaitID,
		messageID: wait.QueueMessageID,
	}
}

// OnRollback queues fn as a compensation if this workflow later fails. Registered
// compensations run in reverse registration order before the failure is final.
func OnRollback[In any](ctx context.Context, fn func(context.Context, In) error, input In, options ...StepOptions) error {
	execution, ok := workflowExecutionFromContext(ctx)
	if !ok {
		return ErrNoWorkflow
	}
	name := rollbackFunctionIdentity(fn)
	option := stepOptions(options)
	execution.AddRollback(func(ctx context.Context, execution *worlds.Execution) error {
		position := execution.NextPosition()
		result := worlds.DurableStep(ctx, worlds.DurableStepOptions{
			Execution:  execution,
			Position:   position,
			Name:       name,
			MaxRetries: option.MaxRetries,
			RetryAfter: func(cause error, attempt int) *time.Time {
				return stepRetryAfter(cause, option, attempt)
			},
			Rollback: true,
		}, func(ctx context.Context, value In) (struct{}, error) {
			return struct{}{}, fn(ctx, input)
		}, input)
		if result.Done {
			return nil
		}
		if result.Err != nil {
			return result.Err
		}
		execution.Suspend()
		return nil
	})
	return nil
}

// Now returns the time captured by a completed inline durable step named now.
func Now(ctx context.Context) (time.Time, error) {
	execution, ok := workflowExecutionFromContext(ctx)
	if !ok {
		return time.Time{}, ErrNoWorkflow
	}
	result := worlds.DurableInlineStep(ctx, worlds.DurableInlineStepOptions{
		Execution: execution,
		Position:  execution.NextPosition(),
		Name:      "now",
	}, func(context.Context) (time.Time, error) {
		return time.Now().UTC(), nil
	})
	return result.Data, result.Err
}

// Map starts one durable step for each input.
func Map[In, Out any](ctx context.Context, step func(context.Context, In) (Out, error), inputs []In) []Promise[Out] {
	results := make([]Promise[Out], len(inputs))
	for index, input := range inputs {
		results[index] = Background(ctx, step, input)
	}
	return results
}

// All waits for every mapped step and returns their outputs in input order.
func All[In, Out any](ctx context.Context, step func(context.Context, In) (Out, error), inputs []In) ([]Out, error) {
	return Join(ctx, Map(ctx, step, inputs)...)
}

// Join waits for every result in order. A result may be either a durable step
// from this run or a Run handle for another workflow. Pending results suspend
// the current replay; their terminal transitions must arrange a later queue
// delivery for the waiting execution.
func Join[T any](ctx context.Context, results ...Promise[T]) ([]T, error) {
	execution, ok := workflowExecutionFromContext(ctx)
	if !ok {
		return nil, ErrNoWorkflow
	}
	values := make([]T, len(results))
	for index, result := range results {
		done, err := result.Done(ctx)
		if err != nil {
			return nil, err
		}
		if !done {
			execution.Suspend()
		}
		value, err := result.Await(ctx)
		if err != nil {
			return nil, err
		}
		values[index] = value
	}
	return values, nil
}

// JoinSettled waits for every promise. Individual promise failures do not
// cause the join to fail; callers can inspect each promise afterwards.
func JoinSettled[T any](ctx context.Context, results ...Promise[T]) ([]T, error) {
	execution, ok := workflowExecutionFromContext(ctx)
	if !ok {
		return nil, ErrNoWorkflow
	}
	values := make([]T, len(results))
	for index, result := range results {
		done, _ := result.Done(ctx)
		if !done {
			execution.Suspend()
		}
		value, _ := result.Await(ctx)
		values[index] = value
	}
	return values, nil
}

// Cancel durably cancels every promise. Supplying no promises is
// a successful no-op.
func Cancel[T any](ctx context.Context, results ...Promise[T]) error {
	for _, result := range results {
		if err := result.Cancel(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Select waits for the first promise to settle, returning that exact promise.
func Select[T any](ctx context.Context, results ...Promise[T]) (Promise[T], error) {
	return selectPromise(ctx, false, results...)
}

// SelectOk waits for the first successfully completed promise. It ignores
// failed promises while another promise can still succeed.
func SelectOk[T any](ctx context.Context, results ...Promise[T]) (Promise[T], error) {
	return selectPromise(ctx, true, results...)
}

// SelectWithCancel returns the first settled promise and cancels every other
// supplied promise.
func SelectWithCancel[T any](ctx context.Context, results ...Promise[T]) (Promise[T], error) {
	return selectWithCancel(ctx, false, results...)
}

// SelectOkWithCancel returns the first successful promise and cancels every
// other supplied promise.
func SelectOkWithCancel[T any](ctx context.Context, results ...Promise[T]) (Promise[T], error) {
	return selectWithCancel(ctx, true, results...)
}

func selectPromise[T any](ctx context.Context, successful bool, results ...Promise[T]) (Promise[T], error) {
	index, err := selectPromiseIndex(ctx, successful, results...)
	if index < 0 {
		return nil, err
	}
	return results[index], err
}

func selectPromiseIndex[T any](ctx context.Context, successful bool, results ...Promise[T]) (int, error) {
	execution, ok := workflowExecutionFromContext(ctx)
	if !ok {
		return -1, ErrNoWorkflow
	}
	var firstErr error
	pending := false
	for index, result := range results {
		done, err := result.Done(ctx)
		if !done {
			pending = true
			continue
		}
		if !successful || err == nil {
			return index, err
		}
		if firstErr == nil {
			firstErr = err
		}
	}
	if pending {
		execution.Suspend()
	}
	return -1, firstErr
}

func selectWithCancel[T any](ctx context.Context, successful bool, results ...Promise[T]) (Promise[T], error) {
	index, err := selectPromiseIndex(ctx, successful, results...)
	if index < 0 {
		return nil, err
	}
	winner := results[index]
	for resultIndex, result := range results {
		if resultIndex == index {
			continue
		}
		if cancelErr := Cancel(ctx, result); cancelErr != nil {
			return winner, cancelErr
		}
	}
	return winner, err
}

type StepOptions struct{ MaxRetries int }

func stepOptions(values []StepOptions) StepOptions {
	if len(values) == 0 {
		return StepOptions{MaxRetries: 3}
	}
	return values[len(values)-1]
}

func stepRetryAfter(cause error, options StepOptions, attempt int) *time.Time {
	var fatal fatalError
	if errors.As(cause, &fatal) || attempt > options.MaxRetries {
		return nil
	}
	var retryable retryableError
	due := time.Now()
	if errors.As(cause, &retryable) && retryable.after > 0 {
		due = due.Add(retryable.after)
	}
	return &due
}

func typeIdentity[In any]() string {
	typeOf := reflect.TypeFor[In]()
	for typeOf.Kind() == reflect.Pointer {
		typeOf = typeOf.Elem()
	}
	return typeOf.PkgPath() + "." + typeOf.Name()
}

func functionIdentity[In, Out any](fn func(context.Context, In) (Out, error)) string {
	return runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
}

func rollbackFunctionIdentity[In any](fn func(context.Context, In) error) string {
	return runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
}
