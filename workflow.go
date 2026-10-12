// Package workflow makes ordinary named Go functions durable workflows.
package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"runtime"
	"time"

	"github.com/eriicafes/go-workflow/workflowops"
)

type (
	// World is the worker storage and dispatch protocol.
	World = workflowops.World
	// Runner is the erased, executable form of a workflow function.
	Runner = workflowops.Runner
	// RunnerSet maps workflow identities to their executable runners.
	RunnerSet = workflowops.RunnerSet
	// Status is a durable workflow or operation status.
	Status = workflowops.Status
)

const (
	StatusQueued      = workflowops.StatusQueued
	StatusRunning     = workflowops.StatusRunning
	StatusRetryWait   = workflowops.StatusRetryWait
	StatusWaiting     = workflowops.StatusWaiting
	StatusRollingBack = workflowops.StatusRollingBack
	StatusCompleted   = workflowops.StatusCompleted
	StatusFailed      = workflowops.StatusFailed
	StatusCancelled   = workflowops.StatusCancelled
)

// workflow is a marker for a Go type that can start a workflow.
type workflow[Out any] interface {
	Workflow(Out)
}

// Run names a root workflow execution.
type Run[Out any] struct {
	id     string
	client Client
}

func (w *Run[Out]) ID() string {
	return w.id
}

// Status returns this run's current durable status.
func (w *Run[Out]) Status() (Status, error) {
	info, found, err := w.client.GetRun(context.Background(), w.id)
	if err != nil {
		return "", err
	}
	if !found {
		return "", ErrRunNotFound
	}
	return info.Status, nil
}

// Wake makes pending Sleep calls in this run immediately eligible to execute.
// It returns the number of sleeps made ready.
func (w *Run[Out]) Wake() (int, error) {
	return w.client.Wake(context.Background(), w.id)
}

// Wait blocks until the run finishes.
func (w *Run[Out]) Wait(ctx context.Context) error {
	_, err := w.rawValue(ctx)
	return err
}

// Value blocks, then returns the typed workflow output.
func (w *Run[Out]) Value(ctx context.Context) (Out, error) {
	var zero Out
	raw, err := w.rawValue(ctx)
	if err != nil {
		return zero, err
	}
	if err := json.Unmarshal(raw, &zero); err != nil {
		return zero, fmt.Errorf("workflow: decode run output: %w", err)
	}
	return zero, nil
}

func (w *Run[Out]) Unwrap(ctx context.Context) Out {
	value, err := w.Value(ctx)
	if err != nil {
		panic(err)
	}
	return value
}

func (w *Run[Out]) rawValue(ctx context.Context) (json.RawMessage, error) {
	if execution, active := workflowExecutionFromContext(ctx); active {
		task, err := w.Task()
		if err != nil {
			return nil, err
		}
		if err := awaitSuspendingTask(ctx, execution, task); err != nil {
			return nil, err
		}
	}
	raw, err := w.client.WaitRun(ctx, w.id)
	return raw, err
}

func (w *Run[Out]) Task() (*workflowops.Task, error) {
	return &workflowops.Task{RunID: w.id, Sequence: workflowops.RootSequence}, nil
}

// Reader opens this run's stream from a handler or durable step. It returns
// ErrNoWorkflow when called by the root workflow body.
func (w *Run[Out]) Reader(ctx context.Context, options ...ReaderOption) (io.Reader, error) {
	return w.ReaderAt(ctx, 0, options...)
}

// ReaderAt opens this run's stream at offset. Negative offsets are relative to
// the current end of the stream.
func (w *Run[Out]) ReaderAt(ctx context.Context, offset int64, options ...ReaderOption) (io.Reader, error) {
	if _, active := workflowExecutionFromContext(ctx); active {
		return nil, ErrNoWorkflow
	}
	opts := readerOption(options)
	return newReader(ctx, w.client, workflowops.Stream{
		RunID:     w.id,
		Namespace: opts.Namespace,
	}, offset), nil
}

// Writer obtains this run's stream writer from an application handler or
// durable step. It returns ErrNoWorkflow in a root workflow body.
func (w *Run[Out]) Writer(ctx context.Context, options ...WriterOption) (io.WriteCloser, func() error, error) {
	if _, active := workflowExecutionFromContext(ctx); active {
		return nil, nil, ErrNoWorkflow
	}
	opts := writerOption(options)
	streams, ok := ctx.Value(stepStreamsContextKey{}).(*stepStreams)
	if !ok {
		streams = &stepStreams{}
	}
	value, err := streams.acquire(ctx, w.client, workflowops.Stream{
		RunID:     w.id,
		Namespace: opts.Namespace,
	})
	if err != nil {
		return nil, nil, err
	}
	return value, value.release, nil
}

// Get returns a handle for an existing workflow.
// The handle is bound to the client in ctx. Use NewContext or Middleware before calling Get.
func Get[Out any](ctx context.Context, id string) (*Run[Out], error) {
	client, _, err := clientFromContext(ctx)
	if err != nil {
		return nil, err
	}
	return &Run[Out]{id: id, client: client}, nil
}

// Start enqueues a new workflow. A worker must register the corresponding Runner with its World.
// The handle is bound to the client in ctx. Use NewContext or Middleware before calling Start.
func Start[In workflow[Out], Out any](ctx context.Context, input In) (*Run[Out], error) {
	client, execution, err := clientFromContext(ctx)
	if err != nil {
		return nil, err
	}

	name := typeIdentity[In]()
	raw, err := json.Marshal(input)
	if err != nil {
		return nil, fmt.Errorf("workflow: encode %s input: %w", name, err)
	}
	if execution != nil {
		sequence := execution.Sequence()
		id, err := execution.World().CreateRun(ctx, execution.Lease(), workflowops.RunRequest{
			RunID:       execution.RunID(),
			Sequence:    sequence,
			Kind:        workflowops.KindRun,
			Identity:    name,
			Fingerprint: fingerprint(name, 2),
			Input:       raw,
		})
		if err != nil {
			return nil, err
		}
		return &Run[Out]{id: id, client: client}, nil
	}
	id, err := client.CreateRun(ctx, nil, workflowops.RunRequest{
		Identity:    name,
		Fingerprint: name,
		Input:       raw,
		Sequence:    workflowops.RootSequence,
		Kind:        workflowops.KindRun,
	})
	if err != nil {
		return nil, err
	}
	return &Run[Out]{id: id, client: client}, nil
}

// Runners indexes workflow runners by name, retaining the last runner for
// each name and skipping unnamed runners.
func Runners(runners ...Runner) RunnerSet {
	set := make(RunnerSet, len(runners))
	for _, runner := range runners {
		if name := runner.Name(); name != "" {
			set[name] = runner
		}
	}
	return set
}

// Use converts a typed workflow function into a Runner. Register
// the returned runner with a World in the worker process.
func Use[In workflow[Out], Out any](function func(context.Context, In) (Out, error)) Runner {
	name := typeIdentity[In]()
	return workflowops.NewRunner(name, func(ctx context.Context, raw json.RawMessage) (json.RawMessage, error) {
		var input In
		if err := json.Unmarshal(raw, &input); err != nil {
			return nil, fmt.Errorf("workflow: decode %s input: %w", name, err)
		}
		output, err := function(ctx, input)
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

// Step invokes a durable operation and waits for its value.
func Step[In, Out any](
	ctx context.Context,
	fn func(context.Context, In) (Out, error),
	input In,
	options ...StepOptions,
) (Out, error) {
	return Background(ctx, fn, input, options...).Value(ctx)
}

// Background records a durable command. fn may be a named function or an
// inline closure. The analyzer rejects closures that capture workflow state.
func Background[In, Out any](
	ctx context.Context,
	fn func(context.Context, In) (Out, error),
	input In,
	options ...StepOptions,
) Result[Out] {
	name := functionIdentity(fn)
	execution, active := workflowExecutionFromContext(ctx)
	if !active {
		return errorResult[Out](ErrNoWorkflow)
	}
	return background(execution, ctx, name, fn, input, stepOptions(options))
}

func Sleep(ctx context.Context, duration time.Duration) error {
	execution, active := workflowExecutionFromContext(ctx)
	if !active {
		return ErrNoWorkflow
	}
	return timer(execution, ctx, duration, workflowops.KindSleep, 3).Wait(ctx)
}

// Now returns the current time as a durable workflow value. Replaying the
// workflow returns the time recorded by its original execution.
func Now(ctx context.Context) (time.Time, error) {
	execution, active := workflowExecutionFromContext(ctx)
	if !active {
		return time.Time{}, ErrNoWorkflow
	}
	return now(execution, ctx)
}

// Timeout starts a durable timer that cancels its containing workflow when it
// fires. Cancel the returned task before it fires to disarm the timeout.
func Timeout(ctx context.Context, duration time.Duration) Result[struct{}] {
	execution, active := workflowExecutionFromContext(ctx)
	if !active {
		return errorResult[struct{}](ErrNoWorkflow)
	}
	return timer(execution, ctx, duration, workflowops.KindTimeout, 3)
}

// OnRollback registers fn as a compensation for the current workflow. The
// runtime invokes registered compensations in reverse order when the workflow
// fails. A compensation has no workflow output; it either completes or fails.
func OnRollback[In any](
	ctx context.Context,
	fn func(context.Context, In) error,
	input In,
	options ...StepOptions,
) error {
	name := rollbackIdentity(fn)
	execution, active := workflowExecutionFromContext(ctx)
	if !active {
		return ErrNoWorkflow
	}
	return rollback(execution, ctx, name, fn, input, stepOptions(options))
}

func typeIdentity[In any]() string {
	typeOf := reflect.TypeFor[In]()
	return typeOf.PkgPath() + "." + typeOf.Name()
}

func functionIdentity[In, Out any](fn func(context.Context, In) (Out, error)) string {
	return runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
}

func rollbackIdentity[In any](fn func(context.Context, In) error) string {
	return runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
}

func fingerprint(name string, skip int) string {
	_, file, line, ok := runtime.Caller(skip)
	if !ok {
		return name
	}
	return fmt.Sprintf("%s@%s:%d", name, file, line)
}
