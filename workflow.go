// Package workflow makes ordinary named Go functions durable workflows.
package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"time"

	"github.com/eriicafes/go-workflow/workflowops"
)

type (
	// World is the worker storage and dispatch protocol.
	World = workflowops.World
	// Event is one replay-safe workflow output record.
	Event = workflowops.Event
	// Runner is the erased, executable form of a workflow function.
	Runner = workflowops.Runner
	// RunnerSet maps workflow identities to their executable runners.
	RunnerSet = workflowops.RunnerSet
)

// workflow is a marker for a Go type that can start a workflow.
type workflow[Out any] interface {
	Workflow(Out)
}

// Workflow names a root workflow execution.
type Workflow[Out any] struct {
	id  string
	ctx context.Context
}

func (run *Workflow[Out]) ID() string {
	return run.id
}

// Wait blocks until the run finishes.
func (run *Workflow[Out]) Wait(ctx context.Context) error {
	_, err := run.rawValue(ctx)
	return err
}

// Value blocks, then returns the typed workflow output.
func (run *Workflow[Out]) Value(ctx context.Context) (Out, error) {
	var zero Out
	raw, err := run.rawValue(ctx)
	if err != nil {
		return zero, err
	}
	if err := json.Unmarshal(raw, &zero); err != nil {
		return zero, fmt.Errorf("workflow: decode run output: %w", err)
	}
	return zero, nil
}

func (run *Workflow[Out]) Unwrap(ctx context.Context) Out {
	value, err := run.Value(ctx)
	if err != nil {
		panic(err)
	}
	return value
}

func (run *Workflow[Out]) rawValue(ctx context.Context) (json.RawMessage, error) {
	if run == nil {
		return nil, ErrRunNotFound
	}
	if execution, active := executionFromContext(ctx); active {
		task := run.task()
		if task == nil {
			return nil, fmt.Errorf("workflow: run is not durable")
		}
		if err := awaitTask(ctx, execution, task); err != nil {
			return nil, err
		}
	}
	client, err := run.client()
	if err != nil {
		return nil, err
	}
	return client.WaitRun(ctx, run.id)
}

func (run *Workflow[Out]) task() *workflowops.Task {
	if _, active := parentExecutionFromContext(run.ctx); !active {
		return nil
	}
	return &workflowops.Task{RunID: run.id, Sequence: workflowops.RootSequence}
}

// Events returns events emitted by this workflow after the supplied offset.
func (run *Workflow[Out]) Events(ctx context.Context, after int64) ([]Event, error) {
	if run == nil {
		return nil, ErrRunNotFound
	}
	client, err := run.client()
	if err != nil {
		return nil, err
	}
	return client.Events(ctx, run.id, after)
}

func (run *Workflow[Out]) client() (Client, error) {
	if client, err := clientFromContext(run.ctx); err == nil {
		return client, nil
	}
	if execution, active := parentExecutionFromContext(run.ctx); active {
		return execution.World(), nil
	}
	return nil, ErrNoClient
}

// Read will stream event data emitted by this workflow.
//
// TODO: retain the event offset and partial event data, then poll or subscribe
// through Client so Read can block until data or terminal workflow completion.
func (run *Workflow[Out]) Read(data []byte) (int, error) {
	return 0, errors.New("workflow: event streaming is not implemented")
}

// Start enqueues a named workflow input. The input type must declare
// Workflow(Out). A worker must register the corresponding Runner with its
// World.
func Start[In workflow[Out], Out any](ctx context.Context, input In) (*Workflow[Out], error) {
	identity := typeIdentity[In]()
	raw, err := json.Marshal(input)
	if err != nil {
		return nil, fmt.Errorf("workflow: encode %s input: %w", identity, err)
	}
	if execution, active := parentExecutionFromContext(ctx); active {
		sequence := execution.Sequence()
		id, err := execution.World().CreateRun(ctx, execution.Lease(), workflowops.RunRequest{
			RunID:       execution.RunID(),
			Sequence:    sequence,
			Kind:        workflowops.KindRun,
			Identity:    identity,
			Fingerprint: fingerprint(identity, 2),
			Input:       raw,
		})
		if err != nil {
			return nil, err
		}
		return &Workflow[Out]{id: id, ctx: ctx}, nil
	}
	client, err := clientFromContext(ctx)
	if err != nil {
		return nil, err
	}
	id, err := client.CreateRun(ctx, nil, workflowops.RunRequest{
		Identity:    identity,
		Fingerprint: identity,
		Input:       raw,
		Sequence:    workflowops.RootSequence,
		Kind:        workflowops.KindRun,
	})
	if err != nil {
		return nil, err
	}
	return &Workflow[Out]{id: id, ctx: ctx}, nil
}

// Runners validates and indexes workflow runners by identity.
func Runners(runners ...Runner) (RunnerSet, error) {
	set := make(RunnerSet, len(runners))
	for _, runner := range runners {
		identity := runner.Identity()
		if identity == "" {
			return nil, fmt.Errorf("workflow: empty runner identity")
		}
		if _, exists := set[identity]; exists {
			return nil, fmt.Errorf("workflow: duplicate runner: %s", identity)
		}
		set[identity] = runner
	}
	return set, nil
}

// Use converts a typed workflow function into a Runner. Register
// the returned runner with a World in the worker process.
func Use[In workflow[Out], Out any](function func(context.Context, In) (Out, error)) Runner {
	identity := typeIdentity[In]()
	return workflowops.NewRunner(identity, func(ctx context.Context, raw json.RawMessage) (json.RawMessage, error) {
		var input In
		if err := json.Unmarshal(raw, &input); err != nil {
			return nil, fmt.Errorf("workflow: decode %s input: %w", identity, err)
		}
		output, err := function(ctx, input)
		if err != nil {
			return nil, err
		}
		encoded, err := json.Marshal(output)
		if err != nil {
			return nil, fmt.Errorf("workflow: encode %s output: %w", identity, err)
		}
		return encoded, nil
	})
}

// Step invokes a durable operation and waits for its value.
func Step[In, Out any](ctx context.Context, fn func(context.Context, In) (Out, error), input In) (Out, error) {
	return Background(ctx, fn, input).Value(ctx)
}

// Background records a durable command. fn may be a named function or an
// inline closure. The analyzer rejects closures that capture workflow state.
func Background[In, Out any](ctx context.Context, fn func(context.Context, In) (Out, error), input In) Result[Out] {
	identity, err := functionIdentity(fn)
	if err != nil {
		var zero Out
		return resolvedResult(zero, err)
	}
	execution, active := executionFromContext(ctx)
	if !active {
		var zero Out
		return resolvedResult(zero, ErrNoWorkflow)
	}
	return background(execution, ctx, identity, fn, input)
}

func Sleep(ctx context.Context, duration time.Duration) error {
	execution, active := executionFromContext(ctx)
	if !active {
		return ErrNoWorkflow
	}
	return timeout(execution, ctx, duration, 3).Wait(ctx)
}

// Timeout completes after duration as a durable timer operation.
func Timeout(ctx context.Context, duration time.Duration) Result[struct{}] {
	execution, active := executionFromContext(ctx)
	if !active {
		return resolvedResult(struct{}{}, ErrNoWorkflow)
	}
	return timeout(execution, ctx, duration, 3)
}

func typeIdentity[In any]() string {
	typeOf := reflect.TypeFor[In]()
	return typeOf.PkgPath() + "." + typeOf.Name()
}

func functionIdentity[In, Out any](fn func(context.Context, In) (Out, error)) (string, error) {
	return runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name(), nil
}

func fingerprint(identity string, skip int) string {
	_, file, line, ok := runtime.Caller(skip)
	if !ok {
		return identity
	}
	return fmt.Sprintf("%s@%s:%d", identity, file, line)
}
