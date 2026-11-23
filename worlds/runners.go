package worlds

import (
	"context"
)

// Runner is the function that executes a workflow, paired with the workflow
// name so core.Runtime can replay it. The workflow name is derived from the
// workflow type, its package path and type name, and runs record it to select
// their Runner. The function receives and returns payloads, and core.Runtime
// passes it the run's core.Execution through its context, whose EncodePayload and
// DecodePayload read and write them.
type Runner struct {
	name string
	run  func(context.Context, []byte) ([]byte, error)
}

// NewRunner returns a Runner that executes the workflow named name with run.
// run receives the run's input payload, the workflow's argument list, and
// returns its output payload.
func NewRunner(name string, run func(context.Context, []byte) ([]byte, error)) Runner {
	return Runner{name: name, run: run}
}

// Name returns the workflow name.
func (r Runner) Name() string { return r.name }

// Run calls the runner's function with input.
func (r Runner) Run(ctx context.Context, input []byte) ([]byte, error) {
	return r.run(ctx, input)
}

// RunnerSet maps workflow names to Runners.
type RunnerSet map[string]Runner

// Runners returns a RunnerSet holding values. A Runner with an empty name is
// skipped, and a later Runner replaces an earlier one with the same name.
func Runners(values ...Runner) RunnerSet {
	set := make(RunnerSet, len(values))
	for _, runner := range values {
		if runner.Name() != "" {
			set[runner.Name()] = runner
		}
	}
	return set
}

// Runtime executes a delivery with Go workflows: it replays the Runner of the
// delivery's run from runners. *core.Runtime implements it. A World builds
// one Runtime without runners and passes it to its Executor for each
// delivery.
type Runtime interface {
	Execute(ctx context.Context, runners RunnerSet, input QueueInput) error
}

// Executor executes each delivery a World's queue makes. A RunnerSet
// executes it with Go workflows on runtime, and a RemoteExecutor sends it to
// a Workflow SDK app instead.
//
// Execute's result is the delivery's outcome: nil acknowledges the message, a
// *Redelivery asks for it again, and any other error makes the World retry it
// as its queue retries failures.
type Executor interface {
	Execute(ctx context.Context, runtime Runtime, input QueueInput) error
}

// Execute executes input with the runners in s on runtime. A run whose
// workflow s does not hold fails with ErrWorkflowNotRegistered. A delivery
// that names no run, such as a Workflow SDK health check queued through
// world-http, holds nothing for Go workflows and is acknowledged.
func (s RunnerSet) Execute(ctx context.Context, runtime Runtime, input QueueInput) error {
	if input.RunID == "" {
		return nil
	}
	return runtime.Execute(ctx, s, input)
}

// BoundExecutor is an Executor with the Runtime it executes on, for World
// code that executes a delivery with a call of Execute(ctx, input). A nil
// Executor executes as an empty RunnerSet, which fails each run as
// unregistered.
type BoundExecutor struct {
	Executor Executor
	Runtime  Runtime
}

// Execute executes input with b.Executor on b.Runtime.
func (b BoundExecutor) Execute(ctx context.Context, input QueueInput) error {
	if b.Executor == nil {
		return RunnerSet(nil).Execute(ctx, b.Runtime, input)
	}
	return b.Executor.Execute(ctx, b.Runtime, input)
}
