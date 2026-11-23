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
