package worlds

import (
	"context"
	"encoding/json"
)

// Runner is the function that executes a workflow, paired with the workflow
// name so the Runtime can replay it. The workflow name is derived from the
// workflow type, its package path and type name, and runs record it to select
// their Runner. The function receives and returns JSON, and the Runtime passes
// it the run's Execution through its context.
type Runner struct {
	name string
	run  func(context.Context, json.RawMessage) (json.RawMessage, error)
}

// NewRunner returns a Runner that executes the workflow named name with run.
// run receives the JSON-encoded workflow input and returns the JSON-encoded
// output.
func NewRunner(name string, run func(context.Context, json.RawMessage) (json.RawMessage, error)) Runner {
	return Runner{name: name, run: run}
}

// Name returns the workflow name.
func (r Runner) Name() string { return r.name }

// Run calls the runner's function with input.
func (r Runner) Run(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
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
