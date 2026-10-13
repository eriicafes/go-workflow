package worlds

import (
	"context"
	"encoding/json"
)

// Runner executes a registered workflow definition. The concrete runtime keeps
// replay and target-step state in Execution rather than exposing it to users.
type Runner struct {
	name string
	run  func(context.Context, json.RawMessage) (json.RawMessage, error)
}

func NewRunner(name string, run func(context.Context, json.RawMessage) (json.RawMessage, error)) Runner {
	return Runner{name: name, run: run}
}

func (r Runner) Name() string { return r.name }

func (r Runner) Run(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
	return r.run(ctx, input)
}

type RunnerSet map[string]Runner

func Runners(values ...Runner) RunnerSet {
	set := make(RunnerSet, len(values))
	for _, runner := range values {
		if runner.Name() != "" {
			set[runner.Name()] = runner
		}
	}
	return set
}
