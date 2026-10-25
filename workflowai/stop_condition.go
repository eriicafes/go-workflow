package workflowai

import (
	"slices"
)

// StopCondition reports whether the loop stops after the latest step.
type StopCondition func(steps []StepResult) bool

// IsStepCount stops the loop once n steps have run.
func IsStepCount(n int) StopCondition {
	return func(steps []StepResult) bool { return len(steps) >= n }
}

// HasToolCall stops the loop after a step that calls any of the named tools.
func HasToolCall(names ...string) StopCondition {
	return func(steps []StepResult) bool {
		if len(steps) == 0 {
			return false
		}
		for _, call := range steps[len(steps)-1].ToolCalls {
			if slices.Contains(names, call.ToolName) {
				return true
			}
		}
		return false
	}
}
