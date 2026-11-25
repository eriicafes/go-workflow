package workflowai

import (
	"context"

	"github.com/digitallysavvy/go-ai/pkg/provider"
)

// Callbacks are called as the agent runs. They run in workflow context, so a
// replay calls them again for the steps it replays. Plain logging may repeat
// on replay; record side effects with workflow.Step or workflow.InlineStep.
type Callbacks struct {
	OnStart              func(ctx context.Context, event StartEvent)
	OnStepStart          func(ctx context.Context, event StepStartEvent)
	OnToolExecutionStart func(ctx context.Context, event ToolExecutionStartEvent)
	OnToolExecutionEnd   func(ctx context.Context, event ToolExecutionEndEvent)
	OnStepEnd            func(ctx context.Context, step StepResult)
	OnEnd                func(ctx context.Context, event EndEvent)
	OnError              func(ctx context.Context, err error)
	// OnAbort is called instead of OnEnd and OnError when a cancelled
	// context or the Timeout aborts the call.
	OnAbort func(ctx context.Context, event AbortEvent)
}

// merge returns callbacks that call c and then other.
func (c Callbacks) merge(other Callbacks) Callbacks {
	return Callbacks{
		OnStart:              chain(c.OnStart, other.OnStart),
		OnStepStart:          chain(c.OnStepStart, other.OnStepStart),
		OnToolExecutionStart: chain(c.OnToolExecutionStart, other.OnToolExecutionStart),
		OnToolExecutionEnd:   chain(c.OnToolExecutionEnd, other.OnToolExecutionEnd),
		OnStepEnd:            chain(c.OnStepEnd, other.OnStepEnd),
		OnEnd:                chain(c.OnEnd, other.OnEnd),
		OnError:              chain(c.OnError, other.OnError),
		OnAbort:              chain(c.OnAbort, other.OnAbort),
	}
}

type (
	// StartEvent describes a call when it starts.
	StartEvent struct {
		Messages       []ModelMessage
		Model          provider.LanguageModel
		RuntimeContext any
		ToolsContext   map[string]any
	}
	// StepStartEvent describes a step before its model call. Steps holds
	// the steps that finished before it.
	StepStartEvent struct {
		StepNumber     int
		Messages       []ModelMessage
		Model          provider.LanguageModel
		Steps          []StepResult
		RuntimeContext any
		ToolsContext   map[string]any
	}
	// ToolExecutionStartEvent describes a tool call that starts executing,
	// with the conversation up to the step that made it and the tool's
	// context.
	ToolExecutionStartEvent struct {
		StepNumber  int
		ToolCall    ToolCall
		Messages    []ModelMessage
		ToolContext any
	}
	// EndEvent is the result of a call that ended, with the runtime and tools
	// context it ended with.
	EndEvent struct {
		Result
		RuntimeContext any
		ToolsContext   map[string]any
	}
	// AbortEvent holds the steps that finished before the call was aborted.
	AbortEvent struct {
		Steps []StepResult
	}
	// ToolExecutionEndEvent describes a tool call that finished executing:
	// its Output when Success is set, its Error otherwise, and how long it
	// ran in workflow time.
	ToolExecutionEndEvent struct {
		StepNumber  int
		ToolCall    ToolCall
		Output      any
		Error       error
		Success     bool
		DurationMs  int64
		Messages    []ModelMessage
		ToolContext any
	}
)

func chain[E any](first, second func(context.Context, E)) func(context.Context, E) {
	return func(ctx context.Context, event E) {
		if first != nil {
			first(ctx, event)
		}
		if second != nil {
			second(ctx, event)
		}
	}
}
