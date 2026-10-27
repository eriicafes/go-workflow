package workflowai

import (
	"context"
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
	OnEnd                func(ctx context.Context, result Result)
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
	StartEvent struct {
		Messages []ModelMessage
	}
	StepStartEvent struct {
		StepNumber int
		Messages   []ModelMessage
	}
	ToolExecutionStartEvent struct {
		StepNumber int
		ToolCall   ToolCall
	}
	// AbortEvent holds the steps that finished before the call was aborted.
	AbortEvent struct {
		Steps []StepResult
	}
	ToolExecutionEndEvent struct {
		StepNumber int
		ToolCall   ToolCall
		Output     any
		Error      error
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
