package workflow

import (
	"context"
	"errors"

	"github.com/eriicafes/go-workflow/worlds"
)

// Promise is a typed workflow-local dependency. Durable steps, hooks, and
// workflow runs satisfy it, allowing Join and Select-style primitives to
// suspend until a value becomes available.
type Promise[T any] interface {
	Done(context.Context) (bool, error)
	Await(context.Context) (T, error)
	Unwrap(context.Context) T
	Cancel(context.Context) error
}

type promise[T any] struct {
	data      T
	done      bool
	err       error
	execution *worlds.Execution
	stepID    worlds.StepID
	waitID    worlds.WaitID
	messageID string
}

func errorPromise[T any](err error) Promise[T] {
	return &promise[T]{done: true, err: err}
}

func (p *promise[T]) Done(ctx context.Context) (bool, error) {
	if _, ok := workflowExecutionFromContext(ctx); !ok {
		return false, ErrNoWorkflow
	}
	return p.done, p.err
}

func (p *promise[T]) Await(ctx context.Context) (T, error) {
	var zero T
	execution, ok := workflowExecutionFromContext(ctx)
	if !ok {
		return zero, ErrNoWorkflow
	}
	if p.done {
		return p.data, p.err
	}
	execution.Suspend()
	return zero, nil
}

func (p *promise[T]) Unwrap(ctx context.Context) T {
	value, err := p.Await(ctx)
	if err != nil {
		panic(err)
	}
	return value
}

func (p *promise[T]) Cancel(ctx context.Context) error {
	if _, ok := workflowExecutionFromContext(ctx); !ok {
		return ErrNoWorkflow
	}
	if p.done {
		return nil
	}
	if p.execution == nil {
		return nil
	}
	event := worlds.EventsCreateEvent{}
	switch {
	case p.stepID != "":
		event.EventType = worlds.EventTypeStepCancelled
		event.CorrelationID = string(p.stepID)
	case p.waitID != "":
		event.EventType = worlds.EventTypeWaitCompleted
		event.CorrelationID = string(p.waitID)
	default:
		return nil
	}
	eventCount := p.execution.EventCount()
	result, err := p.execution.World().EventsCreate(ctx, worlds.EventsCreateInput{
		RunID:   p.execution.Run().RunID,
		Event:   event,
		Options: worlds.EventsCreateOptions{EventCount: &eventCount},
	})
	if p.stepID != "" && (errors.Is(err, worlds.ErrStepNotStartable) || errors.Is(err, worlds.ErrRunTerminal)) {
		return nil
	}
	if err != nil {
		return err
	}
	p.execution.Merge(result, worlds.Event{
		RunID:         p.execution.Run().RunID,
		EventID:       result.EventID,
		EventType:     event.EventType,
		CorrelationID: event.CorrelationID,
		Data:          event.Data,
	})
	if p.waitID == "" || p.messageID == "" {
		return nil
	}
	err = p.execution.World().QueueCancel(ctx, worlds.QueueCancelInput{MessageID: p.messageID})
	if errors.Is(err, worlds.ErrQueueMessageNotFound) {
		return nil
	}
	return err
}
