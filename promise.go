package workflow

import (
	"context"
	"errors"

	"github.com/eriicafes/go-workflow/worlds"
)

// Promise is the pending result of an operation started in workflow context,
// such as a step, timer, hook, or webhook. Awaiting a promise that has not
// settled suspends the run until it does. Join, Select, and related functions
// wait on several promises at once. Promise methods return ErrNoWorkflow
// outside workflow context.
//
// Run implements Promise, and its methods also work in step context and outside
// a run. In workflow context, awaiting a Run that has not finished returns
// ErrRunNotFinished instead of suspending.
type Promise[T any] interface {
	// Done reports whether the promise has settled, whether it succeeded or
	// failed. It never suspends the run, and it does not return the error the
	// promise failed with, which Await returns. Its error reports only that
	// settlement could not be determined, such as ErrNoWorkflow.
	Done(context.Context) (bool, error)
	// Await returns the promise's value, or the error it failed with. When
	// the promise has not settled, Await suspends the run.
	Await(context.Context) (T, error)
	// Unwrap is like Await but panics if Await returns an error.
	Unwrap(context.Context) T
	// Cancel cancels the promise. A step is cancelled, a timer settles
	// immediately, a hook stops accepting payloads, and a child run is
	// cancelled. Cancelling a settled promise is a no-op.
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
	return p.done, nil
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
