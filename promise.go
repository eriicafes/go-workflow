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
// Replay delivers results in the order they were recorded, and only where the
// runner waits: Await, Join, and the Select functions. Done reports what has
// been delivered so far, so it gives the same answer at the same point of
// every replay, and Select picks the promise that settled first.
//
// Run implements Promise, and its methods also work in step context and outside
// a run. In workflow context, awaiting a Run that has not finished returns
// ErrRunNotFinished instead of suspending.
type Promise[T any] interface {
	// Done reports whether the promise has settled, whether it succeeded or
	// failed, as of the results delivered so far. It never suspends the run or
	// waits for more results, and it does not return the error the promise
	// failed with, which Await returns. Its error reports only that settlement
	// could not be determined, such as ErrNoWorkflow.
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
	// err is the error declaring the operation failed with, which settles
	// the promise at once.
	err       error
	execution *worlds.Execution
	// id is the step or timer whose settlement replay delivers, and outcome
	// decodes it.
	id        string
	outcome   func() (T, error)
	stepID    worlds.StepID
	waitID    worlds.WaitID
	messageID string
}

// settlement is implemented by promises whose settlement replay delivers from
// the event log, so the Select functions can order them by when they settled.
// It reports false for a promise that settled when it was declared.
type settlement interface {
	settlementID() (string, bool)
}

func (p *promise[T]) settlementID() (string, bool) {
	return p.id, p.err == nil && p.id != ""
}

func errorPromise[T any](err error) Promise[T] {
	return &promise[T]{err: err}
}

func (p *promise[T]) settled() bool {
	return p.err != nil || p.id == "" || p.execution.Settled(p.id)
}

func (p *promise[T]) Done(ctx context.Context) (bool, error) {
	if _, ok := workflowExecutionFromContext(ctx); !ok {
		return false, ErrNoWorkflow
	}
	return p.settled(), nil
}

func (p *promise[T]) Await(ctx context.Context) (T, error) {
	var zero T
	execution, ok := workflowExecutionFromContext(ctx)
	if !ok {
		return zero, ErrNoWorkflow
	}
	if p.err != nil {
		return zero, p.err
	}
	if p.id == "" {
		return zero, nil
	}
	if _, ok := execution.Select(p.id); !ok {
		execution.Suspend()
	}
	return p.outcome()
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
	if p.execution == nil || p.settled() {
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
	// A step that already finished cannot be cancelled, and leaves nothing
	// to do.
	p.execution.CreateEvent(ctx, event, func(err error) error {
		if p.stepID != "" && (errors.Is(err, worlds.ErrStepNotStartable) || errors.Is(err, worlds.ErrRunTerminal)) {
			return nil
		}
		return err
	})
	if p.waitID == "" || p.messageID == "" {
		return nil
	}
	// Cancelling the timer's delivery is best-effort: a delivery left behind
	// only replays the run once more.
	_ = p.execution.World().QueueCancel(ctx, worlds.QueueCancelInput{MessageID: p.messageID})
	return nil
}
