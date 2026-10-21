package workflow

import (
	"context"

	"github.com/eriicafes/go-workflow/worlds"
)

// promise is the private, value-independent settlement contract.
type promise interface {
	done(context.Context) (settled bool, settledErr error, err error)
	settlementID() (string, bool)
}

// Result holds the outcome of a background step, timer, or continuation.
// Reading a pending result suspends the workflow until it settles.
type Result[T any] struct {
	err     error
	id      string
	outcome func() (T, error)
	settled func() bool
}

// NewResult returns an already-settled result containing value and err.
func NewResult[T any](value T, err error) *Result[T] {
	return &Result[T]{
		settled: func() bool { return true },
		outcome: func() (T, error) { return value, err },
	}
}

func (p *Result[T]) settlementID() (string, bool) {
	if p == nil {
		return "", false
	}
	return p.id, p.err == nil && p.id != ""
}

// done reports a nil Result as settled with ErrNilResult.
func (p *Result[T]) done(context.Context) (bool, error, error) {
	if p == nil {
		return true, ErrNilResult, nil
	}
	if p.err != nil {
		return true, p.err, nil
	}
	if !p.settled() {
		return false, nil, nil
	}
	_, err := p.outcome()
	return true, err, nil
}

// Value waits for the result and returns its value or operation error.
func (p *Result[T]) Value(ctx context.Context) (T, error) {
	var zero T
	if _, err := Select(ctx, p); err != nil {
		return zero, err
	}
	return p.outcome()
}

// Unwrap waits for the result and panics if it failed.
func (p *Result[T]) Unwrap(ctx context.Context) T {
	value, err := p.Value(ctx)
	if err != nil {
		panic(err)
	}
	return value
}

// Then follows the result returned by fn after source settles.
// The callback runs in workflow context during replay. Return pending work
// without waiting inside the callback so other Select inputs can still win.
// A nil result from fn settles the chain with ErrNilResult.
func Then[T, U any](ctx context.Context, source *Result[T], fn func(T, error) *Result[U]) *Result[U] {
	execution, ok := workflowExecutionFromContext(ctx)
	if !ok {
		return &Result[U]{err: ErrNoWorkflow}
	}
	var value U
	var cause error
	var next *Result[U]
	started, complete, running := false, false, false
	result := &Result[U]{outcome: func() (U, error) { return value, cause }}
	advance := func() {
		if started || running {
			return
		}
		settled, _, _ := source.done(ctx)
		if !settled {
			return
		}
		// Inline workflow operations can advance replay and re-enter this callback.
		running = true
		input, inputErr := source.Value(ctx)
		next = fn(input, inputErr)
		result.id = string(execution.DeliveredEventID())
		started, running = true, false
	}
	result.settled = func() bool {
		// A returned Then may have been registered after its dependants.
		// Inspect ready sources here too, without requiring another event.
		advance()
		if complete {
			return true
		}
		if !started {
			return false
		}
		settled, settledErr, _ := next.done(ctx)
		if !settled {
			return false
		}
		if settledErr != nil {
			cause = settledErr
		} else {
			value, cause = next.outcome()
		}
		// An already-settled result cannot make the chain settle before its
		// callback ran. A later result determines the chain's settlement instead.
		if id, ok := next.settlementID(); ok {
			if event, found := execution.SettledEvent(id); found {
				after, _ := worlds.EventSlot(worlds.EventID(result.id))
				following, _ := worlds.EventSlot(event.EventID)
				if following > after {
					result.id = string(event.EventID)
				}
			}
		}
		complete = true
		return true
	}
	execution.OnAdvance(advance)
	return result
}
