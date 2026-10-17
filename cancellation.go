package workflow

import (
	"context"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// WithCancel creates a durable cancellation context in workflow context.
// It returns ErrNoWorkflow outside workflow context.
func WithCancel(ctx context.Context) (context.Context, func() error, error) {
	child, cancel, err := WithCancelCause(ctx)
	if err != nil {
		return nil, nil, err
	}
	return child, func() error { return cancel(nil) }, nil
}

// WithCancelCause creates a durable cancellation context whose first cancellation cause wins.
// It returns ErrNoWorkflow outside workflow context.
func WithCancelCause(ctx context.Context) (context.Context, func(error) error, error) {
	execution, ok := workflowExecutionFromContext(ctx)
	if !ok {
		return nil, nil, ErrNoWorkflow
	}
	return worlds.NewCancelContext(ctx, execution)
}

// WithTimeout creates a durable cancellation context that is cancelled after duration.
// The cancel function cancels the context early and disarms its timer.
// It returns ErrNoWorkflow outside workflow context.
func WithTimeout(ctx context.Context, duration time.Duration) (context.Context, func() error, error) {
	return WithTimeoutCause(ctx, duration, context.DeadlineExceeded)
}

// WithTimeoutCause is WithTimeout with a custom expiry cause.
// A nil cause uses context.DeadlineExceeded. Manual cancellation uses
// context.Canceled and disarms the timer.
func WithTimeoutCause(ctx context.Context, duration time.Duration, cause error) (context.Context, func() error, error) {
	return WithDeadlineCause(ctx, time.Now().Add(duration), cause)
}

// WithDeadline creates a durable cancellation context that is cancelled at deadline.
// Expiry sets context.Cause to context.DeadlineExceeded; manual cancellation uses
// context.Canceled. Cancellation affects steps and timers using the context, and
// does not cancel runs. It returns ErrNoWorkflow outside workflow context.
func WithDeadline(ctx context.Context, deadline time.Time) (context.Context, func() error, error) {
	return WithDeadlineCause(ctx, deadline, context.DeadlineExceeded)
}

// WithDeadlineCause is WithDeadline with a custom expiry cause.
// A nil cause uses context.DeadlineExceeded. Manual cancellation uses
// context.Canceled and disarms the timer.
func WithDeadlineCause(ctx context.Context, deadline time.Time, cause error) (context.Context, func() error, error) {
	cancelCtx, cancel, err := WithCancelCause(ctx)
	if err != nil {
		return nil, nil, err
	}
	timer := Timer(cancelCtx, time.Until(deadline))
	if timer.err != nil {
		return nil, nil, timer.err
	}
	if cause == nil {
		cause = context.DeadlineExceeded
	}
	Then(ctx, timer, func(_ struct{}, err error) *Result[struct{}] {
		if err == nil {
			err = cancel(cause)
		}
		return NewResult(struct{}{}, err)
	})
	return cancelCtx, func() error { return cancel(nil) }, nil
}

// CancelFunc returns the controller carried by a managed step context.
func CancelFunc(ctx context.Context) (func() error, bool) {
	if _, ok := stepExecutionFromContext(ctx); !ok {
		return nil, false
	}
	for _, ref := range worlds.CancellationReferences(ctx) {
		if ref.CanCancel {
			return func() error {
				_, err := worlds.CancelContext(context.WithoutCancel(ctx), ref, context.Canceled)
				return err
			}, true
		}
	}
	return nil, false
}

// CancelAny retains ctx's values and cancels when ctx or a source cancels.
// Outside workflow context, it returns ctx unchanged.
func CancelAny(ctx context.Context, sources ...context.Context) context.Context {
	execution, ok := workflowExecutionFromContext(ctx)
	if !ok {
		return ctx
	}
	return worlds.CombineCancelContexts(ctx, execution, sources...)
}
