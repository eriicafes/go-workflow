package workflow

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// StepOptions holds the options for a step.
type StepOptions struct {
	// MaxRetries is the number of times a failed step is retried before it
	// fails. Omitting StepOptions allows 3 retries, while the zero value
	// allows none. A FatalError is never retried.
	MaxRetries int
}

func stepOptions(values []StepOptions) StepOptions {
	if len(values) == 0 {
		return StepOptions{MaxRetries: 3}
	}
	return values[len(values)-1]
}

// Step calls the step fn with input from a workflow and returns its result,
// suspending the run until the step has settled. Step is Background followed by
// Value. It returns ErrNoWorkflow when ctx does not belong to a run.
//
// When the step fails, Step returns the step's error as an *Error, which
// errors.Is and errors.As match against the error the step returned.
func Step[In, Out any](ctx context.Context, fn func(context.Context, In) (Out, error), input In, options ...StepOptions) (Out, error) {
	return Background(ctx, fn, input, options...).Value(ctx)
}

// Background starts the step fn with input from a workflow and returns a
// promise for its result without waiting for it. Outside workflow context, the
// promise fails with ErrNoWorkflow.
//
// The step is identified by fn's function name and its position in the runner.
// It is retried on error according to options, where the last option applies
// and the default allows 3 retries. fn is executed at least once and should be
// idempotent.
func Background[In, Out any](ctx context.Context, fn func(context.Context, In) (Out, error), input In, options ...StepOptions) *Result[Out] {
	execution, ok := workflowExecutionFromContext(ctx)
	if !ok {
		return &Result[Out]{err: ErrNoWorkflow}
	}
	option := stepOptions(options)
	durable := worlds.DurableStep(ctx, worlds.DurableStepOptions{
		Execution:  execution,
		Name:       functionIdentity(fn),
		MaxRetries: option.MaxRetries,
	}, fn, input)
	return &Result[Out]{
		err:     durable.Err,
		id:      string(durable.StepID),
		settled: func() bool { return execution.Settled(string(durable.StepID)) },
		outcome: func() (Out, error) {
			return worlds.StepOutcome[Out](execution, durable.StepID)
		},
	}
}

// InlineStep calls the step fn with input in workflow context, without a step
// delivery, and records its result, so replay returns the recorded result
// instead of calling fn again. Like Step, the step is identified by fn's
// function name and its position in the runner, records input, and is retried
// on error according to options, where the last option applies and the
// default allows 3 retries. It returns the step's final error as an *Error,
// and ErrNoWorkflow outside a run.
//
// A retry suspends the run and runs on a later replay once it is due. Use
// InlineStep for short work that rarely fails, such as generating an ID or
// reading another run, and Step for anything else. fn is executed at least
// once, and a delivery that stopped before recording its result calls it
// again, so it should be idempotent or free of side effects. A World error
// while recording the step retries the delivery.
//
// In step context, InlineStep calls fn without recording it.
func InlineStep[In, Out any](ctx context.Context, fn func(context.Context, In) (Out, error), input In, options ...StepOptions) (Out, error) {
	execution, ok := workflowExecutionFromContext(ctx)
	if !ok {
		if _, inStep := worlds.ExecutionFromContext(ctx); inStep {
			return fn(ctx, input)
		}
		var zero Out
		return zero, ErrNoWorkflow
	}
	option := stepOptions(options)
	result := worlds.DurableInlineStep(ctx, worlds.DurableInlineStepOptions{
		Execution:  execution,
		Name:       functionIdentity(fn),
		MaxRetries: option.MaxRetries,
	}, fn, input)
	return result.Data, result.Err
}

// Sleep suspends the run for duration. Use it instead of time.Sleep in a
// workflow. It is Timer followed by Value.
func Sleep(ctx context.Context, duration time.Duration) error {
	_, err := Timer(ctx, duration).Value(ctx)
	return err
}

// Timer returns a timer, which is a promise that settles after duration.
// Outside workflow context, the promise fails with ErrNoWorkflow.
func Timer(ctx context.Context, duration time.Duration) *Result[struct{}] {
	execution, ok := workflowExecutionFromContext(ctx)
	if !ok {
		return &Result[struct{}]{err: ErrNoWorkflow}
	}
	wait := worlds.DurableWait(ctx, worlds.DurableWaitOptions{
		Execution: execution,
		DueAt:     time.Now().Add(duration),
	})
	return &Result[struct{}]{
		err:     wait.Err,
		id:      string(wait.WaitID),
		settled: func() bool { return execution.Settled(string(wait.WaitID)) || ctx.Err() != nil },
		outcome: func() (struct{}, error) {
			if !execution.Settled(string(wait.WaitID)) && ctx.Err() != nil {
				return struct{}{}, FatalError(context.Canceled)
			}
			return struct{}{}, nil
		},
	}
}

// Rollback registers fn with input as a durable compensation for workflow failure.
// Defer it after the work it reverses. Compensations run in registration order
// and retry according to options. It panics with ErrNoWorkflow outside workflow context.
func Rollback[In any](ctx context.Context, fn func(context.Context, In) error, input In, options ...StepOptions) {
	execution, ok := workflowExecutionFromContext(ctx)
	if !ok {
		panic(ErrNoWorkflow)
	}
	option := stepOptions(options)
	execution.AddRollback(func(ctx context.Context, execution *worlds.Execution) error {
		result := worlds.DurableStep(ctx, worlds.DurableStepOptions{
			Execution:  execution,
			Name:       rollbackFunctionIdentity(fn),
			MaxRetries: option.MaxRetries,
			Rollback:   true,
		}, func(ctx context.Context, value In) (struct{}, error) {
			return struct{}{}, fn(ctx, value)
		}, input)
		if result.Err != nil {
			return result.Err
		}
		// A compensation that fails is recorded and does not stop the others.
		return Join(ctx, &Result[struct{}]{
			id:      string(result.StepID),
			settled: func() bool { return execution.Settled(string(result.StepID)) },
			outcome: func() (struct{}, error) {
				return worlds.StepOutcome[struct{}](execution, result.StepID)
			},
		})
	})
}

// Now returns the current time in UTC, recorded the first time it runs so that
// replay returns the same time. In workflow context, a runner reads time only
// through Now, never time.Now. Now returns ErrNoWorkflow outside workflow
// context, including step context, where time.Now is safe to use.
func Now(ctx context.Context) (time.Time, error) {
	execution, ok := workflowExecutionFromContext(ctx)
	if !ok {
		return time.Time{}, ErrNoWorkflow
	}
	result := worlds.DurableInlineStep(ctx, worlds.DurableInlineStepOptions{
		Execution: execution,
		Name:      "now",
	}, func(context.Context, struct{}) (time.Time, error) {
		return time.Now().UTC(), nil
	}, struct{}{})
	return result.Data, result.Err
}

// Map starts one background step per input, returning results in input order.
func Map[In, Out any](ctx context.Context, step func(context.Context, In) (Out, error), inputs []In) []*Result[Out] {
	results := make([]*Result[Out], len(inputs))
	for index, input := range inputs {
		results[index] = Background(ctx, step, input)
	}
	return results
}

// All starts every step and collects successful values in input order.
// If any step fails, it cancels the others and returns that failure.
func All[In, Out any](ctx context.Context, step func(context.Context, In) (Out, error), inputs []In) ([]Out, error) {
	if len(inputs) == 0 {
		return []Out{}, nil
	}
	cancelCtx, cancel, err := WithCancel(ctx)
	if err != nil {
		return nil, err
	}
	results := Map(cancelCtx, step, inputs)
	pending := make([]promise, len(results))
	for i, result := range results {
		pending[i] = result
	}
	values := make([]Out, len(results))
	for len(pending) != 0 {
		winner, err := Select(ctx, pending...)
		if err != nil {
			return nil, errors.Join(err, cancel())
		}
		result := winner.(*Result[Out])
		value, err := result.Value(ctx)
		if err != nil {
			return nil, errors.Join(err, cancel())
		}
		values[slices.Index(results, result)] = value
		i := slices.Index(pending, winner)
		pending = slices.Delete(pending, i, i+1)
	}
	return values, nil
}

// Join waits for every input to settle, ignoring operation failures.
func Join(ctx context.Context, results ...promise) error {
	for _, result := range results {
		winner, err := Select(ctx, result)
		if err != nil && winner == nil {
			return err
		}
	}
	return nil
}

// JoinOk waits for every input to succeed and returns the first recorded failure.
// It does not cancel the remaining inputs when one fails.
func JoinOk(ctx context.Context, results ...promise) error {
	pending := append([]promise(nil), results...)
	for len(pending) != 0 {
		winner, err := Select(ctx, pending...)
		if err != nil {
			return err
		}
		i := slices.Index(pending, winner)
		pending = slices.Delete(pending, i, i+1)
	}
	return nil
}

// Select waits for the first recorded settlement and returns the original input.
func Select(ctx context.Context, results ...promise) (promise, error) {
	return selectPromise(ctx, false, results...)
}

// SelectOk skips failed inputs until one succeeds or every input fails.
func SelectOk(ctx context.Context, results ...promise) (promise, error) {
	return selectPromise(ctx, true, results...)
}

func selectPromise(ctx context.Context, successful bool, results ...promise) (promise, error) {
	execution, ok := workflowExecutionFromContext(ctx)
	if !ok {
		return nil, ErrNoWorkflow
	}
	for {
		var winner promise
		var winnerErr, firstErr error
		var earliest uint64
		pending := false
		for _, result := range results {
			settled, settledErr, err := result.done()
			if err != nil {
				return nil, err
			}
			if !settled {
				pending = true
				continue
			}
			if successful && settledErr != nil {
				if firstErr == nil {
					firstErr = settledErr
				}
				continue
			}
			var slot uint64
			if id, ok := result.settlementID(); ok {
				if event, found := execution.SettledEvent(id); found {
					slot, _ = worlds.EventSlot(event.EventID)
				}
			}
			if winner == nil || slot < earliest {
				winner, winnerErr, earliest = result, settledErr, slot
			}
		}
		if winner != nil {
			return winner, winnerErr
		}
		if !pending {
			return nil, firstErr
		}
		if !execution.Advance() {
			execution.Suspend()
		}
	}
}
