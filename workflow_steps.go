package workflow

import (
	"context"
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
// Await. It returns ErrNoWorkflow when ctx does not belong to a run.
//
// When the step fails, Step returns the step's error as an *Error, which
// errors.Is and errors.As match against the error the step returned.
func Step[In, Out any](ctx context.Context, fn func(context.Context, In) (Out, error), input In, options ...StepOptions) (Out, error) {
	return Background(ctx, fn, input, options...).Await(ctx)
}

// Background starts the step fn with input from a workflow and returns a
// promise for its result without waiting for it. Outside workflow context, the
// promise fails with ErrNoWorkflow.
//
// The step is identified by fn's function name and its position in the runner.
// It is retried on error according to options, where the last option applies
// and the default allows 3 retries. fn is executed at least once and should be
// idempotent.
func Background[In, Out any](ctx context.Context, fn func(context.Context, In) (Out, error), input In, options ...StepOptions) Promise[Out] {
	execution, ok := workflowExecutionFromContext(ctx)
	if !ok {
		return errorPromise[Out](ErrNoWorkflow)
	}
	option := stepOptions(options)
	durable := worlds.DurableStep(ctx, worlds.DurableStepOptions{
		Execution:  execution,
		Name:       functionIdentity(fn),
		MaxRetries: option.MaxRetries,
		RetryAfter: func(cause error, attempt int) *time.Time {
			return stepRetryAfter(cause, option, attempt)
		},
	}, fn, input)
	return &promise[Out]{
		err:       durable.Err,
		execution: execution,
		id:        string(durable.StepID),
		outcome: func() (Out, error) {
			return worlds.StepOutcome[Out](execution, durable.StepID)
		},
		stepID: durable.StepID,
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
		RetryAfter: func(cause error, attempt int) *time.Time {
			return stepRetryAfter(cause, option, attempt)
		},
	}, fn, input)
	return result.Data, result.Err
}

// Sleep suspends the run for duration. Use it instead of time.Sleep in a
// workflow. It is Timer followed by Await.
func Sleep(ctx context.Context, duration time.Duration) error {
	_, err := Timer(ctx, duration).Await(ctx)
	return err
}

// Timer returns a timer, which is a promise that settles after duration.
// Outside workflow context, the promise fails with ErrNoWorkflow.
func Timer(ctx context.Context, duration time.Duration) Promise[struct{}] {
	return timer(ctx, duration, false)
}

// Timeout returns a timer that cancels the run when it fires. Cancel the
// returned promise to disarm it.
func Timeout(ctx context.Context, duration time.Duration) Promise[struct{}] {
	return timer(ctx, duration, true)
}

func timer(ctx context.Context, duration time.Duration, cancel bool) Promise[struct{}] {
	execution, ok := workflowExecutionFromContext(ctx)
	if !ok {
		return errorPromise[struct{}](ErrNoWorkflow)
	}
	wait := worlds.DurableWait(ctx, worlds.DurableWaitOptions{
		Execution: execution,
		DueAt:     time.Now().Add(duration),
		Cancel:    cancel,
	})
	return &promise[struct{}]{
		err:       wait.Err,
		execution: execution,
		id:        string(wait.WaitID),
		outcome:   func() (struct{}, error) { return struct{}{}, nil },
		waitID:    wait.WaitID,
		messageID: wait.QueueMessageID,
	}
}

// OnRollback registers fn with input as a compensation step that runs if the
// run later fails. Compensations run in reverse registration order before
// the run is marked failed, and retry according to options like other steps. It
// returns ErrNoWorkflow when ctx does not belong to a run.
func OnRollback[In any](ctx context.Context, fn func(context.Context, In) error, input In, options ...StepOptions) error {
	execution, ok := workflowExecutionFromContext(ctx)
	if !ok {
		return ErrNoWorkflow
	}
	option := stepOptions(options)
	execution.AddRollback(func(ctx context.Context, execution *worlds.Execution) error {
		result := worlds.DurableStep(ctx, worlds.DurableStepOptions{
			Execution:  execution,
			Name:       rollbackFunctionIdentity(fn),
			MaxRetries: option.MaxRetries,
			RetryAfter: func(cause error, attempt int) *time.Time {
				return stepRetryAfter(cause, option, attempt)
			},
			Rollback: true,
		}, func(ctx context.Context, value In) (struct{}, error) {
			return struct{}{}, fn(ctx, input)
		}, input)
		if result.Err != nil {
			return result.Err
		}
		// A compensation that fails is recorded and does not stop the others.
		if _, ok := execution.Select(string(result.StepID)); !ok {
			execution.Suspend()
		}
		return nil
	})
	return nil
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

// Map starts the step once for each input, as Background does, and returns
// their promises in input order.
func Map[In, Out any](ctx context.Context, step func(context.Context, In) (Out, error), inputs []In) []Promise[Out] {
	results := make([]Promise[Out], len(inputs))
	for index, input := range inputs {
		results[index] = Background(ctx, step, input)
	}
	return results
}

// All calls the step once for each input and returns their outputs in input
// order, suspending the run until every one has settled. It is Map followed by
// Join.
func All[In, Out any](ctx context.Context, step func(context.Context, In) (Out, error), inputs []In) ([]Out, error) {
	return Join(ctx, Map(ctx, step, inputs)...)
}

// Join waits for every promise and returns their values in argument order. A
// promise that has not settled suspends the run. Join returns the first error
// it finds in argument order, and ErrNoWorkflow when ctx does not belong to a
// run.
func Join[T any](ctx context.Context, results ...Promise[T]) ([]T, error) {
	if _, ok := workflowExecutionFromContext(ctx); !ok {
		return nil, ErrNoWorkflow
	}
	values := make([]T, len(results))
	for index, result := range results {
		value, err := result.Await(ctx)
		if err != nil {
			return nil, err
		}
		values[index] = value
	}
	return values, nil
}

// JoinSettled waits for every promise to settle and returns their values in
// argument order, with the zero value for each promise that failed. Failed
// promises do not make JoinSettled fail, so inspect each one with Await.
// It returns ErrNoWorkflow when ctx does not belong to a run.
func JoinSettled[T any](ctx context.Context, results ...Promise[T]) ([]T, error) {
	if _, ok := workflowExecutionFromContext(ctx); !ok {
		return nil, ErrNoWorkflow
	}
	values := make([]T, len(results))
	for index, result := range results {
		value, _ := result.Await(ctx)
		values[index] = value
	}
	return values, nil
}

// Cancel cancels every promise in argument order and stops at the first error.
// Promises that have already settled are left unchanged.
func Cancel[T any](ctx context.Context, results ...Promise[T]) error {
	for _, result := range results {
		if err := result.Cancel(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Select waits until one of the promises has settled and returns it with the
// error it settled with. The promise that settled first wins, in the order its
// result was recorded, so every replay picks the same one. When none has
// settled, the run suspends.
func Select[T any](ctx context.Context, results ...Promise[T]) (Promise[T], error) {
	return selectPromise(ctx, false, results...)
}

// SelectOk waits until one of the promises has completed successfully and
// returns it, the first to complete winning as in Select. Failed promises are
// skipped while another can still succeed. When all have failed, SelectOk
// returns a nil promise and the first error in argument order.
func SelectOk[T any](ctx context.Context, results ...Promise[T]) (Promise[T], error) {
	return selectPromise(ctx, true, results...)
}

// SelectWithCancel is like Select but cancels every other promise once one has
// settled.
func SelectWithCancel[T any](ctx context.Context, results ...Promise[T]) (Promise[T], error) {
	return selectWithCancel(ctx, false, results...)
}

// SelectOkWithCancel is like SelectOk but cancels every other promise once one
// has completed successfully.
func SelectOkWithCancel[T any](ctx context.Context, results ...Promise[T]) (Promise[T], error) {
	return selectWithCancel(ctx, true, results...)
}

func selectPromise[T any](ctx context.Context, successful bool, results ...Promise[T]) (Promise[T], error) {
	index, err := selectPromiseIndex(ctx, successful, results...)
	if index < 0 {
		return nil, err
	}
	return results[index], err
}

func selectWithCancel[T any](ctx context.Context, successful bool, results ...Promise[T]) (Promise[T], error) {
	index, err := selectPromiseIndex(ctx, successful, results...)
	if index < 0 {
		return nil, err
	}
	winner := results[index]
	for resultIndex, result := range results {
		if resultIndex == index {
			continue
		}
		if cancelErr := result.Cancel(ctx); cancelErr != nil {
			return winner, cancelErr
		}
	}
	return winner, err
}

// selectPromiseIndex returns the index of the promise that settled first, or
// completed first when successful is true. Promises that settled when they
// were declared come first, in argument order. The rest are ordered by when
// replay delivers their results. A promise that does not report its
// settlement to replay, such as a user-defined one, is checked with Done each
// time the walk delivers a result.
func selectPromiseIndex[T any](ctx context.Context, successful bool, results ...Promise[T]) (int, error) {
	execution, ok := workflowExecutionFromContext(ctx)
	if !ok {
		return -1, ErrNoWorkflow
	}
	pending := make(map[int]bool, len(results))
	for index := range results {
		pending[index] = true
	}
	firstErrIndex, firstErr := -1, error(nil)
	settle := func(index int) (int, error, bool) {
		delete(pending, index)
		_, err := results[index].Await(ctx)
		if !successful || err == nil {
			return index, err, true
		}
		if firstErrIndex < 0 || index < firstErrIndex {
			firstErrIndex, firstErr = index, err
		}
		return 0, nil, false
	}
	for len(pending) > 0 {
		// Promises outside the walk, in argument order.
		var ids []string
		owners := make(map[string]int)
		settledNow := -1
		for index := range results {
			if !pending[index] {
				continue
			}
			if s, ok := results[index].(settlement); ok {
				if id, ok := s.settlementID(); ok {
					if _, seen := owners[id]; !seen {
						owners[id] = index
						ids = append(ids, id)
					}
					continue
				}
			}
			done, err := results[index].Done(ctx)
			if err != nil {
				return -1, err
			}
			if done && settledNow < 0 {
				settledNow = index
			}
		}
		if settledNow >= 0 {
			if index, err, won := settle(settledNow); won {
				return index, err
			}
			continue
		}
		if len(ids) == 0 {
			if len(pending) > 0 {
				execution.Suspend()
			}
			break
		}
		id, ok := execution.Select(ids...)
		if !ok {
			execution.Suspend()
		}
		if index, err, won := settle(owners[id]); won {
			return index, err
		}
		// Every promise with this settlement failed.
		for index := range pending {
			if s, ok := results[index].(settlement); ok {
				if other, ok := s.settlementID(); ok && other == id {
					settle(index)
				}
			}
		}
	}
	return -1, firstErr
}
