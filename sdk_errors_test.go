package workflow_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/testworld"
)

type sdkRetriedInput struct{}
type sdkRetriedOutput struct{}

func (sdkRetriedInput) Workflow(sdkRetriedOutput) {}

type sdkWrappedFatalInput struct{}
type sdkWrappedFatalOutput struct{}

func (sdkWrappedFatalInput) Workflow(sdkWrappedFatalOutput) {}

type sdkChildInput struct{ Fail bool }
type sdkChildOutput struct{}

func (sdkChildInput) Workflow(sdkChildOutput) {}

type sdkAwaitChildInput struct{ RunID string }
type sdkAwaitChildOutput struct{}

func (sdkAwaitChildInput) Workflow(sdkAwaitChildOutput) {}

// sdkAttempts counts the attempts of steps by their input.
type sdkAttempts struct {
	mutex sync.Mutex
	count map[string]int
}

func (a *sdkAttempts) add(key string) int {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	if a.count == nil {
		a.count = map[string]int{}
	}
	a.count[key]++
	return a.count[key]
}

func (a *sdkAttempts) get(key string) int {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	return a.count[key]
}

// SDK: packages/errors/src/fatal-error.test.ts > FatalError.is > returns false for plain errors
// SDK: packages/errors/src/terminal-run-error.test.ts > terminal run errors are non-retryable > leaves the non-terminal run errors retryable
func TestSDKStepErrorsThatAreNotFatalAreRetried(t *testing.T) {
	// The workflow starts the steps in the same order on every replay.
	names := []string{"plain", "not finished", "not found"}
	failures := map[string]error{
		"plain":        errors.New("boom"),
		"not finished": workflow.ErrRunNotFinished,
		"not found":    worlds.ErrRunNotFound,
	}
	var attempts sdkAttempts
	world := testworld.New(worlds.Runners(workflow.Use(func(ctx context.Context, _ sdkRetriedInput) (sdkRetriedOutput, error) {
		var results []*workflow.Result[struct{}]
		for _, name := range names {
			results = append(results, workflow.Background(ctx, func(_ context.Context, name string) (struct{}, error) {
				if attempts.add(name) == 1 {
					return struct{}{}, failures[name]
				}
				return struct{}{}, nil
			}, name, workflow.StepOptions{MaxRetries: 1}))
		}
		for _, result := range results {
			if _, err := result.Value(ctx); err != nil {
				return sdkRetriedOutput{}, err
			}
		}
		return sdkRetriedOutput{}, nil
	})))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), 10*time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, sdkRetriedInput{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Value(ctx); err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		if got := attempts.get(name); got != 2 {
			t.Errorf("attempts of the step failing with %s = %d, want 2", name, got)
		}
	}
}

// SDK: packages/errors/src/fatal-error.test.ts > FatalError.is > returns true for any error with fatal: true
func TestSDKAnErrorWrappingAFatalErrorIsNotRetried(t *testing.T) {
	var attempts sdkAttempts
	world := testworld.New(worlds.Runners(workflow.Use(func(ctx context.Context, _ sdkWrappedFatalInput) (sdkWrappedFatalOutput, error) {
		_, err := workflow.Step(ctx, func(context.Context, struct{}) (struct{}, error) {
			attempts.add("fatal")
			return struct{}{}, fmt.Errorf("context violation: %w", workflow.FatalError(errors.New("never succeeds")))
		}, struct{}{}, workflow.StepOptions{MaxRetries: 3})
		return sdkWrappedFatalOutput{}, err
	})))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), 5*time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, sdkWrappedFatalInput{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Value(ctx); err == nil || attempts.get("fatal") != 1 {
		t.Fatalf("run error = %v, attempts = %d, want a failed run after one attempt", err, attempts.get("fatal"))
	}
}

// SDK: packages/errors/src/terminal-run-error.test.ts > terminal run errors are non-retryable > marks WorkflowRunFailedError fatal
func TestSDKAStepAwaitingAFailedRunIsNotRetried(t *testing.T) {
	awaited := awaitChildInStep(t, true)
	var failed *workflow.RunFailedError
	if !errors.As(awaited.err, &failed) || failed.RunID != worlds.RunID(awaited.childID) || failed.ErrorCode != "USER_ERROR" || failed.Cause == nil || failed.Cause.Error() != "boom" {
		t.Fatalf("step error = %#v, want the child's RunFailedError", awaited.err)
	}
	if !workflow.IsFatal(awaited.err) || awaited.attempts != 1 {
		t.Fatalf("fatal = %v, attempts = %d, want a fatal error after one attempt", workflow.IsFatal(awaited.err), awaited.attempts)
	}
}

// SDK: packages/errors/src/terminal-run-error.test.ts > terminal run errors are non-retryable > marks WorkflowRunCancelledError fatal
func TestSDKAStepAwaitingACancelledRunIsNotRetried(t *testing.T) {
	awaited := awaitChildInStep(t, false)
	var cancelled *workflow.RunCancelledError
	if !errors.As(awaited.err, &cancelled) || cancelled.RunID != worlds.RunID(awaited.childID) || !errors.Is(awaited.err, workflow.ErrRunCancelled) {
		t.Fatalf("step error = %#v, want the child's RunCancelledError", awaited.err)
	}
	if !workflow.IsFatal(awaited.err) || awaited.attempts != 1 {
		t.Fatalf("fatal = %v, attempts = %d, want a fatal error after one attempt", workflow.IsFatal(awaited.err), awaited.attempts)
	}
}

// awaitedChild is what a step that awaited a finished child run saw.
type awaitedChild struct {
	childID  string
	err      error
	attempts int
}

// awaitChildInStep finishes a child run, failed when fail is set and
// cancelled otherwise, and awaits it in a step of another run that allows
// three retries.
func awaitChildInStep(t *testing.T, fail bool) awaitedChild {
	t.Helper()
	var mutex sync.Mutex
	var awaited awaitedChild
	world := testworld.New(worlds.Runners(
		workflow.Use(func(ctx context.Context, input sdkChildInput) (sdkChildOutput, error) {
			if input.Fail {
				return sdkChildOutput{}, errors.New("boom")
			}
			_, err := workflow.Timer(ctx, time.Hour).Value(ctx)
			return sdkChildOutput{}, err
		}),
		workflow.Use(func(ctx context.Context, input sdkAwaitChildInput) (sdkAwaitChildOutput, error) {
			_, err := workflow.Step(ctx, func(ctx context.Context, runID string) (struct{}, error) {
				child, err := workflow.Get[sdkChildOutput](ctx, runID)
				if err != nil {
					return struct{}{}, err
				}
				_, err = child.Value(ctx)
				mutex.Lock()
				awaited.err, awaited.attempts = err, awaited.attempts+1
				mutex.Unlock()
				return struct{}{}, err
			}, input.RunID, workflow.StepOptions{MaxRetries: 3})
			return sdkAwaitChildOutput{}, err
		}),
	))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), 10*time.Second)
	defer cancel()
	child, err := workflow.Start(ctx, sdkChildInput{Fail: fail})
	if err != nil {
		t.Fatal(err)
	}
	if !fail {
		if err := child.Cancel(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := child.Value(ctx); err == nil {
		t.Fatal("the child run finished without an error")
	}
	run, err := workflow.Start(ctx, sdkAwaitChildInput{RunID: child.ID()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Value(ctx); err == nil {
		t.Fatal("the run awaiting the child succeeded")
	}
	mutex.Lock()
	defer mutex.Unlock()
	awaited.childID = child.ID()
	return awaited
}
