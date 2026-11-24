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
