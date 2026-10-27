package workflow_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/testworld"
)

type composedInput struct{}
type composedOutput struct {
	Value int
	Name  string
}

func (composedInput) Workflow(composedOutput) {}

func TestThenRecoversAndRunsDependentStepsAcrossReplay(t *testing.T) {
	var sourceCalls, dependentCalls atomic.Int32
	world := testworld.New(workflow.Use(func(ctx context.Context, _ composedInput) (composedOutput, error) {
		source := workflow.Background(ctx, func(context.Context, struct{}) (string, error) {
			sourceCalls.Add(1)
			return "", workflow.FatalError(errors.New("missing"))
		}, struct{}{})
		recovered := workflow.Then(ctx, source, func(value string, err error) *workflow.Result[string] {
			if err != nil {
				return workflow.NewResult("Guest", nil)
			}
			return workflow.NewResult(value, nil)
		})
		calls := 0
		length := workflow.Then(ctx, recovered, func(value string, err error) *workflow.Result[int] {
			calls++
			if err != nil {
				return workflow.NewResult(0, err)
			}
			return workflow.Background(ctx, func(_ context.Context, name string) (int, error) {
				dependentCalls.Add(1)
				return len(name), nil
			}, value)
		})
		if err := workflow.JoinOk(ctx, recovered, length); err != nil {
			return composedOutput{}, err
		}
		value, err := length.Value(ctx)
		if err != nil {
			return composedOutput{}, err
		}
		if _, err := length.Value(ctx); err != nil {
			return composedOutput{}, err
		}
		if calls != 1 {
			return composedOutput{}, errors.New("completed callback ran again")
		}
		return composedOutput{Value: value, Name: recovered.Unwrap(ctx)}, nil
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), 2*time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, composedInput{})
	if err != nil {
		t.Fatal(err)
	}
	output, err := run.Value(ctx)
	if err != nil || output.Value != 5 || output.Name != "Guest" || sourceCalls.Load() != 1 || dependentCalls.Load() != 1 {
		t.Fatalf("output=%#v err=%v calls=%d/%d", output, err, sourceCalls.Load(), dependentCalls.Load())
	}
}

func TestThenAllowsOtherSelectWinnerWhileReturnedStepIsPending(t *testing.T) {
	started := make(chan struct{})
	world := testworld.New(workflow.Use(func(ctx context.Context, _ composedInput) (composedOutput, error) {
		source := workflow.Background(ctx, increment, 1)
		chained := workflow.Then(ctx, source, func(value int, err error) *workflow.Result[int] {
			if err != nil {
				return workflow.NewResult(0, err)
			}
			return workflow.Background(ctx, func(ctx context.Context, value int) (int, error) {
				close(started)
				<-ctx.Done()
				return 0, ctx.Err()
			}, value)
		})
		other := workflow.Background(ctx, func(ctx context.Context, _ struct{}) (string, error) {
			select {
			case <-started:
				return "other", nil
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}, struct{}{})
		winner, err := workflow.Select(ctx, chained, other)
		if err != nil {
			return composedOutput{}, err
		}
		if winner != other {
			return composedOutput{}, errors.New("chain settled before its returned step")
		}
		return composedOutput{Name: other.Unwrap(ctx)}, nil
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), 2*time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, composedInput{})
	if err != nil {
		t.Fatal(err)
	}
	output, err := run.Value(ctx)
	if err != nil || output.Name != "other" {
		t.Fatalf("output=%#v err=%v", output, err)
	}
}

func TestThenFollowsNestedChainWithoutAnotherEvent(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, _ composedInput) (composedOutput, error) {
		source := workflow.Background(ctx, increment, 1)
		outer := workflow.Then(ctx, source, func(value int, err error) *workflow.Result[int] {
			if err != nil {
				return workflow.NewResult(0, err)
			}
			inner := workflow.Background(ctx, increment, value)
			return workflow.Then(ctx, inner, func(value int, err error) *workflow.Result[int] {
				return workflow.NewResult(value*2, err)
			})
		})
		dependent := workflow.Then(ctx, outer, func(value int, err error) *workflow.Result[int] {
			return workflow.NewResult(value+4, err)
		})
		value, err := dependent.Value(ctx)
		return composedOutput{Value: value}, err
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), 2*time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, composedInput{})
	if err != nil {
		t.Fatal(err)
	}
	output, err := run.Value(ctx)
	if err != nil || output.Value != 10 {
		t.Fatalf("output=%#v err=%v", output, err)
	}
}

func TestThenAdoptsReturnedResultFailure(t *testing.T) {
	for _, background := range []bool{false, true} {
		t.Run(map[bool]string{false: "immediate", true: "background"}[background], func(t *testing.T) {
			failure := errors.New("returned result failed")
			world := testworld.New(workflow.Use(func(ctx context.Context, _ composedInput) (composedOutput, error) {
				chained := workflow.Then(ctx, workflow.NewResult(1, nil), func(value int, err error) *workflow.Result[int] {
					if background {
						return workflow.Background(ctx, func(context.Context, int) (int, error) {
							return 0, workflow.FatalError(failure)
						}, value)
					}
					return workflow.NewResult(0, failure)
				})
				winner, err := workflow.Select(ctx, chained)
				if winner != chained || !errors.Is(err, failure) {
					return composedOutput{}, errors.New("chain did not adopt its returned result's failure")
				}
				if _, err := chained.Value(ctx); !errors.Is(err, failure) {
					return composedOutput{}, errors.New("chain lost its returned result's failure")
				}
				return composedOutput{Name: "failed"}, nil
			}))
			defer world.Close()
			ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), 2*time.Second)
			defer cancel()
			run, err := workflow.Start(ctx, composedInput{})
			if err != nil {
				t.Fatal(err)
			}
			output, err := run.Value(ctx)
			if err != nil || output.Name != "failed" {
				t.Fatalf("output=%#v err=%v", output, err)
			}
		})
	}
}

func TestThenPreservesRecordedSettlementOrder(t *testing.T) {
	for _, test := range []struct {
		name    string
		later   bool
		failure bool
	}{
		{name: "earlier returned result"},
		{name: "later returned result", later: true},
		{name: "immediate failure", failure: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			world := testworld.NewManual()
			defer world.Close()
			runner := workflow.Use(func(ctx context.Context, _ composedInput) (composedOutput, error) {
				first := workflow.Background(ctx, increment, 0)
				middle := workflow.Background(ctx, increment, 1)
				last := workflow.Background(ctx, increment, 2)
				source, returned := last, first
				if test.later {
					source, returned = first, last
				}
				chained := workflow.Then(ctx, source, func(value int, err error) *workflow.Result[int] {
					if test.failure {
						return workflow.NewResult(0, errors.New("chain failed"))
					}
					return returned
				})
				if err := workflow.JoinOk(ctx, first, middle, last); err != nil {
					return composedOutput{}, err
				}
				winner, err := workflow.Select(ctx, chained, middle)
				if err != nil {
					return composedOutput{}, err
				}
				if winner != middle {
					return composedOutput{}, errors.New("chain won before it could settle")
				}
				return composedOutput{Value: middle.Unwrap(ctx)}, nil
			})
			// Steps are queued, so the manual queue decides the order they settle in.
			ctx, cancel := context.WithTimeout(workflow.SetMaxInlineSteps(context.Background(), 0), 2*time.Second)
			defer cancel()
			clientCtx := workflow.NewContext(ctx, workflow.NewClient(world))
			run, err := workflow.Start(clientCtx, composedInput{})
			if err != nil {
				t.Fatal(err)
			}
			runtime := worlds.NewRuntime(world, worlds.Runners(runner))
			// The manual queue executes first, middle, then last before replaying
			// the flow, so every result is visible when Select compares them.
			for len(world.QueuedMessages()) != 0 {
				message := world.QueuedMessages()[0]
				if err := runtime.Execute(ctx, message.Input); err != nil {
					t.Fatal(err)
				}
				if err := world.QueueCancel(ctx, worlds.QueueCancelInput{MessageID: message.ID}); err != nil {
					t.Fatal(err)
				}
			}
			output, err := run.Value(clientCtx)
			if err != nil || output.Value != 2 {
				t.Fatalf("output=%#v err=%v", output, err)
			}
		})
	}
}
