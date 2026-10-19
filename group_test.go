package workflow_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/testworld"
)

type groupInput struct{}
type groupOutput struct{ Value int }

func (groupInput) Workflow(groupOutput) {}

func TestMapResultsPreservesHandlesAndOrder(t *testing.T) {
	first, second := workflow.NewResult(1, nil), workflow.NewResult(2, nil)
	inputs := workflow.MapResults([]*workflow.Result[int]{first, second})
	if len(inputs) != 2 || inputs[0] != first || inputs[1] != second {
		t.Fatalf("mapped inputs = %v", inputs)
	}
	if len(workflow.MapResults[int](nil)) != 0 {
		t.Fatal("nil results produced inputs")
	}
}

func TestWaitGroupTracksMixedResultsHooksAndGroups(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		world := testworld.New(workflow.Use(func(ctx context.Context, _ groupInput) (groupOutput, error) {
			var group, nested workflow.WaitGroup
			first := workflow.Go(ctx, &group, increment, 1)
			text := workflow.Go(ctx, &group, func(_ context.Context, value string) (string, error) {
				return value + "!", nil
			}, "hello")
			results := workflow.Map(ctx, increment, []int{2, 3})
			group.Add(workflow.MapResults(results)...)
			nested.Add(workflow.Timer(ctx, time.Hour), workflow.NewResult(0, errors.New("ignored")))
			group.Add(&nested)
			hook := workflow.CreateHook[int](ctx)
			defer hook.Dispose(ctx)
			if _, err := hook.GetConflict(ctx); err != nil {
				return groupOutput{}, err
			}
			workflow.Go(ctx, &group, func(ctx context.Context, token string) (struct{}, error) {
				return struct{}{}, workflow.ResumeHook(ctx, token, 5)
			}, hook.Token())
			group.Add(hook)
			if err := group.Wait(ctx); err != nil {
				return groupOutput{}, err
			}
			value, err := hook.Next(ctx)
			if err != nil {
				return groupOutput{}, err
			}
			if text.Unwrap(ctx) != "hello!" {
				return groupOutput{}, errors.New("wrong string result")
			}
			return groupOutput{Value: first.Unwrap(ctx) + results[0].Unwrap(ctx) + results[1].Unwrap(ctx) + value}, nil
		}))
		defer world.Close()
		ctx := workflow.NewContext(t.Context(), workflow.NewClient(world))
		run, err := workflow.Start(ctx, groupInput{})
		if err != nil {
			t.Fatal(err)
		}
		out, err := run.Value(ctx)
		if err != nil || out.Value != 14 {
			t.Fatalf("result = %+v, %v", out, err)
		}
	})
}

func TestErrGroupWaitsForAllAndReturnsFirstRecordedFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		firstFailure, laterFailure := errors.New("first failure"), errors.New("later failure")
		world := testworld.New(workflow.Use(func(ctx context.Context, _ groupInput) (groupOutput, error) {
			var group workflow.ErrGroup
			start, err := workflow.Now(ctx)
			if err != nil {
				return groupOutput{}, err
			}
			// Declaration order differs from failure order.
			workflow.Go(ctx, &group, func(context.Context, struct{}) (int, error) {
				time.Sleep(2 * time.Hour)
				return 0, workflow.FatalError(laterFailure)
			}, struct{}{})
			workflow.Go(ctx, &group, func(context.Context, struct{}) (string, error) {
				time.Sleep(time.Hour)
				return "", workflow.FatalError(firstFailure)
			}, struct{}{})
			group.Add(workflow.Timer(ctx, 3*time.Hour))
			if err := group.Wait(ctx); !errors.Is(err, firstFailure) {
				return groupOutput{}, fmt.Errorf("group error = %v", err)
			}
			end, err := workflow.Now(ctx)
			if err != nil {
				return groupOutput{}, err
			}
			if end.Sub(start) < 3*time.Hour {
				return groupOutput{}, errors.New("group returned before every member settled")
			}
			return groupOutput{Value: 1}, nil
		}))
		defer world.Close()
		ctx := workflow.NewContext(t.Context(), workflow.NewClient(world))
		run, err := workflow.Start(ctx, groupInput{})
		if err != nil {
			t.Fatal(err)
		}
		out, err := run.Value(ctx)
		if err != nil || out.Value != 1 {
			t.Fatalf("result = %+v, %v", out, err)
		}
	})
}

func TestErrGroupCancelsAfterObservationAndStillWaitsForAll(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		failure := errors.New("failed work")
		started := make(chan struct{}, 1)
		var cancelled atomic.Bool
		world := testworld.New(workflow.Use(func(ctx context.Context, _ groupInput) (groupOutput, error) {
			group, groupCtx, err := workflow.ErrGroupWithContext(ctx)
			if err != nil {
				return groupOutput{}, err
			}
			blocked := workflow.Go(groupCtx, group, func(ctx context.Context, _ struct{}) (int, error) {
				select {
				case started <- struct{}{}:
				default:
				}
				<-ctx.Done()
				cancelled.Store(errors.Is(context.Cause(ctx), failure))
				return 0, nil
			}, struct{}{})
			workflow.Go(groupCtx, group, func(context.Context, struct{}) (string, error) {
				<-started
				time.Sleep(10 * time.Minute)
				return "", workflow.FatalError(failure)
			}, struct{}{})
			// This timer observes the original context, so Add must not cancel it.
			timer := workflow.Timer(ctx, 2*time.Hour)
			group.Add(timer)
			early := workflow.Timer(ctx, time.Minute)
			winner, err := workflow.Select(ctx, group, early)
			if err != nil || winner != early {
				return groupOutput{}, fmt.Errorf("early winner = %T, %v", winner, err)
			}
			if err := workflow.Sleep(ctx, time.Hour); err != nil {
				return groupOutput{}, err
			}
			if !cancelled.Load() {
				return groupOutput{}, errors.New("cancellation waited for group.Wait")
			}
			if err := group.Wait(ctx); !errors.Is(err, failure) {
				return groupOutput{}, fmt.Errorf("group error = %v", err)
			}
			if _, err := blocked.Value(ctx); !errors.Is(err, context.Canceled) {
				return groupOutput{}, fmt.Errorf("cancelled step error = %v", err)
			}
			if _, err := timer.Value(ctx); err != nil {
				return groupOutput{}, err
			}
			return groupOutput{Value: 1}, nil
		}))
		defer world.Close()
		ctx := workflow.NewContext(t.Context(), workflow.NewClient(world))
		run, err := workflow.Start(ctx, groupInput{})
		if err != nil {
			t.Fatal(err)
		}
		out, err := run.Value(ctx)
		if err != nil || out.Value != 1 {
			t.Fatalf("result = %+v, %v", out, err)
		}
	})
}

func TestGroupsSelectByFinalMemberSettlement(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		world := testworld.New(workflow.Use(func(ctx context.Context, _ groupInput) (groupOutput, error) {
			var group workflow.WaitGroup
			group.Add(workflow.Timer(ctx, 2*time.Hour), workflow.Timer(ctx, time.Hour))
			timer := workflow.Timer(ctx, 90*time.Minute)
			winner, err := workflow.Select(ctx, &group, timer)
			if err != nil || winner != timer {
				return groupOutput{}, fmt.Errorf("first winner = %T, %v", winner, err)
			}
			if err := workflow.Join(ctx, &group); err != nil {
				return groupOutput{}, err
			}
			// Both are ready now; argument order must not choose the group.
			winner, err = workflow.Select(ctx, &group, timer)
			if err != nil || winner != timer {
				return groupOutput{}, fmt.Errorf("recorded winner = %T, %v", winner, err)
			}
			return groupOutput{Value: 1}, nil
		}))
		defer world.Close()
		ctx := workflow.NewContext(t.Context(), workflow.NewClient(world))
		run, err := workflow.Start(ctx, groupInput{})
		if err != nil {
			t.Fatal(err)
		}
		out, err := run.Value(ctx)
		if err != nil || out.Value != 1 {
			t.Fatalf("result = %+v, %v", out, err)
		}
	})
}

func TestErrGroupWaitCancelsOnSuccess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		world := testworld.New(workflow.Use(func(ctx context.Context, _ groupInput) (groupOutput, error) {
			group, groupCtx, err := workflow.ErrGroupWithContext(ctx)
			if err != nil {
				return groupOutput{}, err
			}
			workflow.Go(groupCtx, group, increment, 1)
			if err := group.Wait(ctx); err != nil {
				return groupOutput{}, err
			}
			if !errors.Is(context.Cause(groupCtx), context.Canceled) {
				return groupOutput{}, fmt.Errorf("context cause = %v", context.Cause(groupCtx))
			}
			return groupOutput{Value: 1}, nil
		}))
		defer world.Close()
		ctx := workflow.NewContext(t.Context(), workflow.NewClient(world))
		run, err := workflow.Start(ctx, groupInput{})
		if err != nil {
			t.Fatal(err)
		}
		out, err := run.Value(ctx)
		if err != nil || out.Value != 1 {
			t.Fatalf("result = %+v, %v", out, err)
		}
	})
}

func TestErrGroupSelectionWaitsForEveryMember(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		failure := errors.New("failed member")
		world := testworld.New(workflow.Use(func(ctx context.Context, _ groupInput) (groupOutput, error) {
			var group workflow.ErrGroup
			group.Add(workflow.NewResult(0, failure), workflow.Timer(ctx, 2*time.Hour))
			timer := workflow.Timer(ctx, time.Hour)
			winner, err := workflow.Select(ctx, &group, timer)
			if err != nil || winner != timer {
				return groupOutput{}, fmt.Errorf("early winner = %T, %v", winner, err)
			}
			winner, err = workflow.Select(ctx, &group)
			if winner != &group || !errors.Is(err, failure) {
				return groupOutput{}, fmt.Errorf("group winner = %T, %v", winner, err)
			}
			var empty workflow.WaitGroup
			if err := empty.Wait(ctx); err != nil {
				return groupOutput{}, err
			}
			return groupOutput{Value: 1}, nil
		}))
		defer world.Close()
		ctx := workflow.NewContext(t.Context(), workflow.NewClient(world))
		run, err := workflow.Start(ctx, groupInput{})
		if err != nil {
			t.Fatal(err)
		}
		out, err := run.Value(ctx)
		if err != nil || out.Value != 1 {
			t.Fatalf("result = %+v, %v", out, err)
		}
	})
}

func TestGroupIncludesWorkAddedByContinuation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		world := testworld.New(workflow.Use(func(ctx context.Context, _ groupInput) (groupOutput, error) {
			var group workflow.WaitGroup
			start, err := workflow.Now(ctx)
			if err != nil {
				return groupOutput{}, err
			}
			chain := workflow.Then(ctx, workflow.Timer(ctx, time.Hour), func(_ struct{}, err error) *workflow.Result[int] {
				group.Add(workflow.Timer(ctx, time.Hour))
				return workflow.NewResult(1, err)
			})
			group.Add(chain)
			if err := group.Wait(ctx); err != nil {
				return groupOutput{}, err
			}
			end, err := workflow.Now(ctx)
			if err != nil {
				return groupOutput{}, err
			}
			if end.Sub(start) < 2*time.Hour {
				return groupOutput{}, errors.New("work added by continuation did not settle")
			}
			return groupOutput{Value: chain.Unwrap(ctx)}, nil
		}))
		defer world.Close()
		ctx := workflow.NewContext(t.Context(), workflow.NewClient(world))
		run, err := workflow.Start(ctx, groupInput{})
		if err != nil {
			t.Fatal(err)
		}
		out, err := run.Value(ctx)
		if err != nil || out.Value != 1 {
			t.Fatalf("result = %+v, %v", out, err)
		}
	})
}

func TestGroupObservesFailuresOnlyAfterFirstWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		failure := errors.New("unobserved failure")
		world := testworld.New(workflow.Use(func(ctx context.Context, _ groupInput) (groupOutput, error) {
			group, groupCtx, err := workflow.ErrGroupWithContext(ctx)
			if err != nil {
				return groupOutput{}, err
			}
			group.Add(workflow.NewResult(0, failure))
			if err := workflow.Sleep(ctx, time.Hour); err != nil {
				return groupOutput{}, err
			}
			if groupCtx.Err() != nil {
				return groupOutput{}, errors.New("group observed failure before its first wait")
			}
			if err := group.Wait(ctx); !errors.Is(err, failure) {
				return groupOutput{}, fmt.Errorf("group error = %v", err)
			}
			return groupOutput{Value: 1}, nil
		}))
		defer world.Close()
		ctx := workflow.NewContext(t.Context(), workflow.NewClient(world))
		run, err := workflow.Start(ctx, groupInput{})
		if err != nil {
			t.Fatal(err)
		}
		out, err := run.Value(ctx)
		if err != nil || out.Value != 1 {
			t.Fatalf("result = %+v, %v", out, err)
		}
	})
}

func TestSettledGroupsIgnoreNewWorkWithoutCreatingSteps(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		failure := errors.New("failed group")
		world := testworld.New(workflow.Use(func(ctx context.Context, _ groupInput) (groupOutput, error) {
			var wait workflow.WaitGroup
			var group workflow.ErrGroup
			if err := wait.Wait(ctx); err != nil {
				return groupOutput{}, err
			}
			group.Add(workflow.NewResult(0, failure))
			if err := group.Wait(ctx); !errors.Is(err, failure) {
				return groupOutput{}, fmt.Errorf("group error = %v", err)
			}
			wait.Add(workflow.NewResult(1, errors.New("late failure")))
			group.Add(workflow.NewResult(1, errors.New("late failure")))
			if err := wait.Wait(ctx); err != nil {
				return groupOutput{}, err
			}
			if err := group.Wait(ctx); !errors.Is(err, failure) {
				return groupOutput{}, fmt.Errorf("settled group error changed: %v", err)
			}
			first := workflow.Go(ctx, &wait, increment, 1)
			second := workflow.Go(ctx, &group, increment, 1)
			if first != nil || second != nil {
				return groupOutput{}, errors.New("completed group returned a new result")
			}
			if err := wait.Wait(ctx); err != nil {
				return groupOutput{}, err
			}
			if err := group.Wait(ctx); !errors.Is(err, failure) {
				return groupOutput{}, fmt.Errorf("settled group error changed: %v", err)
			}
			return groupOutput{Value: 1}, nil
		}))
		defer world.Close()
		ctx := workflow.NewContext(t.Context(), workflow.NewClient(world))
		run, err := workflow.Start(ctx, groupInput{})
		if err != nil {
			t.Fatal(err)
		}
		out, err := run.Value(ctx)
		if err != nil || out.Value != 1 {
			t.Fatalf("result = %+v, %v", out, err)
		}
		events, err := world.EventsList(ctx, worlds.EventsListInput{RunID: worlds.RunID(run.ID())})
		if err != nil {
			t.Fatal(err)
		}
		created := 0
		for _, event := range events.Data {
			if event.EventType == worlds.EventTypeStepCreated {
				created++
			}
		}
		if created != 0 {
			t.Fatalf("created %d steps, want none", created)
		}
	})
}

func TestSettledErrGroupKeepsItsOutcomeAfterHookDisposal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		world := testworld.New(workflow.Use(func(ctx context.Context, _ groupInput) (groupOutput, error) {
			group, groupCtx, err := workflow.ErrGroupWithContext(ctx)
			if err != nil {
				return groupOutput{}, err
			}
			hook := workflow.CreateHook[int](ctx)
			defer hook.Dispose(ctx)
			if _, err := hook.GetConflict(ctx); err != nil {
				return groupOutput{}, err
			}
			workflow.Go(groupCtx, group, func(ctx context.Context, token string) (struct{}, error) {
				return struct{}{}, workflow.ResumeHook(ctx, token, 7)
			}, hook.Token())
			group.Add(hook)
			if err := group.Wait(ctx); err != nil {
				return groupOutput{}, err
			}
			value, err := hook.Next(ctx)
			if err != nil {
				return groupOutput{}, err
			}
			hook.Dispose(ctx)
			if err := group.Wait(ctx); err != nil {
				return groupOutput{}, err
			}
			return groupOutput{Value: value}, nil
		}))
		defer world.Close()
		ctx := workflow.NewContext(t.Context(), workflow.NewClient(world))
		run, err := workflow.Start(ctx, groupInput{})
		if err != nil {
			t.Fatal(err)
		}
		out, err := run.Value(ctx)
		if err != nil || out.Value != 7 {
			t.Fatalf("result = %+v, %v", out, err)
		}
	})
}

func TestGroupsRequireWorkflowToWait(t *testing.T) {
	ctx := context.Background()
	var wait workflow.WaitGroup
	var group workflow.ErrGroup
	if err := wait.Wait(ctx); !errors.Is(err, workflow.ErrNoWorkflow) {
		t.Fatalf("WaitGroup.Wait = %v", err)
	}
	if err := group.Wait(ctx); !errors.Is(err, workflow.ErrNoWorkflow) {
		t.Fatalf("ErrGroup.Wait = %v", err)
	}
	if _, _, err := workflow.ErrGroupWithContext(ctx); !errors.Is(err, workflow.ErrNoWorkflow) {
		t.Fatalf("ErrGroupWithContext = %v", err)
	}
}
