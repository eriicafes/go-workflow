package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/testworld"
)

type hookConflictInput struct{ Token string }
type hookConflictOutput struct {
	RunID     string
	Conflicts bool
}
type hookResumeInput struct{ Token string }
type hookResumeOutput struct{ Value string }

func (hookConflictInput) Workflow(hookConflictOutput) {}
func (hookResumeInput) Workflow(hookResumeOutput)     {}

func TestHookGetConflictClaimsWithoutWaitingForPayload(t *testing.T) {
	runner := workflow.Use(func(ctx context.Context, input hookResumeInput) (hookResumeOutput, error) {
		hook, err := workflow.CreateHook[string](ctx, workflow.HookOption{Token: input.Token})
		if err != nil {
			return hookResumeOutput{}, err
		}
		defer hook.Dispose(ctx)
		if hook.ID() != "" || hook.Token() != input.Token {
			return hookResumeOutput{}, errors.New("unclaimed hook identity changed")
		}
		if _, err := workflow.Now(ctx); err != nil {
			return hookResumeOutput{}, err
		}
		if hook.ID() != "" {
			return hookResumeOutput{}, errors.New("another operation registered the hook")
		}
		for range 2 {
			if owner, err := hook.GetConflict(ctx); err != nil {
				return hookResumeOutput{}, err
			} else if owner != nil {
				return hookResumeOutput{}, workflow.ErrHookConflict
			}
		}
		if hook.ID() == "" {
			return hookResumeOutput{}, errors.New("GetConflict did not register the hook")
		}
		return hookResumeOutput{Value: hook.Token()}, nil
	})
	world := testworld.NewManual()
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, hookResumeInput{Token: "claim-on-conflict"})
	if err != nil {
		t.Fatal(err)
	}
	if err := worlds.NewRuntime(world, worlds.Runners(runner)).Execute(ctx, world.QueuedMessages()[0].Input); err != nil {
		t.Fatal(err)
	}
	output, err := run.Value(ctx)
	if err != nil || output.Value != "claim-on-conflict" {
		t.Fatalf("output=%#v err=%v", output, err)
	}
	events, err := world.EventsList(ctx, worlds.EventsListInput{RunID: worlds.RunID(run.ID())})
	if err != nil {
		t.Fatal(err)
	}
	claims := 0
	for _, event := range events.Data {
		if event.EventType == worlds.EventTypeHookCreated {
			claims++
		}
	}
	if claims != 1 {
		t.Fatalf("claims=%d, want one", claims)
	}
}

func TestHookCombinatorsRegisterBeforeWaitingForPayload(t *testing.T) {
	for _, operation := range []string{"Next", "Values", "Select", "SelectOk", "Join", "JoinOk"} {
		t.Run(operation, func(t *testing.T) {
			runner := workflow.Use(func(ctx context.Context, input hookResumeInput) (hookResumeOutput, error) {
				hook, err := workflow.CreateHook[string](ctx, workflow.HookOption{Token: input.Token})
				if err != nil {
					return hookResumeOutput{}, err
				}
				defer hook.Dispose(ctx)
				switch operation {
				case "Values":
					for value, err := range hook.Values(ctx) {
						return hookResumeOutput{Value: value}, err
					}
				case "Select", "SelectOk":
					var winner interface{}
					if operation == "Select" {
						winner, err = workflow.Select(ctx, hook)
					} else {
						winner, err = workflow.SelectOk(ctx, hook)
					}
					if err == nil && winner != hook {
						err = errors.New("selection lost the original hook")
					}
				case "Join":
					err = workflow.Join(ctx, hook)
				case "JoinOk":
					err = workflow.JoinOk(ctx, hook)
				}
				if err != nil {
					return hookResumeOutput{}, err
				}
				value, err := hook.Next(ctx)
				return hookResumeOutput{Value: value}, err
			})
			world := testworld.NewManual()
			defer world.Close()
			ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
			defer cancel()
			run, err := workflow.Start(ctx, hookResumeInput{Token: "wait-" + operation})
			if err != nil {
				t.Fatal(err)
			}
			runtime := worlds.NewRuntime(world, worlds.Runners(runner))
			delivery := world.QueuedMessages()[0].Input
			if err := runtime.Execute(ctx, delivery); err != nil {
				t.Fatal(err)
			}
			if _, found, err := workflow.GetHook[string](ctx, "wait-"+operation); err != nil || !found {
				t.Fatalf("registration found=%t err=%v", found, err)
			}
			if err := workflow.ResumeHook(ctx, "wait-"+operation, "payload"); err != nil {
				t.Fatal(err)
			}
			if err := runtime.Execute(ctx, delivery); err != nil {
				t.Fatal(err)
			}
			output, err := run.Value(ctx)
			if err != nil || output.Value != "payload" {
				t.Fatalf("output=%#v err=%v", output, err)
			}
		})
	}
}

func TestHookRegistersAtAnotherPromiseWaitAndKeepsTokenAfterInterruptedCommit(t *testing.T) {
	runner := workflow.Use(func(ctx context.Context, _ hookResumeInput) (hookResumeOutput, error) {
		hook, err := workflow.CreateHook[string](ctx)
		if err != nil {
			return hookResumeOutput{}, err
		}
		defer hook.Dispose(ctx)
		if len(hook.Token()) != 21 || hook.ID() != "" {
			return hookResumeOutput{}, errors.New("generated identity unavailable before registration")
		}
		token, err := workflow.Step(ctx, func(_ context.Context, token string) (string, error) {
			return token, nil
		}, hook.Token())
		if err != nil {
			return hookResumeOutput{}, err
		}
		if token != hook.Token() {
			return hookResumeOutput{}, errors.New("token changed after the step captured it")
		}
		value, err := hook.Next(ctx)
		return hookResumeOutput{Value: value}, err
	})
	base := testworld.NewManual()
	defer base.Close()
	commitErr := errors.New("registration unavailable")
	world := &flakyEventWorld{World: base, eventType: worlds.EventTypeHookCreated, err: commitErr}
	world.failures.Store(1)
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, hookResumeInput{})
	if err != nil {
		t.Fatal(err)
	}
	runtime := worlds.NewRuntime(world, worlds.Runners(runner))
	delivery := base.QueuedMessages()[0].Input
	if err := runtime.Execute(ctx, delivery); !errors.Is(err, commitErr) {
		t.Fatalf("interrupted commit error=%v", err)
	}
	events, err := base.EventsList(ctx, worlds.EventsListInput{RunID: worlds.RunID(run.ID())})
	if err != nil {
		t.Fatal(err)
	}
	var capturedToken string
	var stepID worlds.StepID
	for _, event := range events.Data {
		if event.EventType == worlds.EventTypeHookCreated {
			t.Fatal("hook registered before the failed commit")
		}
		if event.EventType == worlds.EventTypeStepCreated {
			var created worlds.DataStepCreated
			if err := json.Unmarshal(event.Data, &created); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(created.Input, &capturedToken); err != nil {
				t.Fatal(err)
			}
			stepID = worlds.StepID(event.CorrelationID)
		}
	}
	if capturedToken == "" {
		t.Fatal("step was not started alongside pending registration")
	}
	if err := runtime.Execute(ctx, delivery); err != nil {
		t.Fatal(err)
	}
	if _, found, err := workflow.GetHook[string](ctx, capturedToken); err != nil || !found {
		t.Fatalf("captured token registration found=%t err=%v", found, err)
	}
	stepDelivery := delivery
	stepDelivery.StepID = &stepID
	if err := runtime.Execute(ctx, stepDelivery); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Execute(ctx, delivery); err != nil {
		t.Fatal(err)
	}
	if err := workflow.ResumeHook(ctx, capturedToken, "stable"); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Execute(ctx, delivery); err != nil {
		t.Fatal(err)
	}
	output, err := run.Value(ctx)
	if err != nil || output.Value != "stable" {
		t.Fatalf("output=%#v err=%v", output, err)
	}
}

func TestHookGetConflictPreventsWorkBeforeOwnership(t *testing.T) {
	for _, checkFirst := range []bool{false, true} {
		name := "wait-after-step"
		if checkFirst {
			name = "claim-before-step"
		}
		t.Run(name, func(t *testing.T) {
			ownerRunner := workflow.Use(func(ctx context.Context, input hookConflictInput) (hookConflictOutput, error) {
				hook, err := workflow.CreateHook[string](ctx, workflow.HookOption{Token: input.Token, MinRetention: time.Hour})
				if err != nil {
					return hookConflictOutput{}, err
				}
				_, err = hook.GetConflict(ctx)
				return hookConflictOutput{}, err
			})
			duplicateRunner := workflow.Use(func(ctx context.Context, input hookResumeInput) (hookResumeOutput, error) {
				hook, err := workflow.CreateHook[string](ctx, workflow.HookOption{Token: input.Token})
				if err != nil {
					return hookResumeOutput{}, err
				}
				defer hook.Dispose(ctx)
				if checkFirst {
					owner, err := hook.GetConflict(ctx)
					if err != nil {
						return hookResumeOutput{}, err
					}
					if owner != nil {
						return hookResumeOutput{Value: owner.ID()}, nil
					}
				}
				_, err = workflow.Step(ctx, func(context.Context, struct{}) (struct{}, error) {
					return struct{}{}, nil
				}, struct{}{})
				if err != nil {
					return hookResumeOutput{}, err
				}
				_, err = hook.Next(ctx)
				return hookResumeOutput{}, err
			})
			world := testworld.New(ownerRunner, duplicateRunner)
			defer world.Close()
			ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
			defer cancel()
			owner, err := workflow.Start(ctx, hookConflictInput{Token: "exclusive"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := owner.Value(ctx); err != nil {
				t.Fatal(err)
			}
			duplicate, err := workflow.Start(ctx, hookResumeInput{Token: "exclusive"})
			if err != nil {
				t.Fatal(err)
			}
			output, err := duplicate.Value(ctx)
			if checkFirst {
				if err != nil || output.Value != owner.ID() {
					t.Fatalf("owner output=%#v err=%v", output, err)
				}
			} else if !errors.Is(err, workflow.ErrHookConflict) {
				t.Fatalf("payload wait error=%v", err)
			}
			events, err := world.EventsList(ctx, worlds.EventsListInput{RunID: worlds.RunID(duplicate.ID())})
			if err != nil {
				t.Fatal(err)
			}
			completed := 0
			for _, event := range events.Data {
				if event.EventType == worlds.EventTypeStepCompleted {
					completed++
				}
			}
			want := 1
			if checkFirst {
				want = 0
			}
			if completed != want {
				t.Fatalf("completed steps=%d want=%d", completed, want)
			}
		})
	}
}

func TestSelectOrdersHookConflictAfterAnEarlierStepSettlement(t *testing.T) {
	ownerRunner := workflow.Use(func(ctx context.Context, input hookConflictInput) (hookConflictOutput, error) {
		hook, err := workflow.CreateHook[string](ctx, workflow.HookOption{Token: input.Token, MinRetention: time.Hour})
		if err != nil {
			return hookConflictOutput{}, err
		}
		_, err = hook.GetConflict(ctx)
		return hookConflictOutput{}, err
	})
	runner := workflow.Use(func(ctx context.Context, input hookResumeInput) (hookResumeOutput, error) {
		hook, err := workflow.CreateHook[string](ctx, workflow.HookOption{Token: input.Token})
		if err != nil {
			return hookResumeOutput{}, err
		}
		defer hook.Dispose(ctx)
		step := workflow.Background(ctx, func(context.Context, struct{}) (struct{}, error) {
			return struct{}{}, nil
		}, struct{}{})
		winner, err := workflow.Select(ctx, hook, step)
		if err != nil {
			return hookResumeOutput{}, err
		}
		if winner != step {
			return hookResumeOutput{}, errors.New("conflict overtook an earlier step settlement")
		}
		return hookResumeOutput{Value: "step"}, nil
	})
	base := testworld.NewManual()
	defer base.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(base)), time.Second)
	defer cancel()
	if _, err := workflow.Start(ctx, hookConflictInput{Token: "ordered-conflict"}); err != nil {
		t.Fatal(err)
	}
	ownerDelivery := base.QueuedMessages()[0].Input
	if err := worlds.NewRuntime(base, worlds.Runners(ownerRunner)).Execute(ctx, ownerDelivery); err != nil {
		t.Fatal(err)
	}
	run, err := workflow.Start(ctx, hookResumeInput{Token: "ordered-conflict"})
	if err != nil {
		t.Fatal(err)
	}
	commitErr := errors.New("registration unavailable")
	world := &flakyEventWorld{World: base, eventType: worlds.EventTypeHookCreated, err: commitErr}
	world.failures.Store(1)
	runtime := worlds.NewRuntime(world, worlds.Runners(runner))
	delivery := base.QueuedMessages()[1].Input
	if err := runtime.Execute(ctx, delivery); !errors.Is(err, commitErr) {
		t.Fatalf("interrupted commit error=%v", err)
	}
	// The started step can finish before the retry commits the conflicting claim.
	if err := runtime.Execute(ctx, base.QueuedMessages()[2].Input); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Execute(ctx, delivery); err != nil {
		t.Fatal(err)
	}
	output, err := run.Value(ctx)
	if err != nil || output.Value != "step" {
		t.Fatalf("output=%#v err=%v", output, err)
	}
}

func TestUnregisteredHookDisposeDoesNotClaimToken(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, input hookResumeInput) (hookResumeOutput, error) {
		hook, err := workflow.CreateHook[string](ctx, workflow.HookOption{Token: input.Token})
		if err != nil {
			return hookResumeOutput{}, err
		}
		defer hook.Dispose(ctx)
		return hookResumeOutput{Value: hook.Token()}, nil
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, hookResumeInput{Token: "unused-token"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Value(ctx); err != nil {
		t.Fatal(err)
	}
	events, err := world.EventsList(ctx, worlds.EventsListInput{RunID: worlds.RunID(run.ID())})
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events.Data {
		if event.EventType == worlds.EventTypeHookCreated || event.EventType == worlds.EventTypeHookDisposed {
			t.Fatalf("unregistered hook wrote %s", event.EventType)
		}
	}
}

func TestHookGetConflictRecordsTheOwningRun(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, input hookConflictInput) (hookConflictOutput, error) {
		hook, createErr := workflow.CreateHook[struct{}](ctx, workflow.HookOption{Token: input.Token, MinRetention: time.Hour})
		if createErr != nil && !errors.Is(createErr, workflow.ErrHookConflict) {
			return hookConflictOutput{}, createErr
		}
		owner, err := hook.GetConflict(ctx)
		if owner == nil {
			return hookConflictOutput{}, err
		}
		return hookConflictOutput{RunID: owner.ID(), Conflicts: true}, err
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	first, err := workflow.Start(ctx, hookConflictInput{Token: "shared-token"})
	if err != nil {
		t.Fatal(err)
	}
	firstValue, err := first.Value(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if firstValue.Conflicts {
		t.Fatalf("first hook conflict = %#v", firstValue)
	}
	second, err := workflow.Start(ctx, hookConflictInput{Token: "shared-token"})
	if err != nil {
		t.Fatal(err)
	}
	secondValue, err := second.Value(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !secondValue.Conflicts || secondValue.RunID != first.ID() {
		t.Fatalf("second hook conflict = %#v, first = %q", secondValue, first.ID())
	}
	events, err := world.EventsList(ctx, worlds.EventsListInput{RunID: worlds.RunID(second.ID())})
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events.Data {
		if event.EventType == worlds.EventTypeHookConflict {
			return
		}
	}
	t.Fatal("missing hook_conflict event")
}

func TestGetHookAndResumeHookDeliverTheTypedValue(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, input hookResumeInput) (hookResumeOutput, error) {
		hook, err := workflow.CreateHook[string](ctx, workflow.HookOption{Token: input.Token})
		if err != nil {
			return hookResumeOutput{}, err
		}
		value, err := hook.Next(ctx)
		return hookResumeOutput{Value: value}, err
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, hookResumeInput{Token: "resume-token"})
	if err != nil {
		t.Fatal(err)
	}
	var stored worlds.Hook
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		page, err := world.HooksList(ctx, worlds.HooksListInput{})
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Data) != 0 {
			stored = page.Data[0]
			break
		}
	}
	if stored.Token == "" {
		t.Fatal("hook was not registered")
	}
	hook, found, err := workflow.GetHook[string](ctx, stored.Token)
	if err != nil || !found || hook.ID() != string(stored.HookID) || hook.Token() != stored.Token {
		t.Fatalf("GetHook = %#v, found=%t, err=%v", hook, found, err)
	}
	if _, found, err := workflow.GetHook[string](ctx, "missing-token"); err != nil || found {
		t.Fatalf("missing GetHook found=%t, err=%v", found, err)
	}
	if err := workflow.ResumeHook(ctx, stored.Token, "resumed"); err != nil {
		t.Fatal(err)
	}
	output, err := run.Value(ctx)
	if err != nil || output.Value != "resumed" {
		t.Fatalf("output = %#v, %v", output, err)
	}
}

func TestHookLookupAndResumeRequireStepOrExternalContext(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, input hookResumeInput) (hookResumeOutput, error) {
		hook, err := workflow.CreateHook[string](ctx, workflow.HookOption{Token: input.Token})
		if err != nil {
			return hookResumeOutput{}, err
		}
		defer hook.Dispose(ctx)
		if _, err := hook.GetConflict(ctx); err != nil {
			return hookResumeOutput{}, err
		}
		if _, _, err := workflow.GetHook[string](ctx, input.Token); !errors.Is(err, workflow.ErrNotInStep) {
			return hookResumeOutput{}, errors.New("workflow lookup was accepted")
		}
		if err := workflow.ResumeHook(ctx, input.Token, "workflow"); !errors.Is(err, workflow.ErrNotInStep) {
			return hookResumeOutput{}, errors.New("workflow resume was accepted")
		}
		for _, attempts := range []int{0, 1} {
			if err := workflow.ResumeHookWithRetries(ctx, input.Token, "workflow", attempts, time.Second); !errors.Is(err, workflow.ErrNotInStep) {
				return hookResumeOutput{}, errors.New("workflow retrying resume was accepted")
			}
		}
		if _, err := workflow.Step(ctx, func(ctx context.Context, token string) (struct{}, error) {
			lookup, found, err := workflow.GetHook[string](ctx, token)
			if err != nil {
				return struct{}{}, err
			}
			if !found || lookup.Token() != token {
				return struct{}{}, errors.New("step lookup did not find the hook")
			}
			if _, err := hook.Next(ctx); !errors.Is(err, workflow.ErrNoWorkflow) {
				return struct{}{}, errors.New("step received a workflow hook")
			}
			if _, err := hook.GetConflict(ctx); !errors.Is(err, workflow.ErrNoWorkflow) {
				return struct{}{}, errors.New("step checked hook ownership")
			}
			if err := hook.Dispose(ctx); !errors.Is(err, workflow.ErrNoWorkflow) {
				return struct{}{}, errors.New("step disposed a workflow hook")
			}
			return struct{}{}, workflow.ResumeHookWithRetries(ctx, token, "step", 1, time.Second)
		}, input.Token); err != nil {
			return hookResumeOutput{}, err
		}
		value, err := hook.Next(ctx)
		return hookResumeOutput{Value: value}, err
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, hookResumeInput{Token: "context-guard"})
	if err != nil {
		t.Fatal(err)
	}
	output, err := run.Value(ctx)
	if err != nil || output.Value != "step" {
		t.Fatalf("output=%#v err=%v", output, err)
	}
}

func TestResumeHookWithRetriesUsesFixedIntervalsAndHonorsCancellation(t *testing.T) {
	for _, cancelDuringWait := range []bool{false, true} {
		name := "exhausted"
		if cancelDuringWait {
			name = "cancelled"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				world := testworld.NewManual()
				defer world.Close()
				ctx := workflow.NewContext(context.Background(), workflow.NewClient(world))
				wantErr, wantElapsed := workflow.ErrHookNotFound, 30*time.Second
				if cancelDuringWait {
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, 15*time.Second)
					defer cancel()
					wantErr, wantElapsed = context.DeadlineExceeded, 15*time.Second
				}
				start := time.Now()
				err := workflow.ResumeHookWithRetries(ctx, "missing", "value", 4, 10*time.Second)
				if !errors.Is(err, wantErr) || time.Since(start) != wantElapsed {
					t.Fatalf("error=%v elapsed=%v, want %v after %v", err, time.Since(start), wantErr, wantElapsed)
				}
			})
		})
	}
}

func TestResumeHookWithRetriesRejectsNonpositiveAttempts(t *testing.T) {
	if err := workflow.ResumeHookWithRetries(context.Background(), "missing", "value", 0, time.Second); !errors.Is(err, workflow.ErrHookNotFound) {
		t.Fatalf("retry error = %v", err)
	}
}

func TestHookValuesBreakAndNextShareTheReplayCursor(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, input hookResumeInput) (hookResumeOutput, error) {
		hook, err := workflow.CreateHook[string](ctx, workflow.HookOption{Token: input.Token})
		if err != nil {
			return hookResumeOutput{}, err
		}
		defer hook.Dispose(ctx)
		var first string
		for value, err := range hook.Values(ctx) {
			if err != nil {
				return hookResumeOutput{}, err
			}
			first = value
			break
		}
		second, err := hook.Next(ctx)
		return hookResumeOutput{Value: first + second}, err
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), 2*time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, hookResumeInput{Token: "two-values"})
	if err != nil {
		t.Fatal(err)
	}
	if err := workflow.ResumeHookWithRetries(ctx, "two-values", "first", 5, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := workflow.ResumeHook(ctx, "two-values", "second"); err != nil {
		t.Fatal(err)
	}
	output, err := run.Value(ctx)
	if err != nil || output.Value != "firstsecond" {
		t.Fatalf("output=%#v err=%v", output, err)
	}
	if _, found, err := workflow.GetHook[string](ctx, "two-values"); err != nil || found {
		t.Fatalf("disposed hook remains available: %t %v", found, err)
	}
	events, err := world.EventsList(ctx, worlds.EventsListInput{RunID: worlds.RunID(run.ID())})
	if err != nil {
		t.Fatal(err)
	}
	disposals := 0
	for _, event := range events.Data {
		if event.EventType == worlds.EventTypeHookDisposed {
			disposals++
		}
	}
	if disposals != 1 {
		t.Fatalf("disposals=%d", disposals)
	}
}
