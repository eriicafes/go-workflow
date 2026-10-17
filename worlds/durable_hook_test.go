package worlds_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/testworld"
)

type durableHookInput struct{ Token string }
type durableHookOutput struct {
	Value     string
	Cancelled bool
}
type hookConflictInput struct{ Token string }
type hookConflictOutput struct {
	Owner    string
	Conflict bool
}
type hookForceInput struct {
	Token string
	Force bool
}
type hookForceOutput struct{ ForceClaimed bool }
type hookRetentionInput struct {
	Token     string
	Retention time.Duration
}
type hookRetentionOutput struct{ Value string }

func (durableHookInput) Workflow(durableHookOutput)     {}
func (hookConflictInput) Workflow(hookConflictOutput)   {}
func (hookForceInput) Workflow(hookForceOutput)         {}
func (hookRetentionInput) Workflow(hookRetentionOutput) {}

func TestSameRunTakeoverPreservesPayloadShapeAndExplicitDisposalStopsBufferedValues(t *testing.T) {
	base := testworld.NewManual()
	defer base.Close()
	runner := workflow.Use(func(ctx context.Context, input durableHookInput) (durableHookOutput, error) {
		old, err := workflow.CreateHook[map[string]any](ctx, workflow.HookOption{Token: input.Token})
		if err != nil {
			return durableHookOutput{}, err
		}
		if owner, err := old.GetConflict(ctx); err != nil {
			return durableHookOutput{}, err
		} else if owner != nil {
			return durableHookOutput{}, workflow.ErrHookConflict
		}
		oldID := old.ID()
		current, err := workflow.CreateHook[map[string]any](ctx, workflow.HookOption{Token: input.Token, Force: true})
		if err != nil {
			return durableHookOutput{}, err
		}
		if owner, err := current.GetConflict(ctx); err != nil {
			return durableHookOutput{}, err
		} else if owner != nil {
			return durableHookOutput{}, workflow.ErrHookConflict
		}
		current.ID()
		defer current.Dispose(ctx)
		if current.ClaimedFrom == nil || current.ClaimedFrom.HookID != worlds.HookID(oldID) {
			return durableHookOutput{}, errors.New("missing takeover metadata")
		}
		if _, err := old.Next(ctx); !errors.Is(err, workflow.ErrHookForceClaimed) {
			return durableHookOutput{}, errors.New("old hook did not observe takeover")
		}
		payload, err := current.Next(ctx)
		if err != nil {
			return durableHookOutput{}, err
		}
		if err := current.Dispose(ctx); err != nil {
			return durableHookOutput{}, err
		}
		if _, err := current.Next(ctx); !errors.Is(err, workflow.ErrHookNotActive) {
			return durableHookOutput{}, errors.New("disposed hook yielded buffered data")
		}
		return durableHookOutput{Value: payload["response"].(map[string]any)["message"].(string)}, nil
	})
	runtime := worlds.NewRuntime(base, worlds.Runners(runner))
	ctx, stop := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(base)), 3*time.Second)
	defer stop()
	run, err := workflow.Start(ctx, durableHookInput{Token: "same-run-token"})
	if err != nil {
		t.Fatal(err)
	}
	deliverNext(t, ctx, base, runtime)
	stored, err := base.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: "same-run-token"})
	if err != nil {
		t.Fatal(err)
	}
	lookup, found, err := workflow.GetHook[map[string]any](ctx, stored.Token)
	if err != nil || !found || lookup.ClaimedFrom == nil || lookup.ClaimedFrom.RunID != worlds.RunID(run.ID()) {
		t.Fatalf("lookup=%#v found=%t err=%v", lookup, found, err)
	}
	for _, message := range []string{"kept", "discarded"} {
		if err := workflow.ResumeHook(ctx, "same-run-token", map[string]any{"response": map[string]any{"message": message}}); err != nil {
			t.Fatal(err)
		}
	}
	drain(t, ctx, base, runtime)
	output, err := run.Value(ctx)
	if err != nil || output.Value != "kept" {
		t.Fatalf("output=%#v err=%v", output, err)
	}
}

func TestDeferredDisposalSurvivesSuspensionAndRunsOnApplicationPanic(t *testing.T) {
	for _, wrapped := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct defer", true: "wrapped defer"}[wrapped], func(t *testing.T) {
			world := testworld.New(workflow.Use(func(ctx context.Context, input durableHookInput) (durableHookOutput, error) {
				hook, err := workflow.CreateHook[string](ctx, workflow.HookOption{Token: input.Token, MinRetention: time.Hour})
				if err != nil {
					return durableHookOutput{}, err
				}
				if owner, err := hook.GetConflict(ctx); err != nil {
					return durableHookOutput{}, err
				} else if owner != nil {
					return durableHookOutput{}, workflow.ErrHookConflict
				}
				if wrapped {
					defer func() { _ = hook.Dispose(ctx) }()
				} else {
					defer hook.Dispose(ctx)
				}
				value, err := hook.Next(ctx)
				if err != nil {
					return durableHookOutput{}, err
				}
				if _, err := workflow.Step(ctx, func(_ context.Context, value string) (string, error) { return value, nil }, value); err != nil {
					return durableHookOutput{}, err
				}
				panic(errors.New("application failure"))
			}))
			defer world.Close()
			ctx, stop := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), 3*time.Second)
			defer stop()
			run, err := workflow.Start(ctx, durableHookInput{Token: "deferred-disposal"})
			if err != nil {
				t.Fatal(err)
			}
			if err := workflow.ResumeHookWithRetries(ctx, "deferred-disposal", "received", 5, 100*time.Millisecond); err != nil {
				t.Fatal(err)
			}
			if _, err := run.Value(ctx); err == nil || err.Error() != "application failure" {
				t.Fatalf("run error=%v", err)
			}
			if _, found, err := workflow.GetHook[string](ctx, "deferred-disposal"); err != nil || found {
				t.Fatalf("panic retained disposed token: found=%t err=%v", found, err)
			}
			page, err := world.EventsList(ctx, worlds.EventsListInput{RunID: worlds.RunID(run.ID())})
			if err != nil {
				t.Fatal(err)
			}
			disposed := 0
			for _, event := range page.Data {
				if event.EventType == worlds.EventTypeHookDisposed {
					disposed++
				}
			}
			if disposed != 1 {
				t.Fatalf("disposals=%d", disposed)
			}
		})
	}
}

func TestDurableHookResumesAndRecordsDelivery(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, input durableHookInput) (durableHookOutput, error) {
		hook, err := workflow.CreateHook[string](ctx, workflow.HookOption{Token: input.Token})
		if err != nil {
			return durableHookOutput{}, err
		}
		if owner, err := hook.GetConflict(ctx); err != nil {
			return durableHookOutput{}, err
		} else if owner != nil {
			return durableHookOutput{}, workflow.ErrHookConflict
		}
		value, err := hook.Next(ctx)
		return durableHookOutput{Value: value}, err
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, durableHookInput{Token: "durable-hook"})
	if err != nil {
		t.Fatal(err)
	}
	var hook worlds.Hook
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		page, err := world.HooksList(ctx, worlds.HooksListInput{})
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Data) != 0 {
			hook = page.Data[0]
			break
		}
	}
	if hook.HookID == "" {
		t.Fatal("hook was not created")
	}
	if err := workflow.ResumeHook(ctx, hook.Token, "received"); err != nil {
		t.Fatal(err)
	}
	output, err := run.Value(ctx)
	if err != nil || output.Value != "received" {
		t.Fatalf("output = %#v, %v", output, err)
	}
	events, err := world.EventsList(ctx, worlds.EventsListInput{RunID: worlds.RunID(run.ID())})
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events.Data {
		if event.EventType == worlds.EventTypeHookReceived && event.CorrelationID == string(hook.HookID) {
			return
		}
	}
	t.Fatal("missing hook_received event")
}

func TestDurableHookCancellationIsReturnedToTheWorkflow(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, _ durableHookInput) (durableHookOutput, error) {
		hook, err := workflow.CreateHook[string](ctx)
		if err != nil {
			return durableHookOutput{}, err
		}
		if owner, err := hook.GetConflict(ctx); err != nil {
			return durableHookOutput{}, err
		} else if owner != nil {
			return durableHookOutput{}, workflow.ErrHookConflict
		}
		if err := hook.Dispose(ctx); err != nil {
			return durableHookOutput{}, err
		}
		_, err = hook.Next(ctx)
		return durableHookOutput{Cancelled: errors.Is(err, worlds.ErrHookNotActive)}, nil
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, durableHookInput{})
	if err != nil {
		t.Fatal(err)
	}
	output, err := run.Value(ctx)
	if err != nil || !output.Cancelled {
		t.Fatalf("cancelled hook output = %#v, %v", output, err)
	}
}

func TestDurableHookConflictRecordsTheOwningRun(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, input hookConflictInput) (hookConflictOutput, error) {
		hook, createErr := workflow.CreateHook[struct{}](ctx, workflow.HookOption{Token: input.Token, MinRetention: time.Hour})
		if createErr != nil && !errors.Is(createErr, workflow.ErrHookConflict) {
			return hookConflictOutput{}, createErr
		}
		owner, err := hook.GetConflict(ctx)
		if owner == nil {
			return hookConflictOutput{}, err
		}
		return hookConflictOutput{Owner: owner.ID(), Conflict: true}, err
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	first, err := workflow.Start(ctx, hookConflictInput{Token: "shared-token"})
	if err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		page, err := world.HooksList(ctx, worlds.HooksListInput{})
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Data) != 0 {
			break
		}
	}
	second, err := workflow.Start(ctx, hookConflictInput{Token: "shared-token"})
	if err != nil {
		t.Fatal(err)
	}
	output, err := second.Value(ctx)
	if err != nil || !output.Conflict || output.Owner != first.ID() {
		t.Fatalf("conflict output = %#v, %v", output, err)
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

func TestDurableHookForceClaimWakesTheDisplacedRun(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, input hookForceInput) (hookForceOutput, error) {
		hook, err := workflow.CreateHook[string](ctx, workflow.HookOption{Token: input.Token, Force: input.Force})
		if err != nil && !errors.Is(err, worlds.ErrHookForceClaimed) {
			return hookForceOutput{}, err
		}
		if owner, err := hook.GetConflict(ctx); err != nil {
			return hookForceOutput{}, err
		} else if owner != nil {
			return hookForceOutput{}, workflow.ErrHookConflict
		}
		_, err = hook.Next(ctx)
		return hookForceOutput{ForceClaimed: errors.Is(err, worlds.ErrHookForceClaimed)}, nil
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	first, err := workflow.Start(ctx, hookForceInput{Token: "force-token"})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		if _, err := world.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: "force-token"}); err == nil {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("first hook was not registered")
	}
	second, err := workflow.Start(ctx, hookForceInput{Token: "force-token", Force: true})
	if err != nil {
		t.Fatal(err)
	}
	firstOutput, err := first.Value(ctx)
	if err != nil || !firstOutput.ForceClaimed {
		t.Fatalf("displaced output = %#v, %v", firstOutput, err)
	}
	hook, err := world.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: "force-token"})
	if err != nil || hook.RunID != worlds.RunID(second.ID()) {
		t.Fatalf("new owner = %#v, %v", hook, err)
	}
	events, err := world.EventsList(ctx, worlds.EventsListInput{RunID: worlds.RunID(first.ID())})
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events.Data {
		var disposed worlds.HookDisposedEventData
		if event.EventType == worlds.EventTypeHookDisposed && json.Unmarshal(event.Data, &disposed) == nil && disposed.ForceClaimedBy != nil {
			return
		}
	}
	t.Fatal("missing forced hook_disposed event")
}

func TestDurableHookRetentionKeepsAndThenReleasesTheToken(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, input hookRetentionInput) (hookRetentionOutput, error) {
		hook, err := workflow.CreateHook[string](ctx, workflow.HookOption{Token: input.Token, MinRetention: input.Retention})
		if err != nil {
			return hookRetentionOutput{}, err
		}
		if owner, err := hook.GetConflict(ctx); err != nil {
			return hookRetentionOutput{}, err
		} else if owner != nil {
			return hookRetentionOutput{}, workflow.ErrHookConflict
		}
		value, err := hook.Next(ctx)
		return hookRetentionOutput{Value: value}, err
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, hookRetentionInput{Token: "retained-token", Retention: 35 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	resumed := false
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		if err := workflow.ResumeHook(ctx, "retained-token", "done"); err == nil {
			resumed = true
			break
		}
	}
	if !resumed {
		t.Fatal("retained hook was not registered")
	}
	if _, err := run.Value(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := world.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: "retained-token"}); err != nil {
		t.Fatalf("retained token = %v", err)
	}
	time.Sleep(45 * time.Millisecond)
	if _, err := world.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: "retained-token"}); !errors.Is(err, worlds.ErrHookNotFound) {
		t.Fatalf("expired token error = %v", err)
	}
}
