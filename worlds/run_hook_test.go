package worlds_test

import (
	"context"
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

func (durableHookInput) Workflow(durableHookOutput)   {}
func (hookConflictInput) Workflow(hookConflictOutput) {}

func TestDurableHookResumesAndRecordsDelivery(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, input durableHookInput) (durableHookOutput, error) {
		hook, err := workflow.CreateHook[string](ctx, workflow.HookOption{Token: input.Token})
		if err != nil {
			return durableHookOutput{}, err
		}
		value, err := hook.Await(ctx)
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
	output, err := run.Await(ctx)
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
		if err := hook.Cancel(ctx); err != nil {
			return durableHookOutput{}, err
		}
		_, err = hook.Await(ctx)
		return durableHookOutput{Cancelled: errors.Is(err, worlds.ErrHookNotActive)}, nil
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, durableHookInput{})
	if err != nil {
		t.Fatal(err)
	}
	output, err := run.Await(ctx)
	if err != nil || !output.Cancelled {
		t.Fatalf("cancelled hook output = %#v, %v", output, err)
	}
}

func TestDurableHookConflictRecordsTheOwningRun(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, input hookConflictInput) (hookConflictOutput, error) {
		hook, createErr := workflow.CreateHook[struct{}](ctx, workflow.HookOption{Token: input.Token})
		if createErr != nil && !errors.Is(createErr, workflow.ErrHookConflict) {
			return hookConflictOutput{}, createErr
		}
		owner, conflict, err := hook.GetConflict(ctx)
		return hookConflictOutput{Owner: string(owner), Conflict: conflict}, err
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
	output, err := second.Await(ctx)
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
