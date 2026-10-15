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

func TestDurableHookForceClaimWakesTheDisplacedRun(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, input hookForceInput) (hookForceOutput, error) {
		hook, err := workflow.CreateHook[string](ctx, workflow.HookOption{Token: input.Token, Force: input.Force})
		if err != nil && !errors.Is(err, worlds.ErrHookForceClaimed) {
			return hookForceOutput{}, err
		}
		_, err = hook.Await(ctx)
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
	firstOutput, err := first.Await(ctx)
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
		if event.EventType == worlds.EventTypeHookForceClaimed {
			return
		}
	}
	t.Fatal("missing hook_force_claimed event")
}

func TestDurableHookRetentionKeepsAndThenReleasesTheToken(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, input hookRetentionInput) (hookRetentionOutput, error) {
		hook, err := workflow.CreateHook[string](ctx, workflow.HookOption{Token: input.Token, MinRetention: input.Retention})
		if err != nil {
			return hookRetentionOutput{}, err
		}
		value, err := hook.Await(ctx)
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
	if _, err := run.Await(ctx); err != nil {
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
