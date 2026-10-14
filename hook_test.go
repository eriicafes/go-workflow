package workflow_test

import (
	"context"
	"errors"
	"testing"
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

func TestHookGetConflictRecordsTheOwningRun(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, input hookConflictInput) (hookConflictOutput, error) {
		hook, createErr := workflow.CreateHook[struct{}](ctx, workflow.HookOption{Token: input.Token})
		if createErr != nil && !errors.Is(createErr, workflow.ErrHookConflict) {
			return hookConflictOutput{}, createErr
		}
		owner, conflict, err := hook.GetConflict(ctx)
		return hookConflictOutput{RunID: string(owner), Conflicts: conflict}, err
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	first, err := workflow.Start(ctx, hookConflictInput{Token: "shared-token"})
	if err != nil {
		t.Fatal(err)
	}
	firstValue, err := first.Await(ctx)
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
	secondValue, err := second.Await(ctx)
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
		value, err := hook.Await(ctx)
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
	output, err := run.Await(ctx)
	if err != nil || output.Value != "resumed" {
		t.Fatalf("output = %#v, %v", output, err)
	}
}

func TestResumeHookWithRetriesRejectsNonpositiveAttempts(t *testing.T) {
	if err := workflow.ResumeHookWithRetries(context.Background(), "missing", "value", 0); !errors.Is(err, workflow.ErrHookNotFound) {
		t.Fatalf("retry error = %v", err)
	}
}
