package workflowtest

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

func conformSteps(t *testing.T, world worlds.World) {
	ctx := context.Background()
	runID := conformCreateRun(t, world, "steps")
	conformAppend(t, world, runID, worlds.EventTypeRunStarted, "", nil)
	created := conformAppend(t, world, runID, worlds.EventTypeStepCreated, "step_a", worlds.DataStepCreated{StepName: "fetch", Input: json.RawMessage(`"step-in"`)})
	if created.Step == nil || !created.StepCreated || created.Step.StepID != "step_a" || created.Step.StepName != "fetch" || created.Step.Status != worlds.StatusPending || string(created.Step.Input) != `"step-in"` {
		t.Fatalf("step_created result = %#v step=%#v", created, created.Step)
	}
	plain := conformAppend(t, world, runID, worlds.EventTypeWaitCreated, "wait", nil)
	if plain.Step != nil || plain.StepCreated {
		t.Fatalf("non-step result has step: %#v", plain)
	}
	_, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeStepStarted, CorrelationID: "step_missing"}})
	conformExpect(t, err, worlds.ErrStepNotStartable, "start missing step")

	started := conformAppend(t, world, runID, worlds.EventTypeStepStarted, "step_a", nil)
	step, err := world.StepsGet(ctx, worlds.StepsGetInput{RunID: runID, StepID: "step_a"})
	if err != nil || step.Status != worlds.StatusRunning || step.Attempt != 1 || step.StartedAt == nil {
		t.Fatalf("running step = %#v err=%v", step, err)
	}
	firstStartedAt := *step.StartedAt
	// Starting a running step records a new attempt, so a delivery can take
	// over a step whose previous attempt's delivery died.
	restarted, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeStepStarted, CorrelationID: "step_a"}})
	if err != nil || restarted.EventID == started.EventID || restarted.AllocatedEventCount != 1 {
		t.Fatalf("restarted step = %#v err=%v", restarted, err)
	}
	step, err = world.StepsGet(ctx, worlds.StepsGetInput{RunID: runID, StepID: "step_a"})
	if err != nil || step.Status != worlds.StatusRunning || step.Attempt != 2 || step.StartedAt == nil || !step.StartedAt.Equal(firstStartedAt) {
		t.Fatalf("restarted step = %#v err=%v", step, err)
	}
	retryAt := time.Now().Add(time.Minute).UTC().Truncate(time.Millisecond)
	conformAppend(t, world, runID, worlds.EventTypeStepRetrying, "step_a", worlds.DataStepResult{Error: json.RawMessage(`{"message":"transient"}`), RetryAfter: &retryAt})
	step, err = world.StepsGet(ctx, worlds.StepsGetInput{RunID: runID, StepID: "step_a"})
	if err != nil || step.Status != worlds.StatusPending || step.RetryAfter == nil || !step.RetryAfter.Equal(retryAt) || string(step.Error) != `{"message":"transient"}` {
		t.Fatalf("retrying step = %#v err=%v", step, err)
	}
	conformAppend(t, world, runID, worlds.EventTypeStepStarted, "step_a", nil)
	conformAppend(t, world, runID, worlds.EventTypeStepCompleted, "step_a", worlds.DataStepResult{Output: json.RawMessage(`"step-out"`)})
	step, err = world.StepsGet(ctx, worlds.StepsGetInput{RunID: runID, StepID: "step_a"})
	if err != nil || step.Status != worlds.StatusCompleted || step.Attempt != 3 || string(step.Output) != `"step-out"` || step.CompletedAt == nil {
		t.Fatalf("completed step = %#v err=%v", step, err)
	}
	stripped, err := world.StepsGet(ctx, worlds.StepsGetInput{RunID: runID, StepID: "step_a", ResolveData: worlds.ResolveDataNone})
	if err != nil || stripped.Input != nil || stripped.Output != nil {
		t.Fatalf("ResolveDataNone step = %#v err=%v", stripped, err)
	}
	_, err = world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeStepStarted, CorrelationID: "step_a"}})
	conformExpect(t, err, worlds.ErrStepNotStartable, "start completed step")
	_, err = world.StepsGet(ctx, worlds.StepsGetInput{RunID: runID, StepID: "step_missing"})
	conformExpect(t, err, worlds.ErrStepNotFound, "missing step")

	time.Sleep(2 * time.Millisecond)
	conformAppend(t, world, runID, worlds.EventTypeStepCreated, "step_b", worlds.DataStepCreated{StepName: "second"})
	cause := errors.New("fatal")
	encoded, err := json.Marshal(worlds.NewError(worlds.FatalError{Cause: cause}))
	if err != nil {
		t.Fatal(err)
	}
	conformAppend(t, world, runID, worlds.EventTypeStepFailed, "step_b", worlds.DataStepResult{Error: encoded})
	step, err = world.StepsGet(ctx, worlds.StepsGetInput{RunID: runID, StepID: "step_b"})
	if err != nil || step.Status != worlds.StatusFailed || string(step.Error) != string(encoded) || step.CompletedAt == nil {
		t.Fatalf("failed step = %#v err=%v", step, err)
	}
	var fatal worlds.FatalError
	decoded := worlds.DecodeError(step.Error)
	if !errors.Is(decoded, cause) || !errors.As(decoded, &fatal) {
		t.Fatalf("stored step error lost its cause or fatal marker: %s", step.Error)
	}
	steps, err := world.StepsList(ctx, worlds.StepsListInput{RunID: runID})
	if err != nil || len(steps.Data) != 2 || steps.Data[0].StepID != "step_a" || steps.Data[1].StepID != "step_b" {
		t.Fatalf("steps = %#v err=%v", steps, err)
	}
	paged, err := world.StepsList(ctx, worlds.StepsListInput{RunID: runID, Pagination: worlds.Pagination{Limit: 1}})
	if err != nil || len(paged.Data) != 1 || !paged.HasMore || paged.Cursor == "" {
		t.Fatalf("first step page = %#v err=%v", paged, err)
	}
	next, err := world.StepsList(ctx, worlds.StepsListInput{RunID: runID, Pagination: worlds.Pagination{Limit: 1, Cursor: paged.Cursor}})
	if err != nil || len(next.Data) != 1 || next.Data[0].StepID != "step_b" || next.HasMore {
		t.Fatalf("second step page = %#v err=%v", next, err)
	}
}

func conformTerminalSteps(t *testing.T, world worlds.World) {
	ctx := context.Background()
	runID := conformCreateRun(t, world, "terminal_steps")
	conformAppend(t, world, runID, worlds.EventTypeStepCreated, "step_a", worlds.DataStepCreated{StepName: "a"})
	conformAppend(t, world, runID, worlds.EventTypeStepStarted, "step_a", nil)
	conformAppend(t, world, runID, worlds.EventTypeStepCreated, "step_b", worlds.DataStepCreated{StepName: "b"})
	conformAppend(t, world, runID, worlds.EventTypeRunCancelled, "", nil)
	conformAppend(t, world, runID, worlds.EventTypeStepCompleted, "step_a", nil)
	step, err := world.StepsGet(ctx, worlds.StepsGetInput{RunID: runID, StepID: "step_a"})
	if err != nil || step.Status != worlds.StatusCompleted {
		t.Fatalf("late result = %#v, %v", step, err)
	}
	_, err = world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeStepStarted, CorrelationID: "step_b"}})
	conformExpect(t, err, worlds.ErrRunTerminal, "start after terminal run")
}
