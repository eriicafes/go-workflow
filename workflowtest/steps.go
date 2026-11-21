package workflowtest

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
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
	started := conformAppend(t, world, runID, worlds.EventTypeStepStarted, "step_a", worlds.DataStepStarted{StepName: "fetch", Attempt: 1})
	step, err := world.StepsGet(ctx, worlds.StepsGetInput{RunID: runID, StepID: "step_a"})
	if err != nil || step.Status != worlds.StatusRunning || step.Attempt != 1 || step.StartedAt == nil {
		t.Fatalf("running step = %#v err=%v", step, err)
	}
	// The runtime reads an attempt's number and start time back from its
	// step_started.
	startedEvent, err := world.EventsGet(ctx, worlds.EventsGetInput{RunID: runID, EventID: started.EventID})
	var startedData worlds.DataStepStarted
	if err != nil || json.Unmarshal(startedEvent.Data, &startedData) != nil || startedData.Attempt != 1 || startedData.StepName != "fetch" || startedEvent.CreatedAt.IsZero() {
		t.Fatalf("step_started = %#v err=%v", startedEvent, err)
	}
	listed, err := world.EventsList(ctx, worlds.EventsListInput{RunID: runID})
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range listed.Data {
		if event.EventID == started.EventID && event.CreatedAt.IsZero() {
			t.Fatalf("listed step_started has no creation time: %#v", event)
		}
	}
	firstStartedAt := *step.StartedAt
	// Starting a running step records a new attempt, so a delivery can take
	// over a step whose previous attempt's delivery died.
	restarted, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: conformEvent(t, worlds.EventTypeStepStarted, "step_a", worlds.DataStepStarted{StepName: "fetch", Attempt: 2})})
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
	conformAppend(t, world, runID, worlds.EventTypeStepCompleted, "step_a", worlds.DataStepResult{Result: json.RawMessage(`"step-out"`)})
	step, err = world.StepsGet(ctx, worlds.StepsGetInput{RunID: runID, StepID: "step_a"})
	if err != nil || step.Status != worlds.StatusCompleted || step.Attempt != 3 || string(step.Output) != `"step-out"` || step.CompletedAt == nil {
		t.Fatalf("completed step = %#v err=%v", step, err)
	}
	stripped, err := world.StepsGet(ctx, worlds.StepsGetInput{RunID: runID, StepID: "step_a", ResolveData: worlds.ResolveDataNone})
	if err != nil || stripped.Input != nil || stripped.Output != nil {
		t.Fatalf("ResolveDataNone step = %#v err=%v", stripped, err)
	}
	_, err = world.StepsGet(ctx, worlds.StepsGetInput{RunID: runID, StepID: "step_missing"})
	conformExpect(t, err, worlds.ErrStepNotFound, "missing step")

	time.Sleep(2 * time.Millisecond)
	conformAppend(t, world, runID, worlds.EventTypeStepCreated, "step_b", worlds.DataStepCreated{StepName: "second"})
	cause := errors.New("fatal")
	encoded, err := worlds.EncodeErrorPayload(worlds.FatalError{Cause: cause}, worlds.PayloadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	conformAppend(t, world, runID, worlds.EventTypeStepStarted, "step_b", nil)
	conformAppend(t, world, runID, worlds.EventTypeStepFailed, "step_b", worlds.DataStepResult{Error: encoded})
	step, err = world.StepsGet(ctx, worlds.StepsGetInput{RunID: runID, StepID: "step_b"})
	if err != nil || step.Status != worlds.StatusFailed || string(step.Error) != string(encoded) || step.CompletedAt == nil {
		t.Fatalf("failed step = %#v err=%v", step, err)
	}
	var fatal worlds.FatalError
	decoded := worlds.DecodeErrorPayload(step.Error, nil)
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

// conformTerminalSteps checks what a finished run admits: no step or wait
// is created, started, or retried, and a step's final result and a wait's
// completion are still recorded.
func conformTerminalSteps(t *testing.T, world worlds.World) {
	ctx := context.Background()
	runID := conformCreateRun(t, world, "terminal_steps")
	conformAppend(t, world, runID, worlds.EventTypeStepCreated, "step_a", worlds.DataStepCreated{StepName: "a"})
	conformAppend(t, world, runID, worlds.EventTypeStepStarted, "step_a", worlds.DataStepStarted{StepName: "a", Attempt: 1})
	conformAppend(t, world, runID, worlds.EventTypeStepCreated, "step_b", worlds.DataStepCreated{StepName: "b"})
	conformAppend(t, world, runID, worlds.EventTypeStepCreated, "step_c", worlds.DataStepCreated{StepName: "c"})
	conformAppend(t, world, runID, worlds.EventTypeStepStarted, "step_c", worlds.DataStepStarted{StepName: "c", Attempt: 1})
	conformAppend(t, world, runID, worlds.EventTypeWaitCreated, "wait_a", worlds.DataWaitCreated{ResumeAt: time.Now()})
	conformAppend(t, world, runID, worlds.EventTypeRunCancelled, "", nil)
	for _, refused := range []worlds.EventsCreateEvent{
		conformEvent(t, worlds.EventTypeStepCreated, "step_d", worlds.DataStepCreated{StepName: "d"}),
		conformEvent(t, worlds.EventTypeStepStarted, "step_b", worlds.DataStepStarted{StepName: "b", Attempt: 1}),
		conformEvent(t, worlds.EventTypeStepStarted, "step_e", worlds.DataStepStarted{StepName: "e", Attempt: 1, Inline: true, Input: []byte(`"in"`)}),
		conformEvent(t, worlds.EventTypeStepRetrying, "step_c", worlds.DataStepResult{StepName: "c", Attempt: 1, Error: []byte(`{"message":"x"}`)}),
		conformEvent(t, worlds.EventTypeWaitCreated, "wait_b", worlds.DataWaitCreated{ResumeAt: time.Now()}),
	} {
		_, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: refused})
		conformExpect(t, err, worlds.ErrRunTerminal, string(refused.EventType)+" after the run finished")
	}
	conformAppend(t, world, runID, worlds.EventTypeStepCompleted, "step_a", worlds.DataStepResult{StepName: "a"})
	conformAppend(t, world, runID, worlds.EventTypeStepFailed, "step_c", worlds.DataStepResult{StepName: "c", Attempt: 1, Error: []byte(`{"message":"x"}`)})
	conformAppend(t, world, runID, worlds.EventTypeWaitCompleted, "wait_a", nil)
	for id, want := range map[worlds.StepID]worlds.Status{"step_a": worlds.StatusCompleted, "step_c": worlds.StatusFailed} {
		step, err := world.StepsGet(ctx, worlds.StepsGetInput{RunID: runID, StepID: id})
		if err != nil || step.Status != want {
			t.Fatalf("late result of %s = %#v, %v", id, step, err)
		}
	}
}

// conformStepAdmission checks the step checks the runtime relies on: a
// repeated step_created is refused, and a step's result is
// recorded whether or not its step is running.
func conformStepAdmission(t *testing.T, world worlds.World) {
	ctx := context.Background()
	runID := conformCreateRun(t, world, "step_admission")
	conformAppend(t, world, runID, worlds.EventTypeRunStarted, "", nil)
	conformAppend(t, world, runID, worlds.EventTypeStepCreated, "step_a", worlds.DataStepCreated{StepName: "first", Input: json.RawMessage(`"in"`)})

	// A repeated step_created is refused and keeps the step, as in the
	// Workflow SDK's Worlds.
	_, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: conformEvent(t, worlds.EventTypeStepCreated, "step_a", worlds.DataStepCreated{StepName: "second"})})
	conformExpect(t, err, worlds.ErrEventConflict, "repeated step_created")
	if step, err := world.StepsGet(ctx, worlds.StepsGetInput{RunID: runID, StepID: "step_a"}); err != nil || step.StepName != "first" {
		t.Fatalf("step after a repeated step_created = %#v err=%v", step, err)
	}

	// A step that ran out of attempts fails without starting, and a step
	// whose input could not be encoded fails right after its creation.
	conformAppend(t, world, runID, worlds.EventTypeStepFailed, "step_a", worlds.DataStepResult{StepName: "first", Attempt: 1, Error: json.RawMessage(`{"message":"x"}`)})
	step, err := world.StepsGet(ctx, worlds.StepsGetInput{RunID: runID, StepID: "step_a"})
	if err != nil || step.StepName != "first" || step.Status != worlds.StatusFailed || step.CompletedAt == nil {
		t.Fatalf("step = %#v err=%v", step, err)
	}
	conformAppend(t, world, runID, worlds.EventTypeStepCreated, "step_b", worlds.DataStepCreated{StepName: "second"})
	conformAppend(t, world, runID, worlds.EventTypeStepCompleted, "step_b", worlds.DataStepResult{StepName: "second", Result: json.RawMessage(`"out"`)})
	step, err = world.StepsGet(ctx, worlds.StepsGetInput{RunID: runID, StepID: "step_b"})
	if err != nil || step.Status != worlds.StatusCompleted || string(step.Output) != `"out"` {
		t.Fatalf("step = %#v err=%v", step, err)
	}
}

// conformInlineSteps checks an inline step, which its first step_started
// creates, with its name and input, and which no step_created precedes.
func conformInlineSteps(t *testing.T, world worlds.World) {
	ctx := context.Background()
	runID := conformCreateRun(t, world, "inline_steps")
	conformAppend(t, world, runID, worlds.EventTypeRunStarted, "", nil)
	creation := worlds.DataStepStarted{StepName: "inline", WorkflowName: "conformance", Attempt: 1, Input: json.RawMessage(`"inline-in"`), Inline: true}
	started := conformAppend(t, world, runID, worlds.EventTypeStepStarted, "step_inline", creation)
	if started.AllocatedEventCount != 1 {
		t.Fatalf("creating step_started = %#v", started)
	}
	step, err := world.StepsGet(ctx, worlds.StepsGetInput{RunID: runID, StepID: "step_inline"})
	if err != nil || step.StepName != "inline" || step.Status != worlds.StatusRunning || step.Attempt != 1 || string(step.Input) != `"inline-in"` || step.StartedAt == nil {
		t.Fatalf("inline step = %#v err=%v", step, err)
	}
	events := conformAllEvents(t, world, runID)
	for _, event := range events {
		if event.EventType == worlds.EventTypeStepCreated {
			t.Fatalf("an inline step recorded step_created: %#v", event)
		}
	}
	stored := events[len(events)-1]
	var data worlds.DataStepStarted
	if stored.EventType != worlds.EventTypeStepStarted || json.Unmarshal(stored.Data, &data) != nil || !data.Inline || data.StepName != "inline" || data.WorkflowName != "conformance" || data.Attempt != 1 || string(data.Input) != `"inline-in"` {
		t.Fatalf("stored creating step_started = %#v", stored)
	}
	// Its input is a payload: listing without data removes it.
	stripped, err := world.EventsGet(ctx, worlds.EventsGetInput{RunID: runID, EventID: started.EventID, ResolveData: worlds.EventsResolveDataNone})
	data = worlds.DataStepStarted{}
	if err != nil || json.Unmarshal(stripped.Data, &data) != nil || data.Input != nil || data.StepName != "inline" {
		t.Fatalf("creating step_started without data = %#v err=%v", stripped, err)
	}
	// An inline step is created once.
	_, err = world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: conformEvent(t, worlds.EventTypeStepStarted, "step_inline", creation)})
	conformExpect(t, err, worlds.ErrEventConflict, "repeated creating step_started")

	// A failed inline attempt retries as a step delivery, which starts and
	// completes it.
	retryAt := time.Now().UTC().Truncate(time.Millisecond)
	conformAppend(t, world, runID, worlds.EventTypeStepRetrying, "step_inline", worlds.DataStepResult{StepName: "inline", Attempt: 1, Error: json.RawMessage(`{"message":"x"}`), RetryAfter: &retryAt, CreatorMessageID: "msg_flow"})
	conformAppend(t, world, runID, worlds.EventTypeStepStarted, "step_inline", worlds.DataStepStarted{StepName: "inline", Attempt: 2})
	conformAppend(t, world, runID, worlds.EventTypeStepCompleted, "step_inline", worlds.DataStepResult{StepName: "inline", WorkflowName: "conformance", Result: json.RawMessage(`"inline-out"`)})
	step, err = world.StepsGet(ctx, worlds.StepsGetInput{RunID: runID, StepID: "step_inline"})
	if err != nil || step.Status != worlds.StatusCompleted || step.Attempt != 2 || string(step.Input) != `"inline-in"` || string(step.Output) != `"inline-out"` {
		t.Fatalf("completed inline step = %#v err=%v", step, err)
	}
}

func conformLargePayloads(t *testing.T, world worlds.World) {
	ctx := context.Background()
	// Each payload fits in its own event, while together they exceed what a
	// World could keep in one record of a step. Event data carries payloads
	// in base64, so each must stay under 300 KB to fit a 400 KB record.
	payload := func(fill string) json.RawMessage {
		return json.RawMessage(`"` + strings.Repeat(fill, 250<<10) + `"`)
	}
	input, output := payload("i"), payload("o")
	runID := conformRunID("payloads")
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "payloads", Input: input}); err != nil {
		t.Fatal(err)
	}
	conformAppend(t, world, runID, worlds.EventTypeRunStarted, "", nil)
	conformAppend(t, world, runID, worlds.EventTypeStepCreated, "step_a", worlds.DataStepCreated{StepName: "big", Input: input})
	conformAppend(t, world, runID, worlds.EventTypeStepStarted, "step_a", nil)
	conformAppend(t, world, runID, worlds.EventTypeStepCompleted, "step_a", worlds.DataStepResult{Result: output})
	encoded, err := json.Marshal(worlds.NewError(errors.New("failed")))
	if err != nil {
		t.Fatal(err)
	}
	conformAppend(t, world, runID, worlds.EventTypeRunFailed, "", worlds.DataRunFailed{Error: encoded})

	step, err := world.StepsGet(ctx, worlds.StepsGetInput{RunID: runID, StepID: "step_a"})
	if err != nil || string(step.Input) != string(input) || string(step.Output) != string(output) {
		t.Fatalf("step input %d bytes, output %d bytes, err=%v", len(step.Input), len(step.Output), err)
	}
	steps, err := world.StepsList(ctx, worlds.StepsListInput{RunID: runID})
	if err != nil || len(steps.Data) != 1 || string(steps.Data[0].Output) != string(output) {
		t.Fatalf("listed steps = %d, err=%v", len(steps.Data), err)
	}
	stripped, err := world.StepsGet(ctx, worlds.StepsGetInput{RunID: runID, StepID: "step_a", ResolveData: worlds.ResolveDataNone})
	if err != nil || stripped.Input != nil || stripped.Output != nil || stripped.Status != worlds.StatusCompleted {
		t.Fatalf("ResolveDataNone step = %d/%d bytes, status %s, err=%v", len(stripped.Input), len(stripped.Output), stripped.Status, err)
	}

	run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID})
	if err != nil || string(run.Input) != string(input) || string(run.Error) != string(encoded) {
		t.Fatalf("run input %d bytes, error %s, err=%v", len(run.Input), run.Error, err)
	}
	strippedRun, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID, ResolveData: worlds.ResolveDataNone})
	if err != nil || strippedRun.Input != nil || string(strippedRun.Error) != string(encoded) {
		t.Fatalf("ResolveDataNone run = input %d bytes, error %s, err=%v", len(strippedRun.Input), strippedRun.Error, err)
	}
}
