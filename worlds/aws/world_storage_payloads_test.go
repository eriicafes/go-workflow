package awsworld

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/eriicafes/go-workflow/worlds"
)

func TestPayloadsAreReadFromEvents(t *testing.T) {
	ctx := context.Background()
	dynamo := newFakeDynamo()
	world := testWorld(t, dynamo, &fakeSQS{}, nil)
	now := time.Now().UTC()
	store := func(item map[string]ddb.AttributeValue) { dynamo.items[itemID(item)] = item }
	event := func(slot uint64, eventType worlds.EventType, correlationID string, data any) {
		encoded, _ := json.Marshal(data)
		store(eventItem(worlds.Event{RunID: "wrun_1", EventID: worlds.EventIDForSlot(slot), EventType: eventType, CorrelationID: correlationID, Data: encoded, CreatedAt: now}, slot))
	}
	event(1, worlds.EventTypeRunCreated, "", worlds.DataRunCreated{WorkflowName: "flow", Input: json.RawMessage(`"in"`)})
	event(2, worlds.EventTypeStepCreated, "step_a", worlds.DataStepCreated{StepName: "a", Input: json.RawMessage(`"step in"`)})
	event(3, worlds.EventTypeStepRetrying, "step_a", worlds.DataStepResult{Error: json.RawMessage(`"retry"`)})
	event(4, worlds.EventTypeHookCreated, "hook_a", worlds.DataHookCreated{Token: "tok", Metadata: []byte(`"meta"`)})
	event(5, worlds.EventTypeRunFailed, "", worlds.DataRunFailed{Error: json.RawMessage(`"boom"`)})
	store(runItem(runView{Run: worlds.Run{RunID: "wrun_1", WorkflowName: "flow", Status: worlds.StatusFailed, CreatedAt: now, UpdatedAt: now}, eventCount: 5, resultEventID: worlds.EventIDForSlot(5)}))
	store(stepItem(stepView{Step: worlds.Step{RunID: "wrun_1", StepID: "step_a", Status: worlds.StatusPending, CreatedAt: now, UpdatedAt: now}, createdEventID: worlds.EventIDForSlot(2), resultEventID: worlds.EventIDForSlot(3)}))
	hook, err := hookItem(worlds.Hook{RunID: "wrun_1", HookID: "hook_a", Token: "tok", CreatedAt: now, UpdatedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	setString(hook, "createdEventId", string(worlds.EventIDForSlot(4)))
	store(hook)

	run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: "wrun_1"})
	if err != nil || string(run.Input) != `"in"` || run.Output != nil || string(run.Error) != `"boom"` {
		t.Fatalf("run = %q %q %q err=%v", run.Input, run.Output, run.Error, err)
	}
	if run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: "wrun_1", ResolveData: worlds.ResolveDataNone}); err != nil || run.Input != nil || string(run.Error) != `"boom"` {
		t.Fatalf("unresolved run = %q %q err=%v", run.Input, run.Error, err)
	}
	step, err := world.StepsGet(ctx, worlds.StepsGetInput{RunID: "wrun_1", StepID: "step_a"})
	if err != nil || string(step.Input) != `"step in"` || step.Output != nil || string(step.Error) != `"retry"` {
		t.Fatalf("step = %q %q %q err=%v", step.Input, step.Output, step.Error, err)
	}
	if step, err := world.StepsGet(ctx, worlds.StepsGetInput{RunID: "wrun_1", StepID: "step_a", ResolveData: worlds.ResolveDataNone}); err != nil || step.Input != nil || string(step.Error) != `"retry"` {
		t.Fatalf("unresolved step = %q %q err=%v", step.Input, step.Error, err)
	}
	got, err := world.HooksGet(ctx, worlds.HooksGetInput{HookID: "hook_a"})
	if err != nil || string(got.Metadata) != `"meta"` {
		t.Fatalf("hook metadata = %q err=%v", got.Metadata, err)
	}
	calls := dynamo.batchCalls
	if got, err := world.HooksGet(ctx, worlds.HooksGetInput{HookID: "hook_a", ResolveData: worlds.ResolveDataNone}); err != nil || got.Metadata != nil || dynamo.batchCalls != calls {
		t.Fatalf("unresolved hook metadata = %q err=%v, events read %d times", got.Metadata, err, dynamo.batchCalls-calls)
	}
}

func TestCompletedPayloadsSkippedWithoutData(t *testing.T) {
	ctx := context.Background()
	dynamo := newFakeDynamo()
	world := testWorld(t, dynamo, &fakeSQS{}, nil)
	now := time.Now().UTC()
	output, _ := json.Marshal(worlds.DataRunCompleted{Output: json.RawMessage(`"out"`)})
	event := eventItem(worlds.Event{RunID: "wrun_1", EventID: worlds.EventIDForSlot(2), EventType: worlds.EventTypeRunCompleted, Data: output, CreatedAt: now}, 2)
	dynamo.items[itemID(event)] = event
	item := runItem(runView{Run: worlds.Run{RunID: "wrun_1", Status: worlds.StatusCompleted, CreatedAt: now, UpdatedAt: now}, eventCount: 2, resultEventID: worlds.EventIDForSlot(2)})
	dynamo.items[itemID(item)] = item

	run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: "wrun_1", ResolveData: worlds.ResolveDataNone})
	if err != nil || run.Output != nil || run.Error != nil || dynamo.batchCalls != 0 {
		t.Fatalf("run = %q %q err=%v batch calls=%d", run.Output, run.Error, err, dynamo.batchCalls)
	}
	if run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: "wrun_1"}); err != nil || string(run.Output) != `"out"` || run.Error != nil {
		t.Fatalf("resolved run = %q %q err=%v", run.Output, run.Error, err)
	}
}
