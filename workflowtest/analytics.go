package workflowtest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

func conformAnalytics(t *testing.T, world worlds.World) {
	analytics := world.Analytics()
	if analytics == nil {
		t.Skip("analytics not supported")
	}
	ctx := context.Background()
	name := fmt.Sprintf("analytics_%d", time.Now().UnixNano())
	before := time.Now().Add(-time.Second)
	first := conformRunID("analytics")
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: first, WorkflowName: name, Attributes: map[string]string{"zone": "eu", "tier": "gold"}}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Millisecond)
	second := conformRunID("analytics")
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: second, WorkflowName: name, Attributes: map[string]string{"zone": "us"}}); err != nil {
		t.Fatal(err)
	}
	conformAppend(t, world, second, worlds.EventTypeRunStarted, "", nil)

	run, err := analytics.RunsGet(ctx, worlds.AnalyticsRunsGetInput{RunID: second})
	if err != nil || run.RunID != second || run.Status != worlds.StatusRunning || run.Attributes["zone"] != "us" || run.StartedAt == nil {
		t.Fatalf("analytics run = %#v err=%v", run, err)
	}
	_, err = analytics.RunsGet(ctx, worlds.AnalyticsRunsGetInput{RunID: "wrun_missing"})
	conformExpect(t, err, worlds.ErrRunNotFound, "analytics missing run")
	eu, err := analytics.RunsList(ctx, worlds.AnalyticsRunsListInput{WorkflowName: name, Attributes: map[string]string{"zone": "eu"}})
	if err != nil || len(eu.Data) != 1 || eu.Data[0].RunID != first {
		t.Fatalf("analytics runs by attribute = %#v err=%v", eu, err)
	}
	running, err := analytics.RunsList(ctx, worlds.AnalyticsRunsListInput{WorkflowName: name, Status: []worlds.Status{worlds.StatusRunning}})
	if err != nil || len(running.Data) != 1 || running.Data[0].RunID != second {
		t.Fatalf("analytics runs by status = %#v err=%v", running, err)
	}
	future := time.Now().Add(time.Hour)
	// The Workflow SDK's analytics take both bounds of a window or neither.
	later := future.Add(time.Hour)
	none, err := analytics.RunsList(ctx, worlds.AnalyticsRunsListInput{WorkflowName: name, StartTime: &future, EndTime: &later})
	if err != nil || len(none.Data) != 0 {
		t.Fatalf("analytics runs after StartTime = %#v err=%v", none, err)
	}
	window, err := analytics.RunsList(ctx, worlds.AnalyticsRunsListInput{WorkflowName: name, StartTime: &before, EndTime: &future, Pagination: worlds.Pagination{Limit: 1}})
	if err != nil || len(window.Data) != 1 || !window.HasMore || window.Data[0].RunID != first {
		t.Fatalf("analytics runs in window = %#v err=%v", window, err)
	}
	attributes, err := analytics.AttributesList(ctx, worlds.AnalyticsAttributesListInput{WorkflowName: name})
	if err != nil || len(attributes.Data) != 2 || attributes.Data[0].Key != "tier" || attributes.Data[0].RunCount != 1 || attributes.Data[1].Key != "zone" || attributes.Data[1].RunCount != 2 {
		t.Fatalf("analytics attributes = %#v err=%v", attributes, err)
	}

	due := time.Now().Add(time.Minute).UTC().Truncate(time.Millisecond)
	conformAppend(t, world, second, worlds.EventTypeWaitCreated, "wait_a", worlds.DataWaitCreated{ResumeAt: due})
	time.Sleep(2 * time.Millisecond)
	conformAppend(t, world, second, worlds.EventTypeWaitCreated, "wait_b", worlds.DataWaitCreated{ResumeAt: due})
	conformAppend(t, world, second, worlds.EventTypeWaitCompleted, "wait_a", nil)
	waits, err := analytics.WaitsList(ctx, worlds.AnalyticsWaitsListInput{RunID: second})
	if err != nil || len(waits.Data) != 2 || waits.Data[0].WaitID != "wait_a" || waits.Data[0].Status != worlds.WaitStatusCompleted || waits.Data[0].CompletedAt == nil || !waits.Data[0].ResumeAt.Equal(due) || waits.Data[1].Status != worlds.WaitStatusWaiting || waits.Data[1].CompletedAt != nil || !waits.Data[1].ResumeAt.Equal(due) {
		t.Fatalf("analytics waits = %#v err=%v", waits, err)
	}
	wait, err := analytics.WaitsGet(ctx, worlds.AnalyticsWaitsGetInput{RunID: second, WaitID: "wait_b"})
	if err != nil || wait.WaitID != "wait_b" || wait.ResumeAt == nil || !wait.ResumeAt.Equal(due) {
		t.Fatalf("analytics wait = %#v err=%v", wait, err)
	}
	if waiting, err := analytics.WaitsList(ctx, worlds.AnalyticsWaitsListInput{RunID: second, Status: worlds.WaitStatusWaiting}); err != nil || len(waiting.Data) != 1 || waiting.Data[0].WaitID != "wait_b" {
		t.Fatalf("analytics waiting waits = %#v err=%v", waiting, err)
	}
	_, err = analytics.WaitsGet(ctx, worlds.AnalyticsWaitsGetInput{RunID: second, WaitID: "wait_missing"})
	conformExpect(t, err, worlds.ErrEventNotFound, "missing wait")
	_, err = analytics.WaitsList(ctx, worlds.AnalyticsWaitsListInput{RunID: "wrun_missing"})
	conformExpect(t, err, worlds.ErrRunNotFound, "waits of missing run")

	// Steps, events, and hooks are summarized without their payloads, and a
	// disposed hook is still reported, from its events.
	conformAppend(t, world, second, worlds.EventTypeStepCreated, "step_a", worlds.DataStepCreated{StepName: "charge", Input: []byte("input")})
	step, err := analytics.StepsGet(ctx, worlds.AnalyticsStepsGetInput{RunID: second, StepID: "step_a"})
	if err != nil || step.StepName != "charge" || step.Status != worlds.StatusPending {
		t.Fatalf("analytics step = %#v err=%v", step, err)
	}
	if steps, err := analytics.StepsList(ctx, worlds.AnalyticsStepsListInput{RunID: second}); err != nil || len(steps.Data) != 1 || steps.Data[0].StepID != "step_a" {
		t.Fatalf("analytics steps = %#v err=%v", steps, err)
	}
	token := fmt.Sprintf("analytics-%d", time.Now().UnixNano())
	if _, err := conformClaim(world, second, "hook_a", token, false); err != nil {
		t.Fatal(err)
	}
	conformAppend(t, world, second, worlds.EventTypeHookReceived, "hook_a", conformReceived(t, []byte("payload")))
	conformAppend(t, world, second, worlds.EventTypeHookDisposed, "hook_a", nil)
	hook, err := analytics.HooksGet(ctx, worlds.AnalyticsHooksGetInput{RunID: second, HookID: "hook_a"})
	if err != nil || hook.Status != worlds.AnalyticsHookDisposed || hook.ReceivedAt == nil || hook.DisposedAt == nil || hook.UpdatedAt.Before(hook.CreatedAt) {
		t.Fatalf("analytics disposed hook = %#v err=%v", hook, err)
	}
	if hooks, err := analytics.HooksList(ctx, worlds.AnalyticsHooksListInput{RunID: second}); err != nil || len(hooks.Data) != 1 || hooks.Data[0].HookID != "hook_a" {
		t.Fatalf("analytics hooks = %#v err=%v", hooks, err)
	}
	_, err = analytics.HooksGet(ctx, worlds.AnalyticsHooksGetInput{RunID: second, HookID: "hook_missing"})
	conformExpect(t, err, worlds.ErrHookNotFound, "analytics missing hook")

	created, err := analytics.EventsList(ctx, worlds.AnalyticsEventsListInput{RunID: second, EventType: worlds.EventTypeStepCreated})
	if err != nil || len(created.Data) != 1 || created.Data[0].StepName != "charge" || created.Data[0].WorkflowName != name || created.Data[0].RunCreatedAt.IsZero() {
		t.Fatalf("analytics step_created events = %#v err=%v", created, err)
	}
	if timers, err := analytics.EventsList(ctx, worlds.AnalyticsEventsListInput{RunID: second, EventType: worlds.EventTypeWaitCreated, Pagination: worlds.Pagination{Limit: 1}}); err != nil || len(timers.Data) != 1 || !timers.HasMore || timers.Data[0].ResumeAt == nil || !timers.Data[0].ResumeAt.Equal(due) {
		t.Fatalf("analytics wait_created events = %#v err=%v", timers, err)
	}
	if byHook, err := analytics.EventsListByCorrelationID(ctx, worlds.AnalyticsEventsListByCorrelationIDInput{RunID: second, CorrelationID: "hook_a"}); err != nil || len(byHook.Data) != 3 || byHook.Data[0].IsWebhook == nil {
		t.Fatalf("analytics hook events = %#v err=%v", byHook, err)
	}
	many, err := analytics.EventsGetMany(ctx, worlds.AnalyticsEventsGetManyInput{RunID: second, EventIDs: []worlds.EventID{created.Data[0].EventID, worlds.EventIDForSlot(9999)}})
	if err != nil || len(many) != 1 || many[0].EventID != created.Data[0].EventID {
		t.Fatalf("analytics events getMany = %#v err=%v", many, err)
	}
}
