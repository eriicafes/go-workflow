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
	none, err := analytics.RunsList(ctx, worlds.AnalyticsRunsListInput{WorkflowName: name, StartTime: &future})
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
	conformAppend(t, world, second, worlds.EventTypeWaitCreated, "wait_a", worlds.DataWaitCreated{ResumeAt: due, QueueMessageID: "msg_1"})
	time.Sleep(2 * time.Millisecond)
	conformAppend(t, world, second, worlds.EventTypeWaitCreated, "wait_b", worlds.DataWaitCreated{ResumeAt: due})
	conformAppend(t, world, second, worlds.EventTypeWaitCompleted, "wait_a", nil)
	waits, err := analytics.WaitsList(ctx, worlds.AnalyticsWaitsListInput{RunID: second})
	if err != nil || len(waits.Data) != 2 || waits.Data[0].WaitID != "wait_a" || waits.Data[0].CompletedAt == nil || waits.Data[0].QueueMessageID != "msg_1" || !waits.Data[0].DueAt.Equal(due) || waits.Data[1].CompletedAt != nil || !waits.Data[1].DueAt.Equal(due) {
		t.Fatalf("analytics waits = %#v err=%v", waits, err)
	}
	wait, err := analytics.WaitsGet(ctx, worlds.AnalyticsWaitsGetInput{RunID: second, WaitID: "wait_b"})
	if err != nil || wait.WaitID != "wait_b" || !wait.DueAt.Equal(due) {
		t.Fatalf("analytics wait = %#v err=%v", wait, err)
	}
	_, err = analytics.WaitsGet(ctx, worlds.AnalyticsWaitsGetInput{RunID: second, WaitID: "wait_missing"})
	conformExpect(t, err, worlds.ErrEventNotFound, "missing wait")
	_, err = analytics.WaitsList(ctx, worlds.AnalyticsWaitsListInput{RunID: "wrun_missing"})
	conformExpect(t, err, worlds.ErrRunNotFound, "waits of missing run")
}
