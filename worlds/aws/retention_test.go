package awsworld

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/eriicafes/go-workflow/worlds"
)

func TestRetentionRequiresScheduler(t *testing.T) {
	config := Config{BaseURL: testBaseURL, TableName: "workflow", QueueURL: testQueueURL, Retention: time.Hour}
	if _, err := newWorld(config, newFakeDynamo(), &fakeSQS{}, nil); err == nil {
		t.Fatal("New accepted a retention over 15 minutes without Scheduler")
	}
	config.Retention = 10 * time.Minute
	if _, err := newWorld(config, newFakeDynamo(), &fakeSQS{}, nil); err != nil {
		t.Fatalf("New rejected a retention SQS can delay without Scheduler: %v", err)
	}
	config.Retention = 0
	config.Retentions = map[string]time.Duration{"long": time.Hour}
	if _, err := newWorld(config, newFakeDynamo(), &fakeSQS{}, nil); err == nil {
		t.Fatal("New accepted a workflow retention over 15 minutes without Scheduler")
	}
	config.Retentions = nil
	config.Retention = -time.Second
	if _, err := newWorld(config, newFakeDynamo(), &fakeSQS{}, nil); err == nil {
		t.Fatal("New accepted a negative retention")
	}
}

func TestScheduleExpiry(t *testing.T) {
	dynamo, queue := newFakeDynamo(), &fakeSQS{}
	world := testWorld(t, dynamo, queue, newFakeScheduler())
	ctx := context.Background()
	due := time.Now().Add(5 * time.Minute)
	for _, run := range []worlds.Run{
		{RunID: "wrun_short", WorkflowName: "short", ExpiredAt: &due},
		{RunID: "wrun_kept", WorkflowName: "kept"},
	} {
		if err := world.scheduleExpiry(ctx, run); err != nil {
			t.Fatal(err)
		}
	}
	if dynamo.has("expire-wrun_kept") {
		t.Error("a run without ExpiredAt was scheduled to expire")
	}
	record, found, err := world.queueRecord(ctx, "expire-wrun_short")
	if err != nil || !found || record.notBefore == nil || !record.notBefore.Equal(due) {
		t.Fatalf("short run's expiry = %+v, %v, %v", record, found, err)
	}
	if len(queue.messages) != 1 {
		t.Fatalf("%d messages, want the short run's expiry through SQS", len(queue.messages))
	}
}

// TestExpireRun seeds a finished run's items and runs its expiry delivery.
func TestExpireRun(t *testing.T) {
	dynamo, queue, schedules := newFakeDynamo(), &fakeSQS{}, newFakeScheduler()
	world := testWorld(t, dynamo, queue, schedules)
	ctx := context.Background()

	expired := time.Now().Add(-time.Minute)
	done := seedRun(t, dynamo, "wrun_done", "hook_done", "token_done", &expired)
	kept := seedRun(t, dynamo, "wrun_kept", "hook_kept", "token_kept", nil)

	if err := world.queueExpiry(ctx, "wrun_done", "retention", expired); err != nil {
		t.Fatal(err)
	}
	if err := world.deliver(ctx, nil, awsv2.ToString(queue.messages[0].MessageBody)); err != nil {
		t.Fatal(err)
	}
	if dynamo.has("expire-wrun_done") {
		t.Error("the expiry record was not deleted")
	}
	// The run's items stay, without its payloads.
	for _, id := range done {
		if dynamo.items[id] == nil {
			t.Errorf("%s was deleted", id)
		}
	}
	event := eventFromItem(dynamo.items[done[1]])
	if payload := worlds.EventPayload(event); payload != nil {
		t.Errorf("expired run's run_created payload = %s", payload)
	}
	if !readBool(dynamo.items[done[0]], "dataPurged") {
		t.Error("the expired run is not marked purged")
	}
	if dynamo.items[itemID(chunkKey("wrun_done", "out", 0))] != nil {
		t.Error("the expired run's stream chunk was not deleted")
	}
	if meta, err := world.streamMeta(ctx, "wrun_done", "out"); err != nil || meta.count != 0 || !meta.closed {
		t.Errorf("expired run's stream = %+v, %v", meta, err)
	}
	if payload := worlds.EventPayload(eventFromItem(dynamo.items[kept[1]])); string(payload) != `"input"` {
		t.Errorf("another run's payload = %s", payload)
	}
	run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: "wrun_done"})
	if err != nil || run.ExpiredAt == nil || run.Input != nil {
		t.Fatalf("expired run = %+v, %v", run, err)
	}
}

// TestRetentionDynamo finishes runs on DynamoDB Local and checks that their
// expiry deletes their data.
func TestRetentionDynamo(t *testing.T) {
	client := dynamoClient(t)
	queue, schedules := &fakeSQS{}, newFakeScheduler()
	world, err := newWorld(Config{
		BaseURL:   testBaseURL,
		TableName: newTable(t, client),
		QueueURL:  testQueueURL,
		Scheduler: &SchedulerConfig{TargetARN: "arn:aws:sqs:us-east-1:000000000000:workflow", RoleARN: "arn:aws:iam::000000000000:role/scheduler"},
		Retention: time.Hour,
	}, client, queue, schedules)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	runID := worlds.NewRunID()
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "retention", Input: []byte(`"input"`)}); err != nil {
		t.Fatal(err)
	}
	for _, event := range []worlds.EventsCreateEvent{
		{EventType: worlds.EventTypeRunStarted},
		{EventType: worlds.EventTypeStepCreated, CorrelationID: "step", Data: []byte(`{"stepName":"step","input":"aW5wdXQ="}`)},
		{EventType: worlds.EventTypeHookCreated, CorrelationID: "hook", Data: []byte(`{"token":"token"}`)},
		{EventType: worlds.EventTypeRunCompleted, Data: []byte(`{"output":"b2s="}`)},
	} {
		if _, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: event}); err != nil {
			t.Fatalf("EventsCreate(%s): %v", event.EventType, err)
		}
	}
	run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID})
	if err != nil || run.ExpiredAt == nil || run.ExpiredAt.Sub(*run.CompletedAt) != time.Hour || run.Output == nil {
		t.Fatalf("finished run = %+v, %v", run, err)
	}
	schedule := schedules.schedules[scheduleName("expire-"+string(runID))]
	if schedule == nil {
		t.Fatal("finishing the run scheduled no expiry")
	}
	if err := world.deliver(ctx, nil, awsv2.ToString(schedule.Target.Input)); err != nil {
		t.Fatal(err)
	}
	// The record is due in an hour, so the delivery is sent on, not run.
	if run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID}); err != nil || run.Output == nil {
		t.Fatalf("run before its expiry = %+v, %v", run, err)
	}

	if err := world.expireRun(ctx, worlds.QueueInput{RunID: runID, WorkflowName: "retention"}); err != nil {
		t.Fatal(err)
	}
	run, err = world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID})
	if err != nil || run.Status != worlds.StatusCompleted || run.Input != nil || run.Output != nil || !worlds.RunDataExpired(run, time.Now()) {
		t.Fatalf("expired run = %+v, %v", run, err)
	}
	step, err := world.StepsGet(ctx, worlds.StepsGetInput{RunID: runID, StepID: "step"})
	if err != nil || step.StepName != "step" || step.Input != nil {
		t.Fatalf("expired run's step = %+v, %v", step, err)
	}
}

// seedRun stores a finished run's view, its run_created event with a
// payload, a step, a hook, its token, and a stream chunk, and returns their
// item IDs: the run's view first, then its event.
func seedRun(t *testing.T, dynamo *fakeDynamo, runID, hookID, token string, expiredAt *time.Time) []string {
	t.Helper()
	now := time.Now()
	hook, err := hookItem(worlds.Hook{HookID: worlds.HookID(hookID), RunID: worlds.RunID(runID), Token: token, CreatedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	created, err := json.Marshal(worlds.DataRunCreated{WorkflowName: "retention", Input: []byte(`"input"`)})
	if err != nil {
		t.Fatal(err)
	}
	items := []map[string]ddb.AttributeValue{
		runItem(runView{Run: worlds.Run{RunID: worlds.RunID(runID), WorkflowName: "retention", Status: worlds.StatusCompleted, CreatedAt: now, UpdatedAt: now, CompletedAt: &now, ExpiredAt: expiredAt}, eventCount: 1}),
		eventItem(worlds.Event{RunID: worlds.RunID(runID), EventID: worlds.EventIDForSlot(1), EventType: worlds.EventTypeRunCreated, Data: created, CreatedAt: now}, 1),
		stepItem(stepView{Step: worlds.Step{RunID: worlds.RunID(runID), StepID: "step", StepName: "step", Status: worlds.StatusCompleted, CreatedAt: now, UpdatedAt: now}}),
		hook,
		tokenItem(token, tokenView{hookID: worlds.HookID(hookID), runID: worlds.RunID(runID)}),
		merge(streamKey(runID, "out"), map[string]ddb.AttributeValue{"name": avs("out"), "count": avn(1), "closed": avbool(false)}),
		merge(chunkKey(runID, "out", 0), map[string]ddb.AttributeValue{"data": avb([]byte("chunk"))}),
	}
	ids := make([]string, len(items)-1)
	for i, item := range items {
		id := itemID(item)
		dynamo.items[id] = item
		if i < len(ids) {
			ids[i] = id
		}
	}
	return ids
}
