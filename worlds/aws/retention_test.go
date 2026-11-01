package awsworld

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
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

func TestScheduleExpiryByWorkflow(t *testing.T) {
	dynamo, queue := newFakeDynamo(), &fakeSQS{}
	world := testWorld(t, dynamo, queue, newFakeScheduler())
	world.config.Retention = time.Hour
	world.config.Retentions = map[string]time.Duration{"short": 5 * time.Minute, "kept": 0}
	completed := time.Now()
	for _, workflow := range []string{"short", "kept"} {
		run := worlds.Run{RunID: worlds.RunID("wrun_" + workflow), WorkflowName: workflow, CompletedAt: &completed}
		if err := world.scheduleExpiry(context.Background(), run); err != nil {
			t.Fatal(err)
		}
	}
	if dynamo.has("expire-wrun_kept") {
		t.Error("a run of a workflow kept forever was scheduled to expire")
	}
	record, found, err := world.queueRecord(context.Background(), "expire-wrun_short")
	if err != nil || !found || record.notBefore == nil || !record.notBefore.Equal(completed.Add(5*time.Minute)) {
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
	world.config.Retention = time.Hour
	ctx := context.Background()

	done := seedRun(t, dynamo, "wrun_done", "hook_done", "token_done", nil)
	kept := seedRun(t, dynamo, "wrun_kept", "hook_kept", "token_kept", nil)
	retainUntil := time.Now().Add(2 * time.Hour)
	retained := seedRun(t, dynamo, "wrun_retained", "hook_retained", "token_retained", &retainUntil)

	// The retention has passed, so the delivery is sent now.
	finished := worlds.Run{RunID: "wrun_done", WorkflowName: "retention", CompletedAt: awsv2.Time(time.Now().Add(-2 * time.Hour))}
	if err := world.scheduleExpiry(ctx, finished); err != nil {
		t.Fatal(err)
	}
	if !dynamo.has("expire-wrun_done") || len(queue.messages) != 1 {
		t.Fatalf("expiry record %v, %d messages", dynamo.has("expire-wrun_done"), len(queue.messages))
	}
	// Scheduling it again, as a retried append does, replaces the record.
	if err := world.scheduleExpiry(ctx, finished); err != nil {
		t.Fatal(err)
	}
	if err := world.deliver(ctx, nil, awsv2.ToString(queue.messages[0].MessageBody)); err != nil {
		t.Fatal(err)
	}
	for _, id := range done {
		if dynamo.items[id] != nil {
			t.Errorf("%s was not deleted", id)
		}
	}
	if dynamo.has("expire-wrun_done") {
		t.Error("the expiry record was not deleted")
	}
	for _, id := range kept {
		if dynamo.items[id] == nil {
			t.Errorf("%s of another run was deleted", id)
		}
	}

	// A retained token reschedules the expiry for when its retention ends.
	if err := world.queueExpiry(ctx, "wrun_retained", "retention", time.Now().Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(queueMessage{ID: "expire-wrun_retained", Input: worlds.QueueInput{RunID: "wrun_retained", WorkflowName: "retention"}})
	if err := world.deliver(ctx, nil, string(body)); err != nil {
		t.Fatal(err)
	}
	for _, id := range retained {
		if dynamo.items[id] == nil {
			t.Errorf("%s with a retained token was deleted", id)
		}
	}
	record, found, err := world.queueRecord(ctx, "expire-wrun_retained")
	if err != nil || !found || record.notBefore == nil || record.notBefore.Sub(retainUntil).Abs() > time.Second {
		t.Fatalf("rescheduled record = %+v, %v, %v", record, found, err)
	}
	if schedules.schedules[scheduleName("expire-wrun_retained")] == nil {
		t.Fatal("the rescheduled expiry has no schedule")
	}
}

// TestRetentionDynamo finishes runs on DynamoDB Local and checks that their
// expiry deletes them.
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
	runID := worlds.RunID("wrun_retention")
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "retention", Input: []byte(`"input"`)}); err != nil {
		t.Fatal(err)
	}
	for _, event := range []worlds.EventsCreateEvent{
		{EventType: worlds.EventTypeRunStarted},
		{EventType: worlds.EventTypeStepCreated, CorrelationID: "step", Data: []byte(`{"stepName":"step","input":1}`)},
		{EventType: worlds.EventTypeHookCreated, CorrelationID: "hook", Data: []byte(`{"token":"token"}`)},
		{EventType: worlds.EventTypeRunCompleted, Data: []byte(`{"output":"ok"}`)},
	} {
		if _, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: event}); err != nil {
			t.Fatalf("EventsCreate(%s): %v", event.EventType, err)
		}
	}
	schedule := schedules.schedules[scheduleName("expire-"+string(runID))]
	if schedule == nil {
		t.Fatal("finishing the run scheduled no expiry")
	}
	if err := world.deliver(ctx, nil, awsv2.ToString(schedule.Target.Input)); err != nil {
		t.Fatal(err)
	}
	// The record is due in an hour, so the delivery is sent on, not run.
	if run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID}); err != nil || run.Status != worlds.StatusCompleted {
		t.Fatalf("run before its expiry = %+v, %v", run, err)
	}

	if _, err := world.expireRun(ctx, worlds.QueueInput{RunID: runID, WorkflowName: "retention"}); err != nil {
		t.Fatal(err)
	}
	if _, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID}); err == nil {
		t.Fatal("the expired run is still readable")
	}
	var left int
	err = world.query(ctx, &dynamodb.QueryInput{
		KeyConditionExpression:    awsv2.String("pk = :pk"),
		ExpressionAttributeValues: map[string]ddb.AttributeValue{":pk": avs(runPartitionKey(string(runID)))},
	}, func(map[string]ddb.AttributeValue) bool { left++; return true })
	if err != nil || left != 0 {
		t.Fatalf("%d items left in the run partition, %v", left, err)
	}
	if _, found, err := world.item(ctx, tokenKey("token")); err != nil || found {
		t.Fatalf("token item left: %v, %v", found, err)
	}
	page, err := world.RunsList(ctx, worlds.RunsListInput{})
	if err != nil || len(page.Data) != 0 {
		t.Fatalf("RunsList = %v, %v", page.Data, err)
	}
}

// seedRun stores a finished run's view, an event, a step, a hook, and its
// token, and returns their item IDs.
func seedRun(t *testing.T, dynamo *fakeDynamo, runID, hookID, token string, retainUntil *time.Time) []string {
	t.Helper()
	now := time.Now()
	hook, err := hookItem(worlds.Hook{HookID: worlds.HookID(hookID), RunID: worlds.RunID(runID), Token: token, CreatedAt: now, UpdatedAt: now, TokenRetentionUntil: retainUntil})
	if err != nil {
		t.Fatal(err)
	}
	items := []map[string]ddb.AttributeValue{
		runItem(runView{Run: worlds.Run{RunID: worlds.RunID(runID), WorkflowName: "retention", Status: worlds.StatusCompleted, CreatedAt: now, UpdatedAt: now, CompletedAt: &now}}),
		eventItem(worlds.Event{RunID: worlds.RunID(runID), EventID: "evnt_1", EventType: worlds.EventTypeRunCreated, CreatedAt: now}, 1),
		stepItem(stepView{Step: worlds.Step{RunID: worlds.RunID(runID), StepID: "step", StepName: "step", Status: worlds.StatusCompleted, CreatedAt: now, UpdatedAt: now}}),
		hook,
		tokenItem(token, tokenView{hookID: worlds.HookID(hookID), runID: worlds.RunID(runID), retainUntil: retainUntil}),
	}
	ids := make([]string, len(items))
	for i, item := range items {
		ids[i] = itemID(item)
		dynamo.items[ids[i]] = item
	}
	return ids
}
