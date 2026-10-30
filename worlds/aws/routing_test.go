package awsworld

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/worlds"
)

type routedWorkflow struct {
	workflow.Struct[struct{}]
}

type orderedWorkflow struct {
	workflow.Struct[struct{}]
}

const (
	backfillURL = "https://sqs.us-east-1.amazonaws.com/000000000000/backfill"
	backfillARN = "arn:aws:sqs:us-east-1:000000000000:backfill"
	orderedURL  = "https://sqs.us-east-1.amazonaws.com/000000000000/ordered.fifo"
	orderedARN  = "arn:aws:sqs:us-east-1:000000000000:ordered.fifo"
	schedARN    = "arn:aws:iam::000000000000:role/scheduler"
	defaultARN  = "arn:aws:sqs:us-east-1:000000000000:workflow"
)

func routingWorld(t *testing.T, queue *fakeSQS, schedules *fakeScheduler) *World {
	t.Helper()
	world, err := newWorld(Config{
		BaseURL:   testBaseURL,
		TableName: "workflow",
		QueueURL:  testQueueURL,
		Queues: map[string]QueueConfig{
			"backfill": {URL: backfillURL, ARN: backfillARN},
			"ordered":  {URL: orderedURL, ARN: orderedARN, FIFO: true},
		},
		Routes: worlds.Routes(
			worlds.Route[routedWorkflow]("backfill"),
			worlds.Route[*orderedWorkflow]("ordered"),
		),
		Scheduler: &SchedulerConfig{TargetARN: defaultARN, RoleARN: schedARN},
	}, newFakeDynamo(), queue, schedules)
	if err != nil {
		t.Fatal(err)
	}
	return world
}

func TestRoutesNameWorkflowsByType(t *testing.T) {
	routes := worlds.Routes(worlds.Route[routedWorkflow]("backfill"), worlds.Route[*orderedWorkflow]("ordered"))
	if routes[worlds.WorkflowName[routedWorkflow]()] != "backfill" || routes[worlds.WorkflowName[orderedWorkflow]()] != "ordered" || len(routes) != 2 {
		t.Fatalf("routes = %v", routes)
	}
}

func TestQueueRoutesByWorkflow(t *testing.T) {
	ctx := context.Background()
	queue := &fakeSQS{}
	world := routingWorld(t, queue, newFakeScheduler())

	if _, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_routed", WorkflowName: worlds.WorkflowName[routedWorkflow]()}); err != nil {
		t.Fatal(err)
	}
	if _, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_other", WorkflowName: "unrouted"}); err != nil {
		t.Fatal(err)
	}
	later := time.Now().Add(time.Minute)
	if _, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_routed", WorkflowName: worlds.WorkflowName[routedWorkflow](), NotBefore: &later}); err != nil {
		t.Fatal(err)
	}

	sent := queue.sent()
	if len(sent) != 3 {
		t.Fatalf("sent %d messages, want 3", len(sent))
	}
	if url := awsv2.ToString(sent[0].QueueUrl); url != backfillURL {
		t.Errorf("routed delivery went to %s", url)
	}
	if url := awsv2.ToString(sent[1].QueueUrl); url != testQueueURL {
		t.Errorf("unrouted delivery went to %s", url)
	}
	// a standard routed queue keeps the SQS delay
	if url, delay := awsv2.ToString(sent[2].QueueUrl), sent[2].DelaySeconds; url != backfillURL || delay < 59 || delay > 60 {
		t.Errorf("delayed routed delivery went to %s with delay %d", url, delay)
	}
	for _, message := range sent {
		if message.MessageGroupId != nil || message.MessageDeduplicationId != nil {
			t.Errorf("standard queue message has FIFO attributes: %+v", message)
		}
	}
}

func TestQueueSchedulesLongDelaysToTheRoutedQueue(t *testing.T) {
	schedules := newFakeScheduler()
	world := routingWorld(t, &fakeSQS{}, schedules)
	later := time.Now().Add(time.Hour)
	result, err := world.Queue(context.Background(), worlds.QueueInput{RunID: "wrun_routed", WorkflowName: worlds.WorkflowName[routedWorkflow](), NotBefore: &later})
	if err != nil {
		t.Fatal(err)
	}
	schedule := schedules.schedules[scheduleName(result.MessageID)]
	if schedule == nil || awsv2.ToString(schedule.Target.Arn) != backfillARN || schedule.Target.SqsParameters != nil {
		t.Fatalf("schedule = %+v, want one targeting the backfill queue", schedule)
	}
}

func TestQueueFIFO(t *testing.T) {
	ctx := context.Background()
	queue, schedules := &fakeSQS{}, newFakeScheduler()
	world := routingWorld(t, queue, schedules)
	name := worlds.WorkflowName[orderedWorkflow]()

	now, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_ordered", WorkflowName: name})
	if err != nil {
		t.Fatal(err)
	}
	sent := queue.sent()
	if len(sent) != 1 {
		t.Fatalf("sent %d messages, want 1", len(sent))
	}
	message := sent[0]
	if awsv2.ToString(message.QueueUrl) != orderedURL || awsv2.ToString(message.MessageGroupId) != "wrun_ordered" || awsv2.ToString(message.MessageDeduplicationId) != now.MessageID || message.DelaySeconds != 0 {
		t.Fatalf("FIFO message = %+v", message)
	}

	// a delayed delivery, even a short one, goes through Scheduler in the run's group
	soon := time.Now().Add(2 * time.Second)
	delayed, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_ordered", WorkflowName: name, NotBefore: &soon})
	if err != nil {
		t.Fatal(err)
	}
	if len(queue.sent()) != 1 {
		t.Fatal("a delayed FIFO delivery was sent with SQS")
	}
	schedule := schedules.schedules[scheduleName(delayed.MessageID)]
	assertFIFOSchedule(t, schedule, delayed.MessageID)
}

func TestConsumerResendsAnEarlyDeliveryToItsQueue(t *testing.T) {
	ctx := context.Background()
	queue, schedules := &fakeSQS{}, newFakeScheduler()
	world := routingWorld(t, queue, schedules)
	handler := Queue(world, worlds.Runners())

	later := time.Now().Add(10 * time.Minute)
	standard := worlds.QueueInput{RunID: "wrun_routed", WorkflowName: worlds.WorkflowName[routedWorkflow](), NotBefore: &later}
	standardResult, err := world.Queue(ctx, standard)
	if err != nil {
		t.Fatal(err)
	}
	fifo := worlds.QueueInput{RunID: "wrun_ordered", WorkflowName: worlds.WorkflowName[orderedWorkflow](), NotBefore: &later}
	fifoResult, err := world.Queue(ctx, fifo)
	if err != nil {
		t.Fatal(err)
	}
	sentBefore, schedulesBefore := len(queue.sent()), len(schedules.schedules)

	early := sqsRecord(t, standardResult.MessageID, standard)
	early.EventSourceARN = backfillARN
	earlyFIFO := sqsRecord(t, fifoResult.MessageID, fifo)
	earlyFIFO.EventSourceARN = orderedARN
	response, err := handler(ctx, events.SQSEvent{Records: []events.SQSMessage{early, earlyFIFO}})
	if err != nil || len(response.BatchItemFailures) != 0 {
		t.Fatalf("response = %+v, err = %v", response, err)
	}

	sent := queue.sent()
	if len(sent) != sentBefore+1 || awsv2.ToString(sent[len(sent)-1].QueueUrl) != backfillURL {
		t.Fatalf("standard early delivery was not sent again to its queue: %+v", sent[sentBefore:])
	}
	if len(schedules.schedules) != schedulesBefore+1 {
		t.Fatalf("FIFO early delivery created %d schedules, want 1", len(schedules.schedules)-schedulesBefore)
	}
	for scheduleName, schedule := range schedules.schedules {
		if !strings.Contains(awsv2.ToString(schedule.Target.Input), fifoResult.MessageID) || scheduleName == "go-workflow-"+fifoResult.MessageID {
			continue
		}
		assertFIFOSchedule(t, schedule, fifoResult.MessageID)
		var resent, first queueMessage
		_ = json.Unmarshal([]byte(awsv2.ToString(schedule.Target.Input)), &resent)
		_ = json.Unmarshal([]byte(awsv2.ToString(schedules.schedules["go-workflow-"+fifoResult.MessageID].Target.Input)), &first)
		if resent.Publish == "" || resent.Publish == first.Publish {
			t.Fatalf("re-sent FIFO body publish = %q, first %q; want distinct bodies for content-based deduplication", resent.Publish, first.Publish)
		}
	}
}

func TestConsumerChangesVisibilityOnTheSourceQueue(t *testing.T) {
	queue := &fakeSQS{}
	world := routingWorld(t, queue, newFakeScheduler())
	record := sqsRecord(t, "missing", worlds.QueueInput{RunID: "wrun"})
	record.EventSourceARN = backfillARN
	// a record whose queue item is gone is acknowledged; force a failure with a broken store instead
	dynamo := newFakeDynamo()
	dynamo.getErr = context.DeadlineExceeded
	world.dynamo = dynamo
	if _, err := Queue(world, worlds.Runners())(context.Background(), events.SQSEvent{Records: []events.SQSMessage{record}}); err != nil {
		t.Fatal(err)
	}
	if len(queue.visibility) == 0 || awsv2.ToString(queue.visibility[len(queue.visibility)-1].QueueUrl) != backfillURL {
		t.Fatalf("visibility changes = %+v, want them on the backfill queue", queue.visibility)
	}
}

func TestNewValidatesQueues(t *testing.T) {
	scheduler := &SchedulerConfig{TargetARN: defaultARN, RoleARN: schedARN}
	tests := map[string]Config{
		"unknown route queue": {Routes: worlds.Routes(worlds.Route[routedWorkflow]("missing"))},
		"queue without URL":   {Queues: map[string]QueueConfig{"backfill": {ARN: backfillARN}}},
		"FIFO without Scheduler": {
			Queues: map[string]QueueConfig{"ordered": {URL: orderedURL, ARN: orderedARN, FIFO: true}},
		},
		"queue without ARN under Scheduler": {
			Queues:    map[string]QueueConfig{"backfill": {URL: backfillURL}},
			Scheduler: scheduler,
		},
	}
	for name, config := range tests {
		config.BaseURL, config.TableName, config.QueueURL = testBaseURL, "workflow", testQueueURL
		var schedules schedulerAPI
		if config.Scheduler != nil {
			schedules = newFakeScheduler()
		}
		if _, err := newWorld(config, newFakeDynamo(), &fakeSQS{}, schedules); err == nil {
			t.Errorf("%s: New accepted the config", name)
		}
	}
	// a standard queue needs no ARN without Scheduler
	if _, err := newWorld(Config{BaseURL: testBaseURL, TableName: "workflow", QueueURL: testQueueURL, Queues: map[string]QueueConfig{"backfill": {URL: backfillURL}}, Routes: worlds.Routes(worlds.Route[routedWorkflow]("backfill"))}, newFakeDynamo(), &fakeSQS{}, nil); err != nil {
		t.Fatalf("standard queue without Scheduler: %v", err)
	}
}

func assertFIFOSchedule(t *testing.T, schedule *scheduler.CreateScheduleInput, messageID string) {
	t.Helper()
	if schedule == nil {
		t.Fatal("no schedule for the delayed FIFO delivery")
	}
	if awsv2.ToString(schedule.Target.Arn) != orderedARN || schedule.Target.SqsParameters == nil || awsv2.ToString(schedule.Target.SqsParameters.MessageGroupId) != "wrun_ordered" {
		t.Fatalf("schedule target = %+v, want the FIFO queue in the run's group", schedule.Target)
	}
	if !strings.Contains(awsv2.ToString(schedule.Target.Input), messageID) {
		t.Fatalf("schedule input = %s, want delivery %s", awsv2.ToString(schedule.Target.Input), messageID)
	}
}
