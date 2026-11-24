package awsworld

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/worlds"
)

func TestCapabilitiesReportTheQueueContract(t *testing.T) {
	without := testWorld(t, newFakeDynamo(), &fakeSQS{}, nil).Capabilities()
	if without.MaxQueueDelay != 15*time.Minute {
		t.Fatalf("capabilities without Scheduler = %+v", without)
	}
	with := testWorld(t, newFakeDynamo(), &fakeSQS{}, newFakeScheduler()).Capabilities()
	if with.MaxQueueDelay != 0 {
		t.Fatalf("capabilities with Scheduler = %+v", with)
	}
}

// A delivery due beyond SQS's longest delay is accepted without Scheduler,
// as a Workflow SDK runtime queues one through world-http: it is sent with
// that delay, and each early arrival sends it again until it is due.
func TestQueueHoldsALongDelayWithoutScheduler(t *testing.T) {
	ctx := context.Background()
	dynamo, queue := newFakeDynamo(), &fakeSQS{}
	world := testWorld(t, dynamo, queue, nil)
	later := time.Now().Add(time.Hour)
	result, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_later", NotBefore: &later})
	if err != nil {
		t.Fatal(err)
	}
	sent := queue.sent()
	if len(sent) != 1 || sent[0].DelaySeconds != int32(maxSQSDelay/time.Second) {
		t.Fatalf("sent = %d messages, first delayed %ds, want one delayed %ds", len(sent), sent[0].DelaySeconds, int32(maxSQSDelay/time.Second))
	}
	executor := &recordingExecutor{}
	response, err := Queue(world, executor)(ctx, events.SQSEvent{Records: []events.SQSMessage{sqsRecord(t, result.MessageID, worlds.QueueInput{RunID: "wrun_later", NotBefore: &later})}})
	if err != nil || len(response.BatchItemFailures) != 0 {
		t.Fatalf("early arrival: response = %+v, err = %v", response, err)
	}
	if len(executor.inputs) != 0 {
		t.Fatal("an early arrival was executed")
	}
	if sent := queue.sent(); len(sent) != 2 || sent[1].DelaySeconds != int32(maxSQSDelay/time.Second) {
		t.Fatalf("early arrival re-sent %d messages, want one more delayed %ds", len(queue.sent())-1, int32(maxSQSDelay/time.Second))
	}
}

func TestQueueDeduplicatesByIdempotencyKey(t *testing.T) {
	ctx := context.Background()
	dynamo, queue := newFakeDynamo(), &fakeSQS{}
	world := testWorld(t, dynamo, queue, nil)

	first, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_a", IdempotencyKey: "key"})
	if err != nil {
		t.Fatal(err)
	}
	again, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_a", IdempotencyKey: "key"})
	if err != nil {
		t.Fatal(err)
	}
	if again.MessageID != first.MessageID || len(queue.sent()) != 1 {
		t.Fatalf("repeat = %s, first %s, %d sends; want the first message once", again.MessageID, first.MessageID, len(queue.sent()))
	}
	other, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_b", IdempotencyKey: "key"})
	if err != nil || other.MessageID == first.MessageID {
		t.Fatalf("another run's key = %s, %v; want its own message", other.MessageID, err)
	}
	for range 2 {
		if _, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_a"}); err != nil {
			t.Fatal(err)
		}
	}
	if len(queue.sent()) != 4 {
		t.Fatalf("%d sends, want deliveries without a key sent every time", len(queue.sent()))
	}

	// Once the first message is acknowledged, its key queues a new one.
	if err := world.deleteQueueRecord(ctx, first.MessageID); err != nil {
		t.Fatal(err)
	}
	next, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_a", IdempotencyKey: "key"})
	if err != nil || next.MessageID == first.MessageID || !dynamo.has(next.MessageID) {
		t.Fatalf("key after acknowledgement = %s, %v; want a new message", next.MessageID, err)
	}
	if read(dynamo.items[itemID(dedupeKey("wrun_a", "key"))], "messageId") != next.MessageID {
		t.Fatal("the new message does not hold the key")
	}

	// A claim past its window is taken over even while its message lives.
	dynamo.items[itemID(dedupeKey("wrun_a", "key"))]["dedupeUntil"] = avn(time.Now().Add(-time.Second).UnixMilli())
	late, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_a", IdempotencyKey: "key"})
	if err != nil || late.MessageID == next.MessageID {
		t.Fatalf("key past its window = %s, %v; want a new message", late.MessageID, err)
	}
}

func TestQueueReleasesTheKeyWhenPublishingFails(t *testing.T) {
	ctx := context.Background()
	dynamo, queue := newFakeDynamo(), &fakeSQS{err: errors.New("sqs down")}
	world := testWorld(t, dynamo, queue, nil)
	if _, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_a", IdempotencyKey: "key"}); err == nil {
		t.Fatal("queue succeeded while SQS failed")
	}
	if dynamo.count() != 0 {
		t.Fatalf("a failed publish left %d items", dynamo.count())
	}
	queue.err = nil
	result, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_a", IdempotencyKey: "key"})
	if err != nil || !dynamo.has(result.MessageID) || len(queue.sent()) != 1 {
		t.Fatalf("retry = %+v, %v", result, err)
	}
}

// fullInput sets every QueueInput field the queue carries.
func fullInput() worlds.QueueInput {
	stepID := worlds.StepID("step_1")
	resumeAt := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	hopAt := resumeAt.Add(-time.Hour)
	return worlds.QueueInput{
		RunID: "wrun_full", QueueName: "q", WorkflowName: "flow", StepID: &stepID,
		IdempotencyKey: "key", DeploymentID: "dpl", DeploymentMismatchRetryCount: 1, ReplayDivergenceCount: 2,
		StepName: "step", StepAttempt: 3, StepResent: true, StepCreatedAt: hopAt, RunSpecVersion: worlds.SpecVersion,
		CompleteWaits:    []string{"wait_1"},
		Recovery:         true,
		WaitContinuation: &worlds.WaitContinuation{CorrelationID: "wait_1", Attempt: 2, ResumeAt: &resumeAt, HopAt: &hopAt},
		Retention:        worlds.StepRetention,
	}
}

func TestQueueRoundTripsEveryField(t *testing.T) {
	ctx := context.Background()
	dynamo, queue, schedules := newFakeDynamo(), &fakeSQS{}, newFakeScheduler()
	world := testWorld(t, dynamo, queue, schedules)
	input := fullInput()
	result, err := world.Queue(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	record, _, err := world.queueRecord(ctx, result.MessageID)
	// The World stamps when the message was queued.
	if err == nil && record.input.CreatedAt.IsZero() {
		t.Fatal("record has no CreatedAt")
	}
	input.CreatedAt = record.input.CreatedAt
	if err != nil || !reflect.DeepEqual(record.input, input) {
		t.Fatalf("record input = %+v, %v; want %+v", record.input, err, input)
	}
	var message queueMessage
	if err := json.Unmarshal([]byte(awsv2.ToString(queue.sent()[0].MessageBody)), &message); err != nil || !reflect.DeepEqual(message.Input, input) {
		t.Fatalf("message input = %+v, %v", message.Input, err)
	}
	if record.expiresAt == nil || record.expiresAt.Before(time.Now().Add(worlds.StepRetention-time.Minute)) {
		t.Fatalf("expiresAt = %v, want the message's retention from now", record.expiresAt)
	}

	// A schedule carries the input when it fits, and the ID alone otherwise.
	later := time.Now().Add(time.Hour)
	input.NotBefore, input.IdempotencyKey = &later, ""
	scheduled, err := world.Queue(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	body := awsv2.ToString(schedules.schedules[scheduleName(scheduled.MessageID)].Target.Input)
	if err := json.Unmarshal([]byte(body), &message); err != nil || message.ID != scheduled.MessageID || !reflect.DeepEqual(message.Input.CompleteWaits, input.CompleteWaits) || message.Input.WaitContinuation == nil {
		t.Fatalf("schedule body = %s, %v", body, err)
	}
	input.CompleteWaits = nil
	for len(input.CompleteWaits) < maxScheduleInput/8 {
		input.CompleteWaits = append(input.CompleteWaits, fmt.Sprintf("wait_%d", len(input.CompleteWaits)))
	}
	large, err := world.Queue(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	body = awsv2.ToString(schedules.schedules[scheduleName(large.MessageID)].Target.Input)
	if len(body) > maxScheduleInput || json.Unmarshal([]byte(body), &message) != nil || message.ID != large.MessageID {
		t.Fatalf("large schedule body of %d bytes", len(body))
	}
	if record, _, _ := world.queueRecord(ctx, large.MessageID); !reflect.DeepEqual(record.input.CompleteWaits, input.CompleteWaits) {
		t.Fatal("the record lost the large input")
	}
}

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

// recordingExecutor records the deliveries it executes.
type recordingExecutor struct {
	inputs []worlds.QueueInput
}

func (e *recordingExecutor) Execute(_ context.Context, _ worlds.Runtime, input worlds.QueueInput) error {
	e.inputs = append(e.inputs, input)
	return nil
}
