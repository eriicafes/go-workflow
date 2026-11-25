package awsworld

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-lambda-go/events"
	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/eriicafes/go-workflow/worlds"
)

func TestDelaySeconds(t *testing.T) {
	tests := map[time.Duration]int32{
		-time.Second:                    0,
		0:                               0,
		time.Millisecond:                1,
		2 * time.Second:                 2,
		2*time.Second + time.Nanosecond: 3,
		15 * time.Minute:                900,
		time.Hour:                       900,
	}
	for delay, want := range tests {
		if got := delaySeconds(delay); got != want {
			t.Errorf("delaySeconds(%v) = %d, want %d", delay, got, want)
		}
	}
}

func TestConsumer(t *testing.T) {
	ctx := context.Background()
	dynamo, queue := newFakeDynamo(), &fakeSQS{}
	world := testWorld(t, dynamo, queue, nil)
	handler := Queue(world, worlds.Runners())

	now := time.Now().UTC()
	done := runView{Run: worlds.Run{RunID: "wrun_done", WorkflowName: "flow", Status: worlds.StatusCompleted, Attributes: map[string]string{}, CreatedAt: now, UpdatedAt: now, CompletedAt: &now}, eventCount: 2}
	dynamo.items[itemID(runKey("wrun_done"))] = runItem(done)

	succeeded, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_done"})
	if err != nil {
		t.Fatal(err)
	}
	failing, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_missing"})
	if err != nil {
		t.Fatal(err)
	}
	later := time.Now().Add(10 * time.Minute)
	early, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_done", NotBefore: &later})
	if err != nil {
		t.Fatal(err)
	}
	sentBefore := len(queue.sent())

	response, err := handler(ctx, events.SQSEvent{Records: []events.SQSMessage{
		sqsRecord(t, succeeded.MessageID, worlds.QueueInput{RunID: "wrun_done"}),
		sqsRecord(t, "cancelled", worlds.QueueInput{RunID: "wrun_done"}),
		sqsRecord(t, failing.MessageID, worlds.QueueInput{RunID: "wrun_missing"}),
		sqsRecord(t, early.MessageID, worlds.QueueInput{RunID: "wrun_done", NotBefore: &later}),
		{MessageId: "msg-garbage", Body: "not json"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	failed := map[string]bool{}
	for _, failure := range response.BatchItemFailures {
		failed[failure.ItemIdentifier] = true
	}
	if len(failed) != 1 || !failed["msg-"+failing.MessageID] {
		t.Fatalf("batch item failures = %#v, want only the failed delivery", response.BatchItemFailures)
	}
	// The failed delivery waits out a backoff for its third receive.
	var backoff []int32
	for _, change := range queue.visibility {
		if *change.ReceiptHandle == "rh-"+failing.MessageID {
			backoff = append(backoff, change.VisibilityTimeout)
		}
	}
	if len(backoff) != 1 || backoff[0] != 4 {
		t.Fatalf("visibility changes = %v, want a 4s backoff", backoff)
	}
	if dynamo.has(succeeded.MessageID) {
		t.Fatal("completed delivery left its queue record")
	}
	if !dynamo.has(failing.MessageID) || !dynamo.has(early.MessageID) {
		t.Fatal("failed or early delivery lost its queue record")
	}
	sent := queue.sent()
	if len(sent) != sentBefore+1 {
		t.Fatalf("early delivery re-sent %d messages", len(sent)-sentBefore)
	}
	if resent := sent[len(sent)-1]; resent.DelaySeconds < 590 || resent.DelaySeconds > 600 {
		t.Fatalf("re-sent delay = %d", resent.DelaySeconds)
	}
}

func TestConsumerLeavesRecordsToAnotherInvocationNearTheDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dynamo, queue := newFakeDynamo(), &fakeSQS{}
		world := testWorld(t, dynamo, queue, nil)
		now := time.Now().UTC()
		done := runView{Run: worlds.Run{RunID: "wrun_done", WorkflowName: "flow", Status: worlds.StatusCompleted, Attributes: map[string]string{}, CreatedAt: now, UpdatedAt: now, CompletedAt: &now}, eventCount: 2}
		dynamo.items[itemID(runKey("wrun_done"))] = runItem(done)
		var records []events.SQSMessage
		for range 3 {
			queued, err := world.Queue(t.Context(), worlds.QueueInput{RunID: "wrun_done"})
			if err != nil {
				t.Fatal(err)
			}
			records = append(records, sqsRecord(t, queued.MessageID, worlds.QueueInput{RunID: "wrun_done"}))
		}

		// The first delivery passes halfway to the deadline.
		dynamo.getDelay = 6 * time.Second
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		response, err := Queue(world, worlds.Runners())(ctx, events.SQSEvent{Records: records})
		if err != nil {
			t.Fatal(err)
		}
		if len(response.BatchItemFailures) != 2 || response.BatchItemFailures[0].ItemIdentifier != records[1].MessageId || response.BatchItemFailures[1].ItemIdentifier != records[2].MessageId {
			t.Fatalf("batch item failures = %#v, want the records after the first", response.BatchItemFailures)
		}
		var visibility []int32
		for _, change := range queue.visibility {
			if *change.ReceiptHandle != records[0].ReceiptHandle {
				visibility = append(visibility, change.VisibilityTimeout)
			}
		}
		if len(visibility) != 2 || visibility[0] != 0 || visibility[1] != 0 {
			t.Fatalf("visibility changes = %v, want the deferred records visible at once", visibility)
		}
	})
}

func TestConsumerStoreError(t *testing.T) {
	dynamo := newFakeDynamo()
	dynamo.getErr = errors.New("throttled")
	world := testWorld(t, dynamo, &fakeSQS{}, nil)
	response, err := Queue(world, worlds.Runners())(context.Background(), events.SQSEvent{Records: []events.SQSMessage{sqsRecord(t, "id", worlds.QueueInput{RunID: "wrun"})}})
	if err != nil || len(response.BatchItemFailures) != 1 || response.BatchItemFailures[0].ItemIdentifier != "msg-id" {
		t.Fatalf("response = %#v err=%v", response, err)
	}
}

func sqsRecord(t *testing.T, id string, input worlds.QueueInput) events.SQSMessage {
	t.Helper()
	body, err := json.Marshal(queueMessage{ID: id, Input: input})
	if err != nil {
		t.Fatal(err)
	}
	return events.SQSMessage{MessageId: "msg-" + id, ReceiptHandle: "rh-" + id, Body: string(body), Attributes: map[string]string{"ApproximateReceiveCount": "3"}}
}

func TestRetryBackoffGrowsWithReceives(t *testing.T) {
	for receives, want := range map[string]time.Duration{"": time.Second, "1": time.Second, "2": 2 * time.Second, "5": 16 * time.Second, "40": maxRetryBackoff} {
		if got := retryBackoff(receives); got != want {
			t.Errorf("retryBackoff(%q) = %s, want %s", receives, got, want)
		}
	}
}

func TestConsumerWaitsForAThrottle(t *testing.T) {
	dynamo, queue := newFakeDynamo(), &fakeSQS{}
	dynamo.getErr = &worlds.ThrottleError{After: 2 * time.Minute}
	world := testWorld(t, dynamo, queue, nil)
	if _, err := Queue(world, worlds.Runners())(context.Background(), events.SQSEvent{Records: []events.SQSMessage{sqsRecord(t, "id", worlds.QueueInput{RunID: "wrun"})}}); err != nil {
		t.Fatal(err)
	}
	// The third receive would back off 4s; the throttle asked for 2m.
	if len(queue.visibility) != 1 || queue.visibility[0].VisibilityTimeout != 120 {
		t.Fatalf("visibility = %+v", queue.visibility)
	}
}

// fakeExecutor records the deliveries it executes and returns its results in
// turn, then nil.
type fakeExecutor struct {
	inputs  []worlds.QueueInput
	results []error
}

func (f *fakeExecutor) Execute(_ context.Context, input worlds.QueueInput) error {
	f.inputs = append(f.inputs, input)
	if len(f.results) == 0 {
		return nil
	}
	err := f.results[0]
	f.results = f.results[1:]
	return err
}

// lastBody returns the body of the last message sent.
func lastBody(t *testing.T, queue *fakeSQS) string {
	t.Helper()
	sent := queue.sent()
	if len(sent) == 0 {
		t.Fatal("nothing sent")
	}
	return awsv2.ToString(sent[len(sent)-1].MessageBody)
}

func TestRedeliveryKeepsTheMessageAndCountsAttempts(t *testing.T) {
	ctx := context.Background()
	dynamo, queue := newFakeDynamo(), &fakeSQS{}
	world := testWorld(t, dynamo, queue, nil)
	stepID := worlds.StepID("step_1")
	queued, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_a", StepID: &stepID})
	if err != nil {
		t.Fatal(err)
	}
	runtime := &fakeExecutor{results: []error{worlds.Redeliver(0), worlds.Redeliver(time.Minute), errors.New("failed")}}

	// A handoff is sent again at once.
	if err := world.deliver(ctx, runtime, lastBody(t, queue)); err != nil {
		t.Fatal(err)
	}
	if sent := queue.sent(); len(sent) != 2 || sent[1].DelaySeconds != 0 {
		t.Fatalf("handoff sent %+v", sent)
	}
	// A redelivery after a delay is sent again with it and due then.
	if err := world.deliver(ctx, runtime, lastBody(t, queue)); err != nil {
		t.Fatal(err)
	}
	if sent := queue.sent(); len(sent) != 3 || sent[2].DelaySeconds < 59 || sent[2].DelaySeconds > 60 {
		t.Fatalf("redelivery sent %+v", sent)
	}
	record, _, _ := world.queueRecord(ctx, queued.MessageID)
	if record.notBefore == nil || time.Until(*record.notBefore) < 59*time.Second {
		t.Fatalf("record notBefore = %v, want a minute ahead", record.notBefore)
	}
	// Arriving early, it is sent again without executing.
	if err := world.deliver(ctx, runtime, lastBody(t, queue)); err != nil || len(runtime.inputs) != 2 || len(queue.sent()) != 4 {
		t.Fatalf("early arrival: err=%v, %d executions, %d sends", err, len(runtime.inputs), len(queue.sent()))
	}
	dynamo.items[itemID(queueKey(queued.MessageID))]["notBefore"] = avs(stamp(time.Now().Add(-time.Second)))
	// A failure stays with SQS, and the next delivery succeeds.
	if err := world.deliver(ctx, runtime, lastBody(t, queue)); err == nil {
		t.Fatal("the failed delivery reported no error")
	}
	if err := world.deliver(ctx, runtime, lastBody(t, queue)); err != nil {
		t.Fatal(err)
	}
	if dynamo.has(queued.MessageID) {
		t.Fatal("the completed delivery kept its record")
	}
	want := [][2]int{{1, 1}, {2, 1}, {3, 2}, {4, 3}}
	if len(runtime.inputs) != len(want) {
		t.Fatalf("%d executions, want %d", len(runtime.inputs), len(want))
	}
	for i, input := range runtime.inputs {
		if input.MessageID != queued.MessageID || input.DeliveryCount != want[i][0] || input.Attempt != want[i][1] {
			t.Errorf("execution %d: message %s, DeliveryCount %d, Attempt %d; want %s, %v", i, input.MessageID, input.DeliveryCount, input.Attempt, queued.MessageID, want[i])
		}
	}
	// A delivery whose record is gone is acknowledged without executing.
	if err := world.deliver(ctx, runtime, lastBody(t, queue)); err != nil || len(runtime.inputs) != len(want) {
		t.Fatalf("delivery after acknowledgement: err=%v, %d executions", err, len(runtime.inputs))
	}
}

func TestDeliverCapsALongRedeliveryWithoutScheduler(t *testing.T) {
	ctx := context.Background()
	dynamo, queue := newFakeDynamo(), &fakeSQS{}
	world := testWorld(t, dynamo, queue, nil)
	stepID := worlds.StepID("step_1")
	queued, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_a", StepID: &stepID})
	if err != nil {
		t.Fatal(err)
	}
	if err := world.deliver(ctx, &fakeExecutor{results: []error{worlds.Redeliver(48 * time.Hour)}}, lastBody(t, queue)); err != nil {
		t.Fatal(err)
	}
	if sent := queue.sent(); sent[len(sent)-1].DelaySeconds != 900 {
		t.Fatalf("delay = %d, want SQS's longest", sent[len(sent)-1].DelaySeconds)
	}
	if record, _, _ := world.queueRecord(ctx, queued.MessageID); record.notBefore == nil || time.Until(*record.notBefore) < 47*time.Hour {
		t.Fatalf("record notBefore = %v, want the full delay", record.notBefore)
	}
}

func TestDeliverSchedulesALongRedelivery(t *testing.T) {
	ctx := context.Background()
	dynamo, queue, schedules := newFakeDynamo(), &fakeSQS{}, newFakeScheduler()
	world := testWorld(t, dynamo, queue, schedules)
	stepID := worlds.StepID("step_1")
	queued, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_a", StepID: &stepID})
	if err != nil {
		t.Fatal(err)
	}
	if err := world.deliver(ctx, &fakeExecutor{results: []error{worlds.Redeliver(48 * time.Hour)}}, lastBody(t, queue)); err != nil {
		t.Fatal(err)
	}
	if len(schedules.schedules) != 1 || len(queue.sent()) != 1 {
		t.Fatalf("%d schedules, %d sends; want the redelivery scheduled", len(schedules.schedules), len(queue.sent()))
	}
	for _, schedule := range schedules.schedules {
		if !strings.Contains(awsv2.ToString(schedule.Target.Input), queued.MessageID) {
			t.Fatalf("schedule input = %s", awsv2.ToString(schedule.Target.Input))
		}
	}
}

func TestQueueDropsAMessagePastItsRetention(t *testing.T) {
	ctx := context.Background()
	dynamo, queue := newFakeDynamo(), &fakeSQS{}
	world := testWorld(t, dynamo, queue, nil)
	stepID := worlds.StepID("step_1")
	queued, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_a", StepID: &stepID, Retention: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	dynamo.items[itemID(queueKey(queued.MessageID))]["expiresAt"] = avs(stamp(time.Now().Add(-time.Second)))
	runtime := &fakeExecutor{}
	if err := world.deliver(ctx, runtime, lastBody(t, queue)); err != nil || len(runtime.inputs) != 0 || dynamo.has(queued.MessageID) {
		t.Fatalf("err=%v, %d executions, record kept %v", err, len(runtime.inputs), dynamo.has(queued.MessageID))
	}
}

func TestConsumerReportsNoFailureForARedelivery(t *testing.T) {
	ctx := context.Background()
	dynamo, queue := newFakeDynamo(), &fakeSQS{}
	world := testWorld(t, dynamo, queue, nil)
	stepID := worlds.StepID("step_1")
	if _, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_a", StepID: &stepID}); err != nil {
		t.Fatal(err)
	}
	var message queueMessage
	if err := json.Unmarshal([]byte(lastBody(t, queue)), &message); err != nil {
		t.Fatal(err)
	}
	response := world.consume(ctx, &fakeExecutor{results: []error{worlds.Redeliver(time.Minute)}}, events.SQSEvent{Records: []events.SQSMessage{sqsRecord(t, message.ID, message.Input)}})
	if len(response.BatchItemFailures) != 0 || len(queue.visibility) != 0 {
		t.Fatalf("response = %+v, visibility changes %+v; want the message acknowledged and sent again", response, queue.visibility)
	}
}
