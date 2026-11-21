package awsworld

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/eriicafes/go-workflow/worlds"
)

func TestCapabilitiesReportTheQueueContract(t *testing.T) {
	without := testWorld(t, newFakeDynamo(), &fakeSQS{}, nil).Capabilities()
	if !without.InBandFence || !without.MaxConcurrency || without.MaxQueueDelay != 15*time.Minute {
		t.Fatalf("capabilities without Scheduler = %+v", without)
	}
	with := testWorld(t, newFakeDynamo(), &fakeSQS{}, newFakeScheduler()).Capabilities()
	if !with.InBandFence || !with.MaxConcurrency || with.MaxQueueDelay != 0 {
		t.Fatalf("capabilities with Scheduler = %+v", with)
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
		StepName: "step", StepAttempt: 3, StepResent: true, RunSpecVersion: worlds.SpecVersion,
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
