package awsworld

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
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
