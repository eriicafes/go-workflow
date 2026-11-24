package workflowtest

import (
	"context"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

func conformQueue(t *testing.T, world worlds.World) {
	ctx := context.Background()
	runID := conformCreateRun(t, world, "queue")
	later := time.Now().Add(time.Hour)
	result, err := world.Queue(ctx, worlds.QueueInput{RunID: runID, NotBefore: &later, IdempotencyKey: "conformance"})
	if err != nil || result.MessageID == "" {
		t.Fatalf("queue = %#v err=%v", result, err)
	}
	// A second message with the key is absorbed while the first lives, alone
	// or in a batch, and the World answers with the first one's ID.
	again, err := world.Queue(ctx, worlds.QueueInput{RunID: runID, NotBefore: &later, IdempotencyKey: "conformance"})
	if err != nil || again.MessageID != result.MessageID {
		t.Fatalf("queue with a repeated key = %#v err=%v, want message %s", again, err, result.MessageID)
	}
	keyed, err := world.QueueBatch(ctx, worlds.QueueBatchInput{Items: []worlds.QueueInput{{RunID: runID, NotBefore: &later, IdempotencyKey: "conformance"}, {RunID: runID, NotBefore: &later, IdempotencyKey: "conformance-other"}}})
	if err != nil || len(keyed) != 2 || keyed[0].MessageID != result.MessageID || keyed[1].MessageID == "" || keyed[1].MessageID == result.MessageID {
		t.Fatalf("batch with a repeated key = %#v err=%v, want message %s first", keyed, err, result.MessageID)
	}
	// Without a key, every message is new.
	a, errA := world.Queue(ctx, worlds.QueueInput{RunID: runID, NotBefore: &later})
	b, errB := world.Queue(ctx, worlds.QueueInput{RunID: runID, NotBefore: &later})
	if errA != nil || errB != nil || a.MessageID == "" || a.MessageID == b.MessageID {
		t.Fatalf("unkeyed = %q, %q err=%v %v, want two messages", a.MessageID, b.MessageID, errA, errB)
	}
	stepID := worlds.StepID("step_queue")
	batch, err := world.QueueBatch(ctx, worlds.QueueBatchInput{Items: []worlds.QueueInput{{RunID: runID, NotBefore: &later}, {RunID: runID, StepID: &stepID, NotBefore: &later}}})
	if err != nil || len(batch) != 2 || batch[0].MessageID == "" || batch[0].MessageID == batch[1].MessageID {
		t.Fatalf("queue batch = %#v err=%v", batch, err)
	}
}
