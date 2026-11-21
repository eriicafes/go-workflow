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
	// A second message with the key is absorbed while the first lives.
	if _, err := world.Queue(ctx, worlds.QueueInput{RunID: runID, NotBefore: &later, IdempotencyKey: "conformance"}); err != nil {
		t.Fatalf("queue with a repeated key: %v", err)
	}
	if !world.Capabilities().InBandFence {
		t.Fatal("Capabilities().InBandFence = false, but every World fences in-band writes")
	}
	stepID := worlds.StepID("step_queue")
	batch, err := world.QueueBatch(ctx, worlds.QueueBatchInput{Items: []worlds.QueueInput{{RunID: runID, NotBefore: &later}, {RunID: runID, StepID: &stepID, NotBefore: &later}}})
	if err != nil || len(batch) != 2 || batch[0].MessageID == "" || batch[0].MessageID == batch[1].MessageID {
		t.Fatalf("queue batch = %#v err=%v", batch, err)
	}
}
