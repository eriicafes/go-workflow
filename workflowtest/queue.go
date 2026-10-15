package workflowtest

import (
	"context"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

func conformQueue(t *testing.T, world worlds.World) {
	ctx := context.Background()
	conformExpect(t, world.QueueCancel(ctx, worlds.QueueCancelInput{MessageID: "missing"}), worlds.ErrQueueMessageNotFound, "cancel missing message")
	runID := conformCreateRun(t, world, "queue")
	later := time.Now().Add(time.Hour)
	result, err := world.Queue(ctx, worlds.QueueInput{RunID: runID, NotBefore: &later, IdempotencyKey: "conformance"})
	if err != nil || result.MessageID == "" {
		t.Fatalf("queue = %#v err=%v", result, err)
	}
	if err := world.QueueCancel(ctx, worlds.QueueCancelInput{MessageID: result.MessageID}); err != nil {
		t.Fatalf("cancel queued message: %v", err)
	}
	conformExpect(t, world.QueueCancel(ctx, worlds.QueueCancelInput{MessageID: result.MessageID}), worlds.ErrQueueMessageNotFound, "cancel cancelled message")
	stepID := worlds.StepID("step_queue")
	batch, err := world.QueueBatch(ctx, worlds.QueueBatchInput{Items: []worlds.QueueInput{{RunID: runID, NotBefore: &later}, {RunID: runID, StepID: &stepID, NotBefore: &later}}})
	if err != nil || len(batch) != 2 || batch[0].MessageID == "" || batch[0].MessageID == batch[1].MessageID {
		t.Fatalf("queue batch = %#v err=%v", batch, err)
	}
	for _, item := range batch {
		if err := world.QueueCancel(ctx, worlds.QueueCancelInput{MessageID: item.MessageID}); err != nil {
			t.Fatalf("cancel batch message: %v", err)
		}
	}
}
