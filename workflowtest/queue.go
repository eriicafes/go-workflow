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
	stepID := worlds.StepID("step_queue")
	batch, err := world.QueueBatch(ctx, worlds.QueueBatchInput{Items: []worlds.QueueInput{{RunID: runID, NotBefore: &later}, {RunID: runID, StepID: &stepID, NotBefore: &later}}})
	if err != nil || len(batch) != 2 || batch[0].MessageID == "" || batch[0].MessageID == batch[1].MessageID {
		t.Fatalf("queue batch = %#v err=%v", batch, err)
	}
}
