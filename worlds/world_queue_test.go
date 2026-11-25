package worlds_test

import (
	"context"
	"errors"
	"testing"

	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/testworld"
)

// refusingQueue refuses the deliveries of the run refuse names with cause.
type refusingQueue struct {
	*testworld.World
	refuse worlds.RunID
	cause  error
}

func (q refusingQueue) Queue(ctx context.Context, input worlds.QueueInput) (worlds.QueueResult, error) {
	if input.RunID == q.refuse {
		return worlds.QueueResult{}, q.cause
	}
	return q.World.Queue(ctx, input)
}

func (q refusingQueue) QueueBatch(ctx context.Context, input worlds.QueueBatchInput) ([]worlds.QueueResult, error) {
	return worlds.QueueEach(ctx, q.Queue, input.Items), nil
}

// shortQueue answers a batch with one result too few.
type shortQueue struct{ *testworld.World }

func (q shortQueue) QueueBatch(ctx context.Context, input worlds.QueueBatchInput) ([]worlds.QueueResult, error) {
	results, err := q.World.QueueBatch(ctx, input)
	return results[:len(results)-1], err
}

func TestQueueBatchReportsEachItemsOutcome(t *testing.T) {
	ctx := context.Background()
	manual := testworld.NewManual(nil)
	defer manual.Close()
	items := []worlds.QueueInput{{RunID: "wrun_a"}, {RunID: "wrun_b"}, {RunID: "wrun_c"}}

	// A refused item is reported in its own result, in the order of the
	// items, and the others are stored.
	busy := &worlds.ThrottleError{}
	world := refusingQueue{World: manual, refuse: "wrun_b", cause: busy}
	results, err := world.QueueBatch(ctx, worlds.QueueBatchInput{Items: items})
	if err != nil || len(results) != 3 || results[0].Err != nil || results[0].MessageID == "" || !errors.Is(results[1].Err, busy) || !results[1].Retryable || results[1].MessageID != "" || results[2].Err != nil || results[2].MessageID == "" {
		t.Fatalf("batch = %+v, %v, want the second item refused retryably", results, err)
	}
	if queued := manual.Queued(); len(queued) != 2 || queued[0].RunID != "wrun_a" || queued[1].RunID != "wrun_c" {
		t.Fatalf("queued = %+v, want the two stored items", queued)
	}
	// A failure that is not transient is not retryable.
	refused := errors.New("refused")
	world.cause = refused
	if results, _ := world.QueueBatch(ctx, worlds.QueueBatchInput{Items: items}); !errors.Is(results[1].Err, refused) || results[1].Retryable {
		t.Fatalf("batch = %+v, want the second item refused for good", results)
	}

	// QueueAll turns a batch that was not stored whole into one error.
	var failed *worlds.QueueBatchError
	if _, err := worlds.QueueAll(ctx, world, items); !errors.As(err, &failed) || failed.Failed != 1 || failed.Total != 3 || failed.Retryable || !errors.Is(err, refused) {
		t.Fatalf("QueueAll = %v, want one of three refused for good", err)
	}
	if results, err := worlds.QueueAll(ctx, manual, items); err != nil || len(results) != 3 {
		t.Fatalf("QueueAll of a stored batch = %+v, %v", results, err)
	}
	// An answer that leaves an item out says nothing about it, so the batch
	// is to be sent again.
	if _, err := worlds.QueueAll(ctx, shortQueue{manual}, items); !errors.As(err, &failed) || !failed.Retryable || failed.Failed != 3 {
		t.Fatalf("QueueAll of a short answer = %v, want a retryable failure of the batch", err)
	}
	if results, err := worlds.QueueAll(ctx, manual, nil); err != nil || results != nil {
		t.Fatalf("QueueAll of nothing = %+v, %v", results, err)
	}
}
