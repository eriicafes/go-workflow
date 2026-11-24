package postgresworld

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// SDK: packages/world-postgres/src/job-lease.test.ts > createJobLeases > renews a running delivery every quarter of the stale window and stops when it finishes
func TestSDKDeliveryRenewsItsLeaseWhileItRuns(t *testing.T) {
	t.Parallel()
	url, schema := testSchema(t)
	world := testWorld(t, testPool(t, url, schema), Config{LeaseDuration: 300 * time.Millisecond})
	ctx := context.Background()
	queued, err := world.Queue(ctx, worlds.QueueInput{RunID: runA})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := world.claim(ctx, 10, nil)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claimed = %#v err=%v", claimed, err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		world.deliver(ctx, executorFunc(func(context.Context, worlds.QueueInput) error {
			time.Sleep(1200 * time.Millisecond)
			return nil
		}), claimed[0])
	}()
	// The delivery outlives its lease four times over, and no other worker
	// claims it meanwhile.
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		if again, err := world.claim(ctx, 10, nil); err != nil || len(again) != 0 {
			t.Fatalf("claimed while the delivery runs = %#v err=%v", again, err)
		}
	}
	<-done
	if rows := queueRows(t, world); rows != 0 {
		t.Fatalf("message %s left %d rows after its delivery succeeded", queued.MessageID, rows)
	}
}

// SDK: packages/world-postgres/src/job-lease.test.ts > createJobLeases > refuses to acknowledge a delivery whose lock was taken away
func TestSDKDeliveryThatLostItsLeaseIsNotAcknowledged(t *testing.T) {
	t.Parallel()
	url, schema := testSchema(t)
	world := testWorld(t, testPool(t, url, schema), Config{LeaseDuration: 300 * time.Millisecond})
	ctx := context.Background()
	queued, err := world.Queue(ctx, worlds.QueueInput{RunID: runA})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := world.claim(ctx, 10, nil)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claimed = %#v err=%v", claimed, err)
	}
	cancelled := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		world.deliver(ctx, executorFunc(func(ctx context.Context, _ worlds.QueueInput) error {
			// Another worker takes the delivery over, as after this one
			// stalled past its lease.
			if _, err := world.pool.Exec(context.Background(), `UPDATE workflow_queue SET deliveries = deliveries + 1, lease_until = now() + interval '1 minute' WHERE id = $1`, queued.MessageID); err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				close(cancelled)
			case <-time.After(5 * time.Second):
			}
			// Success, which must not acknowledge the other worker's
			// delivery.
			return nil
		}), claimed[0])
	}()
	select {
	case <-cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("the delivery was not told it lost its lease")
	}
	<-done
	var deliveries int
	var leased bool
	if err := world.pool.QueryRow(ctx, `SELECT deliveries, lease_until > now() FROM workflow_queue WHERE id = $1`, queued.MessageID).Scan(&deliveries, &leased); err != nil {
		t.Fatalf("the other worker's delivery is gone: %v", err)
	}
	if deliveries != claimed[0].deliveries+1 || !leased {
		t.Fatalf("row = deliveries %d leased %v, want the other worker's", deliveries, leased)
	}
}

// SDK: packages/world-postgres/src/queue.test.ts > postgres queue http execution > leaves job attempts for redeliveries past core max deliveries
func TestSDKJobHasAttemptsPastTheCoresMaxDeliveries(t *testing.T) {
	t.Parallel()
	world := newTestWorld(t)
	queued, err := world.Queue(context.Background(), worlds.QueueInput{RunID: runA})
	if err != nil {
		t.Fatal(err)
	}
	// The core records MAX_DELIVERIES_EXCEEDED on delivery 49, and a
	// transient failure of that write must still be redelivered.
	if _, err := world.pool.Exec(context.Background(), `UPDATE workflow_queue SET deliveries = 48`); err != nil {
		t.Fatal(err)
	}
	if seen, ok := deliverDue(t, world, queued.MessageID, errors.New("terminal write failed")); !ok || seen.Attempt != 49 {
		t.Fatalf("delivery = %#v claimed %v, want attempt 49", seen, ok)
	}
	if rows := queueRows(t, world); rows != 1 {
		t.Fatalf("rows after the failed terminal write = %d, want the message kept", rows)
	}
}
