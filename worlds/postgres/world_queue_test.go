package postgresworld

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

func TestBackoff(t *testing.T) {
	cases := map[int]time.Duration{0: time.Second, 1: time.Second, 2: 2 * time.Second, 3: 4 * time.Second, 9: 256 * time.Second, 10: 5 * time.Minute, 64: 5 * time.Minute, 1000: 5 * time.Minute}
	for attempts, want := range cases {
		if got := backoff(attempts); got != want {
			t.Errorf("backoff(%d) = %v, want %v", attempts, got, want)
		}
	}
}

func TestQueueRoundTripsEveryField(t *testing.T) {
	t.Parallel()
	world := newTestWorld(t)
	ctx := context.Background()
	stepID := worlds.StepID("step_a")
	due := time.Now().Add(-time.Second).UTC().Round(time.Millisecond)
	resumeAt, hopAt := due.Add(time.Hour), due
	want := worlds.QueueInput{
		RunID: runA, QueueName: "q", WorkflowName: "wf", StepID: &stepID, NotBefore: &due,
		IdempotencyKey: "key", DeploymentID: "dpl_1", DeploymentMismatchRetryCount: 3, ReplayDivergenceCount: 2,
		StepName: "step", StepAttempt: 4, StepResent: true, RunSpecVersion: worlds.SpecVersion,
		CompleteWaits:    []string{"wait_a", "wait_b"},
		Recovery:         true,
		WaitContinuation: &worlds.WaitContinuation{CorrelationID: "wait_a", Attempt: 1, ResumeAt: &resumeAt, HopAt: &hopAt},
		Retention:        time.Hour,
	}
	queued, err := world.Queue(ctx, want)
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := world.claim(ctx, 10, nil)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claimed = %#v err=%v", claimed, err)
	}
	if !strings.HasPrefix(queued.MessageID, "msg_") {
		t.Fatalf("message ID %q, want msg_<ULID>", queued.MessageID)
	}
	want.MessageID, want.Attempt, want.DeliveryCount = queued.MessageID, 1, 1
	if got := claimed[0].input; !reflect.DeepEqual(got, want) {
		t.Fatalf("claimed input = %#v\nwant %#v", got, want)
	}
}

func TestQueueDeduplicatesByIdempotencyKey(t *testing.T) {
	t.Parallel()
	world := newTestWorld(t)
	ctx := context.Background()
	first, err := world.Queue(ctx, worlds.QueueInput{RunID: runA, IdempotencyKey: "key"})
	if err != nil {
		t.Fatal(err)
	}
	again, err := world.Queue(ctx, worlds.QueueInput{RunID: runA, IdempotencyKey: "key"})
	if err != nil || again.MessageID != first.MessageID {
		t.Fatalf("duplicate key = %#v err=%v, want %s", again, err, first.MessageID)
	}
	batch, err := world.QueueBatch(ctx, worlds.QueueBatchInput{Items: []worlds.QueueInput{{RunID: runA, IdempotencyKey: "key"}, {RunID: runA, IdempotencyKey: "other"}}})
	if err != nil || len(batch) != 2 || batch[0].MessageID != first.MessageID || batch[1].MessageID == first.MessageID {
		t.Fatalf("batch = %#v err=%v", batch, err)
	}
	other, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_b", IdempotencyKey: "key"})
	if err != nil || other.MessageID == first.MessageID {
		t.Fatalf("same key in another run = %#v err=%v", other, err)
	}
	// Without a key nothing is deduplicated.
	a, errA := world.Queue(ctx, worlds.QueueInput{RunID: runA})
	b, errB := world.Queue(ctx, worlds.QueueInput{RunID: runA})
	if errA != nil || errB != nil || a.MessageID == b.MessageID {
		t.Fatalf("unkeyed = %s, %s err=%v %v", a.MessageID, b.MessageID, errA, errB)
	}
	// An executing message still deduplicates.
	claimed, err := world.claim(ctx, 10, nil)
	if err != nil || !slices.ContainsFunc(claimed, func(item delivery) bool { return item.id == first.MessageID }) {
		t.Fatalf("claimed = %#v err=%v", claimed, err)
	}
	if again, err := world.Queue(ctx, worlds.QueueInput{RunID: runA, IdempotencyKey: "key"}); err != nil || again.MessageID != first.MessageID {
		t.Fatalf("key while executing = %#v err=%v", again, err)
	}
	// Once the message is gone, the key queues a new one.
	if _, err := world.pool.Exec(ctx, `DELETE FROM workflow_queue WHERE id = $1`, first.MessageID); err != nil {
		t.Fatal(err)
	}
	if fresh, err := world.Queue(ctx, worlds.QueueInput{RunID: runA, IdempotencyKey: "key"}); err != nil || fresh.MessageID == first.MessageID {
		t.Fatalf("key after delete = %#v err=%v", fresh, err)
	}
}

func TestQueueDropsAMessagePastItsRetention(t *testing.T) {
	t.Parallel()
	world := newTestWorld(t)
	ctx := context.Background()
	expired, err := world.Queue(ctx, worlds.QueueInput{RunID: runA, IdempotencyKey: "key", Retention: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	if claimed, err := world.claim(ctx, 10, nil); err != nil || len(claimed) != 0 {
		t.Fatalf("claimed expired = %#v err=%v", claimed, err)
	}
	if fresh, err := world.Queue(ctx, worlds.QueueInput{RunID: runA, IdempotencyKey: "key"}); err != nil || fresh.MessageID == expired.MessageID {
		t.Fatalf("key after expiry = %#v err=%v", fresh, err)
	}
}

// executorFunc executes deliveries with a function.
type executorFunc func(context.Context, worlds.QueueInput) error

func (f executorFunc) Execute(ctx context.Context, input worlds.QueueInput) error {
	return f(ctx, input)
}

func TestRedeliveryKeepsTheMessageAndCountsAttempts(t *testing.T) {
	t.Parallel()
	world := newTestWorld(t)
	ctx := context.Background()
	queued, err := world.Queue(ctx, worlds.QueueInput{RunID: runA, IdempotencyKey: "key", RunSpecVersion: worlds.SpecVersion})
	if err != nil {
		t.Fatal(err)
	}
	// A spec 9 flow's handoff, then a delayed redelivery, then a handoff,
	// then success.
	results := []error{worlds.Redeliver(0), worlds.Redeliver(50 * time.Millisecond), worlds.Redeliver(0), nil}
	var mu sync.Mutex
	var seen []worlds.QueueInput
	done := make(chan struct{})
	execute := executorFunc(func(_ context.Context, input worlds.QueueInput) error {
		mu.Lock()
		defer mu.Unlock()
		seen = append(seen, input)
		result := results[len(seen)-1]
		if len(seen) == len(results) {
			close(done)
		}
		return result
	})
	workerCtx, stop := context.WithCancel(ctx)
	stopped := make(chan error, 1)
	go func() { stopped <- world.work(workerCtx, execute, WorkerOptions{Concurrency: 2}) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("deliveries did not finish")
	}
	stop()
	<-stopped
	wantCounts := [][2]int{{1, 1}, {2, 1}, {3, 2}, {4, 2}}
	for index, input := range seen {
		if input.MessageID != queued.MessageID || input.DeliveryCount != wantCounts[index][0] || input.Attempt != wantCounts[index][1] {
			t.Errorf("delivery %d: message %s count %d attempt %d, want %s %v", index, input.MessageID, input.DeliveryCount, input.Attempt, queued.MessageID, wantCounts[index])
		}
	}
	var rows int
	if err := world.pool.QueryRow(ctx, `SELECT count(*) FROM workflow_queue`).Scan(&rows); err != nil || rows != 0 {
		t.Fatalf("rows after success = %d err=%v", rows, err)
	}
}

func TestClaimHoldsAFlowWhileAnotherOfItsRunRuns(t *testing.T) {
	t.Parallel()
	world := newTestWorld(t)
	ctx := context.Background()
	stepID := worlds.StepID("step_a")
	resumeAt, hopAt := time.Now().Add(time.Hour), time.Now()
	ids := map[string]string{}
	for name, input := range map[string]worlds.QueueInput{
		"flowA1": {RunID: runA, IdempotencyKey: "1"},
		"flowA2": {RunID: runA, IdempotencyKey: "2"},
		"stepA":  {RunID: runA, StepID: &stepID},
		"hopA":   {RunID: runA, WaitContinuation: &worlds.WaitContinuation{CorrelationID: "wait", ResumeAt: &resumeAt, HopAt: &hopAt}},
		"flowB":  {RunID: "wrun_b"},
	} {
		queued, err := world.Queue(ctx, input)
		if err != nil {
			t.Fatal(err)
		}
		ids[queued.MessageID] = name
	}
	claimed, err := world.claim(ctx, 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	var flowA delivery
	for _, item := range claimed {
		names[ids[item.id]] = true
		if name := ids[item.id]; name == "flowA1" || name == "flowA2" {
			flowA = item
		}
	}
	if len(claimed) != 4 || !names["stepA"] || !names["hopA"] || !names["flowB"] || names["flowA1"] == names["flowA2"] {
		t.Fatalf("first claim = %v", names)
	}
	if again, err := world.claim(ctx, 10, nil); err != nil || len(again) != 0 {
		t.Fatalf("flow claimed while another flow of its run executes: %#v err=%v", again, err)
	}
	world.deliver(ctx, executorFunc(func(context.Context, worlds.QueueInput) error { return nil }), flowA)
	again, err := world.claim(ctx, 10, nil)
	if err != nil || len(again) != 1 || again[0].id == flowA.id || !again[0].input.SerializedFlow() || again[0].input.RunID != runA {
		t.Fatalf("claim after flow finished = %#v err=%v", again, err)
	}
}

// TestWorkersSerializeFlows runs workers in two Worlds and checks two flows
// of one run never execute at once, while step deliveries run beside them.
func TestQueueDeliversOneFlowOfARunAtATime(t *testing.T) {
	t.Parallel()
	url, schema := testSchema(t)
	a := testWorld(t, testPool(t, url, schema), Config{})
	b := testWorld(t, testPool(t, url, schema), Config{})
	ctx := context.Background()
	const flows, steps = 8, 8
	for i := range flows {
		if _, err := a.Queue(ctx, worlds.QueueInput{RunID: runA}); err != nil {
			t.Fatal(err)
		}
		stepID := worlds.StepID(fmt.Sprintf("step_%d", i))
		if i < steps {
			if _, err := a.Queue(ctx, worlds.QueueInput{RunID: runA, StepID: &stepID}); err != nil {
				t.Fatal(err)
			}
		}
	}
	var mu sync.Mutex
	runningFlows, runningSteps, maxFlows, maxSteps, finished := 0, 0, 0, 0, 0
	done := make(chan struct{})
	execute := executorFunc(func(_ context.Context, input worlds.QueueInput) error {
		mu.Lock()
		if input.SerializedFlow() {
			runningFlows++
			maxFlows = max(maxFlows, runningFlows)
		} else {
			runningSteps++
			maxSteps = max(maxSteps, runningSteps)
		}
		mu.Unlock()
		time.Sleep(30 * time.Millisecond)
		mu.Lock()
		defer mu.Unlock()
		if input.SerializedFlow() {
			runningFlows--
		} else {
			runningSteps--
		}
		if finished++; finished == flows+steps {
			close(done)
		}
		return nil
	})
	workerCtx, stop := context.WithCancel(ctx)
	stopped := make(chan error, 2)
	for _, world := range []*World{a, b} {
		go func() { stopped <- world.work(workerCtx, execute, WorkerOptions{Concurrency: 4}) }()
	}
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("deliveries did not finish")
	}
	stop()
	<-stopped
	<-stopped
	if maxFlows != 1 || maxSteps < 2 {
		t.Fatalf("most flows at once = %d, steps = %d", maxFlows, maxSteps)
	}
}

// deliverDue makes the message's row due, claims it, and delivers it with
// result, returning the input its delivery saw, or false when nothing was
// claimed.
func deliverDue(t *testing.T, world *World, id string, result error) (worlds.QueueInput, bool) {
	t.Helper()
	ctx := context.Background()
	if _, err := world.pool.Exec(ctx, `UPDATE workflow_queue SET not_before = now() - interval '1 second' WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
	claimed, err := world.claim(ctx, 10, nil)
	if err != nil || len(claimed) > 1 {
		t.Fatalf("claimed = %#v err=%v", claimed, err)
	}
	if len(claimed) == 0 {
		return worlds.QueueInput{}, false
	}
	var seen worlds.QueueInput
	world.deliver(ctx, executorFunc(func(_ context.Context, input worlds.QueueInput) error {
		seen = input
		return result
	}), claimed[0])
	return seen, true
}

// queueRows counts the queue's rows.
func queueRows(t *testing.T, world *World) int {
	t.Helper()
	var rows int
	if err := world.pool.QueryRow(context.Background(), `SELECT count(*) FROM workflow_queue`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	return rows
}

// TestRedeliveryCountsAttemptsAsWorldPostgres checks Attempt across
// redeliveries and failures, as world-postgres counts it: a spec 9 handoff
// keeps it, a spec 9 flow's delayed redelivery counts, a failure counts, and
// any other redelivery, as a step's in-place retry, starts it over from where
// the message's job started it (after a handoff, the attempt it carried).
// DeliveryCount counts every delivery.
func TestRedeliveryCountsAttemptsAsWorldPostgres(t *testing.T) {
	t.Parallel()
	failed := errors.New("failed")
	stepID := worlds.StepID("step_a")
	resumeAt, hopAt := time.Now().Add(time.Hour), time.Now()
	later := worlds.Redeliver(50 * time.Millisecond)
	for name, test := range map[string]struct {
		input    worlds.QueueInput
		results  []error
		attempts []int
	}{
		"spec 9 flow": {
			worlds.QueueInput{RunID: runA, RunSpecVersion: worlds.SpecVersion},
			[]error{failed, worlds.Redeliver(0), later, failed, later, nil},
			[]int{1, 2, 2, 3, 4, 5},
		},
		"spec 9 step": {
			worlds.QueueInput{RunID: runA, StepID: &stepID, RunSpecVersion: worlds.SpecVersion},
			[]error{failed, failed, later, failed, worlds.Redeliver(0), later, nil},
			[]int{1, 2, 3, 1, 2, 2, 2},
		},
		"spec 9 wait hop": {
			worlds.QueueInput{RunID: runA, RunSpecVersion: worlds.SpecVersion, WaitContinuation: &worlds.WaitContinuation{CorrelationID: "wait", ResumeAt: &resumeAt, HopAt: &hopAt}},
			[]error{failed, later, nil},
			[]int{1, 2, 1},
		},
		"spec 8 flow": {
			worlds.QueueInput{RunID: runA, RunSpecVersion: 8},
			[]error{failed, worlds.Redeliver(0), failed, later, nil},
			[]int{1, 2, 1, 2, 1},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			world := newTestWorld(t)
			queued, err := world.Queue(context.Background(), test.input)
			if err != nil {
				t.Fatal(err)
			}
			for index, result := range test.results {
				seen, ok := deliverDue(t, world, queued.MessageID, result)
				if !ok {
					t.Fatalf("delivery %d: nothing claimed", index)
				}
				if seen.MessageID != queued.MessageID || seen.DeliveryCount != index+1 || seen.Attempt != test.attempts[index] {
					t.Fatalf("delivery %d: message %s count %d attempt %d, want %s %d %d", index, seen.MessageID, seen.DeliveryCount, seen.Attempt, queued.MessageID, index+1, test.attempts[index])
				}
			}
			if rows := queueRows(t, world); rows != 0 {
				t.Fatalf("rows after success = %d", rows)
			}
		})
	}
}

// TestFailedDeliveryIsDroppedAfterTheJobsAttempts checks a message whose job
// failed maxJobAttempts deliveries is deleted, as Graphile Worker stops
// retrying a job past its max_attempts, that a crashed delivery counts, and
// that a Redelivery starts a new job with its own attempts.
func TestFailedDeliveryIsDroppedAfterTheJobsAttempts(t *testing.T) {
	t.Parallel()
	failed := errors.New("failed")
	t.Run("failure", func(t *testing.T) {
		t.Parallel()
		world := newTestWorld(t)
		queued, err := world.Queue(context.Background(), worlds.QueueInput{RunID: runA})
		if err != nil {
			t.Fatal(err)
		}
		// The job has failed all but its last two attempts.
		if _, err := world.pool.Exec(context.Background(), `UPDATE workflow_queue SET deliveries = $1`, maxJobAttempts-2); err != nil {
			t.Fatal(err)
		}
		if _, ok := deliverDue(t, world, queued.MessageID, failed); !ok || queueRows(t, world) != 1 {
			t.Fatalf("message dropped before its job's last attempt")
		}
		if _, ok := deliverDue(t, world, queued.MessageID, failed); !ok || queueRows(t, world) != 0 {
			t.Fatalf("message kept after its job's last attempt failed: rows %d", queueRows(t, world))
		}
	})
	t.Run("crash", func(t *testing.T) {
		t.Parallel()
		world := newTestWorld(t)
		queued, err := world.Queue(context.Background(), worlds.QueueInput{RunID: runA})
		if err != nil {
			t.Fatal(err)
		}
		// A worker crashed during the job's last attempt.
		if _, err := world.pool.Exec(context.Background(), `UPDATE workflow_queue SET deliveries = $1, lease_until = now() - interval '1 second'`, maxJobAttempts); err != nil {
			t.Fatal(err)
		}
		if seen, ok := deliverDue(t, world, queued.MessageID, nil); ok || queueRows(t, world) != 0 {
			t.Fatalf("exhausted message delivered = %#v, rows %d", seen, queueRows(t, world))
		}
	})
	t.Run("redelivery", func(t *testing.T) {
		t.Parallel()
		world := newTestWorld(t)
		queued, err := world.Queue(context.Background(), worlds.QueueInput{RunID: runA, RunSpecVersion: worlds.SpecVersion})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := world.pool.Exec(context.Background(), `UPDATE workflow_queue SET deliveries = $1`, maxJobAttempts-1); err != nil {
			t.Fatal(err)
		}
		// The job's last attempt redelivers, so the next job has every
		// attempt.
		if _, ok := deliverDue(t, world, queued.MessageID, worlds.Redeliver(time.Millisecond)); !ok {
			t.Fatal("nothing claimed")
		}
		for range 2 {
			if _, ok := deliverDue(t, world, queued.MessageID, failed); !ok || queueRows(t, world) != 1 {
				t.Fatalf("redelivered message dropped: rows %d", queueRows(t, world))
			}
		}
	})
}

// runA is a run ID minted when the tests start, as a World requires
// of a new run.
var runA = worlds.NewRunID()
