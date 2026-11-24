package localworld

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// recordingQueue records every delivery of a World's queue. Its first
// delivery waits until the test releases it, and answer decides what each
// delivery returns.
type recordingQueue struct {
	mu      sync.Mutex
	seen    []worlds.QueueInput
	release chan struct{}
	world   *World
}

func newRecordingQueue(t *testing.T, answer func(seen []worlds.QueueInput, input worlds.QueueInput) error) *recordingQueue {
	t.Helper()
	recorder := &recordingQueue{release: make(chan struct{})}
	recorder.world = queueWorld(t, Config{}, func(_ context.Context, input worlds.QueueInput) error {
		recorder.mu.Lock()
		recorder.seen = append(recorder.seen, input)
		seen := append([]worlds.QueueInput(nil), recorder.seen...)
		recorder.mu.Unlock()
		if len(seen) == 1 {
			<-recorder.release
		}
		if answer == nil {
			return nil
		}
		return answer(seen, input)
	})
	return recorder
}

func (r *recordingQueue) deliveries() []worlds.QueueInput {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]worlds.QueueInput(nil), r.seen...)
}

// waitFor waits until count deliveries were made.
func (r *recordingQueue) waitFor(t *testing.T, count int) []worlds.QueueInput {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for len(r.deliveries()) < count {
		if time.Now().After(deadline) {
			t.Fatalf("%d deliveries, want %d", len(r.deliveries()), count)
		}
		time.Sleep(time.Millisecond)
	}
	return r.deliveries()
}

func (r *recordingQueue) queue(t *testing.T, input worlds.QueueInput) string {
	t.Helper()
	result, err := r.world.Queue(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	return result.MessageID
}

func spec9(runID worlds.RunID) worlds.QueueInput {
	return worlds.QueueInput{RunID: runID, RunSpecVersion: worlds.SpecVersion}
}

// SDK: packages/world-local/src/queue.test.ts > coalescing a spec 9 run’s orchestrator messages > covers the wakes queued behind a delivery with one re-run of it
func TestSDKQueueCoversWakesWithOneRerun(t *testing.T) {
	t.Parallel()
	recorder := newRecordingQueue(t, nil)
	first := recorder.queue(t, spec9(runA))
	recorder.waitFor(t, 1)
	coalesced := map[string]bool{}
	for range 3 {
		coalesced[recorder.queue(t, spec9(runA))] = true
	}
	waitWaiters(t, recorder.world, runA, 3)
	// Another run's wake is not this run's to cover.
	recorder.queue(t, spec9(runB))
	if seen := recorder.waitFor(t, 2); seen[1].RunID != runB {
		t.Fatalf("second delivery = %+v, want the other run's", seen[1])
	}
	close(recorder.release)
	recorder.waitFor(t, 3)
	waitQueueEmpty(t, recorder.world)
	var runADeliveries []worlds.QueueInput
	for _, input := range recorder.deliveries() {
		if input.RunID == runA {
			runADeliveries = append(runADeliveries, input)
		}
		if coalesced[input.MessageID] {
			t.Fatalf("coalesced wake %s was delivered", input.MessageID)
		}
	}
	// The re-run is the same message: the next delivery count and the same
	// attempt.
	if len(runADeliveries) != 2 {
		t.Fatalf("run deliveries = %+v, want 2", runADeliveries)
	}
	for index, input := range runADeliveries {
		if input.MessageID != first || input.Attempt != 1 || input.DeliveryCount != index+1 {
			t.Fatalf("run delivery %d = message %s attempt %d count %d", index+1, input.MessageID, input.Attempt, input.DeliveryCount)
		}
	}
}

// The SDK's legacy hook resume and invocation messages have no Go
// counterpart: a run's first delivery and a wake-up naming its waits are the
// flows that carry what the log does not.
//
// SDK: packages/world-local/src/queue.test.ts > coalescing a spec 9 run’s orchestrator messages > leaves a start message, a legacy hook resume, a wake-up and an invocation to their own deliveries
func TestSDKQueueLeavesStartAndWakeUpToTheirOwnDeliveries(t *testing.T) {
	t.Parallel()
	recorder := newRecordingQueue(t, nil)
	recorder.queue(t, spec9(runA))
	recorder.waitFor(t, 1)
	start := spec9(runA)
	start.RunInput = &worlds.EventsCreateRunInput{RunID: runA, WorkflowName: "wf", DeploymentID: "dpl"}
	startID := recorder.queue(t, start)
	// A message waits behind the run's delivery once its sender's goroutine
	// reaches the run's gate, so the next is sent after that.
	waitWaiters(t, recorder.world, runA, 1)
	// A wake-up names waits the log does not hold, which coalescing loses.
	wakeUp := spec9(runA)
	wakeUp.CompleteWaits = []string{"wait_1"}
	wakeUpID := recorder.queue(t, wakeUp)
	waitWaiters(t, recorder.world, runA, 2)
	close(recorder.release)
	seen := recorder.waitFor(t, 3)
	waitQueueEmpty(t, recorder.world)
	seen = recorder.deliveries()
	if len(seen) != 3 || seen[1].MessageID != startID || seen[2].MessageID != wakeUpID || seen[1].DeliveryCount != 1 || seen[2].DeliveryCount != 1 {
		t.Fatalf("deliveries = %+v, want the start and the wake-up after the first", seen)
	}
	if len(seen[2].CompleteWaits) != 1 || seen[2].CompleteWaits[0] != "wait_1" {
		t.Fatalf("wake-up = %+v", seen[2])
	}
}

// SDK: packages/world-local/src/queue.test.ts > coalescing a spec 9 run’s orchestrator messages > re-runs at most ${MAX_LOCAL_COALESCED_RERUNS} times per hold, then delivers what still waits
func TestSDKQueueRerunsAtMostTheCapPerHold(t *testing.T) {
	t.Parallel()
	var first string
	var recorder *recordingQueue
	ready := make(chan struct{})
	// Every delivery of the first message queues one more wake of its run.
	recorder = newRecordingQueue(t, func(seen []worlds.QueueInput, input worlds.QueueInput) error {
		<-ready
		if input.MessageID == first && len(seen) <= maxCoalescedReruns+1 {
			recorder.queue(t, spec9(runA))
			waitWaiters(t, recorder.world, runA, 1)
		}
		return nil
	})
	first = recorder.queue(t, spec9(runA))
	close(ready)
	recorder.waitFor(t, 1)
	close(recorder.release)
	seen := recorder.waitFor(t, maxCoalescedReruns+2)
	var ofFirst []worlds.QueueInput
	for _, input := range seen[:maxCoalescedReruns+1] {
		if input.MessageID == first {
			ofFirst = append(ofFirst, input)
		}
	}
	if len(ofFirst) != maxCoalescedReruns+1 || ofFirst[len(ofFirst)-1].DeliveryCount != maxCoalescedReruns+1 {
		t.Fatalf("first message delivered %d times", len(ofFirst))
	}
	for _, input := range ofFirst {
		if input.Attempt != 1 {
			t.Fatalf("re-run attempt = %d, want 1", input.Attempt)
		}
	}
	// The wake queued by its last re-run is delivered as its own message.
	if next := seen[maxCoalescedReruns+1]; next.MessageID == first || next.DeliveryCount != 1 {
		t.Fatalf("delivery after the cap = message %s count %d", next.MessageID, next.DeliveryCount)
	}
}

// SDK: packages/world-local/src/queue.test.ts > coalescing a spec 9 run’s orchestrator messages > delivers a handoff before the messages queued behind it, as a new invocation
func TestSDKQueueDeliversAHandoffBeforeWhatQueuedBehindIt(t *testing.T) {
	t.Parallel()
	handedOff := false
	var first string
	ready := make(chan struct{})
	recorder := newRecordingQueue(t, func(_ []worlds.QueueInput, input worlds.QueueInput) error {
		<-ready
		if !handedOff && input.MessageID == first {
			handedOff = true
			return worlds.Redeliver(0)
		}
		return nil
	})
	first = recorder.queue(t, spec9(runA))
	close(ready)
	recorder.waitFor(t, 1)
	// Not coalescible, so only the handoff's place decides which goes next.
	start := spec9(runA)
	start.RunInput = &worlds.EventsCreateRunInput{RunID: runA, WorkflowName: "wf", DeploymentID: "dpl"}
	startID := recorder.queue(t, start)
	waitWaiters(t, recorder.world, runA, 1)
	close(recorder.release)
	seen := recorder.waitFor(t, 3)
	if seen[0].MessageID != first || seen[1].MessageID != first || seen[2].MessageID != startID {
		t.Fatalf("deliveries = %s %s %s, want %s %s %s", seen[0].MessageID, seen[1].MessageID, seen[2].MessageID, first, first, startID)
	}
	if seen[1].Attempt != 1 || seen[1].DeliveryCount != 2 {
		t.Fatalf("handoff = attempt %d count %d, want 1 2", seen[1].Attempt, seen[1].DeliveryCount)
	}
}

// The Go QueueInput carries no hook resume timing to carry over.
//
// SDK: packages/world-local/src/queue.test.ts > coalescing a spec 9 run’s orchestrator messages > covers the wakes waiting behind a handoff with its redelivery, carrying a hook wake's timing
func TestSDKQueueHandoffCoversTheWakesWaitingBehindIt(t *testing.T) {
	t.Parallel()
	handedOff := false
	var first string
	ready := make(chan struct{})
	recorder := newRecordingQueue(t, func(_ []worlds.QueueInput, input worlds.QueueInput) error {
		<-ready
		if !handedOff && input.MessageID == first {
			handedOff = true
			return worlds.Redeliver(0)
		}
		return nil
	})
	first = recorder.queue(t, spec9(runA))
	close(ready)
	recorder.waitFor(t, 1)
	wakes := map[string]bool{recorder.queue(t, spec9(runA)): true, recorder.queue(t, spec9(runA)): true}
	waitWaiters(t, recorder.world, runA, 2)
	close(recorder.release)
	recorder.waitFor(t, 2)
	waitQueueEmpty(t, recorder.world)
	time.Sleep(50 * time.Millisecond)
	// The redelivery covered both wakes: no re-run after it, and neither
	// wake was delivered.
	seen := recorder.deliveries()
	if len(seen) != 2 || seen[0].MessageID != first || seen[1].MessageID != first {
		t.Fatalf("deliveries = %+v, want the first message twice", seen)
	}
	for _, input := range seen {
		if wakes[input.MessageID] {
			t.Fatalf("wake %s was delivered", input.MessageID)
		}
	}
}

// SDK: packages/world-local/src/queue.test.ts > queue transport timeouts > aborts an in-flight delivery when the queue closes
func TestSDKCloseAbortsAnInFlightDelivery(t *testing.T) {
	t.Parallel()
	started := make(chan struct{})
	aborted := make(chan struct{})
	world := queueWorld(t, Config{}, func(ctx context.Context, _ worlds.QueueInput) error {
		close(started)
		<-ctx.Done()
		close(aborted)
		return ctx.Err()
	})
	if _, err := world.Queue(context.Background(), spec9(runA)); err != nil {
		t.Fatal(err)
	}
	<-started
	closed := make(chan error, 1)
	go func() { closed <- world.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not return while a delivery was in flight")
	}
	select {
	case <-aborted:
	default:
		t.Fatal("the in-flight delivery was not aborted")
	}
}

// SDK: packages/world-local/src/queue.test.ts > localRetryDelayMs > keeps retrying a failing message for longer than a step message's stall window
func TestSDKRetryDelayOutlastsAStepMessagesStallWindow(t *testing.T) {
	// The shortest span over which a queue redelivers a step's message,
	// the SDK's QUEUE_MIN_STEP_REDELIVERY_SPAN_MS.
	const minStepRedeliverySpan = time.Hour
	var span, previous time.Duration
	for failed := 1; failed < maxDeliveries; failed++ {
		delay := retryDelay(failed)
		if delay < previous {
			t.Fatalf("retryDelay(%d) = %s, below the delay before it, %s", failed, delay, previous)
		}
		previous = delay
		span += delay
	}
	if span <= minStepRedeliverySpan {
		t.Fatalf("retries span %s, want more than %s", span, minStepRedeliverySpan)
	}
}
