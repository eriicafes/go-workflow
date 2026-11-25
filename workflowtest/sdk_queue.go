package workflowtest

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// SDK: packages/world-local/src/queue.test.ts > queue timeout re-enqueue > dedupes simultaneous sends under the same idempotency key
func conformSDKQueueDedupesSimultaneousSends(t *testing.T, world worlds.World) {
	runID := conformCreateRun(t, world, "sdk_queue_simultaneous")
	later := time.Now().Add(time.Hour)
	results := make([]worlds.QueueResult, 2)
	errs := make([]error, 2)
	stepID := worlds.StepID("step_01ABC")
	var group sync.WaitGroup
	for i := range results {
		group.Go(func() {
			results[i], errs[i] = world.Queue(context.Background(), worlds.QueueInput{RunID: runID, StepID: &stepID, NotBefore: &later, IdempotencyKey: "same-step"})
		})
	}
	group.Wait()
	if errs[0] != nil || errs[1] != nil || results[0].MessageID == "" || results[1].MessageID != results[0].MessageID {
		t.Fatalf("simultaneous sends = %#v err=%v", results, errs)
	}
}

// conformSDKQueueDedupesKeyedMessages queues each kind of message the
// runtime keys twice: a step's message, sent again without its delay, a
// wait's last timer, and an intermediate hop of a long wait's timer. Each is
// stored once, and a wake, which has no key, every time.
//
// SDK: packages/world-postgres/src/queue.test.ts > postgres queue http execution > deduplicates an earlier run's step message against any job holding its key
// SDK: packages/world-postgres/src/queue.test.ts > postgres queue http execution > deduplicates a spec 9 run's keyed message against any job holding its key
func conformSDKQueueDedupesKeyedMessages(t *testing.T, world worlds.World) {
	ctx := context.Background()
	runID := conformCreateRun(t, world, "sdk_queue_keyed")
	later := time.Now().Add(time.Hour)
	resumeAt, hopAt := later.Add(time.Hour), later
	queue := func(input worlds.QueueInput) string {
		t.Helper()
		input.RunID, input.RunSpecVersion = runID, worlds.SpecVersion
		result, err := world.Queue(ctx, input)
		if err != nil || result.MessageID == "" {
			t.Fatalf("Queue(%+v) = %#v err=%v", input, result, err)
		}
		return result.MessageID
	}
	stepID := worlds.StepID("step_01ABC")
	step := queue(worlds.QueueInput{StepID: &stepID, StepName: "retrying-step", NotBefore: &later, IdempotencyKey: "step_01ABC"})
	// Sent again after a send that failed, without its delay.
	if again := queue(worlds.QueueInput{StepID: &stepID, StepName: "retrying-step", IdempotencyKey: "step_01ABC"}); again != step {
		t.Fatalf("step message sent again = %s, want %s", again, step)
	}
	timer := worlds.QueueInput{NotBefore: &later, IdempotencyKey: "wait:wait_01ABC:2000:2000", WaitContinuation: &worlds.WaitContinuation{CorrelationID: "wait_01ABC"}}
	if first, again := queue(timer), queue(timer); first != again {
		t.Fatalf("wait timer sent twice = %s, %s", first, again)
	}
	hop := worlds.QueueInput{NotBefore: &later, IdempotencyKey: "wait:wait_01ABC:2000:1000", WaitContinuation: &worlds.WaitContinuation{CorrelationID: "wait_01ABC", ResumeAt: &resumeAt, HopAt: &hopAt}}
	if first, again := queue(hop), queue(hop); first != again {
		t.Fatalf("wait hop sent twice = %s, %s", first, again)
	}
	wake := worlds.QueueInput{NotBefore: &later}
	if first, again := queue(wake), queue(wake); first == again {
		t.Fatalf("two wakes were stored as one message %s", first)
	}
}

// sdkExecutor executes each delivery with a function, the way a test
// watches a World's deliveries.
type sdkExecutor func(context.Context, worlds.QueueInput) error

func (f sdkExecutor) Execute(ctx context.Context, _ worlds.Runtime, input worlds.QueueInput) error {
	return f(ctx, input)
}

// sdkOpen returns a World that delivers its messages to execute.
type sdkOpen func(execute func(context.Context, worlds.QueueInput) error) worlds.World

// sdkReceive waits for the next delivery.
func sdkReceive(t *testing.T, deliveries <-chan worlds.QueueInput, within time.Duration) worlds.QueueInput {
	t.Helper()
	select {
	case input := <-deliveries:
		return input
	case <-time.After(within):
		t.Fatalf("no delivery within %s", within)
		return worlds.QueueInput{}
	}
}

// sdkExpectNoDelivery checks no delivery arrives within d.
func sdkExpectNoDelivery(t *testing.T, deliveries <-chan worlds.QueueInput, d time.Duration) {
	t.Helper()
	select {
	case input := <-deliveries:
		t.Fatalf("unexpected delivery %+v", input)
	case <-time.After(d):
	}
}

// sdkQueue queues input and returns its message ID.
func sdkQueue(t *testing.T, world worlds.World, input worlds.QueueInput) string {
	t.Helper()
	result, err := world.Queue(context.Background(), input)
	if err != nil {
		t.Fatalf("Queue(%+v): %v", input, err)
	}
	return result.MessageID
}

// sdkFlow returns a flow of a spec 9 run, which a World delivers one at a
// time.
func sdkFlow(runID worlds.RunID) worlds.QueueInput {
	return worlds.QueueInput{RunID: runID, RunSpecVersion: worlds.SpecVersion}
}

// SDK: packages/world-local/src/queue.test.ts > queue timeout re-enqueue > queue retries when handler returns timeoutSeconds > 0
// SDK: packages/world-local/src/queue.test.ts > queue timeout re-enqueue > redelivers the SAME message on timeoutSeconds: one id, one createdAt, increasing deliveryCount
func conformSDKDeliveryRedelivers(t *testing.T, open sdkOpen) {
	deliveries := make(chan worlds.QueueInput, 10)
	world := open(func(_ context.Context, input worlds.QueueInput) error {
		deliveries <- input
		if input.DeliveryCount < 3 {
			return worlds.Redeliver(time.Second)
		}
		return nil
	})
	flow := sdkFlow(worlds.NewRunID())
	flow.IdempotencyKey, flow.Retention = "step_01ABC", time.Hour
	id := sdkQueue(t, world, flow)
	var seen []worlds.QueueInput
	for range 3 {
		seen = append(seen, sdkReceive(t, deliveries, 20*time.Second))
	}
	for index, input := range seen {
		if input.MessageID != id || input.DeliveryCount != index+1 || input.Attempt != index+1 {
			t.Fatalf("delivery %d = message %s count %d attempt %d, want %s %d %d", index+1, input.MessageID, input.DeliveryCount, input.Attempt, id, index+1, index+1)
		}
		if !input.CreatedAt.Equal(seen[0].CreatedAt) {
			t.Fatalf("delivery %d was created at %s, the first at %s", index+1, input.CreatedAt, seen[0].CreatedAt)
		}
	}
	if seen[0].CreatedAt.IsZero() {
		t.Fatal("the delivery does not say when its message was created")
	}
}

// SDK: packages/world-local/src/queue.test.ts > queue timeout re-enqueue > dedupes a second send under the same idempotency key while the first message is retrying
func conformSDKDeliveryDedupesWhileRetrying(t *testing.T, open sdkOpen) {
	release := make(chan struct{})
	deliveries := make(chan worlds.QueueInput, 10)
	var calls atomic.Int32
	world := open(func(_ context.Context, input worlds.QueueInput) error {
		deliveries <- input
		if calls.Add(1) == 1 {
			<-release
			return worlds.Redeliver(time.Second)
		}
		return nil
	})
	runID := worlds.NewRunID()
	stepID := worlds.StepID("step_01ABC")
	message := worlds.QueueInput{RunID: runID, StepID: &stepID, StepName: "test-step", RunSpecVersion: worlds.SpecVersion, IdempotencyKey: "step_01ABC"}
	first := sdkQueue(t, world, message)
	sdkReceive(t, deliveries, 10*time.Second)
	if second := sdkQueue(t, world, message); second != first {
		t.Fatalf("second send = %s, want %s", second, first)
	}
	close(release)
	if again := sdkReceive(t, deliveries, 20*time.Second); again.MessageID != first {
		t.Fatalf("redelivery = %s, want %s", again.MessageID, first)
	}
	sdkExpectNoDelivery(t, deliveries, 1500*time.Millisecond)
}

// SDK: packages/world-local/src/queue.test.ts > queue timeout re-enqueue > delivers a wake sent without an idempotency key every time
// SDK: packages/world-postgres/src/queue.test.ts > postgres queue http execution > stamps createdAt on a new message and keeps an idempotency key off a wake
func conformSDKDeliveryUnkeyedWakes(t *testing.T, open sdkOpen) {
	deliveries := make(chan worlds.QueueInput, 10)
	world := open(func(_ context.Context, input worlds.QueueInput) error {
		deliveries <- input
		return nil
	})
	runID := worlds.NewRunID()
	queued := time.Now()
	a, b := sdkQueue(t, world, sdkFlow(runID)), sdkQueue(t, world, sdkFlow(runID))
	if a == b {
		t.Fatalf("two wakes were stored as one message %s", a)
	}
	// Both are executed, though one may be covered by a re-run of the other.
	for range 2 {
		input := sdkReceive(t, deliveries, 10*time.Second)
		if input.IdempotencyKey != "" || input.CreatedAt.Before(queued.Add(-time.Second)) || time.Since(input.CreatedAt) > time.Minute {
			t.Fatalf("wake = key %q created %s", input.IdempotencyKey, input.CreatedAt)
		}
	}
}

// SDK: packages/world-local/src/queue.test.ts > queue timeout re-enqueue > queue retries when the handler rejects
func conformSDKDeliveryRetriesFailures(t *testing.T, open sdkOpen) {
	deliveries := make(chan worlds.QueueInput, 10)
	world := open(func(_ context.Context, input worlds.QueueInput) error {
		deliveries <- input
		if input.DeliveryCount < 3 {
			return context.DeadlineExceeded
		}
		return nil
	})
	id := sdkQueue(t, world, sdkFlow(worlds.NewRunID()))
	for count := 1; count <= 3; count++ {
		if input := sdkReceive(t, deliveries, 30*time.Second); input.MessageID != id || input.DeliveryCount != count {
			t.Fatalf("delivery %d = message %s count %d", count, input.MessageID, input.DeliveryCount)
		}
	}
	sdkExpectNoDelivery(t, deliveries, 200*time.Millisecond)
}

// SDK: packages/world-local/src/queue.test.ts > queue timeout re-enqueue > queue retries immediately when handler returns timeoutSeconds: 0
// SDK: packages/world-local/src/queue.test.ts > handoff > redelivers a spec 9 message at once without counting the handed-off deliveries as attempts
// SDK: packages/world-postgres/src/queue.test.ts > keeps attempt across a { timeoutSeconds: 0 } handoff of a spec 9 orchestrator message
func conformSDKDeliveryHandoff(t *testing.T, open sdkOpen) {
	deliveries := make(chan worlds.QueueInput, 10)
	world := open(func(_ context.Context, input worlds.QueueInput) error {
		deliveries <- input
		if input.DeliveryCount < 3 {
			return worlds.Redeliver(0)
		}
		return nil
	})
	id := sdkQueue(t, world, sdkFlow(worlds.NewRunID()))
	first := sdkReceive(t, deliveries, 10*time.Second)
	started := time.Now()
	seen := []worlds.QueueInput{first, sdkReceive(t, deliveries, 10*time.Second), sdkReceive(t, deliveries, 10*time.Second)}
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Fatalf("handoffs took %s", elapsed)
	}
	for index, input := range seen {
		if input.MessageID != id || input.DeliveryCount != index+1 || input.Attempt != 1 {
			t.Fatalf("delivery %d = message %s count %d attempt %d, want %s %d 1", index+1, input.MessageID, input.DeliveryCount, input.Attempt, id, index+1)
		}
	}
}

// SDK: packages/world-local/src/queue.test.ts > queue delaySeconds > honors delaySeconds before delivering the message
func conformSDKDeliveryHonorsDelay(t *testing.T, open sdkOpen) {
	deliveries := make(chan worlds.QueueInput, 10)
	world := open(func(_ context.Context, input worlds.QueueInput) error {
		deliveries <- input
		return nil
	})
	flow := sdkFlow(worlds.NewRunID())
	due := time.Now().Add(2 * time.Second)
	flow.NotBefore = &due
	sdkQueue(t, world, flow)
	sdkReceive(t, deliveries, 20*time.Second)
	if early := time.Until(due); early > 50*time.Millisecond {
		t.Fatalf("delivered %s before it was due", early)
	}
	sdkExpectNoDelivery(t, deliveries, 200*time.Millisecond)
}

// SDK: packages/world-local/src/queue.test.ts > queue delaySeconds > does not call setTimeout for delaySeconds: 0
// SDK: packages/world-local/src/queue.test.ts > queue delaySeconds > does not call setTimeout when delaySeconds is omitted
func conformSDKDeliveryWithoutDelay(t *testing.T, open sdkOpen) {
	deliveries := make(chan worlds.QueueInput, 10)
	world := open(func(_ context.Context, input worlds.QueueInput) error {
		deliveries <- input
		return nil
	})
	now := time.Now()
	zero := sdkFlow(worlds.NewRunID())
	zero.NotBefore = &now
	for _, flow := range []worlds.QueueInput{zero, sdkFlow(worlds.NewRunID())} {
		started := time.Now()
		sdkQueue(t, world, flow)
		sdkReceive(t, deliveries, 10*time.Second)
		if elapsed := time.Since(started); elapsed > 2*time.Second {
			t.Fatalf("a message without a delay was delivered after %s", elapsed)
		}
	}
}

// sdkHolder executes deliveries, holding each until the test releases it,
// and tracks how many deliveries of each run's flows and of each step
// overlap.
type sdkHolder struct {
	mu        sync.Mutex
	started   []worlds.QueueInput
	releases  []chan struct{}
	active    map[string]int
	maxActive map[string]int
}

func newSDKHolder() *sdkHolder {
	return &sdkHolder{active: map[string]int{}, maxActive: map[string]int{}}
}

func (h *sdkHolder) execute(ctx context.Context, input worlds.QueueInput) error {
	key := string(input.RunID)
	if input.StepID != nil {
		key += "/" + string(*input.StepID)
	}
	release := make(chan struct{})
	h.mu.Lock()
	h.active[key]++
	h.maxActive[key] = max(h.maxActive[key], h.active[key])
	h.started = append(h.started, input)
	h.releases = append(h.releases, release)
	h.mu.Unlock()
	select {
	case <-release:
	case <-ctx.Done():
	}
	h.mu.Lock()
	h.active[key]--
	h.mu.Unlock()
	return nil
}

func (h *sdkHolder) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.started)
}

// waitFor waits until count deliveries started.
func (h *sdkHolder) waitFor(t *testing.T, count int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for h.count() < count {
		if time.Now().After(deadline) {
			t.Fatalf("%d deliveries started, want %d", h.count(), count)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// release lets the index-th delivery finish.
func (h *sdkHolder) release(index int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	close(h.releases[index])
}

func (h *sdkHolder) releaseAll() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, release := range h.releases {
		select {
		case <-release:
		default:
			close(release)
		}
	}
}

// SDK: packages/world-local/src/queue.test.ts > per-run orchestrator serialization > holds a second orchestrator delivery of a spec 9 run until the first finishes
func conformSDKDeliverySerializesFlows(t *testing.T, open sdkOpen) {
	holder := newSDKHolder()
	world := open(holder.execute)
	defer holder.releaseAll()
	runID := worlds.NewRunID()
	sdkQueue(t, world, sdkFlow(runID))
	sdkQueue(t, world, sdkFlow(runID))
	holder.waitFor(t, 1)
	// The second delivery has every chance to start.
	time.Sleep(200 * time.Millisecond)
	if count := holder.count(); count != 1 {
		t.Fatalf("%d deliveries of the run's flows started at once", count)
	}
	holder.release(0)
	holder.waitFor(t, 2)
	holder.release(1)
	holder.mu.Lock()
	defer holder.mu.Unlock()
	if most := holder.maxActive[string(runID)]; most != 1 {
		t.Fatalf("%d flows of the run executed at once", most)
	}
}

// SDK: packages/world-local/src/queue.test.ts > per-run orchestrator serialization > keeps different runs, and a run’s steps, in parallel with its orchestrator
func conformSDKDeliveryRunsAndStepsInParallel(t *testing.T, open sdkOpen) {
	holder := newSDKHolder()
	world := open(holder.execute)
	defer holder.releaseAll()
	runA, runB := worlds.NewRunID(), worlds.NewRunID()
	stepA, stepB := worlds.StepID("step_1"), worlds.StepID("step_2")
	sdkQueue(t, world, sdkFlow(runA))
	sdkQueue(t, world, sdkFlow(runB))
	sdkQueue(t, world, worlds.QueueInput{RunID: runA, StepID: &stepA, StepName: "add", RunSpecVersion: worlds.SpecVersion})
	sdkQueue(t, world, worlds.QueueInput{RunID: runA, StepID: &stepB, StepName: "add", RunSpecVersion: worlds.SpecVersion})
	holder.waitFor(t, 4)
}

// SDK: packages/world-local/src/queue.test.ts > per-run orchestrator serialization > frees the run while a delivery waits out its timeoutSeconds
func conformSDKDeliveryFreesTheRunDuringARedeliveryDelay(t *testing.T, open sdkOpen) {
	var mu sync.Mutex
	var order []string
	firstID := make(chan string, 1)
	var first string
	world := open(func(_ context.Context, input worlds.QueueInput) error {
		mu.Lock()
		if first == "" {
			first = <-firstID
		}
		tag := "second"
		if input.MessageID == first {
			tag = "first"
		}
		order = append(order, tag)
		redeliver := tag == "first" && input.DeliveryCount == 1
		mu.Unlock()
		if redeliver {
			return worlds.Redeliver(2 * time.Second)
		}
		return nil
	})
	runID := worlds.NewRunID()
	firstID <- sdkQueue(t, world, sdkFlow(runID))
	got := func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), order...)
	}
	waitOrder := func(want ...string) {
		t.Helper()
		deadline := time.Now().Add(20 * time.Second)
		for len(got()) < len(want) {
			if time.Now().After(deadline) {
				t.Fatalf("order = %q, want %q", got(), want)
			}
			time.Sleep(5 * time.Millisecond)
		}
		sdkExpectTexts(t, "order", got(), want...)
	}
	waitOrder("first")
	// The first message is between deliveries, so the second one runs.
	time.Sleep(300 * time.Millisecond)
	sdkQueue(t, world, sdkFlow(runID))
	waitOrder("first", "second")
	waitOrder("first", "second", "first")
}

// SDK: packages/world-postgres/src/queue.test.ts > postgres queue http execution > preserves the first schedule when an earlier run's other keyed message is published again
func conformSDKDeliveryKeepsTheFirstSchedule(t *testing.T, open sdkOpen) {
	deliveries := make(chan worlds.QueueInput, 10)
	world := open(func(_ context.Context, input worlds.QueueInput) error {
		deliveries <- input
		return nil
	})
	due := time.Now().Add(2 * time.Second)
	timer := worlds.QueueInput{RunID: worlds.NewRunID(), RunSpecVersion: worlds.SpecVersion, IdempotencyKey: "wait_01ABC:1704067205", WaitContinuation: &worlds.WaitContinuation{CorrelationID: "wait_01ABC"}}
	timer.NotBefore = &due
	first := sdkQueue(t, world, timer)
	timer.NotBefore = nil
	if again := sdkQueue(t, world, timer); again != first {
		t.Fatalf("timer sent again = %s, want %s", again, first)
	}
	input := sdkReceive(t, deliveries, 20*time.Second)
	if early := time.Until(due); early > 50*time.Millisecond || input.MessageID != first {
		t.Fatalf("delivered message %s %s before it was due", input.MessageID, early)
	}
	sdkExpectNoDelivery(t, deliveries, 500*time.Millisecond)
}
