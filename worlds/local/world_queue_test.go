package localworld

import (
	"context"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// queueWorld returns a prepared World that hands each delivery to execute
// instead of the runtime, and retries failed deliveries at once.
func queueWorld(t *testing.T, config Config, execute func(context.Context, worlds.QueueInput) error) *World {
	t.Helper()
	if config.Dir == "" {
		config.Dir = t.TempDir()
	}
	config.SkipRecovery = true
	world, err := Open(config, nil)
	if err != nil {
		t.Fatal(err)
	}
	world.execute = execute
	world.queue.retry = func(int) time.Duration { return 10 * time.Millisecond }
	if err := world.Prepare(context.Background()); err != nil {
		_ = world.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = world.Close() })
	return world
}

func receive(t *testing.T, deliveries <-chan worlds.QueueInput) worlds.QueueInput {
	t.Helper()
	select {
	case input := <-deliveries:
		return input
	case <-time.After(5 * time.Second):
		t.Fatal("no delivery")
		return worlds.QueueInput{}
	}
}

func expectNone(t *testing.T, deliveries <-chan worlds.QueueInput) {
	t.Helper()
	select {
	case input := <-deliveries:
		t.Fatalf("unexpected delivery %+v", input)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestRetryDelay(t *testing.T) {
	for failed, want := range map[int]time.Duration{1: 5 * time.Second, 32: 5 * time.Second, 33: 10 * time.Second, 34: 20 * time.Second, 36: 60 * time.Second, 200: 60 * time.Second} {
		if got := retryDelay(failed); got != want {
			t.Fatalf("retryDelay(%d) = %s, want %s", failed, got, want)
		}
	}
}

func TestRedeliveryKeepsTheMessageAndCountsAttempts(t *testing.T) {
	t.Parallel()
	deliveries := make(chan worlds.QueueInput, 10)
	// The first delivery asks for a redelivery after a delay, the second
	// hands off, and the third succeeds.
	world := queueWorld(t, Config{}, func(_ context.Context, input worlds.QueueInput) error {
		deliveries <- input
		switch input.DeliveryCount {
		case 1:
			return worlds.Redeliver(50 * time.Millisecond)
		case 2:
			return worlds.Redeliver(0)
		}
		return nil
	})
	result, err := world.Queue(context.Background(), worlds.QueueInput{RunID: "wrun_a"})
	if err != nil {
		t.Fatal(err)
	}
	first := receive(t, deliveries)
	start := time.Now()
	second := receive(t, deliveries)
	if time.Since(start) < 40*time.Millisecond {
		t.Fatal("redelivered before its delay")
	}
	third := receive(t, deliveries)
	for index, value := range []struct {
		input                  worlds.QueueInput
		deliveryCount, attempt int
	}{{first, 1, 1}, {second, 2, 2}, {third, 3, 2}} {
		if value.input.MessageID != result.MessageID || value.input.DeliveryCount != value.deliveryCount || value.input.Attempt != value.attempt {
			t.Fatalf("delivery %d = message %s count %d attempt %d, want %s %d %d", index+1, value.input.MessageID, value.input.DeliveryCount, value.input.Attempt, result.MessageID, value.deliveryCount, value.attempt)
		}
	}
	waitQueueEmpty(t, world)
}

func TestFailedDeliveryCountsAnAttempt(t *testing.T) {
	t.Parallel()
	deliveries := make(chan worlds.QueueInput, 10)
	world := queueWorld(t, Config{}, func(_ context.Context, input worlds.QueueInput) error {
		deliveries <- input
		if input.DeliveryCount == 1 {
			return context.DeadlineExceeded
		}
		return nil
	})
	if _, err := world.Queue(context.Background(), worlds.QueueInput{RunID: "wrun_a"}); err != nil {
		t.Fatal(err)
	}
	receive(t, deliveries)
	if second := receive(t, deliveries); second.DeliveryCount != 2 || second.Attempt != 2 {
		t.Fatalf("retry = count %d attempt %d", second.DeliveryCount, second.Attempt)
	}
}

func TestQueueLosesMessagesOnClose(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	deliveries := make(chan worlds.QueueInput, 10)
	world := queueWorld(t, Config{Dir: dir}, func(_ context.Context, input worlds.QueueInput) error {
		deliveries <- input
		return nil
	})
	later := time.Now().Add(200 * time.Millisecond)
	if _, err := world.Queue(context.Background(), worlds.QueueInput{RunID: "wrun_a", NotBefore: &later}); err != nil {
		t.Fatal(err)
	}
	if err := world.Close(); err != nil {
		t.Fatal(err)
	}
	queueWorld(t, Config{Dir: dir}, func(_ context.Context, input worlds.QueueInput) error {
		deliveries <- input
		return nil
	})
	time.Sleep(time.Until(later))
	expectNone(t, deliveries)
}

func TestQueueDeduplicatesOnlyWhileTheMessageLives(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	deliveries := make(chan worlds.QueueInput, 10)
	world := queueWorld(t, Config{}, func(_ context.Context, input worlds.QueueInput) error {
		deliveries <- input
		<-release
		return nil
	})
	ctx := context.Background()
	first, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_a", IdempotencyKey: "k"})
	if err != nil {
		t.Fatal(err)
	}
	receive(t, deliveries)
	// The message is executing but lives until it is acknowledged.
	again, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_b", IdempotencyKey: "k"})
	if err != nil || again.MessageID != first.MessageID {
		t.Fatalf("queue while executing = %v err=%v, want %s", again, err, first.MessageID)
	}
	close(release)
	waitQueueEmpty(t, world)
	after, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_a", IdempotencyKey: "k"})
	if err != nil || after.MessageID == first.MessageID {
		t.Fatalf("queue after acknowledgement = %v err=%v", after, err)
	}
	receive(t, deliveries)
}

func TestQueueDeliversOneFlowOfARunAtATime(t *testing.T) {
	t.Parallel()
	var running, overlap, steps atomic.Int32
	var stepsTogether atomic.Bool
	world := queueWorld(t, Config{}, func(_ context.Context, input worlds.QueueInput) error {
		if input.StepID != nil {
			// Both steps must be executing at once to finish.
			if steps.Add(1) == 2 {
				stepsTogether.Store(true)
			}
			deadline := time.Now().Add(2 * time.Second)
			for steps.Load() < 2 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			return nil
		}
		if running.Add(1) > 1 {
			overlap.Store(1)
		}
		time.Sleep(20 * time.Millisecond)
		running.Add(-1)
		if input.DeliveryCount == 1 && input.CompleteWaits != nil {
			return worlds.Redeliver(0)
		}
		return nil
	})
	stepA, stepB := worlds.StepID("step_a"), worlds.StepID("step_b")
	items := []worlds.QueueInput{
		{RunID: "wrun_a"},
		{RunID: "wrun_a"},
		{RunID: "wrun_a", CompleteWaits: []string{"w"}}, // hands off once
		{RunID: "wrun_a"},
		{RunID: "wrun_a", StepID: &stepA},
		{RunID: "wrun_a", StepID: &stepB},
	}
	if _, err := world.QueueBatch(context.Background(), worlds.QueueBatchInput{Items: items}); err != nil {
		t.Fatal(err)
	}
	waitQueueEmpty(t, world)
	if overlap.Load() != 0 {
		t.Fatal("two flows of one run executed at once")
	}
	if !stepsTogether.Load() {
		t.Fatal("step deliveries were held back")
	}
}

func TestQueueCoalescesFlowsWaitingBehindADelivery(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	deliveries := make(chan worlds.QueueInput, 20)
	world := queueWorld(t, Config{}, func(_ context.Context, input worlds.QueueInput) error {
		deliveries <- input
		if input.DeliveryCount == 1 && input.RunInput == nil && len(input.CompleteWaits) == 0 {
			<-release
		}
		return nil
	})
	ctx := context.Background()
	holder, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_a"})
	if err != nil {
		t.Fatal(err)
	}
	receive(t, deliveries)
	var coalesced []string
	for range 3 {
		result, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_a"})
		if err != nil {
			t.Fatal(err)
		}
		coalesced = append(coalesced, result.MessageID)
	}
	wake, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_a", CompleteWaits: []string{"w"}})
	if err != nil {
		t.Fatal(err)
	}
	waitWaiters(t, world, "wrun_a", 4)
	close(release)
	// The holder re-runs once for the three flows it took, then the wake,
	// which names waits the log does not, is delivered on its own.
	rerun := receive(t, deliveries)
	if rerun.MessageID != holder.MessageID || rerun.DeliveryCount != 2 || rerun.Attempt != 1 {
		t.Fatalf("re-run = message %s count %d attempt %d, want %s 2 1", rerun.MessageID, rerun.DeliveryCount, rerun.Attempt, holder.MessageID)
	}
	if own := receive(t, deliveries); own.MessageID != wake.MessageID {
		t.Fatalf("delivered %s, want the wake %s", own.MessageID, wake.MessageID)
	}
	waitQueueEmpty(t, world)
	expectNone(t, deliveries)
	_ = coalesced
}

func TestQueueHandoffKeepsTheGateAndCoversWaitingFlows(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	deliveries := make(chan worlds.QueueInput, 20)
	world := queueWorld(t, Config{}, func(_ context.Context, input worlds.QueueInput) error {
		deliveries <- input
		if input.DeliveryCount == 1 && input.CompleteWaits != nil {
			<-release
			return worlds.Redeliver(0)
		}
		return nil
	})
	ctx := context.Background()
	holder, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_a", CompleteWaits: []string{"w"}})
	if err != nil {
		t.Fatal(err)
	}
	receive(t, deliveries)
	if _, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_a"}); err != nil {
		t.Fatal(err)
	}
	waitWaiters(t, world, "wrun_a", 1)
	close(release)
	handoff := receive(t, deliveries)
	if handoff.MessageID != holder.MessageID || handoff.DeliveryCount != 2 || handoff.Attempt != 1 {
		t.Fatalf("handoff = message %s count %d attempt %d, want %s 2 1", handoff.MessageID, handoff.DeliveryCount, handoff.Attempt, holder.MessageID)
	}
	waitQueueEmpty(t, world)
	expectNone(t, deliveries)
}

func TestQueueSleepsDelaysWithoutASlot(t *testing.T) {
	t.Parallel()
	deliveries := make(chan worlds.QueueInput, 10)
	world := queueWorld(t, Config{Concurrency: 1}, func(_ context.Context, input worlds.QueueInput) error {
		deliveries <- input
		return nil
	})
	ctx := context.Background()
	later := time.Now().Add(time.Hour)
	if _, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_a", NotBefore: &later}); err != nil {
		t.Fatal(err)
	}
	if _, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_b"}); err != nil {
		t.Fatal(err)
	}
	if got := receive(t, deliveries); got.RunID != "wrun_b" {
		t.Fatalf("delivered %s, want wrun_b", got.RunID)
	}
}

func TestQueueIntermediateHopIsNotHeld(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	deliveries := make(chan worlds.QueueInput, 10)
	world := queueWorld(t, Config{}, func(_ context.Context, input worlds.QueueInput) error {
		deliveries <- input
		if input.WaitContinuation == nil {
			<-release
		}
		return nil
	})
	defer close(release)
	ctx := context.Background()
	if _, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_a"}); err != nil {
		t.Fatal(err)
	}
	receive(t, deliveries)
	resumeAt, hopAt := time.Now().Add(time.Hour), time.Now()
	if _, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_a", WaitContinuation: &worlds.WaitContinuation{CorrelationID: "w", ResumeAt: &resumeAt, HopAt: &hopAt}}); err != nil {
		t.Fatal(err)
	}
	if hop := receive(t, deliveries); hop.WaitContinuation == nil {
		t.Fatalf("got %+v, want the hop", hop)
	}
}

func TestQueueRoundTripsEveryField(t *testing.T) {
	t.Parallel()
	deliveries := make(chan worlds.QueueInput, 1)
	world := queueWorld(t, Config{}, func(_ context.Context, input worlds.QueueInput) error {
		deliveries <- input
		return nil
	})
	stepID := worlds.StepID("step_a")
	notBefore := time.Now().Add(-time.Second).UTC().Truncate(time.Millisecond)
	resumeAt := notBefore.Add(time.Hour)
	hopAt := notBefore.Add(time.Minute)
	input := worlds.QueueInput{
		RunID: "wrun_a", QueueName: "q", WorkflowName: "wf", StepID: &stepID, NotBefore: &notBefore,
		IdempotencyKey: "key", DeploymentID: "dpl", DeploymentMismatchRetryCount: 2, ReplayDivergenceCount: 3,
		StepName: "step", StepAttempt: 4, StepResent: true, RunSpecVersion: 9,
		CompleteWaits:    []string{"w1", "w2"},
		Recovery:         true,
		WaitContinuation: &worlds.WaitContinuation{CorrelationID: "w", Attempt: 5, ResumeAt: &resumeAt, HopAt: &hopAt},
		Retention:        time.Hour,
	}
	result, err := world.Queue(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	got := receive(t, deliveries)
	want := input
	want.MessageID, want.DeliveryCount, want.Attempt = result.MessageID, 1, 1
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("delivered %+v\nwant %+v", got, want)
	}
}

func TestCapabilitiesReportTheQueueContract(t *testing.T) {
	t.Parallel()
	capabilities := (*World)(nil).Capabilities()
	if !capabilities.InBandFence || !capabilities.MaxConcurrency || capabilities.MaxQueueDelay != 0 {
		t.Fatalf("capabilities = %+v", capabilities)
	}
}

// waitWaiters waits until count deliveries of the run wait behind its gate.
func waitWaiters(t *testing.T, world *World, runID worlds.RunID, count int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		world.queue.gate.mu.Lock()
		waiting := len(world.queue.gate.runs[runID])
		world.queue.gate.mu.Unlock()
		if waiting == count {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d waiters, want %d", waiting, count)
		}
		time.Sleep(time.Millisecond)
	}
}

func waitQueueEmpty(t *testing.T, world *World) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for world.queue.live.Load() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("messages left: %d", world.queue.live.Load())
		}
		time.Sleep(5 * time.Millisecond)
	}
}
