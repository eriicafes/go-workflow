package localworld

import (
	"context"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// queueWorld returns a prepared World that hands each delivery to execute
// instead of the runtime.
func queueWorld(t *testing.T, dir string, execute func(context.Context, worlds.QueueInput) error) *World {
	t.Helper()
	world, err := Open(Config{Dir: dir}, nil)
	if err != nil {
		t.Fatal(err)
	}
	world.execute = execute
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

func TestQueueRedelivery(t *testing.T) {
	t.Parallel()
	deliveries := make(chan worlds.QueueInput, 10)
	// The first delivery asks for a redelivery after a delay, the second
	// hands off, and the third succeeds.
	world := queueWorld(t, t.TempDir(), func(_ context.Context, input worlds.QueueInput) error {
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

func TestQueueFailureCountsAttempt(t *testing.T) {
	t.Parallel()
	deliveries := make(chan worlds.QueueInput, 10)
	world := queueWorld(t, t.TempDir(), func(_ context.Context, input worlds.QueueInput) error {
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

func TestQueueRedeliverySurvivesRestart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	deliveries := make(chan worlds.QueueInput, 10)
	world := queueWorld(t, dir, func(_ context.Context, input worlds.QueueInput) error {
		deliveries <- input
		return worlds.Redeliver(time.Hour)
	})
	result, err := world.Queue(context.Background(), worlds.QueueInput{RunID: "wrun_a"})
	if err != nil {
		t.Fatal(err)
	}
	receive(t, deliveries)
	time.Sleep(50 * time.Millisecond)
	if err := world.Close(); err != nil {
		t.Fatal(err)
	}
	var record queueRecord
	reopened := queueWorld(t, dir, func(context.Context, worlds.QueueInput) error { return nil })
	if ok, err := reopened.readOptional(queuePath(result.MessageID), &record); err != nil || !ok {
		t.Fatalf("message after restart: ok=%v err=%v", ok, err)
	}
	if record.Deliveries != 1 || record.DueAt == nil || time.Until(*record.DueAt) < 50*time.Minute {
		t.Fatalf("record after restart = %+v", record)
	}
}

func TestQueueDeduplicatesWhileMessageLives(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	deliveries := make(chan worlds.QueueInput, 10)
	world := queueWorld(t, t.TempDir(), func(_ context.Context, input worlds.QueueInput) error {
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
	again, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_a", IdempotencyKey: "k"})
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

func TestQueueSerializesFlowsOfARun(t *testing.T) {
	t.Parallel()
	var running, overlap, steps atomic.Int32
	var stepsTogether atomic.Bool
	var done sync.WaitGroup
	world := queueWorld(t, t.TempDir(), func(_ context.Context, input worlds.QueueInput) error {
		defer done.Done()
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
	ctx := context.Background()
	stepA, stepB := worlds.StepID("step_a"), worlds.StepID("step_b")
	done.Add(7)
	items := []worlds.QueueInput{
		{RunID: "wrun_a"},
		{RunID: "wrun_a"},
		{RunID: "wrun_a", CompleteWaits: []string{"w"}}, // hands off once
		{RunID: "wrun_a"},
		{RunID: "wrun_a", StepID: &stepA},
		{RunID: "wrun_a", StepID: &stepB},
	}
	if _, err := world.QueueBatch(ctx, worlds.QueueBatchInput{Items: items}); err != nil {
		t.Fatal(err)
	}
	wait := make(chan struct{})
	go func() { done.Wait(); close(wait) }()
	select {
	case <-wait:
	case <-time.After(10 * time.Second):
		t.Fatal("deliveries did not finish")
	}
	if overlap.Load() != 0 {
		t.Fatal("two flows of one run executed at once")
	}
	if !stepsTogether.Load() {
		t.Fatal("step deliveries were held back")
	}
}

func TestQueueIntermediateHopIsNotHeld(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	deliveries := make(chan worlds.QueueInput, 10)
	world := queueWorld(t, t.TempDir(), func(_ context.Context, input worlds.QueueInput) error {
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

func TestQueueRoundTripsInput(t *testing.T) {
	t.Parallel()
	deliveries := make(chan worlds.QueueInput, 1)
	world := queueWorld(t, t.TempDir(), func(_ context.Context, input worlds.QueueInput) error {
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
		StepName: "step", StepAttempt: 4, StepResent: true, StepInput: []byte{0, 1, 2}, RunSpecVersion: 9,
		RunInput:         &worlds.RunInput{WorkflowName: "wf", DeploymentID: "dpl", SpecVersion: 9},
		CompleteWaits:    []string{"w1", "w2"},
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

func TestCapabilities(t *testing.T) {
	t.Parallel()
	capabilities := (*World)(nil).Capabilities()
	if !capabilities.InBandFence || !capabilities.MaxConcurrency || capabilities.MaxQueueDelay != 0 {
		t.Fatalf("capabilities = %+v", capabilities)
	}
}

func waitQueueEmpty(t *testing.T, world *World) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		names, err := listJSON(world.root, "queue")
		if err != nil {
			t.Fatal(err)
		}
		if len(names) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("messages left: %v", names)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
