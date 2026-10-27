package worlds_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/testworld"
)

type inlineStepInput struct{}
type inlineStepOutput struct{ Captured time.Time }

func (inlineStepInput) Workflow(inlineStepOutput) {}

func TestDurableInlineStepRecordsNowInOneAttempt(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, _ inlineStepInput) (inlineStepOutput, error) {
		captured, err := workflow.Now(ctx)
		return inlineStepOutput{Captured: captured}, err
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, inlineStepInput{})
	if err != nil {
		t.Fatal(err)
	}
	output, err := run.Value(ctx)
	if err != nil || output.Captured.IsZero() {
		t.Fatalf("inline output = %#v, %v", output, err)
	}
	events, err := world.EventsList(ctx, worlds.EventsListInput{RunID: worlds.RunID(run.ID())})
	if err != nil {
		t.Fatal(err)
	}
	counts := map[worlds.EventType]int{}
	for _, event := range events.Data {
		counts[event.EventType]++
	}
	if counts[worlds.EventTypeStepCreated] != 1 || counts[worlds.EventTypeStepStarted] != 1 || counts[worlds.EventTypeStepCompleted] != 1 {
		t.Fatalf("inline events = %#v", events.Data)
	}
}

func TestInlineStepAttemptBelongsToTheFlowThatStartedIt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var attempts atomic.Int32
	started := make(chan struct{})
	runner := workflow.Use(func(ctx context.Context, _ inlineInput) (inlineOutput, error) {
		value, err := workflow.InlineStep(ctx, func(ctx context.Context, value int) (int, error) {
			if attempts.Add(1) == 1 {
				close(started)
				<-ctx.Done()
				return 0, ctx.Err()
			}
			return value, nil
		}, 7)
		return inlineOutput{Values: []int{value}}, err
	})
	world, runtime, runID, delivery := startInline(t, ctx, runner, inlineInput{})
	crash(t, ctx, runtime, delivery.Input, started)

	// Another flow leaves the attempt to its owner until the lease ends.
	other := delivery.Input
	other.MessageID = "other"
	if err := runtime.Execute(ctx, other); err != nil {
		t.Fatal(err)
	}
	if attempts.Load() != 1 {
		t.Fatalf("attempts = %d, want the owner's only", attempts.Load())
	}

	// The owner's redelivery runs it again at once.
	if err := runtime.Execute(ctx, delivery.Input); err != nil {
		t.Fatal(err)
	}
	output, err := runOutput(t, ctx, world, runID)
	if err != nil || output.Values[0] != 7 || attempts.Load() != 2 {
		t.Fatalf("output=%v err=%v attempts=%d", output, err, attempts.Load())
	}
}
