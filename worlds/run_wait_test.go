package worlds_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/testworld"
)

type durableWaitInput struct{ Timeout bool }
type durableWaitOutput struct{}

func (durableWaitInput) Workflow(durableWaitOutput) {}

func TestDurableWaitCompletesAndCancellingWaitCancelsRun(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, input durableWaitInput) (durableWaitOutput, error) {
		if input.Timeout {
			_, err := workflow.Timeout(ctx, time.Millisecond).Await(ctx)
			return durableWaitOutput{}, err
		}
		_, err := workflow.Timer(ctx, time.Millisecond).Await(ctx)
		return durableWaitOutput{}, err
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	completed, err := workflow.Start(ctx, durableWaitInput{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := completed.Await(ctx); err != nil {
		t.Fatal(err)
	}
	cancelled, err := workflow.Start(ctx, durableWaitInput{Timeout: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cancelled.Await(ctx); !errors.Is(err, workflow.ErrRunCancelled) {
		t.Fatalf("cancelling wait error = %v", err)
	}
	events, err := world.EventsList(ctx, worlds.EventsListInput{RunID: worlds.RunID(cancelled.ID())})
	if err != nil {
		t.Fatal(err)
	}
	seenWait, seenCancel := false, false
	for _, event := range events.Data {
		seenWait = seenWait || event.EventType == worlds.EventTypeWaitCompleted
		seenCancel = seenCancel || event.EventType == worlds.EventTypeRunCancelled
	}
	if !seenWait || !seenCancel {
		t.Fatalf("timeout events = %#v", events.Data)
	}
}

func TestWakeRunCompletesOnlySelectedWaits(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, _ durableWaitInput) (durableWaitOutput, error) {
		_, err := workflow.Join(ctx, workflow.Timer(ctx, time.Hour), workflow.Timer(ctx, time.Hour))
		return durableWaitOutput{}, err
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, durableWaitInput{})
	if err != nil {
		t.Fatal(err)
	}
	var waitIDs []string
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		events, err := world.EventsList(ctx, worlds.EventsListInput{RunID: worlds.RunID(run.ID())})
		if err != nil {
			t.Fatal(err)
		}
		waitIDs = waitIDs[:0]
		for _, event := range events.Data {
			if event.EventType == worlds.EventTypeWaitCreated {
				waitIDs = append(waitIDs, event.CorrelationID)
			}
		}
		if len(waitIDs) == 2 {
			break
		}
	}
	if len(waitIDs) != 2 {
		t.Fatalf("wait IDs = %#v", waitIDs)
	}
	if count, err := worlds.WakeRun(ctx, world, worlds.RunID(run.ID()), waitIDs[:1]); err != nil || count != 1 {
		t.Fatalf("selected wake count = %d, %v", count, err)
	}
	if done, err := run.Done(ctx); err != nil || done {
		t.Fatalf("run done after one wake = %t, %v", done, err)
	}
	if count, err := worlds.WakeRun(ctx, world, worlds.RunID(run.ID()), nil); err != nil || count != 1 {
		t.Fatalf("remaining wake count = %d, %v", count, err)
	}
	if _, err := run.Await(ctx); err != nil {
		t.Fatal(err)
	}
}
