package core_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/core"
	"github.com/eriicafes/go-workflow/worlds/testworld"
)

// TestMain turns lingering off: the tests drive deliveries by hand, or check
// what a delivery leaves once it suspends. The tests of lingering turn it on.
func TestMain(m *testing.M) {
	core.LingerDuration = 0
	os.Exit(m.Run())
}

// withLinger turns lingering on for a test.
func withLinger(t *testing.T) {
	core.LingerDuration = 5 * time.Second
	t.Cleanup(func() { core.LingerDuration = 0 })
}

type lingerInput struct{ Token string }
type lingerOutput struct{ Payloads []string }

func (lingerInput) Workflow(lingerOutput) {}

// lingerRunner takes two payloads of its hook.
var lingerRunner = workflow.Use(func(ctx context.Context, input lingerInput) (lingerOutput, error) {
	hook := workflow.CreateHook[string](ctx, workflow.HookOption{Token: input.Token})
	var output lingerOutput
	for range 2 {
		payload, err := hook.Next(ctx)
		if err != nil {
			return output, err
		}
		output.Payloads = append(output.Payloads, payload)
	}
	return output, nil
})

func TestSuspendedFlowLingersAndTakesTheNextPayloadInTheSameDelivery(t *testing.T) {
	withLinger(t)
	world := testworld.NewManual(lingerRunner)
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), 10*time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, lingerInput{Token: "linger"})
	if err != nil {
		t.Fatal(err)
	}
	resumed := make(chan error, 1)
	go func() {
		if err := workflow.ResumeHookWithRetries(ctx, "linger", "first", 100, 20*time.Millisecond); err != nil {
			resumed <- err
			return
		}
		resumed <- workflow.ResumeHookWithRetries(ctx, "linger", "second", 100, 20*time.Millisecond)
	}()
	// One delivery takes both payloads: it lingers on the open hook after
	// each pass, and each payload starts another.
	if err := core.NewRuntime(world, worlds.Runners(lingerRunner)).Execute(ctx, world.QueuedMessages()[0].Input); err != nil {
		t.Fatal(err)
	}
	if err := <-resumed; err != nil {
		t.Fatal(err)
	}
	if output, err := run.Value(ctx); err != nil || len(output.Payloads) != 2 {
		t.Fatalf("output = %+v err = %v", output, err)
	}
}

func TestSuspendedFlowStopsLingeringWhenNothingArrives(t *testing.T) {
	withLinger(t)
	core.HookLingerDuration = 50 * time.Millisecond
	defer func() { core.HookLingerDuration = 5 * time.Second }()
	world := testworld.NewManual(lingerRunner)
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), 10*time.Second)
	defer cancel()
	if _, err := workflow.Start(ctx, lingerInput{Token: "idle"}); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if err := core.NewRuntime(world, worlds.Runners(lingerRunner)).Execute(ctx, world.QueuedMessages()[0].Input); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed < 50*time.Millisecond || elapsed > 2*time.Second {
		t.Fatalf("the delivery took %v, want it to linger for the hook window", elapsed)
	}
}

// pollOnlyWorld hides the World's push, so a delivery never lingers.
type pollOnlyWorld struct{ worlds.World }

func TestSuspendedFlowDoesNotLingerWithoutAPush(t *testing.T) {
	withLinger(t)
	world := testworld.NewManual(lingerRunner)
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), 10*time.Second)
	defer cancel()
	if _, err := workflow.Start(ctx, lingerInput{Token: "nopush"}); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if err := core.NewRuntime(pollOnlyWorld{world}, worlds.Runners(lingerRunner)).Execute(ctx, world.QueuedMessages()[0].Input); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("the delivery took %v, want no linger", elapsed)
	}
}
