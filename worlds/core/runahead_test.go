package core_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/core"
	"github.com/eriicafes/go-workflow/worlds/testworld"
)

type aheadInput struct{ Timer, Hook bool }
type aheadOutput struct {
	Sum      int
	Payloads []string
}

func (aheadInput) Workflow(aheadOutput) {}

// aheadWorld slows each in-band step_completed down, so a flow that runs
// ahead starts the next step while it is in flight, and can commit an event
// of another writer, or answer with another time, just before one.
type aheadWorld struct {
	*testworld.World
	writing  atomic.Int32
	overlaps atomic.Int32
	loads    atomic.Int32
	mu       sync.Mutex
	// before runs once, before the first in-band step_completed commits.
	before func(ctx context.Context, runID worlds.RunID)
	// shift moves the time an in-band write is answered with.
	shift time.Duration
}

func (w *aheadWorld) EventsCreate(ctx context.Context, input worlds.EventsCreateInput) (worlds.EventResult, error) {
	if input.Event.EventType != worlds.EventTypeStepCompleted || input.Options.InBandCount == nil {
		return w.World.EventsCreate(ctx, input)
	}
	w.writing.Add(1)
	defer w.writing.Add(-1)
	time.Sleep(20 * time.Millisecond)
	w.mu.Lock()
	before := w.before
	w.before = nil
	w.mu.Unlock()
	if before != nil {
		before(ctx, input.RunID)
	}
	result, err := w.World.EventsCreate(ctx, input)
	result.CreatedAt = result.CreatedAt.Add(w.shift)
	return result, err
}

func (w *aheadWorld) RunsGet(ctx context.Context, input worlds.RunsGetInput) (worlds.Run, error) {
	w.loads.Add(1)
	return w.World.RunsGet(ctx, input)
}

// aheadRunner chains four steps, each on the previous result. With Timer set
// it first declares an hour's timer it never waits for.
func aheadRunner(world *aheadWorld) core.Runner {
	return workflow.Use(func(ctx context.Context, input aheadInput) (aheadOutput, error) {
		if input.Timer {
			_ = workflow.Timer(ctx, time.Hour)
		}
		var output aheadOutput
		hook := workflow.CreateHook[string](ctx, workflow.HookOption{Token: "ahead"})
		if input.Hook {
			// The runner takes one payload, and waits for the next only
			// after its steps.
			first, err := hook.Next(ctx)
			if err != nil {
				return output, err
			}
			output.Payloads = append(output.Payloads, first)
		}
		value := 0
		for range 4 {
			var err error
			value, err = workflow.Step(ctx, func(_ context.Context, value int) (int, error) {
				if world.writing.Load() > 0 {
					world.overlaps.Add(1)
				}
				return value + 1, nil
			}, value)
			if err != nil {
				return output, err
			}
		}
		output.Sum = value
		if input.Hook {
			second, err := hook.Next(ctx)
			if err != nil {
				return output, err
			}
			output.Payloads = append(output.Payloads, second)
		}
		return output, nil
	})
}

func startAhead(t *testing.T, ctx context.Context, input aheadInput) (*aheadWorld, *core.Runtime, *workflow.Run[aheadOutput]) {
	t.Helper()
	world := &aheadWorld{World: testworld.NewManual()}
	t.Cleanup(world.Close)
	run, err := workflow.Start(workflow.NewContext(ctx, workflow.NewClient(world)), input)
	if err != nil {
		t.Fatal(err)
	}
	return world, core.NewRuntime(world, core.Runners(aheadRunner(world))), run
}

// injectPayload resumes the hook "ahead" out-of-band.
func injectPayload(world *aheadWorld, value string) func(context.Context, worlds.RunID) {
	return func(ctx context.Context, _ worlds.RunID) {
		if err := workflow.ResumeHook(workflow.NewContext(ctx, workflow.NewClient(world.World)), "ahead", value); err != nil {
			panic(err)
		}
	}
}

// injectAttribute sets an attribute of the run out-of-band.
func injectAttribute(world *aheadWorld) func(context.Context, worlds.RunID) {
	return func(ctx context.Context, runID worlds.RunID) {
		value := "set"
		if err := world.World.RunsSetAttributes(ctx, worlds.RunsSetAttributesInput{RunID: runID, Changes: []worlds.RunsAttributeChange{{Key: "late", Value: &value}}}); err != nil {
			panic(err)
		}
	}
}

// injectWaitCompletion completes the run's open wait out-of-band, as no
// writer but the flow does.
func injectWaitCompletion(world *aheadWorld) func(context.Context, worlds.RunID) {
	return func(ctx context.Context, runID worlds.RunID) {
		waits, err := worlds.PendingWaits(ctx, world.World, runID)
		if err != nil || len(waits) != 1 {
			panic(fmt.Sprintf("pending waits = %v, %v", waits, err))
		}
		if _, err := world.World.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeWaitCompleted, CorrelationID: waits[0]}}); err != nil {
			panic(err)
		}
	}
}

func aheadEvents(t *testing.T, ctx context.Context, world *aheadWorld, runID string) []worlds.Event {
	t.Helper()
	page, err := world.World.EventsList(ctx, worlds.EventsListInput{RunID: worlds.RunID(runID)})
	if err != nil {
		t.Fatal(err)
	}
	return page.Data
}

func TestFlowRunsAheadOfItsInlineResults(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	world, runtime, run := startAhead(t, ctx, aheadInput{})
	drain(t, ctx, world.World, runtime)
	output, err := run.Value(workflow.NewContext(ctx, workflow.NewClient(world)))
	if err != nil || output.Sum != 4 {
		t.Fatalf("output = %#v err = %v", output, err)
	}
	// The steps after the first ran while the result before them was being
	// written.
	if world.overlaps.Load() == 0 {
		t.Fatal("no step ran while an earlier result was being written")
	}
}

func TestRunAheadOffWritesEachResultFirst(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	world := &aheadWorld{World: testworld.NewManual()}
	defer world.Close()
	run, err := workflow.Start(workflow.NewContext(ctx, workflow.NewClient(world)), aheadInput{})
	if err != nil {
		t.Fatal(err)
	}
	drain(t, ctx, world.World, core.NewRuntime(world, core.Runners(aheadRunner(world)), core.WithRunAheadDepth(0)))
	if output, err := run.Value(workflow.NewContext(ctx, workflow.NewClient(world))); err != nil || output.Sum != 4 || world.overlaps.Load() != 0 {
		t.Fatalf("output = %#v err = %v overlaps = %d", output, err, world.overlaps.Load())
	}
}

func TestRunAheadFoldsAnInertEventBelowItsWrites(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	world, runtime, run := startAhead(t, ctx, aheadInput{})
	// An attribute write, which no runner reads, lands below a write ahead.
	world.before = injectAttribute(world)
	delivery := world.QueuedMessages()[0]
	if err := runtime.Execute(ctx, delivery.Input); err != nil {
		t.Fatal(err)
	}
	if loads := world.loads.Load(); loads != 1 {
		t.Fatalf("the delivery read the run %d times, want once: it went on", loads)
	}
	output, err := run.Value(workflow.NewContext(ctx, workflow.NewClient(world)))
	if err != nil || output.Sum != 4 {
		t.Fatalf("output = %#v err = %v", output, err)
	}
	if status, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: worlds.RunID(run.ID())}); err != nil || status.Attributes["late"] != "set" {
		t.Fatalf("run = %#v err = %v", status, err)
	}
}

func TestRunAheadStopsAtAnEventThatCouldChangeADecision(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	world, runtime, run := startAhead(t, ctx, aheadInput{Timer: true})
	// A timer an hour away keeps the boundary inert, but another writer
	// completes it below a write ahead, so the delivery starts again.
	world.before = injectWaitCompletion(world)
	message := world.QueuedMessages()[0]
	if err := runtime.Execute(ctx, message.Input); err != nil {
		t.Fatal(err)
	}
	if loads := world.loads.Load(); loads < 2 {
		t.Fatalf("the delivery read the run %d times, want it to start again", loads)
	}
	if err := world.Acknowledge(ctx, message.ID); err != nil {
		t.Fatal(err)
	}
	drain(t, ctx, world.World, runtime)
	output, err := run.Value(workflow.NewContext(ctx, workflow.NewClient(world)))
	if err != nil || output.Sum != 4 {
		t.Fatalf("output = %#v err = %v", output, err)
	}
}

func TestRunAheadStopsForAWorldThatStoresAnotherTime(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	world, runtime, run := startAhead(t, ctx, aheadInput{})
	world.shift = time.Second
	drain(t, ctx, world.World, runtime)
	output, err := run.Value(workflow.NewContext(ctx, workflow.NewClient(world)))
	if err != nil || output.Sum != 4 {
		t.Fatalf("output = %#v err = %v", output, err)
	}
	// The World is not run ahead of again: each step after the first
	// waits for the result before it.
	world.overlaps.Store(0)
	again, err := workflow.Start(workflow.NewContext(ctx, workflow.NewClient(world)), aheadInput{})
	if err != nil {
		t.Fatal(err)
	}
	drain(t, ctx, world.World, runtime)
	if output, err := again.Value(workflow.NewContext(ctx, workflow.NewClient(world))); err != nil || output.Sum != 4 || world.overlaps.Load() != 0 {
		t.Fatalf("second run output = %#v err = %v overlaps = %d", output, err, world.overlaps.Load())
	}
}

func TestRunAheadFoldsAPayloadOfAHookNobodyWaitsOn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	world, runtime, run := startAhead(t, ctx, aheadInput{Hook: true})
	deliverNext(t, ctx, world.World, runtime)
	injectPayload(world, "first")(ctx, worlds.RunID(run.ID()))
	// The runner took the first payload, and the second lands below a write
	// ahead while it runs its steps, so the delivery goes on and the runner
	// takes it once it waits again.
	world.before = injectPayload(world, "second")
	world.loads.Store(0)
	message, ok := nextDue(world.World)
	if !ok {
		t.Fatal("no delivery after the payload")
	}
	if err := runtime.Execute(ctx, message.Input); err != nil {
		t.Fatal(err)
	}
	if loads := world.loads.Load(); loads != 1 {
		t.Fatalf("the delivery read the run %d times, want once: it went on", loads)
	}
	if err := world.Acknowledge(ctx, message.ID); err != nil {
		t.Fatal(err)
	}
	drain(t, ctx, world.World, runtime)
	output, err := run.Value(workflow.NewContext(ctx, workflow.NewClient(world)))
	if err != nil || output.Sum != 4 || len(output.Payloads) != 2 || output.Payloads[0] != "first" || output.Payloads[1] != "second" {
		t.Fatalf("output = %#v err = %v", output, err)
	}
}
