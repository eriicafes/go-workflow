package workflowtest

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/core"
)

// The run-ahead cases run a flow against the World with their own Runtime,
// delivering its message themselves, and stand in for another writer by
// appending to the World while a write the flow ran ahead of is in flight.
// They queue nothing the World delivers in the meantime.

type runAheadInput struct {
	Timer, Hook bool
	// Token is the token of the run's hook.
	Token string
}

type runAheadOutput struct {
	Sum      int
	Payloads []string
}

func (runAheadInput) Workflow(runAheadOutput) {}

// runAheadWorld slows each in-band step_completed down, so a flow that runs
// ahead starts its next step before the write commits, and lets a case
// append another writer's event, or answer with another time, just before
// one commits.
type runAheadWorld struct {
	worlds.World
	// committed counts the in-band step_completed writes that committed,
	// and overlaps the steps that started before the result of the step
	// before them committed.
	committed atomic.Int32
	overlaps  atomic.Int32
	// bodies counts the step bodies run.
	bodies atomic.Int32
	loads  atomic.Int32
	mu     sync.Mutex
	// before runs its first function before the next in-band
	// step_completed commits, and each next one before the one after.
	before []func(context.Context)
	// shift moves the time an in-band step_completed is answered with.
	shift time.Duration
	// incomplete answers an in-band step_completed with no events below
	// it, and MoreEvents set when there are some.
	incomplete bool
}

func (w *runAheadWorld) EventsCreate(ctx context.Context, input worlds.EventsCreateInput) (worlds.EventResult, error) {
	if input.Event.EventType != worlds.EventTypeStepCompleted || input.Options.InBandCount == nil {
		return w.World.EventsCreate(ctx, input)
	}
	time.Sleep(20 * time.Millisecond)
	w.mu.Lock()
	var before func(context.Context)
	if len(w.before) > 0 {
		before, w.before = w.before[0], w.before[1:]
	}
	w.mu.Unlock()
	if before != nil {
		before(ctx)
	}
	result, err := w.World.EventsCreate(ctx, input)
	if err == nil {
		w.committed.Add(1)
	}
	result.CreatedAt = result.CreatedAt.Add(w.shift)
	if w.incomplete && len(result.Events) > 0 {
		result.Events, result.MoreEvents = nil, true
	}
	return result, err
}

func (w *runAheadWorld) RunsGet(ctx context.Context, input worlds.RunsGetInput) (worlds.Run, error) {
	w.loads.Add(1)
	return w.World.RunsGet(ctx, input)
}

// runAheadRunner chains four steps, each on the previous result. With Timer
// set it first declares an hour's timer it never waits for, and with Hook set
// it takes one payload of its hook before the steps and one after.
func runAheadRunner(world *runAheadWorld) worlds.Runner {
	return workflow.Use(func(ctx context.Context, input runAheadInput) (runAheadOutput, error) {
		var output runAheadOutput
		if input.Timer {
			_ = workflow.Timer(ctx, time.Hour)
		}
		hook := workflow.CreateHook[string](ctx, workflow.HookOption{Token: input.Token})
		if input.Hook {
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
				world.bodies.Add(1)
				// value counts the steps before this one.
				if int(world.committed.Load()) < value {
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

// runAheadRun is a run of runAheadRunner and the Runtime that delivers it.
type runAheadRun struct {
	t       *testing.T
	world   *runAheadWorld
	runtime worlds.BoundExecutor
	runID   worlds.RunID
	count   int
}

func startRunAhead(t *testing.T, world *runAheadWorld, input runAheadInput) *runAheadRun {
	t.Helper()
	runID := worlds.NewRunID()
	input.Token = "runahead:" + string(runID)
	encoded, err := worlds.EncodePayload([1]runAheadInput{input}, worlds.PayloadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := world.World.EventsCreateRun(context.Background(), worlds.EventsCreateRunInput{RunID: runID, WorkflowName: worlds.WorkflowName[runAheadInput](), Input: encoded}); err != nil {
		t.Fatal(err)
	}
	return &runAheadRun{t: t, world: world, runtime: worlds.BoundExecutor{Executor: worlds.Runners(runAheadRunner(world)), Runtime: core.NewRuntime(world)}, runID: runID}
}

// deliver executes one delivery of the run's flow message and returns how
// often it read the run.
func (r *runAheadRun) deliver() int32 {
	r.t.Helper()
	r.count++
	r.world.loads.Store(0)
	input := worlds.QueueInput{RunID: r.runID, WorkflowName: worlds.WorkflowName[runAheadInput](), MessageID: "runahead", DeliveryCount: r.count, Attempt: r.count}
	if err := r.runtime.Execute(context.Background(), input); err != nil {
		r.t.Fatalf("delivery %d: %v", r.count, err)
	}
	return r.world.loads.Load()
}

// output returns the output the run completed with.
func (r *runAheadRun) output() runAheadOutput {
	r.t.Helper()
	run, err := r.world.World.RunsGet(context.Background(), worlds.RunsGetInput{RunID: r.runID})
	if err != nil || run.Status != worlds.StatusCompleted {
		r.t.Fatalf("run = %+v err = %v, want it completed", run, err)
	}
	var output runAheadOutput
	if err := worlds.DecodePayload(run.Output, nil, &output); err != nil {
		r.t.Fatal(err)
	}
	return output
}

// payload appends a payload to the run's hook, as ResumeHook does, without
// queueing a delivery.
func (r *runAheadRun) payload(value string) func(context.Context) {
	return func(ctx context.Context) {
		hook, err := r.world.World.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: "runahead:" + string(r.runID)})
		if err != nil {
			r.t.Error(err)
			return
		}
		encoded, err := worlds.EncodePayload(value, worlds.PayloadOptions{})
		if err != nil {
			r.t.Error(err)
			return
		}
		data, _ := worlds.EncodeHookReceived(encoded, hook.ResumeContext.WorkflowName)
		if _, err := r.world.World.EventsCreate(ctx, worlds.EventsCreateInput{RunID: r.runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeHookReceived, CorrelationID: string(hook.HookID), Data: data}}); err != nil {
			r.t.Error(err)
		}
	}
}

func conformRunAheadOverlapsWrites(t *testing.T, base worlds.World) {
	world := &runAheadWorld{World: base}
	run := startRunAhead(t, world, runAheadInput{})
	run.deliver()
	if output := run.output(); output.Sum != 4 {
		t.Fatalf("output = %+v", output)
	}
	// The steps after the first ran before the result before them was
	// written.
	if world.overlaps.Load() == 0 {
		t.Fatal("no step ran before an earlier result was written")
	}
}

func conformRunAheadFoldsInertEvents(t *testing.T, base worlds.World) {
	world := &runAheadWorld{World: base}
	run := startRunAhead(t, world, runAheadInput{})
	// An attribute write, which no runner reads, lands below a write ahead.
	world.before = []func(context.Context){setAttribute(t, world, run, "late")}
	if loads := run.deliver(); loads != 1 {
		t.Fatalf("the delivery read the run %d times, want once: it went on", loads)
	}
	if output := run.output(); output.Sum != 4 {
		t.Fatalf("output = %+v", output)
	}
	if world.Capabilities().SetAttributes {
		if stored, err := world.World.RunsGet(context.Background(), worlds.RunsGetInput{RunID: run.runID}); err != nil || stored.Attributes["late"] != "set" {
			t.Fatalf("run = %+v err = %v", stored, err)
		}
	}
}

func conformRunAheadFoldsUnwatchedHookPayloads(t *testing.T, base worlds.World) {
	world := &runAheadWorld{World: base}
	run := startRunAhead(t, world, runAheadInput{Hook: true})
	run.deliver()
	run.payload("first")(context.Background())
	// The runner took the first payload, and the second lands below a write
	// ahead while it runs its steps, when nothing waits on the hook, so the
	// delivery goes on and the runner takes it once it waits again.
	world.before = []func(context.Context){run.payload("second")}
	if loads := run.deliver(); loads != 1 {
		t.Fatalf("the delivery read the run %d times, want once: it went on", loads)
	}
	if output := run.output(); output.Sum != 4 || len(output.Payloads) != 2 || output.Payloads[0] != "first" || output.Payloads[1] != "second" {
		t.Fatalf("output = %+v", output)
	}
}

func conformRunAheadStopsAtPathChangingEvents(t *testing.T, base worlds.World) {
	world := &runAheadWorld{World: base}
	run := startRunAhead(t, world, runAheadInput{Timer: true})
	// A timer an hour away keeps the boundary inert, but another writer
	// completes it below a write ahead, so the delivery starts again from a
	// fresh read of the log.
	world.before = []func(context.Context){func(ctx context.Context) {
		waits, err := worlds.PendingWaits(ctx, world.World, run.runID)
		if err != nil || len(waits) != 1 {
			t.Errorf("pending waits = %v err = %v", waits, err)
			return
		}
		if _, err := world.World.EventsCreate(ctx, worlds.EventsCreateInput{RunID: run.runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeWaitCompleted, CorrelationID: waits[0]}}); err != nil {
			t.Error(err)
		}
	}}
	if loads := run.deliver(); loads < 2 {
		t.Fatalf("the delivery read the run %d times, want it to start again", loads)
	}
	if output := run.output(); output.Sum != 4 {
		t.Fatalf("output = %+v", output)
	}
}

func conformRunAheadStopsForAnotherTime(t *testing.T, base worlds.World) {
	world := &runAheadWorld{World: base}
	world.shift = time.Second
	run := startRunAhead(t, world, runAheadInput{})
	run.deliver()
	if output := run.output(); output.Sum != 4 {
		t.Fatalf("output = %+v", output)
	}
	// A World that stores a write at another time than it was sent is not
	// run ahead of again: each step after the first waits for the result
	// before it.
	world.overlaps.Store(0)
	world.committed.Store(0)
	again := startRunAhead(t, world, runAheadInput{})
	again.deliver()
	if output := again.output(); output.Sum != 4 || world.overlaps.Load() != 0 {
		t.Fatalf("second run output = %+v overlaps = %d", output, world.overlaps.Load())
	}
}

// setAttribute sets the run's attribute key, which no runner reads.
func setAttribute(t *testing.T, world *runAheadWorld, run *runAheadRun, key string) func(context.Context) {
	return func(ctx context.Context) {
		value := "set"
		if err := world.World.RunsSetAttributes(ctx, worlds.RunsSetAttributesInput{RunID: run.runID, Changes: []worlds.RunsAttributeChange{{Key: key, Value: &value}}}); err != nil {
			t.Error(err)
		}
	}
}

func conformRunAheadRepairsTwiceInARow(t *testing.T, base worlds.World) {
	world := &runAheadWorld{World: base}
	run := startRunAhead(t, world, runAheadInput{})
	// Inert events land below two writes ahead in a row, and the flow folds
	// each in.
	world.before = []func(context.Context){setAttribute(t, world, run, "first"), setAttribute(t, world, run, "second")}
	if loads := run.deliver(); loads != 1 {
		t.Fatalf("the delivery read the run %d times, want once: it went on", loads)
	}
	if output := run.output(); output.Sum != 4 || world.overlaps.Load() == 0 {
		t.Fatalf("output = %+v overlaps = %d, want steps run ahead", output, world.overlaps.Load())
	}
	if world.Capabilities().SetAttributes {
		if stored, err := world.World.RunsGet(context.Background(), worlds.RunsGetInput{RunID: run.runID}); err != nil || stored.Attributes["first"] != "set" || stored.Attributes["second"] != "set" {
			t.Fatalf("run = %+v err = %v", stored, err)
		}
	}
}

func conformRunAheadRepairsFromAnIncompleteReport(t *testing.T, base worlds.World) {
	world := &runAheadWorld{World: base, incomplete: true}
	run := startRunAhead(t, world, runAheadInput{})
	// The answer of a write ahead leaves out an event below it, so the
	// delivery replays again from the log once its writes ahead settle,
	// and runs no step twice.
	world.before = []func(context.Context){func(context.Context) {}, setAttribute(t, world, run, "late")}
	if loads := run.deliver(); loads != 2 {
		t.Fatalf("the delivery read the run %d times, want twice: once more for the repair", loads)
	}
	if output := run.output(); output.Sum != 4 || world.bodies.Load() != 4 {
		t.Fatalf("output = %+v bodies = %d", output, world.bodies.Load())
	}
	if world.Capabilities().SetAttributes {
		if stored, err := world.World.RunsGet(context.Background(), worlds.RunsGetInput{RunID: run.runID}); err != nil || stored.Attributes["late"] != "set" {
			t.Fatalf("run = %+v err = %v", stored, err)
		}
	}
}
