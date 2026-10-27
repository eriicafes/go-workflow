package worlds_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/testworld"
)

func TestRuntimeMarksRunsWithUnknownWorkflowsAsFailed(t *testing.T) {
	world := testworld.New()
	defer world.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	runID := worlds.RunID("wrun_missing_workflow")
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "missing"}); err != nil {
		t.Fatal(err)
	}
	if _, err := world.Queue(ctx, worlds.QueueInput{RunID: runID, IdempotencyKey: string(runID)}); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID})
		if err != nil {
			t.Fatal(err)
		}
		if run.Status == worlds.StatusFailed {
			if !strings.Contains(string(run.Error), "workflow not found") {
				t.Fatalf("failure = %q", run.Error)
			}
			return
		}
	}
	t.Fatal("unknown workflow did not fail")
}

type compensationFailureWorld struct {
	*testworld.World
	eventType worlds.EventType
	cause     error
	queue     bool
	terminal  bool
	armed     bool
}

func (w *compensationFailureWorld) EventsCreate(ctx context.Context, input worlds.EventsCreateInput) (worlds.EventResult, error) {
	if w.armed && input.Event.EventType == w.eventType {
		w.armed = false
		if !w.terminal {
			return worlds.EventResult{}, w.cause
		}
		if _, err := w.World.EventsCreate(ctx, worlds.EventsCreateInput{RunID: input.RunID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeRunCancelled}}); err != nil {
			return worlds.EventResult{}, err
		}
	}
	return w.World.EventsCreate(ctx, input)
}

func (w *compensationFailureWorld) Queue(ctx context.Context, input worlds.QueueInput) (worlds.QueueResult, error) {
	if w.armed && w.queue && input.StepID != nil {
		w.armed = false
		return worlds.QueueResult{}, w.cause
	}
	return w.World.Queue(ctx, input)
}

func TestCompensationDeliveryErrors(t *testing.T) {
	for _, scenario := range []struct {
		name     string
		step     bool
		queue    bool
		terminal bool
	}{
		{name: "flow storage"},
		{name: "flow queue", queue: true},
		{name: "flow terminal", terminal: true},
		{name: "step storage", step: true},
		{name: "step terminal", step: true, terminal: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			base := testworld.NewManual()
			defer base.Close()
			cause := errors.New("compensation delivery unavailable")
			world := &compensationFailureWorld{World: base, cause: cause, queue: scenario.queue, terminal: scenario.terminal}
			world.eventType = worlds.EventTypeStepCreated
			if scenario.step {
				world.eventType = worlds.EventTypeAttrSet
			}
			failure := errors.New("workflow failure")
			runner := workflow.Use(func(ctx context.Context, _ versionedInput) (versionedOutput, error) {
				defer workflow.Rollback(ctx, func(ctx context.Context, _ struct{}) error {
					execution, _ := worlds.ExecutionFromContext(ctx)
					execution.CreateEvent(worlds.EventsCreateEvent{EventType: worlds.EventTypeAttrSet, Data: []byte(`{"changes":[]}`)}, nil)
					return nil
				}, struct{}{})
				return versionedOutput{}, failure
			})
			runtime := worlds.NewRuntime(world, worlds.Runners(runner))
			ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
			defer cancel()
			run, err := workflow.Start(ctx, versionedInput{})
			if err != nil {
				t.Fatal(err)
			}
			if scenario.step {
				deliverNext(t, ctx, base, runtime)
			}
			world.armed = true
			message := base.QueuedMessages()[0]
			err = runtime.Execute(ctx, message.Input)
			if world.armed {
				t.Fatal("compensation did not reach the injected failure")
			}
			if scenario.terminal {
				if err != nil {
					t.Fatalf("terminal delivery error = %v", err)
				}
				if status, err := run.Status(ctx); err != nil || status != workflow.StatusCancelled {
					t.Fatalf("status = %v, error = %v", status, err)
				}
				return
			}
			if err != cause {
				t.Fatalf("delivery error = %T %v, want the underlying cause", err, err)
			}
			drain(t, ctx, base, runtime)
			if _, err := run.Value(ctx); !errors.Is(err, failure) {
				t.Fatalf("workflow error = %v", err)
			}
		})
	}
}

type versionedInput struct{}
type versionedOutput struct{ Version string }

func (versionedInput) Workflow(versionedOutput) {}

var (
	alphaCalls    atomic.Int32
	betaCalls     atomic.Int32
	rollbackCalls atomic.Int32
)

func alpha(context.Context, struct{}) (string, error) {
	alphaCalls.Add(1)
	return "alpha", nil
}

func beta(context.Context, struct{}) (string, error) {
	betaCalls.Add(1)
	return "beta", nil
}

func undo(context.Context, struct{}) error {
	rollbackCalls.Add(1)
	return nil
}

// alphaThenBeta is the runner as first deployed.
func alphaThenBeta(ctx context.Context, _ versionedInput) (versionedOutput, error) {
	if _, err := workflow.Step(ctx, alpha, struct{}{}); err != nil {
		return versionedOutput{}, err
	}

	if _, err := workflow.Step(ctx, beta, struct{}{}); err != nil {
		return versionedOutput{}, err
	}

	return versionedOutput{Version: "v1"}, nil
}

// betaSwallowingErrors replaces alpha with beta and ignores step errors, so a
// divergence returned to runner code would complete the run with wrong state.
func betaSwallowingErrors(ctx context.Context, _ versionedInput) (versionedOutput, error) {
	defer workflow.Rollback(ctx, undo, struct{}{})

	_, _ = workflow.Step(ctx, beta, struct{}{})
	return versionedOutput{Version: "v2"}, nil
}

// alphaOnly drops the second step.
func alphaOnly(ctx context.Context, _ versionedInput) (versionedOutput, error) {
	if _, err := workflow.Step(ctx, alpha, struct{}{}); err != nil {
		return versionedOutput{}, err
	}

	return versionedOutput{Version: "v2"}, nil
}

func resetCalls() {
	alphaCalls.Store(0)
	betaCalls.Store(0)
	rollbackCalls.Store(0)
}

// deliverNext executes and acknowledges the oldest queued delivery.
func deliverNext(t *testing.T, ctx context.Context, world *testworld.World, runtime *worlds.Runtime) worlds.QueueInput {
	t.Helper()
	messages := world.QueuedMessages()
	if len(messages) == 0 {
		t.Fatal("no queued delivery")
	}

	message := messages[0]
	if err := runtime.Execute(ctx, message.Input); err != nil {
		t.Fatal(err)
	}

	if err := world.QueueCancel(ctx, worlds.QueueCancelInput{MessageID: message.ID}); err != nil {
		t.Fatal(err)
	}
	return message.Input
}

func drain(t *testing.T, ctx context.Context, world *testworld.World, runtime *worlds.Runtime) {
	t.Helper()
	for len(world.QueuedMessages()) > 0 {
		deliverNext(t, ctx, world, runtime)
	}
}

func startVersioned(t *testing.T, ctx context.Context, world *testworld.World) *workflow.Run[versionedOutput] {
	t.Helper()
	run, err := workflow.Start(workflow.NewContext(ctx, workflow.NewClient(world)), versionedInput{})
	if err != nil {
		t.Fatal(err)
	}
	return run
}

// awaitFailure awaits run and checks that it failed with an error whose
// message contains want.
func awaitFailure(t *testing.T, ctx context.Context, world worlds.World, run *workflow.Run[versionedOutput], want string) {
	t.Helper()
	_, err := run.Value(workflow.NewContext(ctx, workflow.NewClient(world)))
	var failure *workflow.Error
	if !errors.As(err, &failure) || !strings.Contains(err.Error(), want) {
		t.Fatalf("run error = %v, want one containing %q", err, want)
	}
}

func TestReplayDivergenceFailsRunWithoutReachingRunnerCode(t *testing.T) {
	resetCalls()
	ctx, cancel := context.WithTimeout(queuedSteps(), 5*time.Second)
	defer cancel()

	world := testworld.NewManual()
	defer world.Close()

	v1 := worlds.NewRuntime(world, worlds.Runners(workflow.Use(alphaThenBeta)))
	v2 := worlds.NewRuntime(world, worlds.Runners(workflow.Use(betaSwallowingErrors)))
	run := startVersioned(t, ctx, world)

	// v1 records alpha at position 0 and completes it.
	deliverNext(t, ctx, world, v1)
	deliverNext(t, ctx, world, v1)

	// v2 reaches beta at position 0 and swallows step errors. Each diverged
	// delivery is queued again as an immediate recovery delivery that counts
	// the divergence, until the fourth fails the run.
	for count := 1; count <= 3; count++ {
		deliverNext(t, ctx, world, v2)
		queued := world.Queued()
		if len(queued) != 1 || queued[0].StepID != nil || queued[0].NotBefore != nil || queued[0].ReplayDivergenceCount != count {
			t.Fatalf("recovery delivery %d = %#v", count, queued)
		}

		if status, err := run.Status(workflow.NewContext(ctx, workflow.NewClient(world))); err != nil || status.Terminal() {
			t.Fatalf("status after recovery delivery %d = %q, %v", count, status, err)
		}
	}

	drain(t, ctx, world, v2)

	awaitFailure(t, ctx, world, run, "corrupted event log at position 0 after 3 recovery replays")
	if _, err := run.Value(workflow.NewContext(ctx, workflow.NewClient(world))); !errors.Is(err, worlds.ErrCorruptedEventLog) {
		t.Fatalf("run error = %v, want ErrCorruptedEventLog", err)
	}

	if betaCalls.Load() != 0 || rollbackCalls.Load() != 0 {
		t.Fatalf("beta calls = %d, rollback calls = %d", betaCalls.Load(), rollbackCalls.Load())
	}
}

func TestReplayDivergenceRequeuesTheDivergedStepDelivery(t *testing.T) {
	resetCalls()
	ctx, cancel := context.WithTimeout(queuedSteps(), 5*time.Second)
	defer cancel()

	world := testworld.NewManual()
	defer world.Close()

	v1 := worlds.NewRuntime(world, worlds.Runners(workflow.Use(alphaThenBeta)))
	v2 := worlds.NewRuntime(world, worlds.Runners(workflow.Use(betaSwallowingErrors)))
	run := startVersioned(t, ctx, world)

	// v1 records alpha at position 0 and queues its step delivery.
	deliverNext(t, ctx, world, v1)

	// v2 replays to the step and reaches beta where alpha was recorded. The
	// recovery delivery targets the same step, so the step is not orphaned.
	diverged := deliverNext(t, ctx, world, v2)
	queued := world.Queued()
	if len(queued) != 1 || queued[0].StepID == nil || *queued[0].StepID != *diverged.StepID || queued[0].ReplayDivergenceCount != 1 {
		t.Fatalf("recovery delivery = %#v", queued)
	}

	if alphaCalls.Load() != 0 || betaCalls.Load() != 0 {
		t.Fatalf("alpha calls = %d, beta calls = %d", alphaCalls.Load(), betaCalls.Load())
	}

	drain(t, ctx, world, v2)

	awaitFailure(t, ctx, world, run, "corrupted event log at position 0 after 3 recovery replays")
}

func TestReplayDivergenceCatchesUnreachedOperations(t *testing.T) {
	resetCalls()
	ctx, cancel := context.WithTimeout(queuedSteps(), 5*time.Second)
	defer cancel()

	world := testworld.NewManual()
	defer world.Close()

	v1 := worlds.NewRuntime(world, worlds.Runners(workflow.Use(alphaThenBeta)))
	v2 := worlds.NewRuntime(world, worlds.Runners(workflow.Use(alphaOnly)))
	run := startVersioned(t, ctx, world)

	// v1 records and completes alpha and beta, leaving the final flow queued.
	for range 4 {
		deliverNext(t, ctx, world, v1)
	}

	// v2 returns after alpha without reaching the recorded beta.
	drain(t, ctx, world, v2)

	awaitFailure(t, ctx, world, run, "corrupted event log at position 1")
}

type hookedInput struct{ Token string }
type hookedOutput struct{ Value string }

func (hookedInput) Workflow(hookedOutput) {}

func awaitHook(ctx context.Context, input hookedInput) (hookedOutput, error) {
	hook := workflow.CreateHook[string](ctx, workflow.HookOption{Token: input.Token})
	if owner, err := hook.GetConflict(ctx); err != nil {
		return hookedOutput{}, err
	} else if owner != nil {
		return hookedOutput{}, workflow.ErrHookConflict
	}

	value, err := hook.Next(ctx)
	return hookedOutput{Value: value}, err
}

// deployedWorld is a view of a shared test World as one deployment of a World
// with deployment affinity, such as the Vercel World.
type deployedWorld struct {
	*testworld.World
	id   string
	gone string
}

func (w deployedWorld) Capabilities() worlds.Capabilities {
	capabilities := w.World.Capabilities()
	capabilities.DeploymentAffinity = true
	return capabilities
}

func (w deployedWorld) DeploymentID() string { return w.id }

// Queue reports the gone deployment as unavailable, as Vercel does for a
// deleted deployment.
func (w deployedWorld) Queue(ctx context.Context, input worlds.QueueInput) (worlds.QueueResult, error) {
	if w.gone != "" && input.DeploymentID == w.gone {
		return worlds.QueueResult{}, worlds.ErrDeploymentUnavailable
	}
	return w.World.Queue(ctx, input)
}

func TestDeploymentAffinityReroutesDeliveriesToTheRunsDeployment(t *testing.T) {
	resetCalls()
	ctx, cancel := context.WithTimeout(queuedSteps(), 5*time.Second)
	defer cancel()

	world := testworld.NewManual()
	defer world.Close()

	d1 := deployedWorld{World: world, id: "dpl_1"}
	d2 := deployedWorld{World: world, id: "dpl_2"}
	v1 := worlds.NewRuntime(d1, worlds.Runners(workflow.Use(alphaThenBeta)))
	v2 := worlds.NewRuntime(d2, worlds.Runners(workflow.Use(betaSwallowingErrors)))

	// A run started through dpl_1's client is pinned to dpl_1.
	run, err := workflow.Start(workflow.NewContext(ctx, workflow.NewClient(d1)), versionedInput{})
	if err != nil {
		t.Fatal(err)
	}

	stored, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: worlds.RunID(run.ID())})
	if err != nil {
		t.Fatal(err)
	}

	if stored.DeploymentID != "dpl_1" {
		t.Fatalf("run deployment = %q", stored.DeploymentID)
	}

	// dpl_1 executes the first flow, which addresses the step to dpl_1.
	deliverNext(t, ctx, world, v1)
	if queued := world.Queued(); len(queued) != 1 || queued[0].DeploymentID != "dpl_1" {
		t.Fatalf("queued step = %#v", queued)
	}

	// dpl_2 receives the step and re-routes it to dpl_1 without replaying.
	deliverNext(t, ctx, world, v2)
	queued := world.Queued()
	if len(queued) != 1 || queued[0].DeploymentID != "dpl_1" || queued[0].DeploymentMismatchRetryCount != 1 {
		t.Fatalf("re-routed delivery = %#v", queued)
	}

	if queued[0].NotBefore == nil || !queued[0].NotBefore.After(time.Now()) {
		t.Fatalf("re-routed delivery is not delayed: %v", queued[0].NotBefore)
	}

	if alphaCalls.Load() != 0 || betaCalls.Load() != 0 {
		t.Fatalf("alpha calls = %d, beta calls = %d", alphaCalls.Load(), betaCalls.Load())
	}

	// dpl_1 finishes the run on its own code.
	drain(t, ctx, world, v1)

	output, err := run.Value(workflow.NewContext(ctx, workflow.NewClient(d1)))
	if err != nil {
		t.Fatal(err)
	}

	if output.Version != "v1" {
		t.Fatalf("version = %q", output.Version)
	}
}

func TestDeploymentAffinityFailsRunAfterRerouteBudget(t *testing.T) {
	resetCalls()
	ctx, cancel := context.WithTimeout(queuedSteps(), 5*time.Second)
	defer cancel()

	world := testworld.NewManual()
	defer world.Close()

	d1 := deployedWorld{World: world, id: "dpl_1"}
	d2 := deployedWorld{World: world, id: "dpl_2"}
	v2 := worlds.NewRuntime(d2, worlds.Runners(workflow.Use(betaSwallowingErrors)))
	run, err := workflow.Start(workflow.NewContext(ctx, workflow.NewClient(d1)), versionedInput{})
	if err != nil {
		t.Fatal(err)
	}

	// Every delivery keeps reaching dpl_2.
	drain(t, ctx, world, v2)

	awaitFailure(t, ctx, d1, run, `pinned to deployment "dpl_1" but was delivered to "dpl_2" after 3 re-routes`)
	if _, err := run.Value(workflow.NewContext(ctx, workflow.NewClient(d1))); !errors.Is(err, worlds.ErrDeploymentMismatch) {
		t.Fatalf("run error = %v, want ErrDeploymentMismatch", err)
	}

	if betaCalls.Load() != 0 || rollbackCalls.Load() != 0 {
		t.Fatalf("beta calls = %d, rollback calls = %d", betaCalls.Load(), rollbackCalls.Load())
	}
}

func TestDeploymentAffinityFailsRunWhenDeploymentIsUnavailable(t *testing.T) {
	resetCalls()
	ctx, cancel := context.WithTimeout(queuedSteps(), 5*time.Second)
	defer cancel()

	world := testworld.NewManual()
	defer world.Close()

	d1 := deployedWorld{World: world, id: "dpl_1"}
	d2 := deployedWorld{World: world, id: "dpl_2", gone: "dpl_1"}
	v2 := worlds.NewRuntime(d2, worlds.Runners(workflow.Use(betaSwallowingErrors)))
	run, err := workflow.Start(workflow.NewContext(ctx, workflow.NewClient(d1)), versionedInput{})
	if err != nil {
		t.Fatal(err)
	}

	// The re-route to the deleted dpl_1 fails, so the run fails at once.
	deliverNext(t, ctx, world, v2)

	awaitFailure(t, ctx, d1, run, "after 0 re-routes")
	if _, err := run.Value(workflow.NewContext(ctx, workflow.NewClient(d1))); !errors.Is(err, worlds.ErrDeploymentMismatch) {
		t.Fatalf("run error = %v, want ErrDeploymentMismatch", err)
	}

	if len(world.Queued()) != 0 {
		t.Fatalf("queued after failure = %#v", world.Queued())
	}
}

func TestDeploymentAffinityAddressesHookResumesToTheRunsDeployment(t *testing.T) {
	ctx, cancel := context.WithTimeout(queuedSteps(), 5*time.Second)
	defer cancel()

	world := testworld.NewManual()
	defer world.Close()

	d1 := deployedWorld{World: world, id: "dpl_1"}
	d2 := deployedWorld{World: world, id: "dpl_2"}
	v1 := worlds.NewRuntime(d1, worlds.Runners(workflow.Use(awaitHook)))
	hook := workflow.NewHookToken()
	if _, err := workflow.Start(workflow.NewContext(ctx, workflow.NewClient(d1)), hookedInput{Token: hook}); err != nil {
		t.Fatal(err)
	}

	drain(t, ctx, world, v1)

	// A resume from dpl_2's client is addressed to dpl_1.
	if err := workflow.ResumeHook(workflow.NewContext(ctx, workflow.NewClient(d2)), hook, "value"); err != nil {
		t.Fatal(err)
	}

	queued := world.Queued()
	if len(queued) != 1 || queued[0].DeploymentID != "dpl_1" {
		t.Fatalf("resume delivery = %#v", queued)
	}
}

// deliverStep executes and acknowledges the queued delivery of the step whose
// function name ends in name.
func deliverStep(t *testing.T, ctx context.Context, world *testworld.World, runtime *worlds.Runtime, runID worlds.RunID, name string) {
	t.Helper()
	steps, err := world.StepsList(ctx, worlds.StepsListInput{RunID: runID})
	if err != nil {
		t.Fatal(err)
	}

	for _, step := range steps.Data {
		if !strings.HasSuffix(step.StepName, name) {
			continue
		}
		for _, message := range world.QueuedMessages() {
			if message.Input.StepID == nil || *message.Input.StepID != step.StepID {
				continue
			}
			if err := runtime.Execute(ctx, message.Input); err != nil {
				t.Fatal(err)
			}

			if err := world.QueueCancel(ctx, worlds.QueueCancelInput{MessageID: message.ID}); err != nil {
				t.Fatal(err)
			}
			return
		}
	}
	t.Fatalf("no queued delivery for step %s", name)
}

type raceInput struct{}
type raceOutput struct{ Winner string }

func (raceInput) Workflow(raceOutput) {}

func lookupFirst(context.Context, struct{}) (string, error)  { return "first", nil }
func lookupSecond(context.Context, struct{}) (string, error) { return "second", nil }
func announceFirst(context.Context, string) (string, error)  { return "first", nil }
func announceSecond(context.Context, string) (string, error) { return "second", nil }

// race announces the lookup that finishes first, through a step named after
// it, so a replay that picked another winner would diverge.
func race(ctx context.Context, _ raceInput) (raceOutput, error) {
	first := workflow.Background(ctx, lookupFirst, struct{}{})
	second := workflow.Background(ctx, lookupSecond, struct{}{})
	winner, err := workflow.Select(ctx, first, second)
	if err != nil {
		return raceOutput{}, err
	}

	value, err := winner.(*workflow.Result[string]).Value(ctx)
	if err != nil {
		return raceOutput{}, err
	}

	announce := announceFirst
	if value == "second" {
		announce = announceSecond
	}
	announced, err := workflow.Step(ctx, announce, value)
	return raceOutput{Winner: announced}, err
}

func TestSelectKeepsTheFirstPromiseToSettleAcrossReplays(t *testing.T) {
	ctx, cancel := context.WithTimeout(queuedSteps(), 5*time.Second)
	defer cancel()

	world := testworld.NewManual()
	defer world.Close()

	runtime := worlds.NewRuntime(world, worlds.Runners(workflow.Use(race)))
	client := workflow.NewContext(ctx, workflow.NewClient(world))
	run, err := workflow.Start(client, raceInput{})
	if err != nil {
		t.Fatal(err)
	}

	runID := worlds.RunID(run.ID())

	// The second lookup finishes first, and the flow announces it.
	deliverNext(t, ctx, world, runtime)
	deliverStep(t, ctx, world, runtime, runID, "lookupSecond")
	deliverNext(t, ctx, world, runtime)

	// Once both lookups have finished, later replays still pick the second,
	// although it comes after the first in argument order.
	drain(t, ctx, world, runtime)

	output, err := run.Value(client)
	if err != nil {
		t.Fatal(err)
	}

	if output.Winner != "second" {
		t.Fatalf("winner = %q, want second", output.Winner)
	}
}

type nudgeInput struct{ Token string }
type nudgeOutput struct {
	Nudged bool
	Value  string
}

func (nudgeInput) Workflow(nudgeOutput) {}

func prepare(context.Context, struct{}) (struct{}, error) { return struct{}{}, nil }
func nudge(context.Context, struct{}) (struct{}, error)   { return struct{}{}, nil }

// awaitWithNudge sends a nudge when the hook has not been resumed by the time
// the preparation step finishes.
func awaitWithNudge(ctx context.Context, input nudgeInput) (nudgeOutput, error) {
	hook := workflow.CreateHook[string](ctx, workflow.HookOption{Token: input.Token})
	if owner, err := hook.GetConflict(ctx); err != nil {
		return nudgeOutput{}, err
	} else if owner != nil {
		return nudgeOutput{}, workflow.ErrHookConflict
	}

	preparation := workflow.Background(ctx, prepare, struct{}{})
	winner, err := workflow.Select(ctx, hook, preparation)
	if err != nil {
		return nudgeOutput{}, err
	}

	var output nudgeOutput
	if winner == preparation {
		if _, err := workflow.Step(ctx, nudge, struct{}{}); err != nil {
			return nudgeOutput{}, err
		}

		output.Nudged = true
	}

	output.Value, err = hook.Next(ctx)
	return output, err
}

func TestDoneGivesTheSameAnswerOnEveryReplay(t *testing.T) {
	ctx, cancel := context.WithTimeout(queuedSteps(), 5*time.Second)
	defer cancel()

	world := testworld.NewManual()
	defer world.Close()

	runtime := worlds.NewRuntime(world, worlds.Runners(workflow.Use(awaitWithNudge)))
	client := workflow.NewContext(ctx, workflow.NewClient(world))
	token := workflow.NewHookToken()
	run, err := workflow.Start(client, nudgeInput{Token: token})
	if err != nil {
		t.Fatal(err)
	}

	// The preparation finishes before the hook is resumed, so the flow nudges.
	deliverNext(t, ctx, world, runtime)
	deliverNext(t, ctx, world, runtime)
	deliverNext(t, ctx, world, runtime)

	// The hook is resumed afterwards. Later replays must still see it as not
	// done where the runner checked it, and keep the nudge.
	if err := workflow.ResumeHook(client, token, "approved"); err != nil {
		t.Fatal(err)
	}

	drain(t, ctx, world, runtime)

	output, err := run.Value(client)
	if err != nil {
		t.Fatal(err)
	}

	if !output.Nudged || output.Value != "approved" {
		t.Fatalf("output = %+v, want nudged and approved", output)
	}
}

// racingWorld lets another flow declare the same step just before this flow's
// first in-band write of its steps, as a concurrent delivery of the run would.
type racingWorld struct {
	*testworld.World
	raced bool
}

func (w *racingWorld) EventsCreateBatch(ctx context.Context, input worlds.EventsCreateBatchInput) (worlds.EventBatchResult, error) {
	if !w.raced && input.InBandCount != nil && input.Events[0].EventType == worlds.EventTypeStepCreated {
		w.raced = true
		other := input.Events[0]
		other.CorrelationID = "step_other_flow"
		if _, err := w.World.EventsCreate(ctx, worlds.EventsCreateInput{RunID: input.RunID, Event: other, Options: worlds.EventsCreateOptions{InBandCount: input.InBandCount}}); err != nil {
			return worlds.EventBatchResult{}, err
		}

		stepID := worlds.StepID("step_other_flow")
		if _, err := w.World.Queue(ctx, worlds.QueueInput{RunID: input.RunID, StepID: &stepID}); err != nil {
			return worlds.EventBatchResult{}, err
		}
	}
	return w.World.EventsCreateBatch(ctx, input)
}

func TestSupersededFlowReplaysInsteadOfDuplicatingDeclarations(t *testing.T) {
	resetCalls()
	ctx, cancel := context.WithTimeout(queuedSteps(), 5*time.Second)
	defer cancel()

	world := testworld.NewManual()
	defer world.Close()

	racing := &racingWorld{World: world}
	runtime := worlds.NewRuntime(racing, worlds.Runners(workflow.Use(alphaThenBeta)))
	run := startVersioned(t, ctx, world)
	drain(t, ctx, world, runtime)

	output, err := run.Value(workflow.NewContext(ctx, workflow.NewClient(world)))
	if err != nil {
		t.Fatal(err)
	}

	if output.Version != "v1" {
		t.Fatalf("version = %q", output.Version)
	}

	events, err := world.EventsList(ctx, worlds.EventsListInput{RunID: worlds.RunID(run.ID())})
	if err != nil {
		t.Fatal(err)
	}

	declared := map[string]int{}
	for _, event := range events.Data {
		if event.EventType == worlds.EventTypeStepCreated {
			declared[event.CorrelationID]++
		}
	}
	if len(declared) != 2 || declared["step_other_flow"] != 1 {
		t.Fatalf("declared steps = %v, want the other flow's step and beta", declared)
	}

	if alphaCalls.Load() != 1 {
		t.Fatalf("alpha calls = %d, want 1", alphaCalls.Load())
	}
}

// lossyWorld fails the first step delivery it is asked to queue, after the
// flow has recorded the step.
type lossyWorld struct {
	*testworld.World
	failed bool
}

var errQueueUnavailable = errors.New("queue unavailable")

func (w *lossyWorld) Queue(ctx context.Context, input worlds.QueueInput) (worlds.QueueResult, error) {
	if !w.failed && input.StepID != nil {
		w.failed = true
		return worlds.QueueResult{}, errQueueUnavailable
	}
	return w.World.Queue(ctx, input)
}

func TestRedeliveredFlowQueuesAStepItFailedToQueue(t *testing.T) {
	resetCalls()
	ctx, cancel := context.WithTimeout(queuedSteps(), 5*time.Second)
	defer cancel()
	world := testworld.NewManual()
	defer world.Close()

	lossy := &lossyWorld{World: world}
	runtime := worlds.NewRuntime(lossy, worlds.Runners(workflow.Use(alphaThenBeta)))
	run := startVersioned(t, ctx, world)
	delivery := world.QueuedMessages()[0]

	// The flow records alpha, then fails to queue it, so the delivery fails.
	if err := runtime.Execute(ctx, delivery.Input); !errors.Is(err, errQueueUnavailable) {
		t.Fatalf("delivery error = %v, want the queue error", err)
	}

	// Its redelivery finds alpha recorded but never started, and queues it.
	if err := runtime.Execute(ctx, delivery.Input); err != nil {
		t.Fatal(err)
	}
	if err := world.QueueCancel(ctx, worlds.QueueCancelInput{MessageID: delivery.ID}); err != nil {
		t.Fatal(err)
	}
	if queued := world.QueuedMessages(); len(queued) != 1 || queued[0].Input.StepID == nil {
		t.Fatalf("queued = %#v, want alpha's delivery", queued)
	}

	drain(t, ctx, world, runtime)
	if _, err := run.Value(workflow.NewContext(ctx, workflow.NewClient(world))); err != nil {
		t.Fatal(err)
	}
	if alphaCalls.Load() != 1 {
		t.Fatalf("alpha calls = %d, want 1", alphaCalls.Load())
	}
}

func TestSupersededFlowQueuesOneFollowUpAndAcknowledges(t *testing.T) {
	resetCalls()
	ctx, cancel := context.WithTimeout(queuedSteps(), 5*time.Second)
	defer cancel()

	world := testworld.NewManual()
	defer world.Close()

	racing := &racingWorld{World: world}
	runtime := worlds.NewRuntime(racing, worlds.Runners(workflow.Use(alphaThenBeta)))
	run := startVersioned(t, ctx, world)
	deliverNext(t, ctx, world, runtime)

	followUps := 0
	for _, message := range world.QueuedMessages() {
		if strings.HasPrefix(message.Input.IdempotencyKey, "superseded:") {
			followUps++
			if message.Input.StepID != nil || message.Input.NotBefore != nil {
				t.Fatalf("follow-up = %#v, want an immediate flow", message.Input)
			}
		}
	}
	if followUps != 1 {
		t.Fatalf("follow-ups = %d, want 1", followUps)
	}

	drain(t, ctx, world, runtime)
	if _, err := run.Value(workflow.NewContext(ctx, workflow.NewClient(world))); err != nil {
		t.Fatal(err)
	}
}

type sleeperInput struct{}
type sleeperOutput struct{}

func (sleeperInput) Workflow(sleeperOutput) {}

func sleepAnHour(ctx context.Context, _ sleeperInput) (sleeperOutput, error) {
	return sleeperOutput{}, workflow.Sleep(ctx, time.Hour)
}

// refusingWorld refuses the first in-band wait_created as if another flow had
// written first, without appending it.
type refusingWorld struct {
	*testworld.World
	refused bool
}

func (w *refusingWorld) EventsCreate(ctx context.Context, input worlds.EventsCreateInput) (worlds.EventResult, error) {
	if !w.refused && input.Options.InBandCount != nil && input.Event.EventType == worlds.EventTypeWaitCreated {
		w.refused = true
		return worlds.EventResult{}, worlds.ErrInBandSuperseded
	}
	return w.World.EventsCreate(ctx, input)
}

func TestRefusedWaitCancelsItsQueuedTimer(t *testing.T) {
	ctx, cancel := context.WithTimeout(queuedSteps(), 5*time.Second)
	defer cancel()

	world := testworld.NewManual()
	defer world.Close()

	refusing := &refusingWorld{World: world}
	runtime := worlds.NewRuntime(refusing, worlds.Runners(workflow.Use(sleepAnHour)))
	if _, err := workflow.Start(workflow.NewContext(ctx, workflow.NewClient(world)), sleeperInput{}); err != nil {
		t.Fatal(err)
	}
	deliverNext(t, ctx, world, runtime)
	if !refusing.refused {
		t.Fatal("wait_created was not refused")
	}
	for _, message := range world.QueuedMessages() {
		if message.Input.NotBefore != nil && time.Until(*message.Input.NotBefore) > time.Minute {
			t.Fatalf("the refused wait left its timer queued: %#v", message.Input)
		}
	}

	// The follow-up replays and declares the wait once.
	deliverNext(t, ctx, world, runtime)
	timers := 0
	for _, message := range world.QueuedMessages() {
		if message.Input.NotBefore != nil && time.Until(*message.Input.NotBefore) > time.Minute {
			timers++
		}
	}
	if timers != 1 {
		t.Fatalf("timers = %d, want 1", timers)
	}
}

type backoffInput struct{}
type backoffOutput struct{}

func (backoffInput) Workflow(backoffOutput) {}

func failForAnHour(context.Context, struct{}) (struct{}, error) {
	return struct{}{}, workflow.RetryableError(errors.New("not yet"), time.Hour)
}

func awaitBackoff(ctx context.Context, _ backoffInput) (backoffOutput, error) {
	_, err := workflow.Step(ctx, failForAnHour, struct{}{}, workflow.StepOptions{MaxRetries: 1})
	return backoffOutput{}, err
}

func TestRuntimeRecordsFailedStepAfterRunCancellationWithoutRetry(t *testing.T) {
	cause := errors.New("step failed")
	runner := workflow.Use(func(ctx context.Context, _ backoffInput) (backoffOutput, error) {
		_, err := workflow.Step(ctx, func(ctx context.Context, _ struct{}) (struct{}, error) {
			run, err := workflow.Get[backoffOutput](ctx, workflow.GetWorkflowMetadata(ctx).RunID)
			if err != nil {
				return struct{}{}, err
			}
			if err := run.Cancel(ctx); err != nil {
				return struct{}{}, err
			}
			return struct{}{}, workflow.RetryableError(cause, time.Hour)
		}, struct{}{}, workflow.StepOptions{MaxRetries: 3})
		return backoffOutput{}, err
	})
	world := testworld.NewManual()
	defer world.Close()
	ctx := workflow.NewContext(queuedSteps(), workflow.NewClient(world))
	runtime := worlds.NewRuntime(world, worlds.Runners(runner))
	run, err := workflow.Start(ctx, backoffInput{})
	if err != nil {
		t.Fatal(err)
	}
	deliverNext(t, ctx, world, runtime)
	delivery := deliverNext(t, ctx, world, runtime)
	step, err := world.StepsGet(ctx, worlds.StepsGetInput{RunID: worlds.RunID(run.ID()), StepID: *delivery.StepID})
	if err != nil || step.Status != worlds.StatusFailed || step.RetryAfter != nil || step.Attempt != 1 || !errors.Is(worlds.DecodeError(step.Error), cause) {
		t.Fatalf("late failure = %#v, err=%v", step, err)
	}
	for _, message := range world.QueuedMessages() {
		if message.Input.StepID != nil {
			t.Fatalf("cancelled run queued a retry: %#v", message.Input)
		}
	}
	drain(t, ctx, world, runtime)
	if status, err := run.Status(ctx); err != nil || status != workflow.StatusCancelled {
		t.Fatalf("run status=%s, err=%v", status, err)
	}
}

func TestSuspendingFlowLeavesARetryThatIsNotDueQueuedOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(queuedSteps(), 5*time.Second)
	defer cancel()
	world := testworld.NewManual()
	defer world.Close()
	runtime := worlds.NewRuntime(world, worlds.Runners(workflow.Use(awaitBackoff)))
	run, err := workflow.Start(workflow.NewContext(ctx, workflow.NewClient(world)), backoffInput{})
	if err != nil {
		t.Fatal(err)
	}
	deliverNext(t, ctx, world, runtime) // declares the step
	deliverNext(t, ctx, world, runtime) // fails it and queues the retry in an hour

	// Another flow suspends on the step while its retry is not yet due.
	flow, err := world.Queue(ctx, worlds.QueueInput{RunID: worlds.RunID(run.ID())})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Execute(ctx, worlds.QueueInput{RunID: worlds.RunID(run.ID())}); err != nil {
		t.Fatal(err)
	}
	if err := world.QueueCancel(ctx, worlds.QueueCancelInput{MessageID: flow.MessageID}); err != nil {
		t.Fatal(err)
	}

	steps := 0
	for _, message := range world.QueuedMessages() {
		if message.Input.StepID != nil {
			steps++
		}
	}
	if steps != 1 {
		t.Fatalf("step deliveries = %d, want only the retry", steps)
	}
}

func TestEarlyTimerDeliveryQueuesItsReplacement(t *testing.T) {
	ctx, cancel := context.WithTimeout(queuedSteps(), 5*time.Second)
	defer cancel()
	world := testworld.NewManual()
	defer world.Close()
	runtime := worlds.NewRuntime(world, worlds.Runners(workflow.Use(sleepAnHour)))
	run, err := workflow.Start(workflow.NewContext(ctx, workflow.NewClient(world)), sleeperInput{})
	if err != nil {
		t.Fatal(err)
	}
	deliverNext(t, ctx, world, runtime)

	// The timer's own delivery arrives before the wait is due, as when the
	// delivering worker's clock is behind the declaring worker's.
	timer := world.QueuedMessages()[0]
	if timer.Input.NotBefore == nil {
		t.Fatalf("queued = %#v, want the timer", timer.Input)
	}
	if err := runtime.Execute(ctx, timer.Input); err != nil {
		t.Fatal(err)
	}
	if err := world.QueueCancel(ctx, worlds.QueueCancelInput{MessageID: timer.ID}); err != nil {
		t.Fatal(err)
	}

	queued := world.QueuedMessages()
	if len(queued) != 1 || queued[0].Input.IdempotencyKey != timer.Input.IdempotencyKey || queued[0].Input.NotBefore == nil || !queued[0].Input.NotBefore.Equal(*timer.Input.NotBefore) {
		t.Fatalf("queued = %#v, want the timer queued again", queued)
	}
	if status, err := run.Status(workflow.NewContext(ctx, workflow.NewClient(world))); err != nil || status != workflow.StatusRunning {
		t.Fatalf("status = %s, err = %v", status, err)
	}
}

// flowQueueFailure fails the next queued flow delivery while armed.
type flowQueueFailure struct {
	*testworld.World
	armed bool
}

func (w *flowQueueFailure) Queue(ctx context.Context, input worlds.QueueInput) (worlds.QueueResult, error) {
	if w.armed && input.StepID == nil {
		w.armed = false
		return worlds.QueueResult{}, errQueueUnavailable
	}
	return w.World.Queue(ctx, input)
}

func TestRedeliveredStepQueuesTheFlowItFailedToQueue(t *testing.T) {
	resetCalls()
	ctx, cancel := context.WithTimeout(queuedSteps(), 5*time.Second)
	defer cancel()
	world := testworld.NewManual()
	defer world.Close()
	failing := &flowQueueFailure{World: world}
	runtime := worlds.NewRuntime(failing, worlds.Runners(workflow.Use(alphaThenBeta)))
	run := startVersioned(t, ctx, world)
	deliverNext(t, ctx, world, runtime)

	// alpha completes, then queueing the flow that consumes it fails.
	step := world.QueuedMessages()[0]
	failing.armed = true
	if err := runtime.Execute(ctx, step.Input); !errors.Is(err, errQueueUnavailable) {
		t.Fatalf("delivery error = %v, want the queue error", err)
	}
	// The redelivery finds alpha settled and queues the flow.
	if err := runtime.Execute(ctx, step.Input); err != nil {
		t.Fatal(err)
	}
	if err := world.QueueCancel(ctx, worlds.QueueCancelInput{MessageID: step.ID}); err != nil {
		t.Fatal(err)
	}

	drain(t, ctx, world, runtime)
	if _, err := run.Value(workflow.NewContext(ctx, workflow.NewClient(world))); err != nil {
		t.Fatal(err)
	}
	if alphaCalls.Load() != 1 || betaCalls.Load() != 1 {
		t.Fatalf("alpha calls = %d, beta calls = %d, want 1 each", alphaCalls.Load(), betaCalls.Load())
	}
}

func TestReturnedRunCancelledErrorFailsTheRun(t *testing.T) {
	runner := workflow.Use(func(ctx context.Context, _ backoffInput) (backoffOutput, error) {
		return backoffOutput{}, fmt.Errorf("child: %w", workflow.ErrRunCancelled)
	})
	ctx, cancel := context.WithTimeout(queuedSteps(), 5*time.Second)
	defer cancel()
	world := testworld.NewManual()
	defer world.Close()
	runtime := worlds.NewRuntime(world, worlds.Runners(runner))
	client := workflow.NewContext(ctx, workflow.NewClient(world))
	run, err := workflow.Start(client, backoffInput{})
	if err != nil {
		t.Fatal(err)
	}
	drain(t, ctx, world, runtime)
	if status, err := run.Status(client); err != nil || status != workflow.StatusFailed {
		t.Fatalf("status = %s, err = %v, want failed", status, err)
	}
}

var backoffCalls atomic.Int32

func countedFailForAnHour(context.Context, struct{}) (struct{}, error) {
	backoffCalls.Add(1)
	return struct{}{}, workflow.RetryableError(errors.New("not yet"), time.Hour)
}

func TestEarlyStepDeliveryWaitsForItsRetry(t *testing.T) {
	backoffCalls.Store(0)
	runner := workflow.Use(func(ctx context.Context, _ backoffInput) (backoffOutput, error) {
		_, err := workflow.Step(ctx, countedFailForAnHour, struct{}{}, workflow.StepOptions{MaxRetries: 3})
		return backoffOutput{}, err
	})
	ctx, cancel := context.WithTimeout(queuedSteps(), 5*time.Second)
	defer cancel()
	world := testworld.NewManual()
	defer world.Close()
	runtime := worlds.NewRuntime(world, worlds.Runners(runner))
	if _, err := workflow.Start(workflow.NewContext(ctx, workflow.NewClient(world)), backoffInput{}); err != nil {
		t.Fatal(err)
	}
	deliverNext(t, ctx, world, runtime)
	first := deliverNext(t, ctx, world, runtime) // fails, retry in an hour

	// The first attempt's delivery is redelivered before the retry is due.
	if err := runtime.Execute(ctx, first); err != nil {
		t.Fatal(err)
	}
	if backoffCalls.Load() != 1 {
		t.Fatalf("step calls = %d, want 1", backoffCalls.Load())
	}
	keys := map[string]bool{}
	for _, message := range world.QueuedMessages() {
		if message.Input.StepID == nil {
			continue
		}
		if message.Input.NotBefore == nil || time.Until(*message.Input.NotBefore) < 50*time.Minute {
			t.Fatalf("step delivery %#v is not held until the retry", message.Input)
		}
		keys[message.Input.IdempotencyKey] = true
	}
	if len(keys) != 1 {
		t.Fatalf("retry keys = %v, want one shared key", keys)
	}
}

func TestHookNextSkipsAPayloadThatFailsToDecode(t *testing.T) {
	runner := workflow.Use(func(ctx context.Context, input hookedInput) (hookedOutput, error) {
		hook := workflow.CreateHook[string](ctx, workflow.HookOption{Token: input.Token})
		if _, err := hook.Next(ctx); err == nil {
			return hookedOutput{}, errors.New("first payload decoded")
		}
		value, err := hook.Next(ctx)
		return hookedOutput{Value: value}, err
	})
	ctx, cancel := context.WithTimeout(queuedSteps(), 5*time.Second)
	defer cancel()
	world := testworld.NewManual()
	defer world.Close()
	runtime := worlds.NewRuntime(world, worlds.Runners(runner))
	client := workflow.NewContext(ctx, workflow.NewClient(world))
	token := workflow.NewHookToken()
	run, err := workflow.Start(client, hookedInput{Token: token})
	if err != nil {
		t.Fatal(err)
	}
	drain(t, ctx, world, runtime)
	if err := workflow.ResumeHook(client, token, 123); err != nil {
		t.Fatal(err)
	}
	if err := workflow.ResumeHook(client, token, "good"); err != nil {
		t.Fatal(err)
	}
	drain(t, ctx, world, runtime)
	output, err := run.Value(client)
	if err != nil || output.Value != "good" {
		t.Fatalf("output = %#v, err = %v", output, err)
	}
}

// queueOutage fails every Queue call.
type queueOutage struct{ *testworld.World }

func (queueOutage) Queue(context.Context, worlds.QueueInput) (worlds.QueueResult, error) {
	return worlds.QueueResult{}, errQueueUnavailable
}

func TestResumeHookReportsARecordedPayloadItCouldNotQueue(t *testing.T) {
	ctx, cancel := context.WithTimeout(queuedSteps(), 5*time.Second)
	defer cancel()
	world := testworld.NewManual()
	defer world.Close()
	runtime := worlds.NewRuntime(world, worlds.Runners(workflow.Use(awaitHook)))
	token := workflow.NewHookToken()
	run, err := workflow.Start(workflow.NewContext(ctx, workflow.NewClient(world)), hookedInput{Token: token})
	if err != nil {
		t.Fatal(err)
	}
	drain(t, ctx, world, runtime)
	hook, err := world.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: token})
	if err != nil {
		t.Fatal(err)
	}
	result, err := worlds.ResumeHook(ctx, queueOutage{world}, hook, []byte(`"value"`))
	if !errors.Is(err, worlds.ErrResumeNotQueued) || !errors.Is(err, errQueueUnavailable) || result.EventID == "" {
		t.Fatalf("result = %#v, err = %v", result, err)
	}

	// WakeRun queues the run even though it has no waits to complete.
	if _, err := worlds.WakeRun(ctx, world, worlds.RunID(run.ID()), nil); err != nil {
		t.Fatal(err)
	}
	drain(t, ctx, world, runtime)
	output, err := run.Value(workflow.NewContext(ctx, workflow.NewClient(world)))
	if err != nil || output.Value != "value" {
		t.Fatalf("output = %#v, err = %v", output, err)
	}
}

func TestDecodedFatalErrorKeepsItsCause(t *testing.T) {
	cause := errors.New("boom")
	for _, err := range []error{
		worlds.FatalError{Cause: cause},
		worlds.RetryableError{Cause: cause, After: time.Minute},
	} {
		data, marshalErr := json.Marshal(worlds.NewError(err))
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		decoded := worlds.DecodeError(data)
		var fatal worlds.FatalError
		var retryable worlds.RetryableError
		switch {
		case errors.As(decoded, &fatal):
			if fatal.Error() != "boom" || !errors.Is(fatal, cause) {
				t.Fatalf("fatal = %v, cause = %v", fatal, fatal.Cause)
			}
		case errors.As(decoded, &retryable):
			if retryable.Error() != "boom" || retryable.After != time.Minute || !errors.Is(retryable, cause) {
				t.Fatalf("retryable = %v, cause = %v", retryable, retryable.Cause)
			}
		default:
			t.Fatalf("%v decoded without its type", err)
		}
	}
	if message := (worlds.FatalError{}).Error(); message == "" {
		t.Fatal("a FatalError without a cause has no message")
	}
}

func TestThenSettlesANilCallbackResultWithAnError(t *testing.T) {
	runner := workflow.Use(func(ctx context.Context, _ backoffInput) (backoffOutput, error) {
		source := workflow.Background(ctx, alpha, struct{}{})
		_, err := workflow.Then(ctx, source, func(string, error) *workflow.Result[string] { return nil }).Value(ctx)
		if !errors.Is(err, workflow.ErrNilResult) {
			return backoffOutput{}, fmt.Errorf("then error = %v", err)
		}
		var missing *workflow.Result[string]
		if _, err := workflow.Select(ctx, missing); !errors.Is(err, workflow.ErrNilResult) {
			return backoffOutput{}, fmt.Errorf("select error = %v", err)
		}
		return backoffOutput{}, nil
	})
	ctx, cancel := context.WithTimeout(queuedSteps(), 5*time.Second)
	defer cancel()
	world := testworld.NewManual()
	defer world.Close()
	runtime := worlds.NewRuntime(world, worlds.Runners(runner))
	client := workflow.NewContext(ctx, workflow.NewClient(world))
	run, err := workflow.Start(client, backoffInput{})
	if err != nil {
		t.Fatal(err)
	}
	drain(t, ctx, world, runtime)
	if _, err := run.Value(client); err != nil {
		t.Fatal(err)
	}
}

// queuedSteps returns a context whose deliveries run no step inline, for tests
// of queued step deliveries.
func queuedSteps() context.Context {
	return worlds.WithMaxInlineSteps(context.Background(), 0)
}

type inlineInput struct{ Steps int }
type inlineOutput struct{ Values []int }

func (inlineInput) Workflow(inlineOutput) {}

func double(_ context.Context, value int) (int, error) { return value * 2, nil }

func fanOut(ctx context.Context, input inlineInput) (inlineOutput, error) {
	values := make([]int, input.Steps)
	for i := range values {
		values[i] = i
	}
	values, err := workflow.All(ctx, double, values)
	return inlineOutput{Values: values}, err
}

// startInline starts a run of runner on a manual World and returns the World,
// a Runtime for runner, the run's ID, and its first flow delivery.
func startInline(t *testing.T, ctx context.Context, runner worlds.Runner, input inlineInput) (*testworld.World, *worlds.Runtime, worlds.RunID, testworld.QueueMessage) {
	t.Helper()
	world := testworld.NewManual()
	t.Cleanup(world.Close)
	run, err := workflow.Start(workflow.NewContext(ctx, workflow.NewClient(world)), input)
	if err != nil {
		t.Fatal(err)
	}
	return world, worlds.NewRuntime(world, worlds.Runners(runner)), worlds.RunID(run.ID()), world.QueuedMessages()[0]
}

// inlineSteps returns the number of attempts the run's flows started inline.
func inlineSteps(t *testing.T, ctx context.Context, world worlds.World, runID worlds.RunID) int {
	t.Helper()
	events, err := world.EventsList(ctx, worlds.EventsListInput{RunID: runID})
	if err != nil {
		t.Fatal(err)
	}
	started := 0
	for _, event := range events.Data {
		var data worlds.DataStepStarted
		if event.EventType == worlds.EventTypeStepStarted && json.Unmarshal(event.Data, &data) == nil && data.OwnerMessageID != "" {
			started++
		}
	}
	return started
}

func stepDeliveries(world *testworld.World) []worlds.QueueInput {
	var deliveries []worlds.QueueInput
	for _, message := range world.QueuedMessages() {
		if message.Input.StepID != nil {
			deliveries = append(deliveries, message.Input)
		}
	}
	return deliveries
}

func TestFlowRunsTheFirstStepsInlineAndQueuesTheRest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	world, runtime, runID, delivery := startInline(t, ctx, workflow.Use(fanOut), inlineInput{Steps: 10})
	if err := runtime.Execute(ctx, delivery.Input); err != nil {
		t.Fatal(err)
	}
	if promoted, queued := inlineSteps(t, ctx, world, runID), len(stepDeliveries(world)); promoted != 3 || queued != 7 {
		t.Fatalf("inline=%d queued=%d, want 3 and 7", promoted, queued)
	}
	if err := world.QueueCancel(ctx, worlds.QueueCancelInput{MessageID: delivery.ID}); err != nil {
		t.Fatal(err)
	}
	drain(t, ctx, world, runtime)
	output, err := runOutput(t, ctx, world, runID)
	if err != nil || len(output.Values) != 10 || output.Values[9] != 18 {
		t.Fatalf("output=%v err=%v", output, err)
	}
}

func TestFlowRunsAChainOfStepsInOneDelivery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	runner := workflow.Use(func(ctx context.Context, input inlineInput) (inlineOutput, error) {
		value := 1
		for range input.Steps {
			var err error
			if value, err = workflow.Step(ctx, double, value); err != nil {
				return inlineOutput{}, err
			}
		}
		return inlineOutput{Values: []int{value}}, nil
	})
	world, runtime, runID, delivery := startInline(t, ctx, runner, inlineInput{Steps: 5})
	if err := runtime.Execute(ctx, delivery.Input); err != nil {
		t.Fatal(err)
	}
	if queued := len(world.QueuedMessages()); queued != 1 {
		t.Fatalf("queued = %d, want only the executed delivery", queued)
	}
	output, err := runOutput(t, ctx, world, runID)
	if err != nil || output.Values[0] != 32 {
		t.Fatalf("output=%v err=%v", output, err)
	}
}

func TestPromotedStepQueuesItsRetryAsAStepDelivery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var attempts atomic.Int32
	runner := workflow.Use(func(ctx context.Context, _ inlineInput) (inlineOutput, error) {
		value, err := workflow.Step(ctx, func(_ context.Context, value int) (int, error) {
			if attempts.Add(1) == 1 {
				return 0, errors.New("try again")
			}
			return value, nil
		}, 7, workflow.StepOptions{MaxRetries: 1})
		return inlineOutput{Values: []int{value}}, err
	})
	world, runtime, runID, delivery := startInline(t, ctx, runner, inlineInput{})
	if err := runtime.Execute(ctx, delivery.Input); err != nil {
		t.Fatal(err)
	}
	retries := stepDeliveries(world)
	if len(retries) != 1 || retries[0].IdempotencyKey != "retry:"+string(*retries[0].StepID)+":1" {
		t.Fatalf("step deliveries = %#v, want the retry", retries)
	}
	if err := world.QueueCancel(ctx, worlds.QueueCancelInput{MessageID: delivery.ID}); err != nil {
		t.Fatal(err)
	}
	drain(t, ctx, world, runtime)
	output, err := runOutput(t, ctx, world, runID)
	if err != nil || output.Values[0] != 7 || attempts.Load() != 2 {
		t.Fatalf("output=%v err=%v attempts=%d", output, err, attempts.Load())
	}
}

// crashingStepRunner returns a runner whose step's first attempt runs until
// its context ends, and the channel closed when it starts.
func crashingStepRunner(attempts *atomic.Int32) (worlds.Runner, chan struct{}) {
	started := make(chan struct{})
	return workflow.Use(func(ctx context.Context, _ inlineInput) (inlineOutput, error) {
		value, err := workflow.Step(ctx, func(ctx context.Context, value int) (int, error) {
			if attempts.Add(1) == 1 {
				close(started)
				<-ctx.Done()
				return 0, ctx.Err()
			}
			return value, nil
		}, 7)
		return inlineOutput{Values: []int{value}}, err
	}), started
}

// crash executes delivery until its step starts, then stops it, as when the
// process running it dies.
func crash(t *testing.T, ctx context.Context, runtime *worlds.Runtime, delivery worlds.QueueInput, started chan struct{}) {
	t.Helper()
	crashed, stop := context.WithCancel(ctx)
	go func() {
		<-started
		stop()
	}()
	if err := runtime.Execute(crashed, delivery); err == nil {
		t.Fatal("crashed delivery succeeded")
	}
}

func TestRedeliveredFlowRunsTheStepItStartedInlineAgain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var attempts atomic.Int32
	runner, started := crashingStepRunner(&attempts)
	world, runtime, runID, delivery := startInline(t, ctx, runner, inlineInput{})
	crash(t, ctx, runtime, delivery.Input, started)

	// The queue redelivers the flow's message, which owns the step, so it
	// runs the step again at once.
	if err := runtime.Execute(ctx, delivery.Input); err != nil {
		t.Fatal(err)
	}
	if queued := len(world.QueuedMessages()); queued != 1 {
		t.Fatalf("queued = %d, want only the redelivered message", queued)
	}
	output, err := runOutput(t, ctx, world, runID)
	if err != nil || output.Values[0] != 7 || attempts.Load() != 2 {
		t.Fatalf("output=%v err=%v attempts=%d", output, err, attempts.Load())
	}
}

func TestStepDeliveryTakesOverAPromotedStepAfterItsLease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		var attempts atomic.Int32
		runner, started := crashingStepRunner(&attempts)
		world, runtime, runID, delivery := startInline(t, ctx, runner, inlineInput{})
		crash(t, ctx, runtime, delivery.Input, started)
		if err := world.QueueCancel(ctx, worlds.QueueCancelInput{MessageID: delivery.ID}); err != nil {
			t.Fatal(err)
		}

		// While the lease lasts, another flow leaves the step to its owner.
		other := delivery.Input
		other.MessageID = "other"
		if err := runtime.Execute(ctx, other); err != nil {
			t.Fatal(err)
		}
		if queued := world.QueuedMessages(); len(queued) != 0 {
			t.Fatalf("queued = %#v, want nothing during the lease", queued)
		}

		// After the lease, a flow hands the step to a step delivery.
		time.Sleep(15 * time.Minute)
		if err := runtime.Execute(ctx, other); err != nil {
			t.Fatal(err)
		}
		takeOver := stepDeliveries(world)
		if len(takeOver) != 1 || takeOver[0].NotBefore != nil {
			t.Fatalf("step deliveries = %#v, want an immediate take-over", takeOver)
		}
		drain(t, ctx, world, runtime)
		output, err := runOutput(t, ctx, world, runID)
		if err != nil || output.Values[0] != 7 || attempts.Load() != 2 {
			t.Fatalf("output=%v err=%v attempts=%d", output, err, attempts.Load())
		}
		step, err := world.StepsGet(ctx, worlds.StepsGetInput{RunID: runID, StepID: *takeOver[0].StepID})
		if err != nil || step.Attempt != 2 {
			t.Fatalf("step = %#v err=%v", step, err)
		}
	})
}

func TestMaxInlineStepsSetsTheLimit(t *testing.T) {
	for _, test := range []struct {
		name    string
		handler int
		runner  int
		want    int
	}{
		{name: "handler", handler: 2, want: 2},
		{name: "handler zero", want: 0},
		{name: "runner", handler: 3, runner: 1, want: 1},
		{name: "clamped", handler: 99, want: 10},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			runner := workflow.Use(func(ctx context.Context, input inlineInput) (inlineOutput, error) {
				if test.runner != 0 {
					ctx = workflow.SetMaxInlineSteps(ctx, test.runner)
				}
				return fanOut(ctx, input)
			})
			world, runtime, runID, delivery := startInline(t, ctx, runner, inlineInput{Steps: 10})
			if err := runtime.Execute(workflow.SetMaxInlineSteps(ctx, test.handler), delivery.Input); err != nil {
				t.Fatal(err)
			}
			if promoted := inlineSteps(t, ctx, world, runID); promoted != test.want {
				t.Fatalf("inline = %d, want %d", promoted, test.want)
			}
		})
	}
}

func TestConcurrentFlowsRunAPromotedStepOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var calls atomic.Int32
	release := make(chan struct{})
	runner := workflow.Use(func(ctx context.Context, _ inlineInput) (inlineOutput, error) {
		value, err := workflow.Step(ctx, func(_ context.Context, value int) (int, error) {
			calls.Add(1)
			<-release
			return value, nil
		}, 7)
		return inlineOutput{Values: []int{value}}, err
	})
	world, runtime, runID, delivery := startInline(t, ctx, runner, inlineInput{})
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() { _ = runtime.Execute(ctx, delivery.Input) })
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("step calls = %d, want 1", calls.Load())
	}
	run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID})
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != worlds.StatusCompleted {
		drain(t, ctx, world, runtime)
	}
	if calls.Load() != 1 {
		t.Fatalf("step calls = %d, want 1", calls.Load())
	}
}

// writeCountingWorld counts the writes that append events for each step.
type writeCountingWorld struct {
	*testworld.World
	mu     sync.Mutex
	writes map[string]int
}

func (w *writeCountingWorld) count(events ...worlds.EventsCreateEvent) {
	w.mu.Lock()
	defer w.mu.Unlock()
	seen := map[string]bool{}
	for _, event := range events {
		if event.CorrelationID != "" && !seen[event.CorrelationID] {
			seen[event.CorrelationID] = true
			w.writes[event.CorrelationID]++
		}
	}
}

func (w *writeCountingWorld) EventsCreate(ctx context.Context, input worlds.EventsCreateInput) (worlds.EventResult, error) {
	w.count(input.Event)
	return w.World.EventsCreate(ctx, input)
}

func (w *writeCountingWorld) EventsCreateBatch(ctx context.Context, input worlds.EventsCreateBatchInput) (worlds.EventBatchResult, error) {
	w.count(input.Events...)
	return w.World.EventsCreateBatch(ctx, input)
}

func TestPromotedStepTakesTwoWrites(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	base := testworld.NewManual()
	defer base.Close()
	world := &writeCountingWorld{World: base, writes: map[string]int{}}
	run, err := workflow.Start(workflow.NewContext(ctx, workflow.NewClient(world)), inlineInput{Steps: 3})
	if err != nil {
		t.Fatal(err)
	}
	runtime := worlds.NewRuntime(world, worlds.Runners(workflow.Use(fanOut)))
	drain(t, ctx, base, runtime)
	if _, err := run.Value(workflow.NewContext(ctx, workflow.NewClient(world))); err != nil {
		t.Fatal(err)
	}
	steps := 0
	for id, writes := range world.writes {
		if id[:5] == "step_" {
			steps++
			if writes != 2 {
				t.Fatalf("step %s took %d writes, want 2", id, writes)
			}
		}
	}
	if steps != 3 {
		t.Fatalf("steps = %d, want 3", steps)
	}
}

func runOutput(t *testing.T, ctx context.Context, world worlds.World, runID worlds.RunID) (inlineOutput, error) {
	t.Helper()
	ctx = workflow.NewContext(ctx, workflow.NewClient(world))
	run, err := workflow.Get[inlineOutput](ctx, string(runID))
	if err != nil {
		t.Fatal(err)
	}
	return run.Value(ctx)
}

func TestFlowThatRecordsAnInlineResultIsStillSupersededByAnotherFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started, release := make(chan struct{}), make(chan struct{})
	var second atomic.Int32
	runner := workflow.Use(func(ctx context.Context, _ inlineInput) (inlineOutput, error) {
		first, err := workflow.Step(ctx, func(_ context.Context, value int) (int, error) {
			close(started)
			<-release
			return value, nil
		}, 1)
		if err != nil {
			return inlineOutput{}, err
		}
		value, err := workflow.Step(ctx, func(_ context.Context, value int) (int, error) {
			second.Add(1)
			return value, nil
		}, first+1)
		return inlineOutput{Values: []int{first, value}}, err
	})
	world, runtime, runID, delivery := startInline(t, ctx, runner, inlineInput{})
	done := make(chan error, 1)
	go func() { done <- runtime.Execute(ctx, delivery.Input) }()

	// Another flow writes while the first step runs inline.
	<-started
	run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeAttrSet, Data: []byte(`{"changes":[]}`)}, Options: worlds.EventsCreateOptions{InBandCount: &run.InBandCount}}); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	// Recording the first step's result does not hide that write, so the
	// flow's next declaration is refused and a follow-up flow replays.
	if second.Load() != 0 {
		t.Fatal("superseded flow ran the second step")
	}
	followUps := 0
	for _, message := range world.QueuedMessages() {
		if strings.HasPrefix(message.Input.IdempotencyKey, "superseded:") {
			followUps++
		}
	}
	if followUps != 1 {
		t.Fatalf("follow-ups = %d, want 1", followUps)
	}
}

func TestFlowWithoutAMessageIDQueuesNewSteps(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	world, runtime, runID, delivery := startInline(t, ctx, workflow.Use(fanOut), inlineInput{Steps: 2})
	// No message would run a step it started inline again, so none is.
	anonymous := delivery.Input
	anonymous.MessageID = ""
	if err := runtime.Execute(ctx, anonymous); err != nil {
		t.Fatal(err)
	}
	if promoted, queued := inlineSteps(t, ctx, world, runID), len(stepDeliveries(world)); promoted != 0 || queued != 2 {
		t.Fatalf("inline=%d queued=%d, want 0 and 2", promoted, queued)
	}
}

// refusedStartWorld fails the next EventsCreateBatch.
type refusedStartWorld struct {
	*testworld.World
	fail atomic.Bool
}

var errStartUnavailable = errors.New("batch unavailable")

func (w *refusedStartWorld) EventsCreateBatch(ctx context.Context, input worlds.EventsCreateBatchInput) (worlds.EventBatchResult, error) {
	if w.fail.CompareAndSwap(true, false) {
		return worlds.EventBatchResult{}, errStartUnavailable
	}
	return w.World.EventsCreateBatch(ctx, input)
}

func TestFailedStartReleasesTheStepForTheNextDelivery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var attempts atomic.Int32
	runner, started := crashingStepRunner(&attempts)
	base := testworld.NewManual()
	defer base.Close()
	world := &refusedStartWorld{World: base}
	run, err := workflow.Start(workflow.NewContext(ctx, workflow.NewClient(world)), inlineInput{})
	if err != nil {
		t.Fatal(err)
	}
	runtime := worlds.NewRuntime(world, worlds.Runners(runner))
	delivery := base.QueuedMessages()[0]
	crash(t, ctx, runtime, delivery.Input, started)

	// The redelivery fails to start the step again, and the next one does.
	world.fail.Store(true)
	if err := runtime.Execute(ctx, delivery.Input); !errors.Is(err, errStartUnavailable) {
		t.Fatalf("delivery error = %v, want the batch error", err)
	}
	if err := runtime.Execute(ctx, delivery.Input); err != nil {
		t.Fatal(err)
	}
	output, err := runOutput(t, ctx, world, worlds.RunID(run.ID()))
	if err != nil || output.Values[0] != 7 || attempts.Load() != 2 {
		t.Fatalf("output=%v err=%v attempts=%d", output, err, attempts.Load())
	}
}

// undatedWorld returns events without their creation time.
type undatedWorld struct{ *testworld.World }

func (w undatedWorld) EventsList(ctx context.Context, input worlds.EventsListInput) (worlds.Page[worlds.Event], error) {
	page, err := w.World.EventsList(ctx, input)
	for i := range page.Data {
		page.Data[i].CreatedAt = time.Time{}
	}
	return page, err
}

func TestUndatedInlineAttemptKeepsItsLease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		var attempts atomic.Int32
		runner, started := crashingStepRunner(&attempts)
		base := testworld.NewManual()
		defer base.Close()
		world := undatedWorld{World: base}
		if _, err := workflow.Start(workflow.NewContext(ctx, workflow.NewClient(world)), inlineInput{}); err != nil {
			t.Fatal(err)
		}
		runtime := worlds.NewRuntime(world, worlds.Runners(runner))
		delivery := base.QueuedMessages()[0]
		crash(t, ctx, runtime, delivery.Input, started)

		// Without a start time, the attempt's age is unknown, so another
		// flow leaves it to its owner however long it waits.
		time.Sleep(time.Hour)
		other := delivery.Input
		other.MessageID = "other"
		if err := runtime.Execute(ctx, other); err != nil {
			t.Fatal(err)
		}
		if takeOver := stepDeliveries(base); len(takeOver) != 0 {
			t.Fatalf("step deliveries = %#v, want none", takeOver)
		}
	})
}
