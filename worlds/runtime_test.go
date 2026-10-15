package worlds_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
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
	if err := workflow.OnRollback(ctx, undo, struct{}{}); err != nil {
		return versionedOutput{}, err
	}

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
	_, err := run.Await(workflow.NewContext(ctx, workflow.NewClient(world)))
	var failure *workflow.Error
	if !errors.As(err, &failure) || !strings.Contains(err.Error(), want) {
		t.Fatalf("run error = %v, want one containing %q", err, want)
	}
}

func TestReplayDivergenceFailsRunWithoutReachingRunnerCode(t *testing.T) {
	resetCalls()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
	if _, err := run.Await(workflow.NewContext(ctx, workflow.NewClient(world))); !errors.Is(err, worlds.ErrCorruptedEventLog) {
		t.Fatalf("run error = %v, want ErrCorruptedEventLog", err)
	}

	if betaCalls.Load() != 0 || rollbackCalls.Load() != 0 {
		t.Fatalf("beta calls = %d, rollback calls = %d", betaCalls.Load(), rollbackCalls.Load())
	}
}

func TestReplayDivergenceRequeuesTheDivergedStepDelivery(t *testing.T) {
	resetCalls()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
	hook, err := workflow.CreateHook[string](ctx, workflow.HookOption{Token: input.Token})
	if err != nil {
		return hookedOutput{}, err
	}

	value, err := hook.Await(ctx)
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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

	output, err := run.Await(workflow.NewContext(ctx, workflow.NewClient(d1)))
	if err != nil {
		t.Fatal(err)
	}

	if output.Version != "v1" {
		t.Fatalf("version = %q", output.Version)
	}
}

func TestDeploymentAffinityFailsRunAfterRerouteBudget(t *testing.T) {
	resetCalls()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
	if _, err := run.Await(workflow.NewContext(ctx, workflow.NewClient(d1))); !errors.Is(err, worlds.ErrDeploymentMismatch) {
		t.Fatalf("run error = %v, want ErrDeploymentMismatch", err)
	}

	if betaCalls.Load() != 0 || rollbackCalls.Load() != 0 {
		t.Fatalf("beta calls = %d, rollback calls = %d", betaCalls.Load(), rollbackCalls.Load())
	}
}

func TestDeploymentAffinityFailsRunWhenDeploymentIsUnavailable(t *testing.T) {
	resetCalls()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
	if _, err := run.Await(workflow.NewContext(ctx, workflow.NewClient(d1))); !errors.Is(err, worlds.ErrDeploymentMismatch) {
		t.Fatalf("run error = %v, want ErrDeploymentMismatch", err)
	}

	if len(world.Queued()) != 0 {
		t.Fatalf("queued after failure = %#v", world.Queued())
	}
}

func TestDeploymentAffinityAddressesHookResumesToTheRunsDeployment(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
