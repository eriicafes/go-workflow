package workflow_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/core"
	"github.com/eriicafes/go-workflow/worlds/testworld"
)

// TestMain turns lingering off: the tests drive deliveries by hand, or check
// what a delivery leaves once it suspends.
func TestMain(m *testing.M) {
	core.LingerDuration = 0
	os.Exit(m.Run())
}

type timerInput struct{}
type timerOutput struct{}

type structWorkflowInput struct {
	workflow.Struct[structWorkflowOutput]
	Value int
}
type structWorkflowOutput struct{ Value int }

type pointerWorkflowInput struct{ Value int }
type pointerWorkflowOutput struct{ Value int }
type unencodableInput struct{ Values chan int }
type unencodableOutput struct{ Values chan int }

type wakeInput struct{}
type wakeOutput struct{}
type childRunInput struct{ Token string }
type childRunOutput struct{ Value string }
type parentRunInput struct{}
type parentRunOutput struct {
	Value    string
	ValueErr string
	ChildID  string
}

func (timerInput) Workflow(timerOutput) {}

func (*pointerWorkflowInput) Workflow(pointerWorkflowOutput) {}
func (unencodableInput) Workflow(unencodableOutput)          {}

func (wakeInput) Workflow(wakeOutput)           {}
func (childRunInput) Workflow(childRunOutput)   {}
func (parentRunInput) Workflow(parentRunOutput) {}

type childCompletion struct {
	Token string
	Value string
}

// resumeParentHook reports a child run's result to its parent through the
// parent's hook.
func resumeParentHook(ctx context.Context, completion childCompletion) (struct{}, error) {
	return struct{}{}, workflow.ResumeHook(ctx, completion.Token, completion.Value)
}

func TestStartedChildRunResumesParentHook(t *testing.T) {
	// The child finishes only once the parent has read its result, so the
	// parent reads it unfinished.
	read := make(chan struct{})
	child := func(ctx context.Context, input childRunInput) (childRunOutput, error) {
		value, err := workflow.Step(ctx, func(context.Context, struct{}) (string, error) {
			<-read
			return "child", nil
		}, struct{}{})
		if err != nil {
			return childRunOutput{}, err
		}
		_, err = workflow.Step(ctx, resumeParentHook, childCompletion{Token: input.Token, Value: value})
		return childRunOutput{Value: value}, err
	}
	parent := func(ctx context.Context, _ parentRunInput) (parentRunOutput, error) {
		hook := workflow.CreateHook[string](ctx)
		if owner, err := hook.GetConflict(ctx); err != nil {
			return parentRunOutput{}, err
		} else if owner != nil {
			return parentRunOutput{}, workflow.ErrHookConflict
		}
		run, err := workflow.Start(ctx, childRunInput{Token: hook.Token()})
		if err != nil {
			return parentRunOutput{}, err
		}
		// Value records the child as unfinished instead of waiting for it.
		_, valueErr := run.Value(ctx)
		value, err := hook.Next(ctx)
		return parentRunOutput{Value: value, ValueErr: fmt.Sprint(valueErr), ChildID: run.ID()}, err
	}
	world := testworld.New(workflow.Use(child), workflow.Use(parent))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), 5*time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, parentRunInput{})
	if err != nil {
		t.Fatal(err)
	}
	for released := false; !released; time.Sleep(time.Millisecond) {
		steps, err := world.StepsList(ctx, worlds.StepsListInput{RunID: worlds.RunID(run.ID())})
		if err != nil {
			t.Fatal(err)
		}
		for _, step := range steps.Data {
			if !released && strings.HasSuffix(step.StepName, ".runValue") && step.Status == worlds.StatusCompleted {
				close(read)
				released = true
			}
		}
	}
	result, err := run.Value(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Value != "child" || result.ValueErr != workflow.ErrRunNotFinished.Error() {
		t.Fatalf("result = %#v", result)
	}
	if !strings.HasPrefix(result.ChildID, "wrun_") || len(result.ChildID) != len(worlds.NewRunID()) {
		t.Fatalf("child ID = %q, want a run ID", result.ChildID)
	}
	childRun, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: worlds.RunID(result.ChildID)})
	if err != nil {
		t.Fatal(err)
	}
	if childRun.Attributes[worlds.ParentRunIDAttribute] != run.ID() || childRun.Attributes[worlds.RootRunIDAttribute] != run.ID() {
		t.Fatalf("child attributes = %#v", childRun.Attributes)
	}
}

func TestStructDeclaresWorkflowOutput(t *testing.T) {
	world := testworld.New(workflow.Use(func(_ context.Context, input structWorkflowInput) (structWorkflowOutput, error) {
		return structWorkflowOutput{Value: input.Value + 1}, nil
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, structWorkflowInput{Value: 41})
	if err != nil {
		t.Fatal(err)
	}
	// The workflow is named after the struct that embeds Struct.
	expected := reflect.TypeFor[structWorkflowInput]().PkgPath() + "." + reflect.TypeFor[structWorkflowInput]().Name()
	if name, err := run.WorkflowName(ctx); err != nil || name != expected {
		t.Fatalf("workflow name = %q, %v; want %q", name, err, expected)
	}
	output, err := run.Value(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if output.Value != 42 {
		t.Fatalf("output = %#v", output)
	}
}

func TestPointerWorkflowMarkerUsesElementTypeIdentity(t *testing.T) {
	world := testworld.New(workflow.Use(func(_ context.Context, input *pointerWorkflowInput) (pointerWorkflowOutput, error) {
		return pointerWorkflowOutput{Value: input.Value + 1}, nil
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, &pointerWorkflowInput{Value: 41})
	if err != nil {
		t.Fatal(err)
	}
	expected := reflect.TypeFor[pointerWorkflowInput]().PkgPath() + "." + reflect.TypeFor[pointerWorkflowInput]().Name()
	if name, err := run.WorkflowName(ctx); err != nil || name != expected {
		t.Fatalf("workflow name = %q, %v; want %q", name, err, expected)
	}
	output, err := run.Value(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if output.Value != 42 {
		t.Fatalf("output = %#v", output)
	}
}

func TestRunWakeCompletesPendingDurableWaits(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, _ timerInput) (timerOutput, error) {
		_, err := workflow.Timer(ctx, time.Hour).Value(ctx)
		return timerOutput{}, err
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, timerInput{})
	if err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		events, err := world.EventsList(ctx, worlds.EventsListInput{RunID: worlds.RunID(run.ID())})
		if err != nil {
			t.Fatal(err)
		}
		for _, event := range events.Data {
			if event.EventType == worlds.EventTypeWaitCreated {
				goto ready
			}
		}
	}
	t.Fatal("timer was not created")

ready:
	count, err := run.WakeUp(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("wake count = %d", count)
	}
	if _, err := run.Value(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestStartAndGetCoverRunLifecycle(t *testing.T) {
	world := testworld.New(workflow.Use(func(_ context.Context, input *pointerWorkflowInput) (pointerWorkflowOutput, error) {
		return pointerWorkflowOutput{Value: input.Value}, nil
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, &pointerWorkflowInput{Value: 42})
	if err != nil {
		t.Fatal(err)
	}
	if exists, err := run.Exists(ctx); err != nil || !exists {
		t.Fatalf("run exists = %t, %v", exists, err)
	}
	if _, err := run.Value(ctx); err != nil {
		t.Fatal(err)
	}
	if status, err := run.Status(ctx); err != nil || status != workflow.StatusCompleted {
		t.Fatalf("run status = %q, %v", status, err)
	}
	if status, err := run.Status(ctx); err != nil || !status.Terminal() {
		t.Fatalf("run status = %s, %v", status, err)
	}
	loaded, err := workflow.Get[pointerWorkflowOutput](ctx, run.ID())
	if err != nil {
		t.Fatal(err)
	}
	if output, err := loaded.Value(ctx); err != nil || output.Value != 42 {
		t.Fatalf("loaded output = %#v, %v", output, err)
	}
	missing, err := workflow.Get[pointerWorkflowOutput](ctx, "wrun_missing")
	if err != nil {
		t.Fatal(err)
	}
	if exists, err := missing.Exists(ctx); err != nil || exists {
		t.Fatalf("missing exists = %t, %v", exists, err)
	}
}

func TestRunValueReportsDeletedData(t *testing.T) {
	world := testworld.New(workflow.Use(func(_ context.Context, input *pointerWorkflowInput) (pointerWorkflowOutput, error) {
		return pointerWorkflowOutput{Value: input.Value}, nil
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, &pointerWorkflowInput{Value: 42})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Value(ctx); err != nil {
		t.Fatal(err)
	}
	result, err := world.RunsDeleteDataMany(ctx, worlds.RunsDeleteDataManyInput{RunIDs: []worlds.RunID{worlds.RunID(run.ID())}})
	if err != nil || result.Summary.Deleted != 1 {
		t.Fatalf("delete = %+v, %v", result, err)
	}
	if _, err := run.Value(ctx); !errors.Is(err, workflow.ErrRunExpired) {
		t.Fatalf("deleted run error = %v", err)
	}
}

func TestDeleteRuns(t *testing.T) {
	world := testworld.New(workflow.Use(func(_ context.Context, input *pointerWorkflowInput) (pointerWorkflowOutput, error) {
		return pointerWorkflowOutput{Value: input.Value}, nil
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, &pointerWorkflowInput{Value: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Value(ctx); err != nil {
		t.Fatal(err)
	}
	result, err := workflow.DeleteRuns(ctx, run.ID(), "wrun_missing")
	if err != nil || result.Summary.Deleted != 1 || result.Summary.NotFound != 1 || result.Results[0].Outcome != worlds.DeleteDataDeleted {
		t.Fatalf("DeleteRuns = %+v, %v", result, err)
	}
	if _, err := run.Value(ctx); !errors.Is(err, workflow.ErrRunExpired) {
		t.Fatalf("deleted run error = %v", err)
	}
	if _, err := workflow.DeleteRuns(ctx); !errors.Is(err, worlds.ErrDeleteDataInvalid) {
		t.Fatalf("DeleteRuns without runs error = %v", err)
	}
	if _, err := workflow.DeleteRuns(ctx, run.ID(), run.ID()); !errors.Is(err, worlds.ErrDeleteDataInvalid) {
		t.Fatalf("DeleteRuns with a repeated run error = %v", err)
	}
	local := workflow.NewContext(context.Background(), workflow.NewClient(noDeleteWorld{world}))
	if _, err := workflow.DeleteRuns(local, run.ID()); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("DeleteRuns on a World without it error = %v", err)
	}
}

type deleteStepInput struct{ Target worlds.RunID }

func (deleteStepInput) Workflow(string) {}

// flakyDeleteWorld fails its first deletion retryably, or every one with a
// rate limit when throttle is set.
type flakyDeleteWorld struct {
	*testworld.World
	calls    *int
	throttle bool
}

func (w flakyDeleteWorld) RunsDeleteDataMany(ctx context.Context, input worlds.RunsDeleteDataManyInput) (worlds.RunsDeleteDataManyResult, error) {
	*w.calls++
	if w.throttle {
		return worlds.RunsDeleteDataManyResult{}, &worlds.ThrottleError{After: 7 * time.Second}
	}
	if *w.calls == 1 {
		results := make([]worlds.DeleteDataResult, len(input.RunIDs))
		for index, id := range input.RunIDs {
			results[index] = worlds.DeleteDataResult{RunID: id, Outcome: worlds.DeleteDataFailed, Code: "busy", Retryable: true}
		}
		return worlds.NewDeleteDataResult(results), nil
	}
	return w.World.RunsDeleteDataMany(ctx, input)
}

func TestDeleteRunsInAStepRetriesFailures(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	runners := core.Runners(workflow.Use(func(ctx context.Context, input deleteStepInput) (string, error) {
		return workflow.Step(ctx, func(ctx context.Context, target worlds.RunID) (string, error) {
			result, err := workflow.DeleteRuns(ctx, string(target))
			if err != nil {
				return "", err
			}
			return string(result.Results[0].Outcome), nil
		}, input.Target, workflow.StepOptions{MaxRetries: 1})
	}))
	for _, throttle := range []bool{false, true} {
		inner := testworld.NewManual()
		calls := 0
		world := flakyDeleteWorld{World: inner, calls: &calls, throttle: throttle}
		if _, err := inner.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: "wrun_target", WorkflowName: "target"}); err != nil {
			t.Fatal(err)
		}
		if _, err := inner.EventsCreate(ctx, worlds.EventsCreateInput{RunID: "wrun_target", Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeRunCancelled}}); err != nil {
			t.Fatal(err)
		}
		clientCtx := workflow.NewContext(ctx, workflow.NewClient(world))
		run, err := workflow.Start(clientCtx, deleteStepInput{Target: "wrun_target"})
		if err != nil {
			t.Fatal(err)
		}
		runtime := core.NewRuntime(world, runners)
		for delivered := 0; delivered < 3 && len(inner.QueuedMessages()) > 0; delivered++ {
			message := inner.QueuedMessages()[0]
			err := runtime.Execute(ctx, message.Input)
			if _, retry := worlds.RedeliveryOf(err); retry {
				// The step's retry waits for its message to come again.
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			_ = inner.Acknowledge(ctx, message.ID)
		}
		if !throttle {
			// The failed run was sent again once, within the step.
			if value, err := run.Value(clientCtx); err != nil || value != string(worlds.DeleteDataDeleted) || calls != 2 {
				t.Fatalf("flaky delete = %q, %v after %d calls", value, err, calls)
			}
			inner.Close()
			continue
		}
		// A rate limit retries the step after the World's wait.
		steps, err := inner.StepsList(ctx, worlds.StepsListInput{RunID: worlds.RunID(run.ID())})
		if err != nil || len(steps.Data) != 1 || steps.Data[0].RetryAfter == nil {
			t.Fatalf("throttled step = %+v, %v", steps.Data, err)
		}
		if wait := time.Until(*steps.Data[0].RetryAfter); wait < 5*time.Second || wait > 7*time.Second {
			t.Fatalf("throttled step retries in %v, want the World's 7s", wait)
		}
		inner.Close()
	}
}

// noDeleteWorld is a World that does not delete run data.
type noDeleteWorld struct{ *testworld.World }

func (noDeleteWorld) Capabilities() worlds.Capabilities { return worlds.Capabilities{} }

type optionsChildInput struct{ Data string }
type optionsParentInput struct{}

func (optionsChildInput) Workflow(string)        {}
func (optionsParentInput) Workflow(worlds.RunID) {}

func TestStartOptions(t *testing.T) {
	child := func(_ context.Context, input optionsChildInput) (string, error) { return input.Data, nil }
	parent := func(ctx context.Context, _ optionsParentInput) (worlds.RunID, error) {
		run, err := workflow.Start(ctx, optionsChildInput{}, workflow.StartOptions{Attributes: map[string]string{"tier": "child"}, Retention: workflow.RetainNone})
		if err != nil {
			return "", err
		}
		return worlds.RunID(run.ID()), nil
	}
	world := testworld.New(workflow.Use(child), workflow.Use(parent))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), 5*time.Second)
	defer cancel()

	// A top-level run records its retention and attributes, and its input
	// as given.
	large := strings.Repeat("compressible ", 1000)
	run, err := workflow.Start(ctx, optionsChildInput{Data: large}, workflow.StartOptions{
		Retention:               workflow.RetainNone,
		Attributes:              map[string]string{"tier": "gold", worlds.RetentionAttribute: "default"},
		AllowReservedAttributes: true,
		NoArgumentCompression:   true,
	})
	if err != nil {
		t.Fatal(err)
	}
	created, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: worlds.RunID(run.ID())})
	if err != nil {
		t.Fatal(err)
	}
	if created.Attributes["tier"] != "gold" || created.Attributes[worlds.RetentionAttribute] != "0" {
		t.Fatalf("attributes = %#v", created.Attributes)
	}
	if label := worlds.PayloadLabel(created.Input); label != worlds.PayloadDevalue {
		t.Fatalf("uncompressed input label = %q", label)
	}
	if _, err := run.Value(ctx); !errors.Is(err, workflow.ErrRunExpired) {
		t.Fatalf("RetainNone run error = %v", err)
	}

	// A child run takes its options beside its lineage.
	parentRun, err := workflow.Start(ctx, optionsParentInput{})
	if err != nil {
		t.Fatal(err)
	}
	childID, err := parentRun.Value(ctx)
	if err != nil {
		t.Fatal(err)
	}
	childRun, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: childID})
	if err != nil {
		t.Fatal(err)
	}
	if childRun.Attributes["tier"] != "child" || childRun.Attributes[worlds.RetentionAttribute] != "0" || childRun.Attributes[worlds.ParentRunIDAttribute] != parentRun.ID() {
		t.Fatalf("child attributes = %#v", childRun.Attributes)
	}

	// A replay records the run it replays, which must be a run ID.
	replay, err := workflow.Start(ctx, optionsChildInput{}, workflow.StartOptions{ReplayedFromRunID: run.ID()})
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: worlds.RunID(replay.ID())})
	if err != nil || replayed.ExecutionContext[worlds.ExecutionContextReplayedFromRunID] != run.ID() {
		t.Fatalf("replay = %+v, %v", replayed.ExecutionContext, err)
	}
	if _, err := workflow.Start(ctx, optionsChildInput{}, workflow.StartOptions{ReplayedFromRunID: "wrun_missing"}); !errors.Is(err, workflow.ErrInvalidStartOptions) {
		t.Fatalf("Start replaying a non-run ID error = %v", err)
	}

	// Attributes outside the SDK's limits start nothing.
	for _, attributes := range []map[string]string{
		{"$reserved": "x"},
		{"": "x"},
		{"key": strings.Repeat("v", worlds.MaxAttributeValueBytes+1)},
	} {
		if _, err := workflow.Start(ctx, optionsChildInput{}, workflow.StartOptions{Attributes: attributes}); !errors.Is(err, worlds.ErrAttributeInvalid) {
			t.Fatalf("Start with attributes %v error = %v", attributes, err)
		}
	}
}

// regionalWorld is a World that remembers the region each run ID was given.
type regionalWorld struct {
	*testworld.World
	regions *sync.Map
}

func (w regionalWorld) RegionRunID(id worlds.RunID, region worlds.Region) worlds.RunID {
	if region == "" {
		region = "iad1"
	}
	w.regions.Store(id, region)
	return id
}

func (w regionalWorld) Capabilities() worlds.Capabilities {
	capabilities := w.World.Capabilities()
	capabilities.RegionalRunIDs = true
	return capabilities
}

func (w regionalWorld) RegionForRunID(id worlds.RunID) (worlds.Region, bool) {
	region, ok := w.regions.Load(id)
	if !ok {
		return "", false
	}
	return region.(worlds.Region), true
}

type regionParentInput struct{}

func (regionParentInput) Workflow(string) {}

func TestStartRegion(t *testing.T) {
	runners := core.Runners(
		workflow.Use(func(_ context.Context, input optionsChildInput) (string, error) { return input.Data, nil }),
		workflow.Use(func(ctx context.Context, _ regionParentInput) (string, error) {
			run, err := workflow.Start(ctx, optionsChildInput{}, workflow.StartOptions{Region: worlds.Region("syd1")})
			if err != nil {
				return "", err
			}
			return run.ID(), nil
		}),
	)
	// The runtime runs on the regional World, so child starts see it.
	inner := testworld.NewManual()
	defer inner.Close()
	world := regionalWorld{inner, &sync.Map{}}
	server := httptest.NewServer(worlds.HTTPHandler(world, "/v1/workflow"))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for name, client := range map[string]workflow.Client{"in process": workflow.NewClient(world), "over HTTP": workflow.NewHTTPClient(server.URL + "/v1/workflow")} {
		clientCtx := workflow.NewContext(ctx, client)
		run, err := workflow.Start(clientCtx, optionsChildInput{}, workflow.StartOptions{Region: worlds.Region("fra1")})
		if err != nil {
			t.Fatal(err)
		}
		if region, _ := world.RegionForRunID(worlds.RunID(run.ID())); region != worlds.Region("fra1") {
			t.Errorf("%s run %s region = %s, want fra1", name, run.ID(), region)
		}
		plain, err := workflow.Start(clientCtx, optionsChildInput{})
		if err != nil {
			t.Fatal(err)
		}
		if region, _ := world.RegionForRunID(worlds.RunID(plain.ID())); region != worlds.Region("iad1") {
			t.Errorf("%s run without Region %s region = %s, want the World's iad1", name, plain.ID(), region)
		}
	}
	parent, err := workflow.Start(workflow.NewContext(ctx, workflow.NewClient(world)), regionParentInput{})
	if err != nil {
		t.Fatal(err)
	}
	runtime := core.NewRuntime(world, runners)
	for _, message := range inner.QueuedMessages() {
		if message.Input.RunID != worlds.RunID(parent.ID()) {
			continue
		}
		if err := runtime.Execute(ctx, message.Input); err != nil {
			t.Fatal(err)
		}
		_ = inner.Acknowledge(ctx, message.ID)
	}
	childID, err := parent.Value(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if region, _ := world.RegionForRunID(worlds.RunID(childID)); region != worlds.Region("syd1") {
		t.Errorf("child run %s region = %s, want syd1", childID, region)
	}
}

type startHookInput struct{ Token string }

func (startHookInput) Workflow(string) {}

func TestStartHook(t *testing.T) {
	world := testworld.NewManual()
	defer world.Close()
	runners := core.Runners(workflow.Use(func(ctx context.Context, input startHookInput) (string, error) {
		return workflow.CreateHook[string](ctx, workflow.HookOption{Token: input.Token}).Next(ctx)
	}))
	server := httptest.NewServer(worlds.HTTPHandler(world, "/v1/workflow"))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for name, client := range map[string]workflow.Client{"in process": workflow.NewClient(world), "over HTTP": workflow.NewHTTPClient(server.URL + "/v1/workflow")} {
		clientCtx := workflow.NewContext(ctx, client)
		token := "start-hook-" + strings.ReplaceAll(name, " ", "-")
		run, err := workflow.Start(clientCtx, startHookInput{Token: token}, workflow.StartOptions{Hook: &workflow.StartHook{Token: token, MinRetention: time.Hour, Metadata: "meta"}})
		if err != nil {
			t.Fatal(err)
		}
		// The hook takes a payload before the run first executes, and the
		// workflow's hook of the same token opens it.
		if err := workflow.ResumeHook(clientCtx, token, "early"); err != nil {
			t.Fatalf("%s: resume before the run started: %v", name, err)
		}
		_, err = workflow.Start(clientCtx, startHookInput{Token: token}, workflow.StartOptions{Hook: &workflow.StartHook{Token: token}})
		var conflict *worlds.HookConflictError
		if !errors.As(err, &conflict) || conflict.RunID != worlds.RunID(run.ID()) {
			t.Fatalf("%s: duplicate start error = %v", name, err)
		}
		runtime := core.NewRuntime(world, runners)
		for len(world.QueuedMessages()) != 0 {
			message := world.QueuedMessages()[0]
			if err := runtime.Execute(ctx, message.Input); err != nil {
				t.Fatal(err)
			}
			_ = world.Acknowledge(ctx, message.ID)
		}
		if value, err := run.Value(clientCtx); err != nil || value != "early" {
			t.Fatalf("%s: run value = %q, %v", name, value, err)
		}
	}
	unsupported := workflow.NewContext(ctx, workflow.NewClient(noDeleteWorld{world}))
	if _, err := workflow.Start(unsupported, startHookInput{}, workflow.StartOptions{Hook: &workflow.StartHook{Token: "t"}}); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("start hook on a World without it error = %v", err)
	}
}

func TestStartRejectsMissingClientAndUnencodableInput(t *testing.T) {
	if _, err := workflow.Start(context.Background(), unencodableInput{}); !errors.Is(err, workflow.ErrNoClient) {
		t.Fatalf("Start without client = %v", err)
	}
	world := testworld.New()
	defer world.Close()
	ctx := workflow.NewContext(context.Background(), workflow.NewClient(world))
	if _, err := workflow.Start(ctx, unencodableInput{Values: make(chan int)}); err == nil {
		t.Fatal("Start accepted an unencodable input")
	}
}

func TestUseRejectsInvalidInputAndUnencodableOutput(t *testing.T) {
	runner := workflow.Use(func(context.Context, unencodableInput) (unencodableOutput, error) {
		return unencodableOutput{Values: make(chan int)}, nil
	})
	if _, err := runner.Run(context.Background(), []byte("not json")); err == nil {
		t.Fatal("runner accepted invalid JSON input")
	}
	if _, err := runner.Run(context.Background(), []byte(`{"Value":1}`)); err == nil {
		t.Fatal("runner accepted an unencodable output")
	}
}

func TestRunCancelMakesPendingRunsTerminalAndIsIdempotent(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, _ wakeInput) (wakeOutput, error) {
		_, err := workflow.Timer(ctx, time.Hour).Value(ctx)
		return wakeOutput{}, err
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, wakeInput{})
	if err != nil {
		t.Fatal(err)
	}
	if err := run.Cancel(ctx); err != nil {
		t.Fatal(err)
	}
	if err := run.Cancel(ctx); err != nil {
		t.Fatalf("second cancel = %v", err)
	}
	if _, err := run.Value(ctx); !errors.Is(err, workflow.ErrRunCancelled) {
		t.Fatalf("cancelled run error = %v", err)
	}
	if status, err := run.Status(ctx); err != nil || status != workflow.StatusCancelled {
		t.Fatalf("cancelled status = %q, %v", status, err)
	}
	if status, err := run.Status(ctx); err != nil || !status.Terminal() {
		t.Fatalf("cancelled run status = %s, %v", status, err)
	}
}

func TestRunWakeUpFiltersWaitCorrelationIDs(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, _ wakeInput) (wakeOutput, error) {
		err := workflow.Join(ctx, workflow.Timer(ctx, time.Hour), workflow.Timer(ctx, time.Hour))
		return wakeOutput{}, err
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, wakeInput{})
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
	if count, err := run.WakeUp(ctx, workflow.WakeOptions{CorrelationIDs: waitIDs[:1]}); err != nil || count != 1 {
		t.Fatalf("filtered wake count = %d, %v", count, err)
	}
	if status, err := run.Status(ctx); err != nil || status.Terminal() {
		t.Fatalf("run status after one wake = %s, %v", status, err)
	}
	// The first wake's flow may not have completed its wait yet.
	if count, err := run.WakeUp(ctx); err != nil || count < 1 {
		t.Fatalf("remaining wake count = %d, %v", count, err)
	}
	if _, err := run.Value(ctx); err != nil {
		t.Fatal(err)
	}
}

type streamAccessInput struct{}
type streamAccessOutput struct{ WorkflowErr string }

func (streamAccessInput) Workflow(streamAccessOutput) {}

func writeOwnStream(ctx context.Context, runID string) (struct{}, error) {
	run, err := workflow.Get[streamAccessOutput](ctx, runID)
	if err != nil {
		return struct{}{}, err
	}
	writer, err := run.Writer(ctx, workflow.WriterOption{Name: "side"})
	if err != nil {
		return struct{}{}, err
	}
	defer writer.Close()
	if _, err := writer.Write([]byte("from step")); err != nil {
		return struct{}{}, err
	}
	return struct{}{}, writer.CloseStream()
}

func TestRunStreamsWorkInStepsNotWorkflowCode(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, _ streamAccessInput) (streamAccessOutput, error) {
		runID := workflow.GetWorkflowMetadata(ctx).RunID
		run, err := workflow.Get[streamAccessOutput](ctx, runID)
		if err != nil {
			return streamAccessOutput{}, err
		}
		_, workflowErr := run.Writer(ctx)
		_, err = workflow.Step(ctx, writeOwnStream, runID)
		return streamAccessOutput{WorkflowErr: fmt.Sprint(workflowErr)}, err
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), 5*time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, streamAccessInput{})
	if err != nil {
		t.Fatal(err)
	}
	output, err := run.Value(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if output.WorkflowErr != workflow.ErrNotInStep.Error() {
		t.Fatalf("Run.Writer in workflow code error = %q, want ErrNotInStep", output.WorkflowErr)
	}
	reader, err := run.Reader(ctx, workflow.ReaderOption{Name: "side"})
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(reader)
	if err != nil || string(data) != "from step" {
		t.Fatalf("stream = %q err=%v", data, err)
	}
}

type clientChoiceInput struct{}
type clientChoiceOutput struct{}
type clientChoiceChild struct{}

func (clientChoiceInput) Workflow(clientChoiceOutput) {}
func (clientChoiceChild) Workflow(struct{})           {}

type clientChoiceWorlds struct {
	explicit workflow.Client
}

var clientChoice clientChoiceWorlds

// startChildren starts one run with the step's context as given and one with
// an explicit client.
func startChildren(ctx context.Context, _ struct{}) (struct{}, error) {
	if _, err := workflow.Start(ctx, clientChoiceChild{}); err != nil {
		return struct{}{}, err
	}
	_, err := workflow.Start(workflow.NewContext(ctx, clientChoice.explicit), clientChoiceChild{})
	return struct{}{}, err
}

func TestStepsIgnoreInheritedClientsButUseExplicitOnes(t *testing.T) {
	other := testworld.NewManual()
	defer other.Close()
	clientChoice.explicit = workflow.NewClient(other)
	runner := workflow.Use(func(ctx context.Context, _ clientChoiceInput) (clientChoiceOutput, error) {
		_, err := workflow.Step(ctx, startChildren, struct{}{})
		return clientChoiceOutput{}, err
	})
	world := testworld.NewManual(runner)
	defer world.Close()
	// The delivery runs in a context that carries a client for another
	// World, as a queue consumer behind Middleware would.
	ctx := workflow.NewContext(context.Background(), workflow.NewClient(other))
	parent, err := workflow.Start(workflow.NewContext(context.Background(), workflow.NewClient(world)), clientChoiceInput{})
	if err != nil {
		t.Fatal(err)
	}
	runtime := core.NewRuntime(world, core.Runners(runner))
	for range 10 {
		executed := false
		for _, message := range world.QueuedMessages() {
			if string(message.Input.RunID) != parent.ID() {
				continue
			}
			if err := runtime.Execute(ctx, message.Input); err != nil {
				t.Fatal(err)
			}
			_ = world.Acknowledge(ctx, message.ID)
			executed = true
			break
		}
		if !executed {
			break
		}
	}
	own, err := world.RunsList(context.Background(), worlds.RunsListInput{})
	if err != nil {
		t.Fatal(err)
	}
	foreign, err := other.RunsList(context.Background(), worlds.RunsListInput{})
	if err != nil {
		t.Fatal(err)
	}
	if len(own.Data) != 2 || len(foreign.Data) != 1 {
		t.Fatalf("runs in the run's World = %d, want 2; in the explicit client's World = %d, want 1", len(own.Data), len(foreign.Data))
	}
}

type monthlyInput struct{}
type monthlyOutput struct{ Elapsed time.Duration }

func (monthlyInput) Workflow(monthlyOutput) {}

type progress struct {
	Message string
	Close   bool
}

// writeProgress writes message to the run's default stream and returns when
// it ran, so the test can measure the time that passed between steps.
func writeProgress(ctx context.Context, p progress) (time.Time, error) {
	writer, err := workflow.Writer(ctx)
	if err != nil {
		return time.Time{}, err
	}
	defer writer.Close()

	if _, err := io.WriteString(writer, p.Message); err != nil {
		return time.Time{}, err
	}

	if p.Close {
		if err := writer.CloseStream(); err != nil {
			return time.Time{}, err
		}
	}
	return time.Now(), nil
}

func monthly(ctx context.Context, _ monthlyInput) (monthlyOutput, error) {
	start, err := workflow.Step(ctx, writeProgress, progress{Message: "waiting;"})
	if err != nil {
		return monthlyOutput{}, err
	}

	if err := workflow.Sleep(ctx, 30*24*time.Hour); err != nil {
		return monthlyOutput{}, err
	}

	end, err := workflow.Step(ctx, writeProgress, progress{Message: "done", Close: true})
	if err != nil {
		return monthlyOutput{}, err
	}

	return monthlyOutput{Elapsed: end.Sub(start)}, nil
}

// TestSleepUnderSynctest checks that the in-memory World blocks instead of
// polling, so testing/synctest advances its fake clock straight to a timer's
// due time. A poll in Run.Value, stream readers, or the World would step the
// clock in small increments and time out on a 30-day sleep.
func TestSleepUnderSynctest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		world := testworld.New(workflow.Use(monthly))
		defer world.Close()

		ctx := workflow.NewContext(t.Context(), workflow.NewClient(world))
		run, err := workflow.Start(ctx, monthlyInput{})
		if err != nil {
			t.Fatal(err)
		}

		reader, err := run.Reader(ctx)
		if err != nil {
			t.Fatal(err)
		}

		streamed, err := io.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}

		if string(streamed) != "waiting;done" {
			t.Fatalf("stream = %q", streamed)
		}

		output, err := run.Value(ctx)
		if err != nil {
			t.Fatal(err)
		}

		if output.Elapsed < 30*24*time.Hour {
			t.Fatalf("elapsed = %v, want at least 30 days", output.Elapsed)
		}
	})
}

type wakeChildInput struct{}
type wakeChildOutput struct{}

func (wakeChildInput) Workflow(wakeChildOutput) {}

type wakeParentInput struct{}
type wakeParentOutput struct {
	Woken int
	Child string
}

func (wakeParentInput) Workflow(wakeParentOutput) {}

func TestRunWakeUpInWorkflowWakesAChildTimer(t *testing.T) {
	world := testworld.New(
		workflow.Use(func(ctx context.Context, _ wakeChildInput) (wakeChildOutput, error) {
			return wakeChildOutput{}, workflow.Sleep(ctx, time.Hour)
		}),
		workflow.Use(func(ctx context.Context, _ wakeParentInput) (wakeParentOutput, error) {
			child, err := workflow.Start(ctx, wakeChildInput{})
			if err != nil {
				return wakeParentOutput{}, err
			}
			// Each WakeUp is recorded, so polling until the child has
			// created its timer replays the same way.
			for range 100 {
				woken, err := child.WakeUp(ctx)
				if err != nil || woken != 0 {
					return wakeParentOutput{Woken: woken, Child: child.ID()}, err
				}
				if err := workflow.Sleep(ctx, 10*time.Millisecond); err != nil {
					return wakeParentOutput{}, err
				}
			}
			return wakeParentOutput{}, errors.New("child never created its timer")
		}),
	)
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), 5*time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, wakeParentInput{})
	if err != nil {
		t.Fatal(err)
	}
	output, err := run.Value(ctx)
	if err != nil || output.Woken != 1 {
		t.Fatalf("output = %#v, err = %v, want one timer woken", output, err)
	}
	child, err := workflow.Get[wakeChildOutput](ctx, output.Child)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := child.Value(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestRunCancellationLetsRunningStepFinish(t *testing.T) {
	for _, test := range []struct {
		name   string
		inline bool
	}{{"step", false}, {"inline", true}} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				started, release := make(chan struct{}), make(chan struct{})
				world := testworld.New(workflow.Use(func(ctx context.Context, _ timerInput) (timerOutput, error) {
					step := workflow.Step[struct{}, struct{}]
					if test.inline {
						step = workflow.InlineStep[struct{}, struct{}]
					}
					_, err := step(ctx, func(ctx context.Context, _ struct{}) (struct{}, error) {
						close(started)
						select {
						case <-release:
							return struct{}{}, nil
						case <-ctx.Done():
							return struct{}{}, errors.New("run cancelled active step")
						}
					}, struct{}{})
					return timerOutput{}, err
				}))
				defer world.Close()
				ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), 2*time.Second)
				defer cancel()
				run, err := workflow.Start(ctx, timerInput{})
				if err != nil {
					t.Fatal(err)
				}
				select {
				case <-started:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				// A first delivery starts an inline body before its start is
				// written, so the run is cancelled once the step is recorded
				// as running.
				for {
					steps, err := world.StepsList(ctx, worlds.StepsListInput{RunID: worlds.RunID(run.ID())})
					if err != nil {
						t.Fatal(err)
					}
					if len(steps.Data) == 1 && steps.Data[0].Status == worlds.StatusRunning {
						break
					}
					time.Sleep(time.Millisecond)
				}
				if err := run.Cancel(ctx); err != nil {
					t.Fatal(err)
				}
				// Allow live cancellation watchers to react before releasing the attempt.
				time.Sleep(time.Second)
				close(release)
				for {
					steps, err := world.StepsList(ctx, worlds.StepsListInput{RunID: worlds.RunID(run.ID())})
					if err != nil {
						t.Fatal(err)
					}
					if len(steps.Data) == 1 && steps.Data[0].Status.Terminal() {
						if steps.Data[0].Status != worlds.StatusCompleted {
							t.Fatalf("step status=%s error=%s", steps.Data[0].Status, steps.Data[0].Error)
						}
						break
					}
					if ctx.Err() != nil {
						t.Fatal(ctx.Err())
					}
					time.Sleep(time.Millisecond)
				}
				if status, err := run.Status(ctx); err != nil || status != workflow.StatusCancelled {
					t.Fatalf("run status=%s err=%v", status, err)
				}
			})
		})
	}
}

func TestRunCanCancelItselfThroughAnInlineStep(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, _ timerInput) (timerOutput, error) {
		run, err := workflow.Get[timerOutput](ctx, workflow.GetWorkflowMetadata(ctx).RunID)
		if err != nil {
			return timerOutput{}, err
		}
		return timerOutput{}, run.Cancel(ctx)
	}))
	defer world.Close()
	ctx, stop := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), 3*time.Second)
	defer stop()
	run, err := workflow.Start(ctx, timerInput{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Value(ctx); !errors.Is(err, workflow.ErrRunCancelled) {
		t.Fatalf("run error=%v", err)
	}
	for {
		steps, err := world.StepsList(ctx, worlds.StepsListInput{RunID: worlds.RunID(run.ID())})
		if err != nil {
			t.Fatal(err)
		}
		if len(steps.Data) == 1 && steps.Data[0].Status == worlds.StatusCompleted {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("inline cancel did not finish: %v steps=%#v", ctx.Err(), steps.Data)
		}
		time.Sleep(time.Millisecond)
	}
}
