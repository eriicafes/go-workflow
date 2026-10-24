package workflow_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/testworld"
)

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
	child := func(ctx context.Context, input childRunInput) (childRunOutput, error) {
		value, err := workflow.Step(ctx, func(_ context.Context, _ struct{}) (string, error) { return "child", nil }, struct{}{})
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
	if count, err := run.WakeUp(ctx); err != nil || count != 1 {
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
	writer, release, err := run.Writer(ctx, workflow.WriterOption{Name: "side"})
	if err != nil {
		return struct{}{}, err
	}
	defer release()
	if _, err := writer.Write([]byte("from step")); err != nil {
		return struct{}{}, err
	}
	return struct{}{}, writer.Close()
}

func TestRunStreamsWorkInStepsNotWorkflowCode(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, _ streamAccessInput) (streamAccessOutput, error) {
		runID := workflow.GetWorkflowMetadata(ctx).RunID
		run, err := workflow.Get[streamAccessOutput](ctx, runID)
		if err != nil {
			return streamAccessOutput{}, err
		}
		_, _, workflowErr := run.Writer(ctx)
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
	runtime := worlds.NewRuntime(world, worlds.Runners(runner))
	for range 10 {
		executed := false
		for _, message := range world.QueuedMessages() {
			if string(message.Input.RunID) != parent.ID() {
				continue
			}
			if err := runtime.Execute(ctx, message.Input); err != nil {
				t.Fatal(err)
			}
			_ = world.QueueCancel(ctx, worlds.QueueCancelInput{MessageID: message.ID})
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
	writer, release, err := workflow.Writer(ctx)
	if err != nil {
		return time.Time{}, err
	}
	defer release()

	if _, err := io.WriteString(writer, p.Message); err != nil {
		return time.Time{}, err
	}

	if p.Close {
		if err := writer.Close(); err != nil {
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
