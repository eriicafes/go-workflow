package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/testworld"
)

type joinedInput struct{}
type joinedOutput struct{ Values []int }
type timerInput struct{}
type timerOutput struct{}
type rollbackInput struct{}
type rollbackOutput struct{}
type nowInput struct{}
type nowOutput struct{ Value time.Time }
type pointerWorkflowInput struct{ Value int }
type pointerWorkflowOutput struct{ Value int }
type unencodableInput struct{ Values chan int }
type unencodableOutput struct{ Values chan int }
type retryInput struct{}
type retryOutput struct{}
type wakeInput struct{}
type wakeOutput struct{}
type childRunInput struct{}
type childRunOutput struct{ Value string }
type parentRunInput struct{}
type parentRunOutput struct{ Value string }
type collectionInput struct{}
type collectionOutput struct {
	All              []int
	SettledFailed    bool
	SelectError      bool
	SelectOK         int
	SelectWithCancel bool
	SelectOKCancel   bool
}
type collectionStepInput struct {
	Value int
	Delay time.Duration
	Fail  bool
}

func (joinedInput) Workflow(joinedOutput)                    {}
func (timerInput) Workflow(timerOutput)                      {}
func (rollbackInput) Workflow(rollbackOutput)                {}
func (nowInput) Workflow(nowOutput)                          {}
func (*pointerWorkflowInput) Workflow(pointerWorkflowOutput) {}
func (unencodableInput) Workflow(unencodableOutput)          {}
func (retryInput) Workflow(retryOutput)                      {}
func (wakeInput) Workflow(wakeOutput)                        {}
func (childRunInput) Workflow(childRunOutput)                {}
func (parentRunInput) Workflow(parentRunOutput)              {}
func (collectionInput) Workflow(collectionOutput)            {}

func increment(_ context.Context, value int) (int, error) { return value + 1, nil }

func collectionStep(_ context.Context, input collectionStepInput) (int, error) {
	if input.Delay > 0 {
		time.Sleep(input.Delay)
	}
	if input.Fail {
		return 0, errors.New("collection step failed")
	}
	return input.Value, nil
}

func collectionVoidStep(ctx context.Context, input collectionStepInput) (struct{}, error) {
	_, err := collectionStep(ctx, input)
	return struct{}{}, err
}

func TestStartReplaysChildRunAndWakesParent(t *testing.T) {
	child := func(ctx context.Context, _ childRunInput) (childRunOutput, error) {
		value, err := workflow.Step(ctx, func(_ context.Context, _ struct{}) (string, error) { return "child", nil }, struct{}{})
		return childRunOutput{Value: value}, err
	}
	parent := func(ctx context.Context, _ parentRunInput) (parentRunOutput, error) {
		run, err := workflow.Start(ctx, childRunInput{})
		if err != nil {
			return parentRunOutput{}, err
		}
		result, err := run.Await(ctx)
		return parentRunOutput{Value: result.Value}, err
	}
	world := testworld.New(workflow.Use(child), workflow.Use(parent))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, parentRunInput{})
	if err != nil {
		t.Fatal(err)
	}
	result, err := run.Await(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Value != "child" {
		t.Fatalf("result = %#v", result)
	}
	runs, err := world.RunsList(ctx, worlds.RunsListInput{})
	if err != nil {
		t.Fatal(err)
	}
	if len(runs.Data) != 2 {
		t.Fatalf("run count = %d, want 2", len(runs.Data))
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
	output, err := run.Await(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if output.Value != 42 {
		t.Fatalf("output = %#v", output)
	}
}

func TestStepRetriesThroughEngine(t *testing.T) {
	var attempts atomic.Int32
	world := testworld.New(workflow.Use(func(ctx context.Context, input *pointerWorkflowInput) (pointerWorkflowOutput, error) {
		value, err := workflow.Step(ctx, func(_ context.Context, value int) (int, error) {
			if attempts.Add(1) == 1 {
				return 0, errors.New("retry once")
			}
			return value + 1, nil
		}, input.Value, workflow.StepOptions{MaxRetries: 1})
		return pointerWorkflowOutput{Value: value}, err
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, &pointerWorkflowInput{Value: 41})
	if err != nil {
		t.Fatal(err)
	}
	output, err := run.Await(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if output.Value != 42 {
		t.Fatalf("output = %#v", output)
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("attempts = %d, want 2", got)
	}
	events, err := world.EventsList(ctx, worlds.EventsListInput{RunID: worlds.RunID(run.ID())})
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events.Data {
		if event.EventType == worlds.EventTypeStepRetrying {
			return
		}
	}
	t.Fatal("missing step_retrying event")
}

func TestJoinWaitsForEveryDurableResult(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, _ joinedInput) (joinedOutput, error) {
		values, err := workflow.Join(ctx,
			workflow.Background(ctx, increment, 1),
			workflow.Background(ctx, increment, 2),
		)
		return joinedOutput{Values: values}, err
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, joinedInput{})
	if err != nil {
		t.Fatal(err)
	}
	output, err := run.Await(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(output.Values, []int{2, 3}) {
		t.Fatalf("values = %#v", output.Values)
	}
}

func TestTimerAndTimeoutUseDurableWaits(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, _ timerInput) (timerOutput, error) {
		if _, err := workflow.Timer(ctx, time.Millisecond).Await(ctx); err != nil {
			return timerOutput{}, err
		}
		return timerOutput{}, nil
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, timerInput{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Await(ctx); err != nil {
		t.Fatal(err)
	}

	timeoutWorld := testworld.New(workflow.Use(func(ctx context.Context, _ timerInput) (timerOutput, error) {
		_, err := workflow.Timeout(ctx, time.Millisecond).Await(ctx)
		return timerOutput{}, err
	}))
	defer timeoutWorld.Close()
	timeoutRun, err := workflow.Start(workflow.NewContext(ctx, workflow.NewClient(timeoutWorld)), timerInput{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := timeoutRun.Await(ctx); !errors.Is(err, workflow.ErrRunCancelled) {
		t.Fatalf("timeout error = %v", err)
	}
}

func TestOnRollbackRunsInReverseRegistrationOrder(t *testing.T) {
	var calls []string
	var mutex sync.Mutex
	world := testworld.New(workflow.Use(func(ctx context.Context, _ rollbackInput) (rollbackOutput, error) {
		if err := workflow.OnRollback(ctx, func(_ context.Context, value string) error {
			mutex.Lock()
			calls = append(calls, value)
			mutex.Unlock()
			return nil
		}, "first"); err != nil {
			return rollbackOutput{}, err
		}
		if err := workflow.OnRollback(ctx, func(_ context.Context, value string) error {
			mutex.Lock()
			calls = append(calls, value)
			mutex.Unlock()
			return nil
		}, "second"); err != nil {
			return rollbackOutput{}, err
		}
		return rollbackOutput{}, errors.New("primary failure")
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, rollbackInput{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Await(ctx); err == nil || err.Error() != "workflow: run failed: primary failure" {
		t.Fatalf("failure = %v", err)
	}
	mutex.Lock()
	defer mutex.Unlock()
	if !slices.Equal(calls, []string{"second", "first"}) {
		t.Fatalf("rollback calls = %#v", calls)
	}
}

func TestOnRollbackRunsAfterPriorDurableStep(t *testing.T) {
	var calls atomic.Int32
	world := testworld.New(workflow.Use(func(ctx context.Context, _ rollbackInput) (rollbackOutput, error) {
		if _, err := workflow.Step(ctx, increment, 1); err != nil {
			return rollbackOutput{}, err
		}
		if err := workflow.OnRollback(ctx, func(context.Context, struct{}) error {
			calls.Add(1)
			return nil
		}, struct{}{}); err != nil {
			return rollbackOutput{}, err
		}
		return rollbackOutput{}, errors.New("primary failure")
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, rollbackInput{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Await(ctx); err == nil || err.Error() != "workflow: run failed: primary failure" {
		t.Fatalf("failure = %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("rollback calls = %d, want 1", calls.Load())
	}
}

func TestOnRollbackDoesNotRunAfterSuccess(t *testing.T) {
	var calls atomic.Int32
	world := testworld.New(workflow.Use(func(ctx context.Context, _ rollbackInput) (rollbackOutput, error) {
		if err := workflow.OnRollback(ctx, func(context.Context, struct{}) error {
			calls.Add(1)
			return nil
		}, struct{}{}); err != nil {
			return rollbackOutput{}, err
		}
		return rollbackOutput{}, nil
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, rollbackInput{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Await(ctx); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 0 {
		t.Fatalf("rollback calls = %d, want 0", calls.Load())
	}
}

func TestOnRollbackFailureIsRecordedAsStepFailure(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, _ rollbackInput) (rollbackOutput, error) {
		if err := workflow.OnRollback(ctx, func(context.Context, struct{}) error {
			return errors.New("rollback failure")
		}, struct{}{}); err != nil {
			return rollbackOutput{}, err
		}
		return rollbackOutput{}, errors.New("primary failure")
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, rollbackInput{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Await(ctx); err == nil || err.Error() != "workflow: run failed: primary failure" {
		t.Fatalf("failure = %v", err)
	}
	events, err := world.EventsList(ctx, worlds.EventsListInput{RunID: worlds.RunID(run.ID())})
	if err != nil {
		t.Fatal(err)
	}
	rollbackLogged := false
	stepFailed := false
	for _, event := range events.Data {
		var created struct {
			Rollback bool `json:"rollback"`
		}
		if event.EventType == worlds.EventTypeStepCreated && json.Unmarshal(event.Data, &created) == nil && created.Rollback {
			rollbackLogged = true
		}
		var result struct {
			Error []byte `json:"error"`
		}
		if event.EventType == worlds.EventTypeStepFailed && json.Unmarshal(event.Data, &result) == nil && string(result.Error) == "rollback failure" {
			stepFailed = true
		}
	}
	if !rollbackLogged || !stepFailed {
		t.Fatalf("rollback events were not recorded: %#v", events.Data)
	}
}

func TestOnRollbackRegistersAroundDurableSteps(t *testing.T) {
	var calls []string
	var mutex sync.Mutex
	record := func(value string) {
		mutex.Lock()
		calls = append(calls, value)
		mutex.Unlock()
	}
	world := testworld.New(workflow.Use(func(ctx context.Context, _ rollbackInput) (rollbackOutput, error) {
		if err := workflow.OnRollback(ctx, func(context.Context, struct{}) error {
			record("before step")
			return nil
		}, struct{}{}); err != nil {
			return rollbackOutput{}, err
		}
		if _, err := workflow.Step(ctx, func(context.Context, struct{}) (struct{}, error) {
			record("step")
			return struct{}{}, nil
		}, struct{}{}); err != nil {
			return rollbackOutput{}, err
		}
		if err := workflow.OnRollback(ctx, func(context.Context, struct{}) error {
			record("after step")
			return nil
		}, struct{}{}); err != nil {
			return rollbackOutput{}, err
		}
		return rollbackOutput{}, errors.New("primary failure")
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, rollbackInput{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Await(ctx); err == nil || err.Error() != "workflow: run failed: primary failure" {
		t.Fatalf("failure = %v", err)
	}
	mutex.Lock()
	defer mutex.Unlock()
	if !slices.Equal(calls, []string{"step", "after step", "before step"}) {
		t.Fatalf("calls = %#v", calls)
	}
}

func TestNowUsesAnImmediatelyCompletedInlineStep(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, _ nowInput) (nowOutput, error) {
		value, err := workflow.Now(ctx)
		return nowOutput{Value: value}, err
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, nowInput{})
	if err != nil {
		t.Fatal(err)
	}
	output, err := run.Await(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if output.Value.IsZero() {
		t.Fatal("durable now returned zero time")
	}
	events, err := world.EventsList(ctx, worlds.EventsListInput{RunID: worlds.RunID(run.ID())})
	if err != nil {
		t.Fatal(err)
	}
	created, completed := false, false
	for _, event := range events.Data {
		if event.EventType == worlds.EventTypeStepCreated {
			var data struct {
				StepName string `json:"stepName"`
			}
			if err := json.Unmarshal(event.Data, &data); err != nil {
				t.Fatal(err)
			}
			created = data.StepName == "now"
		}
		if event.EventType == worlds.EventTypeStepCompleted {
			completed = true
		}
		if event.EventType == worlds.EventTypeStepStarted {
			t.Fatal("inline now queued targeted step work")
		}
	}
	if !created || !completed {
		t.Fatalf("inline now events created=%t completed=%t", created, completed)
	}
}

func TestRunWakeCompletesPendingDurableWaits(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, _ timerInput) (timerOutput, error) {
		_, err := workflow.Timer(ctx, time.Hour).Await(ctx)
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
	if _, err := run.Await(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestCancellingTimerCancelsItsQueueSchedule(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, _ timerInput) (timerOutput, error) {
		timer := workflow.Timer(ctx, time.Hour)
		if _, err := workflow.Step(ctx, increment, 1); err != nil {
			return timerOutput{}, err
		}
		if err := timer.Cancel(ctx); err != nil {
			return timerOutput{}, err
		}
		return timerOutput{}, nil
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, timerInput{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Await(ctx); err != nil {
		t.Fatal(err)
	}
	for _, queued := range world.Queued() {
		if queued.NotBefore != nil {
			t.Fatalf("cancelled timer remained queued until %v", *queued.NotBefore)
		}
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
	if _, err := run.Await(ctx); err != nil {
		t.Fatal(err)
	}
	if status, err := run.Status(ctx); err != nil || status != workflow.StatusCompleted {
		t.Fatalf("run status = %q, %v", status, err)
	}
	if done, err := run.Done(ctx); err != nil || !done {
		t.Fatalf("run done = %t, %v", done, err)
	}
	loaded, err := workflow.Get[pointerWorkflowOutput](ctx, run.ID())
	if err != nil {
		t.Fatal(err)
	}
	if output, err := loaded.Await(ctx); err != nil || output.Value != 42 {
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

func TestFatalAndRetryableStepErrorsControlRetries(t *testing.T) {
	var fatalAttempts atomic.Int32
	var retryAttempts atomic.Int32
	world := testworld.New(
		workflow.Use(func(ctx context.Context, _ retryInput) (retryOutput, error) {
			_, err := workflow.Step(ctx, func(context.Context, struct{}) (struct{}, error) {
				fatalAttempts.Add(1)
				return struct{}{}, workflow.FatalError(errors.New("fatal"))
			}, struct{}{}, workflow.StepOptions{MaxRetries: 3})
			return retryOutput{}, err
		}),
		workflow.Use(func(ctx context.Context, _ collectionInput) (collectionOutput, error) {
			_, err := workflow.Step(ctx, func(context.Context, struct{}) (struct{}, error) {
				if retryAttempts.Add(1) == 1 {
					return struct{}{}, workflow.RetryableError(errors.New("retry"), time.Millisecond)
				}
				return struct{}{}, nil
			}, struct{}{}, workflow.StepOptions{MaxRetries: 1})
			return collectionOutput{}, err
		}),
	)
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	fatalRun, err := workflow.Start(ctx, retryInput{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fatalRun.Await(ctx); err == nil || fatalAttempts.Load() != 1 {
		t.Fatalf("fatal run error = %v, attempts = %d", err, fatalAttempts.Load())
	}
	retryRun, err := workflow.Start(ctx, collectionInput{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := retryRun.Await(ctx); err != nil || retryAttempts.Load() != 2 {
		t.Fatalf("retry run error = %v, attempts = %d", err, retryAttempts.Load())
	}
}

func TestWorkflowCollectionHelpers(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, _ collectionInput) (collectionOutput, error) {
		all, err := workflow.All(ctx, collectionStep, []collectionStepInput{{Value: 1}, {Value: 2}})
		if err != nil {
			return collectionOutput{}, err
		}
		settled := workflow.Map(ctx, collectionStep, []collectionStepInput{{Value: 3}, {Fail: true}})
		if _, err := workflow.JoinSettled(ctx, settled...); err != nil {
			return collectionOutput{}, err
		}
		_, settledErr := settled[1].Await(ctx)

		failed := workflow.Background(ctx, collectionStep, collectionStepInput{Fail: true, Delay: time.Millisecond}, workflow.StepOptions{MaxRetries: 0})
		success := workflow.Background(ctx, collectionStep, collectionStepInput{Value: 4, Delay: 2 * time.Millisecond})
		selected, selectErr := workflow.Select(ctx, failed, success)
		if selected == nil {
			return collectionOutput{}, errors.New("Select returned no promise")
		}

		failedOK := workflow.Background(ctx, collectionStep, collectionStepInput{Fail: true, Delay: time.Millisecond}, workflow.StepOptions{MaxRetries: 0})
		successOK := workflow.Background(ctx, collectionStep, collectionStepInput{Value: 5, Delay: 2 * time.Millisecond})
		selectedOK, err := workflow.SelectOk(ctx, failedOK, successOK)
		if err != nil {
			return collectionOutput{}, err
		}
		okValue, err := selectedOK.Await(ctx)
		if err != nil {
			return collectionOutput{}, err
		}

		timer := workflow.Timer(ctx, time.Hour)
		successCancel := workflow.Background(ctx, collectionVoidStep, collectionStepInput{Value: 6})
		selectedCancel, err := workflow.SelectWithCancel(ctx, timer, successCancel)
		if err != nil {
			return collectionOutput{}, err
		}
		if _, err := selectedCancel.Await(ctx); err != nil {
			return collectionOutput{}, err
		}

		failedOKCancel := workflow.Background(ctx, collectionVoidStep, collectionStepInput{Fail: true, Delay: time.Millisecond}, workflow.StepOptions{MaxRetries: 0})
		timerOKCancel := workflow.Timer(ctx, time.Hour)
		successOKCancel := workflow.Background(ctx, collectionVoidStep, collectionStepInput{Value: 7, Delay: 2 * time.Millisecond})
		selectedOKCancel, err := workflow.SelectOkWithCancel(ctx, failedOKCancel, timerOKCancel, successOKCancel)
		if err != nil {
			return collectionOutput{}, err
		}
		if _, err := selectedOKCancel.Await(ctx); err != nil {
			return collectionOutput{}, err
		}
		return collectionOutput{All: all, SettledFailed: settledErr != nil, SelectError: selectErr != nil, SelectOK: okValue, SelectWithCancel: selectedCancel != nil, SelectOKCancel: selectedOKCancel != nil}, nil
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), 2*time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, collectionInput{})
	if err != nil {
		t.Fatal(err)
	}
	output, err := run.Await(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(output.All, []int{1, 2}) || !output.SettledFailed || !output.SelectError || output.SelectOK != 5 || !output.SelectWithCancel || !output.SelectOKCancel {
		t.Fatalf("collection output = %#v", output)
	}
	for _, queued := range world.Queued() {
		if queued.NotBefore != nil {
			t.Fatalf("SelectWithCancel left timer queued until %v", *queued.NotBefore)
		}
	}
}

func TestRunCancelMakesPendingRunsTerminalAndIsIdempotent(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, _ wakeInput) (wakeOutput, error) {
		_, err := workflow.Timer(ctx, time.Hour).Await(ctx)
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
	if _, err := run.Await(ctx); !errors.Is(err, workflow.ErrRunCancelled) {
		t.Fatalf("cancelled run error = %v", err)
	}
	if status, err := run.Status(ctx); err != nil || status != workflow.StatusCancelled {
		t.Fatalf("cancelled status = %q, %v", status, err)
	}
}

func TestRunWakeUpFiltersWaitCorrelationIDs(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, _ wakeInput) (wakeOutput, error) {
		_, err := workflow.Join(ctx, workflow.Timer(ctx, time.Hour), workflow.Timer(ctx, time.Hour))
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
	if done, err := run.Done(ctx); err != nil || done {
		t.Fatalf("run done after one wake = %t, %v", done, err)
	}
	if count, err := run.WakeUp(ctx); err != nil || count != 1 {
		t.Fatalf("remaining wake count = %d, %v", count, err)
	}
	if _, err := run.Await(ctx); err != nil {
		t.Fatal(err)
	}
}
