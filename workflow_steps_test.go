package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
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
type rollbackInput struct{}
type rollbackOutput struct{}
type nowInput struct{}
type nowOutput struct{ Value time.Time }
type retryInput struct{}
type retryOutput struct{}
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

func (joinedInput) Workflow(joinedOutput)         {}
func (rollbackInput) Workflow(rollbackOutput)     {}
func (nowInput) Workflow(nowOutput)               {}
func (retryInput) Workflow(retryOutput)           {}
func (collectionInput) Workflow(collectionOutput) {}

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

type inlineStepInput struct{}
type inlineStepOutput struct{ First, Replayed int64 }

func (inlineStepInput) Workflow(inlineStepOutput) {}

var inlineCounter atomic.Int64

func nextInlineValue(context.Context) int64 { return inlineCounter.Add(1) }

// flakyEventWorld fails EventsCreate for events of eventType with err for its
// first failures calls.
type flakyEventWorld struct {
	*testworld.World
	eventType worlds.EventType
	failures  atomic.Int32
	err       error
}

func (w *flakyEventWorld) EventsCreate(ctx context.Context, input worlds.EventsCreateInput) (worlds.EventResult, error) {
	if input.Event.EventType == w.eventType && w.failures.Add(-1) >= 0 {
		return worlds.EventResult{}, w.err
	}
	return w.World.EventsCreate(ctx, input)
}

func inlineStepRunner() worlds.Runner {
	return workflow.Use(func(ctx context.Context, _ inlineStepInput) (inlineStepOutput, error) {
		first, err := workflow.InlineStep(ctx, nextInlineValue)
		if err != nil {
			return inlineStepOutput{}, err
		}
		// A timer makes the runner replay, and the replay must return the
		// recorded value instead of calling nextInlineValue again.
		if err := workflow.Sleep(ctx, time.Millisecond); err != nil {
			return inlineStepOutput{}, err
		}
		return inlineStepOutput{First: first, Replayed: first}, nil
	})
}

func TestInlineStepRecordsItsResultOnce(t *testing.T) {
	world := testworld.New(inlineStepRunner())
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), 5*time.Second)
	defer cancel()
	before := inlineCounter.Load()
	run, err := workflow.Start(ctx, inlineStepInput{})
	if err != nil {
		t.Fatal(err)
	}
	output, err := run.Await(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if calls := inlineCounter.Load() - before; calls != 1 || output.First != before+1 {
		t.Fatalf("fn calls = %d, output = %#v", calls, output)
	}
	events, err := world.EventsList(ctx, worlds.EventsListInput{RunID: worlds.RunID(run.ID())})
	if err != nil {
		t.Fatal(err)
	}
	for index, event := range events.Data {
		if event.EventType == worlds.EventTypeStepCreated {
			if next := events.Data[index+1]; next.EventType != worlds.EventTypeStepCompleted || next.CorrelationID != event.CorrelationID {
				t.Fatalf("step_created is not followed by its step_completed: %#v", events.Data)
			}
			return
		}
	}
	t.Fatal("no inline step recorded")
}

func TestInlineStepStorageErrorRetriesDeliveryAndCompletesStep(t *testing.T) {
	runner := inlineStepRunner()
	storageErr := errors.New("storage unavailable")
	world := &flakyEventWorld{World: testworld.NewManual(runner), eventType: worlds.EventTypeStepCompleted, err: storageErr}
	world.failures.Store(1)
	defer world.Close()
	ctx := workflow.NewContext(context.Background(), workflow.NewClient(world))
	run, err := workflow.Start(ctx, inlineStepInput{})
	if err != nil {
		t.Fatal(err)
	}
	delivery := world.QueuedMessages()[0].Input
	runtime := worlds.NewRuntime(world, worlds.Runners(runner))
	// step_created is recorded, then recording step_completed fails, so the
	// delivery returns the storage error instead of failing the run.
	if err := runtime.Execute(ctx, delivery); !errors.Is(err, storageErr) {
		t.Fatalf("first delivery error = %v, want the storage error", err)
	}
	if status, err := run.Status(ctx); err != nil || status != workflow.StatusRunning {
		t.Fatalf("status after storage error = %q err=%v", status, err)
	}
	// The retried delivery finds step_created without a result and completes it.
	if err := runtime.Execute(ctx, delivery); err != nil {
		t.Fatalf("retried delivery: %v", err)
	}
	events, err := world.EventsList(ctx, worlds.EventsListInput{RunID: worlds.RunID(run.ID())})
	if err != nil {
		t.Fatal(err)
	}
	created, completed := 0, 0
	for _, event := range events.Data {
		switch event.EventType {
		case worlds.EventTypeStepCreated:
			created++
		case worlds.EventTypeStepCompleted:
			completed++
		}
	}
	if created != 1 || completed != 1 {
		t.Fatalf("step_created = %d, step_completed = %d, want 1 each", created, completed)
	}
}

func TestInlineStepStorageErrorBeforeStepCreatedRecordsNothing(t *testing.T) {
	runner := inlineStepRunner()
	storageErr := errors.New("storage unavailable")
	world := &flakyEventWorld{World: testworld.NewManual(runner), eventType: worlds.EventTypeStepCreated, err: storageErr}
	world.failures.Store(1)
	defer world.Close()
	ctx := workflow.NewContext(context.Background(), workflow.NewClient(world))
	run, err := workflow.Start(ctx, inlineStepInput{})
	if err != nil {
		t.Fatal(err)
	}
	if err := worlds.NewRuntime(world, worlds.Runners(runner)).Execute(ctx, world.QueuedMessages()[0].Input); !errors.Is(err, storageErr) {
		t.Fatalf("delivery error = %v, want the storage error", err)
	}
	events, err := world.EventsList(ctx, worlds.EventsListInput{RunID: worlds.RunID(run.ID())})
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events.Data {
		if event.EventType == worlds.EventTypeStepCreated || event.EventType == worlds.EventTypeRunFailed {
			t.Fatalf("unexpected %s after a failed step_created: %#v", event.EventType, events.Data)
		}
	}
}
