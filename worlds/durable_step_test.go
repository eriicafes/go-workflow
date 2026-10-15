package worlds_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/testworld"
)

type durableStepInput struct{ Value int }
type durableStepOutput struct{ Value int }
type fatalStepInput struct{}
type fatalStepOutput struct{}
type inlineStepInput struct{}
type inlineStepOutput struct{ Captured time.Time }
type panicStepInput struct{}
type panicStepOutput struct{}

func (durableStepInput) Workflow(durableStepOutput) {}
func (fatalStepInput) Workflow(fatalStepOutput)     {}
func (inlineStepInput) Workflow(inlineStepOutput)   {}
func (panicStepInput) Workflow(panicStepOutput)     {}

func TestDurableStepRecordsRetryAndCompletionEvents(t *testing.T) {
	var attempts atomic.Int32
	world := testworld.New(workflow.Use(func(ctx context.Context, input durableStepInput) (durableStepOutput, error) {
		value, err := workflow.Step(ctx, func(_ context.Context, value int) (int, error) {
			if attempts.Add(1) == 1 {
				return 0, errors.New("retry")
			}
			return value + 1, nil
		}, input.Value, workflow.StepOptions{MaxRetries: 1})
		return durableStepOutput{Value: value}, err
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, durableStepInput{Value: 41})
	if err != nil {
		t.Fatal(err)
	}
	output, err := run.Await(ctx)
	if err != nil || output.Value != 42 || attempts.Load() != 2 {
		t.Fatalf("output = %#v, attempts=%d, err=%v", output, attempts.Load(), err)
	}
	events, err := world.EventsList(ctx, worlds.EventsListInput{RunID: worlds.RunID(run.ID())})
	if err != nil {
		t.Fatal(err)
	}
	seen := map[worlds.EventType]bool{}
	for _, event := range events.Data {
		seen[event.EventType] = true
	}
	for _, eventType := range []worlds.EventType{worlds.EventTypeStepCreated, worlds.EventTypeStepStarted, worlds.EventTypeStepRetrying, worlds.EventTypeStepCompleted} {
		if !seen[eventType] {
			t.Fatalf("missing %s event in %#v", eventType, events.Data)
		}
	}
}

func TestFatalStepFailureDoesNotScheduleARetry(t *testing.T) {
	var attempts atomic.Int32
	world := testworld.New(workflow.Use(func(ctx context.Context, _ fatalStepInput) (fatalStepOutput, error) {
		_, err := workflow.Step(ctx, func(context.Context, struct{}) (struct{}, error) {
			attempts.Add(1)
			return struct{}{}, workflow.FatalError(errors.New("fatal"))
		}, struct{}{}, workflow.StepOptions{MaxRetries: 3})
		return fatalStepOutput{}, err
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, fatalStepInput{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Await(ctx); err == nil || attempts.Load() != 1 {
		t.Fatalf("fatal step error=%v attempts=%d", err, attempts.Load())
	}
	events, err := world.EventsList(ctx, worlds.EventsListInput{RunID: worlds.RunID(run.ID())})
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events.Data {
		if event.EventType == worlds.EventTypeStepRetrying {
			t.Fatal("fatal step was retried")
		}
	}
}

func TestDurableInlineStepPersistsNowWithoutTargetedExecution(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, _ inlineStepInput) (inlineStepOutput, error) {
		captured, err := workflow.Now(ctx)
		return inlineStepOutput{Captured: captured}, err
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, inlineStepInput{})
	if err != nil {
		t.Fatal(err)
	}
	output, err := run.Await(ctx)
	if err != nil || output.Captured.IsZero() {
		t.Fatalf("inline output = %#v, %v", output, err)
	}
	events, err := world.EventsList(ctx, worlds.EventsListInput{RunID: worlds.RunID(run.ID())})
	if err != nil {
		t.Fatal(err)
	}
	seenCreated, seenCompleted := false, false
	for _, event := range events.Data {
		switch event.EventType {
		case worlds.EventTypeStepCreated:
			seenCreated = true
		case worlds.EventTypeStepCompleted:
			seenCompleted = true
		case worlds.EventTypeStepStarted:
			t.Fatal("inline step was sent to targeted execution")
		}
	}
	if !seenCreated || !seenCompleted {
		t.Fatalf("inline events = %#v", events.Data)
	}
}

func TestTargetedStepErrorPanicIsRecordedAsStepFailure(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, _ panicStepInput) (panicStepOutput, error) {
		_, err := workflow.Step(ctx, func(context.Context, struct{}) (struct{}, error) {
			panic(errors.New("step panic"))
		}, struct{}{}, workflow.StepOptions{MaxRetries: 0})
		return panicStepOutput{}, err
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, panicStepInput{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Await(ctx); err == nil {
		t.Fatal("step panic did not fail the run")
	}
	events, err := world.EventsList(ctx, worlds.EventsListInput{RunID: worlds.RunID(run.ID())})
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events.Data {
		if event.EventType == worlds.EventTypeStepFailed {
			return
		}
	}
	t.Fatal("missing step_failed event")
}

type redeliveredStepInput struct{}
type redeliveredStepOutput struct{}

func (redeliveredStepInput) Workflow(redeliveredStepOutput) {}

func TestRedeliveredStepRunsAsANewAttempt(t *testing.T) {
	var executions atomic.Int32
	runner := workflow.Use(func(ctx context.Context, _ redeliveredStepInput) (redeliveredStepOutput, error) {
		_, err := workflow.Step(ctx, func(context.Context, struct{}) (struct{}, error) {
			executions.Add(1)
			return struct{}{}, nil
		}, struct{}{})
		return redeliveredStepOutput{}, err
	})
	world := testworld.NewManual(runner)
	defer world.Close()
	ctx := workflow.NewContext(context.Background(), workflow.NewClient(world))
	run, err := workflow.Start(ctx, redeliveredStepInput{})
	if err != nil {
		t.Fatal(err)
	}
	runtime := worlds.NewRuntime(world, worlds.Runners(runner))
	if err := runtime.Execute(ctx, world.QueuedMessages()[0].Input); err != nil {
		t.Fatal(err)
	}
	var delivery worlds.QueueInput
	for _, message := range world.QueuedMessages() {
		if message.Input.StepID != nil {
			delivery = message.Input
		}
	}
	if delivery.StepID == nil {
		t.Fatal("no step delivery queued")
	}
	// A delivery started the step and ended before recording its result, so
	// the queue redelivers the step's message.
	if _, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: worlds.RunID(run.ID()), Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeStepStarted, CorrelationID: string(*delivery.StepID)}}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Execute(ctx, delivery); err != nil {
		t.Fatal(err)
	}
	step, err := world.StepsGet(ctx, worlds.StepsGetInput{RunID: worlds.RunID(run.ID()), StepID: *delivery.StepID})
	if err != nil || executions.Load() != 1 || step.Status != worlds.StatusCompleted || step.Attempt != 2 {
		t.Fatalf("step = %#v executions=%d err=%v", step, executions.Load(), err)
	}
}
