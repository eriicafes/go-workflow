package core_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/testworld"
)

type inlineStepInput struct{}
type inlineStepOutput struct{ Captured time.Time }

func (inlineStepInput) Workflow(inlineStepOutput) {}

func wallClock(context.Context, struct{}) (time.Time, error) { return time.Now(), nil }

func TestDurableInlineStepRecordsItsResultInOneAttempt(t *testing.T) {
	world := testworld.New(worlds.Runners(workflow.Use(func(ctx context.Context, _ inlineStepInput) (inlineStepOutput, error) {
		captured, err := workflow.InlineStep(ctx, wallClock, struct{}{})
		return inlineStepOutput{Captured: captured}, err
	})))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, inlineStepInput{})
	if err != nil {
		t.Fatal(err)
	}
	output, err := run.Value(ctx)
	if err != nil || output.Captured.IsZero() {
		t.Fatalf("inline output = %#v, %v", output, err)
	}
	events, err := world.EventsList(ctx, worlds.EventsListInput{RunID: worlds.RunID(run.ID())})
	if err != nil {
		t.Fatal(err)
	}
	counts := map[worlds.EventType]int{}
	for _, event := range events.Data {
		counts[event.EventType]++
	}
	// An inline step's first step_started creates it.
	if counts[worlds.EventTypeStepCreated] != 0 || counts[worlds.EventTypeStepStarted] != 1 || counts[worlds.EventTypeStepCompleted] != 1 {
		t.Fatalf("inline events = %#v", events.Data)
	}
}

func TestFlowRunsAgainAnInlineAttemptAStoppedDeliveryLeft(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var attempts atomic.Int32
	started := make(chan struct{})
	runner := workflow.Use(func(ctx context.Context, _ inlineInput) (inlineOutput, error) {
		value, err := workflow.InlineStep(ctx, func(ctx context.Context, value int) (int, error) {
			if attempts.Add(1) == 1 {
				close(started)
				<-ctx.Done()
				return 0, ctx.Err()
			}
			return value, nil
		}, 7, workflow.StepOptions{MaxRetries: 1})
		return inlineOutput{Values: []int{value}}, err
	})
	world, runtime, runID, delivery := startInline(t, ctx, runner, inlineInput{})
	crash(t, ctx, runtime, delivery.Input, started)

	// Any flow that finds the attempt without a result runs the next one,
	// since the fence keeps a second flow from writing alongside it.
	other := delivery.Input
	other.MessageID, other.RunInput = "other", nil
	if err := runtime.Execute(ctx, other); err != nil {
		t.Fatal(err)
	}
	output, err := runOutput(t, ctx, world, runID)
	if err != nil || output.Values[0] != 7 || attempts.Load() != 2 {
		t.Fatalf("output=%v err=%v attempts=%d", output, err, attempts.Load())
	}
}

type latencyInput struct{}
type latencyOutput struct{}

func (latencyInput) Workflow(latencyOutput) {}

func TestInlineStepsRecordLatencyTelemetryOnTheirFinalEvent(t *testing.T) {
	failing := errors.New("second step failed")
	world := testworld.New(worlds.Runners(workflow.Use(func(ctx context.Context, _ latencyInput) (latencyOutput, error) {
		if _, err := workflow.Step(ctx, wallClock, struct{}{}); err != nil {
			return latencyOutput{}, err
		}
		_, err := workflow.Step(ctx, func(context.Context, struct{}) (struct{}, error) {
			return struct{}{}, workflow.FatalError(failing)
		}, struct{}{})
		if err == nil {
			return latencyOutput{}, errors.New("the second step did not fail")
		}
		// A step that follows a timer is not measured.
		if err := workflow.Sleep(ctx, time.Millisecond); err != nil {
			return latencyOutput{}, err
		}
		_, err = workflow.Step(ctx, wallClock, struct{}{})
		return latencyOutput{}, err
	})))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), 5*time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, latencyInput{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Value(ctx); err != nil {
		t.Fatal(err)
	}
	events, err := world.EventsList(ctx, worlds.EventsListInput{RunID: worlds.RunID(run.ID())})
	if err != nil {
		t.Fatal(err)
	}
	var results []worlds.DataStepResult
	for _, event := range events.Data {
		if event.EventType == worlds.EventTypeStepCompleted || event.EventType == worlds.EventTypeStepFailed {
			var result worlds.DataStepResult
			if err := json.Unmarshal(event.Data, &result); err != nil {
				t.Fatal(err)
			}
			results = append(results, result)
		}
	}
	if len(results) != 3 {
		t.Fatalf("%d step results, want 3", len(results))
	}
	// The run's first step: the time from the run's creation, and from its
	// start, to the step.
	first := results[0]
	if first.TTFS == nil || *first.TTFS < 0 || *first.TTFS > 5000 || first.RSFS == nil || first.FinalSchedulingReplay == nil ||
		first.STSO != nil || first.StepCount != 0 || first.EventCount != 0 || !slices.Contains(first.Optimizations, "turbo") {
		t.Fatalf("first step's telemetry = %s", mustJSON(t, first))
	}
	if !slices.Contains(first.Optimizations, "optimisticStart") && !slices.Contains(first.Optimizations, "preclaimedStart") {
		t.Fatalf("first step's optimizations = %v, want how its start was sent", first.Optimizations)
	}
	// The step right after it: the time since the first step's result.
	second := results[1]
	if second.STSO == nil || *second.STSO < 0 || second.StepCount != 1 || second.EventCount == 0 || second.TTFS != nil || second.RSFS != nil || second.FinalSchedulingReplay == nil {
		t.Fatalf("second step's telemetry = %s", mustJSON(t, second))
	}
	third := results[2]
	if third.TTFS != nil || third.STSO != nil || third.RSFS != nil || third.FinalSchedulingReplay != nil || third.Optimizations != nil {
		t.Fatalf("third step's telemetry = %s, want none", mustJSON(t, third))
	}
}

func mustJSON(t *testing.T, value any) string {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
