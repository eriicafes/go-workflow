package workflowtest_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/workflowtest"
)

type sleepInput struct {
	Delay time.Duration
}

type sleepOutput struct {
	Completed bool
}

func (sleepInput) Workflow(sleepOutput) {}

func sleepWorkflow(ctx context.Context, input sleepInput) (sleepOutput, error) {
	if err := workflow.Sleep(ctx, input.Delay); err != nil {
		return sleepOutput{}, err
	}
	return sleepOutput{Completed: true}, nil
}

type stepInput struct {
	Value int
}

type stepOutput struct {
	Value int
}

func (stepInput) Workflow(stepOutput) {}

func stepWorkflow(ctx context.Context, input stepInput) (stepOutput, error) {
	value, err := workflow.Step(ctx, double, input.Value)
	return stepOutput{Value: value}, err
}

func double(_ context.Context, value int) (int, error) {
	return value * 2, nil
}

func TestWorldExecutesDurableSteps(t *testing.T) {
	world := workflowtest.New(workflow.Use(stepWorkflow))
	defer world.Close()

	run, err := workflow.Start(
		workflow.NewContext(context.Background(), world),
		stepInput{Value: 21},
	)
	if err != nil {
		t.Fatal(err)
	}
	output, err := run.Value(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if output.Value != 42 {
		t.Fatalf("output = %#v, want 42", output)
	}
}

func TestWorldSchedulesTimersWithWallClockTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		world := workflowtest.New(workflow.Use(sleepWorkflow))
		defer world.Close()

		delay := 20 * time.Millisecond
		started := time.Now()
		run, err := workflow.Start(
			workflow.NewContext(context.Background(), world),
			sleepInput{Delay: delay},
		)
		if err != nil {
			t.Fatal(err)
		}

		// workflowtest deliberately uses time.Sleep. synctest advances the
		// standard clock once this test and the worker goroutines are blocked.
		if err := run.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
		if elapsed := time.Since(started); elapsed < delay {
			t.Fatalf("timer completed after %s, want at least %s", elapsed, delay)
		}
	})
}
