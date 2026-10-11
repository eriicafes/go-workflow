package workflowtest_test

import (
	"context"
	"errors"
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

func timeoutWorkflow(ctx context.Context, input sleepInput) (sleepOutput, error) {
	if err := workflow.Timeout(ctx, input.Delay).Wait(ctx); err != nil {
		return sleepOutput{}, err
	}
	return sleepOutput{Completed: true}, nil
}

func delayedWorkflowStep(ctx context.Context, input sleepInput) (sleepOutput, error) {
	select {
	case <-time.After(input.Delay):
		return sleepOutput{Completed: true}, nil
	case <-ctx.Done():
		return sleepOutput{}, ctx.Err()
	}
}

func selectWithCancelWorkflow(ctx context.Context, _ sleepInput) (sleepOutput, error) {
	fast := workflow.Background(ctx, delayedWorkflowStep, sleepInput{Delay: time.Millisecond})
	slow := workflow.Background(ctx, delayedWorkflowStep, sleepInput{Delay: time.Second})
	_, err := workflow.SelectWithCancel(ctx, fast, slow)
	return sleepOutput{Completed: err == nil}, err
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

func TestCancelRunIsIdempotent(t *testing.T) {
	world := workflowtest.New(workflow.Use(sleepWorkflow))
	defer world.Close()

	ctx := workflow.NewContext(context.Background(), world)
	run, err := workflow.Start(ctx, sleepInput{Delay: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if err := workflow.Cancel(ctx, run); err != nil {
		t.Fatal(err)
	}
	if err := workflow.Cancel(ctx, run); err != nil {
		t.Fatal(err)
	}
	if err := run.Wait(context.Background()); !errors.Is(err, workflow.ErrCancelled) {
		t.Fatalf("Wait error = %v, want ErrCancelled", err)
	}
}

func TestTimeoutCancelsItsRun(t *testing.T) {
	world := workflowtest.New(workflow.Use(timeoutWorkflow))
	defer world.Close()

	run, err := workflow.Start(
		workflow.NewContext(context.Background(), world),
		sleepInput{Delay: 10 * time.Millisecond},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := run.Wait(context.Background()); !errors.Is(err, workflow.ErrCancelled) {
		t.Fatalf("Wait error = %v, want ErrCancelled", err)
	}
}

func TestSelectWithCancelCompletesAfterCancellingLosers(t *testing.T) {
	world := workflowtest.New(workflow.Use(selectWithCancelWorkflow))
	defer world.Close()

	run, err := workflow.Start(
		workflow.NewContext(context.Background(), world),
		sleepInput{},
	)
	if err != nil {
		t.Fatal(err)
	}
	output, err := run.Value(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !output.Completed {
		t.Fatal("select workflow did not complete")
	}
}
