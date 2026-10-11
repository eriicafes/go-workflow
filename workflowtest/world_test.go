package workflowtest_test

import (
	"context"
	"errors"
	"sync/atomic"
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

type retryInput struct{}
type retryOutput struct {
	Attempts int
	Metadata workflow.StepMetadata
}

func (retryInput) Workflow(retryOutput) {}

var retryCalls atomic.Int32

func retryWorkflow(ctx context.Context, _ retryInput) (retryOutput, error) {
	metadata := workflow.GetStepMetadata(ctx)
	value, err := workflow.Step(ctx, retryStep, struct{}{})
	return retryOutput{Attempts: value, Metadata: metadata}, err
}

func retryStep(ctx context.Context, _ struct{}) (int, error) {
	metadata := workflow.GetStepMetadata(ctx)
	attempt := int(retryCalls.Add(1))
	if metadata.ID == "" || metadata.Name == "" || metadata.Attempt != attempt {
		return 0, errors.New("unexpected step metadata")
	}
	if attempt < 4 {
		return 0, errors.New("try again")
	}
	return attempt, nil
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

func TestStepRetriesAndMetadata(t *testing.T) {
	retryCalls.Store(0)
	world := workflowtest.New(workflow.Use(retryWorkflow))
	defer world.Close()

	run, err := workflow.Start(workflow.NewContext(context.Background(), world), retryInput{})
	if err != nil {
		t.Fatal(err)
	}
	output, err := run.Value(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if output.Attempts != 4 {
		t.Fatalf("attempts = %d, want 4", output.Attempts)
	}
}

type nowInput struct{}
type nowOutput struct {
	Value time.Time
}

func (nowInput) Workflow(nowOutput) {}

func nowWorkflow(ctx context.Context, _ nowInput) (nowOutput, error) {
	value, err := workflow.Now(ctx)
	return nowOutput{Value: value}, err
}

func TestNowRecordsTimeAsDurableValue(t *testing.T) {
	world := workflowtest.New(workflow.Use(nowWorkflow))
	defer world.Close()

	run, err := workflow.Start(workflow.NewContext(context.Background(), world), nowInput{})
	if err != nil {
		t.Fatal(err)
	}
	output, err := run.Value(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if output.Value.IsZero() {
		t.Fatal("workflow.Now returned zero time")
	}
}

type nowThenStepInput struct {
	Value int
}

type nowThenStepOutput struct {
	Now   time.Time
	Value int
}

func (nowThenStepInput) Workflow(nowThenStepOutput) {}

func nowThenStepWorkflow(ctx context.Context, input nowThenStepInput) (nowThenStepOutput, error) {
	value, err := workflow.Now(ctx)
	if err != nil {
		return nowThenStepOutput{}, err
	}
	doubled, err := workflow.Step(ctx, double, input.Value)
	return nowThenStepOutput{Now: value, Value: doubled}, err
}

func TestNowReplaysBeforeATargetedStep(t *testing.T) {
	world := workflowtest.New(workflow.Use(nowThenStepWorkflow))
	defer world.Close()

	run, err := workflow.Start(
		workflow.NewContext(context.Background(), world),
		nowThenStepInput{Value: 21},
	)
	if err != nil {
		t.Fatal(err)
	}
	output, err := run.Value(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if output.Now.IsZero() || output.Value != 42 {
		t.Fatalf("output = %#v, want a time and 42", output)
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
