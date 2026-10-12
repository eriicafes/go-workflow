package localworld_test

import (
	"context"
	"errors"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow"
	localworld "github.com/eriicafes/go-workflow/worlds/local"
)

type childInput struct {
	Value int
}

type childOutput struct {
	Value int
}

func (childInput) Workflow(childOutput) {}

type parentInput struct {
	Value int
}

type parentOutput struct {
	Value int
}

func (parentInput) Workflow(parentOutput) {}

func child(ctx context.Context, input childInput) (childOutput, error) {
	value, err := workflow.Step(ctx, double, input.Value)
	return childOutput{Value: value}, err
}

func parent(ctx context.Context, input parentInput) (parentOutput, error) {
	run, err := workflow.Start(ctx, childInput{Value: input.Value})
	if err != nil {
		return parentOutput{}, err
	}
	value, err := run.Value(ctx)
	return parentOutput{Value: value.Value}, err
}

func double(_ context.Context, value int) (int, error) {
	return value * 2, nil
}

func TestWorldRunsStepsAndNestedWorkflows(t *testing.T) {
	runners := workflow.Runners(workflow.Use(child), workflow.Use(parent))
	world, err := localworld.Open(t.TempDir(), runners)
	if err != nil {
		t.Fatal(err)
	}
	defer world.Close()
	if err := world.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	run, err := workflow.Start(
		workflow.NewContext(context.Background(), world),
		parentInput{Value: 21},
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	output, err := run.Value(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if output.Value != 42 {
		t.Fatalf("output = %#v, want 42", output)
	}
}

type hookInput struct {
	Token string
}

type hookOutput struct {
	Value string
}

func (hookInput) Workflow(hookOutput) {}

func hookWorkflow(ctx context.Context, input hookInput) (hookOutput, error) {
	hook := workflow.NewHook[string](workflow.HookOption{Token: input.Token})
	value, err := hook.Value(ctx)
	return hookOutput{Value: value}, err
}

func TestWorldResumesHook(t *testing.T) {
	world, err := localworld.Open(t.TempDir(), workflow.Runners(workflow.Use(hookWorkflow)))
	if err != nil {
		t.Fatal(err)
	}
	defer world.Close()
	if err := world.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx := workflow.NewContext(context.Background(), world)
	run, err := workflow.Start(ctx, hookInput{Token: "local-hook"})
	if err != nil {
		t.Fatal(err)
	}
	if err := workflow.ResumeHookWithRetries(ctx, "local-hook", "resumed", 20); err != nil {
		t.Fatal(err)
	}
	output, err := run.Value(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if output.Value != "resumed" {
		t.Fatalf("value = %q, want resumed", output.Value)
	}
}

type delayedInput struct {
	Value    int
	Delay    time.Duration
	Fail     bool
	Terminal bool
	Failure  string
}

func delayed(_ context.Context, input delayedInput) (int, error) {
	time.Sleep(input.Delay)
	if input.Fail {
		cause := errors.New("failed step")
		if input.Failure != "" {
			cause = errors.New(input.Failure)
		}
		if input.Terminal {
			return 0, workflow.FatalError(cause)
		}
		return 0, cause
	}
	return input.Value, nil
}

type mapInput struct{}
type mapOutput struct{ Values []int }

func (mapInput) Workflow(mapOutput) {}

func mapped(ctx context.Context, _ mapInput) (mapOutput, error) {
	results := workflow.Map(ctx, delayed, []delayedInput{
		{Value: 1, Delay: 20 * time.Millisecond},
		{Value: 2, Delay: 10 * time.Millisecond},
	})
	if err := workflow.Join(ctx, workflow.Tasks(results)...); err != nil {
		return mapOutput{}, err
	}
	values := make([]int, len(results))
	for index, result := range results {
		value, err := result.Value(ctx)
		if err != nil {
			return mapOutput{}, err
		}
		values[index] = value
	}
	return mapOutput{Values: values}, nil
}

type allInput struct{}
type allOutput struct {
	Results int
	Failed  bool
	Error   string
}

func (allInput) Workflow(allOutput) {}

func allFailure(ctx context.Context, _ allInput) (allOutput, error) {
	results, err := workflow.All(ctx, delayed, []delayedInput{
		{Value: 1, Delay: 250 * time.Millisecond},
		{Delay: 20 * time.Millisecond, Fail: true, Failure: "slow failure"},
		{Delay: 10 * time.Millisecond, Fail: true, Terminal: true, Failure: "fast failure"},
	})
	message := ""
	if err != nil {
		message = err.Error()
	}
	return allOutput{Results: len(results), Failed: err != nil, Error: message}, nil
}

type settledInput struct{}
type settledOutput struct{ Completed bool }

func (settledInput) Workflow(settledOutput) {}

func settled(ctx context.Context, _ settledInput) (settledOutput, error) {
	results := workflow.Map(ctx, delayed, []delayedInput{
		{Value: 1, Delay: 30 * time.Millisecond},
		{Delay: 10 * time.Millisecond, Fail: true},
	})
	if err := workflow.JoinSettled(ctx, workflow.Tasks(results)...); err != nil {
		return settledOutput{}, err
	}
	return settledOutput{Completed: true}, nil
}

type selectInput struct{}
type selectOutput struct{ Winner int }

func (selectInput) Workflow(selectOutput) {}

func selectd(ctx context.Context, _ selectInput) (selectOutput, error) {
	results := workflow.Map(ctx, delayed, []delayedInput{
		{Value: 1, Delay: 30 * time.Millisecond},
		{Value: 2, Delay: 10 * time.Millisecond},
	})
	winner, err := workflow.Select(ctx, workflow.Tasks(results)...)
	if err != nil {
		return selectOutput{}, err
	}
	result, ok := winner.(workflow.Result[int])
	if !ok {
		return selectOutput{}, errors.New("workflow: unexpected select winner")
	}
	value, err := result.Value(ctx)
	return selectOutput{Winner: value}, err
}

type failedSelectInput struct{}
type failedSelectOutput struct {
	SelectError string
	WaitError   string
}

func (failedSelectInput) Workflow(failedSelectOutput) {}

func failedSelect(ctx context.Context, _ failedSelectInput) (failedSelectOutput, error) {
	results := []workflow.Result[int]{
		workflow.Background(ctx, delayed, delayedInput{Value: 1, Delay: 30 * time.Millisecond}, workflow.StepOptions{MaxRetries: 0}),
		workflow.Background(ctx, delayed, delayedInput{Delay: 10 * time.Millisecond, Fail: true, Failure: "select failure"}, workflow.StepOptions{MaxRetries: 0}),
	}
	winner, selectErr := workflow.Select(ctx, workflow.Tasks(results)...)
	waitErr := winner.Wait(ctx)
	return failedSelectOutput{
		SelectError: errorMessage(selectErr),
		WaitError:   errorMessage(waitErr),
	}, nil
}

func TestWorldComposesMappedResults(t *testing.T) {
	runners := workflow.Runners(
		workflow.Use(mapped),
		workflow.Use(allFailure),
		workflow.Use(settled),
		workflow.Use(selectd),
		workflow.Use(failedSelect),
	)
	world, err := localworld.Open(t.TempDir(), runners)
	if err != nil {
		t.Fatal(err)
	}
	defer world.Close()
	if err := world.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	mappedRun, err := workflow.Start(workflow.NewContext(ctx, world), mapInput{})
	if err != nil {
		t.Fatal(err)
	}
	mappedOutput, err := mappedRun.Value(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := mappedOutput.Values, []int{1, 2}; !equalInts(got, want) {
		t.Fatalf("mapped values = %v, want %v", got, want)
	}

	started := time.Now()
	allRun, err := workflow.Start(workflow.NewContext(ctx, world), allInput{})
	if err != nil {
		t.Fatal(err)
	}
	allOutput, err := allRun.Value(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !allOutput.Failed || allOutput.Results != 0 || allOutput.Error != "fast failure" {
		t.Fatalf("All output = %#v", allOutput)
	}
	// The file-backed world fsyncs each state transition, so allow local
	// filesystem latency while still proving it did not wait for the 250ms step.
	if elapsed := time.Since(started); elapsed >= 230*time.Millisecond {
		t.Fatalf("All waited %s for unfinished mapped work", elapsed)
	}

	started = time.Now()
	settledRun, err := workflow.Start(workflow.NewContext(ctx, world), settledInput{})
	if err != nil {
		t.Fatal(err)
	}
	settledOutput, err := settledRun.Value(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !settledOutput.Completed || time.Since(started) < 25*time.Millisecond {
		t.Fatalf("JoinSettled output = %#v", settledOutput)
	}

	selectRun, err := workflow.Start(workflow.NewContext(ctx, world), selectInput{})
	if err != nil {
		t.Fatal(err)
	}
	selectOutput, err := selectRun.Value(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if selectOutput.Winner != 2 {
		t.Fatalf("Select winner = %d, want 2", selectOutput.Winner)
	}

	failedSelectRun, err := workflow.Start(workflow.NewContext(ctx, world), failedSelectInput{})
	if err != nil {
		t.Fatal(err)
	}
	failedSelectOutput, err := failedSelectRun.Value(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if failedSelectOutput.SelectError != "select failure" || failedSelectOutput.WaitError != "select failure" {
		t.Fatalf("failed Select output = %#v", failedSelectOutput)
	}
}

type rollbackInput struct{}
type rollbackOutput struct{}

func (rollbackInput) Workflow(rollbackOutput) {}

func TestWorldRollsBackInReverseRegistrationOrder(t *testing.T) {
	var (
		mu    sync.Mutex
		order []string
	)
	step := func(context.Context, string) (struct{}, error) { return struct{}{}, nil }
	rollback := func(_ context.Context, value string) error {
		mu.Lock()
		order = append(order, value)
		mu.Unlock()
		return nil
	}
	flow := func(ctx context.Context, _ rollbackInput) (rollbackOutput, error) {
		if _, err := workflow.Step(ctx, step, "first"); err != nil {
			return rollbackOutput{}, err
		}
		if err := workflow.OnRollback(ctx, rollback, "first"); err != nil {
			return rollbackOutput{}, err
		}
		if _, err := workflow.Step(ctx, step, "second"); err != nil {
			return rollbackOutput{}, err
		}
		if err := workflow.OnRollback(ctx, rollback, "second"); err != nil {
			return rollbackOutput{}, err
		}
		return rollbackOutput{}, errors.New("failed")
	}
	runners := workflow.Runners(workflow.Use(flow))
	world, err := localworld.Open(t.TempDir(), runners)
	if err != nil {
		t.Fatal(err)
	}
	defer world.Close()
	if err := world.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	run, err := workflow.Start(workflow.NewContext(context.Background(), world), rollbackInput{})
	if err != nil {
		t.Fatal(err)
	}
	if err := run.Wait(context.Background()); err == nil || err.Error() != "failed" {
		t.Fatalf("run error = %v, want failed", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if got, want := order, []string{"second", "first"}; !slices.Equal(got, want) {
		t.Fatalf("rollback order = %v, want %v", got, want)
	}
}

func equalInts(left, right []int) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func errorMessage(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func TestWorldPersistsCompletedWorkAndLocksStorage(t *testing.T) {
	directory := t.TempDir()
	var calls atomic.Int32
	runner := workflow.Use(func(ctx context.Context, input childInput) (childOutput, error) {
		calls.Add(1)
		return child(ctx, input)
	})

	runners := workflow.Runners(runner)
	world, err := localworld.Open(directory, runners)
	if err != nil {
		t.Fatal(err)
	}
	if err := world.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	other, err := localworld.Open(directory, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := other.Start(context.Background()); !errors.Is(err, localworld.ErrInUse) {
		t.Fatalf("second Start error = %v, want ErrInUse", err)
	}
	_ = other.Close()

	run, err := workflow.Start(workflow.NewContext(context.Background(), world), childInput{Value: 5})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Value(context.Background()); err != nil {
		t.Fatal(err)
	}
	id := run.ID()
	completedCalls := calls.Load()
	if completedCalls == 0 {
		t.Fatal("workflow did not execute")
	}
	if err := world.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := localworld.Open(directory, runners)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := reopened.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.WaitRun(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != completedCalls {
		t.Fatalf("completed workflow executed again after reopening: before=%d after=%d", completedCalls, calls.Load())
	}
}

func TestWorldRejectsCorruptVersion(t *testing.T) {
	directory := t.TempDir()
	if err := os.WriteFile(directory+"/version.json", []byte("not json"), 0600); err != nil {
		t.Fatal(err)
	}
	world, err := localworld.Open(directory, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer world.Close()
	if err := world.Start(context.Background()); err == nil {
		t.Fatal("Start accepted corrupt version record")
	}
}
