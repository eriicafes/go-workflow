package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
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
type stepErrorInput struct{}
type stepErrorOutput struct{}
type collectionInput struct{}

type collectionOutput struct {
	All           []int
	SettledFailed bool
	SelectError   bool
	SelectOK      int
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
func (stepErrorInput) Workflow(stepErrorOutput)   {}
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
	output, err := run.Value(ctx)
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
		first := workflow.Background(ctx, increment, 1)
		second := workflow.Background(ctx, increment, 2)
		if err := workflow.JoinOk(ctx, first, second); err != nil {
			return joinedOutput{}, err
		}
		return joinedOutput{Values: []int{first.Unwrap(ctx), second.Unwrap(ctx)}}, nil
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, joinedInput{})
	if err != nil {
		t.Fatal(err)
	}
	output, err := run.Value(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(output.Values, []int{2, 3}) {
		t.Fatalf("values = %#v", output.Values)
	}
}

func TestTimerUsesDurableWaits(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, _ timerInput) (timerOutput, error) {
		if _, err := workflow.Timer(ctx, time.Millisecond).Value(ctx); err != nil {
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
	if _, err := run.Value(ctx); err != nil {
		t.Fatal(err)
	}

}

func TestJoinAndJoinOkBubbleWorkflowContextErrors(t *testing.T) {
	ctx := context.Background()
	result := workflow.Background(ctx, increment, 1)
	for name, err := range map[string]error{
		"Join":   workflow.Join(ctx, result),
		"JoinOk": workflow.JoinOk(ctx, result),
	} {
		if !errors.Is(err, workflow.ErrNoWorkflow) {
			t.Errorf("%s error=%v", name, err)
		}
	}
	if err := workflow.Join(ctx); err != nil {
		t.Fatalf("empty Join error=%v", err)
	}
	if err := workflow.JoinOk(ctx); err != nil {
		t.Fatalf("empty JoinOk error=%v", err)
	}
}

func TestRollbackRunsInDeferOrder(t *testing.T) {
	var calls []string
	var mutex sync.Mutex
	world := testworld.New(workflow.Use(func(ctx context.Context, _ rollbackInput) (rollbackOutput, error) {
		defer workflow.Rollback(ctx, func(_ context.Context, value string) error {
			mutex.Lock()
			calls = append(calls, value)
			mutex.Unlock()
			return nil
		}, "first")
		defer workflow.Rollback(ctx, func(_ context.Context, value string) error {
			mutex.Lock()
			calls = append(calls, value)
			mutex.Unlock()
			return nil
		}, "second")
		return rollbackOutput{}, errors.New("primary failure")
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, rollbackInput{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Value(ctx); err == nil || err.Error() != "primary failure" {
		t.Fatalf("failure = %v", err)
	}
	mutex.Lock()
	defer mutex.Unlock()
	if !slices.Equal(calls, []string{"second", "first"}) {
		t.Fatalf("rollback calls = %#v", calls)
	}
}

func TestRollbackRunsAfterPriorDurableStep(t *testing.T) {
	var calls atomic.Int32
	world := testworld.New(workflow.Use(func(ctx context.Context, _ rollbackInput) (rollbackOutput, error) {
		if _, err := workflow.Step(ctx, increment, 1); err != nil {
			return rollbackOutput{}, err
		}
		defer workflow.Rollback(ctx, func(context.Context, struct{}) error {
			calls.Add(1)
			return nil
		}, struct{}{})
		return rollbackOutput{}, errors.New("primary failure")
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, rollbackInput{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Value(ctx); err == nil || err.Error() != "primary failure" {
		t.Fatalf("failure = %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("rollback calls = %d, want 1", calls.Load())
	}
}

func TestRollbackDirectCallsRegisterWithoutExecuting(t *testing.T) {
	var calls []string
	var mutex sync.Mutex
	world := testworld.New(workflow.Use(func(ctx context.Context, _ rollbackInput) (rollbackOutput, error) {
		calledEarly := false
		workflow.Rollback(ctx, func(context.Context, struct{}) error {
			calledEarly = true
			mutex.Lock()
			calls = append(calls, "first")
			mutex.Unlock()
			return nil
		}, struct{}{})
		workflow.Rollback(ctx, func(context.Context, struct{}) error {
			calledEarly = true
			mutex.Lock()
			calls = append(calls, "second")
			mutex.Unlock()
			return nil
		}, struct{}{})
		if calledEarly {
			return rollbackOutput{}, errors.New("registration executed compensation")
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
	if _, err := run.Value(ctx); err == nil || err.Error() != "primary failure" {
		t.Fatalf("failure = %v", err)
	}
	mutex.Lock()
	defer mutex.Unlock()
	if !slices.Equal(calls, []string{"first", "second"}) {
		t.Fatalf("rollback calls = %#v", calls)
	}
}

func TestRollbackRunsOnApplicationPanics(t *testing.T) {
	for _, stringPanic := range []bool{false, true} {
		t.Run(fmt.Sprint(stringPanic), func(t *testing.T) {
			var calls atomic.Int32
			failure := errors.New("application panic")
			world := testworld.New(workflow.Use(func(ctx context.Context, _ rollbackInput) (rollbackOutput, error) {
				defer workflow.Rollback(ctx, func(context.Context, struct{}) error {
					calls.Add(1)
					return nil
				}, struct{}{})
				if stringPanic {
					panic("application panic")
				}
				panic(failure)
			}))
			defer world.Close()
			ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
			defer cancel()
			run, err := workflow.Start(ctx, rollbackInput{})
			if err != nil {
				t.Fatal(err)
			}
			_, err = run.Value(ctx)
			if stringPanic {
				if err == nil || err.Error() != "workflow: panic: application panic" {
					t.Fatalf("panic error=%v", err)
				}
			} else if !errors.Is(err, failure) {
				t.Fatalf("panic error=%v", err)
			}
			if calls.Load() == 0 {
				t.Fatal("application panic did not run compensation")
			}
		})
	}
}

func TestHandledStepPanicsDoNotRunRollbacks(t *testing.T) {
	for _, inline := range []bool{false, true} {
		t.Run(fmt.Sprint(inline), func(t *testing.T) {
			var calls atomic.Int32
			world := testworld.New(workflow.Use(func(ctx context.Context, _ rollbackInput) (rollbackOutput, error) {
				defer workflow.Rollback(ctx, func(context.Context, struct{}) error {
					calls.Add(1)
					return nil
				}, struct{}{})
				var err error
				if inline {
					_, err = workflow.InlineStep(ctx, func(context.Context, struct{}) (struct{}, error) {
						panic("step panic")
					}, struct{}{}, workflow.StepOptions{MaxRetries: 0})
				} else {
					_, err = workflow.Step(ctx, func(context.Context, struct{}) (struct{}, error) {
						panic("step panic")
					}, struct{}{}, workflow.StepOptions{MaxRetries: 0})
				}
				if err == nil || err.Error() != "workflow: panic: step panic" {
					return rollbackOutput{}, fmt.Errorf("step panic error=%v", err)
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
			if _, err := run.Value(ctx); err != nil {
				t.Fatal(err)
			}
			if calls.Load() != 0 {
				t.Fatal("handled step panic ran compensation")
			}
			steps, err := world.StepsList(ctx, worlds.StepsListInput{RunID: worlds.RunID(run.ID())})
			if err != nil || len(steps.Data) != 1 || steps.Data[0].Status != worlds.StatusFailed {
				t.Fatalf("panic step=%#v error=%v", steps, err)
			}
		})
	}
}

func TestRollbackPanicsOutsideWorkflowContext(t *testing.T) {
	defer func() {
		err, ok := recover().(error)
		if !ok || !errors.Is(err, workflow.ErrNoWorkflow) {
			t.Fatalf("Rollback panic=%v", err)
		}
	}()
	workflow.Rollback(context.Background(), func(context.Context, struct{}) error { return nil }, struct{}{})
}

func TestRollbackDoesNotRunAfterSuccess(t *testing.T) {
	var calls atomic.Int32
	world := testworld.New(workflow.Use(func(ctx context.Context, _ rollbackInput) (rollbackOutput, error) {
		defer workflow.Rollback(ctx, func(context.Context, struct{}) error {
			calls.Add(1)
			return nil
		}, struct{}{})
		return rollbackOutput{}, nil
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, rollbackInput{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Value(ctx); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 0 {
		t.Fatalf("rollback calls = %d, want 0", calls.Load())
	}
}

func TestRollbackFailureIsRecordedAsStepFailure(t *testing.T) {
	for _, mode := range []string{"return", "error panic", "string panic"} {
		t.Run(mode, func(t *testing.T) {
			wantFailure := "rollback failure"
			if mode == "string panic" {
				wantFailure = "workflow: panic: " + wantFailure
			}
			var continued atomic.Int32
			world := testworld.New(workflow.Use(func(ctx context.Context, _ rollbackInput) (rollbackOutput, error) {
				defer workflow.Rollback(ctx, func(context.Context, struct{}) error {
					continued.Add(1)
					return nil
				}, struct{}{})
				defer workflow.Rollback(ctx, func(context.Context, struct{}) error {
					switch mode {
					case "error panic":
						panic(errors.New("rollback failure"))
					case "string panic":
						panic("rollback failure")
					}
					return errors.New("rollback failure")
				}, struct{}{}, workflow.StepOptions{MaxRetries: 0})
				return rollbackOutput{}, errors.New("primary failure")
			}))
			defer world.Close()
			ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
			defer cancel()
			run, err := workflow.Start(ctx, rollbackInput{})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := run.Value(ctx); err == nil || err.Error() != "primary failure" {
				t.Fatalf("failure = %v", err)
			}
			events, err := world.EventsList(ctx, worlds.EventsListInput{RunID: worlds.RunID(run.ID())})
			if err != nil {
				t.Fatal(err)
			}
			rollbackLogged := false
			stepFailed := false
			for _, event := range events.Data {
				var created worlds.DataStepCreated
				if event.EventType == worlds.EventTypeStepCreated && json.Unmarshal(event.Data, &created) == nil && created.Rollback {
					rollbackLogged = true
				}
				var result worlds.DataStepResult
				if event.EventType == worlds.EventTypeStepFailed && json.Unmarshal(event.Data, &result) == nil && worlds.DecodeError(result.Error).Error() == wantFailure {
					stepFailed = true
				}
			}
			if !rollbackLogged || !stepFailed {
				t.Fatalf("rollback events were not recorded: %#v", events.Data)
			}

			if continued.Load() == 0 {
				t.Fatal("failed rollback prevented the remaining compensation")
			}
		})
	}
}

func TestRollbackRegistersAroundDurableSteps(t *testing.T) {
	var calls []string
	var mutex sync.Mutex
	record := func(value string) {
		mutex.Lock()
		calls = append(calls, value)
		mutex.Unlock()
	}
	world := testworld.New(workflow.Use(func(ctx context.Context, _ rollbackInput) (rollbackOutput, error) {
		defer workflow.Rollback(ctx, func(context.Context, struct{}) error {
			record("before step")
			return nil
		}, struct{}{})
		if _, err := workflow.Step(ctx, func(context.Context, struct{}) (struct{}, error) {
			record("step")
			return struct{}{}, nil
		}, struct{}{}); err != nil {
			return rollbackOutput{}, err
		}
		defer workflow.Rollback(ctx, func(context.Context, struct{}) error {
			record("after step")
			return nil
		}, struct{}{})
		return rollbackOutput{}, errors.New("primary failure")
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, rollbackInput{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Value(ctx); err == nil || err.Error() != "primary failure" {
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
	output, err := run.Value(ctx)
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
	created, started, completed := false, 0, false
	for _, event := range events.Data {
		if event.EventType == worlds.EventTypeStepCreated {
			var data worlds.DataStepCreated
			if err := json.Unmarshal(event.Data, &data); err != nil {
				t.Fatal(err)
			}
			created = data.StepName == "now"
		}
		if event.EventType == worlds.EventTypeStepStarted {
			started++
		}
		if event.EventType == worlds.EventTypeStepCompleted {
			completed = true
		}
	}
	// Now is an inline step: one attempt, run in the flow.
	if !created || started != 1 || !completed {
		t.Fatalf("inline now events created=%t started=%d completed=%t", created, started, completed)
	}
}

func TestCancelledTimerSettlesThroughItsContext(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, _ timerInput) (timerOutput, error) {
		cancelCtx, cancel, err := workflow.WithCancel(ctx)
		if err != nil {
			return timerOutput{}, err
		}
		timer := workflow.Timer(cancelCtx, time.Hour)
		if err := cancel(); err != nil {
			return timerOutput{}, err
		}
		if _, err := timer.Value(ctx); err == nil {
			return timerOutput{}, errors.New("cancelled timer succeeded")
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
	if _, err := run.Value(ctx); err != nil {
		t.Fatal(err)
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
	if _, err := fatalRun.Value(ctx); err == nil || fatalAttempts.Load() != 1 {
		t.Fatalf("fatal run error = %v, attempts = %d", err, fatalAttempts.Load())
	}
	retryRun, err := workflow.Start(ctx, collectionInput{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := retryRun.Value(ctx); err != nil || retryAttempts.Load() != 2 {
		t.Fatalf("retry run error = %v, attempts = %d", err, retryAttempts.Load())
	}
}

var errCardDeclined = errors.New("card declined")

type declinedError struct{ Code string }

func (e *declinedError) Error() string { return "declined: " + e.Code }
func (e *declinedError) Unwrap() error { return errCardDeclined }

func TestStepErrorKeepsIdentityAcrossReplayAndRuns(t *testing.T) {
	var mutex sync.Mutex
	var inWorkflow []string
	world := testworld.New(workflow.Use(func(ctx context.Context, _ stepErrorInput) (stepErrorOutput, error) {
		_, err := workflow.Step(ctx, func(context.Context, struct{}) (struct{}, error) {
			return struct{}{}, workflow.FatalError(fmt.Errorf("charge: %w", &declinedError{Code: "insufficient_funds"}))
		}, struct{}{})
		var declined *declinedError
		mutex.Lock()
		inWorkflow = []string{err.Error(), fmt.Sprint(errors.Is(err, errCardDeclined))}
		if errors.As(err, &declined) {
			inWorkflow = append(inWorkflow, declined.Code)
		}
		mutex.Unlock()
		return stepErrorOutput{}, fmt.Errorf("payment: %w", err)
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, stepErrorInput{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = run.Value(ctx)
	var stepErr *workflow.Error
	var declined *declinedError
	if !errors.As(err, &stepErr) || err.Error() != "payment: charge: declined: insufficient_funds" {
		t.Fatalf("run error = %#v", err)
	}
	if !errors.Is(err, errCardDeclined) || !errors.As(err, &declined) || declined.Code != "insufficient_funds" {
		t.Fatalf("run error does not match its chain: %v", err)
	}
	mutex.Lock()
	defer mutex.Unlock()
	if !slices.Equal(inWorkflow, []string{"charge: declined: insufficient_funds", "true", "insufficient_funds"}) {
		t.Fatalf("step error in workflow = %#v", inWorkflow)
	}
}

func TestWorkflowCollectionHelpers(t *testing.T) {
	// Fake time orders the failed and successful steps by their delays.
	synctest.Test(t, func(t *testing.T) {
		world := testworld.New(workflow.Use(func(ctx context.Context, _ collectionInput) (collectionOutput, error) {
			all, err := workflow.All(ctx, collectionStep, []collectionStepInput{{Value: 1}, {Value: 2}})
			if err != nil {
				return collectionOutput{}, err
			}
			settled := workflow.Map(ctx, collectionStep, []collectionStepInput{{Value: 3}, {Fail: true}})
			if err := workflow.Join(ctx, settled[0], settled[1]); err != nil {
				return collectionOutput{}, err
			}
			_, settledErr := settled[1].Value(ctx)

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
			okValue, err := selectedOK.(*workflow.Result[int]).Value(ctx)
			if err != nil {
				return collectionOutput{}, err
			}

			return collectionOutput{All: all, SettledFailed: settledErr != nil, SelectError: selectErr != nil, SelectOK: okValue}, nil
		}))
		defer world.Close()
		ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), 2*time.Second)
		defer cancel()
		run, err := workflow.Start(ctx, collectionInput{})
		if err != nil {
			t.Fatal(err)
		}
		output, err := run.Value(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(output.All, []int{1, 2}) || !output.SettledFailed || !output.SelectError || output.SelectOK != 5 {
			t.Fatalf("collection output = %#v", output)
		}
	})
}

type inlineStepInput struct{}
type inlineStepOutput struct{ First, Replayed int64 }

func (inlineStepInput) Workflow(inlineStepOutput) {}

var inlineCounter atomic.Int64

func nextInlineValue(context.Context, struct{}) (int64, error) { return inlineCounter.Add(1), nil }

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

func (w *flakyEventWorld) EventsCreateBatch(ctx context.Context, input worlds.EventsCreateBatchInput) (worlds.EventBatchResult, error) {
	for _, event := range input.Events {
		if event.EventType == w.eventType && w.failures.Add(-1) >= 0 {
			return worlds.EventBatchResult{}, w.err
		}
	}
	return w.World.EventsCreateBatch(ctx, input)
}

func inlineStepRunner() worlds.Runner {
	return workflow.Use(func(ctx context.Context, _ inlineStepInput) (inlineStepOutput, error) {
		first, err := workflow.InlineStep(ctx, nextInlineValue, struct{}{})
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
	output, err := run.Value(ctx)
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
			started, completed := events.Data[index+1], events.Data[index+2]
			if started.EventType != worlds.EventTypeStepStarted || completed.EventType != worlds.EventTypeStepCompleted || completed.CorrelationID != event.CorrelationID {
				t.Fatalf("step_created is not followed by step_started and step_completed: %#v", events.Data)
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
	before := inlineCounter.Load()
	// fn runs, then recording step_completed fails, so the delivery returns
	// the storage error instead of failing the run.
	if err := runtime.Execute(ctx, delivery); !errors.Is(err, storageErr) {
		t.Fatalf("first delivery error = %v, want the storage error", err)
	}
	// The retried delivery finds the attempt without a result, starts
	// another, and completes the step.
	if err := runtime.Execute(ctx, delivery); err != nil {
		t.Fatalf("retried delivery: %v", err)
	}
	if calls := inlineCounter.Load() - before; calls != 2 {
		t.Fatalf("fn calls = %d, want 2", calls)
	}
	events, err := world.EventsList(ctx, worlds.EventsListInput{RunID: worlds.RunID(run.ID())})
	if err != nil {
		t.Fatal(err)
	}
	counts := map[worlds.EventType]int{}
	for _, event := range events.Data {
		counts[event.EventType]++
	}
	if counts[worlds.EventTypeStepCreated] != 1 || counts[worlds.EventTypeStepStarted] != 2 || counts[worlds.EventTypeStepCompleted] != 1 {
		t.Fatalf("event counts = %v", counts)
	}
}

type flakyInlineInput struct{ Failures int32 }
type flakyInlineOutput struct{ Attempts int32 }

func (flakyInlineInput) Workflow(flakyInlineOutput) {}

var errFlakyInline = errors.New("flaky inline step")

func TestInlineStepRetriesAFailedAttemptAndThenFails(t *testing.T) {
	var attempts atomic.Int32
	world := testworld.New(workflow.Use(func(ctx context.Context, input flakyInlineInput) (flakyInlineOutput, error) {
		// Retries run with the input recorded by the first attempt.
		_, err := workflow.InlineStep(ctx, func(ctx context.Context, failures int32) (struct{}, error) {
			if attempts.Add(1) <= failures {
				return struct{}{}, errFlakyInline
			}
			return struct{}{}, nil
		}, input.Failures, workflow.StepOptions{MaxRetries: 2})
		return flakyInlineOutput{Attempts: attempts.Load()}, err
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), 5*time.Second)
	defer cancel()

	run, err := workflow.Start(ctx, flakyInlineInput{Failures: 2})
	if err != nil {
		t.Fatal(err)
	}
	output, err := run.Value(ctx)
	if err != nil || output.Attempts != 3 {
		t.Fatalf("output = %#v, err = %v, want success on the third attempt", output, err)
	}

	attempts.Store(0)
	run, err = workflow.Start(ctx, flakyInlineInput{Failures: 3})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Value(ctx); !errors.Is(err, errFlakyInline) {
		t.Fatalf("error = %v, want the step's error once retries run out", err)
	}
	events, err := world.EventsList(ctx, worlds.EventsListInput{RunID: worlds.RunID(run.ID())})
	if err != nil {
		t.Fatal(err)
	}
	counts := map[worlds.EventType]int{}
	for _, event := range events.Data {
		counts[event.EventType]++
	}
	if counts[worlds.EventTypeStepStarted] != 3 || counts[worlds.EventTypeStepRetrying] != 2 || counts[worlds.EventTypeStepFailed] != 1 {
		t.Fatalf("event counts = %v", counts)
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

func TestInlineStepQueuesItsRetryBeforeRecordingIt(t *testing.T) {
	var attempts atomic.Int32
	runner := workflow.Use(func(ctx context.Context, input flakyInlineInput) (flakyInlineOutput, error) {
		_, err := workflow.InlineStep(ctx, func(ctx context.Context, failures int32) (struct{}, error) {
			if attempts.Add(1) <= failures {
				return struct{}{}, errFlakyInline
			}
			return struct{}{}, nil
		}, input.Failures)
		return flakyInlineOutput{Attempts: attempts.Load()}, err
	})
	storageErr := errors.New("storage unavailable")
	world := &flakyEventWorld{World: testworld.NewManual(runner), eventType: worlds.EventTypeStepRetrying, err: storageErr}
	world.failures.Store(1)
	defer world.Close()
	ctx := workflow.NewContext(context.Background(), workflow.NewClient(world))
	run, err := workflow.Start(ctx, flakyInlineInput{Failures: 1})
	if err != nil {
		t.Fatal(err)
	}
	first := world.QueuedMessages()[0]
	runtime := worlds.NewRuntime(world, worlds.Runners(runner))

	// The attempt fails, its retry flow is queued, and then recording
	// step_retrying fails.
	if err := runtime.Execute(ctx, first.Input); !errors.Is(err, storageErr) {
		t.Fatalf("delivery error = %v, want the storage error", err)
	}
	if err := world.QueueCancel(ctx, worlds.QueueCancelInput{MessageID: first.ID}); err != nil {
		t.Fatal(err)
	}
	if queued := world.QueuedMessages(); len(queued) != 1 || queued[0].Input.NotBefore == nil {
		t.Fatalf("queued = %#v, want the retry flow", queued)
	}

	// The retry flow alone finishes the run.
	for len(world.QueuedMessages()) != 0 {
		message := world.QueuedMessages()[0]
		if err := runtime.Execute(ctx, message.Input); err != nil {
			t.Fatal(err)
		}
		if err := world.QueueCancel(ctx, worlds.QueueCancelInput{MessageID: message.ID}); err != nil {
			t.Fatal(err)
		}
	}
	output, err := run.Value(ctx)
	if err != nil || output.Attempts != 2 {
		t.Fatalf("output = %#v, err = %v, want success on the second attempt", output, err)
	}
}

func TestAllCancelsEarlierRunningInputWhenLaterInputFails(t *testing.T) {
	started := make(chan struct{})
	stopped := make(chan struct{})
	failure := errors.New("later input failed")
	var attempts [2]atomic.Int32
	world := testworld.New(workflow.Use(func(ctx context.Context, _ joinedInput) (joinedOutput, error) {
		values, err := workflow.All(ctx, func(ctx context.Context, input int) (int, error) {
			attempts[input].Add(1)
			if input == 0 {
				close(started)
				<-ctx.Done()
				close(stopped)
				return 0, ctx.Err()
			}
			<-started
			return 0, workflow.FatalError(failure)
		}, []int{0, 1})
		return joinedOutput{Values: values}, err
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), 2*time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, joinedInput{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Value(ctx); !errors.Is(err, failure) {
		t.Fatalf("All error=%v, want original failure", err)
	}
	select {
	case <-stopped:
	case <-ctx.Done():
		t.Fatal("All did not cancel the earlier running input")
	}
	if attempts[0].Load() != 1 || attempts[1].Load() != 1 {
		t.Fatalf("attempts=%d/%d", attempts[0].Load(), attempts[1].Load())
	}
}

func TestSelectReturnsFailedWinnerAndJoinIgnoresOperationFailure(t *testing.T) {
	failure := errors.New("selected failure")
	world := testworld.New(workflow.Use(func(ctx context.Context, _ joinedInput) (joinedOutput, error) {
		failed := workflow.Background(ctx, func(context.Context, struct{}) (int, error) {
			return 0, workflow.FatalError(failure)
		}, struct{}{})
		success := workflow.Background(ctx, increment, 2)
		winner, err := workflow.Select(ctx, failed)
		if winner != failed || !errors.Is(err, failure) {
			return joinedOutput{}, errors.New("Select did not return both failed winner and its error")
		}
		if err := workflow.Join(ctx, failed, success); err != nil {
			return joinedOutput{}, err
		}
		return joinedOutput{Values: []int{success.Unwrap(ctx)}}, nil
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), 2*time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, joinedInput{})
	if err != nil {
		t.Fatal(err)
	}
	output, err := run.Value(ctx)
	if err != nil || !slices.Equal(output.Values, []int{3}) {
		t.Fatalf("output=%#v err=%v", output, err)
	}
}

func TestJoinOkReturnsLaterFailureWithoutWaitingForEarlierTimer(t *testing.T) {
	failure := errors.New("later input failed")
	world := testworld.New(workflow.Use(func(ctx context.Context, _ joinedInput) (joinedOutput, error) {
		timer := workflow.Timer(ctx, time.Hour)
		failed := workflow.Background(ctx, func(context.Context, struct{}) (int, error) {
			return 0, workflow.FatalError(failure)
		}, struct{}{})
		if err := workflow.JoinOk(ctx, timer, failed); !errors.Is(err, failure) {
			return joinedOutput{}, errors.New("JoinOk did not return the later input's failure")
		}
		return joinedOutput{}, nil
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), 2*time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, joinedInput{})
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
	for _, event := range events.Data {
		if event.EventType == worlds.EventTypeWaitCompleted {
			t.Fatal("JoinOk waited for the earlier timer")
		}
	}
}

type inlineBackgroundInput struct{ Fail bool }
type inlineBackgroundOutput struct{ Sum int }

func (inlineBackgroundInput) Workflow(inlineBackgroundOutput) {}

func inlineBackgroundRunner(calls *atomic.Int32) worlds.Runner {
	return workflow.Use(func(ctx context.Context, input inlineBackgroundInput) (inlineBackgroundOutput, error) {
		double := func(_ context.Context, value int) (int, error) {
			if calls.Add(1) == 1 && input.Fail {
				return 0, errors.New("try again")
			}
			return value * 2, nil
		}
		a := workflow.InlineBackground(ctx, double, 1, workflow.StepOptions{MaxRetries: 1})
		b := workflow.InlineBackground(ctx, double, 2)
		if _, err := workflow.Select(ctx, a, b); err != nil {
			return inlineBackgroundOutput{}, err
		}
		if err := workflow.JoinOk(ctx, a, b); err != nil {
			return inlineBackgroundOutput{}, err
		}
		var group workflow.ErrGroup
		c := workflow.InlineBackground(ctx, double, 3)
		group.Add(c)
		if err := group.Wait(ctx); err != nil {
			return inlineBackgroundOutput{}, err
		}
		// A timer makes the runner replay with the recorded results.
		if err := workflow.Sleep(ctx, time.Millisecond); err != nil {
			return inlineBackgroundOutput{}, err
		}
		return inlineBackgroundOutput{Sum: a.Unwrap(ctx) + b.Unwrap(ctx) + c.Unwrap(ctx)}, nil
	})
}

func TestInlineBackgroundComposesWithSelectJoinAndGroups(t *testing.T) {
	var calls atomic.Int32
	world := testworld.New(inlineBackgroundRunner(&calls))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, inlineBackgroundInput{})
	if err != nil {
		t.Fatal(err)
	}
	output, err := run.Value(ctx)
	if err != nil || output.Sum != 12 || calls.Load() != 3 {
		t.Fatalf("output=%#v err=%v calls=%d", output, err, calls.Load())
	}
}

func TestInlineBackgroundRunsInlineWithoutStepDeliveries(t *testing.T) {
	for _, fail := range []bool{false, true} {
		var calls atomic.Int32
		runner := inlineBackgroundRunner(&calls)
		world := testworld.NewManual(runner)
		defer world.Close()
		// Promotion is off, but inline steps still run inline.
		ctx := workflow.SetMaxInlineSteps(workflow.NewContext(context.Background(), workflow.NewClient(world)), 0)
		run, err := workflow.Start(ctx, inlineBackgroundInput{Fail: fail})
		if err != nil {
			t.Fatal(err)
		}
		runtime := worlds.NewRuntime(world, worlds.Runners(runner))
		for len(world.QueuedMessages()) != 0 {
			message := world.QueuedMessages()[0]
			if message.Input.StepID != nil {
				t.Fatalf("queued a step delivery: %#v", message.Input)
			}
			if err := runtime.Execute(ctx, message.Input); err != nil {
				t.Fatal(err)
			}
			if err := world.QueueCancel(ctx, worlds.QueueCancelInput{MessageID: message.ID}); err != nil {
				t.Fatal(err)
			}
		}
		output, err := run.Value(ctx)
		// A failed first attempt is retried once.
		want := int32(3)
		if fail {
			want = 4
		}
		if err != nil || output.Sum != 12 || calls.Load() != want {
			t.Fatalf("fail=%v output=%#v err=%v calls=%d", fail, output, err, calls.Load())
		}
	}
}
