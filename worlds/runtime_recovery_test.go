package worlds_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/testworld"
)

func TestEarlyTimerDeliveryQueuesItsReplacement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	world := testworld.NewManual()
	defer world.Close()
	runtime := worlds.NewRuntime(world, worlds.Runners(workflow.Use(sleepAnHour)))
	run, err := workflow.Start(workflow.NewContext(ctx, workflow.NewClient(world)), sleeperInput{})
	if err != nil {
		t.Fatal(err)
	}
	deliverNext(t, ctx, world, runtime)

	// The timer's own delivery arrives before the wait is due, as when the
	// delivering worker's clock is behind the declaring worker's.
	timer := world.QueuedMessages()[0]
	if timer.Input.NotBefore == nil {
		t.Fatalf("queued = %#v, want the timer", timer.Input)
	}
	if err := runtime.Execute(ctx, timer.Input); err != nil {
		t.Fatal(err)
	}
	if err := world.QueueCancel(ctx, worlds.QueueCancelInput{MessageID: timer.ID}); err != nil {
		t.Fatal(err)
	}

	queued := world.QueuedMessages()
	if len(queued) != 1 || queued[0].Input.IdempotencyKey != timer.Input.IdempotencyKey || queued[0].Input.NotBefore == nil || !queued[0].Input.NotBefore.Equal(*timer.Input.NotBefore) {
		t.Fatalf("queued = %#v, want the timer queued again", queued)
	}
	if status, err := run.Status(workflow.NewContext(ctx, workflow.NewClient(world))); err != nil || status != workflow.StatusRunning {
		t.Fatalf("status = %s, err = %v", status, err)
	}
}

// flowQueueFailure fails the next queued flow delivery while armed.
type flowQueueFailure struct {
	*testworld.World
	armed bool
}

func (w *flowQueueFailure) Queue(ctx context.Context, input worlds.QueueInput) (worlds.QueueResult, error) {
	if w.armed && input.StepID == nil {
		w.armed = false
		return worlds.QueueResult{}, errQueueUnavailable
	}
	return w.World.Queue(ctx, input)
}

func TestRedeliveredStepQueuesTheFlowItFailedToQueue(t *testing.T) {
	resetCalls()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	world := testworld.NewManual()
	defer world.Close()
	failing := &flowQueueFailure{World: world}
	runtime := worlds.NewRuntime(failing, worlds.Runners(workflow.Use(alphaThenBeta)))
	run := startVersioned(t, ctx, world)
	deliverNext(t, ctx, world, runtime)

	// alpha completes, then queueing the flow that consumes it fails.
	step := world.QueuedMessages()[0]
	failing.armed = true
	if err := runtime.Execute(ctx, step.Input); !errors.Is(err, errQueueUnavailable) {
		t.Fatalf("delivery error = %v, want the queue error", err)
	}
	// The redelivery finds alpha settled and queues the flow.
	if err := runtime.Execute(ctx, step.Input); err != nil {
		t.Fatal(err)
	}
	if err := world.QueueCancel(ctx, worlds.QueueCancelInput{MessageID: step.ID}); err != nil {
		t.Fatal(err)
	}

	drain(t, ctx, world, runtime)
	if _, err := run.Value(workflow.NewContext(ctx, workflow.NewClient(world))); err != nil {
		t.Fatal(err)
	}
	if alphaCalls.Load() != 1 || betaCalls.Load() != 1 {
		t.Fatalf("alpha calls = %d, beta calls = %d, want 1 each", alphaCalls.Load(), betaCalls.Load())
	}
}

func TestReturnedRunCancelledErrorFailsTheRun(t *testing.T) {
	runner := workflow.Use(func(ctx context.Context, _ backoffInput) (backoffOutput, error) {
		return backoffOutput{}, fmt.Errorf("child: %w", workflow.ErrRunCancelled)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	world := testworld.NewManual()
	defer world.Close()
	runtime := worlds.NewRuntime(world, worlds.Runners(runner))
	client := workflow.NewContext(ctx, workflow.NewClient(world))
	run, err := workflow.Start(client, backoffInput{})
	if err != nil {
		t.Fatal(err)
	}
	drain(t, ctx, world, runtime)
	if status, err := run.Status(client); err != nil || status != workflow.StatusFailed {
		t.Fatalf("status = %s, err = %v, want failed", status, err)
	}
}

var backoffCalls atomic.Int32

func countedFailForAnHour(context.Context, struct{}) (struct{}, error) {
	backoffCalls.Add(1)
	return struct{}{}, workflow.RetryableError(errors.New("not yet"), time.Hour)
}

func TestEarlyStepDeliveryWaitsForItsRetry(t *testing.T) {
	backoffCalls.Store(0)
	runner := workflow.Use(func(ctx context.Context, _ backoffInput) (backoffOutput, error) {
		_, err := workflow.Step(ctx, countedFailForAnHour, struct{}{}, workflow.StepOptions{MaxRetries: 3})
		return backoffOutput{}, err
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	world := testworld.NewManual()
	defer world.Close()
	runtime := worlds.NewRuntime(world, worlds.Runners(runner))
	if _, err := workflow.Start(workflow.NewContext(ctx, workflow.NewClient(world)), backoffInput{}); err != nil {
		t.Fatal(err)
	}
	deliverNext(t, ctx, world, runtime)
	first := deliverNext(t, ctx, world, runtime) // fails, retry in an hour

	// The first attempt's delivery is redelivered before the retry is due.
	if err := runtime.Execute(ctx, first); err != nil {
		t.Fatal(err)
	}
	if backoffCalls.Load() != 1 {
		t.Fatalf("step calls = %d, want 1", backoffCalls.Load())
	}
	keys := map[string]bool{}
	for _, message := range world.QueuedMessages() {
		if message.Input.StepID == nil {
			continue
		}
		if message.Input.NotBefore == nil || time.Until(*message.Input.NotBefore) < 50*time.Minute {
			t.Fatalf("step delivery %#v is not held until the retry", message.Input)
		}
		keys[message.Input.IdempotencyKey] = true
	}
	if len(keys) != 1 {
		t.Fatalf("retry keys = %v, want one shared key", keys)
	}
}

func TestHookNextSkipsAPayloadThatFailsToDecode(t *testing.T) {
	runner := workflow.Use(func(ctx context.Context, input hookedInput) (hookedOutput, error) {
		hook := workflow.CreateHook[string](ctx, workflow.HookOption{Token: input.Token})
		if _, err := hook.Next(ctx); err == nil {
			return hookedOutput{}, errors.New("first payload decoded")
		}
		value, err := hook.Next(ctx)
		return hookedOutput{Value: value}, err
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	world := testworld.NewManual()
	defer world.Close()
	runtime := worlds.NewRuntime(world, worlds.Runners(runner))
	client := workflow.NewContext(ctx, workflow.NewClient(world))
	token := workflow.NewHookToken()
	run, err := workflow.Start(client, hookedInput{Token: token})
	if err != nil {
		t.Fatal(err)
	}
	drain(t, ctx, world, runtime)
	if err := workflow.ResumeHook(client, token, 123); err != nil {
		t.Fatal(err)
	}
	if err := workflow.ResumeHook(client, token, "good"); err != nil {
		t.Fatal(err)
	}
	drain(t, ctx, world, runtime)
	output, err := run.Value(client)
	if err != nil || output.Value != "good" {
		t.Fatalf("output = %#v, err = %v", output, err)
	}
}

// queueOutage fails every Queue call.
type queueOutage struct{ *testworld.World }

func (queueOutage) Queue(context.Context, worlds.QueueInput) (worlds.QueueResult, error) {
	return worlds.QueueResult{}, errQueueUnavailable
}

func TestResumeHookReportsARecordedPayloadItCouldNotQueue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	world := testworld.NewManual()
	defer world.Close()
	runtime := worlds.NewRuntime(world, worlds.Runners(workflow.Use(awaitHook)))
	token := workflow.NewHookToken()
	run, err := workflow.Start(workflow.NewContext(ctx, workflow.NewClient(world)), hookedInput{Token: token})
	if err != nil {
		t.Fatal(err)
	}
	drain(t, ctx, world, runtime)
	hook, err := world.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: token})
	if err != nil {
		t.Fatal(err)
	}
	result, err := worlds.ResumeHook(ctx, queueOutage{world}, hook, []byte(`"value"`))
	if !errors.Is(err, worlds.ErrResumeNotQueued) || !errors.Is(err, errQueueUnavailable) || result.EventID == "" {
		t.Fatalf("result = %#v, err = %v", result, err)
	}

	// WakeRun queues the run even though it has no waits to complete.
	if _, err := worlds.WakeRun(ctx, world, worlds.RunID(run.ID()), nil); err != nil {
		t.Fatal(err)
	}
	drain(t, ctx, world, runtime)
	output, err := run.Value(workflow.NewContext(ctx, workflow.NewClient(world)))
	if err != nil || output.Value != "value" {
		t.Fatalf("output = %#v, err = %v", output, err)
	}
}

func TestDecodedFatalErrorKeepsItsCause(t *testing.T) {
	cause := errors.New("boom")
	for _, err := range []error{
		worlds.FatalError{Cause: cause},
		worlds.RetryableError{Cause: cause, After: time.Minute},
	} {
		data, marshalErr := json.Marshal(worlds.NewError(err))
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		decoded := worlds.DecodeError(data)
		var fatal worlds.FatalError
		var retryable worlds.RetryableError
		switch {
		case errors.As(decoded, &fatal):
			if fatal.Error() != "boom" || !errors.Is(fatal, cause) {
				t.Fatalf("fatal = %v, cause = %v", fatal, fatal.Cause)
			}
		case errors.As(decoded, &retryable):
			if retryable.Error() != "boom" || retryable.After != time.Minute || !errors.Is(retryable, cause) {
				t.Fatalf("retryable = %v, cause = %v", retryable, retryable.Cause)
			}
		default:
			t.Fatalf("%v decoded without its type", err)
		}
	}
	if message := (worlds.FatalError{}).Error(); message == "" {
		t.Fatal("a FatalError without a cause has no message")
	}
}

func TestThenSettlesANilCallbackResultWithAnError(t *testing.T) {
	runner := workflow.Use(func(ctx context.Context, _ backoffInput) (backoffOutput, error) {
		source := workflow.Background(ctx, alpha, struct{}{})
		_, err := workflow.Then(ctx, source, func(string, error) *workflow.Result[string] { return nil }).Value(ctx)
		if !errors.Is(err, workflow.ErrNilResult) {
			return backoffOutput{}, fmt.Errorf("then error = %v", err)
		}
		var missing *workflow.Result[string]
		if _, err := workflow.Select(ctx, missing); !errors.Is(err, workflow.ErrNilResult) {
			return backoffOutput{}, fmt.Errorf("select error = %v", err)
		}
		return backoffOutput{}, nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	world := testworld.NewManual()
	defer world.Close()
	runtime := worlds.NewRuntime(world, worlds.Runners(runner))
	client := workflow.NewContext(ctx, workflow.NewClient(world))
	run, err := workflow.Start(client, backoffInput{})
	if err != nil {
		t.Fatal(err)
	}
	drain(t, ctx, world, runtime)
	if _, err := run.Value(client); err != nil {
		t.Fatal(err)
	}
}
