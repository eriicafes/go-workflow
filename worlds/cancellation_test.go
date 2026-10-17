package worlds_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/testworld"
)

type cancelRaceWorld struct {
	worlds.World
	arrivals  atomic.Int32
	ready     chan struct{}
	failWrite atomic.Bool
	mu        sync.Mutex
	causes    []string
}

func (w *cancelRaceWorld) EventsCreate(ctx context.Context, input worlds.EventsCreateInput) (worlds.EventResult, error) {
	var data struct {
		Cause json.RawMessage `json:"cause"`
	}
	if w.ready != nil && input.Event.EventType == worlds.EventTypeHookReceived && json.Unmarshal(input.Event.Data, &data) == nil && len(data.Cause) != 0 {
		if w.arrivals.Add(1) == 2 {
			close(w.ready)
		}
		select {
		case <-w.ready:
		case <-ctx.Done():
			return worlds.EventResult{}, ctx.Err()
		}
	}
	return w.World.EventsCreate(ctx, input)
}

func (w *cancelRaceWorld) StreamsWrite(ctx context.Context, input worlds.StreamsWriteInput) error {
	if w.failWrite.Swap(false) {
		return errors.New("notification unavailable")
	}
	var data struct {
		Cause json.RawMessage `json:"cause"`
	}
	if json.Unmarshal(input.Chunk, &data) == nil && len(data.Cause) != 0 {
		w.mu.Lock()
		w.causes = append(w.causes, worlds.DecodeError(data.Cause).Error())
		w.mu.Unlock()
	}
	return w.World.StreamsWrite(ctx, input)
}

func TestConcurrentCancellationPublishesFirstCommittedCause(t *testing.T) {
	ctx, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	base := testworld.NewManual()
	defer base.Close()
	world := &cancelRaceWorld{World: base, ready: make(chan struct{})}
	runner := workflow.Use(func(ctx context.Context, _ versionedInput) (versionedOutput, error) {
		cancelCtx, _, err := workflow.WithCancel(ctx)
		if err != nil {
			return versionedOutput{}, err
		}
		step := func(ctx context.Context, cause string) (struct{}, error) {
			_, err := worlds.CancelContext(context.WithoutCancel(ctx), worlds.CancellationReferences(ctx)[0], errors.New(cause))
			return struct{}{}, err
		}
		a := workflow.Background(cancelCtx, step, "first contender")
		b := workflow.Background(cancelCtx, step, "second contender")
		return versionedOutput{}, workflow.Join(ctx, a, b)
	})
	runtime := worlds.NewRuntime(world, worlds.Runners(runner))
	clientCtx := workflow.NewContext(ctx, workflow.NewClient(world))
	run, err := workflow.Start(clientCtx, versionedInput{})
	if err != nil {
		t.Fatal(err)
	}
	deliverNext(t, ctx, base, runtime)
	var deliveries []worlds.QueueInput
	for _, message := range base.QueuedMessages() {
		if message.Input.StepID != nil {
			deliveries = append(deliveries, message.Input)
		}
	}
	if len(deliveries) != 2 {
		t.Fatalf("step deliveries=%d", len(deliveries))
	}
	errorsCh := make(chan error, 2)
	for _, delivery := range deliveries {
		go func() { errorsCh <- runtime.Execute(ctx, delivery) }()
	}
	for range deliveries {
		if err := <-errorsCh; err != nil {
			t.Fatal(err)
		}
	}
	events, err := base.EventsList(ctx, worlds.EventsListInput{RunID: worlds.RunID(run.ID())})
	if err != nil {
		t.Fatal(err)
	}
	var first string
	for _, event := range events.Data {
		var data struct {
			Cause json.RawMessage `json:"cause"`
		}
		if event.EventType == worlds.EventTypeHookReceived && json.Unmarshal(event.Data, &data) == nil && len(data.Cause) != 0 {
			first = worlds.DecodeError(data.Cause).Error()
			break
		}
	}
	world.mu.Lock()
	defer world.mu.Unlock()
	if len(world.causes) != 2 || first == "" {
		t.Fatalf("first=%q notifications=%v", first, world.causes)
	}
	for _, cause := range world.causes {
		if cause != first {
			t.Fatalf("notification=%q first=%q", cause, first)
		}
	}
}

func TestChildRunsRequireExplicitCancellation(t *testing.T) {
	for _, failNotification := range []bool{false, true} {
		t.Run(map[bool]string{false: "notified", true: "notification retry"}[failNotification], func(t *testing.T) {
			ctx, stop := context.WithTimeout(context.Background(), 3*time.Second)
			defer stop()
			base := testworld.NewManual()
			defer base.Close()
			world := &cancelRaceWorld{World: base}
			world.failWrite.Store(failNotification)
			runner := workflow.Use(func(ctx context.Context, _ versionedInput) (versionedOutput, error) {
				cancelCtx, cancel, err := workflow.WithCancelCause(ctx)
				if err != nil {
					return versionedOutput{}, err
				}
				before, err := workflow.Start(cancelCtx, durableHookInput{Token: "before-cancellation"})
				if err != nil {
					return versionedOutput{}, err
				}
				if err := cancel(errors.New("original")); (err != nil) != failNotification {
					return versionedOutput{}, errors.New("unexpected cancellation notification result")
				}
				if err := cancel(errors.New("replacement")); err != nil {
					return versionedOutput{}, err
				}
				after, err := workflow.Start(cancelCtx, durableHookInput{Token: "after-cancellation"})
				if err != nil {
					return versionedOutput{}, err
				}
				for _, child := range []*workflow.Run[durableHookOutput]{before, after} {
					status, err := child.Status(cancelCtx)
					if err != nil {
						return versionedOutput{}, err
					}
					if status != workflow.StatusPending {
						return versionedOutput{}, errors.New("context cancellation changed child run status")
					}
					exists, err := child.Exists(cancelCtx)
					if err != nil || !exists {
						return versionedOutput{}, errors.New("context cancellation affected child Exists")
					}
					if _, err := child.WorkflowName(cancelCtx); err != nil {
						return versionedOutput{}, err
					}
					if _, err := child.Value(cancelCtx); !errors.Is(err, workflow.ErrRunNotFinished) {
						return versionedOutput{}, errors.New("context cancellation affected child Value")
					}
					if _, err := child.WakeUp(cancelCtx); err != nil {
						return versionedOutput{}, err
					}
					if cancelCtx.Err() != nil {
						if err := child.Cancel(cancelCtx); err != nil {
							return versionedOutput{}, err
						}
					}
					status, err = child.Status(cancelCtx)
					if err != nil {
						return versionedOutput{}, err
					}
					if status != workflow.StatusCancelled {
						return versionedOutput{}, errors.New("explicit cancellation did not cancel child run")
					}
				}
				return versionedOutput{Version: context.Cause(cancelCtx).Error()}, nil
			})
			runtime := worlds.NewRuntime(world, worlds.Runners(runner))
			clientCtx := workflow.NewContext(ctx, workflow.NewClient(world))
			run, err := workflow.Start(clientCtx, versionedInput{})
			if err != nil {
				t.Fatal(err)
			}
			drain(t, ctx, base, runtime)
			output, err := run.Value(clientCtx)
			if err != nil || output.Version != "original" {
				t.Fatalf("output=%#v err=%v", output, err)
			}
		})
	}
}
