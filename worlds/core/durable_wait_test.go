package core_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/core"
	"github.com/eriicafes/go-workflow/worlds/testworld"
)

type durableWaitInput struct{}
type durableWaitOutput struct{}

func (durableWaitInput) Workflow(durableWaitOutput) {}

func TestDurableWaitCompletes(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, _ durableWaitInput) (durableWaitOutput, error) {
		_, err := workflow.Timer(ctx, time.Millisecond).Value(ctx)
		return durableWaitOutput{}, err
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	completed, err := workflow.Start(ctx, durableWaitInput{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := completed.Value(ctx); err != nil {
		t.Fatal(err)
	}
	// The wait_completed records when the wait was due, as its wait_created.
	events, err := world.EventsList(ctx, worlds.EventsListInput{RunID: worlds.RunID(completed.ID())})
	if err != nil {
		t.Fatal(err)
	}
	var due time.Time
	for _, event := range events.Data {
		switch event.EventType {
		case worlds.EventTypeWaitCreated:
			var data worlds.DataWaitCreated
			_ = json.Unmarshal(event.Data, &data)
			due = data.ResumeAt
		case worlds.EventTypeWaitCompleted:
			var data worlds.DataWaitCompleted
			if json.Unmarshal(event.Data, &data) != nil || data.ResumeAt == nil || !data.ResumeAt.Equal(due) {
				t.Fatalf("wait_completed data = %s, want resumeAt %v", event.Data, due)
			}
			return
		}
	}
	t.Fatal("no wait_completed")
}

func TestWakeRunCompletesOnlySelectedWaits(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, _ durableWaitInput) (durableWaitOutput, error) {
		err := workflow.Join(ctx, workflow.Timer(ctx, time.Hour), workflow.Timer(ctx, time.Hour))
		return durableWaitOutput{}, err
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, durableWaitInput{})
	if err != nil {
		t.Fatal(err)
	}
	var waitIDs []string
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		events, err := world.EventsList(ctx, worlds.EventsListInput{RunID: worlds.RunID(run.ID())})
		if err != nil {
			t.Fatal(err)
		}
		waitIDs = waitIDs[:0]
		for _, event := range events.Data {
			if event.EventType == worlds.EventTypeWaitCreated {
				waitIDs = append(waitIDs, event.CorrelationID)
			}
		}
		if len(waitIDs) == 2 {
			break
		}
	}
	if len(waitIDs) != 2 {
		t.Fatalf("wait IDs = %#v", waitIDs)
	}
	if count, err := worlds.WakeRun(ctx, world, worlds.RunID(run.ID()), waitIDs[:1]); err != nil || count != 1 {
		t.Fatalf("selected wake count = %d, %v", count, err)
	}
	if status, err := run.Status(ctx); err != nil || status.Terminal() {
		t.Fatalf("run status after one wake = %s, %v", status, err)
	}
	// The flow the first wake queued may not have completed its wait yet,
	// and a wait that two wakes complete completes once.
	if count, err := worlds.WakeRun(ctx, world, worlds.RunID(run.ID()), nil); err != nil || count < 1 {
		t.Fatalf("remaining wake count = %d, %v", count, err)
	}
	if _, err := run.Value(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestCancelledDeadlinesDoNotFireOnReplayAfterExpiry(t *testing.T) {
	for _, test := range []struct {
		name     string
		deadline bool
		parent   bool
	}{
		{name: "timeout"},
		{name: "deadline", deadline: true},
		{name: "parent cancellation", deadline: true, parent: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				world := testworld.NewManual()
				defer world.Close()
				runner := workflow.Use(func(ctx context.Context, _ durableWaitInput) (durableWaitOutput, error) {
					parent := ctx
					var cancelParent func(error) error
					var err error
					if test.parent {
						parent, cancelParent, err = workflow.WithCancelCause(ctx)
						if err != nil {
							return durableWaitOutput{}, err
						}
					}
					var cancelCtx context.Context
					var cancel func() error
					if test.deadline {
						now := workflow.Now(ctx)
						cancelCtx, cancel, err = workflow.WithDeadline(parent, now.Add(time.Hour))
					} else {
						cancelCtx, cancel, err = workflow.WithTimeout(parent, time.Hour)
					}
					if err != nil {
						return durableWaitOutput{}, err
					}
					var cause error = context.Canceled
					if test.parent {
						cause = errors.New("parent stopped")
						err = cancelParent(cause)
					} else {
						err = cancel()
					}
					if err != nil {
						return durableWaitOutput{}, err
					}
					if !errors.Is(context.Cause(cancelCtx), cause) {
						return durableWaitOutput{}, errors.New("cancellation cause changed on replay")
					}
					return durableWaitOutput{}, workflow.Sleep(ctx, 2*time.Hour)
				})
				runtime := core.NewRuntime(world, core.Runners(runner))
				ctx := workflow.NewContext(context.Background(), workflow.NewClient(world))
				run, err := workflow.Start(ctx, durableWaitInput{})
				if err != nil {
					t.Fatal(err)
				}
				deliverNext(t, ctx, world, runtime)
				// Replay reconstructs the deadline timer before reaching the recorded cancellation.
				time.Sleep(3 * time.Hour)
				drain(t, ctx, world, runtime)
				status, err := run.Status(ctx)
				if err != nil || status != workflow.StatusCompleted {
					t.Fatalf("status=%s err=%v", status, err)
				}
				if _, err := run.Value(ctx); err != nil {
					t.Fatal(err)
				}
				events, err := world.EventsList(ctx, worlds.EventsListInput{RunID: worlds.RunID(run.ID())})
				if err != nil {
					t.Fatal(err)
				}
				completed := 0
				for _, event := range events.Data {
					if event.EventType == worlds.EventTypeWaitCompleted {
						completed++
					}
				}
				if completed != 1 {
					t.Fatalf("completed timers=%d; only the independent sleep should fire", completed)
				}
			})
		})
	}
}

func TestWaitCompletedForAnotherTimeThanItsWaitDiverges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	world := testworld.NewManual()
	defer world.Close()
	runtime := core.NewRuntime(world, core.Runners(workflow.Use(func(ctx context.Context, _ durableWaitInput) (durableWaitOutput, error) {
		return durableWaitOutput{}, workflow.Sleep(ctx, time.Hour)
	})))
	run, err := workflow.Start(workflow.NewContext(ctx, workflow.NewClient(world)), durableWaitInput{})
	if err != nil {
		t.Fatal(err)
	}
	runID := worlds.RunID(run.ID())
	if err := runtime.Execute(ctx, world.QueuedMessages()[0].Input); err != nil {
		t.Fatal(err)
	}
	events, err := world.EventsList(ctx, worlds.EventsListInput{RunID: runID})
	if err != nil {
		t.Fatal(err)
	}
	created := events.Data[len(events.Data)-1]
	var wait worlds.DataWaitCreated
	if created.EventType != worlds.EventTypeWaitCreated || json.Unmarshal(created.Data, &wait) != nil {
		t.Fatalf("last event = %s %s, want the wait's creation", created.EventType, created.Data)
	}
	// A completion that names another time is not the completion of the
	// wait the log created.
	other := wait.ResumeAt.Add(time.Second)
	data, _ := json.Marshal(worlds.DataWaitCompleted{ResumeAt: &other})
	if _, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeWaitCompleted, CorrelationID: created.CorrelationID, Data: data}}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Execute(ctx, worlds.QueueInput{RunID: runID, Attempt: 1, DeliveryCount: 1, ReplayDivergenceCount: 3}); err != nil {
		t.Fatal(err)
	}
	_, err = run.Value(workflow.NewContext(ctx, workflow.NewClient(world)))
	if !errors.Is(err, worlds.ErrCorruptedEventLog) || !strings.Contains(err.Error(), "recorded wait_completed has resumeAt") {
		t.Fatalf("run = %v, want a corrupted event log for the wait's completion", err)
	}
}
