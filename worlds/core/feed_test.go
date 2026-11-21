package core_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/core"
	"github.com/eriicafes/go-workflow/worlds/testworld"
)

type feedInput struct{ Token string }
type feedOutput struct{ Payload string }

func (feedInput) Workflow(feedOutput) {}

// feedRunner waits for its hook's payload while a step it runs inline waits
// for the runner to have it.
func feedRunner() core.Runner {
	release := make(chan struct{})
	var once sync.Once
	return workflow.Use(func(ctx context.Context, input feedInput) (feedOutput, error) {
		hook := workflow.CreateHook[string](ctx, workflow.HookOption{Token: input.Token})
		step := workflow.InlineBackground(ctx, func(ctx context.Context, _ struct{}) (struct{}, error) {
			select {
			case <-release:
			case <-ctx.Done():
			}
			return struct{}{}, nil
		}, struct{}{})
		payload, err := hook.Next(ctx)
		if err != nil {
			return feedOutput{}, err
		}
		once.Do(func() { close(release) })
		if _, err := step.Value(ctx); err != nil {
			return feedOutput{}, err
		}
		return feedOutput{Payload: payload}, nil
	})
}

func TestFlowWaitingOnAnInlineStepTakesAPushedPayloadBeforeItReadsTheLog(t *testing.T) {
	// The flow would read the log only after an hour, so the payload can only
	// reach it through the World's feed.
	logPoll, feedPoll := core.LogPollInterval, core.FeedPollInterval
	core.LogPollInterval, core.FeedPollInterval = time.Hour, time.Hour
	defer func() { core.LogPollInterval, core.FeedPollInterval = logPoll, feedPoll }()

	world := testworld.New(feedRunner())
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), 5*time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, feedInput{Token: "feed"})
	if err != nil {
		t.Fatal(err)
	}
	if err := workflow.ResumeHookWithRetries(ctx, "feed", "pushed", 50, 20*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if output, err := run.Value(ctx); err != nil || output.Payload != "pushed" {
		t.Fatalf("output = %+v err = %v", output, err)
	}
}

// pollOnly hides the World's push.
type pollOnly struct{ worlds.World }

func TestFlowWaitingOnAnInlineStepPollsTheLogWithoutAPush(t *testing.T) {
	logPoll, feedPoll := core.LogPollInterval, core.FeedPollInterval
	core.LogPollInterval, core.FeedPollInterval = 20*time.Millisecond, time.Hour
	defer func() { core.LogPollInterval, core.FeedPollInterval = logPoll, feedPoll }()

	runner := feedRunner()
	base := testworld.NewManual(runner)
	defer base.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(base)), 5*time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, feedInput{Token: "poll"})
	if err != nil {
		t.Fatal(err)
	}
	resumed := make(chan error, 1)
	go func() { resumed <- workflow.ResumeHookWithRetries(ctx, "poll", "polled", 100, 20*time.Millisecond) }()
	// The flow's delivery waits on its inline step until the poll brings
	// it the payload.
	if err := core.NewRuntime(pollOnly{base}, core.Runners(runner)).Execute(ctx, base.QueuedMessages()[0].Input); err != nil {
		t.Fatal(err)
	}
	if err := <-resumed; err != nil {
		t.Fatal(err)
	}
	if output, err := run.Value(ctx); err != nil || output.Payload != "polled" {
		t.Fatalf("output = %+v err = %v", output, err)
	}
}
