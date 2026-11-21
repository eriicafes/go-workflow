package core_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/worlds/core"
	"github.com/eriicafes/go-workflow/worlds/testworld"
)

type feedInput struct{ Token string }
type feedOutput struct{ Payload string }

func (feedInput) Workflow(feedOutput) {}

func TestFlowWaitingOnAnInlineStepTakesAPushedPayloadBeforeItReadsTheLog(t *testing.T) {
	// The flow would read the log only after an hour, so the payload can only
	// reach it through the World's feed.
	logPoll, feedPoll := core.LogPollInterval, core.FeedPollInterval
	core.LogPollInterval, core.FeedPollInterval = time.Hour, time.Hour
	defer func() { core.LogPollInterval, core.FeedPollInterval = logPoll, feedPoll }()

	release := make(chan struct{})
	var once sync.Once
	world := testworld.New(workflow.Use(func(ctx context.Context, input feedInput) (feedOutput, error) {
		hook := workflow.CreateHook[string](ctx, workflow.HookOption{Token: input.Token})
		// The step runs inline until the runner has the payload.
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
	}))
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
