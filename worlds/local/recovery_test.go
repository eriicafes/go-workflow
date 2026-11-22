package localworld

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

func TestRecoveryReleasesOrphanedInlineSteps(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	deliveries := make(chan worlds.QueueInput, 10)
	world := queueWorld(t, Config{Dir: dir}, func(context.Context, worlds.QueueInput) error { return nil })
	runID := worlds.NewRunID()
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "orphans"}); err != nil {
		t.Fatal(err)
	}
	inline, _ := json.Marshal(worlds.DataStepStarted{StepName: "inline", Inline: true})
	for _, event := range []worlds.EventsCreateEvent{
		{EventType: worlds.EventTypeRunStarted},
		// An inline step a dead process was running, and a queued step,
		// which is left to its message.
		{EventType: worlds.EventTypeStepStarted, CorrelationID: "step_inline", Data: inline},
		{EventType: worlds.EventTypeStepCreated, CorrelationID: "step_queued", Data: []byte(`{"stepName":"queued"}`)},
		{EventType: worlds.EventTypeStepStarted, CorrelationID: "step_queued"},
		{EventType: worlds.EventTypeWaitCreated, CorrelationID: "wait_a", Data: []byte(`{"resumeAt":"` + time.Now().Add(200*time.Millisecond).UTC().Format(time.RFC3339Nano) + `"}`)},
	} {
		if _, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: event}); err != nil {
			t.Fatalf("%s: %v", event.EventType, err)
		}
	}
	if err := world.Close(); err != nil {
		t.Fatal(err)
	}
	// The steps were started by a process that is gone.
	previous := bootedAt
	bootedAt = time.Now()
	defer func() { bootedAt = previous }()

	reopened, err := Open(Config{Dir: dir}, nil)
	if err != nil {
		t.Fatal(err)
	}
	reopened.execute = func(_ context.Context, input worlds.QueueInput) error {
		deliveries <- input
		return nil
	}
	if err := reopened.Prepare(ctx); err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	page, err := reopened.EventsList(ctx, worlds.EventsListInput{RunID: runID})
	if err != nil {
		t.Fatal(err)
	}
	var released []string
	var release worlds.Event
	for _, event := range page.Data {
		if event.EventType == worlds.EventTypeStepRetrying {
			released, release = append(released, event.CorrelationID), event
		}
	}
	if len(released) != 1 || released[0] != "step_inline" {
		t.Fatalf("released %q, want the inline step", released)
	}
	// The release names the step and a fixed error, and nothing else.
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(release.Data, &fields); err != nil || len(fields) != 2 || string(fields["stepName"]) != `"inline"` {
		t.Fatalf("release data = %s err=%v, want its stepName and error alone", release.Data, err)
	}
	var result worlds.DataStepResult
	const payload = `devl[["Error",1],{"name":2,"message":3},"Error","The process running this step exited before it finished."]`
	if err := json.Unmarshal(release.Data, &result); err != nil || string(result.Error) != payload {
		t.Fatalf("release error = %q err=%v", result.Error, err)
	}
	if decoded := worlds.DecodeErrorPayload(result.Error, nil); decoded == nil || decoded.Message != orphanedStepMessage {
		t.Fatalf("decoded release error = %#v", decoded)
	}
	step, err := reopened.StepsGet(ctx, worlds.StepsGetInput{RunID: runID, StepID: "step_inline"})
	if err != nil || step.Status != worlds.StatusPending || len(step.Error) == 0 || step.RetryAfter != nil {
		t.Fatalf("released step = %#v err=%v", step, err)
	}
	// The recovery flow, then the open wait's backstop wake.
	recovery := receive(t, deliveries)
	if !recovery.Recovery || recovery.RunID != runID {
		t.Fatalf("first delivery = %+v, want the recovery flow", recovery)
	}
	wake := receive(t, deliveries)
	if wake.Recovery || wake.NotBefore == nil || wake.IdempotencyKey == "" {
		t.Fatalf("second delivery = %+v, want the wait's backstop", wake)
	}
}
