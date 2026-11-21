package awsworld

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

func TestStartHookOfAnExistingRunIsNotAConflict(t *testing.T) {
	dynamo := newFakeDynamo()
	world := testWorld(t, dynamo, &fakeSQS{}, newFakeScheduler())
	now := time.Now()
	// The run and its start hook were created by an earlier attempt.
	runID := worlds.RunID("wrun_retried")
	dynamo.items[itemID(runKey(string(runID)))] = runItem(runView{Run: worlds.Run{RunID: runID, WorkflowName: "wf", Status: worlds.StatusPending, CreatedAt: now, UpdatedAt: now}, eventCount: 1})
	dynamo.items[itemID(tokenKey("tok"))] = tokenItem("tok", tokenView{hookID: worlds.StartHookID(runID), runID: runID})
	input := worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "wf", StartHook: &worlds.StartHook{Token: "tok"}}
	if _, err := world.startHookItems(context.Background(), input, now); !errors.Is(err, worlds.ErrRunAlreadyExists) {
		t.Fatalf("retried create error = %v, want ErrRunAlreadyExists", err)
	}
	// Another run's create with the token conflicts, naming the holder.
	input.RunID = "wrun_other"
	var conflict *worlds.HookConflictError
	if _, err := world.startHookItems(context.Background(), input, now); !errors.As(err, &conflict) || conflict.RunID != runID {
		t.Fatalf("other run's create error = %v", err)
	}
}
