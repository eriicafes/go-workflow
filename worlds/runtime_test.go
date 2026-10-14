package worlds_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/testworld"
)

func TestRuntimeMarksRunsWithUnknownWorkflowsAsFailed(t *testing.T) {
	world := testworld.New()
	defer world.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	runID := worlds.RunID("wrun_missing_workflow")
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "missing"}); err != nil {
		t.Fatal(err)
	}
	if _, err := world.Queue(ctx, worlds.QueueInput{RunID: runID, IdempotencyKey: string(runID)}); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID})
		if err != nil {
			t.Fatal(err)
		}
		if run.Status == worlds.StatusFailed {
			if !strings.Contains(string(run.Error), "workflow not found") {
				t.Fatalf("failure = %q", run.Error)
			}
			return
		}
	}
	t.Fatal("unknown workflow did not fail")
}
