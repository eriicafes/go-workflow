package postgresworld

import (
	"context"
	"testing"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/worlds"
)

type routedWorkflow struct {
	workflow.Struct[string]
}

func TestQueueRoutesWorkflowsToTheQueuesWorkersClaim(t *testing.T) {
	t.Parallel()
	url, schema := testSchema(t)
	world := testWorld(t, testPool(t, url, schema), Config{Routes: worlds.Routes(worlds.Route[routedWorkflow]("backfill"))})
	ctx := context.Background()
	if !world.Capabilities().QueueRouting {
		t.Fatal("QueueRouting = false, want true with routes")
	}

	routed, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_routed", WorkflowName: worlds.WorkflowName[routedWorkflow]()})
	if err != nil {
		t.Fatal(err)
	}
	unrouted, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_other", WorkflowName: "example.com/other.Workflow"})
	if err != nil {
		t.Fatal(err)
	}

	claimed, err := world.claim(ctx, 10, []string{DefaultQueue})
	if err != nil || len(claimed) != 1 || claimed[0].id != unrouted.MessageID {
		t.Fatalf("default queue claimed %#v err=%v, want only the unrouted delivery", claimed, err)
	}
	claimed, err = world.claim(ctx, 10, []string{"backfill"})
	if err != nil || len(claimed) != 1 || claimed[0].id != routed.MessageID || claimed[0].input.QueueName != "backfill" {
		t.Fatalf("backfill queue claimed %#v err=%v, want the routed delivery", claimed, err)
	}
}
