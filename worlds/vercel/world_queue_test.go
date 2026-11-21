package vercelworld

import (
	"context"
	"testing"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/worlds"
)

type routedWorkflow struct {
	workflow.Struct[string]
}

func TestQueuePublishesRoutedWorkflowsToTheirTopic(t *testing.T) {
	server := newRecorder(t, nil)
	world := newQueueWorld(t, server)
	world.config.Topics = map[string]string{"backfill": "workflow-backfill"}
	world.config.Routes = worlds.Routes(worlds.Route[routedWorkflow]("backfill"))

	_, err := world.Queue(context.Background(), worlds.QueueInput{RunID: "wrun_1", WorkflowName: worlds.WorkflowName[routedWorkflow]()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := world.Queue(context.Background(), worlds.QueueInput{RunID: "wrun_2", WorkflowName: "example.com/other.Workflow"}); err != nil {
		t.Fatal(err)
	}

	requests := server.all()
	if len(requests) != 2 || requests[0].Path != "/api/v3/topic/workflow-backfill" || requests[1].Path != "/api/v3/topic/workflow" {
		t.Fatalf("published to %+v, want the routed topic then the default topic", requests)
	}
}
