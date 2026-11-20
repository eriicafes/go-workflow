package vercelworld

import (
	"context"
	"encoding/json"
	"strings"
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
	// The message keeps the topic, so a later hop is published onto it.
	var message queueMessage
	if err := json.Unmarshal([]byte(requests[0].Body), &message); err != nil || message.Topic != "workflow-backfill" {
		t.Fatalf("message = %+v, %v, want the routed topic kept", message, err)
	}
}

func TestNewRejectsARouteWithoutATopic(t *testing.T) {
	_, err := New(Config{
		BaseURL:  "https://app.example.com/v1/workflow",
		QueueURL: "https://iad1.vercel-queue.com/api/v3",
		Topic:    "workflow",
		Routes:   worlds.Routes(worlds.Route[routedWorkflow]("backfill")),
	})
	if err == nil || !strings.Contains(err.Error(), "has no topic") {
		t.Fatalf("New = %v, want an error for the queue without a topic", err)
	}
}
