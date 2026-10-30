package vercelworld

import (
	"context"
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
	world, _ := newQueueWorld(t, server, false)
	world.config.Topics = map[string]string{"backfill": "workflow-backfill"}
	world.config.Routes = worlds.Routes(worlds.Route[routedWorkflow]("backfill"))

	routed, err := world.Queue(context.Background(), worlds.QueueInput{RunID: "wrun_1", WorkflowName: worlds.WorkflowName[routedWorkflow]()})
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
	// the record keeps the topic, so a QStash timer is released onto it
	record, ok, err := world.getQueueRecord(context.Background(), routed.MessageID)
	if err != nil || !ok || record.Topic != "workflow-backfill" {
		t.Fatalf("record = %+v, %v, %v, want the routed topic kept", record, ok, err)
	}
}

func TestNewRejectsARouteWithoutATopic(t *testing.T) {
	_, err := New(Config{
		BaseURL:  "https://app.example.com/v1/workflow",
		RedisURL: "redis://localhost:6379",
		QueueURL: "https://iad1.vercel-queue.com/api/v3",
		Topic:    "workflow",
		Routes:   worlds.Routes(worlds.Route[routedWorkflow]("backfill")),
	})
	if err == nil || !strings.Contains(err.Error(), "has no topic") {
		t.Fatalf("New = %v, want an error for the queue without a topic", err)
	}
}
