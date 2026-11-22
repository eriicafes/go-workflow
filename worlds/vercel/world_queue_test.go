package vercelworld

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/core"
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

func TestQueueConsumerNamesTheStepsAFlowRan(t *testing.T) {
	all, _, _ := newWorlds(t, 1)
	world := all[0]
	ctx := context.Background()
	runID := worlds.NewRunID()
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "flow"}); err != nil {
		t.Fatal(err)
	}
	if _, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeRunStarted}}); err != nil {
		t.Fatal(err)
	}
	created, _ := json.Marshal(worlds.DataStepCreated{StepName: "step", Input: []byte(`"x"`)})
	started, _ := json.Marshal(worlds.DataStepStarted{StepName: "step", Attempt: 1})
	for _, stepID := range []string{"step_a", "step_b", "step_c"} {
		if _, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeStepCreated, CorrelationID: stepID, Data: created}}); err != nil {
			t.Fatal(err)
		}
	}
	// The delivery runs step_a and step_b. It starts step_c too, and stops
	// before it runs it: a start alone names no step.
	var recorder worlds.StepExecutions = world
	handler := &consumer{world: world, execute: func(ctx context.Context, _ worlds.QueueInput) error {
		if _, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeStepStarted, CorrelationID: "step_c", Data: started}}); err != nil {
			return err
		}
		recorder.RecordStepExecution(ctx, "step_a")
		recorder.RecordStepExecution(ctx, "step_b")
		return nil
	}}
	deliver := func(message queueMessage) *httptest.ResponseRecorder {
		t.Helper()
		body, _ := json.Marshal(message)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, deliveryRequest("msg_1", string(body)))
		if response.Code != http.StatusOK {
			t.Fatalf("delivery = %d %s", response.Code, response.Body)
		}
		return response
	}
	if got := deliver(queueMessage{ID: "flow", RunID: runID}).Header().Get(stepIDsHeader); got != `["step_a","step_b"]` {
		t.Fatalf("flow delivery names steps %q, want the two it ran", got)
	}
	// A step's own delivery is linked to its step by its message.
	stepID := worlds.StepID("step_a")
	handler.execute = func(ctx context.Context, _ worlds.QueueInput) error {
		recorder.RecordStepExecution(ctx, "step_a")
		return nil
	}
	if got := deliver(queueMessage{ID: "step", RunID: runID, StepID: &stepID}).Header().Get(stepIDsHeader); got != "" {
		t.Fatalf("step delivery names steps %q, want none", got)
	}
	// No more than ten are named.
	handler.execute = func(ctx context.Context, _ worlds.QueueInput) error {
		for index := range 2 * maxStepIDs {
			recorder.RecordStepExecution(ctx, worlds.StepID("step_"+string(rune('a'+index))))
			recorder.RecordStepExecution(ctx, "step_a")
		}
		return nil
	}
	var named []string
	_ = json.Unmarshal([]byte(deliver(queueMessage{ID: "flow", RunID: runID}).Header().Get(stepIDsHeader)), &named)
	if len(named) != maxStepIDs {
		t.Fatalf("flow delivery names %d steps, want %d", len(named), maxStepIDs)
	}
}

type namedStepInput struct{}
type namedStepOutput struct{}

func (namedStepInput) Workflow(namedStepOutput) {}

func TestFlowDeliveryNamesTheInlineStepTheRuntimeRan(t *testing.T) {
	all, queue, _ := newWorlds(t, 1)
	world := all[0]
	consumer := Queue(world, core.Runners(workflow.Use(func(ctx context.Context, _ namedStepInput) (namedStepOutput, error) {
		_, err := workflow.InlineStep(ctx, func(context.Context, struct{}) (struct{}, error) { return struct{}{}, nil }, struct{}{})
		return namedStepOutput{}, err
	})))
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), 10*time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, namedStepInput{})
	if err != nil {
		t.Fatal(err)
	}
	// The run's first delivery runs the step itself.
	var named string
	for named == "" && ctx.Err() == nil {
		message, request := queue.next()
		if message == nil {
			time.Sleep(5 * time.Millisecond)
			continue
		}
		response := httptest.NewRecorder()
		consumer.ServeHTTP(response, request.WithContext(ctx))
		queue.pushed(message)
		named = response.Header().Get(stepIDsHeader)
	}
	steps, err := world.StepsList(ctx, worlds.StepsListInput{RunID: worlds.RunID(run.ID())})
	if err != nil || len(steps.Data) != 1 {
		t.Fatalf("steps = %+v, %v", steps, err)
	}
	if want := `["` + string(steps.Data[0].StepID) + `"]`; named != want {
		t.Fatalf("flow delivery names %q, want %s", named, want)
	}
}
