package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/testworld"
)

type attributesInput struct{}
type attributesOutput struct{ Reserved bool }

func (attributesInput) Workflow(attributesOutput) {}

func ptrTo(value string) *string { return &value }

func tagFromStep(ctx context.Context, value string) (struct{}, error) {
	return struct{}{}, workflow.SetAttributes(ctx, map[string]*string{"step": &value})
}

func TestSetAttributesRecordsWritesInWorkflowAndStepContext(t *testing.T) {
	world := testworld.New(worlds.Runners(workflow.Use(func(ctx context.Context, _ attributesInput) (attributesOutput, error) {
		if err := workflow.SetAttributes(ctx, map[string]*string{"phase": ptrTo("init"), "order": ptrTo("ord_1")}); err != nil {
			return attributesOutput{}, err
		}
		if _, err := workflow.Step(ctx, tagFromStep, "tagged"); err != nil {
			return attributesOutput{}, err
		}
		if err := workflow.SetAttributes(ctx, map[string]*string{"phase": ptrTo("done"), "order": nil}); err != nil {
			return attributesOutput{}, err
		}
		err := workflow.SetAttributes(ctx, map[string]*string{"$internal": ptrTo("x")})
		return attributesOutput{Reserved: errors.Is(err, worlds.ErrAttributeInvalid)}, nil
	})))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, attributesInput{})
	if err != nil {
		t.Fatal(err)
	}
	output, err := run.Value(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !output.Reserved {
		t.Fatal("a reserved key without AllowReservedAttributes was accepted")
	}
	stored, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: worlds.RunID(run.ID())})
	if err != nil {
		t.Fatal(err)
	}
	if len(stored.Attributes) != 2 || stored.Attributes["phase"] != "done" || stored.Attributes["step"] != "tagged" {
		t.Fatalf("attributes = %v", stored.Attributes)
	}
	events, err := world.EventsList(ctx, worlds.EventsListInput{RunID: worlds.RunID(run.ID())})
	if err != nil {
		t.Fatal(err)
	}
	var writes []string
	for _, event := range events.Data {
		if event.EventType != worlds.EventTypeAttrSet {
			continue
		}
		var data worlds.DataAttrSet
		if err := json.Unmarshal(event.Data, &data); err != nil {
			t.Fatal(err)
		}
		keys := make([]string, len(data.Changes))
		for index, change := range data.Changes {
			keys[index] = change.Key
		}
		switch data.Writer.Type {
		case worlds.AttributeWriterWorkflow:
			// Workflow writes carry a drawn ID, with changes in key order.
			if !strings.HasPrefix(event.CorrelationID, "attr_") {
				t.Fatalf("workflow write correlation ID = %q", event.CorrelationID)
			}
		case worlds.AttributeWriterStep:
			if event.CorrelationID != "" || data.Writer.StepID == "" || data.Writer.Attempt != 1 {
				t.Fatalf("step write = %+v correlation %q", data, event.CorrelationID)
			}
		}
		writes = append(writes, data.Writer.Type+":"+strings.Join(keys, ","))
	}
	if strings.Join(writes, " ") != "workflow:order,phase step:step workflow:order,phase" {
		t.Fatalf("attribute writes = %v", writes)
	}
	if err := workflow.SetAttributes(ctx, map[string]*string{"a": ptrTo("b")}); !errors.Is(err, workflow.ErrNoWorkflow) {
		t.Fatalf("SetAttributes outside a run = %v, want ErrNoWorkflow", err)
	}
}

func TestSetAttributesPastTheRunLimitFailsTheRun(t *testing.T) {
	batch := func(prefix string) map[string]*string {
		attributes := map[string]*string{}
		for index := range 40 {
			attributes[prefix+strconv.Itoa(index)] = ptrTo("x")
		}
		return attributes
	}
	world := testworld.New(worlds.Runners(workflow.Use(func(ctx context.Context, _ attributesInput) (attributesOutput, error) {
		if err := workflow.SetAttributes(ctx, batch("a")); err != nil {
			return attributesOutput{}, err
		}
		// Each write is within the limit, and only the World can tell that
		// together they pass it.
		return attributesOutput{}, workflow.SetAttributes(ctx, batch("b"))
	})))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), 5*time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, attributesInput{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Value(ctx); err == nil || !strings.Contains(err.Error(), "failed World validation") {
		t.Fatalf("run = %v, want it failed by the World's refusal", err)
	}
	stored, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: worlds.RunID(run.ID())})
	if err != nil || stored.Status != worlds.StatusFailed || stored.ErrorCode != "USER_ERROR" {
		t.Fatalf("run = %+v, %v, want a failed run with USER_ERROR", stored, err)
	}
}
