package workflow_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/worlds/testworld"
)

type metadataInput struct{}
type metadataOutput struct {
	Workflow workflow.WorkflowMetadata
	Step     workflow.StepMetadata
	StepID   string
}

func (metadataInput) Workflow(metadataOutput) {}

func TestMiddlewareAddsClientToRequestContext(t *testing.T) {
	world := testworld.New()
	defer world.Close()
	handler := workflow.Middleware(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if _, err := workflow.Start(request.Context(), metadataInput{}); err != nil {
			t.Errorf("Start = %v", err)
			writer.WriteHeader(http.StatusInternalServerError)
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	}), workflow.NewClient(world))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/", nil))
	if response.Code != http.StatusNoContent {
		t.Fatalf("status = %d", response.Code)
	}
}

func TestWorkflowAndStepMetadataAreAvailableDuringExecution(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, _ metadataInput) (metadataOutput, error) {
		value, err := workflow.Step(ctx, func(ctx context.Context, _ struct{}) (metadataOutput, error) {
			return metadataOutput{Step: workflow.GetStepMetadata(ctx), StepID: workflow.StepID(ctx)}, nil
		}, struct{}{})
		if err != nil {
			return metadataOutput{}, err
		}
		value.Workflow = workflow.GetWorkflowMetadata(ctx)
		return value, nil
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, metadataInput{})
	if err != nil {
		t.Fatal(err)
	}
	output, err := run.Value(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if output.Workflow.Name == "" || output.Workflow.RunID != run.ID() || output.Workflow.StartedAt.IsZero() {
		t.Fatalf("workflow metadata = %#v", output.Workflow)
	}
	if output.Step.ID == "" || output.Step.ID != output.StepID || output.Step.Name == "" || output.Step.StartedAt.IsZero() || output.Step.Attempt != 1 {
		t.Fatalf("step metadata = %#v, step ID = %q", output.Step, output.StepID)
	}
}
