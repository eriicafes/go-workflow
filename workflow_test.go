package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow"
)

type input struct{ Value string }
type output struct{ Value string }

func (input) Workflow(output) {}

func TestStartRequiresClientContext(t *testing.T) {
	_, err := workflow.Start(context.Background(), input{})
	if !errors.Is(err, workflow.ErrNoClient) {
		t.Fatalf("error = %v, want ErrNoClient", err)
	}
}

func TestRunnersDeduplicatesIdentities(t *testing.T) {
	var unnamed workflow.Runner
	runners := workflow.Runners(
		unnamed,
		workflow.Use(func(context.Context, input) (output, error) { return output{}, nil }),
		workflow.Use(func(context.Context, input) (output, error) { return output{}, nil }),
	)
	if len(runners) != 1 {
		t.Fatalf("runner count = %d, want 1", len(runners))
	}
	if _, exists := runners[""]; exists {
		t.Fatal("unnamed runner was not skipped")
	}
}

func TestStartUsesWorkerHTTPClient(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/.well-known/workflow/v1/runs":
			if request.Method != http.MethodPost {
				http.Error(writer, "expected POST", http.StatusMethodNotAllowed)
				return
			}
			var received map[string]json.RawMessage
			if err := json.NewDecoder(request.Body).Decode(&received); err != nil {
				t.Fatal(err)
			}
			var identity string
			if err := json.Unmarshal(received["identity"], &identity); err != nil {
				t.Fatal(err)
			}
			if len(received) != 2 ||
				identity == "" ||
				string(received["input"]) != `{"Value":"input"}` {
				http.Error(writer, "unexpected input", http.StatusBadRequest)
				return
			}
			_, _ = writer.Write([]byte(`{"id":"run_1"}`))
		case "/.well-known/workflow/v1/runs/run_1":
			_, _ = writer.Write([]byte(`{"output":{"Value":"done"}}`))
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	client := workflow.NewHTTPClient(server.URL)
	run, err := workflow.Start(workflow.NewContext(context.Background(), client), input{Value: "input"})
	if err != nil {
		t.Fatal(err)
	}
	if run.ID() != "run_1" {
		t.Fatalf("run ID = %q", run.ID())
	}
	value, err := run.Value(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if value.Value != "done" {
		t.Fatalf("value = %#v", value)
	}
}

func TestDurablePrimitivesRequireWorkflowContext(t *testing.T) {
	ctx := context.Background()
	_, err := workflow.Step(ctx, func(context.Context, string) (string, error) { return "", nil }, "input")
	if !errors.Is(err, workflow.ErrNoWorkflow) {
		t.Fatalf("Step error = %v", err)
	}
	if err := workflow.Sleep(ctx, time.Millisecond); !errors.Is(err, workflow.ErrNoWorkflow) {
		t.Fatalf("Sleep error = %v", err)
	}
	if _, err := workflow.Background(ctx, func(context.Context, string) (string, error) { return "", nil }, "input").Value(ctx); !errors.Is(err, workflow.ErrNoWorkflow) {
		t.Fatalf("Background error = %v", err)
	}
	if _, err := workflow.Writer(ctx).Write([]byte("log")); !errors.Is(err, workflow.ErrNoWorkflow) {
		t.Fatalf("Writer error = %v", err)
	}
}
