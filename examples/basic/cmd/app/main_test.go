package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/examples/basic/flows"
	"github.com/eriicafes/go-workflow/examples/basic/workflows"
	"github.com/eriicafes/go-workflow/worlds/testworld"
)

func TestReportWorkflows(t *testing.T) {
	world := testworld.New(
		workflow.Use(flows.GenerateReport),
		workflow.Use(flows.PublishReport),
	)
	defer world.Close()

	handler := workflow.Middleware(routes(), workflow.NewClient(world))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	reportID := startWorkflow(t, handler, "/reports")
	run, err := workflow.Get[workflows.GenerateReportResult](workflow.NewContext(ctx, workflow.NewClient(world)), reportID)
	if err != nil {
		t.Fatal(err)
	}
	output, err := run.Await(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if output.Summary != "source for durable workflows, source for durable workflows examples" {
		t.Fatalf("summary = %q", output.Summary)
	}

	publishID := startWorkflow(t, handler, "/reports/publish")
	publish, err := workflow.Get[workflows.PublishReportResult](workflow.NewContext(ctx, workflow.NewClient(world)), publishID)
	if err != nil {
		t.Fatal(err)
	}
	published, err := publish.Await(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if published.Report != output {
		t.Fatalf("published report = %#v, want %#v", published.Report, output)
	}
	if published.Location != "reports/"+output.Summary {
		t.Fatalf("published location = %q", published.Location)
	}
}

func startWorkflow(
	t *testing.T,
	handler http.Handler,
	path string,
) string {
	t.Helper()
	request := httptest.NewRequest(
		http.MethodPost,
		path,
		strings.NewReader(`{"topic":"durable workflows"}`),
	)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d: %s", recorder.Code, http.StatusAccepted, recorder.Body)
	}
	var response struct {
		RunID string `json:"runId"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if response.RunID == "" {
		t.Fatal("response did not include a run ID")
	}
	return response.RunID
}
