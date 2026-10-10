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
	"github.com/eriicafes/go-workflow/examples/basic/workflowdefs"
	"github.com/eriicafes/go-workflow/workflowtest"
)

func TestReportWorkflows(t *testing.T) {
	world := workflowtest.New(
		workflow.Use(flows.GenerateReport),
		workflow.Use(flows.PublishReport),
	)
	defer world.Close()

	server := app{workflows: world}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	reportID := startWorkflow(t, server.createReport, "/reports")
	raw, err := world.WaitRun(ctx, reportID)
	if err != nil {
		t.Fatal(err)
	}
	var output workflowdefs.GenerateReportResult
	if err := json.Unmarshal(raw, &output); err != nil {
		t.Fatal(err)
	}
	if output.Summary != "source for durable workflows, source for durable workflows examples" {
		t.Fatalf("summary = %q", output.Summary)
	}

	publishID := startWorkflow(t, server.publishReport, "/reports/publish")
	raw, err = world.WaitRun(ctx, publishID)
	if err != nil {
		t.Fatal(err)
	}
	var published workflowdefs.PublishReportResult
	if err := json.Unmarshal(raw, &published); err != nil {
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
	handler http.HandlerFunc,
	path string,
) string {
	t.Helper()
	request := httptest.NewRequest(
		http.MethodPost,
		path,
		strings.NewReader(`{"topic":"durable workflows"}`),
	)
	recorder := httptest.NewRecorder()
	handler(recorder, request)
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
