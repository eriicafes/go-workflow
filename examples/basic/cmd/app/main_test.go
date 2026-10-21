package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
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
	output, err := run.Value(ctx)
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
	published, err := publish.Value(ctx)
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
		strings.NewReader(url.Values{"topic": {"durable workflows"}}.Encode()),
	)
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusSeeOther {
		t.Fatalf("status = %d, want %d: %s", recorder.Code, http.StatusSeeOther, recorder.Body)
	}
	location := recorder.Header().Get("Location")
	runID, ok := strings.CutPrefix(location, path+"/")
	if !ok || runID == "" {
		t.Fatalf("redirect location = %q, want %s/{runID}", location, path)
	}
	return runID
}
