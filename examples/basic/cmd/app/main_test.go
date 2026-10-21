package main

import (
	"context"
	"io"
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ctx = workflow.NewContext(ctx, workflow.NewClient(world))

	reportID := post(t, handler, "/reports", url.Values{"topic": {"durable workflows"}}, "/reports/")
	run, err := workflow.Get[workflows.GenerateReportResult](ctx, reportID)
	if err != nil {
		t.Fatal(err)
	}
	output, err := run.Value(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(output.Sources) != 6 || len(output.Sections) != 3 {
		t.Fatalf("report has %d sources and %d sections", len(output.Sources), len(output.Sections))
	}
	if output.Summary != "durable workflows report covering Background, Findings, Outlook" {
		t.Fatalf("summary = %q", output.Summary)
	}
	if progress := get(t, handler, "/runs/"+reportID+"/progress"); !strings.HasSuffix(progress, "report ready\n") {
		t.Fatalf("progress = %q", progress)
	}

	publishID := post(t, handler, "/reports/publish", url.Values{"topic": {"durable workflows"}}, "/reports/publish/")
	review(t, ctx, handler, publishID, url.Values{"reviewer": {"ada"}, "decision": {"changes"}, "comment": {"add costs"}})
	review(t, ctx, handler, publishID, url.Values{"reviewer": {"ada"}, "decision": {"approve"}})

	publish, err := workflow.Get[workflows.PublishReportResult](ctx, publishID)
	if err != nil {
		t.Fatal(err)
	}
	published, err := publish.Value(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(published.Reviews) != 2 || published.Reviews[0].Approved || !published.Reviews[1].Approved {
		t.Fatalf("reviews = %#v", published.Reviews)
	}
	if !strings.HasPrefix(published.Draft, "# durable workflows") || !strings.HasSuffix(published.Draft, "Revision: add costs") {
		t.Fatalf("published draft = %q", published.Draft)
	}
	if !strings.HasPrefix(published.Location, "reports/") {
		t.Fatalf("published location = %q", published.Location)
	}
	progress := get(t, handler, "/runs/"+publishID+"/progress")
	for _, line := range []string{"awaiting review", "ada requested changes: add costs", "ada approved", "published to " + published.Location} {
		if !strings.Contains(progress, line) {
			t.Fatalf("progress is missing %q:\n%s", line, progress)
		}
	}
}

// review posts a review once the run is waiting for one. The run registers its
// review hook only after research, and again after each revision.
func review(t *testing.T, ctx context.Context, handler http.Handler, runID string, form url.Values) {
	t.Helper()
	for {
		recorder := serve(handler, http.MethodPost, "/runs/"+runID+"/review", form)
		if recorder.Code == http.StatusSeeOther {
			return
		}
		if recorder.Code != http.StatusConflict {
			t.Fatalf("review status = %d: %s", recorder.Code, recorder.Body)
		}
		select {
		case <-ctx.Done():
			t.Fatal("run never waited for a review")
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func post(t *testing.T, handler http.Handler, path string, form url.Values, redirectPrefix string) string {
	t.Helper()
	recorder := serve(handler, http.MethodPost, path, form)
	if recorder.Code != http.StatusSeeOther {
		t.Fatalf("status = %d, want %d: %s", recorder.Code, http.StatusSeeOther, recorder.Body)
	}
	location := recorder.Header().Get("Location")
	runID, ok := strings.CutPrefix(location, redirectPrefix)
	if !ok || runID == "" {
		t.Fatalf("redirect location = %q, want %s{runID}", location, redirectPrefix)
	}
	return runID
}

func get(t *testing.T, handler http.Handler, path string) string {
	t.Helper()
	recorder := serve(handler, http.MethodGet, path, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("GET %s status = %d: %s", path, recorder.Code, recorder.Body)
	}
	body, err := io.ReadAll(recorder.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func serve(handler http.Handler, method, path string, form url.Values) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, strings.NewReader(form.Encode()))
	if form != nil {
		request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}
