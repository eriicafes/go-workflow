package vercelworld

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/workflowtest"
	"github.com/eriicafes/go-workflow/worlds"
)

// fakeVQS accepts publishes like the Vercel Queues API.
type fakeVQS struct {
	mu       sync.Mutex
	requests []*http.Request
	bodies   [][]byte
	status   int
}

func (f *fakeVQS) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	body, _ := io.ReadAll(request.Body)
	f.mu.Lock()
	f.requests = append(f.requests, request)
	f.bodies = append(f.bodies, body)
	status := f.status
	f.mu.Unlock()
	if status == 0 {
		status = http.StatusCreated
	}
	writer.WriteHeader(status)
	_, _ = writer.Write([]byte(`{"messageId":"msg_test"}`))
}

func (f *fakeVQS) published() [][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]byte(nil), f.bodies...)
}

// newWorlds returns count Worlds sharing one fake backend, as instances of
// one deployment would, with a fake Vercel Queues API.
func newWorlds(t *testing.T, count int) ([]*World, *fakeVQS, *fakeBackend) {
	t.Helper()
	backend := newFakeBackend(t)
	queue := &fakeVQS{}
	server := httptest.NewServer(queue)
	t.Cleanup(server.Close)
	result := make([]*World, count)
	for index := range result {
		world, err := New(Config{
			BaseURL:    "http://localhost/v1/workflow",
			QueueURL:   server.URL + "/api/v3",
			Topic:      "workflow",
			OIDCToken:  staticOIDCToken("test-oidc"),
			BackendURL: backend.server.URL,
		})
		if err != nil {
			t.Fatal(err)
		}
		result[index] = world
	}
	return result, queue, backend
}

func TestConformance(t *testing.T) {
	workflowtest.Conformance(t, func(t *testing.T) worlds.World {
		all, _, _ := newWorlds(t, 1)
		return all[0]
	})
}

func TestConformanceShared(t *testing.T) {
	workflowtest.ConformanceShared(t, func(t *testing.T) (worlds.World, worlds.World) {
		all, _, _ := newWorlds(t, 2)
		return all[0], all[1]
	})
}

func TestNewValidatesConfig(t *testing.T) {
	valid := Config{BaseURL: "https://example.com/v1/workflow", Topic: "workflow"}
	for name, mutate := range map[string]func(*Config){
		"missing base URL":      func(c *Config) { c.BaseURL = "" },
		"relative base URL":     func(c *Config) { c.BaseURL = "/v1/workflow" },
		"missing topic":         func(c *Config) { c.Topic = "" },
		"token without project": func(c *Config) { c.Token = "token" },
		"route without topic": func(c *Config) {
			c.Routes = map[string]string{"example.com/a.Workflow": "backfill"}
		},
	} {
		config := valid
		mutate(&config)
		if _, err := New(config); err == nil {
			t.Errorf("%s: New succeeded", name)
		}
	}
	if _, err := New(valid); err != nil {
		t.Fatal(err)
	}
}

func TestNewDefaults(t *testing.T) {
	t.Setenv("VERCEL_REGION", "sfo1")
	world, err := New(Config{BaseURL: "https://example.com/v1/workflow", Topic: "workflow"})
	if err != nil {
		t.Fatal(err)
	}
	if queue, header := world.queueURL(envRegion()); queue != "https://sfo1.vercel-queue.com/api/v3" || header != nil || world.api.baseURL != DefaultBackendURL || world.api.proxy {
		t.Fatalf("defaults = queue %q backend %q proxy %v", queue, world.api.baseURL, world.api.proxy)
	}
	proxied, err := New(Config{BaseURL: "https://example.com/v1/workflow", Topic: "workflow", Token: "token", ProjectID: "prj_1", TeamID: "team_1"})
	if err != nil {
		t.Fatal(err)
	}
	if proxied.api.baseURL != DefaultProxyURL || !proxied.api.proxy || proxied.api.env != "production" {
		t.Fatalf("proxy defaults = backend %q proxy %v env %q", proxied.api.baseURL, proxied.api.proxy, proxied.api.env)
	}
}

// TestProxyCredentials checks the headers a World sends through Vercel's API
// proxy with an access token.
func TestProxyCredentials(t *testing.T) {
	var header http.Header
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		header = request.Header.Clone()
		writer.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(server.Close)
	world, err := New(Config{BaseURL: "https://example.com/v1/workflow", Topic: "workflow", Token: "vercel-token", ProjectID: "prj_1", TeamID: "team_1", Environment: "preview", BackendURL: server.URL, OIDCToken: func(context.Context) (string, error) {
		t.Fatal("OIDC token requested through the proxy")
		return "", nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = world.StepsGet(context.Background(), worlds.StepsGetInput{RunID: "wrun_1", StepID: "step_1"})
	if header.Get("Authorization") != "Bearer vercel-token" || header.Get("X-Vercel-Project-Id") != "prj_1" || header.Get("X-Vercel-Team-Id") != "team_1" || header.Get("X-Vercel-Environment") != "preview" || header.Get("X-Vercel-Trusted-Oidc-Idp-Token") != "" {
		t.Fatalf("proxy headers = %v", header)
	}
}

func staticOIDCToken(token string) func(context.Context) (string, error) {
	return func(context.Context) (string, error) { return token, nil }
}

// waitFor polls condition until it holds or a second passes.
func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	for deadline := time.Now().Add(time.Second); !condition(); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("condition not met")
		}
	}
}

func TestRetention(t *testing.T) {
	ctx := context.Background()
	all, _, _ := newWorlds(t, 1)
	world := all[0]
	world.config.Retention = worlds.RetainNone
	world.config.Retentions = Retentions(WorkflowRetention{Workflow: "kept", Retention: worlds.RetainDefault})
	for _, test := range []struct {
		runID, workflow string
		attributes      map[string]string
		want            string
	}{
		{"wrun_01KDELETED0000000000000000", "deleted", nil, worlds.RetainNone.String()},
		{"wrun_01KKEPT000000000000000000", "kept", nil, ""},
		{"wrun_01KOVERRIDDEN0000000000000", "deleted", map[string]string{worlds.RetentionAttribute: "default"}, "default"},
	} {
		_, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: worlds.RunID(test.runID), WorkflowName: test.workflow, Attributes: test.attributes})
		if err != nil {
			t.Fatal(err)
		}
		run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: worlds.RunID(test.runID)})
		if err != nil {
			t.Fatal(err)
		}
		if got := run.Attributes[worlds.RetentionAttribute]; got != test.want {
			t.Errorf("%s: %s = %q, want %q", test.workflow, worlds.RetentionAttribute, got, test.want)
		}
	}
}

func TestRunFailedKeepsItsErrorCodeAndRequestID(t *testing.T) {
	all, _, backend := newWorlds(t, 1)
	world := all[0]
	ctx := context.WithValue(context.Background(), requestIDKey{}, "iad1::abc-123")
	runID, err := worlds.CreateRun(ctx, world, worlds.ClientRunsCreateInput{EventsCreateRunInput: worlds.EventsCreateRunInput{WorkflowName: "wf"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeRunStarted}}); err != nil {
		t.Fatal(err)
	}
	data, err := worlds.EncodeRunFailed(worlds.ErrMaxDeliveriesExceeded, worlds.PayloadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeRunFailed, Data: data}}); err != nil {
		t.Fatal(err)
	}
	events, err := world.EventsList(ctx, worlds.EventsListInput{RunID: runID})
	if err != nil {
		t.Fatal(err)
	}
	last := events.Data[len(events.Data)-1]
	var failed worlds.DataRunFailed
	if err := json.Unmarshal(last.Data, &failed); err != nil || failed.ErrorCode != "MAX_DELIVERIES_EXCEEDED" {
		t.Fatalf("run_failed data = %s, err=%v", last.Data, err)
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if len(backend.vercelIDs) < 2 || backend.vercelIDs[len(backend.vercelIDs)-1] != "iad1::abc-123" {
		t.Fatalf("vercelIds = %q", backend.vercelIDs)
	}
}

func TestRunsCreatedInARegionAreRoutedThere(t *testing.T) {
	all, queue, _ := newWorlds(t, 1)
	world := all[0]
	ctx := context.Background()
	client := worlds.NewClient(world)
	runID, err := client.RunsNewID(ctx, worlds.ClientRunsNewIDInput{Region: RegionSYD1})
	if err != nil {
		t.Fatal(err)
	}
	if region, ok := world.RegionForRunID(runID); !ok || region != RegionSYD1 {
		t.Fatalf("run %s region = %s, %t", runID, region, ok)
	}
	if _, err := client.RunsCreate(ctx, worlds.ClientRunsCreateInput{EventsCreateRunInput: worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "wf"}}); err != nil {
		t.Fatal(err)
	}
	if region := messageRegion(runID); region != RegionSYD1 {
		t.Fatalf("first delivery routed to %s", region)
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if len(queue.requests) != 1 {
		t.Fatalf("first delivery published %d times", len(queue.requests))
	}
}
