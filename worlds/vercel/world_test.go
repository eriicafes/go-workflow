package vercelworld

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/workflowtest"
	"github.com/eriicafes/go-workflow/worlds"
)

// fakeVQS plays the Vercel Queues API: it stores published messages, one per
// idempotency key, and serves their leases, so deliverAll can push them to a
// consumer. A message with a concurrency key that becomes visible while
// another message of the key is being delivered is coalesced into that
// delivery, whose acknowledgment must then cover it. status, when set, is the
// answer to every request instead.
type fakeVQS struct {
	mu       sync.Mutex
	requests []*http.Request
	bodies   [][]byte
	status   int
	messages []*fakeMessage
	sequence int
}

// fakeMessage is a message fakeVQS stores.
type fakeMessage struct {
	id, topic, idempotencyKey, concurrencyKey string
	body                                      []byte
	visibleAt                                 time.Time
	deliveries, handoffs                      int
	receipt                                   string
	// createdAt is when the message was published.
	createdAt time.Time
	// coalesced counts the messages coalesced into this one.
	coalesced int
	// pushing is set while a delivery of the message is being pushed.
	pushing, done bool
}

func (f *fakeVQS) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	body, _ := io.ReadAll(request.Body)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, request)
	f.bodies = append(f.bodies, body)
	if f.status != 0 {
		writer.WriteHeader(f.status)
		_, _ = writer.Write([]byte(`{"messageId":"msg_test"}`))
		return
	}
	path := request.URL.Path
	// publish stores a message, one per idempotency key, and returns its ID.
	publish := func(topic string, header http.Header, body []byte) string {
		key := header.Get("Vqs-Idempotency-Key")
		if key != "" {
			for _, message := range f.messages {
				if message.idempotencyKey == key {
					return message.id
				}
			}
		}
		f.sequence++
		message := &fakeMessage{id: fmt.Sprintf("msg_%d", f.sequence), topic: topic, idempotencyKey: key, concurrencyKey: header.Get("Vqs-Experimental-Concurrency-Key"), body: body, visibleAt: time.Now(), createdAt: time.Now().UTC()}
		if seconds, err := strconv.Atoi(header.Get("Vqs-Delay-Seconds")); err == nil {
			message.visibleAt = message.visibleAt.Add(time.Duration(seconds) * time.Second)
		}
		f.messages = append(f.messages, message)
		return message.id
	}
	switch {
	case request.Method == http.MethodPost && strings.HasPrefix(path, "/api/v3/topic/") && strings.HasSuffix(path, "/batch"):
		// A batch send: a multipart/mixed body with a part per message, whose
		// headers are the message's, answered with a result per part.
		_, params, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
		if err != nil || params["boundary"] == "" {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		topic := strings.TrimSuffix(strings.TrimPrefix(path, "/api/v3/topic/"), "/batch")
		results := []map[string]any{}
		reader := multipart.NewReader(bytes.NewReader(body), params["boundary"])
		for {
			part, err := reader.NextRawPart()
			if err != nil {
				break
			}
			data, _ := io.ReadAll(part)
			results = append(results, map[string]any{"status": "sent", "messageId": publish(topic, http.Header(part.Header), data)})
		}
		if len(results) < 1 || len(results) > 100 {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		writer.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(writer).Encode(map[string]any{"results": results})
	case request.Method == http.MethodPost && strings.HasPrefix(path, "/api/v3/topic/") && !strings.Contains(path, "/consumer/"):
		writer.WriteHeader(http.StatusCreated)
		_, _ = fmt.Fprintf(writer, `{"messageId":%q}`, publish(strings.TrimPrefix(path, "/api/v3/topic/"), request.Header, body))
	case strings.Contains(path, "/lease/"):
		message := f.leased(path[strings.LastIndex(path, "/")+1:])
		if message == nil {
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		if request.Method == http.MethodDelete {
			if covered, _ := strconv.Atoi(request.Header.Get("Vqs-Experimental-If-Coalesced")); message.coalesced > covered {
				writer.Header().Set("Vqs-Error-Code", "concurrency_coalesced_pending")
				writer.Header().Set("Vqs-Experimental-Coalesced", strconv.Itoa(message.coalesced))
				writer.WriteHeader(http.StatusConflict)
				return
			}
			message.done = true
			writer.WriteHeader(http.StatusNoContent)
			return
		}
		var visibility struct {
			VisibilityTimeoutSeconds int64 `json:"visibilityTimeoutSeconds"`
		}
		_ = json.Unmarshal(body, &visibility)
		message.visibleAt = time.Now().Add(time.Duration(visibility.VisibilityTimeoutSeconds) * time.Second)
		if request.Header.Get("Vqs-Experimental-Handoff") == "1" && visibility.VisibilityTimeoutSeconds == 0 {
			message.handoffs++
			writer.Header().Set("Vqs-Experimental-Handoff", "accepted")
		}
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte(`{"success":true}`))
	default:
		writer.WriteHeader(http.StatusNotFound)
	}
}

// leased returns the message whose current lease is receipt.
func (f *fakeVQS) leased(receipt string) *fakeMessage {
	for _, message := range f.messages {
		if message.receipt == receipt && !message.done {
			return message
		}
	}
	return nil
}

// next leases the first message that is visible, coalescing a keyed one into
// its key's delivery, and returns the push request of its delivery, or nil
// when no message is visible.
func (f *fakeVQS) next() (*fakeMessage, *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := time.Now()
	for _, message := range f.messages {
		if message.done || message.pushing || message.visibleAt.After(now) {
			continue
		}
		if holder := f.pushing(message.concurrencyKey); holder != nil {
			holder.coalesced++
			message.done = true
			continue
		}
		message.deliveries++
		message.receipt = fmt.Sprintf("rh_%s_%d", message.id, message.deliveries)
		message.visibleAt = now.Add(vqsLease)
		message.pushing = true
		request := httptest.NewRequest(http.MethodPost, "/queue", bytes.NewReader(message.body))
		request.Header.Set("Ce-Type", "com.vercel.queue.v2beta")
		request.Header.Set("Ce-Vqsqueuename", message.topic)
		request.Header.Set("Ce-Vqsconsumergroup", "api-queue")
		request.Header.Set("Ce-Vqsmessageid", message.id)
		request.Header.Set("Ce-Vqsreceipthandle", message.receipt)
		request.Header.Set("Ce-Vqsdeliverycount", strconv.Itoa(message.deliveries))
		request.Header.Set("Ce-Vqscreatedat", message.createdAt.Format(time.RFC3339Nano))
		if message.handoffs > 0 {
			request.Header.Set("Ce-Vqsexperimentalhandoffcount", strconv.Itoa(message.handoffs))
		}
		request.Header.Set("Content-Type", "application/json")
		return message, request
	}
	return nil, nil
}

// pushing returns the message of key whose delivery is being pushed.
func (f *fakeVQS) pushing(key string) *fakeMessage {
	if key == "" {
		return nil
	}
	for _, message := range f.messages {
		if message.pushing && message.concurrencyKey == key {
			return message
		}
	}
	return nil
}

// pushed ends the push of message's delivery.
func (f *fakeVQS) pushed(message *fakeMessage) {
	f.mu.Lock()
	defer f.mu.Unlock()
	message.pushing = false
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

func TestConformanceDelivery(t *testing.T) {
	workflowtest.ConformanceDelivery(t, func(t *testing.T, executor worlds.Executor) worlds.World {
		all, queue, _ := newWorlds(t, 1)
		ctx, stop := context.WithCancel(context.Background())
		stopped := make(chan struct{})
		go func() {
			defer close(stopped)
			deliverAll(ctx, t, queue, Queue(all[0], executor))
		}()
		t.Cleanup(func() {
			stop()
			<-stopped
		})
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
	if queue, header := proxied.queueURL(RegionSFO1); queue != DefaultProxyURL+"/queues-proxy/api/v3" || header.Get("X-Vercel-Queue-Region") != "sfo1" || header.Get("X-Vercel-Project-Id") != "prj_1" {
		t.Fatalf("proxy queue = %q, %v", queue, header)
	}
}

func TestNewReadsTheBackendFromTheEnvironment(t *testing.T) {
	t.Setenv("VERCEL_WORKFLOW_SERVER_URL", "https://backend.example.com/")
	t.Setenv("WORKFLOW_VERCEL_BACKEND_URL", "https://proxy.example.com/v1/workflow")
	config := Config{BaseURL: "https://example.com/v1/workflow", Topic: "workflow"}
	direct, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	// Without a token the backend's host is used directly.
	if direct.api.baseURL != "https://backend.example.com/api" || direct.api.forwardTo != "" {
		t.Fatalf("direct backend = %q, forwarded to %q", direct.api.baseURL, direct.api.forwardTo)
	}
	// With one, requests go to the proxy named, which is told the backend.
	config.Token, config.ProjectID = "token", "prj_1"
	proxied, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	header := http.Header{}
	if err := proxied.api.authorize(context.Background(), header); err != nil {
		t.Fatal(err)
	}
	if proxied.api.baseURL != "https://proxy.example.com/v1/workflow" || header.Get("X-Vercel-Workflow-Api-Url") != "https://backend.example.com/" {
		t.Fatalf("proxied backend = %q, headers %v", proxied.api.baseURL, header)
	}
	// BackendURL is the caller's choice over either.
	config.BackendURL = "https://chosen.example.com"
	chosen, err := New(config)
	if err != nil || chosen.api.baseURL != "https://chosen.example.com" {
		t.Fatalf("chosen backend = %q, %v", chosen.api.baseURL, err)
	}
}

func TestNewReadsTheStreamsTransport(t *testing.T) {
	for value, overHTTP := range map[string]bool{"": false, "http": true, " HTTP ": true, "ws": false, "https": false} {
		t.Setenv("WORKFLOW_STREAMS_TRANSPORT", value)
		world, err := New(Config{BaseURL: "https://example.com/v1/workflow", Topic: "workflow"})
		if err != nil || world.streamsOverHTTP != overHTTP {
			t.Errorf("WORKFLOW_STREAMS_TRANSPORT=%q: over HTTP %v, %v, want %v", value, world.streamsOverHTTP, err, overHTTP)
		}
	}
}

func TestUserAgentNamesTheClientAndDeployment(t *testing.T) {
	world, err := New(Config{BaseURL: "https://example.com/v1/workflow", Topic: "workflow", DeploymentID: "dpl_1"})
	if err != nil {
		t.Fatal(err)
	}
	want := "go-workflow/vercelworld " + runtime.Version() + " " + runtime.GOOS + " (" + runtime.GOARCH + ") dpl_1"
	if world.api.userAgent != want {
		t.Fatalf("user agent = %q, want %q", world.api.userAgent, want)
	}
}

// TestProxyCredentials checks the headers a World sends through Vercel's API
// proxy with an access token.
func TestProxyCredentials(t *testing.T) {
	var header http.Header
	var path string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		header, path = request.Header.Clone(), request.URL.Path
		writer.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(server.Close)
	world, err := New(Config{BaseURL: "https://example.com/v1/workflow", Topic: "workflow", Token: "vercel-token", ProjectID: "prj_1", TeamID: "team_1", Environment: "preview", BackendURL: server.URL + "/v1/workflow", OIDCToken: func(context.Context) (string, error) {
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
	// A queue send goes through the proxy too, with the same credentials and
	// the region to forward it to.
	_, _ = world.Queue(context.Background(), worlds.QueueInput{RunID: "wrun_1", WorkflowName: "w"})
	if path != "/v1/workflow/queues-proxy/api/v3/topic/workflow" || header.Get("Authorization") != "Bearer vercel-token" || header.Get("X-Vercel-Project-Id") != "prj_1" || header.Get("X-Vercel-Team-Id") != "team_1" || header.Get("X-Vercel-Environment") != "preview" || header.Get("X-Vercel-Queue-Region") == "" {
		t.Fatalf("proxied queue send = %s with %v", path, header)
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
		{string(worlds.NewRunID()), "deleted", nil, worlds.RetainNone.String()},
		{string(worlds.NewRunID()), "kept", nil, ""},
		{string(worlds.NewRunID()), "deleted", map[string]string{worlds.RetentionAttribute: "default"}, "default"},
	} {
		_, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: worlds.RunID(test.runID), WorkflowName: test.workflow, Attributes: test.attributes, AllowReservedAttributes: true})
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

// The backend filters runs by one status, so RunsList and the analytics
// refuse several, and analytics refuse a window with one bound.
func TestRunFilterRefusals(t *testing.T) {
	worldsList, _, _ := newWorlds(t, 1)
	world, ctx := worldsList[0], context.Background()
	statuses := []worlds.Status{worlds.StatusRunning, worlds.StatusCompleted}
	if _, err := world.RunsList(ctx, worlds.RunsListInput{Status: statuses}); !errors.Is(err, worlds.ErrEventInvalid) {
		t.Fatalf("RunsList by two statuses: %v", err)
	}
	if _, err := world.Analytics().RunsList(ctx, worlds.AnalyticsRunsListInput{Status: statuses}); !errors.Is(err, worlds.ErrEventInvalid) {
		t.Fatalf("analytics RunsList by two statuses: %v", err)
	}
	start := time.Now()
	if _, err := world.Analytics().RunsList(ctx, worlds.AnalyticsRunsListInput{StartTime: &start}); !errors.Is(err, worlds.ErrEventInvalid) {
		t.Fatalf("analytics RunsList with one bound: %v", err)
	}
}

func TestNewRejectsARouteWithoutATopic(t *testing.T) {
	_, err := New(Config{
		BaseURL:  "https://app.example.com/v1/workflow",
		QueueURL: "https://iad1.vercel-queue.com/api/v3",
		Topic:    "workflow",
		Routes:   worlds.Routes(worlds.Route[routedWorkflow]("backfill")),
	})
	if err == nil || !strings.Contains(err.Error(), "has no topic") {
		t.Fatalf("New = %v, want an error for the queue without a topic", err)
	}
}
