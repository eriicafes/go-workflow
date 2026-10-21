package worlds_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/testworld"
)

type clientInput struct{ Value int }
type clientOutput struct{ Value int }

func (clientInput) Workflow(clientOutput) {}

type hookTakeoverWorld struct {
	worlds.World
	takeover atomic.Bool
}

func (w *hookTakeoverWorld) EventsCreate(ctx context.Context, input worlds.EventsCreateInput) (worlds.EventResult, error) {
	if input.Event.EventType == worlds.EventTypeHookReceived && input.RunID == "wrun_old" && w.takeover.CompareAndSwap(true, false) {
		if _, err := w.World.EventsCreate(ctx, worlds.EventsCreateInput{RunID: "wrun_new", Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeHookCreated, CorrelationID: "hook_new", Data: []byte(`{"token":"racing-token","force":true}`)}}); err != nil {
			return worlds.EventResult{}, err
		}
		if _, err := w.World.EventsCreate(ctx, worlds.EventsCreateInput{RunID: "wrun_old", Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeRunCompleted}}); err != nil {
			return worlds.EventResult{}, err
		}
	}
	return w.World.EventsCreate(ctx, input)
}

func TestStaleTokenResumeReturnsTakeoverRaceWithoutRedirectingPayload(t *testing.T) {
	for _, mode := range []string{"direct resume", "HTTP resume", "webhook"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			base := testworld.NewManual()
			defer base.Close()
			world := &hookTakeoverWorld{World: base}
			for _, id := range []worlds.RunID{"wrun_old", "wrun_new"} {
				if _, err := base.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: id, WorkflowName: "takeover"}); err != nil {
					t.Fatal(err)
				}
			}
			data := []byte(`{"token":"racing-token"}`)
			if mode == "webhook" {
				data = []byte(`{"token":"racing-token","webhook":{"method":"POST"}}`)
			}
			if _, err := base.EventsCreate(ctx, worlds.EventsCreateInput{RunID: "wrun_old", Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeHookCreated, CorrelationID: "hook_old", Data: data}}); err != nil {
				t.Fatal(err)
			}
			world.takeover.Store(true)
			if mode == "webhook" {
				response := httptest.NewRecorder()
				worlds.HTTPHandler(world, "").ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/workflow/webhooks/racing-token", bytes.NewBufferString(`{"value":42}`)))
				if response.Code != http.StatusServiceUnavailable {
					t.Fatalf("takeover response=%d body=%s", response.Code, response.Body.String())
				}
			} else {
				client := worlds.NewClient(world)
				if mode == "HTTP resume" {
					client = worlds.NewHTTPClient("http://world/v1/workflow", newInMemoryHTTPClient(worlds.HTTPHandler(world, "")))
				}
				if err := client.HooksResume(ctx, worlds.ClientHooksResumeInput{Token: "racing-token", Data: []byte(`42`)}); err == nil || !strings.Contains(err.Error(), worlds.ErrRunTerminal.Error()) {
					t.Fatalf("resume race error=%v", err)
				}
			}
			for _, id := range []worlds.RunID{"wrun_old", "wrun_new"} {
				page, err := base.EventsList(ctx, worlds.EventsListInput{RunID: id})
				if err != nil {
					t.Fatal(err)
				}
				receives := 0
				for _, event := range page.Data {
					if event.EventType == worlds.EventTypeHookReceived {
						receives++
					}
				}
				want := 0
				if receives != want {
					t.Fatalf("run=%s receives=%d want=%d", id, receives, want)
				}
			}
		})
	}
}

func TestNewClientExecutesWorkflowAndDurableStep(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, input clientInput) (clientOutput, error) {
		value, err := workflow.Step(ctx, func(_ context.Context, value int) (int, error) { return value + 1, nil }, input.Value)
		return clientOutput{Value: value}, err
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, clientInput{Value: 41})
	if err != nil {
		t.Fatal(err)
	}
	output, err := run.Value(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if output.Value != 42 {
		t.Fatalf("output = %#v", output)
	}
}

func TestNewHTTPClientExecutesWorkflow(t *testing.T) {
	world := testworld.New(workflow.Use(func(_ context.Context, input clientInput) (clientOutput, error) {
		return clientOutput{Value: input.Value}, nil
	}))
	defer world.Close()
	client := workflow.NewHTTPClient("http://world/v1/workflow", newInMemoryHTTPClient(worlds.HTTPHandler(world, "")))
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), client), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, clientInput{Value: 7})
	if err != nil {
		t.Fatal(err)
	}
	output, err := run.Value(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if output.Value != 7 {
		t.Fatalf("output = %#v", output)
	}
}

func TestNewHTTPClientRejectsInvalidEndpoints(t *testing.T) {
	for _, endpoint := range []string{"", "/v1/workflow", "localhost:8080"} {
		t.Run(endpoint, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("NewHTTPClient did not panic")
				}
			}()
			_ = workflow.NewHTTPClient(endpoint)
		})
	}
}

func TestHTTPClientReportsRPCFailures(t *testing.T) {
	var calls atomic.Int32
	transport := roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		calls.Add(1)
		return &http.Response{
			StatusCode: http.StatusServiceUnavailable,
			Status:     "503 Service Unavailable",
			Header:     make(http.Header),
			Body:       io.NopCloser(bytes.NewReader(nil)),
			Request:    request,
		}, nil
	})
	client := workflow.NewHTTPClient("http://world/v1/workflow", &http.Client{Transport: transport})
	ctx := workflow.NewContext(context.Background(), client)
	if _, err := workflow.Start(ctx, clientInput{}); err == nil {
		t.Fatal("Start accepted a failed RPC response")
	}
	// A failed spec is returned as an error and not cached.
	before := calls.Load()
	for range 2 {
		if spec, err := client.Spec(ctx); err == nil || spec != (worlds.ClientSpec{}) {
			t.Fatalf("spec after RPC failure = %#v, %v", spec, err)
		}
	}
	if calls.Load()-before != 2 {
		t.Fatalf("spec calls = %d, want 2", calls.Load()-before)
	}
}

func TestHTTPClientRejectsMalformedRPCResponses(t *testing.T) {
	transport := roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Header:     make(http.Header),
			Body:       io.NopCloser(bytes.NewBufferString("not cbor")),
			Request:    request,
		}, nil
	})
	client := workflow.NewHTTPClient("http://world/v1/workflow", &http.Client{Transport: transport})
	if _, err := workflow.Start(workflow.NewContext(context.Background(), client), clientInput{}); err == nil {
		t.Fatal("Start accepted a malformed RPC response")
	}
}

func newInMemoryHTTPClient(handler http.Handler) *http.Client {
	return &http.Client{Transport: roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		return &http.Response{
			StatusCode: recorder.Code,
			Header:     recorder.Result().Header,
			Body:       io.NopCloser(bytes.NewReader(recorder.Body.Bytes())),
			Request:    request,
		}, nil
	})}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestHTTPClientWritesAndReadsRunStreams(t *testing.T) {
	world := testworld.New(workflow.Use(func(_ context.Context, input clientInput) (clientOutput, error) {
		return clientOutput{Value: input.Value}, nil
	}))
	defer world.Close()
	client := workflow.NewHTTPClient("http://world/v1/workflow", newInMemoryHTTPClient(worlds.HTTPHandler(world, "")))
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), client), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, clientInput{Value: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Value(ctx); err != nil {
		t.Fatal(err)
	}
	writer, release, err := run.Writer(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("remote")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := release(); err != nil {
		t.Fatal(err)
	}
	reader, err := run.Reader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "remote" {
		t.Fatalf("stream = %q", data)
	}
}

func TestHTTPClientCachesItsSpec(t *testing.T) {
	world := testworld.New()
	defer world.Close()
	var requests atomic.Int32
	handler := worlds.HTTPHandler(world, "")
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		handler.ServeHTTP(writer, request)
	}))
	defer server.Close()

	client := worlds.NewHTTPClient(server.URL + "/v1/workflow")
	for range 3 {
		spec, err := client.Spec(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if want := (worlds.ClientSpec{SpecVersion: world.SpecVersion(), Capabilities: world.Capabilities()}); spec != want {
			t.Fatalf("spec = %#v, want %#v", spec, want)
		}
	}
	if requests.Load() != 1 {
		t.Fatalf("requests = %d, want 1", requests.Load())
	}
}

func TestResumeHookQueuesADeliveryWhenTheHookIsNotActive(t *testing.T) {
	ctx := context.Background()
	world := testworld.NewManual()
	defer world.Close()
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: "wrun_resume", WorkflowName: "test"}); err != nil {
		t.Fatal(err)
	}
	if _, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: "wrun_resume", Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeHookCreated, CorrelationID: "hook_resume", Data: []byte(`{"token":"resume-token","webhook":{}}`)}}); err != nil {
		t.Fatal(err)
	}
	hook, err := world.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: "resume-token"})
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte(`{"request":{"method":"POST","url":"/"}}`)
	if _, err := worlds.ResumeHook(ctx, world, hook, payload); err != nil {
		t.Fatal(err)
	}

	// A webhook takes one request, so a second resume is refused, as when an
	// earlier resume recorded its request but stopped before queueing.
	if _, err := worlds.ResumeHook(ctx, world, hook, payload); !errors.Is(err, worlds.ErrHookNotActive) {
		t.Fatalf("error = %v, want ErrHookNotActive", err)
	}
	if queued := world.QueuedMessages(); len(queued) != 2 || queued[1].Input.RunID != "wrun_resume" || queued[1].Input.StepID != nil {
		t.Fatalf("queued = %#v, want a flow of the run for each resume", queued)
	}
}

func TestHTTPClientReadsStreamsFromAChunkIndex(t *testing.T) {
	world := testworld.New()
	ctx := context.Background()
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: "wrun_stream", WorkflowName: "stream"}); err != nil {
		t.Fatal(err)
	}
	if err := world.StreamsWriteMulti(ctx, worlds.StreamsWriteMultiInput{RunID: "wrun_stream", Name: "s", Chunks: [][]byte{[]byte("a"), []byte("b"), []byte("c")}}); err != nil {
		t.Fatal(err)
	}
	client := worlds.NewHTTPClient("http://world/v1/workflow", newInMemoryHTTPClient(worlds.HTTPHandler(world, "")))
	chunks, err := client.StreamsGetChunks(ctx, worlds.StreamsGetChunksInput{RunID: "wrun_stream", Name: "s", StartIndex: -2})
	if err != nil || len(chunks.Data) != 2 || chunks.Data[0].Index != 1 || string(chunks.Data[1].Data) != "c" {
		t.Fatalf("last two chunks over HTTP = %#v err=%v", chunks, err)
	}
}

func TestHTTPClientReturnsSentinelErrors(t *testing.T) {
	world := testworld.New()
	defer world.Close()
	client := workflow.NewHTTPClient("http://world/v1/workflow", newInMemoryHTTPClient(worlds.HTTPHandler(world, "")))
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), client), time.Second)
	defer cancel()

	if _, err := client.RunsGet(ctx, worlds.RunsGetInput{RunID: "wrun_missing"}); !errors.Is(err, worlds.ErrRunNotFound) {
		t.Fatalf("RunsGet error = %v, want ErrRunNotFound", err)
	}
	if err := workflow.ResumeHook(ctx, "token-missing", "value"); !errors.Is(err, workflow.ErrHookNotFound) {
		t.Fatalf("ResumeHook error = %v, want ErrHookNotFound", err)
	}
	if hook, ok, err := workflow.GetHook[string](ctx, "token-missing"); hook != nil || ok || err != nil {
		t.Fatalf("GetHook = %v, %v, %v, want not found", hook, ok, err)
	}
	run, err := workflow.Get[clientOutput](ctx, "wrun_missing")
	if err != nil {
		t.Fatal(err)
	}
	if exists, err := run.Exists(ctx); exists || err != nil {
		t.Fatalf("Exists = %v, %v, want false", exists, err)
	}
}

func TestCreateRunQueuesAPendingRunAgain(t *testing.T) {
	ctx := context.Background()
	world := testworld.NewManual()
	defer world.Close()
	input := worlds.ClientRunsCreateInput{RunID: "wrun_retried", WorkflowName: "flow"}
	if _, err := worlds.CreateRun(ctx, queueOutage{world}, input); !errors.Is(err, errQueueUnavailable) {
		t.Fatalf("first create error = %v, want the queue error", err)
	}
	runID, err := worlds.CreateRun(ctx, world, input)
	if !errors.Is(err, worlds.ErrRunAlreadyExists) || runID != input.RunID {
		t.Fatalf("retried create = %q, %v", runID, err)
	}
	if queued := world.Queued(); len(queued) != 1 || queued[0].RunID != input.RunID {
		t.Fatalf("queued = %#v, want the run's delivery", queued)
	}
}
