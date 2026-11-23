package worlds_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/core"
	"github.com/eriicafes/go-workflow/worlds/testworld"
	"github.com/fxamacker/cbor/v2"
)

const httpPrefix = "/v1/workflow"

func TestNormalizeBaseURL(t *testing.T) {
	baseURL, err := worlds.NormalizeBaseURL("https://workflows.example.com/v1/workflow/")
	if err != nil || baseURL != "https://workflows.example.com/v1/workflow" {
		t.Fatalf("NormalizeBaseURL = %q, %v", baseURL, err)
	}
	for _, value := range []string{"", "/v1/workflow", "ftp://workflows.example.com/v1/workflow", "https://workflows.example.com/v1/workflow?key=value", "https://user:password@workflows.example.com/v1/workflow"} {
		if _, err := worlds.NormalizeBaseURL(value); err == nil {
			t.Errorf("NormalizeBaseURL(%q) succeeded", value)
		}
	}
}

type rpcRequest struct {
	Method     string          `cbor:"method"`
	Input      cbor.RawMessage `cbor:"input"`
	RPCVersion *int            `cbor:"rpcVersion,omitempty"`
}

type rpcResponse struct {
	Output cbor.RawMessage `cbor:"output,omitempty"`
	Error  *struct {
		Code    string `cbor:"code"`
		Message string `cbor:"message"`
	} `cbor:"error,omitempty"`
}

func TestEventAppendMaterializesAndReportsSkippedEvents(t *testing.T) {
	world := testworld.New(nil)
	ctx := context.Background()
	created, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{
		RunID:        runTest,
		WorkflowName: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.EventID != worlds.EventIDForSlot(1) || created.EventType != worlds.EventTypeRunCreated || len(created.Events) != 0 {
		t.Fatalf("unexpected creation result: %#v", created)
	}
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runTest, WorkflowName: "test"}); !errors.Is(err, worlds.ErrRunAlreadyExists) {
		t.Fatalf("duplicate run error = %v, want ErrRunAlreadyExists", err)
	}
	started, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runTest, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeRunStarted}, Options: worlds.EventsCreateOptions{EventCount: ptr(0)}})
	if err != nil {
		t.Fatal(err)
	}
	if started.EventID != worlds.EventIDForSlot(2) || len(started.Events) != 1 || started.Events[0].EventType != worlds.EventTypeRunCreated {
		t.Fatalf("unexpected stale writer result: %#v", started)
	}
	firstPage, err := world.EventsList(ctx, worlds.EventsListInput{RunID: runTest, Pagination: worlds.Pagination{Limit: 1}})
	if err != nil || firstPage.Cursor != string(worlds.EventIDForSlot(1)) || !firstPage.HasMore {
		t.Fatalf("first event page: %#v err=%v", firstPage, err)
	}
	secondPage, err := world.EventsList(ctx, worlds.EventsListInput{RunID: runTest, Pagination: worlds.Pagination{Cursor: firstPage.Cursor}})
	if err != nil || len(secondPage.Data) != 1 || secondPage.Data[0].EventID != worlds.EventIDForSlot(2) {
		t.Fatalf("second event page: %#v err=%v", secondPage, err)
	}
	if _, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runTest, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeRunCreated}}); err == nil {
		t.Fatal("expected EventsCreate to reject run_created")
	}

	value := "region"
	err = world.RunsSetAttributes(ctx, worlds.RunsSetAttributesInput{RunID: runTest, Changes: []worlds.RunsAttributeChange{{Key: "zone", Value: &value}}})
	if err != nil {
		t.Fatalf("attributes: %v", err)
	}
	run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runTest})
	if err != nil || run.Attributes["zone"] != "region" {
		t.Fatalf("run: %#v err=%v", run, err)
	}
	analytics := world.Analytics()
	page, err := analytics.RunsList(ctx, worlds.AnalyticsRunsListInput{Attributes: map[string]string{"zone": "region"}})
	if err != nil || len(page.Data) != 1 {
		t.Fatalf("analytics: %#v err=%v", page, err)
	}
}

func TestStreamsDoNotConsumeEventSlots(t *testing.T) {
	world := testworld.New(nil)
	ctx := context.Background()
	_, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runStream, WorkflowName: "test"})
	if err != nil {
		t.Fatal(err)
	}
	if err := world.StreamsWrite(ctx, worlds.StreamsWriteInput{RunID: runStream, Name: "output", Chunk: []byte("one")}); err != nil {
		t.Fatal(err)
	}
	result, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runStream, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeRunStarted}})
	if err != nil || result.EventID != worlds.EventIDForSlot(2) {
		t.Fatalf("event result=%#v err=%v", result, err)
	}
	chunks, err := world.StreamsGetChunks(ctx, worlds.StreamsGetChunksInput{RunID: runStream, Name: "output"})
	if err != nil || len(chunks.Data) != 1 || string(chunks.Data[0].Data) != "one" {
		t.Fatalf("chunks=%#v err=%v", chunks, err)
	}
}

func TestOpenStreamWriterWritesThroughTheStreamer(t *testing.T) {
	world := testworld.New(nil)
	ctx := context.Background()
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runStreamWriter, WorkflowName: "test"}); err != nil {
		t.Fatal(err)
	}
	input := worlds.StreamsOpenWriterInput{RunID: runStreamWriter, Name: "audit", WriterID: worlds.NewWriterID()}
	first, err := worlds.OpenStreamWriter(ctx, world, input)
	if err != nil {
		t.Fatal(err)
	}
	second, err := worlds.OpenStreamWriter(ctx, world, input)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Write(ctx, [][]byte{[]byte("a"), []byte("b")}); err != nil {
		t.Fatal(err)
	}
	if err := second.Write(ctx, [][]byte{[]byte("c")}); err != nil {
		t.Fatal(err)
	}
	if err := first.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := first.Write(ctx, [][]byte{[]byte("x")}); err == nil {
		t.Fatal("released writer accepted a write")
	}
	if err := second.CloseStream(ctx); err != nil {
		t.Fatal(err)
	}
	third, err := worlds.OpenStreamWriter(ctx, world, input)
	if err != nil {
		t.Fatal(err)
	}
	// A closed stream drops the write.
	if err := third.Write(ctx, [][]byte{[]byte("late")}); err != nil {
		t.Fatalf("write to a closed stream = %v", err)
	}
	chunks, err := world.StreamsGetChunks(ctx, worlds.StreamsGetChunksInput{RunID: input.RunID, Name: input.Name})
	if err != nil || len(chunks.Data) != 3 || !chunks.Done || string(chunks.Data[2].Data) != "c" {
		t.Fatalf("chunks = %#v err=%v", chunks, err)
	}
	if !strings.HasPrefix(input.WriterID, "wrtr_") || len(input.WriterID) != len("wrtr_")+26 {
		t.Fatalf("writer ID = %q", input.WriterID)
	}
}

func TestEventResultReportsMaterializedStep(t *testing.T) {
	world := testworld.New(nil)
	ctx := context.Background()
	_, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runStepResult, WorkflowName: "test"})
	if err != nil {
		t.Fatal(err)
	}
	created, err := world.EventsCreate(ctx, worlds.EventsCreateInput{
		RunID: runStepResult,
		Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeStepCreated, CorrelationID: "step_result", Data: []byte(`{"stepName":"result"}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !created.StepCreated || created.AllocatedEventCount != 1 || created.Step == nil || created.Step.StepID != "step_result" || created.Step.StepName != "result" {
		t.Fatalf("created result = %#v", created)
	}
}

func TestEventsGetAndAnalyticsWaitsUseDedicatedInterfaces(t *testing.T) {
	world := testworld.New(nil)
	ctx := context.Background()
	_, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runWaitAnalytics, WorkflowName: "test"})
	if err != nil {
		t.Fatal(err)
	}
	dueAt := time.Now().Add(time.Minute).UTC()
	payload, err := json.Marshal(worlds.DataWaitCreated{ResumeAt: dueAt})
	if err != nil {
		t.Fatal(err)
	}
	created, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runWaitAnalytics, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeWaitCreated, CorrelationID: "wait_1", Data: payload}})
	if err != nil {
		t.Fatal(err)
	}
	event, err := world.EventsGet(ctx, worlds.EventsGetInput{RunID: runWaitAnalytics, EventID: created.EventID})
	if err != nil || event.CorrelationID != "wait_1" {
		t.Fatalf("event = %#v, %v", event, err)
	}
	analytics := world.Analytics()
	wait, err := analytics.WaitsGet(ctx, worlds.AnalyticsWaitsGetInput{RunID: runWaitAnalytics, WaitID: "wait_1"})
	if err != nil || wait.ResumeAt == nil || !wait.ResumeAt.Equal(dueAt) || wait.Status != worlds.WaitStatusWaiting {
		t.Fatalf("wait = %#v, %v", wait, err)
	}
	page, err := analytics.WaitsList(ctx, worlds.AnalyticsWaitsListInput{RunID: runWaitAnalytics})
	if err != nil || len(page.Data) != 1 || page.Data[0].WaitID != "wait_1" {
		t.Fatalf("wait page = %#v, %v", page, err)
	}
}

func TestRemovedStepCancelledEventIsRejected(t *testing.T) {
	world := testworld.New(nil)
	ctx := context.Background()
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runStepCancel, WorkflowName: "test"}); err != nil {
		t.Fatal(err)
	}
	_, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runStepCancel, Event: worlds.EventsCreateEvent{EventType: "step_cancelled", CorrelationID: "step_target"}})
	if !errors.Is(err, worlds.ErrEventInvalid) {
		t.Fatalf("removed event = %v", err)
	}
}

func TestWebhookHandlerOnlyResumesWebhooks(t *testing.T) {
	world := testworld.NewManual(nil)
	defer world.Close()
	ctx := context.Background()
	_, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runWebhook, WorkflowName: "webhook"})
	if err != nil {
		t.Fatal(err)
	}
	createHook := func(id, token string, webhook bool, metadata any) {
		var encoded []byte
		if metadata != nil {
			if encoded, err = worlds.EncodePayload(metadata, worlds.PayloadOptions{}); err != nil {
				t.Fatal(err)
			}
		}
		data, err := json.Marshal(worlds.DataHookCreated{Token: token, IsWebhook: webhook, Metadata: encoded})
		if err != nil {
			t.Fatal(err)
		}
		_, err = world.EventsCreate(ctx, worlds.EventsCreateInput{
			RunID: runWebhook,
			Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeHookCreated, CorrelationID: id, Data: data},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	createHook("hook_public", "public", true, nil)
	createHook("hook_internal", "internal", false, nil)
	createHook("hook_static", "static", true, worlds.WebhookMetadata{Response: &worlds.WebhookResponse{Status: http.StatusCreated, Header: http.Header{"X-Webhook": {"static"}}, Body: &worlds.StreamRef{BodyInit: []byte("ack")}}})

	handler := worlds.HTTPHandler(world, "")
	serve := func(method, token string, body []byte) *httptest.ResponseRecorder {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(method, httpPrefix+"/webhooks/"+token, bytes.NewReader(body)))
		return response
	}
	for range 2 {
		if response := serve(http.MethodPatch, "public", []byte("payload")); response.Code != http.StatusAccepted {
			t.Fatalf("public webhook response = %d %s", response.Code, response.Body)
		}
		if response := serve(http.MethodPost, "static", nil); response.Code != http.StatusCreated || response.Header().Get("X-Webhook") != "static" || response.Body.String() != "ack" {
			t.Fatalf("static webhook response = %d, header=%q, body=%q", response.Code, response.Header().Get("X-Webhook"), response.Body.String())
		}
	}
	if response := serve(http.MethodGet, "internal", nil); response.Code != http.StatusNotFound {
		t.Fatalf("regular hook response = %d", response.Code)
	}
	// Mounted without a {token} wildcard, the handler reads the last path
	// segment, which a trailing slash leaves empty.
	for path, code := range map[string]int{"/hooks/static": http.StatusCreated, "/hooks/static/": http.StatusNotFound} {
		response := httptest.NewRecorder()
		worlds.WebhookHandler(world).ServeHTTP(response, httptest.NewRequest(http.MethodPost, path, nil))
		if response.Code != code {
			t.Fatalf("POST %s = %d, want %d", path, response.Code, code)
		}
	}

	// Each request resumed the hook with a Request, its body written to a
	// byte stream of the run.
	events, err := world.EventsListByCorrelationID(ctx, worlds.EventsListByCorrelationIDInput{RunID: runWebhook, CorrelationID: "hook_public"})
	if err != nil {
		t.Fatal(err)
	}
	requests := 0
	for _, event := range events.Data {
		if event.EventType != worlds.EventTypeHookReceived {
			continue
		}
		var request worlds.WebhookRequest
		// The URL is absolute, as a Request's is.
		if err := worlds.DecodePayload(worlds.DecodeHookReceived(event.Data), nil, &request); err != nil || request.Method != http.MethodPatch || request.Body == nil || request.URL != "http://example.com"+httpPrefix+"/webhooks/public" {
			t.Fatalf("request = %+v err=%v", request, err)
		}
		body, err := worlds.ReadByteStream(ctx, world, runWebhook, *request.Body)
		if err != nil || string(body) != "payload" {
			t.Fatalf("body = %q err=%v", body, err)
		}
		requests++
	}
	if requests != 2 {
		t.Fatalf("requests = %d", requests)
	}
}

func TestWebhookHandlerWaitsForManualResponse(t *testing.T) {
	world := testworld.New(worlds.Runners(worlds.NewRunner("manual", func(ctx context.Context, _ []byte) ([]byte, error) {
		execution, ok := core.ExecutionFromContext(ctx)
		if !ok {
			return nil, nil
		}
		page, err := execution.World().EventsList(ctx, worlds.EventsListInput{RunID: execution.Run().RunID})
		if err != nil {
			return nil, err
		}
		for _, event := range page.Data {
			if event.EventType != worlds.EventTypeHookReceived {
				continue
			}
			var request worlds.WebhookRequest
			if err := execution.DecodePayload(worlds.DecodeHookReceived(event.Data), &request); err != nil {
				return nil, err
			}
			response := worlds.WebhookResponse{Status: http.StatusOK, Header: http.Header{"X-Webhook": {"manual"}}, Body: &worlds.StreamRef{BodyInit: []byte("done")}}
			if err := execution.WriteWebhookResponse(ctx, request, response); err != nil {
				return nil, err
			}
		}
		return nil, nil
	})))
	ctx := context.Background()
	runID := worlds.NewRunID()
	_, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "manual"})
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := worlds.EncodePayload(worlds.WebhookMetadata{Manual: true}, worlds.PayloadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(worlds.DataHookCreated{Token: "manual", IsWebhook: true, Metadata: metadata})
	if err != nil {
		t.Fatal(err)
	}
	_, err = world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeHookCreated, CorrelationID: "hook_manual", Data: data}})
	if err != nil {
		t.Fatal(err)
	}

	response := httptest.NewRecorder()
	worlds.HTTPHandler(world, "").ServeHTTP(response, httptest.NewRequest(http.MethodPost, httpPrefix+"/webhooks/manual", bytes.NewReader([]byte("request"))))
	if response.Code != http.StatusOK || response.Header().Get("X-Webhook") != "manual" || response.Body.String() != "done" {
		t.Fatalf("manual webhook response = %d, header=%q, body=%q", response.Code, response.Header().Get("X-Webhook"), response.Body.String())
	}
}

func TestCBORRPCAndDefaultPrefix(t *testing.T) {
	world := testworld.New(nil)
	handler := worlds.HTTPHandler(world, "")
	input, err := cbor.Marshal(worlds.EventsCreateRunInput{RunID: runRpc, WorkflowName: "rpc"})
	if err != nil {
		t.Fatal(err)
	}
	body, err := cbor.Marshal(rpcRequest{Method: "events.createRun", Input: input})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, httpPrefix+"/rpc", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/cbor")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var envelope rpcResponse
	if err := cbor.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Error != nil {
		t.Fatal(envelope.Error)
	}
	var result worlds.RPCEventResult
	if err := cbor.Unmarshal(envelope.Output, &result); err != nil {
		t.Fatal(err)
	}
	if result.Event == nil || result.Event.EventID != worlds.EventIDForSlot(1) || result.Run == nil || result.Run.RunID != runRpc {
		t.Fatalf("result=%#v", result)
	}

	request = httptest.NewRequest(http.MethodPost, httpPrefix+"/rpc", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/cbor")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	assertRPCError(t, response, "conflict")

	body, err = cbor.Marshal(rpcRequest{Method: "client.spec"})
	if err != nil {
		t.Fatal(err)
	}
	request = httptest.NewRequest(http.MethodPost, httpPrefix+"/rpc", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/cbor")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("spec status=%d body=%s", response.Code, response.Body.String())
	}
	if err := cbor.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Error != nil {
		t.Fatal(envelope.Error)
	}
	var spec worlds.ClientSpec
	if err := cbor.Unmarshal(envelope.Output, &spec); err != nil {
		t.Fatal(err)
	}
	if spec.RPCVersion != worlds.RPCVersion || !spec.Capabilities.Analytics || !spec.Capabilities.SetAttributes {
		t.Fatalf("spec=%#v", spec)
	}
}

func ptr[T any](value T) *T { return &value }

func TestHookTokensUseNanoIDsAlphabet(t *testing.T) {
	const alphabet = "useandom-26T198340PX75pxJACKVERYMINDBUSHWOLF_GQZbfghjklqvwyzrict"
	if token := worlds.NewHookToken(); len(token) != 21 || strings.Trim(token, alphabet) != "" {
		t.Errorf("token %q is not 21 characters of Nano ID's alphabet", token)
	}
}

// runRpc is a run ID minted when the tests start, as a World requires
// of a new run.
var runRpc = worlds.NewRunID()

// runStepCancel is a run ID minted when the tests start, as a World requires
// of a new run.
var runStepCancel = worlds.NewRunID()

// runStepResult is a run ID minted when the tests start, as a World requires
// of a new run.
var runStepResult = worlds.NewRunID()

// runStreamWriter is a run ID minted when the tests start, as a World requires
// of a new run.
var runStreamWriter = worlds.NewRunID()

// runTest is a run ID minted when the tests start, as a World requires
// of a new run.
var runTest = worlds.NewRunID()

// runWaitAnalytics is a run ID minted when the tests start, as a World requires
// of a new run.
var runWaitAnalytics = worlds.NewRunID()

// runWebhook is a run ID minted when the tests start, as a World requires
// of a new run.
var runWebhook = worlds.NewRunID()

// scriptedStartWorld answers a start's EventsCreateRun and Queue calls with
// the errors scripted for them, in order, and then as the World would.
type scriptedStartWorld struct {
	*testworld.World
	create, queue   []error
	creates, queues int
}

func (w *scriptedStartWorld) EventsCreateRun(ctx context.Context, input worlds.EventsCreateRunInput) (worlds.EventResult, error) {
	w.creates++
	if w.creates <= len(w.create) && w.create[w.creates-1] != nil {
		return worlds.EventResult{}, w.create[w.creates-1]
	}
	return w.World.EventsCreateRun(ctx, input)
}

func (w *scriptedStartWorld) Queue(ctx context.Context, input worlds.QueueInput) (worlds.QueueResult, error) {
	w.queues++
	if w.queues <= len(w.queue) && w.queue[w.queues-1] != nil {
		return worlds.QueueResult{}, w.queue[w.queues-1]
	}
	return w.World.Queue(ctx, input)
}

func TestStartRunDecidesAStartHookCandidateBeforeItReturns(t *testing.T) {
	defer worlds.SetStartHookConfirmDelays(time.Millisecond, time.Millisecond, time.Millisecond)()
	ctx := context.Background()
	disabled := fmt.Errorf("%w: off", worlds.ErrStartHookDisabled)
	throttled := &worlds.ThrottleError{}
	start := func(create, queue []error) (*scriptedStartWorld, worlds.RunID, error) {
		manual := testworld.NewManual(nil)
		t.Cleanup(manual.Close)
		world := &scriptedStartWorld{World: manual, create: create, queue: queue}
		runID := worlds.NewRunID()
		return world, runID, worlds.StartRun(ctx, world, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "flow", StartHook: &worlds.StartHook{Token: "token-" + string(runID)}})
	}
	// An admission that is switched off, or fails retryably, is asked for
	// again: the World answers every copy of a candidate the same.
	world, runID, err := start([]error{disabled, throttled}, nil)
	if err != nil || world.creates != 3 || world.queues != 1 {
		t.Fatalf("start = %v after %d creations and %d sends, want it admitted on the third", err, world.creates, world.queues)
	}
	if run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID}); err != nil || run.StartHook == nil {
		t.Fatalf("run = %+v, %v, want the admitted candidate", run, err)
	}
	// A candidate another run's token refuses is refused at once, with the
	// run that holds the token, and its delivery stays queued to be refused.
	taken := worlds.NewRunID()
	err = worlds.StartRun(ctx, world, worlds.EventsCreateRunInput{RunID: taken, WorkflowName: "flow", StartHook: &worlds.StartHook{Token: "token-" + string(runID)}})
	var conflict *worlds.HookConflictError
	if !errors.As(err, &conflict) || conflict.RunID != runID || world.creates != 4 {
		t.Fatalf("start with a held token = %v after %d creations, want the conflict at once", err, world.creates)
	}
}
