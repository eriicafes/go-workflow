package worlds_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
	world := testworld.New()
	ctx := context.Background()
	created, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{
		RunID:        "wrun_test",
		WorkflowName: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.EventID != worlds.EventIDForSlot(1) || created.EventType != worlds.EventTypeRunCreated || len(created.Events) != 0 {
		t.Fatalf("unexpected creation result: %#v", created)
	}
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: "wrun_test", WorkflowName: "test"}); !errors.Is(err, worlds.ErrRunAlreadyExists) {
		t.Fatalf("duplicate run error = %v, want ErrRunAlreadyExists", err)
	}
	started, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: "wrun_test", Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeRunStarted}, Options: worlds.EventsCreateOptions{EventCount: ptr(0)}})
	if err != nil {
		t.Fatal(err)
	}
	if started.EventID != worlds.EventIDForSlot(2) || len(started.Events) != 1 || started.Events[0].EventType != worlds.EventTypeRunCreated {
		t.Fatalf("unexpected stale writer result: %#v", started)
	}
	firstPage, err := world.EventsList(ctx, worlds.EventsListInput{RunID: "wrun_test", Pagination: worlds.Pagination{Limit: 1}})
	if err != nil || firstPage.Cursor != string(worlds.EventIDForSlot(1)) || !firstPage.HasMore {
		t.Fatalf("first event page: %#v err=%v", firstPage, err)
	}
	secondPage, err := world.EventsList(ctx, worlds.EventsListInput{RunID: "wrun_test", Pagination: worlds.Pagination{Cursor: firstPage.Cursor}})
	if err != nil || len(secondPage.Data) != 1 || secondPage.Data[0].EventID != worlds.EventIDForSlot(2) {
		t.Fatalf("second event page: %#v err=%v", secondPage, err)
	}
	if _, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: "wrun_test", Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeRunCreated}}); err == nil {
		t.Fatal("expected EventsCreate to reject run_created")
	}

	value := "region"
	err = world.RunsSetAttributes(ctx, worlds.RunsSetAttributesInput{RunID: "wrun_test", Changes: []worlds.RunsAttributeChange{{Key: "zone", Value: &value}}})
	if err != nil {
		t.Fatalf("attributes: %v", err)
	}
	run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: "wrun_test"})
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
	world := testworld.New()
	ctx := context.Background()
	_, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: "wrun_stream", WorkflowName: "test"})
	if err != nil {
		t.Fatal(err)
	}
	if err := world.StreamsWrite(ctx, worlds.StreamsWriteInput{RunID: "wrun_stream", Name: "output", Chunk: []byte("one")}); err != nil {
		t.Fatal(err)
	}
	result, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: "wrun_stream", Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeRunStarted}})
	if err != nil || result.EventID != worlds.EventIDForSlot(2) {
		t.Fatalf("event result=%#v err=%v", result, err)
	}
	chunks, err := world.StreamsGetChunks(ctx, worlds.StreamsGetChunksInput{RunID: "wrun_stream", Name: "output"})
	if err != nil || len(chunks.Data) != 1 || string(chunks.Data[0].Data) != "one" {
		t.Fatalf("chunks=%#v err=%v", chunks, err)
	}
}

func TestOpenStreamWriterWritesThroughTheStreamer(t *testing.T) {
	world := testworld.New()
	ctx := context.Background()
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: "wrun_stream_writer", WorkflowName: "test"}); err != nil {
		t.Fatal(err)
	}
	input := worlds.StreamsOpenWriterInput{RunID: "wrun_stream_writer", Name: "audit", WriterID: worlds.NewWriterID()}
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
	if err := third.Write(ctx, [][]byte{[]byte("late")}); !errors.Is(err, worlds.ErrStreamClosed) {
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
	world := testworld.New()
	ctx := context.Background()
	_, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: "wrun_step_result", WorkflowName: "test"})
	if err != nil {
		t.Fatal(err)
	}
	created, err := world.EventsCreate(ctx, worlds.EventsCreateInput{
		RunID: "wrun_step_result",
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
	world := testworld.New()
	ctx := context.Background()
	_, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: "wrun_wait_analytics", WorkflowName: "test"})
	if err != nil {
		t.Fatal(err)
	}
	dueAt := time.Now().Add(time.Minute).UTC()
	payload, err := json.Marshal(worlds.DataWaitCreated{ResumeAt: dueAt})
	if err != nil {
		t.Fatal(err)
	}
	created, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: "wrun_wait_analytics", Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeWaitCreated, CorrelationID: "wait_1", Data: payload}})
	if err != nil {
		t.Fatal(err)
	}
	event, err := world.EventsGet(ctx, worlds.EventsGetInput{RunID: "wrun_wait_analytics", EventID: created.EventID})
	if err != nil || event.CorrelationID != "wait_1" {
		t.Fatalf("event = %#v, %v", event, err)
	}
	analytics := world.Analytics()
	wait, err := analytics.WaitsGet(ctx, worlds.AnalyticsWaitsGetInput{RunID: "wrun_wait_analytics", WaitID: "wait_1"})
	if err != nil || wait.ResumeAt == nil || !wait.ResumeAt.Equal(dueAt) || wait.Status != worlds.WaitStatusWaiting {
		t.Fatalf("wait = %#v, %v", wait, err)
	}
	page, err := analytics.WaitsList(ctx, worlds.AnalyticsWaitsListInput{RunID: "wrun_wait_analytics"})
	if err != nil || len(page.Data) != 1 || page.Data[0].WaitID != "wait_1" {
		t.Fatalf("wait page = %#v, %v", page, err)
	}
}

func TestRemovedStepCancelledEventIsRejected(t *testing.T) {
	world := testworld.New()
	ctx := context.Background()
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: "wrun_step_cancel", WorkflowName: "test"}); err != nil {
		t.Fatal(err)
	}
	_, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: "wrun_step_cancel", Event: worlds.EventsCreateEvent{EventType: "step_cancelled", CorrelationID: "step_target"}})
	if !errors.Is(err, worlds.ErrEventInvalid) {
		t.Fatalf("removed event = %v", err)
	}
}

func TestWebhookHandlerOnlyResumesWebhooks(t *testing.T) {
	world := testworld.NewManual()
	defer world.Close()
	ctx := context.Background()
	_, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: "wrun_webhook", WorkflowName: "webhook"})
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
			RunID: "wrun_webhook",
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

	// Each request resumed the hook with the SDK's Request, its body written
	// to a byte stream of the run.
	events, err := world.EventsListByCorrelationID(ctx, worlds.EventsListByCorrelationIDInput{RunID: "wrun_webhook", CorrelationID: "hook_public"})
	if err != nil {
		t.Fatal(err)
	}
	requests := 0
	for _, event := range events.Data {
		if event.EventType != worlds.EventTypeHookReceived {
			continue
		}
		var request worlds.WebhookRequest
		if err := worlds.DecodePayload(worlds.DecodeHookReceived(event.Data), nil, &request); err != nil || request.Method != http.MethodPatch || request.Body == nil {
			t.Fatalf("request = %+v err=%v", request, err)
		}
		body, err := worlds.ReadByteStream(ctx, world, "wrun_webhook", *request.Body)
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
	world := testworld.New(core.NewRunner("manual", func(ctx context.Context, _ []byte) ([]byte, error) {
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
	}))
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
	world := testworld.New()
	handler := worlds.HTTPHandler(world, "")
	input, err := cbor.Marshal(worlds.EventsCreateRunInput{RunID: "wrun_rpc", WorkflowName: "rpc"})
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
	if result.Event == nil || result.Event.EventID != worlds.EventIDForSlot(1) || result.Run == nil || result.Run.RunID != "wrun_rpc" {
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

func ptr(value int) *int { return &value }

func TestHookTokensUseNanoIDsAlphabet(t *testing.T) {
	const alphabet = "useandom-26T198340PX75pxJACKVERYMINDBUSHWOLF_GQZbfghjklqvwyzrict"
	if token := worlds.NewHookToken(); len(token) != 21 || strings.Trim(token, alphabet) != "" {
		t.Errorf("token %q is not 21 characters of Nano ID's alphabet", token)
	}
}
