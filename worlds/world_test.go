package worlds_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/testworld"
	"github.com/fxamacker/cbor/v2"
)

const httpPrefix = "/v1/workflow"

type rpcRequest struct {
	Method string          `cbor:"method"`
	Input  cbor.RawMessage `cbor:"input"`
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
	if err := world.StreamsWrite(ctx, worlds.StreamsWriteInput{RunID: "wrun_stream", Name: "strm_test", Chunk: []byte("one")}); err != nil {
		t.Fatal(err)
	}
	result, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: "wrun_stream", Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeRunStarted}})
	if err != nil || result.EventID != worlds.EventIDForSlot(2) {
		t.Fatalf("event result=%#v err=%v", result, err)
	}
	chunks, err := world.StreamsGetChunks(ctx, worlds.StreamsGetChunksInput{RunID: "wrun_stream", Name: "strm_test"})
	if err != nil || len(chunks.Chunks) != 1 || string(chunks.Chunks[0].Data) != "one" {
		t.Fatalf("chunks=%#v err=%v", chunks, err)
	}
}

func TestStepCancelledEventCancelsOnlyTheTargetStep(t *testing.T) {
	world := testworld.New()
	ctx := context.Background()
	_, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: "wrun_step_cancel", WorkflowName: "test"})
	if err != nil {
		t.Fatal(err)
	}
	stepData, err := json.Marshal(map[string]any{"stepName": "target"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = world.EventsCreate(ctx, worlds.EventsCreateInput{
		RunID: "wrun_step_cancel",
		Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeStepCreated, CorrelationID: "step_target", Data: stepData},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = world.EventsCreate(ctx, worlds.EventsCreateInput{
		RunID: "wrun_step_cancel",
		Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeStepCancelled, CorrelationID: "step_target"},
	})
	if err != nil {
		t.Fatal(err)
	}
	step, err := world.StepsGet(ctx, worlds.StepsGetInput{RunID: "wrun_step_cancel", StepID: "step_target"})
	if err != nil || step.Status != worlds.StatusCancelled {
		t.Fatalf("step=%#v err=%v", step, err)
	}
	run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: "wrun_step_cancel"})
	if err != nil || run.Status != worlds.StatusPending {
		t.Fatalf("run=%#v err=%v", run, err)
	}
}

func TestWebhookHandlerOnlyResumesPublicWebhooksWithTheConfiguredMethod(t *testing.T) {
	world := testworld.New()
	ctx := context.Background()
	_, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: "wrun_webhook", WorkflowName: "webhook"})
	if err != nil {
		t.Fatal(err)
	}
	createHook := func(id, token string, webhook *worlds.Webhook) {
		data, err := json.Marshal(map[string]any{"token": token, "webhook": webhook})
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
	createHook("hook_public", "public", &worlds.Webhook{Method: http.MethodPatch})
	createHook("hook_internal", "internal", nil)
	createHook("hook_static", "static", &worlds.Webhook{Method: http.MethodPost, Response: &worlds.WebhookResponse{Status: http.StatusCreated, Header: map[string][]string{"X-Webhook": {"static"}}, Body: []byte("ack")}})

	handler := worlds.HTTPHandler(world, nil, "")
	request := httptest.NewRequest(http.MethodPost, httpPrefix+"/webhooks/public", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusMethodNotAllowed || response.Header().Get("Allow") != http.MethodPatch {
		t.Fatalf("wrong-method response = %d, allow=%q", response.Code, response.Header().Get("Allow"))
	}

	request = httptest.NewRequest(http.MethodPatch, httpPrefix+"/webhooks/public", bytes.NewReader([]byte("payload")))
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("public webhook response = %d", response.Code)
	}

	request = httptest.NewRequest(http.MethodGet, httpPrefix+"/webhooks/internal", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("regular hook response = %d", response.Code)
	}

	request = httptest.NewRequest(http.MethodPost, httpPrefix+"/webhooks/static", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusCreated || response.Header().Get("X-Webhook") != "static" || response.Body.String() != "ack" {
		t.Fatalf("static webhook response = %d, header=%q, body=%q", response.Code, response.Header().Get("X-Webhook"), response.Body.String())
	}

	request = httptest.NewRequest(http.MethodPost, httpPrefix+"/webhooks/static", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("second static webhook response = %d", response.Code)
	}
}

func TestWebhookHandlerWaitsForManualResponse(t *testing.T) {
	world := testworld.New(worlds.NewRunner("manual", func(ctx context.Context, _ json.RawMessage) (json.RawMessage, error) {
		execution, ok := worlds.ExecutionFromContext(ctx)
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
			var received worlds.WebhookReceivedEventData
			if err := json.Unmarshal(event.Data, &received); err != nil || received.Request == nil {
				continue
			}
			response, err := json.Marshal(worlds.WebhookReceivedEventData{Response: &worlds.WebhookResponse{Status: http.StatusOK, Header: map[string][]string{"X-Webhook": {"manual"}}, Body: []byte("done")}, ResponseTo: event.EventID})
			if err != nil {
				return nil, err
			}
			_, err = execution.World().EventsCreate(ctx, worlds.EventsCreateInput{RunID: execution.Run().RunID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeHookReceived, CorrelationID: event.CorrelationID, Data: response}})
			return nil, err
		}
		return nil, nil
	}))
	ctx := context.Background()
	_, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: "wrun_manual", WorkflowName: "manual"})
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(map[string]any{"token": "manual", "webhook": &worlds.Webhook{Manual: true}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: "wrun_manual", Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeHookCreated, CorrelationID: "hook_manual", Data: data}})
	if err != nil {
		t.Fatal(err)
	}

	response := httptest.NewRecorder()
	worlds.HTTPHandler(world, nil, "").ServeHTTP(response, httptest.NewRequest(http.MethodPost, httpPrefix+"/webhooks/manual", bytes.NewReader([]byte("request"))))
	if response.Code != http.StatusOK || response.Header().Get("X-Webhook") != "manual" || response.Body.String() != "done" {
		t.Fatalf("manual webhook response = %d, header=%q, body=%q", response.Code, response.Header().Get("X-Webhook"), response.Body.String())
	}

	response = httptest.NewRecorder()
	worlds.HTTPHandler(world, nil, "").ServeHTTP(response, httptest.NewRequest(http.MethodPost, httpPrefix+"/webhooks/manual", bytes.NewReader([]byte("late request"))))
	if response.Code != http.StatusNotFound {
		t.Fatalf("late manual webhook response = %d", response.Code)
	}
}

func TestCBORRPCAndDefaultPrefix(t *testing.T) {
	world := testworld.New()
	handler := worlds.HTTPHandler(world, nil, "")
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
	var result worlds.EventResult
	if err := cbor.Unmarshal(envelope.Output, &result); err != nil {
		t.Fatal(err)
	}
	if result.EventID != worlds.EventIDForSlot(1) {
		t.Fatalf("result=%#v", result)
	}

	body, err = cbor.Marshal(rpcRequest{Method: "world.capabilities"})
	if err != nil {
		t.Fatal(err)
	}
	request = httptest.NewRequest(http.MethodPost, httpPrefix+"/rpc", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/cbor")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("capabilities status=%d body=%s", response.Code, response.Body.String())
	}
	if err := cbor.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Error != nil {
		t.Fatal(envelope.Error)
	}
	var capabilities worlds.Capabilities
	if err := cbor.Unmarshal(envelope.Output, &capabilities); err != nil {
		t.Fatal(err)
	}
	if !capabilities.Analytics || !capabilities.SetAttributes {
		t.Fatalf("capabilities=%#v", capabilities)
	}
}

func ptr(value int) *int { return &value }
