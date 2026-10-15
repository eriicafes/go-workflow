package worlds_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/testworld"
	"github.com/fxamacker/cbor/v2"
)

func TestRPCRejectsUnsupportedContentAndReportsEnvelopeErrors(t *testing.T) {
	handler := worlds.HTTPHandler(testworld.New(), "")
	request := httptest.NewRequest(http.MethodPost, httpPrefix+"/rpc", bytes.NewBufferString("not cbor"))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("unsupported content status = %d", response.Code)
	}

	request = httptest.NewRequest(http.MethodPost, httpPrefix+"/rpc", bytes.NewBufferString("not cbor"))
	request.Header.Set("Content-Type", worlds.RPCContentType)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	assertRPCError(t, response, "internal")

	body, err := cbor.Marshal(rpcRequest{Method: "unknown.method"})
	if err != nil {
		t.Fatal(err)
	}
	request = httptest.NewRequest(http.MethodPost, httpPrefix+"/rpc", bytes.NewReader(body))
	request.Header.Set("Content-Type", worlds.RPCContentType)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	assertRPCError(t, response, "internal")
}

func TestRPCClassifiesMissingWorldRecordsAsNotFound(t *testing.T) {
	input, err := cbor.Marshal(worlds.RunsGetInput{RunID: "wrun_missing"})
	if err != nil {
		t.Fatal(err)
	}
	body, err := cbor.Marshal(rpcRequest{Method: "runs.get", Input: input})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, httpPrefix+"/rpc", bytes.NewReader(body))
	request.Header.Set("Content-Type", worlds.RPCContentType)
	response := httptest.NewRecorder()
	worlds.HTTPHandler(testworld.New(), "").ServeHTTP(response, request)
	assertRPCError(t, response, "not_found")
}

func TestRPCRejectsMismatchedSpecVersion(t *testing.T) {
	version := worlds.SpecVersion + 1
	body, err := cbor.Marshal(rpcRequest{Method: "world.capabilities", SpecVersion: &version})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, httpPrefix+"/rpc", bytes.NewReader(body))
	request.Header.Set("Content-Type", worlds.RPCContentType)
	response := httptest.NewRecorder()
	worlds.HTTPHandler(testworld.New(), "").ServeHTTP(response, request)
	assertRPCError(t, response, "invalid_argument")
}

func TestRPCUsesEventsGetAndAnalyticsWaitInterfaces(t *testing.T) {
	world := testworld.New()
	defer world.Close()
	ctx := context.Background()
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: "wrun_rpc_interfaces", WorkflowName: "test"}); err != nil {
		t.Fatal(err)
	}
	dueAt := time.Now().Add(time.Minute).UTC()
	payload, err := json.Marshal(map[string]any{"dueAt": dueAt, "queueMessageId": "message"})
	if err != nil {
		t.Fatal(err)
	}
	created, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: "wrun_rpc_interfaces", Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeWaitCreated, CorrelationID: "wait_rpc", Data: payload}})
	if err != nil {
		t.Fatal(err)
	}
	handler := worlds.HTTPHandler(world, "")

	call := func(method string, input any, output any) {
		t.Helper()
		encoded, err := cbor.Marshal(input)
		if err != nil {
			t.Fatal(err)
		}
		body, err := cbor.Marshal(rpcRequest{Method: method, Input: encoded})
		if err != nil {
			t.Fatal(err)
		}
		request := httptest.NewRequest(http.MethodPost, httpPrefix+"/rpc", bytes.NewReader(body))
		request.Header.Set("Content-Type", worlds.RPCContentType)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("%s status = %d", method, response.Code)
		}
		var envelope rpcResponse
		if err := cbor.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
			t.Fatal(err)
		}
		if envelope.Error != nil {
			t.Fatal(envelope.Error)
		}
		if err := cbor.Unmarshal(envelope.Output, output); err != nil {
			t.Fatal(err)
		}
	}

	var event worlds.Event
	call("events.get", worlds.EventsGetInput{RunID: "wrun_rpc_interfaces", EventID: created.EventID}, &event)
	if event.EventID != created.EventID {
		t.Fatalf("event = %#v", event)
	}
	var waits worlds.Page[worlds.AnalyticsWait]
	call("analytics.waits.list", worlds.AnalyticsWaitsListInput{RunID: "wrun_rpc_interfaces"}, &waits)
	if len(waits.Data) != 1 || waits.Data[0].WaitID != "wait_rpc" {
		t.Fatalf("waits = %#v", waits)
	}
}

func assertRPCError(t *testing.T, response *httptest.ResponseRecorder, code string) {
	t.Helper()
	if response.Code != http.StatusOK {
		t.Fatalf("RPC status = %d: %s", response.Code, response.Body.String())
	}
	var envelope rpcResponse
	if err := cbor.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Error == nil || envelope.Error.Code != code {
		t.Fatalf("RPC error = %#v, want code %q", envelope.Error, code)
	}
}
