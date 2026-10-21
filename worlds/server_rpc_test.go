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
	body, err := cbor.Marshal(rpcRequest{Method: "client.spec", SpecVersion: &version})
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
	payload, err := json.Marshal(worlds.DataWaitCreated{ResumeAt: dueAt, QueueMessageID: "message"})
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

func TestRPCEncodesTimesAsTaggedRFC3339(t *testing.T) {
	ctx := context.Background()
	world := testworld.New()
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: "wrun_time", WorkflowName: "time"}); err != nil {
		t.Fatal(err)
	}
	want, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: "wrun_time"})
	if err != nil {
		t.Fatal(err)
	}

	input, err := cbor.Marshal(worlds.RunsGetInput{RunID: "wrun_time"})
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
	worlds.HTTPHandler(world, "").ServeHTTP(response, request)
	var envelope rpcResponse
	if err := cbor.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Error != nil {
		t.Fatal(envelope.Error)
	}

	// JavaScript clients decode tag 0 as a Date, unlike untagged unix seconds.
	var fields map[string]cbor.RawMessage
	if err := cbor.Unmarshal(envelope.Output, &fields); err != nil {
		t.Fatal(err)
	}
	if createdAt := fields["CreatedAt"]; len(createdAt) == 0 || createdAt[0] != 0xc0 {
		t.Fatalf("CreatedAt = %x, want tag 0", createdAt)
	}
	var got worlds.Run
	if err := cbor.Unmarshal(envelope.Output, &got); err != nil {
		t.Fatal(err)
	}
	if !got.CreatedAt.Equal(want.CreatedAt) {
		t.Fatalf("CreatedAt = %v, want %v", got.CreatedAt, want.CreatedAt)
	}
}

// callRPC sends one RPC request to handler and decodes its output into a map
// keyed as the wire carries it.
func callRPC(t *testing.T, handler http.Handler, method string, input any) map[string]cbor.RawMessage {
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
	var envelope rpcResponse
	if err := cbor.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Error != nil {
		t.Fatalf("%s: %s %s", method, envelope.Error.Code, envelope.Error.Message)
	}
	var output map[string]cbor.RawMessage
	if err := cbor.Unmarshal(envelope.Output, &output); err != nil {
		t.Fatal(err)
	}
	return output
}

func TestRPCEventWritesReturnWorkflowSDKResults(t *testing.T) {
	handler := worlds.HTTPHandler(testworld.New(), "")

	// A run created without an ID gets one, and the result carries the run.
	created := callRPC(t, handler, "events.createRun", worlds.EventsCreateRunInput{WorkflowName: "rpc", Input: []byte(`"in"`)})
	var run worlds.Run
	if err := cbor.Unmarshal(created["Run"], &run); err != nil || run.RunID == "" || run.WorkflowName != "rpc" {
		t.Fatalf("createRun run = %#v err=%v", run, err)
	}
	var event worlds.Event
	if err := cbor.Unmarshal(created["Event"], &event); err != nil || event.EventType != worlds.EventTypeRunCreated || event.RunID != run.RunID {
		t.Fatalf("createRun event = %#v err=%v", event, err)
	}

	// A step event carries the step it changed, and no run.
	stepData, _ := json.Marshal(worlds.DataStepCreated{StepName: "pkg.step", Input: json.RawMessage(`1`)})
	step := callRPC(t, handler, "events.create", worlds.EventsCreateInput{RunID: run.RunID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeStepCreated, CorrelationID: "step_rpc", Data: stepData}})
	if _, ok := step["Run"]; ok {
		t.Fatalf("step event result has a run: %v", step)
	}
	var value worlds.Step
	if err := cbor.Unmarshal(step["Step"], &value); err != nil || value.StepID != "step_rpc" || value.StepName != "pkg.step" {
		t.Fatalf("step event step = %#v err=%v", value, err)
	}
	var allocated int
	if err := cbor.Unmarshal(step["Allocated"], &allocated); err != nil || allocated != 1 {
		t.Fatalf("step event allocated = %d err=%v", allocated, err)
	}

	// A batch reports each event's own outcome: run_created cannot be
	// appended, while the wait is committed.
	waitData, _ := json.Marshal(worlds.DataWaitCreated{ResumeAt: time.Date(2030, time.January, 1, 0, 0, 0, 0, time.UTC)})
	batch := callRPC(t, handler, "events.createBatch", worlds.EventsCreateBatchInput{RunID: run.RunID, Events: []worlds.EventsCreateEvent{
		{EventType: worlds.EventTypeRunCreated},
		{EventType: worlds.EventTypeWaitCreated, CorrelationID: "wait_rpc", Data: waitData},
	}})
	var results []struct {
		Status int
		Error  string
		Event  *worlds.Event
	}
	if err := cbor.Unmarshal(batch["Results"], &results); err != nil || len(results) != 2 {
		t.Fatalf("batch results = %#v err=%v", results, err)
	}
	if results[0].Status != http.StatusBadRequest || results[0].Error != "invalid_argument" || results[0].Event != nil {
		t.Fatalf("run_created result = %#v", results[0])
	}
	if results[1].Status != http.StatusOK || results[1].Event == nil || results[1].Event.CorrelationID != "wait_rpc" {
		t.Fatalf("wait result = %#v", results[1])
	}
}
