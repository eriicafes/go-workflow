package worlds_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/testworld"
	"github.com/fxamacker/cbor/v2"
)

func TestRPCRejectsUnsupportedContentAndReportsEnvelopeErrors(t *testing.T) {
	handler := worlds.HTTPHandler(testworld.New(), nil, "")
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
	worlds.HTTPHandler(testworld.New(), nil, "").ServeHTTP(response, request)
	assertRPCError(t, response, "not_found")
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
