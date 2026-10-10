package workflow

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/eriicafes/go-workflow/workflowops"
)

type recordingClient struct {
	request workflowops.RunRequest
	calls   int
}

func (client *recordingClient) CreateRun(
	_ context.Context,
	_ *workflowops.LeasedRun,
	request workflowops.RunRequest,
) (string, error) {
	client.calls++
	client.request = request
	return "run_1", nil
}

func (*recordingClient) WaitRun(context.Context, string) (json.RawMessage, error) {
	return nil, nil
}

func (*recordingClient) Events(context.Context, string, int64) ([]Event, error) {
	return nil, nil
}

func TestHTTPHandlerCreatesRootRequest(t *testing.T) {
	client := &recordingClient{}
	request := httptest.NewRequest(
		http.MethodPost,
		apiPrefix,
		strings.NewReader(`{"identity":"example","input":{"value":1}}`),
	)
	response := httptest.NewRecorder()

	NewHTTPHandler(client).ServeHTTP(response, request)

	if response.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusAccepted)
	}
	if client.calls != 1 {
		t.Fatalf("CreateRun calls = %d, want 1", client.calls)
	}
	if client.request.RunID != "" ||
		client.request.Sequence != workflowops.RootSequence ||
		client.request.Kind != workflowops.KindRun ||
		client.request.Fingerprint != "example" {
		t.Fatalf("request = %#v", client.request)
	}
}

func TestHTTPHandlerRejectsInternalRunFields(t *testing.T) {
	client := &recordingClient{}
	request := httptest.NewRequest(
		http.MethodPost,
		apiPrefix,
		strings.NewReader(`{"identity":"example","input":{},"sequence":-1}`),
	)
	response := httptest.NewRecorder()

	NewHTTPHandler(client).ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusBadRequest)
	}
	if client.calls != 0 {
		t.Fatalf("CreateRun calls = %d, want 0", client.calls)
	}
}
