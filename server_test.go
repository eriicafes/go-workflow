package workflow

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/workflowops"
)

type recordingClient struct {
	request workflowops.RunRequest
	calls   int
}

type recordingWorld struct {
	recordingClient
}

var _ World = (*recordingWorld)(nil)

func (client *recordingClient) CreateRun(
	_ context.Context,
	_ *workflowops.Lease,
	request workflowops.RunRequest,
) (string, error) {
	client.calls++
	client.request = request
	return "run_1", nil
}

func (*recordingClient) WaitRun(context.Context, string) (json.RawMessage, error) {
	return nil, nil
}

func (*recordingClient) GetRun(context.Context, string) (workflowops.RunInfo, bool, error) {
	return workflowops.RunInfo{}, false, nil
}

func (*recordingClient) Wake(context.Context, string) (int, error) {
	return 0, nil
}

func (*recordingClient) CancelOperations(context.Context, ...workflowops.Task) error {
	return nil
}

func (*recordingClient) ReadStream(context.Context, workflowops.Stream, int64) (workflowops.StreamRead, error) {
	return workflowops.StreamRead{}, nil
}

func (*recordingClient) LockStream(context.Context, workflowops.Stream, string) (bool, error) {
	return true, nil
}

func (*recordingClient) AppendStream(context.Context, workflowops.Stream, string, []byte) error {
	return nil
}

func (*recordingClient) ReleaseStream(context.Context, workflowops.Stream, string, bool) error {
	return nil
}

func (*recordingClient) GetHook(context.Context, string) (workflowops.Hook, bool, error) {
	return workflowops.Hook{}, false, nil
}

func (*recordingClient) ResumeWebhook(context.Context, string, json.RawMessage) error {
	return nil
}

func (*recordingClient) ReplyWebhook(context.Context, string, json.RawMessage) error {
	return nil
}

func (*recordingClient) WaitWebhookResponse(context.Context, string) (json.RawMessage, error) {
	return nil, nil
}

func (*recordingWorld) CleanupTimer(context.Context, workflowops.Task) error {
	return nil
}

func (*recordingWorld) EnsureOperation(
	context.Context,
	*workflowops.Lease,
	workflowops.OperationRequest,
) (workflowops.Operation, error) {
	return workflowops.Operation{}, nil
}

func (*recordingWorld) RecordOperation(
	context.Context,
	*workflowops.Lease,
	workflowops.OperationRequest,
	json.RawMessage,
) (json.RawMessage, error) {
	return nil, nil
}

func (*recordingWorld) EnsureWait(
	context.Context,
	*workflowops.Lease,
	workflowops.WaitRequest,
) (workflowops.Operation, error) {
	return workflowops.Operation{}, nil
}

func (*recordingWorld) SuspendRun(context.Context, *workflowops.Lease) error {
	return nil
}

func (*recordingWorld) LeaseDuration() time.Duration {
	return time.Minute
}

func (*recordingWorld) HeartbeatRun(context.Context, *workflowops.Lease) error {
	return nil
}

func (*recordingWorld) CompleteRun(context.Context, *workflowops.Lease, json.RawMessage) error {
	return nil
}

func (*recordingWorld) FailRun(context.Context, *workflowops.Lease, error) error {
	return nil
}

func (*recordingWorld) FinishOperation(
	context.Context,
	workflowops.Operation,
	json.RawMessage,
	error,
) error {
	return nil
}

func (*recordingWorld) RetryOperation(
	context.Context,
	workflowops.Operation,
	error,
	time.Time,
) error {
	return nil
}

func (*recordingWorld) WaitOperation(
	context.Context,
	workflowops.Task,
) (json.RawMessage, error) {
	return nil, nil
}

func (*recordingWorld) RegisterHook(
	context.Context,
	*workflowops.Lease,
	workflowops.HookRequest,
) (workflowops.Hook, error) {
	return workflowops.Hook{}, nil
}

func (*recordingWorld) CloseStreams(context.Context, string) error {
	return nil
}

func TestHTTPHandlerCreatesRootRequest(t *testing.T) {
	client := &recordingWorld{}
	request := httptest.NewRequest(
		http.MethodPost,
		APIPrefix+"/runs",
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
	client := &recordingWorld{}
	request := httptest.NewRequest(
		http.MethodPost,
		APIPrefix+"/runs",
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
