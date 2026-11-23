package worlds_test

import (
	"context"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
	"github.com/fxamacker/cbor/v2"
)

// remoteRequest is the body a RemoteExecutor sends, as the app decodes it.
type remoteRequest struct {
	RPCVersion    int            `cbor:"rpcVersion"`
	QueueName     string         `cbor:"queueName"`
	Message       map[string]any `cbor:"message"`
	MessageID     string         `cbor:"messageId"`
	Attempt       int            `cbor:"attempt"`
	DeliveryCount int            `cbor:"deliveryCount"`
	CreatedAt     time.Time      `cbor:"createdAt"`
}

// remoteApp serves a flow route that records each request and answers with
// status and the CBOR encoding of result, or nothing when result is nil.
func remoteApp(t *testing.T, status int, result any) (*httptest.Server, *[]remoteRequest, *[]http.Header) {
	t.Helper()
	var requests []remoteRequest
	var headers []http.Header
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		var request remoteRequest
		if err := cbor.Unmarshal(body, &request); err != nil {
			t.Errorf("decode the delivery: %v", err)
		}
		requests = append(requests, request)
		headers = append(headers, r.Header.Clone())
		w.WriteHeader(status)
		if result != nil {
			data, _ := cbor.Marshal(result)
			_, _ = w.Write(data)
		}
	}))
	t.Cleanup(server.Close)
	return server, &requests, &headers
}

func TestRemoteExecutorSendsTheSDKMessageUnchanged(t *testing.T) {
	server, requests, headers := remoteApp(t, http.StatusOK, nil)
	message, err := cbor.Marshal(map[string]any{
		"queueName": "__wkf_workflow_workflow//./workflows/a//a",
		"message":   map[string]any{"runId": "wrun_1", "traceCarrier": map[string]any{"traceparent": "00-x"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	created := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	input := worlds.QueueInput{RunID: "wrun_1", WorkflowName: "ignored", SDKMessage: message, MessageID: "msg_1", Attempt: 2, DeliveryCount: 3, CreatedAt: created}
	executor := worlds.RemoteExecutor{URL: server.URL, Token: "secret"}
	if err := executor.Execute(context.Background(), nil, input); err != nil {
		t.Fatal(err)
	}
	got := (*requests)[0]
	if got.QueueName != "__wkf_workflow_workflow//./workflows/a//a" || got.MessageID != "msg_1" || got.Attempt != 2 || got.DeliveryCount != 3 || !got.CreatedAt.Equal(created) || got.RPCVersion != worlds.RPCVersion {
		t.Fatalf("delivery = %+v", got)
	}
	if got.Message["runId"] != "wrun_1" || got.Message["traceCarrier"] == nil {
		t.Fatalf("message = %#v, want the SDK message unchanged", got.Message)
	}
	header := (*headers)[0]
	if header.Get("Authorization") != "Bearer secret" || header.Get("Content-Type") != worlds.RPCContentType {
		t.Fatalf("headers = %v", header)
	}
}

func TestRemoteExecutorBuildsAMessageForAGoDelivery(t *testing.T) {
	server, requests, headers := remoteApp(t, http.StatusOK, nil)
	stepID := worlds.StepID("step_1")
	input := worlds.QueueInput{RunID: "wrun_1", WorkflowName: "workflow//./w//w", StepID: &stepID, StepName: "step//./w//s", RunSpecVersion: 9, Recovery: true}
	if err := (worlds.RemoteExecutor{URL: server.URL}).Execute(context.Background(), nil, input); err != nil {
		t.Fatal(err)
	}
	got := (*requests)[0]
	if got.QueueName != "__wkf_workflow_workflow//./w//w" {
		t.Fatalf("queue name = %q", got.QueueName)
	}
	if got.Message["runId"] != "wrun_1" || got.Message["stepId"] != "step_1" || got.Message["stepName"] != "step//./w//s" || got.Message["recovery"] != true {
		t.Fatalf("message = %#v", got.Message)
	}
	if (*headers)[0].Get("Authorization") != "" {
		t.Fatal("sent an Authorization header without a token")
	}
}

func TestRemoteExecutorMapsTheHandlerResult(t *testing.T) {
	tests := []struct {
		name   string
		result any
		want   *time.Duration
	}{
		{"nothing acknowledges", nil, nil},
		{"null acknowledges", cbor.RawMessage{0xf6}, nil},
		{"other data acknowledges", map[string]any{"ok": true}, nil},
		{"timeoutSeconds redelivers", map[string]any{"timeoutSeconds": 5}, ptr(5 * time.Second)},
		{"fractional timeoutSeconds", map[string]any{"timeoutSeconds": 1.5}, ptr(1500 * time.Millisecond)},
		{"zero is a handoff", map[string]any{"timeoutSeconds": 0}, ptr(time.Duration(0))},
		{"infinity waits the longest", map[string]any{"timeoutSeconds": math.Inf(1)}, ptr(worlds.MaxRemoteRedelivery)},
		{"a long delay is capped", map[string]any{"timeoutSeconds": 86400}, ptr(worlds.MaxRemoteRedelivery)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server, _, _ := remoteApp(t, http.StatusOK, test.result)
			err := (worlds.RemoteExecutor{URL: server.URL}).Execute(context.Background(), nil, worlds.QueueInput{RunID: "wrun_1"})
			redelivery, ok := worlds.RedeliveryOf(err)
			switch {
			case test.want == nil && err != nil:
				t.Fatalf("err = %v, want an acknowledgement", err)
			case test.want != nil && (!ok || redelivery.After != *test.want):
				t.Fatalf("err = %v, want a redelivery after %s", err, *test.want)
			}
		})
	}
}

func TestRemoteExecutorFailsOnRefusals(t *testing.T) {
	server, _, _ := remoteApp(t, http.StatusUnauthorized, nil)
	if err := (worlds.RemoteExecutor{URL: server.URL}).Execute(context.Background(), nil, worlds.QueueInput{RunID: "wrun_1"}); !errors.Is(err, worlds.ErrRemoteUnauthorized) {
		t.Fatalf("401: err = %v, want ErrRemoteUnauthorized", err)
	}
	server, _, _ = remoteApp(t, http.StatusInternalServerError, nil)
	err := (worlds.RemoteExecutor{URL: server.URL}).Execute(context.Background(), nil, worlds.QueueInput{RunID: "wrun_1"})
	if _, redelivery := worlds.RedeliveryOf(err); err == nil || redelivery {
		t.Fatalf("500: err = %v, want an error the World retries", err)
	}
}

