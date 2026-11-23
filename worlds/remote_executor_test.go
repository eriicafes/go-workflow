package worlds_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// remoteRequest is a request a RemoteExecutor sent, as the app received it.
type remoteRequest struct {
	header http.Header
	body   string
}

// remoteApp serves a flow route that records each request and answers with
// status and body.
func remoteApp(t *testing.T, status int, body string) (*httptest.Server, *[]remoteRequest) {
	t.Helper()
	var requests []remoteRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		requests = append(requests, remoteRequest{header: r.Header.Clone(), body: string(data)})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(server.Close)
	return server, &requests
}

func TestRemoteExecutorSendsTheSDKMessageUnchanged(t *testing.T) {
	server, requests := remoteApp(t, http.StatusOK, `{"ok":true}`)
	message := `{"runId":"wrun_1","traceCarrier":{"traceparent":"00-x"},"stepInput":{"__type":"Uint8Array","data":"AQID"}}`
	sdk, err := json.Marshal(map[string]any{"queueName": "__wkf_workflow_workflow//./workflows/a//a", "message": json.RawMessage(message)})
	if err != nil {
		t.Fatal(err)
	}
	created := time.Date(2026, 1, 2, 3, 4, 5, 6_000_000, time.UTC)
	input := worlds.QueueInput{RunID: "wrun_1", WorkflowName: "ignored", SDKMessage: sdk, MessageID: "msg_1", Attempt: 2, DeliveryCount: 3, CreatedAt: created}
	if err := (worlds.RemoteExecutor{URL: server.URL, Token: "secret"}).Execute(context.Background(), nil, input); err != nil {
		t.Fatal(err)
	}
	got := (*requests)[0]
	if got.body != message {
		t.Fatalf("body = %s, want the SDK message unchanged", got.body)
	}
	want := map[string]string{
		"Authorization":                "Bearer secret",
		"Content-Type":                 "application/json",
		"x-vqs-queue-name":             "__wkf_workflow_workflow//./workflows/a//a",
		"x-vqs-message-id":             "msg_1",
		"x-vqs-message-attempt":        "2",
		"x-vqs-message-delivery-count": "3",
		"x-vqs-message-created-at":     "2026-01-02T03:04:05.006Z",
	}
	for name, value := range want {
		if got.header.Get(name) != value {
			t.Errorf("%s = %q, want %q", name, got.header.Get(name), value)
		}
	}
}

func TestRemoteExecutorBuildsAMessageForAGoDelivery(t *testing.T) {
	server, requests := remoteApp(t, http.StatusOK, `{"ok":true}`)
	stepID := worlds.StepID("step_1")
	input := worlds.QueueInput{RunID: "wrun_1", WorkflowName: "workflow//./w//w", StepID: &stepID, StepName: "step//./w//s", RunSpecVersion: 9, Recovery: true, MessageID: "msg_1"}
	if err := (worlds.RemoteExecutor{URL: server.URL}).Execute(context.Background(), nil, input); err != nil {
		t.Fatal(err)
	}
	got := (*requests)[0]
	if got.header.Get("x-vqs-queue-name") != "__wkf_workflow_workflow//./w//w" || got.header.Get("x-vqs-message-attempt") != "1" {
		t.Fatalf("headers = %v", got.header)
	}
	var message map[string]any
	if err := json.Unmarshal([]byte(got.body), &message); err != nil {
		t.Fatal(err)
	}
	if message["runId"] != "wrun_1" || message["stepId"] != "step_1" || message["stepName"] != "step//./w//s" || message["recovery"] != true || message["runSpecVersion"] != 9.0 {
		t.Fatalf("message = %#v", message)
	}
	if got.header.Get("Authorization") != "" {
		t.Fatal("sent an Authorization header without a token")
	}
}

func TestRemoteExecutorMapsTheHandlerResult(t *testing.T) {
	tests := []struct {
		name string
		body string
		want *time.Duration
	}{
		{"ok acknowledges", `{"ok":true}`, nil},
		{"null acknowledges", `null`, nil},
		{"nothing acknowledges", ``, nil},
		{"timeoutSeconds redelivers", `{"timeoutSeconds":5}`, ptr(5 * time.Second)},
		{"fractional timeoutSeconds", `{"timeoutSeconds":1.5}`, ptr(1500 * time.Millisecond)},
		{"zero is a handoff", `{"timeoutSeconds":0}`, ptr(time.Duration(0))},
		{"a long delay is capped", `{"timeoutSeconds":86400}`, ptr(worlds.MaxRemoteRedelivery)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server, _ := remoteApp(t, http.StatusOK, test.body)
			err := (worlds.RemoteExecutor{URL: server.URL}).Execute(context.Background(), nil, worlds.QueueInput{RunID: "wrun_1", MessageID: "msg_1"})
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
	server, _ := remoteApp(t, http.StatusUnauthorized, `{"error":"no"}`)
	if err := (worlds.RemoteExecutor{URL: server.URL}).Execute(context.Background(), nil, worlds.QueueInput{RunID: "wrun_1", MessageID: "msg_1"}); !errors.Is(err, worlds.ErrRemoteUnauthorized) {
		t.Fatalf("401: err = %v, want ErrRemoteUnauthorized", err)
	}
	server, _ = remoteApp(t, http.StatusInternalServerError, `"Error: boom"`)
	err := (worlds.RemoteExecutor{URL: server.URL}).Execute(context.Background(), nil, worlds.QueueInput{RunID: "wrun_1", MessageID: "msg_1"})
	if _, redelivery := worlds.RedeliveryOf(err); err == nil || redelivery {
		t.Fatalf("500: err = %v, want an error the World retries", err)
	}
}
