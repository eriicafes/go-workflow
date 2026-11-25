package worlds_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/core"
	"github.com/eriicafes/go-workflow/worlds/testworld"
)

func TestRunnerSetAnswersAWorkflowSDKHealthCheck(t *testing.T) {
	world := testworld.New(worlds.Runners())
	defer world.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	probe := worlds.QueueInput{SDKMessage: []byte(`{"queueName":"__wkf_workflow_health_check","message":{"__healthCheck":true,"correlationId":"probe_01"}}`)}
	check, ok := worlds.ParseHealthCheck(probe)
	if !ok || check.CorrelationID != "probe_01" || check.ProbeRunID != "" {
		t.Fatalf("ParseHealthCheck = %+v, %t", check, ok)
	}
	if err := worlds.Runners().Execute(ctx, core.NewRuntime(world), probe); err != nil {
		t.Fatal(err)
	}
	chunks, err := world.StreamsGetChunks(ctx, worlds.StreamsGetChunksInput{RunID: worlds.HealthCheckRunID("probe_01"), Name: worlds.HealthCheckStreamName("probe_01"), Wait: time.Second})
	if err != nil || len(chunks.Data) != 1 || !chunks.Done {
		t.Fatalf("answer = %+v, %v, want one chunk, done", chunks, err)
	}
	var answer map[string]any
	if err := json.Unmarshal(chunks.Data[0].Data, &answer); err != nil {
		t.Fatal(err)
	}
	if answer["healthy"] != true || answer["correlationId"] != "probe_01" || answer["specVersion"] != float64(worlds.SpecVersion) || answer["timestamp"] == nil {
		t.Fatalf("answer = %v", answer)
	}
	// Other deliveries without a run are acknowledged.
	for _, message := range []string{"", `{"queueName":"__wkf_workflow_x","message":{"other":true}}`} {
		if err := worlds.Runners().Execute(ctx, core.NewRuntime(world), worlds.QueueInput{SDKMessage: []byte(message)}); err != nil {
			t.Fatalf("delivery %q = %v", message, err)
		}
	}
}

func TestWithHealthCheckAnswersTheHealthQuery(t *testing.T) {
	handler := worlds.WithHealthCheck(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	}))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/.well-known/workflow/v1/flow?__health", nil))
	var body map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if recorder.Code != http.StatusOK || recorder.Header().Get("Content-Type") != "application/json" || recorder.Header().Get("Access-Control-Allow-Origin") != "*" ||
		len(body) != 3 || body["healthy"] != true || body["endpoint"] != "/.well-known/workflow/v1/flow" || body["specVersion"] != float64(worlds.SpecVersion) {
		t.Fatalf("health = %d %v %s", recorder.Code, recorder.Header(), recorder.Body)
	}
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodOptions, "/.well-known/workflow/v1/flow?__health", nil))
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("preflight = %d", recorder.Code)
	}
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/.well-known/workflow/v1/flow", nil))
	if recorder.Code != http.StatusTeapot {
		t.Fatalf("flow request = %d, want the wrapped handler's", recorder.Code)
	}
}
