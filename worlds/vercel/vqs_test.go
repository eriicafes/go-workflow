package vercelworld

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

type recorded struct {
	Method string
	Path   string
	Header http.Header
	Body   string
}

// recorder is an HTTP server standing in for Vercel Queues.
type recorder struct {
	mu       sync.Mutex
	requests []recorded
	respond  func(recorded) (int, string)
	server   *httptest.Server
}

func newRecorder(t *testing.T, respond func(recorded) (int, string)) *recorder {
	t.Helper()
	r := &recorder{respond: respond}
	r.server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body := new(strings.Builder)
		_, _ = fmt.Fprint(body, readRequestBody(request))
		entry := recorded{Method: request.Method, Path: request.URL.EscapedPath(), Header: request.Header.Clone(), Body: body.String()}
		r.mu.Lock()
		r.requests = append(r.requests, entry)
		r.mu.Unlock()
		status, response := http.StatusCreated, `{"messageId":"msg_1"}`
		if r.respond != nil {
			status, response = r.respond(entry)
		}
		writer.WriteHeader(status)
		_, _ = writer.Write([]byte(response))
	}))
	t.Cleanup(r.server.Close)
	return r
}

func (r *recorder) all() []recorded {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]recorded(nil), r.requests...)
}

func TestPublishVQS(t *testing.T) {
	server := newRecorder(t, nil)
	world := newQueueWorld(t, server)
	world.config.OIDCToken = func(context.Context) (string, error) { return "rotated-token", nil }
	world.config.DeploymentID = "dpl_1"
	if err := world.publishVQS(context.Background(), queueMessage{ID: "01ID", RunID: "wrun_1"}, "01ID", 90*time.Second+time.Millisecond, ""); err != nil {
		t.Fatal(err)
	}
	if err := world.publishVQS(context.Background(), queueMessage{ID: "02ID", RunID: "wrun_1"}, "02ID", 0, "dpl_run"); err != nil {
		t.Fatal(err)
	}
	requests := server.all()
	first := requests[0]
	if first.Method != http.MethodPost || first.Path != "/api/v3/topic/workflow" || first.Body != `{"id":"01ID","runId":"wrun_1"}` {
		t.Fatalf("publish = %+v", first)
	}
	for header, want := range map[string]string{
		"Authorization":       "Bearer rotated-token",
		"Content-Type":        "application/json",
		"Vqs-Idempotency-Key": "01ID",
		"Vqs-Delay-Seconds":   "91",
		"Vqs-Deployment-Id":   "dpl_1",
	} {
		if got := first.Header.Get(header); got != want {
			t.Errorf("%s = %q, want %q", header, got, want)
		}
	}
	if got := first.Header.Get("Vqs-Retention-Seconds"); got != "" {
		t.Fatalf("publish sets retention %q, want Vercel's default", got)
	}
	if got := requests[1].Header.Get("Vqs-Delay-Seconds"); got != "" {
		t.Fatalf("immediate publish has delay %q", got)
	}
	if got := requests[1].Header.Get("Vqs-Deployment-Id"); got != "dpl_run" {
		t.Fatalf("addressed publish deployment = %q, want dpl_run", got)
	}
}

func TestQueuePublishesTheDelivery(t *testing.T) {
	server := newRecorder(t, nil)
	world := newQueueWorld(t, server)
	stepID := worlds.StepID("step_1")
	due := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	result, err := world.Queue(context.Background(), worlds.QueueInput{RunID: "wrun_1", StepID: &stepID, NotBefore: &due, IdempotencyKey: "key", DeploymentID: "dpl_run", ReplayDivergenceCount: 2, DeploymentMismatchRetryCount: 1})
	if err != nil {
		t.Fatal(err)
	}
	publish := server.all()[0]
	var message queueMessage
	if err := json.Unmarshal([]byte(publish.Body), &message); err != nil {
		t.Fatal(err)
	}
	input := message.input()
	if input.MessageID != result.MessageID || input.RunID != "wrun_1" || *input.StepID != stepID || !input.NotBefore.Equal(due) || input.IdempotencyKey != "key" || input.DeploymentID != "dpl_run" || input.ReplayDivergenceCount != 2 || input.DeploymentMismatchRetryCount != 1 {
		t.Fatalf("published delivery = %+v", input)
	}
	if delay, _ := strconv.Atoi(publish.Header.Get("Vqs-Delay-Seconds")); delay < 3599 || delay > 3600 {
		t.Fatalf("delay = %d", delay)
	}
	if got := publish.Header.Get("Vqs-Idempotency-Key"); got != result.MessageID {
		t.Fatalf("idempotency key = %q", got)
	}

	// A delay beyond Vercel Queues' limit is published as a 23-hour hop.
	later := time.Now().Add(10 * 24 * time.Hour)
	if _, err := world.Queue(context.Background(), worlds.QueueInput{RunID: "wrun_1", NotBefore: &later}); err != nil {
		t.Fatal(err)
	}
	if got := server.all()[1].Header.Get("Vqs-Delay-Seconds"); got != strconv.Itoa(int(vqsMaxQueueDelay/time.Second)) {
		t.Fatalf("long delay = %s", got)
	}
}

func TestPublishVQSReportsUnavailableDeployment(t *testing.T) {
	server := newRecorder(t, nil)
	world := newQueueWorld(t, server)
	unavailable := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Vqs-Error-Code", "consumer_discovery_failed")
		writer.WriteHeader(http.StatusBadGateway)
	}))
	t.Cleanup(unavailable.Close)
	world.config.QueueURL = unavailable.URL + "/api/v3"

	err := world.publishVQS(context.Background(), queueMessage{ID: "01ID", RunID: "wrun_1"}, "01ID", 0, "dpl_gone")
	if !errors.Is(err, worlds.ErrDeploymentUnavailable) {
		t.Fatalf("publish error = %v, want ErrDeploymentUnavailable", err)
	}
}

func TestOIDCTokenFromEnv(t *testing.T) {
	ctx := context.Background()
	token := OIDCTokenFromEnv("WORKFLOW_TEST_OIDC_TOKEN")
	t.Setenv("WORKFLOW_TEST_OIDC_TOKEN", "")
	if _, err := token(ctx); err == nil {
		t.Fatal("empty variable accepted")
	}
	t.Setenv("WORKFLOW_TEST_OIDC_TOKEN", "first")
	if value, err := token(ctx); err != nil || value != "first" {
		t.Fatalf("token = %q %v", value, err)
	}
	t.Setenv("WORKFLOW_TEST_OIDC_TOKEN", "rotated")
	if value, _ := token(ctx); value != "rotated" {
		t.Fatalf("rotated token = %q", value)
	}
	world, err := New(Config{BaseURL: "https://app.example.com/v1/workflow", Topic: "workflow"})
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(DefaultOIDCTokenEnv, "default")
	if value, _ := world.config.OIDCToken(ctx); value != "default" {
		t.Fatalf("default token = %q", value)
	}
}

func TestQueueConsumerPaths(t *testing.T) {
	server := newRecorder(t, func(recorded) (int, string) { return http.StatusOK, `{"success":true}` })
	world := newQueueWorld(t, server)
	handler := Queue(world, nil)
	lease := "/api/v3/topic/workflow/consumer/api-queue/lease/rh%2F1"
	message := func(value queueMessage) string {
		data, _ := json.Marshal(value)
		return string(data)
	}

	// A message this World did not publish is acknowledged without running.
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, deliveryRequest("msg_1", `{"id":"foreign"}`))
	if requests := server.all(); response.Code != http.StatusOK || len(requests) != 1 || requests[0].Method != http.MethodDelete || requests[0].Path != lease {
		t.Fatalf("foreign message: status %d requests %+v", response.Code, requests)
	}
	if got := server.all()[0].Header.Get("Authorization"); got != "Bearer static-token" {
		t.Fatalf("ack authorization = %q", got)
	}

	// An early delivery is hidden until due.
	due := time.Now().Add(30 * time.Minute)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, deliveryRequest("msg_2", message(queueMessage{ID: "early", RunID: "wrun_1", NotBefore: &due})))
	patch := server.all()[1]
	if response.Code != http.StatusAccepted || patch.Method != http.MethodPatch || patch.Path != lease || patch.Body != `{"visibilityTimeoutSeconds":1800}` {
		t.Fatalf("early delivery: status %d request %+v", response.Code, patch)
	}

	// A failing execution schedules a retry.
	world.api.baseURL = "http://127.0.0.1:1"
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, deliveryRequest("msg_3", message(queueMessage{ID: "fail", RunID: "wrun_1"})))
	retry := server.all()[2]
	if response.Code != http.StatusInternalServerError || retry.Method != http.MethodPatch || retry.Body != `{"visibilityTimeoutSeconds":5}` {
		t.Fatalf("failed delivery: status %d request %+v", response.Code, retry)
	}

	// A delivery for a run that does not exist is dropped.
	world.api.baseURL = newFakeBackend(t).server.URL
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, deliveryRequest("msg_4", message(queueMessage{ID: "missing", RunID: "wrun_missing"})))
	if ack := server.all()[3]; response.Code != http.StatusOK || ack.Method != http.MethodDelete {
		t.Fatalf("missing run: status %d request %+v", response.Code, ack)
	}

	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/queue", strings.NewReader("{}")))
	if response.Code != http.StatusBadRequest {
		t.Fatalf("invalid delivery status = %d", response.Code)
	}
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/queue", nil))
	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET status = %d", response.Code)
	}
}

func TestQueueConsumerReceivesByID(t *testing.T) {
	payload := base64.StdEncoding.EncodeToString([]byte(`{"id":"gone"}`))
	server := newRecorder(t, func(request recorded) (int, string) {
		switch {
		case strings.HasSuffix(request.Path, "/id/msg_gone"):
			return http.StatusGone, ""
		case strings.Contains(request.Path, "/id/"):
			return http.StatusOK, `{"messageId":"msg_1","receiptHandle":"rh_2","deliveryCount":1,"contentType":"application/json","body":"` + payload + `"}` + "\n"
		}
		return http.StatusNoContent, ""
	})
	world := newQueueWorld(t, server)
	handler := Queue(world, nil)

	event := `{"type":"com.vercel.queue.v1beta","data":{"queueName":"workflow","consumerGroup":"group","messageId":"msg_1"}}`
	request := httptest.NewRequest(http.MethodPost, "/queue", strings.NewReader(event))
	request.Header.Set("Content-Type", "application/cloudevents+json; charset=utf-8")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	requests := server.all()
	if response.Code != http.StatusOK || len(requests) != 2 {
		t.Fatalf("structured delivery: status %d requests %+v", response.Code, requests)
	}
	if receive := requests[0]; receive.Path != "/api/v3/topic/workflow/consumer/group/id/msg_1" || receive.Header.Get("Accept") != "application/x-ndjson" || receive.Header.Get("Vqs-Visibility-Timeout-Seconds") != "300" {
		t.Fatalf("receive = %+v", receive)
	}
	if ack := requests[1]; ack.Method != http.MethodDelete || ack.Path != "/api/v3/topic/workflow/consumer/group/lease/rh_2" {
		t.Fatalf("ack = %+v", ack)
	}

	// A v2beta delivery without a receipt handle is claimed by ID; one that
	// is already processed is skipped.
	request = deliveryRequest("msg_gone", "")
	request.Header.Del("Ce-Vqsreceipthandle")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || len(server.all()) != 3 {
		t.Fatalf("processed delivery: status %d", response.Code)
	}
}

func TestRetryDelay(t *testing.T) {
	// Each delivery doubles the backoff from a second, within 25% jitter,
	// never below Vercel Queues' 5 seconds and around 15 minutes at most.
	for count, base := range map[int]time.Duration{0: time.Second, 1: time.Second, 4: 8 * time.Second, 6: 32 * time.Second, 10: 512 * time.Second, 11: 15 * time.Minute, 48: 15 * time.Minute} {
		low, high := max(base*3/4, vqsMinRetry), max(base*5/4, vqsMinRetry)
		for range 50 {
			if got := retryDelay(count); got < low || got > high {
				t.Fatalf("retryDelay(%d) = %v, want between %v and %v", count, got, low, high)
			}
		}
	}
	if delaySeconds(time.Millisecond) != 1 || delaySeconds(0) != 0 || delaySeconds(2*time.Second) != 2 {
		t.Fatal("delaySeconds does not round up")
	}
}

// TestQueueConsumerExecutes runs a delivery end to end against the fake
// backend: the run's workflow is unknown, so the runtime fails the run and
// the delivery succeeds.
func TestQueueConsumerExecutes(t *testing.T) {
	world, queue, _ := newWorlds(t, 1)
	ctx := context.Background()
	if _, err := world[0].EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: "wrun_consumer", WorkflowName: "missing"}); err != nil {
		t.Fatal(err)
	}
	if _, err := world[0].Queue(ctx, worlds.QueueInput{RunID: "wrun_consumer"}); err != nil {
		t.Fatal(err)
	}
	published := queue.published()
	if len(published) != 1 {
		t.Fatalf("published = %q", published)
	}
	response := httptest.NewRecorder()
	Queue(world[0], nil).ServeHTTP(response, deliveryRequest("msg_1", string(published[0])))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d %s", response.Code, response.Body)
	}
	run, err := world[0].RunsGet(ctx, worlds.RunsGetInput{RunID: "wrun_consumer"})
	if err != nil || run.Status != worlds.StatusFailed {
		t.Fatalf("run = %+v err=%v", run, err)
	}
}

func readRequestBody(request *http.Request) string {
	data := new(strings.Builder)
	buffer := make([]byte, 4096)
	for {
		n, err := request.Body.Read(buffer)
		data.Write(buffer[:n])
		if err != nil {
			return data.String()
		}
	}
}

// newQueueWorld returns a World whose Vercel Queues requests go to server.
func newQueueWorld(t *testing.T, server *recorder) *World {
	t.Helper()
	world, err := New(Config{BaseURL: "https://app.example.com/v1/workflow", QueueURL: server.server.URL + "/api/v3", Topic: "workflow", OIDCToken: staticOIDCToken("static-token")})
	if err != nil {
		t.Fatal(err)
	}
	return world
}

func deliveryRequest(messageID, body string) *http.Request {
	request := httptest.NewRequest(http.MethodPost, "/queue", strings.NewReader(body))
	request.Header.Set("Ce-Type", "com.vercel.queue.v2beta")
	request.Header.Set("Ce-Vqsqueuename", "workflow")
	request.Header.Set("Ce-Vqsconsumergroup", "api-queue")
	request.Header.Set("Ce-Vqsmessageid", messageID)
	request.Header.Set("Ce-Vqsreceipthandle", "rh/1")
	request.Header.Set("Ce-Vqsdeliverycount", "2")
	request.Header.Set("Content-Type", "application/json")
	return request
}

func TestQueueConsumerPublishesNextHop(t *testing.T) {
	server := newRecorder(t, func(recorded) (int, string) { return http.StatusOK, `{"success":true}` })
	world := newQueueWorld(t, server)
	handler := Queue(world, nil)
	due := time.Now().Add(8 * 24 * time.Hour)
	long, _ := json.Marshal(queueMessage{ID: "long", RunID: "wrun_1", NotBefore: &due, DeploymentID: "dpl_run"})

	// A delivery due beyond one lease is continued on a new message for the
	// rest of the delay, then acknowledged.
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, deliveryRequest("msg_1", string(long)))
	requests := server.all()
	if response.Code != http.StatusOK || len(requests) != 2 {
		t.Fatalf("hop delivery: status %d requests %+v", response.Code, requests)
	}
	publish, ack := requests[0], requests[1]
	if publish.Method != http.MethodPost || publish.Path != "/api/v3/topic/workflow" || publish.Body != string(long) {
		t.Fatalf("next hop publish = %+v", publish)
	}
	for header, want := range map[string]string{
		"Vqs-Idempotency-Key": "long:msg_1",
		"Vqs-Delay-Seconds":   strconv.Itoa(int(vqsMaxQueueDelay / time.Second)),
		"Vqs-Deployment-Id":   "dpl_run",
	} {
		if got := publish.Header.Get(header); got != want {
			t.Errorf("%s = %q, want %q", header, got, want)
		}
	}
	if ack.Method != http.MethodDelete {
		t.Fatalf("hop delivery not acknowledged: %+v", ack)
	}

	// The last hop is delayed only by what remains.
	due = time.Now().Add(20 * time.Hour)
	last, _ := json.Marshal(queueMessage{ID: "long", RunID: "wrun_1", NotBefore: &due})
	handler.ServeHTTP(httptest.NewRecorder(), deliveryRequest("msg_2", string(last)))
	seconds, _ := strconv.ParseInt(server.all()[2].Header.Get("Vqs-Delay-Seconds"), 10, 64)
	if remaining := time.Duration(seconds) * time.Second; remaining > 20*time.Hour+time.Second || remaining < 20*time.Hour-time.Minute {
		t.Fatalf("last hop delay = %s, want about 20 hours", remaining)
	}
}

func TestVQSRoutesByRegion(t *testing.T) {
	server := newRecorder(t, func(recorded) (int, string) { return http.StatusOK, `{"success":true}` })
	world := newQueueWorld(t, server)
	// Through the API proxy, the header names each request's region.
	world.api.proxy = true
	t.Setenv("VERCEL_REGION", "cdg1")
	ctx := context.Background()

	// A message goes to the region its run ID carries, else VERCEL_REGION's.
	tagged := tagRunID("wrun_01ARZ3NDEKTSV4RRFFQ69G5FAV", worlds.RegionSYD1)
	for _, runID := range []worlds.RunID{tagged, "wrun_01ARZ3NDEKTSV4RRFFQ69G5FAV", "wrun_1"} {
		if err := world.publishVQS(ctx, queueMessage{ID: "id", RunID: runID}, "id", 0, ""); err != nil {
			t.Fatal(err)
		}
	}
	for index, want := range []string{"syd1", "cdg1", "cdg1"} {
		if got := server.all()[index].Header.Get("X-Vercel-Queue-Region"); got != want {
			t.Errorf("message %d region = %q, want %s", index, got, want)
		}
	}

	// A delivery's lease is released in the region that delivered it.
	request := deliveryRequest("msg_1", `{"id":"foreign"}`)
	request.Header.Set("Ce-Vqsregion", "hnd1")
	Queue(world, nil).ServeHTTP(httptest.NewRecorder(), request)
	requests := server.all()
	if ack := requests[len(requests)-1]; ack.Method != http.MethodDelete || ack.Header.Get("X-Vercel-Queue-Region") != "hnd1" {
		t.Fatalf("ack = %+v", ack)
	}

	// Without the proxy, an explicit QueueURL is one endpoint, and each
	// region otherwise has its own host.
	world.api.proxy = false
	if queue, header := world.queueURL(worlds.RegionSYD1); queue != world.config.QueueURL || header != nil {
		t.Fatalf("explicit queue = %q, %v", queue, header)
	}
	world.config.QueueURL = ""
	if queue, header := world.queueURL(worlds.RegionSYD1); queue != "https://syd1.vercel-queue.com/api/v3" || header != nil {
		t.Fatalf("syd1 queue = %q, %v", queue, header)
	}
}

func TestQueueConsumerWaitsForAThrottle(t *testing.T) {
	server := newRecorder(t, func(recorded) (int, string) { return http.StatusOK, `{"success":true}` })
	world := newQueueWorld(t, server)
	backend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Retry-After", "120")
		writer.WriteHeader(http.StatusTooManyRequests)
	}))
	defer backend.Close()
	world.api.baseURL = backend.URL
	data, _ := json.Marshal(queueMessage{ID: "throttled", RunID: "wrun_1"})
	response := httptest.NewRecorder()
	Queue(world, nil).ServeHTTP(response, deliveryRequest("msg_1", string(data)))
	retry := server.all()[0]
	if response.Code != http.StatusInternalServerError || retry.Method != http.MethodPatch || retry.Body != `{"visibilityTimeoutSeconds":120}` {
		t.Fatalf("throttled delivery: status %d request %+v", response.Code, retry)
	}
}
