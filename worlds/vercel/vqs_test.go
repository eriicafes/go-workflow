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

var errUnsupported = errors.New("memStore: unsupported command")

type recorded struct {
	Method string
	Path   string
	Header http.Header
	Body   string
}

// recorder is an HTTP server standing in for Vercel Queues and QStash.
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
		_, _ = fmt.Fprint(body, readAll(request))
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

// memStore implements the queue part of redisStore in memory. Other
// commands report an error, or a missing run for scriptLoadRuns.
type memStore struct {
	mu      sync.Mutex
	values  map[string]string
	loadErr error
}

func (s *memStore) Run(_ context.Context, script *redisScript, args ...string) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch script {
	case scriptQueuePut:
		key := args[0] + ":queue:" + args[1]
		_, exists := s.values[key]
		if (args[3] == "NX" && exists) || (args[3] == "XX" && !exists) {
			return int64(0), nil
		}
		s.values[key] = args[2]
		return int64(1), nil
	case scriptQueueTake:
		key := args[0] + ":queue:" + args[1]
		value, ok := s.values[key]
		if !ok {
			return nil, nil
		}
		delete(s.values, key)
		return value, nil
	case scriptLoadRuns:
		if s.loadErr != nil {
			return nil, s.loadErr
		}
		return []any{[]any{[]any{}, []any{}}}, nil
	}
	return nil, errors.New("memStore: unsupported script")
}

func (s *memStore) Get(_ context.Context, key string) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.values[key]
	return value, ok, nil
}

func (s *memStore) record(t *testing.T, id string) (queueRecord, bool) {
	t.Helper()
	raw, ok, _ := s.Get(context.Background(), "p:queue:"+id)
	var record queueRecord
	if ok {
		if err := json.Unmarshal([]byte(raw), &record); err != nil {
			t.Fatal(err)
		}
	}
	return record, ok
}

func (*memStore) MGet(context.Context, ...string) ([]string, error) { return nil, errUnsupported }
func (*memStore) HGet(context.Context, string, string) (string, bool, error) {
	return "", false, errUnsupported
}
func (*memStore) HMGet(context.Context, string, ...string) ([]string, error) {
	return nil, errUnsupported
}
func (*memStore) LIndex(context.Context, string, int64) (string, bool, error) {
	return "", false, errUnsupported
}
func (*memStore) LRange(context.Context, string, int64, int64) ([]string, error) {
	return nil, errUnsupported
}
func (*memStore) SMembers(context.Context, string) ([]string, error) { return nil, errUnsupported }
func (*memStore) Exists(context.Context, string) (bool, error)       { return false, errUnsupported }
func (*memStore) Close() error                                       { return nil }
func (*memStore) ZRangeByScore(context.Context, string, string, string, bool, int64) ([]redisZ, error) {
	return nil, errUnsupported
}

func TestPublishVQS(t *testing.T) {
	server := newRecorder(t, nil)
	world, _ := newQueueWorld(t, server, false)
	world.config.OIDCToken = func(context.Context) (string, error) { return "rotated-token", nil }
	t.Setenv("VERCEL_DEPLOYMENT_ID", "dpl_1")
	if err := world.publishVQS(context.Background(), "01ID", 90*time.Second+time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := world.publishVQS(context.Background(), "02ID", 0); err != nil {
		t.Fatal(err)
	}
	requests := server.all()
	first := requests[0]
	if first.Method != http.MethodPost || first.Path != "/api/v3/topic/workflow" || first.Body != `{"id":"01ID"}` {
		t.Fatalf("publish = %+v", first)
	}
	for header, want := range map[string]string{
		"Authorization":         "Bearer rotated-token",
		"Content-Type":          "application/json",
		"Vqs-Idempotency-Key":   "01ID",
		"Vqs-Delay-Seconds":     "91",
		"Vqs-Retention-Seconds": "604800",
		"Vqs-Deployment-Id":     "dpl_1",
	} {
		if got := first.Header.Get(header); got != want {
			t.Errorf("%s = %q, want %q", header, got, want)
		}
	}
	if got := requests[1].Header.Get("Vqs-Delay-Seconds"); got != "" {
		t.Fatalf("immediate publish has delay %q", got)
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
	world, err := New(Config{BaseURL: "https://app.example.com/v1/workflow", RedisURL: "redis://localhost:6379", QueueURL: "https://iad1.vercel-queue.com/api/v3", Topic: "workflow"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = world.Close() })
	t.Setenv(DefaultOIDCTokenEnv, "default")
	if value, _ := world.config.OIDCToken(ctx); value != "default" {
		t.Fatalf("default token = %q", value)
	}
}

func TestQueueStoresRecordBeforePublishing(t *testing.T) {
	var world *World
	var store *memStore
	server := newRecorder(t, func(request recorded) (int, string) {
		var message vqsMessage
		_ = json.Unmarshal([]byte(request.Body), &message)
		if _, ok := store.record(t, message.ID); !ok {
			t.Errorf("record %s missing at publish time", message.ID)
		}
		return http.StatusCreated, `{"messageId":"msg_1"}`
	})
	world, store = newQueueWorld(t, server, false)
	stepID := worlds.StepID("step_1")
	due := time.Now().Add(time.Hour)
	result, err := world.Queue(context.Background(), worlds.QueueInput{RunID: "wrun_1", StepID: &stepID, NotBefore: &due, IdempotencyKey: "key"})
	if err != nil {
		t.Fatal(err)
	}
	record, ok := store.record(t, result.MessageID)
	if !ok || record.RunID != "wrun_1" || *record.StepID != stepID || !record.NotBefore.Equal(due) || record.IdempotencyKey != "key" {
		t.Fatalf("record = %+v ok=%v", record, ok)
	}
	if delay, _ := strconv.Atoi(server.all()[0].Header.Get("Vqs-Delay-Seconds")); delay < 3599 || delay > 3600 {
		t.Fatalf("delay = %d", delay)
	}
	if err := world.QueueCancel(context.Background(), worlds.QueueCancelInput{MessageID: result.MessageID}); err != nil {
		t.Fatal(err)
	}
	if err := world.QueueCancel(context.Background(), worlds.QueueCancelInput{MessageID: result.MessageID}); !errors.Is(err, worlds.ErrQueueMessageNotFound) {
		t.Fatalf("second cancel = %v", err)
	}
}

func TestQueueRemovesRecordWhenPublishFails(t *testing.T) {
	server := newRecorder(t, func(recorded) (int, string) { return http.StatusTooManyRequests, "slow down" })
	world, store := newQueueWorld(t, server, false)
	_, err := world.Queue(context.Background(), worlds.QueueInput{RunID: "wrun_1"})
	if err == nil || !strings.Contains(err.Error(), "429") {
		t.Fatalf("error = %v", err)
	}
	if len(store.values) != 0 {
		t.Fatalf("records left after failed publish: %v", store.values)
	}
}

func TestQueueLongDelays(t *testing.T) {
	server := newRecorder(t, nil)
	world, store := newQueueWorld(t, server, false)
	due := time.Now().Add(8 * 24 * time.Hour)
	if _, err := world.Queue(context.Background(), worlds.QueueInput{RunID: "wrun_1", NotBefore: &due}); err == nil || !strings.Contains(err.Error(), "QStash") {
		t.Fatalf("long delay without QStash = %v", err)
	}
	if len(store.values) != 0 || len(server.all()) != 0 {
		t.Fatal("long delay without QStash stored or published")
	}
	edge := time.Now().Add(vqsMaxDelay - time.Minute)
	if _, err := world.Queue(context.Background(), worlds.QueueInput{RunID: "wrun_1", NotBefore: &edge}); err != nil {
		t.Fatalf("delay within seven days: %v", err)
	}

	world, store = newQueueWorld(t, server, true)
	result, err := world.Queue(context.Background(), worlds.QueueInput{RunID: "wrun_1", NotBefore: &due})
	if err != nil {
		t.Fatal(err)
	}
	requests := server.all()
	publish := requests[len(requests)-1]
	if publish.Path != "/qstash/v2/publish/https://app.example.com/v1/workflow/qstash" || publish.Header.Get("Authorization") != "Bearer qstash-token" {
		t.Fatalf("QStash publish = %+v", publish)
	}
	if got := publish.Header.Get("Upstash-Not-Before"); got != strconv.FormatInt((due.UnixMilli()+999)/1000, 10) {
		t.Fatalf("Upstash-Not-Before = %q", got)
	}
	if record, _ := store.record(t, result.MessageID); record.QStashMessageID != "msg_1" {
		t.Fatalf("record = %+v", record)
	}
	if err := world.QueueCancel(context.Background(), worlds.QueueCancelInput{MessageID: result.MessageID}); err != nil {
		t.Fatal(err)
	}
	requests = server.all()
	if cancel := requests[len(requests)-1]; cancel.Method != http.MethodDelete || cancel.Path != "/qstash/v2/messages/msg_1" {
		t.Fatalf("QStash cancel = %+v", cancel)
	}
}

func TestQStashHandlerRepublishesToVercelQueue(t *testing.T) {
	server := newRecorder(t, nil)
	world, store := newQueueWorld(t, server, true)
	due := time.Now().Add(time.Hour)
	data, _ := json.Marshal(queueRecord{RunID: "wrun_1", NotBefore: &due, QStashMessageID: "msg_q"})
	store.values["p:queue:01ID"] = string(data)
	handler := HTTP(world)

	body := []byte(`{"id":"01ID"}`)
	request := httptest.NewRequest(http.MethodPost, qstashRoutePath, strings.NewReader(string(body)))
	request.Header.Set("Upstash-Signature", qstashTestSignature(t, body, world.qstashURL, "next"))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("status = %d %s", response.Code, response.Body)
	}
	publish := server.all()[0]
	if publish.Path != "/api/v3/topic/workflow" || publish.Body != `{"id":"01ID"}` || publish.Header.Get("Vqs-Delay-Seconds") == "" {
		t.Fatalf("republish = %+v", publish)
	}
	if record, _ := store.record(t, "01ID"); record.QStashMessageID != "" {
		t.Fatalf("QStash ID kept after release: %+v", record)
	}

	request = httptest.NewRequest(http.MethodPost, qstashRoutePath, strings.NewReader(string(body)))
	request.Header.Set("Upstash-Signature", qstashTestSignature(t, body, "https://evil.example.com/v1/workflow/qstash", "current"))
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("wrong subject status = %d", response.Code)
	}

	missing := []byte(`{"id":"02ID"}`)
	request = httptest.NewRequest(http.MethodPost, qstashRoutePath, strings.NewReader(string(missing)))
	request.Header.Set("Upstash-Signature", qstashTestSignature(t, missing, world.qstashURL, "current"))
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent || len(server.all()) != 1 {
		t.Fatalf("cancelled timer status = %d publishes = %d", response.Code, len(server.all()))
	}
}

func TestQueueConsumerPaths(t *testing.T) {
	server := newRecorder(t, func(recorded) (int, string) { return http.StatusOK, `{"success":true}` })
	world, store := newQueueWorld(t, server, false)
	handler := Queue(world, nil)
	lease := "/api/v3/topic/workflow/consumer/api-queue/lease/rh%2F1"

	// A cancelled record is acknowledged without running.
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, deliveryRequest("msg_1", `{"id":"gone"}`))
	if requests := server.all(); response.Code != http.StatusOK || len(requests) != 1 || requests[0].Method != http.MethodDelete || requests[0].Path != lease {
		t.Fatalf("missing record: status %d requests %+v", response.Code, requests)
	}
	if got := server.all()[0].Header.Get("Authorization"); got != "Bearer static-token" {
		t.Fatalf("ack authorization = %q", got)
	}

	// An early delivery is hidden until due.
	due := time.Now().Add(90 * time.Minute)
	data, _ := json.Marshal(queueRecord{RunID: "wrun_1", NotBefore: &due})
	store.values["p:queue:early"] = string(data)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, deliveryRequest("msg_2", `{"id":"early"}`))
	patch := server.all()[1]
	if response.Code != http.StatusAccepted || patch.Method != http.MethodPatch || patch.Path != lease || patch.Body != `{"visibilityTimeoutSeconds":3600}` {
		t.Fatalf("early delivery: status %d request %+v", response.Code, patch)
	}
	if _, ok := store.record(t, "early"); !ok {
		t.Fatal("early delivery removed its record")
	}

	// A failing execution keeps the record and schedules a retry.
	data, _ = json.Marshal(queueRecord{RunID: "wrun_1"})
	store.values["p:queue:fail"] = string(data)
	store.loadErr = errors.New("redis down")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, deliveryRequest("msg_3", `{"id":"fail"}`))
	retry := server.all()[2]
	if response.Code != http.StatusInternalServerError || retry.Method != http.MethodPatch || retry.Body != `{"visibilityTimeoutSeconds":10}` {
		t.Fatalf("failed delivery: status %d request %+v", response.Code, retry)
	}
	if _, ok := store.record(t, "fail"); !ok {
		t.Fatal("failed delivery removed its record")
	}

	// A delivery for a run that does not exist is dropped.
	store.loadErr = nil
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, deliveryRequest("msg_4", `{"id":"fail"}`))
	if ack := server.all()[3]; response.Code != http.StatusOK || ack.Method != http.MethodDelete {
		t.Fatalf("missing run: status %d request %+v", response.Code, ack)
	}
	if _, ok := store.record(t, "fail"); ok {
		t.Fatal("record kept for a missing run")
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
	world, _ := newQueueWorld(t, server, false)
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
	for count, want := range map[int]time.Duration{0: 5 * time.Second, 1: 5 * time.Second, 2: 10 * time.Second, 4: 40 * time.Second, 20: 5 * time.Minute} {
		if got := retryDelay(count); got != want {
			t.Errorf("retryDelay(%d) = %v, want %v", count, got, want)
		}
	}
	if delaySeconds(time.Millisecond) != 1 || delaySeconds(0) != 0 || delaySeconds(2*time.Second) != 2 {
		t.Fatal("delaySeconds does not round up")
	}
}

// TestQueueConsumerExecutes runs a delivery end to end against the local
// Redis: the run's workflow is unknown, so the runtime fails the run and
// the delivery succeeds.
func TestQueueConsumerExecutes(t *testing.T) {
	world, queue := newWorlds(t, redisTCPConfig(), 1)
	ctx := context.Background()
	if _, err := world[0].EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: "wrun_consumer", WorkflowName: "missing"}); err != nil {
		t.Fatal(err)
	}
	result, err := world[0].Queue(ctx, worlds.QueueInput{RunID: "wrun_consumer"})
	if err != nil {
		t.Fatal(err)
	}
	if published := queue.published(); len(published) != 1 || string(published[0]) != `{"id":"`+result.MessageID+`"}` {
		t.Fatalf("published = %q", published)
	}
	response := httptest.NewRecorder()
	Queue(world[0], nil).ServeHTTP(response, deliveryRequest("msg_1", `{"id":"`+result.MessageID+`"}`))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d %s", response.Code, response.Body)
	}
	run, err := world[0].RunsGet(ctx, worlds.RunsGetInput{RunID: "wrun_consumer"})
	if err != nil || run.Status != worlds.StatusFailed {
		t.Fatalf("run = %+v err=%v", run, err)
	}
	if err := world[0].QueueCancel(ctx, worlds.QueueCancelInput{MessageID: result.MessageID}); !errors.Is(err, worlds.ErrQueueMessageNotFound) {
		t.Fatalf("record after success: %v", err)
	}
}

func readAll(request *http.Request) string {
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

// newQueueWorld returns a World over memStore whose Vercel Queues and QStash
// endpoints are the recorder.
func newQueueWorld(t *testing.T, server *recorder, qstash bool) (*World, *memStore) {
	t.Helper()
	config := Config{BaseURL: "https://app.example.com/v1/workflow", RedisURL: "redis://localhost:6379", QueueURL: server.server.URL + "/api/v3", Topic: "workflow", Prefix: "p", OIDCToken: staticOIDCToken("static-token")}
	if qstash {
		config.QStash = &QStashConfig{BaseURL: server.server.URL + "/qstash", Token: "qstash-token", CurrentSigningKey: "current", NextSigningKey: "next"}
	}
	world, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	_ = world.redis.Close()
	store := &memStore{values: map[string]string{}}
	world.redis = store
	return world, store
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

func staticOIDCToken(token string) func(context.Context) (string, error) {
	return func(context.Context) (string, error) { return token, nil }
}
