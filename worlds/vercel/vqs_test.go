package vercelworld

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"

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
		if strings.HasPrefix(response, "--vqs-test\r\n") {
			writer.Header().Set("Content-Type", "multipart/mixed; boundary=vqs-test")
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
	if id, err := world.publishVQS(context.Background(), queueMessage{ID: "01ID", RunID: "wrun_1"}, vqsSend{IdempotencyKey: "key", ConcurrencyKey: "wrun_1", Delay: 90*time.Second + time.Millisecond}); err != nil || id != "msg_1" {
		t.Fatalf("publish = %q, %v, want Vercel Queues' message ID msg_1", id, err)
	}
	if _, err := world.publishVQS(context.Background(), queueMessage{ID: "02ID", RunID: "wrun_1"}, vqsSend{DeploymentID: "dpl_run", Retention: time.Hour}); err != nil {
		t.Fatal(err)
	}
	requests := server.all()
	first := requests[0]
	if first.Method != http.MethodPost || first.Path != "/api/v3/topic/workflow" || first.Body != `{"id":"01ID","runId":"wrun_1"}` {
		t.Fatalf("publish = %+v", first)
	}
	for header, want := range map[string]string{
		"Authorization":                    "Bearer rotated-token",
		"Content-Type":                     "application/json",
		"Vqs-Idempotency-Key":              "key",
		"Vqs-Experimental-Concurrency-Key": "wrun_1",
		"Vqs-Delay-Seconds":                "91",
		"Vqs-Deployment-Id":                "dpl_1",
		"X-Vercel-Workflow-Run-Id":         "wrun_1",
		"User-Agent":                       world.api.userAgent,
	} {
		if got := first.Header.Get(header); got != want {
			t.Errorf("%s = %q, want %q", header, got, want)
		}
	}
	// A flow's message names its run, and a step's message its step too.
	if got := first.Header.Get("X-Vercel-Workflow-Step-Id"); got != "" {
		t.Fatalf("flow message names step %q", got)
	}
	stepID := worlds.StepID("step_1")
	if _, err := world.publishVQS(context.Background(), queueMessage{ID: "03ID", RunID: "wrun_1", StepID: &stepID}, vqsSend{}); err != nil {
		t.Fatal(err)
	}
	if step := server.all()[2].Header; step.Get("X-Vercel-Workflow-Run-Id") != "wrun_1" || step.Get("X-Vercel-Workflow-Step-Id") != "step_1" {
		t.Fatalf("step message headers = %v", step)
	}
	if got := first.Header.Get("Vqs-Retention-Seconds"); got != "" {
		t.Fatalf("publish sets retention %q, want Vercel's default", got)
	}
	second := requests[1]
	if got := second.Header.Get("Vqs-Delay-Seconds"); got != "" {
		t.Fatalf("immediate publish has delay %q", got)
	}
	if second.Header.Get("Vqs-Idempotency-Key") != "" || second.Header.Get("Vqs-Experimental-Concurrency-Key") != "" || second.Header.Get("Vqs-Retention-Seconds") != "3600" {
		t.Fatalf("second publish headers = %v", second.Header)
	}
	if got := requests[1].Header.Get("Vqs-Deployment-Id"); got != "dpl_run" {
		t.Fatalf("addressed publish deployment = %q, want dpl_run", got)
	}
}

func TestQueueRoundTripsEveryField(t *testing.T) {
	server := newRecorder(t, nil)
	world := newQueueWorld(t, server)
	stepID := worlds.StepID("step_1")
	due := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	resumeAt, hopAt := due.Add(48*time.Hour), due.Add(25*time.Hour)
	sent := worlds.QueueInput{
		RunID: "wrun_1", QueueName: "queue", WorkflowName: "wf", StepID: &stepID, NotBefore: &due, IdempotencyKey: "key", DeploymentID: "dpl_run",
		ReplayDivergenceCount: 2, DeploymentMismatchRetryCount: 1, StepName: "double", StepAttempt: 3, StepResent: true,
		StepCreatedAt:  time.UnixMilli(1_700_000_000_123).UTC(),
		RunSpecVersion: worlds.SpecVersion,
		CompleteWaits:  []string{"wait_1"}, Recovery: true, WaitContinuation: &worlds.WaitContinuation{CorrelationID: "wait_2", Attempt: 1, ResumeAt: &resumeAt, HopAt: &hopAt},
		Retention: worlds.StepRetention,
	}
	result, err := world.Queue(context.Background(), sent)
	if err != nil {
		t.Fatal(err)
	}
	// The result is the message ID Vercel Queues answered with.
	if result.MessageID != "msg_1" {
		t.Fatalf("message ID = %q, want msg_1", result.MessageID)
	}
	publish := server.all()[0]
	var message queueMessage
	if err := json.Unmarshal([]byte(publish.Body), &message); err != nil {
		t.Fatal(err)
	}
	// Every field of the delivery comes back from the message.
	got, want := message.input(), sent
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("published delivery = %+v\nwant %+v", got, want)
	}
	if delay, _ := strconv.Atoi(publish.Header.Get("Vqs-Delay-Seconds")); delay < 3599 || delay > 3600 {
		t.Fatalf("delay = %d", delay)
	}
	// The runtime's key is the message's idempotency key.
	if got := publish.Header.Get("Vqs-Idempotency-Key"); got != "key" {
		t.Fatalf("idempotency key = %q", got)
	}
	if got := publish.Header.Get("Vqs-Retention-Seconds"); got != "604800" {
		t.Fatalf("retention = %q", got)
	}
	if got := publish.Header.Get("Vqs-Deployment-Id"); got != "dpl_run" {
		t.Fatalf("deployment = %q", got)
	}

	// A delivery without a key is published without one, and a delay beyond
	// what the default retention allows is cut to 23 hours: the runtime
	// reaches later timers in hops.
	later := time.Now().Add(10 * 24 * time.Hour)
	if _, err := world.Queue(context.Background(), worlds.QueueInput{RunID: "wrun_1", NotBefore: &later}); err != nil {
		t.Fatal(err)
	}
	long := server.all()[1]
	if got := long.Header.Get("Vqs-Delay-Seconds"); got != strconv.Itoa(int(vqsMaxQueueDelay/time.Second)) {
		t.Fatalf("long delay = %s", got)
	}
	if long.Header.Get("Vqs-Idempotency-Key") != "" || long.Header.Get("Vqs-Retention-Seconds") != "" {
		t.Fatalf("unkeyed publish headers = %v", long.Header)
	}
}

func TestQueueKeysFlowsByTheirRun(t *testing.T) {
	server := newRecorder(t, nil)
	world := newQueueWorld(t, server)
	ctx := context.Background()
	stepID := worlds.StepID("step_1")
	resumeAt, hopAt := time.Now().Add(48*time.Hour), time.Now().Add(time.Hour)
	for _, input := range []worlds.QueueInput{
		{RunID: "wrun_1"},
		{RunID: "wrun_1", StepID: &stepID},
		{RunID: "wrun_1", NotBefore: &hopAt, WaitContinuation: &worlds.WaitContinuation{CorrelationID: "wait_1", ResumeAt: &resumeAt, HopAt: &hopAt}},
		{RunID: "wrun_1", NotBefore: &hopAt, WaitContinuation: &worlds.WaitContinuation{CorrelationID: "wait_1"}},
	} {
		if _, err := world.Queue(ctx, input); err != nil {
			t.Fatal(err)
		}
	}
	// A flow carries its run as its concurrency key, and a step delivery and
	// an intermediate hop of a timer none; the last hop is a flow.
	for index, want := range []string{"wrun_1", "", "", "wrun_1"} {
		if got := server.all()[index].Header.Get("Vqs-Experimental-Concurrency-Key"); got != want {
			t.Errorf("message %d concurrency key = %q, want %q", index, got, want)
		}
	}
}

func TestMessageLifetimeClampsRetentionAndDelay(t *testing.T) {
	for _, test := range []struct {
		delay, retention         time.Duration
		wantDelay, wantRetention int64
	}{
		{0, 0, 0, 0},
		{time.Millisecond, 0, 1, 0},
		{10 * 24 * time.Hour, 0, 23 * 3600, 0},
		{10 * 24 * time.Hour, worlds.StepRetention, 7*24*3600 - 3600, 7 * 24 * 3600},
		{time.Hour, 30 * 24 * time.Hour, 3600, 7 * 24 * 3600},
		{time.Hour, time.Second, 0, 60},
		{time.Hour, 90 * time.Minute, 1800, 5400},
	} {
		delay, retention := messageLifetime(test.delay, test.retention)
		if delay != test.wantDelay || retention != test.wantRetention {
			t.Errorf("messageLifetime(%s, %s) = %d, %d, want %d, %d", test.delay, test.retention, delay, retention, test.wantDelay, test.wantRetention)
		}
	}
}

func TestQueueBatchSendsOneRequestPerDestination(t *testing.T) {
	var mu sync.Mutex
	type batch struct {
		path, deployment string
		parts            []http.Header
		bodies           []string
	}
	var batches []batch
	refuse := ""
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, params, _ := mime.ParseMediaType(request.Header.Get("Content-Type"))
		got := batch{path: request.URL.Path, deployment: request.Header.Get("Vqs-Deployment-Id")}
		results := []map[string]any{}
		reader := multipart.NewReader(request.Body, params["boundary"])
		for {
			part, err := reader.NextRawPart()
			if err != nil {
				break
			}
			data, _ := io.ReadAll(part)
			got.parts, got.bodies = append(got.parts, http.Header(part.Header)), append(got.bodies, string(data))
			var message queueMessage
			_ = json.Unmarshal(data, &message)
			switch {
			case refuse != "" && string(message.RunID) == refuse:
				results = append(results, map[string]any{"status": "failed", "statusCode": 503, "error": "refused", "retryable": true})
			case message.StepID != nil:
				results = append(results, map[string]any{"status": "deferred", "messageId": nil})
			default:
				results = append(results, map[string]any{"status": "sent", "messageId": "msg_" + string(message.RunID)})
			}
		}
		mu.Lock()
		batches = append(batches, got)
		mu.Unlock()
		_ = json.NewEncoder(writer).Encode(map[string]any{"results": results})
	}))
	t.Cleanup(server.Close)
	world, err := New(Config{BaseURL: "https://app.example.com/v1/workflow", QueueURL: server.URL + "/api/v3", Topic: "workflow", Topics: map[string]string{"slow": "workflow-slow"}, Routes: map[string]string{"routed": "slow"}, OIDCToken: staticOIDCToken("static-token"), DeploymentID: "dpl_1"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	stepID := worlds.StepID("step_1")
	results, err := world.QueueBatch(ctx, worlds.QueueBatchInput{Items: []worlds.QueueInput{
		{RunID: "wrun_a", WorkflowName: "w"},
		{RunID: "wrun_b", WorkflowName: "routed"},
		{RunID: "wrun_c", WorkflowName: "w", StepID: &stepID, IdempotencyKey: "step_1", Retention: time.Hour},
	}})
	if err != nil {
		t.Fatal(err)
	}
	// Results are in the order of the items, and a message accepted without an
	// ID has none.
	if want := []worlds.QueueResult{{MessageID: "msg_wrun_a"}, {MessageID: "msg_wrun_b"}, {}}; !reflect.DeepEqual(results, want) {
		t.Fatalf("results = %+v, want %+v", results, want)
	}
	if len(batches) != 2 {
		t.Fatalf("%d requests, want one per topic", len(batches))
	}
	for _, sent := range batches {
		if sent.deployment != "dpl_1" {
			t.Errorf("batch to %s names deployment %q", sent.path, sent.deployment)
		}
		switch sent.path {
		case "/api/v3/topic/workflow/batch":
			if len(sent.parts) != 2 {
				t.Fatalf("default topic batch has %d messages, want 2", len(sent.parts))
			}
			flow, step := sent.parts[0], sent.parts[1]
			if flow.Get("Vqs-Experimental-Concurrency-Key") != "wrun_a" || flow.Get("X-Vercel-Workflow-Run-Id") != "wrun_a" || flow.Get("Content-Type") != "application/json" || !strings.Contains(sent.bodies[0], `"runId":"wrun_a"`) {
				t.Errorf("flow message = %v %s", flow, sent.bodies[0])
			}
			if step.Get("Vqs-Idempotency-Key") != "step_1" || step.Get("Vqs-Retention-Seconds") != "3600" || step.Get("X-Vercel-Workflow-Step-Id") != "step_1" || step.Get("Vqs-Experimental-Concurrency-Key") != "" {
				t.Errorf("step message = %v", step)
			}
		case "/api/v3/topic/workflow-slow/batch":
			if len(sent.parts) != 1 || sent.parts[0].Get("X-Vercel-Workflow-Run-Id") != "wrun_b" {
				t.Errorf("routed batch = %v", sent.parts)
			}
		default:
			t.Errorf("batch sent to %s", sent.path)
		}
	}

	// More messages than one request takes go in several.
	batches = nil
	many := make([]worlds.QueueInput, vqsBatchMax+1)
	for index := range many {
		many[index] = worlds.QueueInput{RunID: worlds.RunID(fmt.Sprintf("wrun_%d", index)), WorkflowName: "w"}
	}
	if results, err := world.QueueBatch(ctx, worlds.QueueBatchInput{Items: many}); err != nil || len(results) != len(many) || len(batches) != 2 || len(batches[0].parts) != vqsBatchMax || len(batches[1].parts) != 1 || results[vqsBatchMax].MessageID != fmt.Sprintf("msg_wrun_%d", vqsBatchMax) {
		t.Fatalf("large batch = %d results in %d requests, %v", len(results), len(batches), err)
	}

	// A message Vercel Queues refuses is reported in its own result, beside
	// the results of the others, with whether sending it again may succeed.
	refuse = "wrun_b"
	items := []worlds.QueueInput{{RunID: "wrun_a", WorkflowName: "w"}, {RunID: "wrun_b", WorkflowName: "w"}}
	results, err = world.QueueBatch(ctx, worlds.QueueBatchInput{Items: items})
	if err != nil || len(results) != 2 || results[0] != (worlds.QueueResult{MessageID: "msg_wrun_a"}) || results[1].MessageID != "" || results[1].Err == nil || !strings.Contains(results[1].Err.Error(), "503: refused") || !results[1].Retryable {
		t.Fatalf("batch with a refused message = %+v, %v", results, err)
	}
	// A caller that needs the whole batch stored gets one error for it.
	var failed *worlds.QueueBatchError
	if _, err := worlds.QueueAll(ctx, world, items); !errors.As(err, &failed) || failed.Failed != 1 || failed.Total != 2 || !failed.Retryable {
		t.Fatalf("QueueAll with a refused message = %v", err)
	}
}

func TestMaxQueueDelayFromEnv(t *testing.T) {
	for value, want := range map[string]time.Duration{"": vqsMaxQueueDelay, "nope": vqsMaxQueueDelay, "0": vqsMaxQueueDelay, "600": 10 * time.Minute} {
		t.Setenv("VERCEL_QUEUE_MAX_DELAY_SECONDS", value)
		if got := maxQueueDelay(); got != want {
			t.Errorf("VERCEL_QUEUE_MAX_DELAY_SECONDS=%q: %v, want %v", value, got, want)
		}
	}
	// It caps the delay of a message without a retention, and is the World's
	// MaxQueueDelay.
	t.Setenv("VERCEL_QUEUE_MAX_DELAY_SECONDS", "600")
	if delay, _ := messageLifetime(time.Hour, 0); delay != 600 {
		t.Fatalf("delay = %ds, want it capped at 600", delay)
	}
	if delay, _ := messageLifetime(3*time.Hour, 6*time.Hour); delay != 3*60*60 {
		t.Fatalf("delay of a message with a retention = %ds, want its own cap", delay)
	}
}

func TestCapabilitiesReportTheQueueContract(t *testing.T) {
	world := newQueueWorld(t, newRecorder(t, nil))
	capabilities := world.Capabilities()
	if capabilities.MaxQueueDelay != 23*time.Hour {
		t.Fatalf("capabilities = %+v", capabilities)
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

	_, err := world.publishVQS(context.Background(), queueMessage{ID: "01ID", RunID: "wrun_1"}, vqsSend{DeploymentID: "dpl_gone"})
	if !errors.Is(err, worlds.ErrDeploymentUnavailable) {
		t.Fatalf("publish error = %v, want ErrDeploymentUnavailable", err)
	}
}

func TestPublishVQSRetriesTransientFailures(t *testing.T) {
	delays := vqsSendRetryDelays
	vqsSendRetryDelays = []time.Duration{time.Millisecond, time.Millisecond}
	t.Cleanup(func() { vqsSendRetryDelays = delays })
	type answer struct {
		status int
		header map[string]string
	}
	var answers []answer
	server := newRecorder(t, func(recorded) (int, string) { return http.StatusCreated, `{"messageId":"msg_1"}` })
	world := newQueueWorld(t, server)
	queue := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		server.mu.Lock()
		count := len(server.requests)
		server.requests = append(server.requests, recorded{Method: request.Method})
		server.mu.Unlock()
		if count < len(answers) {
			for key, value := range answers[count].header {
				writer.Header().Set(key, value)
			}
			if answers[count].status == 0 {
				// A connection the peer closed.
				connection, _, _ := writer.(http.Hijacker).Hijack()
				_ = connection.Close()
				return
			}
			writer.WriteHeader(answers[count].status)
			return
		}
		writer.WriteHeader(http.StatusCreated)
		_, _ = writer.Write([]byte(`{"messageId":"msg_sent"}`))
	}))
	t.Cleanup(queue.Close)
	world.config.QueueURL = queue.URL + "/api/v3"
	publish := func(sent ...answer) (string, int, error) {
		t.Helper()
		server.mu.Lock()
		server.requests = nil
		server.mu.Unlock()
		answers = sent
		id, err := world.publishVQS(context.Background(), queueMessage{ID: "01ID", RunID: "wrun_1"}, vqsSend{DeploymentID: "dpl_1"})
		return id, len(server.all()), err
	}

	// A 5xx and a dropped connection are sent again, up to three attempts.
	if id, attempts, err := publish(answer{status: http.StatusServiceUnavailable}, answer{}); err != nil || id != "msg_sent" || attempts != 3 {
		t.Fatalf("publish = %q, %v after %d attempts, want msg_sent after 3", id, err, attempts)
	}
	if _, attempts, err := publish(answer{status: 500}, answer{status: 500}, answer{status: 500}, answer{}); err == nil || attempts != 3 {
		t.Fatalf("failing publish = %v after %d attempts, want an error after 3", err, attempts)
	}
	// The 502 of a failed consumer discovery is retried, and reported as an
	// unavailable deployment when the retries run out.
	discovery := answer{status: http.StatusBadGateway, header: map[string]string{"Vqs-Error-Code": "consumer_discovery_failed"}}
	if _, attempts, err := publish(discovery, discovery, discovery); !errors.Is(err, worlds.ErrDeploymentUnavailable) || attempts != 3 {
		t.Fatalf("undiscovered publish = %v after %d attempts, want ErrDeploymentUnavailable after 3", err, attempts)
	}
	// A 429 is retried after its Retry-After, unless that is over 5 seconds.
	started := time.Now()
	if _, attempts, err := publish(answer{status: http.StatusTooManyRequests, header: map[string]string{"Retry-After": "0.3"}}); err != nil || attempts != 2 || time.Since(started) < 300*time.Millisecond {
		t.Fatalf("throttled publish = %v after %d attempts in %s", err, attempts, time.Since(started))
	}
	if _, attempts, err := publish(answer{status: http.StatusTooManyRequests}); err != nil || attempts != 2 {
		t.Fatalf("throttled publish without Retry-After = %v after %d attempts, want success after 2", err, attempts)
	}
	if _, attempts, err := publish(answer{status: http.StatusTooManyRequests, header: map[string]string{"Retry-After": "6"}}); err == nil || attempts != 1 {
		t.Fatalf("long-throttled publish = %v after %d attempts, want an error after 1", err, attempts)
	}
	// A 4xx, and the 503 of an unconfigured consumer registry, are Vercel
	// Queues' decision.
	for _, refusal := range []answer{{status: http.StatusBadRequest}, {status: http.StatusUnauthorized}, {status: http.StatusServiceUnavailable, header: map[string]string{"Vqs-Error-Code": "consumer_registry_not_configured"}}} {
		if _, attempts, err := publish(refusal); err == nil || attempts != 1 {
			t.Fatalf("publish refused with %d = %v after %d attempts, want an error after 1", refusal.status, err, attempts)
		}
	}
	// A 202 is a send Vercel Queues took in another region, which has no ID.
	if id, attempts, err := publish(answer{status: http.StatusAccepted}); err != nil || id != "" || attempts != 1 {
		t.Fatalf("failed-over publish = %q, %v after %d attempts, want no ID after 1", id, err, attempts)
	}
}

func TestSendRetryDelay(t *testing.T) {
	ctx := context.Background()
	// The queue request deadline is a transport failure.
	deadline := fmt.Errorf("workflow/vercel: publish queue message: %w", fmt.Errorf("%w after 30s: %w", errQueueRequestDeadline, context.DeadlineExceeded))
	if delay, ok := sendRetryDelay(ctx, deadline, 0); !ok || delay < 100*time.Millisecond || delay >= 200*time.Millisecond {
		t.Fatalf("deadline retry = %s, %v, want 100ms plus jitter", delay, ok)
	}
	if delay, ok := sendRetryDelay(ctx, &vqsSendError{Status: http.StatusInternalServerError}, 5); !ok || delay < 500*time.Millisecond || delay >= 600*time.Millisecond {
		t.Fatalf("late retry = %s, %v, want 500ms plus jitter", delay, ok)
	}
	if delay, ok := sendRetryDelay(ctx, &vqsSendError{Status: http.StatusTooManyRequests, RetryAfter: 2 * time.Second}, 0); !ok || delay != 2*time.Second {
		t.Fatalf("throttled retry = %s, %v, want the Retry-After", delay, ok)
	}
	// An error that is not a transport failure, and the caller's own end,
	// are not retried.
	if _, ok := sendRetryDelay(ctx, errors.New("token"), 0); ok {
		t.Fatal("a token failure is retried")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, ok := sendRetryDelay(canceled, context.Canceled, 0); ok {
		t.Fatal("a canceled send is retried")
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

	// An early delivery is hidden until due, for as long as Vercel Queues
	// honors, and nothing is published for it.
	due := time.Now().Add(8 * 24 * time.Hour)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, deliveryRequest("msg_2", message(queueMessage{ID: "early", RunID: "wrun_1", NotBefore: &due})))
	patch := server.all()[1]
	if response.Code != http.StatusAccepted || len(server.all()) != 2 || patch.Method != http.MethodPatch || patch.Path != lease || patch.Body != `{"visibilityTimeoutSeconds":900}` {
		t.Fatalf("early delivery: status %d request %+v", response.Code, server.all())
	}

	// A failing execution schedules a retry.
	shorten(t, &readRetryBase, time.Millisecond)
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
	// Vercel Queues answers a claim with the message as one multipart part:
	// its lease and counts in the part's headers, its payload as the body.
	message := "--vqs-test\r\n" +
		"Content-Type: application/json\r\nVqs-Message-Id: msg_1\r\nVqs-Receipt-Handle: rh_2\r\nVqs-Delivery-Count: 3\r\nVqs-Experimental-Handoff-Count: 1\r\n" +
		"Vqs-Timestamp: 2026-01-02T03:04:05.000Z\r\nVqs-Expires-At: 2026-01-03T03:04:05.000Z\r\n\r\n" +
		`{"id":"gone"}` + "\r\n--vqs-test--\r\n"
	server := newRecorder(t, func(request recorded) (int, string) {
		switch {
		case strings.HasSuffix(request.Path, "/id/msg_gone"):
			return http.StatusGone, ""
		case strings.Contains(request.Path, "/id/"):
			return http.StatusOK, message
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
	if receive := requests[0]; receive.Path != "/api/v3/topic/workflow/consumer/group/id/msg_1" || receive.Header.Get("Accept") != "multipart/mixed" || receive.Header.Get("Vqs-Visibility-Timeout-Seconds") != "300" {
		t.Fatalf("receive = %+v", receive)
	}
	if ack := requests[1]; ack.Method != http.MethodDelete || ack.Path != "/api/v3/topic/workflow/consumer/group/lease/rh_2" {
		t.Fatalf("ack = %+v", ack)
	}

	claimed, err := world.receiveByID(context.Background(), vqsDelivery{Topic: "workflow", ConsumerGroup: "group", MessageID: "msg_1", Region: RegionIAD1})
	if err != nil || claimed.ReceiptHandle != "rh_2" || string(claimed.Body) != `{"id":"gone"}` || claimed.DeliveryCount != 3 || claimed.HandoffCount != 1 || claimed.CreatedAt.IsZero() || !claimed.ExpiresAt.Equal(claimed.CreatedAt.Add(24*time.Hour)) {
		t.Fatalf("claimed message = %+v, %v", claimed, err)
	}

	// A v2beta delivery without a receipt handle is claimed by ID; one that
	// is already processed is skipped.
	request = deliveryRequest("msg_gone", "")
	request.Header.Del("Ce-Vqsreceipthandle")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || len(server.all()) != 4 {
		t.Fatalf("processed delivery: status %d", response.Code)
	}
}

func TestRetryDelay(t *testing.T) {
	// Each delivery doubles the backoff from a second, less up to 25% jitter,
	// never below Vercel Queues' 5 seconds and 15 minutes at most.
	for count, base := range map[int]time.Duration{0: time.Second, 1: time.Second, 4: 8 * time.Second, 6: 32 * time.Second, 10: 512 * time.Second, 11: 15 * time.Minute, 48: 15 * time.Minute} {
		low, high := max(base*3/4, vqsMinRetry), max(base, vqsMinRetry)
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
	consumerID := worlds.NewRunID()
	if _, err := world[0].EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: consumerID, WorkflowName: "missing"}); err != nil {
		t.Fatal(err)
	}
	if _, err := world[0].Queue(ctx, worlds.QueueInput{RunID: consumerID}); err != nil {
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
	run, err := world[0].RunsGet(ctx, worlds.RunsGetInput{RunID: consumerID})
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

func TestRedeliveryKeepsTheMessageAndCountsAttempts(t *testing.T) {
	server := newRecorder(t, func(recorded) (int, string) { return http.StatusOK, `{"success":true}` })
	world := newQueueWorld(t, server)
	var got worlds.QueueInput
	var result error
	handler := &consumer{world: world, execute: func(_ context.Context, input worlds.QueueInput) error {
		got = input
		return result
	}}
	body, _ := json.Marshal(queueMessage{ID: "01MSG", RunID: "wrun_1"})
	deliver := func(redelivery error) (int, recorded) {
		t.Helper()
		result = redelivery
		request := deliveryRequest("msg_1", string(body))
		request.Header.Set("Ce-Vqsdeliverycount", "5")
		request.Header.Set("Ce-Vqsexperimentalhandoffcount", "2")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		requests := server.all()
		return response.Code, requests[len(requests)-1]
	}

	// The delivery carries Vercel Queues' message ID, its deliveries, and
	// its attempts, which leave out handoffs.
	status, settle := deliver(worlds.Redeliver(30 * time.Second))
	if got.MessageID != "msg_1" || got.DeliveryCount != 5 || got.Attempt != 3 {
		t.Fatalf("delivery = %+v", got)
	}
	// A redelivery hides the message for its delay, unacknowledged.
	if status != http.StatusAccepted || settle.Method != http.MethodPatch || settle.Body != `{"visibilityTimeoutSeconds":30}` || settle.Header.Get("Vqs-Experimental-Handoff") != "" {
		t.Fatalf("redelivery: status %d request %+v", status, settle)
	}
	// A longer one is cut to what Vercel Queues honors.
	if status, settle = deliver(worlds.Redeliver(2 * time.Hour)); status != http.StatusAccepted || settle.Body != `{"visibilityTimeoutSeconds":900}` {
		t.Fatalf("long redelivery: status %d request %+v", status, settle)
	}
	// A redelivery of zero hands the message off.
	if status, settle = deliver(worlds.Redeliver(0)); status != http.StatusAccepted || settle.Method != http.MethodPatch || settle.Body != `{"visibilityTimeoutSeconds":0}` || settle.Header.Get("Vqs-Experimental-Handoff") != "1" {
		t.Fatalf("handoff: status %d request %+v", status, settle)
	}
	for _, request := range server.all() {
		if request.Method == http.MethodDelete {
			t.Fatalf("redelivered message acknowledged: %+v", request)
		}
	}
}

func TestQueueConsumerCoversCoalescedMessages(t *testing.T) {
	// refusals is how many acknowledgments the queue refuses for messages
	// coalesced into the delivery, one more each time; coded drops the error
	// code from the refusals.
	refusals, coded := 0, true
	server := newRecorder(t, nil)
	queue := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		entry := recorded{Method: request.Method, Path: request.URL.EscapedPath(), Header: request.Header.Clone(), Body: readRequestBody(request)}
		server.mu.Lock()
		server.requests = append(server.requests, entry)
		pending := 0
		for _, request := range server.requests {
			if request.Method == http.MethodDelete {
				pending++
			}
		}
		server.mu.Unlock()
		if request.Method == http.MethodDelete && pending <= refusals {
			if coded {
				writer.Header().Set("Vqs-Error-Code", "concurrency_coalesced_pending")
			}
			writer.WriteHeader(http.StatusConflict)
			_, _ = fmt.Fprintf(writer, `{"coalescedCount":%d}`, pending)
			return
		}
		if request.Method == http.MethodPost {
			// A send, which Vercel Queues answers with the message's ID.
			writer.WriteHeader(http.StatusCreated)
			_, _ = writer.Write([]byte(`{"messageId":"msg_continued"}`))
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(queue.Close)
	world := newQueueWorld(t, server)
	world.config.QueueURL = queue.URL + "/api/v3"
	var deliveries []int
	handler := &consumer{world: world, execute: func(_ context.Context, input worlds.QueueInput) error {
		deliveries = append(deliveries, input.DeliveryCount)
		return nil
	}}
	body, _ := json.Marshal(queueMessage{ID: "01MSG", RunID: "wrun_1"})
	createdAt := ""
	deliver := func() int {
		t.Helper()
		server.mu.Lock()
		server.requests = nil
		server.mu.Unlock()
		deliveries = nil
		response := httptest.NewRecorder()
		request := deliveryRequest("msg_1", string(body))
		if createdAt != "" {
			request.Header.Set("Ce-Vqscreatedat", createdAt)
		}
		handler.ServeHTTP(response, request)
		return response.Code
	}

	// An acknowledgment refused for coalesced messages runs the delivery
	// again, as a redelivery would, and then covers them.
	refusals = 2
	if status := deliver(); status != http.StatusOK || !reflect.DeepEqual(deliveries, []int{2, 3, 4}) {
		t.Fatalf("coalesced delivery: status %d deliveries %v", status, deliveries)
	}
	var covered []string
	for _, request := range server.all() {
		if request.Method == http.MethodDelete {
			covered = append(covered, request.Header.Get("Vqs-Experimental-If-Coalesced"))
		}
	}
	if !reflect.DeepEqual(covered, []string{"", "1", "2"}) {
		t.Fatalf("acknowledgments covered %q", covered)
	}

	// A delivery that keeps having messages coalesced into it hands its
	// message off after its last run.
	refusals = 1000
	status := deliver()
	requests := server.all()
	if last := requests[len(requests)-1]; status != http.StatusAccepted || len(deliveries) != vqsMaxCoalescedReruns+1 || last.Method != http.MethodPatch || last.Header.Get("Vqs-Experimental-Handoff") != "1" {
		t.Fatalf("busy delivery: status %d deliveries %d last request %+v", status, len(deliveries), last)
	}

	// A busy delivery of a message near its expiry continues the run on a
	// new message on its last run, before the acknowledgment the queue
	// refuses.
	createdAt = time.Now().Add(-21 * time.Hour).Format(time.RFC3339Nano)
	status = deliver()
	requests = server.all()
	published := -1
	for index, request := range requests {
		if request.Method == http.MethodPost && strings.Contains(request.Body, `"runId":"wrun_1"`) {
			published = index
		}
	}
	if status != http.StatusAccepted || published < 0 || requests[published+1].Method != http.MethodDelete || requests[len(requests)-1].Header.Get("Vqs-Experimental-Handoff") != "1" {
		t.Fatalf("busy delivery near expiry: status %d published at %d of %d", status, published, len(requests))
	}
	createdAt = ""

	// Without the error code a conflict is an ordinary failure.
	refusals, coded = 1, false
	if status := deliver(); status != http.StatusInternalServerError || len(deliveries) != 1 {
		t.Fatalf("conflict: status %d deliveries %v", status, deliveries)
	}
}

func TestVQSRoutesByRegion(t *testing.T) {
	server := newRecorder(t, func(recorded) (int, string) { return http.StatusOK, `{"success":true}` })
	world := newQueueWorld(t, server)
	// Through the API proxy, the header names each request's region.
	world.api.proxy, world.api.project, world.api.env = true, "prj_1", "production"
	world.config.Token = "vercel-token"
	t.Setenv("VERCEL_REGION", "cdg1")
	ctx := context.Background()

	// A message goes to the region its run ID carries, else VERCEL_REGION's.
	tagged := tagRunID("wrun_01ARZ3NDEKTSV4RRFFQ69G5FAV", RegionSYD1)
	// An ID tagged with a region this package does not know carries none.
	unknown, _ := ulid.ParseStrict(strings.TrimPrefix(string(tagged), runIDPrefix))
	unknown[runIDRegionByte] |= runIDRegionMask
	for _, runID := range []worlds.RunID{tagged, "wrun_01ARZ3NDEKTSV4RRFFQ69G5FAV", "wrun_1", worlds.RunID(runIDPrefix + unknown.String())} {
		if _, err := world.publishVQS(ctx, queueMessage{ID: "id", RunID: runID}, vqsSend{}); err != nil {
			t.Fatal(err)
		}
	}
	for index, want := range []string{"syd1", "cdg1", "cdg1", "cdg1"} {
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

	// The proxy takes the access token and the project's headers, under its
	// queues-proxy path unless QueueURL names another root.
	if ack := requests[len(requests)-1]; ack.Header.Get("Authorization") != "Bearer vercel-token" || ack.Header.Get("X-Vercel-Project-Id") != "prj_1" || ack.Header.Get("X-Vercel-Environment") != "production" {
		t.Fatalf("proxied ack headers = %v", ack.Header)
	}
	explicit := world.config.QueueURL
	world.config.QueueURL = ""
	if queue, header := world.queueURL(RegionSYD1); queue != world.api.baseURL+"/queues-proxy/api/v3" || header.Get("X-Vercel-Queue-Region") != "syd1" {
		t.Fatalf("proxied queue = %q, %v", queue, header)
	}
	world.config.QueueURL = explicit

	// Without the proxy, an explicit QueueURL is one endpoint, and each
	// region otherwise has its own host.
	world.api.proxy = false
	if queue, header := world.queueURL(RegionSYD1); queue != world.config.QueueURL || header != nil {
		t.Fatalf("explicit queue = %q, %v", queue, header)
	}
	world.config.QueueURL = ""
	if queue, header := world.queueURL(RegionSYD1); queue != "https://syd1.vercel-queue.com/api/v3" || header != nil {
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

func TestRotationContinuesAMessageNearItsExpiryOrHandoffBound(t *testing.T) {
	now := time.Now()
	for _, test := range []struct {
		name     string
		delivery vqsDelivery
		message  queueMessage
		want     string
	}{
		{"fresh", vqsDelivery{CreatedAt: now.Add(-time.Hour)}, queueMessage{}, ""},
		{"near its default retention", vqsDelivery{CreatedAt: now.Add(-21 * time.Hour)}, queueMessage{}, "expiry"},
		{"far from a longer retention", vqsDelivery{CreatedAt: now.Add(-21 * time.Hour)}, queueMessage{RetentionMs: (48 * time.Hour).Milliseconds()}, ""},
		{"near its reported expiry", vqsDelivery{CreatedAt: now, ExpiresAt: now.Add(time.Hour)}, queueMessage{}, "expiry"},
		{"near the handoff bound", vqsDelivery{HandoffCount: vqsMaxHandoffs - vqsRotationHandoffMargin}, queueMessage{}, "handoff bound"},
		{"nothing known", vqsDelivery{}, queueMessage{}, ""},
	} {
		if got := rotation(test.delivery, test.message, now); got != test.want {
			t.Errorf("%s: rotation = %q, want %q", test.name, got, test.want)
		}
	}
}

func TestHandoffNearExpiryContinuesTheFlowOnANewMessage(t *testing.T) {
	server := newRecorder(t, func(recorded) (int, string) { return http.StatusOK, `{"success":true}` })
	world := newQueueWorld(t, server)
	handler := &consumer{world: world, execute: func(context.Context, worlds.QueueInput) error { return worlds.Redeliver(0) }}
	deliver := func(message queueMessage) (int, []recorded) {
		t.Helper()
		body, _ := json.Marshal(message)
		before := len(server.all())
		request := deliveryRequest("msg_1", string(body))
		request.Header.Set("Ce-Vqsdeliverycount", "3")
		request.Header.Set("Ce-Vqscreatedat", time.Now().Add(-21*time.Hour).Format(time.RFC3339Nano))
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		return response.Code, server.all()[before:]
	}
	flow := queueMessage{ID: "01MSG", RunID: "wrun_1", WorkflowName: "wf", RunSpecVersion: worlds.SpecVersion, ReplayDivergenceCount: 2,
		Recovery: true, CompleteWaits: []string{"wait_1"}}
	status, requests := deliver(flow)
	if status != http.StatusOK || len(requests) != 2 || requests[0].Method != http.MethodPost || requests[1].Method != http.MethodDelete {
		t.Fatalf("status %d requests %+v, want a new message then an acknowledgment", status, requests)
	}
	// The new message is a continuation of the run, not a copy.
	var sent queueMessage
	if err := json.Unmarshal([]byte(requests[0].Body), &sent); err != nil {
		t.Fatal(err)
	}
	if sent.ID == flow.ID || sent.RunID != "wrun_1" || sent.Recovery || sent.ReplayDivergenceCount != 0 || sent.RunSpecVersion != worlds.SpecVersion || len(sent.CompleteWaits) != 1 || sent.NotBefore == nil {
		t.Fatalf("continuation = %+v", sent)
	}
	// A step message is handed off: its key would drop a copy.
	stepID := worlds.StepID("step_1")
	if status, requests := deliver(queueMessage{ID: "01STEP", RunID: "wrun_1", StepID: &stepID}); status != http.StatusAccepted || len(requests) != 1 || requests[0].Header.Get("Vqs-Experimental-Handoff") != "1" {
		t.Fatalf("step message: status %d requests %+v", status, requests)
	}
}
