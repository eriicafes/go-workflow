package vercelworld

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

func TestFramesRoundTrip(t *testing.T) {
	var data []byte
	data, err := encodeFrame(data, map[string]any{"eventType": "step_completed", "eventId": "evnt_1", "correlationId": "step_1", "eventData": map[string]any{}, "createdAt": time.Unix(1700000000, 123000000)}, []byte(`{"ok":true}`))
	if err != nil {
		t.Fatal(err)
	}
	data, _ = encodeFrame(data, map[string]any{"_end": 1, "next": "eid:evnt_1", "hasMore": true, "snapshot": map[string]any{"seq": 3, "seqInBand": 2}}, nil)
	page, err := readEventPage(bytes.NewReader(data), http.Header{})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 1 || page.Cursor != "eid:evnt_1" || !page.HasMore || page.SeqInBand == nil || *page.SeqInBand != 2 {
		t.Fatalf("page = %+v", page)
	}
	event := page.Events[0]
	if event.EventType != "step_completed" || string(event.Data["result"].([]byte)) != `{"ok":true}` || !event.CreatedAt.Equal(time.Unix(1700000000, 123000000)) {
		t.Fatalf("event = %+v", event)
	}

	// A body cut short, or one without its end frame, is an error, and so is
	// a terminal error frame.
	if _, err := readEventPage(bytes.NewReader(data[:len(data)-3]), nil); err == nil {
		t.Fatal("truncated page accepted")
	}
	single, _ := encodeFrame(nil, map[string]any{"eventType": "run_started", "eventId": "evnt_2"}, nil)
	if _, err := readEventPage(bytes.NewReader(single), nil); err == nil {
		t.Fatal("page without end frame accepted")
	}
	failed, _ := encodeFrame(nil, map[string]any{"_error": 1, "code": "payload-missing", "message": "gone"}, nil)
	if _, err := readEventPage(bytes.NewReader(failed), nil); codeOf(err) != "payload-missing" {
		t.Fatalf("error frame = %v", err)
	}

	// The snapshot may come in headers instead.
	end, _ := encodeFrame(nil, map[string]any{"_end": 1, "hasMore": false}, nil)
	header := http.Header{}
	header.Set("X-Wf-Snapshot-Seq-In-Band", "7")
	if page, err := readEventPage(bytes.NewReader(end), header); err != nil || page.SeqInBand == nil || *page.SeqInBand != 7 {
		t.Fatalf("header snapshot = %+v err=%v", page, err)
	}
}

func TestClassify(t *testing.T) {
	for _, test := range []struct {
		status    int
		code      string
		eventType worlds.EventType
		want      error
	}{
		{http.StatusPreconditionFailed, "in-band-superseded", worlds.EventTypeStepCreated, worlds.ErrInBandSuperseded},
		{http.StatusNotFound, "run-not-found", worlds.EventTypeRunStarted, worlds.ErrRunNotFound},
		{http.StatusNotFound, "hook-not-found", worlds.EventTypeHookReceived, worlds.ErrHookNotFound},
		{http.StatusConflict, "hook-force-claimed", worlds.EventTypeHookReceived, worlds.ErrHookForceClaimed},
		{http.StatusConflict, "", worlds.EventTypeRunCreated, worlds.ErrRunAlreadyExists},
		{http.StatusConflict, "", worlds.EventTypeStepCompleted, worlds.ErrEventConflict},
		{http.StatusConflict, "run-terminal", worlds.EventTypeStepStarted, worlds.ErrEventConflict},
		{http.StatusGone, "run-expired", worlds.EventTypeStepStarted, worlds.ErrRunTerminal},
		{http.StatusConflict, "", worlds.EventTypeRunCompleted, worlds.ErrEventConflict},
		{http.StatusConflict, "run-terminal", worlds.EventTypeWaitCreated, worlds.ErrEventConflict},
		{http.StatusGone, "", worlds.EventTypeRunFailed, worlds.ErrRunTerminal},
		{http.StatusConflict, "", worlds.EventTypeHookCreated, worlds.ErrEventConflict},
		{http.StatusConflict, "run-already-started", worlds.EventTypeRunStarted, worlds.ErrEventConflict},
		{http.StatusConflict, "entity-conflict", worlds.EventTypeWaitCompleted, worlds.ErrEventConflict},
		{http.StatusBadRequest, "validation-error", worlds.EventTypeAttrSet, worlds.ErrEventInvalid},
	} {
		err := classify(&apiError{Status: test.status, Code: test.code}, test.eventType)
		if !errors.Is(err, test.want) {
			t.Errorf("%d %s on %s = %v, want %v", test.status, test.code, test.eventType, err, test.want)
		}
	}
	// A 404 without a code is a route the backend does not serve.
	if err := classify(&apiError{Status: http.StatusNotFound}, worlds.EventTypeRunStarted); errors.Is(err, worlds.ErrRunNotFound) {
		t.Fatal("uncoded 404 read as a missing run")
	}
}

func TestDecodeAPIError(t *testing.T) {
	err := decodeAPIError(http.StatusPreconditionFailed, "application/json", []byte(`{"error":"in-band-superseded","message":"stale","seqInBand":4}`))
	if err.Code != "in-band-superseded" || err.Message != "stale" || err.Fields["seqInBand"] == nil {
		t.Fatalf("json error = %+v", err)
	}
	encoded, _ := cborEncode.Marshal(map[string]any{"code": "hook-force-claimed", "message": "taken"})
	if err := decodeAPIError(http.StatusConflict, "application/cbor", encoded); err.Code != "hook-force-claimed" || err.Message != "taken" {
		t.Fatalf("cbor error = %+v", err)
	}
	if err := decodeAPIError(http.StatusBadGateway, "text/plain", []byte("upstream down\n")); err.Message != "upstream down" {
		t.Fatalf("text error = %+v", err)
	}
}

func TestTimesSurviveTheWire(t *testing.T) {
	at := time.Date(2026, time.March, 4, 5, 6, 7, 891234000, time.UTC)
	encoded, err := cborEncode.Marshal(map[string]any{"at": at})
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := cborDecode.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if got, ok := asTime(decoded["at"]); !ok || !got.Equal(at) {
		t.Fatalf("time = %v, want %v", got, at)
	}
	if got, ok := asTime("2026-03-04T05:06:07.891Z"); !ok || !got.Equal(at.Truncate(time.Millisecond)) {
		t.Fatalf("string time = %v", got)
	}
	if got, ok := asTime(int64(1700000000123)); !ok || !got.Equal(time.UnixMilli(1700000000123)) {
		t.Fatalf("millisecond time = %v", got)
	}
}

func TestBackendRateLimitIsAThrottleError(t *testing.T) {
	for _, test := range []struct {
		header   http.Header
		throttle bool
		after    time.Duration
	}{
		{http.Header{"Retry-After": {"7"}}, true, 7 * time.Second},
		{http.Header{}, true, 0},
		// A firewall challenge is a transport failure, not the rate limit.
		{http.Header{"X-Vercel-Mitigated": {"challenge"}}, false, 0},
	} {
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			for key, values := range test.header {
				writer.Header()[key] = values
			}
			writer.WriteHeader(http.StatusTooManyRequests)
		}))
		api := &backend{baseURL: server.URL, http: server.Client(), oidc: func(context.Context) (string, error) { return "token", nil }}
		_, err := api.request(context.Background(), http.MethodGet, "/v2/runs/wrun_1", nil, nil, nil)
		server.Close()
		var throttle *worlds.ThrottleError
		if errors.As(err, &throttle) != test.throttle || (test.throttle && throttle.After != test.after) {
			t.Errorf("429 with %v = %v", test.header, err)
		}
		if statusOf(err) != http.StatusTooManyRequests {
			t.Errorf("429 with %v lost its status: %v", test.header, err)
		}
	}
}

func TestInBandRefusalCarriesTheCounters(t *testing.T) {
	api := decodeAPIError(http.StatusPreconditionFailed, "application/json", []byte(`{"error":"in-band-superseded","message":"stale","seq":9,"seqInBand":4}`))
	err := classify(api, worlds.EventTypeStepCreated)
	var superseded *worlds.InBandSupersededError
	if !errors.Is(err, worlds.ErrInBandSuperseded) || !errors.As(err, &superseded) || superseded.EventCount != 9 || superseded.InBandCount != 4 {
		t.Fatalf("classified 412 = %#v", err)
	}
}

// The backend's /v5 event routes, as the World relies on them. TestBackendContract
// checks them against the fake backend, and TestBackendContractLive against a
// real project. TestBackendAssumptionsLive checks the behaviour the World
// assumes and cannot see from the client.

// contractBackend returns a World to address the backend under test with:
// the fake backend, or with live set a real project, when its credentials
// are in the environment (see TestWorkflowsRunEndToEndLive).
func contractBackend(t *testing.T, live bool) *World {
	t.Helper()
	if !live {
		all, _, _ := newWorlds(t, 1)
		return all[0]
	}
	oidc, token := os.Getenv("WORKFLOW_VERCEL_LIVE_OIDC_TOKEN"), os.Getenv("WORKFLOW_VERCEL_LIVE_TOKEN")
	if oidc == "" && token == "" {
		t.Skip("set WORKFLOW_VERCEL_LIVE_OIDC_TOKEN or WORKFLOW_VERCEL_LIVE_TOKEN to run against Vercel")
	}
	world, err := New(Config{
		BaseURL:    "http://localhost/v1/workflow",
		Topic:      "workflow",
		OIDCToken:  staticOIDCToken(oidc),
		Token:      token,
		ProjectID:  os.Getenv("VERCEL_PROJECT_ID"),
		TeamID:     os.Getenv("VERCEL_TEAM_ID"),
		BackendURL: os.Getenv("WORKFLOW_VERCEL_LIVE_BACKEND_URL"),
	})
	if err != nil {
		t.Fatal(err)
	}
	return world
}

// contractRun creates a run of spec version spec through version's route and
// returns its ID.
func contractRun(t *testing.T, world *World, version string, spec int) worlds.RunID {
	t.Helper()
	runID := worlds.NewRunID()
	meta := map[string]any{"eventType": "run_created", "specVersion": spec, "workflowName": "contract", "deploymentId": "dpl_contract", "occurredAt": time.Now().UTC(), "remoteRefBehavior": "lazy"}
	if _, err := contractPost(t, world, version, runID, "run_created", meta, []byte(`"input"`)); err != nil {
		t.Fatalf("create run: %v", err)
	}
	return runID
}

// contractPost writes one event frame through version's route and decodes
// the answer.
func contractPost(t *testing.T, world *World, version string, runID worlds.RunID, eventType string, meta map[string]any, payload []byte) (map[string]any, error) {
	t.Helper()
	meta["eventType"] = eventType
	if _, ok := meta["remoteRefBehavior"]; !ok {
		meta["remoteRefBehavior"] = "lazy"
	}
	body, err := encodeFrame(nil, meta, payload)
	if err != nil {
		t.Fatal(err)
	}
	return contractSend(t, world, "/"+version+"/runs/"+pathEscape(string(runID))+"/events/"+eventType, body)
}

// contractBatch writes the frames of a batch through version's route.
func contractBatch(t *testing.T, world *World, version string, runID worlds.RunID, metas ...map[string]any) (map[string]any, error) {
	t.Helper()
	var body []byte
	for _, meta := range metas {
		var err error
		if body, err = encodeFrame(body, meta, nil); err != nil {
			t.Fatal(err)
		}
	}
	return contractSend(t, world, "/"+version+"/runs/"+pathEscape(string(runID))+"/events/batch", body)
}

func contractSend(t *testing.T, world *World, path string, body []byte) (map[string]any, error) {
	t.Helper()
	header := http.Header{}
	header.Set("Content-Type", "application/octet-stream")
	response, err := world.api.request(context.Background(), http.MethodPost, path, nil, body, header)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	data, err := readAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	var answer map[string]any
	if err := cborDecode.Unmarshal(data, &answer); err != nil {
		t.Fatalf("decode answer: %v", err)
	}
	return answer, nil
}

// contractSnapshot lists a run's whole log through version's route and
// returns its events and the snapshot its headers carry, or nil counters
// when they carry none.
func contractSnapshot(t *testing.T, world *World, version string, runID worlds.RunID) (eventPage, *int, *int) {
	t.Helper()
	header := http.Header{}
	header.Set("Accept", frameContentType)
	response, err := world.api.request(context.Background(), http.MethodGet, "/"+version+"/runs/"+pathEscape(string(runID))+"/events", url.Values{"returnAll": {"true"}, "remoteRefBehavior": {"resolve"}}, nil, header)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	defer response.Body.Close()
	page, err := readEventPage(response.Body, response.Header)
	if err != nil {
		t.Fatal(err)
	}
	counter := func(name string) *int {
		value, err := strconv.Atoi(response.Header.Get(name))
		if err != nil {
			return nil
		}
		return &value
	}
	return page, counter("X-Wf-Snapshot-Seq"), counter("X-Wf-Snapshot-Seq-In-Band")
}

// contractCounters returns a run's seq and seqInBand from a /v5 listing.
func contractCounters(t *testing.T, world *World, runID worlds.RunID) (int, int, eventPage) {
	t.Helper()
	page, seq, seqInBand := contractSnapshot(t, world, "v5", runID)
	if seq == nil || seqInBand == nil || page.SeqInBand == nil || *page.SeqInBand != *seqInBand {
		t.Fatalf("snapshot = seq %v, seqInBand %v, end frame %v; want both headers and a matching end frame", seq, seqInBand, page.SeqInBand)
	}
	if *seqInBand > *seq || *seq != len(page.Events) {
		t.Fatalf("snapshot seq %d, seqInBand %d over %d events", *seq, *seqInBand, len(page.Events))
	}
	return *seq, *seqInBand, page
}

func TestBackendContract(t *testing.T)     { backendContract(t, false) }
func TestBackendContractLive(t *testing.T) { backendContract(t, true) }

func backendContract(t *testing.T, live bool) {
	wait := func(id string) map[string]any {
		return map[string]any{"eventType": "wait_created", "specVersion": 9, "correlationId": id, "resumeAt": time.Now().Add(time.Hour).UTC(), "occurredAt": time.Now().UTC(), "remoteRefBehavior": "lazy"}
	}
	t.Run("ListSnapshot", func(t *testing.T) {
		world := contractBackend(t, live)
		runID := contractRun(t, world, "v5", 9)
		seq, _, _ := contractCounters(t, world, runID)
		if seq != 1 {
			t.Fatalf("seq = %d after run_created, want 1", seq)
		}
	})
	t.Run("StaleInBandWriteIsRefused", func(t *testing.T) {
		world := contractBackend(t, live)
		runID := contractRun(t, world, "v5", 9)
		seq, seqInBand, _ := contractCounters(t, world, runID)
		meta := wait("wait_01STALE")
		meta["inBand"], meta["expectedSeqInBand"] = true, seqInBand+1
		_, err := contractPost(t, world, "v5", runID, "wait_created", meta, nil)
		var refusal *apiError
		if !errors.As(err, &refusal) || refusal.Status != http.StatusPreconditionFailed || refusal.Code != "in-band-superseded" {
			t.Fatalf("stale in-band write err = %v, want 412 in-band-superseded", err)
		}
		if got, _ := asInt(refusal.Fields["seq"]); got != seq {
			t.Errorf("refusal seq = %v, want %d", refusal.Fields["seq"], seq)
		}
		if got, _ := asInt(refusal.Fields["seqInBand"]); got != seqInBand {
			t.Errorf("refusal seqInBand = %v, want %d", refusal.Fields["seqInBand"], seqInBand)
		}
		if after, afterInBand, _ := contractCounters(t, world, runID); after != seq || afterInBand != seqInBand {
			t.Fatalf("refused write moved the counters to %d, %d", after, afterInBand)
		}
	})
	t.Run("InBandWriteNeedsExpectedCount", func(t *testing.T) {
		world := contractBackend(t, live)
		runID := contractRun(t, world, "v5", 9)
		meta := wait("wait_01NOCOUNT")
		meta["inBand"] = true
		_, err := contractPost(t, world, "v5", runID, "wait_created", meta, nil)
		var refusal *apiError
		if !errors.As(err, &refusal) || refusal.Status != http.StatusBadRequest {
			t.Fatalf("in-band write without a count err = %v, want 400", err)
		}
	})
	t.Run("WritesMoveTheirCounters", func(t *testing.T) {
		world := contractBackend(t, live)
		runID := contractRun(t, world, "v5", 9)
		seq, seqInBand, _ := contractCounters(t, world, runID)
		// An out-of-band write moves seq only.
		if _, err := contractPost(t, world, "v5", runID, "wait_created", wait("wait_01OUTOFBAND"), nil); err != nil {
			t.Fatal(err)
		}
		if after, afterInBand, _ := contractCounters(t, world, runID); after != seq+1 || afterInBand != seqInBand {
			t.Fatalf("out-of-band write moved the counters to %d, %d from %d, %d", after, afterInBand, seq, seqInBand)
		}
		// An in-band write decided from slot seq reports the event it skipped.
		meta := wait("wait_01INBAND")
		meta["inBand"], meta["expectedSeqInBand"], meta["maxSlot"] = true, seqInBand, seq
		answer, err := contractPost(t, world, "v5", runID, "wait_created", meta, nil)
		if err != nil {
			t.Fatal(err)
		}
		if allocated, _ := asInt(answer["allocated"]); allocated != 1 {
			t.Errorf("allocated = %v, want 1", answer["allocated"])
		}
		skipped, _ := answer["events"].([]any)
		if len(skipped) != 1 || str(skipped[0].(map[string]any), "correlationId") != "wait_01OUTOFBAND" {
			t.Errorf("skipped-slot report = %v, want the out-of-band wait", answer["events"])
		}
		if more, _ := answer["hasMore"].(bool); more {
			t.Errorf("hasMore = true for a complete report")
		}
		if after, afterInBand, _ := contractCounters(t, world, runID); after != seq+2 || afterInBand != seqInBand+1 {
			t.Fatalf("in-band write moved the counters to %d, %d from %d, %d", after, afterInBand, seq+1, seqInBand)
		}
	})
	t.Run("BatchIsFencedAsOneBlock", func(t *testing.T) {
		world := contractBackend(t, live)
		runID := contractRun(t, world, "v5", 9)
		seq, seqInBand, _ := contractCounters(t, world, runID)
		fenced := func(id string, count int) map[string]any {
			meta := wait(id)
			meta["inBand"], meta["expectedSeqInBand"], meta["maxSlot"] = true, count, seq
			return meta
		}
		_, err := contractBatch(t, world, "v5", runID, fenced("wait_01BATCHA", seqInBand+1), fenced("wait_01BATCHB", seqInBand+1))
		var refusal *apiError
		if !errors.As(err, &refusal) || refusal.Status != http.StatusPreconditionFailed {
			t.Fatalf("stale batch err = %v, want 412", err)
		}
		answer, err := contractBatch(t, world, "v5", runID, fenced("wait_01BATCHA", seqInBand), fenced("wait_01BATCHB", seqInBand))
		if err != nil {
			t.Fatal(err)
		}
		if allocated, _ := asInt(answer["allocated"]); allocated != 2 {
			t.Errorf("allocated = %v, want 2", answer["allocated"])
		}
		if after, afterInBand, _ := contractCounters(t, world, runID); after != seq+2 || afterInBand != seqInBand+2 {
			t.Fatalf("batch moved the counters to %d, %d from %d, %d", after, afterInBand, seq, seqInBand)
		}
	})
	t.Run("Spec9RunsAreWrittenThroughV5", func(t *testing.T) {
		world := contractBackend(t, live)
		runID := contractRun(t, world, "v5", 9)
		_, err := contractPost(t, world, "v4", runID, "wait_created", wait("wait_01V4"), nil)
		var refusal *apiError
		if !errors.As(err, &refusal) || refusal.Status != http.StatusBadRequest {
			t.Fatalf("/v4 write to a spec 9 run err = %v, want 400", err)
		}
	})
}

func TestBackendAssumptions(t *testing.T)     { backendAssumptions(t, false) }
func TestBackendAssumptionsLive(t *testing.T) { backendAssumptions(t, true) }

// backendAssumptions checks what the World assumes of the backend and
// cannot see from the client, against the fake backend, which implements
// them, or with live set against a real project. Each subtest is one
// assumption the Vercel World's documentation lists.
func backendAssumptions(t *testing.T, live bool) {
	t.Run("FenceOnlyOnV5", func(t *testing.T) {
		// A /v4 write to a run below spec 9 is never refused by the fence.
		world := contractBackend(t, live)
		runID := contractRun(t, world, "v4", 8)
		meta := map[string]any{"specVersion": 8, "correlationId": "wait_01V4FENCE", "resumeAt": time.Now().Add(time.Hour).UTC(), "occurredAt": time.Now().UTC(), "inBand": true, "expectedSeqInBand": 1000}
		if _, err := contractPost(t, world, "v4", runID, "wait_created", meta, nil); err != nil {
			t.Fatalf("stale /v4 in-band write err = %v, want it accepted", err)
		}
	})
	t.Run("CreatedAtIsOccurredAt", func(t *testing.T) {
		world := contractBackend(t, live)
		runID := contractRun(t, world, "v5", 9)
		_, seqInBand, _ := contractCounters(t, world, runID)
		occurred := time.Now().Add(-time.Minute).Truncate(time.Millisecond).UTC()
		meta := map[string]any{"specVersion": 9, "correlationId": "wait_01OCCURRED", "resumeAt": time.Now().Add(time.Hour).UTC(), "occurredAt": occurred, "inBand": true, "expectedSeqInBand": seqInBand}
		if _, err := contractPost(t, world, "v5", runID, "wait_created", meta, nil); err != nil {
			t.Fatal(err)
		}
		_, _, page := contractCounters(t, world, runID)
		last := page.Events[len(page.Events)-1]
		if created, ok := asTime(last.CreatedAt); !ok || !created.Equal(occurred) {
			t.Fatalf("stored createdAt = %v, want the occurredAt %v", last.CreatedAt, occurred)
		}
	})
	t.Run("StepCreationFieldsArePersisted", func(t *testing.T) {
		world := contractBackend(t, live)
		runID := contractRun(t, world, "v5", 9)
		_, seqInBand, _ := contractCounters(t, world, runID)
		meta := map[string]any{"specVersion": 9, "correlationId": "step_01PERSISTED", "stepName": "contract", "inline": false, "creatorMessageId": "msg_contract", "occurredAt": time.Now().UTC(), "inBand": true, "expectedSeqInBand": seqInBand}
		if _, err := contractPost(t, world, "v5", runID, "step_created", meta, []byte(`{"args":[1]}`)); err != nil {
			t.Fatal(err)
		}
		_, _, page := contractCounters(t, world, runID)
		data := page.Events[len(page.Events)-1].Data
		if inline, ok := data["inline"].(bool); !ok || inline || data["creatorMessageId"] != "msg_contract" {
			t.Fatalf("stored step_created data = %v, want inline false and creatorMessageId msg_contract", data)
		}
	})
	t.Run("StepChecksOnSpec9Runs", func(t *testing.T) {
		// A finished run refuses a step's creation and start, and still
		// accepts its result.
		world := contractBackend(t, live)
		runID := contractRun(t, world, "v5", 9)
		_, seqInBand, _ := contractCounters(t, world, runID)
		fenced := func(meta map[string]any) map[string]any {
			meta["specVersion"], meta["occurredAt"], meta["inBand"], meta["expectedSeqInBand"] = 9, time.Now().UTC(), true, seqInBand
			return meta
		}
		if _, err := contractPost(t, world, "v5", runID, "step_created", fenced(map[string]any{"correlationId": "step_01CHECKS", "stepName": "contract", "inline": false, "creatorMessageId": "msg_contract"}), []byte(`{"args":[1]}`)); err != nil {
			t.Fatal(err)
		}
		seqInBand++
		if _, err := contractPost(t, world, "v5", runID, "step_started", map[string]any{"specVersion": 9, "correlationId": "step_01CHECKS", "stepName": "contract", "attempt": 1, "occurredAt": time.Now().UTC()}, nil); err != nil {
			t.Fatal(err)
		}
		if _, err := contractPost(t, world, "v5", runID, "run_completed", fenced(map[string]any{}), []byte(`"done"`)); err != nil {
			t.Fatal(err)
		}
		seqInBand++
		if _, err := contractPost(t, world, "v5", runID, "step_started", map[string]any{"specVersion": 9, "correlationId": "step_01CHECKS", "stepName": "contract", "attempt": 2, "occurredAt": time.Now().UTC()}, nil); err == nil {
			t.Error("step_started on a finished run was accepted")
		}
		if _, err := contractPost(t, world, "v5", runID, "step_created", fenced(map[string]any{"correlationId": "step_01LATE", "stepName": "contract", "inline": false, "creatorMessageId": "msg_contract"}), []byte(`{"args":[1]}`)); err == nil {
			t.Error("step_created on a finished run was accepted")
		}
		if _, err := contractPost(t, world, "v5", runID, "step_completed", map[string]any{"specVersion": 9, "correlationId": "step_01CHECKS", "stepName": "contract", "occurredAt": time.Now().UTC()}, []byte(`1`)); err != nil {
			t.Errorf("step_completed on a finished run err = %v, want it accepted", err)
		}
	})
}
