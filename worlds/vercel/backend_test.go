package vercelworld

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"sync/atomic"
	"syscall"
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
		{http.StatusPreconditionFailed, "in-band-superseded", worlds.EventTypeStepCreated, &worlds.InBandSupersededError{}},
		{http.StatusNotFound, "run-not-found", worlds.EventTypeRunStarted, worlds.ErrRunNotFound},
		{http.StatusNotFound, "hook-not-found", worlds.EventTypeHookReceived, &worlds.HookNotFoundError{}},
		{http.StatusConflict, "hook-force-claimed", worlds.EventTypeHookReceived, &worlds.HookForceClaimedError{}},
		{http.StatusConflict, "", worlds.EventTypeRunCreated, worlds.ErrRunAlreadyExists},
		{http.StatusConflict, "", worlds.EventTypeStepCompleted, worlds.ErrEntityConflict},
		{http.StatusConflict, "run-terminal", worlds.EventTypeStepStarted, worlds.ErrEntityConflict},
		{http.StatusGone, "run-expired", worlds.EventTypeStepStarted, &worlds.RunExpiredError{}},
		{http.StatusConflict, "", worlds.EventTypeRunCompleted, worlds.ErrEntityConflict},
		{http.StatusConflict, "run-terminal", worlds.EventTypeWaitCreated, worlds.ErrEntityConflict},
		{http.StatusGone, "", worlds.EventTypeRunFailed, &worlds.RunExpiredError{}},
		{http.StatusConflict, "", worlds.EventTypeHookCreated, worlds.ErrEntityConflict},
		{http.StatusConflict, "run-already-started", worlds.EventTypeRunStarted, worlds.ErrEntityConflict},
		{http.StatusConflict, "entity-conflict", worlds.EventTypeWaitCompleted, worlds.ErrEntityConflict},
		{http.StatusBadRequest, "validation-error", worlds.EventTypeAttrSet, worlds.ErrEventInvalid},
	} {
		err := classify(&apiError{Status: test.status, Code: test.code}, test.eventType)
		if !matchesError(err, test.want) {
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
		_, err := api.request(context.Background(), entityRequest, http.MethodGet, "/v2/runs/wrun_1", nil, nil, nil)
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
	if !errors.As(err, &superseded) || superseded.EventCount != 9 || superseded.InBandCount != 4 {
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
	world, err := New(liveConfig(t))
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
	meta := map[string]any{"eventType": "run_created", "specVersion": spec, "workflowName": "contract", "deploymentId": "dpl_contract", "occurredAt": time.Now().UTC(), "remoteRefBehavior": "lazy", "inBand": false}
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
	response, err := world.api.request(context.Background(), eventsRequest, http.MethodPost, path, nil, body, header)
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
	response, err := world.api.request(context.Background(), eventsRequest, http.MethodGet, "/"+version+"/runs/"+pathEscape(string(runID))+"/events", url.Values{"returnAll": {"true"}, "remoteRefBehavior": {"resolve"}}, nil, header)
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
		return map[string]any{"eventType": "wait_created", "specVersion": 9, "correlationId": id, "resumeAt": time.Now().Add(time.Hour).UTC(), "occurredAt": time.Now().UTC(), "remoteRefBehavior": "lazy", "inBand": false}
	}
	t.Run("WriteDeclaresWhetherItIsInBand", func(t *testing.T) {
		world := contractBackend(t, live)
		runID := contractRun(t, world, "v5", 9)
		// A write without inBand is refused.
		meta := wait("wait_01UNDECLARED")
		delete(meta, "inBand")
		_, err := contractPost(t, world, "v5", runID, "wait_created", meta, nil)
		var api *apiError
		if !errors.As(err, &api) || api.Status != http.StatusBadRequest || api.Code != "v5-missing-in-band" {
			t.Fatalf("undeclared write = %v, want 400 v5-missing-in-band", err)
		}
	})
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
		if _, err := contractPost(t, world, "v5", runID, "step_started", map[string]any{"specVersion": 9, "correlationId": "step_01CHECKS", "stepName": "contract", "attempt": 1, "occurredAt": time.Now().UTC(), "inBand": false}, nil); err != nil {
			t.Fatal(err)
		}
		if _, err := contractPost(t, world, "v5", runID, "run_completed", fenced(map[string]any{}), []byte(`"done"`)); err != nil {
			t.Fatal(err)
		}
		seqInBand++
		if _, err := contractPost(t, world, "v5", runID, "step_started", map[string]any{"specVersion": 9, "correlationId": "step_01CHECKS", "stepName": "contract", "attempt": 2, "occurredAt": time.Now().UTC(), "inBand": false}, nil); err == nil {
			t.Error("step_started on a finished run was accepted")
		}
		if _, err := contractPost(t, world, "v5", runID, "step_created", fenced(map[string]any{"correlationId": "step_01LATE", "stepName": "contract", "inline": false, "creatorMessageId": "msg_contract"}), []byte(`{"args":[1]}`)); err == nil {
			t.Error("step_created on a finished run was accepted")
		}
		if _, err := contractPost(t, world, "v5", runID, "step_completed", map[string]any{"specVersion": 9, "correlationId": "step_01CHECKS", "stepName": "contract", "occurredAt": time.Now().UTC(), "inBand": false}, []byte(`1`)); err != nil {
			t.Errorf("step_completed on a finished run err = %v, want it accepted", err)
		}
	})
}

// matchesError reports whether err matches want: a sentinel by errors.Is,
// and an error type by having one of that type in its chain.
func matchesError(err, want error) bool {
	if reflect.TypeOf(want) == reflect.TypeOf(errors.New("")) {
		return errors.Is(err, want)
	}
	return errors.As(err, reflect.New(reflect.TypeOf(want)).Interface())
}

func TestClassifyReadsRefusalDetailsAtTheTopLevel(t *testing.T) {
	api := decodeAPIError(http.StatusConflict, "application/json", []byte(`{"error":"start-hook-conflict","message":"taken","token":"tok","conflictingRunId":"wrun_owner"}`))
	var conflict *worlds.HookConflictError
	if err := classify(api, worlds.EventTypeRunCreated); !errors.As(err, &conflict) || !conflict.Start || conflict.Token != "tok" || conflict.RunID != "wrun_owner" {
		t.Fatalf("start-hook-conflict = %#v, want the token and its owner", err)
	}
	api = decodeAPIError(http.StatusConflict, "application/json", []byte(`{"error":"hook-force-claimed","message":"taken","claimedBy":{"runId":"wrun_new","hookId":"hook_new"}}`))
	var claimed *worlds.HookForceClaimedError
	if err := classify(api, worlds.EventTypeHookReceived); !errors.As(err, &claimed) || claimed.RunID != "wrun_new" || claimed.HookID != "hook_new" {
		t.Fatalf("hook-force-claimed = %#v, want the hook that took the token", err)
	}
}

func TestBackendDoesNotFollowRedirects(t *testing.T) {
	var followed atomic.Int32
	other := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { followed.Add(1) }))
	defer other.Close()
	world := newBackendWorld(t, func(writer http.ResponseWriter, request *http.Request) {
		http.Redirect(writer, request, other.URL+"/elsewhere", http.StatusTemporaryRedirect)
	})
	if _, err := world.RunsGet(context.Background(), worlds.RunsGetInput{RunID: "wrun_1"}); statusOf(err) != http.StatusTemporaryRedirect {
		t.Fatalf("redirected read = %v, want the backend's 307", err)
	}
	if followed.Load() != 0 {
		t.Fatal("the redirect was followed with the request's credentials")
	}
}

// shorten sets *value to short for the test.
func shorten(t *testing.T, value *time.Duration, short time.Duration) {
	t.Helper()
	old := *value
	*value = short
	t.Cleanup(func() { *value = old })
}

func TestBackendRetriesATransientRead(t *testing.T) {
	shorten(t, &readRetryBase, time.Millisecond)
	for _, test := range []struct {
		kind     requestKind
		method   string
		status   int
		requests int32
	}{
		{entityRequest, http.MethodGet, http.StatusBadGateway, maxReadRetries + 1},
		{eventsRequest, http.MethodGet, http.StatusServiceUnavailable, maxReadRetries + 1},
		// Only a 500, 502, 503 or 504 is worth another attempt.
		{entityRequest, http.MethodGet, http.StatusNotImplemented, 1},
		{entityRequest, http.MethodGet, http.StatusNotFound, 1},
		// A write is only sent again by retryEventWrite, and a stream request
		// by the streamer.
		{eventsRequest, http.MethodPost, http.StatusBadGateway, 1},
		{streamRequest, http.MethodGet, http.StatusBadGateway, 1},
	} {
		var requests atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			requests.Add(1)
			writer.WriteHeader(test.status)
		}))
		api := &backend{baseURL: server.URL, http: server.Client(), oidc: func(context.Context) (string, error) { return "token", nil }}
		_, err := api.request(context.Background(), test.kind, test.method, "/v2/runs/wrun_1", nil, nil, nil)
		server.Close()
		if statusOf(err) != test.status || requests.Load() != test.requests {
			t.Errorf("%s answered %d: %d requests and %v, want %d requests", test.method, test.status, requests.Load(), err, test.requests)
		}
	}
}

func TestBackendRetriesAFailedConnection(t *testing.T) {
	shorten(t, &readRetryBase, time.Millisecond)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) { writeCBOR(writer, map[string]any{}) }))
	defer server.Close()
	var dials atomic.Int32
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true, DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		if dials.Add(1) <= 2 {
			return nil, &net.OpError{Op: "dial", Net: network, Err: syscall.ECONNREFUSED}
		}
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}}}
	api := &backend{baseURL: server.URL, http: client, oidc: func(context.Context) (string, error) { return "token", nil }}
	if err := api.do(context.Background(), http.MethodGet, "/v2/runs/wrun_1", nil, nil, nil); err != nil || dials.Load() != 3 {
		t.Fatalf("read over a connection refused twice = %v after %d dials, want success on the third", err, dials.Load())
	}
	// A write that never connected is left to its caller.
	dials.Store(1)
	if err := api.do(context.Background(), http.MethodPost, "/v4/runs/purge", nil, map[string]any{}, nil); err == nil || dials.Load() != 2 {
		t.Fatalf("write over a refused connection = %v after %d dials, want its failure", err, dials.Load()-1)
	}
}

func TestEntityReadRetriesAnUnreadableAnswer(t *testing.T) {
	var requests atomic.Int32
	garbled := int32(maxRequestRetries)
	world := newBackendWorld(t, func(writer http.ResponseWriter, _ *http.Request) {
		if requests.Add(1) <= garbled {
			writer.Header().Set("Content-Type", cborContentType)
			_, _ = writer.Write([]byte{0xff, 0xff})
			return
		}
		writeCBOR(writer, map[string]any{"runId": "wrun_1", "status": "running"})
	})
	run, err := world.getRun(context.Background(), "wrun_1", worlds.ResolveDataNone, "", 0)
	if err != nil || run.RunID != "wrun_1" || requests.Load() != garbled+1 {
		t.Fatalf("read of a garbled answer = %+v, %v after %d requests, want the run on request %d", run, err, requests.Load(), garbled+1)
	}
	requests.Store(0)
	garbled = maxRequestRetries + 1
	if _, err := world.getRun(context.Background(), "wrun_1", worlds.ResolveDataNone, "", 0); err == nil || requests.Load() != garbled {
		t.Fatalf("read of an answer garbled every time = %v after %d requests", err, requests.Load())
	}
	// A write's answer is not asked for again: the write may have landed.
	requests.Store(0)
	var out map[string]any
	if err := world.api.do(context.Background(), http.MethodPost, "/v4/runs/purge", nil, map[string]any{}, &out); err == nil || requests.Load() != 1 {
		t.Fatalf("write with a garbled answer = %v after %d requests, want one", err, requests.Load())
	}
}

func TestRequestTimeout(t *testing.T) {
	for value, want := range map[string]time.Duration{"": defaultRequestTimeout, "x": defaultRequestTimeout, "30000": 30 * time.Second, "1": minRequestTimeout, "999999": maxRequestTimeout} {
		t.Setenv("WORKFLOW_REQUEST_TIMEOUT_MS", value)
		if got := requestTimeout(); got != want {
			t.Errorf("WORKFLOW_REQUEST_TIMEOUT_MS=%q: %v, want %v", value, got, want)
		}
	}
}

func TestBackendRequestsHaveADeadline(t *testing.T) {
	t.Setenv("WORKFLOW_REQUEST_TIMEOUT_MS", "")
	shorten(t, &defaultRequestTimeout, 60*time.Millisecond)
	shorten(t, &eventsPhaseTimeout, 60*time.Millisecond)
	shorten(t, &readRetryBase, time.Millisecond)
	ctx := context.Background()
	var requests atomic.Int32
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	flush := func(writer http.ResponseWriter, data []byte) {
		_, _ = writer.Write(data)
		writer.(http.Flusher).Flush()
	}
	end, _ := encodeFrame(nil, map[string]any{"_end": 1, "hasMore": false}, nil)
	slot := func(index int) []byte {
		data, _ := encodeFrame(nil, map[string]any{"runId": "wrun_1", "eventId": string(worlds.EventIDForSlot(uint64(index))), "eventType": "run_cancelled", "createdAt": time.Now()}, nil)
		return data
	}
	var mode atomic.Value
	world := newBackendWorld(t, func(writer http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		switch mode.Load() {
		case "silent":
			select {
			case <-release:
			case <-request.Context().Done():
			}
		case "stalled body":
			writer.Header().Set("Content-Type", frameContentType)
			flush(writer, slot(1))
			select {
			case <-release:
			case <-request.Context().Done():
			}
		case "slow body":
			// Longer in all than the phase timeout, without a silence that long.
			writer.Header().Set("Content-Type", frameContentType)
			for index := 1; index <= 6; index++ {
				flush(writer, slot(index))
				time.Sleep(25 * time.Millisecond)
			}
			next, _ := encodeFrame(nil, map[string]any{"_end": 1, "hasMore": false, "next": "eid:" + string(worlds.EventIDForSlot(6))}, nil)
			flush(writer, next)
		default:
			flush(writer, end)
		}
	})
	timedOut := func(err error) bool {
		failed, ok := errors.AsType[*transientError](err)
		return ok && failed.timeout
	}

	// An entity request has one deadline for all of it, and is not sent
	// again once it passes.
	mode.Store("silent")
	if _, err := world.getRun(ctx, "wrun_1", worlds.ResolveDataNone, "", 0); !timedOut(err) || requests.Load() != 1 {
		t.Fatalf("silent entity read = %v after %d requests, want one timeout", err, requests.Load())
	}
	// An event request waits the phase timeout for its headers.
	requests.Store(0)
	if _, err := world.readEvents(ctx, "/v5/runs/wrun_1/events", nil); !timedOut(err) || requests.Load() != 1 {
		t.Fatalf("silent event read = %v after %d requests, want one timeout", err, requests.Load())
	}
	// And for each part of its body.
	mode.Store("stalled body")
	page, err := world.readEvents(ctx, "/v5/runs/wrun_1/events", nil)
	if _, truncated := errors.AsType[*truncatedError](err); !truncated || !timedOut(err) || len(page.Events) != 1 {
		t.Fatalf("stalled event read = %d events, %v, want the event read and a timeout", len(page.Events), err)
	}
	// A body that keeps arriving is read to its end however long it takes.
	mode.Store("slow body")
	if page, err := world.readEvents(ctx, "/v5/runs/wrun_1/events", nil); err != nil || len(page.Events) != 6 {
		t.Fatalf("slow event read = %d events, %v, want all six", len(page.Events), err)
	}
	// A caller's own cancellation is not the request's timeout.
	mode.Store("silent")
	cancelled, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	if _, err := world.getRun(cancelled, "wrun_1", worlds.ResolveDataNone, "", 0); err == nil || timedOut(err) {
		t.Fatalf("cancelled read = %v, want the caller's cancellation", err)
	}
}

func TestThrottleWaiter(t *testing.T) {
	shorten(t, &throttleMinWait, time.Millisecond)
	ctx := context.Background()
	// The waits of one request add up to the budget and no further.
	waiter := throttleWaiter{}
	if !waiter.wait(ctx, 0) || waiter.waited != throttleMinWait {
		t.Fatalf("a rate limit without a wait waited %v, want the minimum", waiter.waited)
	}
	waiter.waited = throttleBudget - time.Millisecond
	if waiter.wait(ctx, 2*time.Millisecond) {
		t.Fatal("a wait past the budget was made")
	}
	// No wait runs into the reserve before the context's deadline, whatever
	// the budget.
	near, cancel := context.WithTimeout(ctx, throttleDeadlineReserve+time.Second)
	defer cancel()
	if (&throttleWaiter{}).wait(near, 2*time.Second) || (&throttleWaiter{stepOutcome: true}).wait(near, 2*time.Second) {
		t.Fatal("a wait ran into the reserve before the deadline")
	}
	// A step's outcome with a deadline has the long budget, and without one
	// the short.
	far, cancelFar := context.WithTimeout(ctx, time.Hour)
	defer cancelFar()
	outcome := throttleWaiter{stepOutcome: true, waited: throttleBudget}
	if !outcome.wait(far, 0) {
		t.Fatal("a step's outcome gave up at the short budget with time left")
	}
	if (&throttleWaiter{stepOutcome: true, waited: throttleBudget}).wait(ctx, 0) || (&throttleWaiter{waited: throttleBudget}).wait(far, 0) {
		t.Fatal("the long budget was given without a step's outcome and a deadline")
	}
}

func TestClassifyStartHookRefusals(t *testing.T) {
	disabled := decodeAPIError(http.StatusBadRequest, "application/json", []byte(`{"error":"start-hook-disabled","message":"off"}`))
	for _, eventType := range []worlds.EventType{worlds.EventTypeRunCreated, worlds.EventTypeRunStarted} {
		if err := classify(disabled, eventType); !errors.Is(err, worlds.ErrStartHookDisabled) || worlds.IsEventRefusal(err) || errors.Is(err, worlds.ErrStartHookRejected) || worlds.IsRetryableWorldError(err) || statusOf(err) != http.StatusBadRequest {
			t.Fatalf("%s with start-hook-disabled = %v, want ErrStartHookDisabled with the backend's answer and no verdict on the candidate", eventType, err)
		}
	}
	cancelled := decodeAPIError(http.StatusConflict, "application/json", []byte(`{"error":"start-hook-candidate-cancelled","message":"fenced"}`))
	for _, eventType := range []worlds.EventType{worlds.EventTypeRunCreated, worlds.EventTypeRunStarted} {
		if err := classify(cancelled, eventType); !errors.Is(err, worlds.ErrStartHookRejected) || errors.Is(err, worlds.ErrRunAlreadyExists) {
			t.Fatalf("%s of a cancelled candidate = %v, want ErrStartHookRejected", eventType, err)
		}
	}
	var expired *worlds.RunExpiredError
	if err := classify(cancelled, worlds.EventTypeRunCancelled); !errors.Is(err, worlds.ErrEntityConflict) || !errors.As(err, &expired) {
		t.Fatalf("cancellation of a cancelled candidate = %v, want the conflict of a finished run", err)
	}
}

func TestBackendServerErrorIsRetryable(t *testing.T) {
	if !worlds.IsRetryableWorldError(classify(&apiError{Status: http.StatusBadGateway}, worlds.EventTypeRunCreated)) {
		t.Fatal("a 502 on run_created is not retryable")
	}
	if worlds.IsRetryableWorldError(classify(&apiError{Status: http.StatusBadRequest}, worlds.EventTypeRunCreated)) {
		t.Fatal("a 400 on run_created is retryable")
	}
}

func TestRetryEventWrite(t *testing.T) {
	shorten(t, &throttleMinWait, time.Millisecond)
	ctx := context.Background()
	unavailable := &apiError{Status: http.StatusServiceUnavailable}
	for _, test := range []struct {
		name     string
		eligible bool
		failures []error
		calls    int
		fails    bool
	}{
		{"a transient failure of an eligible write", true, []error{unavailable, &transientError{err: errors.New("reset")}}, 3, false},
		{"more transient failures than retries", true, []error{unavailable, unavailable, unavailable}, 3, true},
		{"a transient failure of a write that may land twice", false, []error{unavailable}, 1, true},
		{"a refusal", true, []error{&apiError{Status: http.StatusConflict}}, 1, true},
		{"a request past its own deadline", true, []error{&transientError{err: errors.New("timed out"), timeout: true}}, 2, false},
		{"a firewall challenge", true, []error{&transientError{err: &apiError{Status: http.StatusTooManyRequests}}}, 2, false},
		{"a rate limit, whatever the event", false, []error{&worlds.ThrottleError{After: time.Millisecond}}, 2, false},
		{"a rate limit past the budget", false, []error{&worlds.ThrottleError{After: 2 * throttleBudget}}, 1, true},
		{"a start hook whose run is not stored yet", false, []error{&apiError{Status: http.StatusServiceUnavailable, Code: "start-hook-run-pending", RetryAfter: time.Millisecond}}, 2, false},
	} {
		calls := 0
		_, err := retryEventWrite(ctx, test.eligible, false, func() (int, error) {
			calls++
			if calls <= len(test.failures) {
				return 0, test.failures[calls-1]
			}
			return calls, nil
		})
		if calls != test.calls || (err != nil) != test.fails {
			t.Errorf("%s: %d calls and %v, want %d calls", test.name, calls, err, test.calls)
		}
	}
	for eventType, want := range map[worlds.EventType]bool{
		worlds.EventTypeStepCompleted: true,
		worlds.EventTypeStepStarted:   false,
		worlds.EventTypeStepRetrying:  false,
		worlds.EventTypeHookReceived:  false,
	} {
		if got := retriesTransient(eventType, false, false); got != want {
			t.Errorf("retriesTransient(%s) = %v, want %v", eventType, got, want)
		}
	}
	if !retriesTransient(worlds.EventTypeHookReceived, false, true) || retriesTransient(worlds.EventTypeStepCompleted, true, false) {
		t.Error("a resume with an ID is sent again, and an in-band write never is")
	}
}

func TestRecordsStepOutcome(t *testing.T) {
	marked := worlds.EventsCreateOptions{AfterStepBody: true}
	for _, test := range []struct {
		eventType worlds.EventType
		options   worlds.EventsCreateOptions
		want      bool
	}{
		{worlds.EventTypeStepCompleted, worlds.EventsCreateOptions{}, true},
		{worlds.EventTypeStepRetrying, worlds.EventsCreateOptions{}, true},
		// A step_failed is also written for a step that never ran, so only
		// one its writer marks gets the long budget.
		{worlds.EventTypeStepFailed, worlds.EventsCreateOptions{}, false},
		{worlds.EventTypeStepFailed, marked, true},
		{worlds.EventTypeStepStarted, marked, false},
		{worlds.EventTypeRunFailed, marked, false},
	} {
		if got := recordsStepOutcome(test.eventType, test.options); got != test.want {
			t.Errorf("%s with AfterStepBody %t: %t, want %t", test.eventType, test.options.AfterStepBody, got, test.want)
		}
	}
}
