package vercelworld

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

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

// TestBackendAssumptionsLive checks against a real project what the World
// assumes of the backend and cannot see from the client. Each subtest is one
// assumption the Vercel World's documentation lists.
func TestBackendAssumptionsLive(t *testing.T) {
	t.Run("FenceOnlyOnV5", func(t *testing.T) {
		// A /v4 write to a run below spec 9 is never refused by the fence.
		world := contractBackend(t, true)
		runID := contractRun(t, world, "v4", 8)
		meta := map[string]any{"specVersion": 8, "correlationId": "wait_01V4FENCE", "resumeAt": time.Now().Add(time.Hour).UTC(), "occurredAt": time.Now().UTC(), "inBand": true, "expectedSeqInBand": 1000}
		if _, err := contractPost(t, world, "v4", runID, "wait_created", meta, nil); err != nil {
			t.Fatalf("stale /v4 in-band write err = %v, want it accepted", err)
		}
	})
	t.Run("CreatedAtIsOccurredAt", func(t *testing.T) {
		world := contractBackend(t, true)
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
		world := contractBackend(t, true)
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
		world := contractBackend(t, true)
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
