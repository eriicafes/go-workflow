package vercelworld

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

func TestInBandWriteReadsAnIncompleteReportAsMore(t *testing.T) {
	event := map[string]any{"runId": "wrun_1", "eventId": string(worlds.EventIDForSlot(3)), "eventType": "wait_created", "correlationId": "wait_a", "createdAt": time.Now(), "specVersion": specVersion, "eventData": map[string]any{"resumeAt": time.Now()}}
	world := newBackendWorld(t, func(writer http.ResponseWriter, request *http.Request) {
		if strings.HasSuffix(request.URL.Path, "/events/batch") {
			writeCBOR(writer, map[string]any{"results": []map[string]any{{"status": http.StatusOK, "event": event}}, "allocated": 1, "reportIncomplete": true})
			return
		}
		writeCBOR(writer, map[string]any{"event": event, "allocated": 1, "reportIncomplete": true})
	})
	ctx := context.Background()
	held, inBand := 2, 2
	data, _ := json.Marshal(worlds.DataWaitCreated{ResumeAt: time.Now().Add(time.Hour)})
	create := worlds.EventsCreateEvent{EventType: worlds.EventTypeWaitCreated, CorrelationID: "wait_a", Data: data}
	result, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: "wrun_1", Event: create, Options: worlds.EventsCreateOptions{EventCount: &held, InBandCount: &inBand}})
	if err != nil {
		t.Fatal(err)
	}
	if !result.MoreEvents || result.Events == nil {
		t.Fatalf("write with an incomplete report = %+v, want MoreEvents", result)
	}
	batch, err := world.EventsCreateBatch(ctx, worlds.EventsCreateBatchInput{RunID: "wrun_1", Events: []worlds.EventsCreateEvent{create}, EventCount: &held, InBandCount: &inBand})
	if err != nil {
		t.Fatal(err)
	}
	if !batch.MoreEvents || batch.Events == nil {
		t.Fatalf("batch with an incomplete report = %+v, want MoreEvents", batch)
	}
}

// eventFrames returns the list frames of the events at slots, and an end
// frame naming the last as the cursor when end is set.
func eventFrames(t *testing.T, end bool, slots ...uint64) []byte {
	t.Helper()
	var out []byte
	var err error
	for _, slot := range slots {
		if out, err = encodeFrame(out, map[string]any{"runId": "wrun_1", "eventId": string(worlds.EventIDForSlot(slot)), "eventType": "run_cancelled", "createdAt": time.Now(), "specVersion": specVersion}, nil); err != nil {
			t.Fatal(err)
		}
	}
	if end {
		meta := map[string]any{"_end": 1, "hasMore": false}
		if len(slots) > 0 {
			meta["next"] = "eid:" + string(worlds.EventIDForSlot(slots[len(slots)-1]))
		}
		if out, err = encodeFrame(out, meta, nil); err != nil {
			t.Fatal(err)
		}
	}
	return out
}

func TestWholeLogReadWaitsOutARateLimit(t *testing.T) {
	shorten(t, &throttleMinWait, time.Millisecond)
	var mu sync.Mutex
	var queries []string
	world := newBackendWorld(t, func(writer http.ResponseWriter, request *http.Request) {
		mu.Lock()
		queries = append(queries, request.URL.RawQuery)
		first := len(queries) == 1
		mu.Unlock()
		if first {
			writer.WriteHeader(http.StatusTooManyRequests)
			return
		}
		writer.Header().Set("Content-Type", frameContentType)
		_, _ = writer.Write(eventFrames(t, true, 1, 2))
	})
	ctx := context.Background()
	page, err := world.EventsList(ctx, worlds.EventsListInput{RunID: "wrun_1"})
	if err != nil || len(page.Data) != 2 || len(queries) != 2 || queries[0] != queries[1] {
		t.Fatalf("whole log behind a rate limit = %d events, %v after %q, want the same page asked for again", len(page.Data), err, queries)
	}
	// A page with a limit, and a correlation listing, return the rate limit.
	var throttled *worlds.ThrottleError
	queries = nil
	if _, err := world.EventsList(ctx, worlds.EventsListInput{RunID: "wrun_1", Pagination: worlds.Pagination{Limit: 10}}); !errors.As(err, &throttled) {
		t.Fatalf("limited page behind a rate limit = %v, want the ThrottleError", err)
	}
	queries = nil
	if _, err := world.EventsListByCorrelationID(ctx, worlds.EventsListByCorrelationIDInput{RunID: "wrun_1", CorrelationID: "step_1"}); !errors.As(err, &throttled) {
		t.Fatalf("correlation listing behind a rate limit = %v, want the ThrottleError", err)
	}
}

func TestWholeLogReadContinuesAnAnswerCutOff(t *testing.T) {
	var mu sync.Mutex
	var cursors []string
	cut := 1
	world := newBackendWorld(t, func(writer http.ResponseWriter, request *http.Request) {
		mu.Lock()
		cursors = append(cursors, request.URL.Query().Get("cursor"))
		count := len(cursors)
		mu.Unlock()
		writer.Header().Set("Content-Type", frameContentType)
		if count <= cut {
			// Whole events, then the body ends without its end frame.
			_, _ = writer.Write(eventFrames(t, false, uint64(2*count-1), uint64(2*count)))
			return
		}
		_, _ = writer.Write(eventFrames(t, true, uint64(2*count-1)))
	})
	ctx := context.Background()
	page, err := world.EventsList(ctx, worlds.EventsListInput{RunID: "wrun_1"})
	if err != nil || len(page.Data) != 3 || page.Cursor != "eid:"+string(worlds.EventIDForSlot(3)) {
		t.Fatalf("log cut off once = %d events, cursor %q, %v, want all three", len(page.Data), page.Cursor, err)
	}
	if want := "eid:" + string(worlds.EventIDForSlot(2)); len(cursors) != 2 || cursors[0] != "" || cursors[1] != want {
		t.Fatalf("cursors asked for = %q, want the read continued after %s", cursors, want)
	}
	// It is continued twice and no more.
	cursors, cut = nil, maxPartialReads+1
	var truncated *truncatedError
	if _, err := world.EventsList(ctx, worlds.EventsListInput{RunID: "wrun_1"}); !errors.As(err, &truncated) || len(cursors) != maxPartialReads+1 {
		t.Fatalf("log cut off every time = %v after %d requests, want its failure after %d", err, len(cursors), maxPartialReads+1)
	}
	// A page with a limit is one answer, returned cut off.
	cursors, cut = nil, 1
	if _, err := world.EventsList(ctx, worlds.EventsListInput{RunID: "wrun_1", Pagination: worlds.Pagination{Limit: 2}}); !errors.As(err, &truncated) || len(cursors) != 1 {
		t.Fatalf("limited page cut off = %v after %d requests, want its failure", err, len(cursors))
	}
}

func TestEventListMustAdvanceItsCursor(t *testing.T) {
	world := newBackendWorld(t, func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", frameContentType)
		out := eventFrames(t, false, 1)
		out, _ = encodeFrame(out, map[string]any{"_end": 1, "hasMore": true}, nil)
		_, _ = writer.Write(out)
	})
	if _, err := world.EventsList(context.Background(), worlds.EventsListInput{RunID: "wrun_1"}); err == nil || !strings.Contains(err.Error(), "advance") {
		t.Fatalf("answer with events and no cursor = %v, want a refusal to loop on it", err)
	}
}

func TestEventListParameters(t *testing.T) {
	var mu sync.Mutex
	queries := map[string]map[string][]string{}
	world := newBackendWorld(t, func(writer http.ResponseWriter, request *http.Request) {
		mu.Lock()
		queries[request.URL.Path] = request.URL.Query()
		mu.Unlock()
		writer.Header().Set("Content-Type", frameContentType)
		_, _ = writer.Write(eventFrames(t, true))
	})
	ctx := context.Background()
	if _, err := world.EventsList(ctx, worlds.EventsListInput{RunID: "wrun_1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := world.EventsListByCorrelationID(ctx, worlds.EventsListByCorrelationIDInput{RunID: "wrun_1", CorrelationID: "step_1"}); err != nil {
		t.Fatal(err)
	}
	// A run's whole log is asked for with returnAll, which the correlation
	// route does not take.
	if run := queries["/v5/runs/wrun_1/events"]; run["returnAll"] == nil || run["limit"] != nil {
		t.Fatalf("run listing = %v, want returnAll", run)
	}
	if correlated := queries["/v4/events"]; correlated["returnAll"] != nil || correlated["correlationId"][0] != "step_1" || correlated["runId"][0] != "wrun_1" {
		t.Fatalf("correlation listing = %v, want no returnAll", correlated)
	}
}

func TestEventsListReportsTheRunsEventLimit(t *testing.T) {
	limit := "25000"
	world := newBackendWorld(t, func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", frameContentType)
		if limit != "" {
			writer.Header().Set("X-Wf-Max-Events", limit)
		}
		_, _ = writer.Write(eventFrames(t, true, 1))
	})
	for _, test := range []struct {
		header string
		want   int
	}{{"25000", 25000}, {"", 0}, {"many", 0}, {"-1", 0}} {
		limit = test.header
		page, err := world.EventsList(context.Background(), worlds.EventsListInput{RunID: "wrun_1"})
		if err != nil || page.MaxEvents != test.want {
			t.Errorf("x-wf-max-events %q: MaxEvents %d, %v, want %d", test.header, page.MaxEvents, err, test.want)
		}
	}
}

func TestStepStartedCarriesItsStepsCreationTime(t *testing.T) {
	var metas []map[string]any
	world := newBackendWorld(t, func(writer http.ResponseWriter, request *http.Request) {
		sent, err := newFrameReader(request.Body).next()
		if err != nil {
			t.Error(err)
		}
		metas = append(metas, sent.Meta)
		writeCBOR(writer, map[string]any{"event": map[string]any{"runId": "wrun_1", "eventId": string(worlds.EventIDForSlot(uint64(2 + len(metas)))), "eventType": "step_started", "correlationId": "step_a", "createdAt": time.Now(), "specVersion": specVersion}, "allocated": 1})
	})
	ctx := context.Background()
	created := time.UnixMilli(1_700_000_000_123).UTC()
	start := func(data worlds.DataStepStarted, options worlds.EventsCreateOptions) map[string]any {
		t.Helper()
		encoded, _ := json.Marshal(data)
		before := len(metas)
		if _, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: "wrun_1", Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeStepStarted, CorrelationID: "step_a", Data: encoded}, Options: options}); err != nil {
			t.Fatal(err)
		}
		if len(metas) != before+1 {
			t.Fatalf("%d writes were sent, want one", len(metas)-before)
		}
		return metas[before]
	}
	// The first attempt's start of a step created before it carries the
	// time, in milliseconds.
	if meta := start(worlds.DataStepStarted{StepName: "step", Attempt: 1}, worlds.EventsCreateOptions{StepCreatedAt: created}); fmt.Sprint(meta["stepCreatedAt"]) != "1700000000123" {
		t.Fatalf("meta = %v, want stepCreatedAt 1700000000123", meta)
	}
	// Nothing else does: a start without the time, a retry's, and the start
	// that creates an inline step, which carries the step's input.
	for name, meta := range map[string]map[string]any{
		"a start without a creation time": start(worlds.DataStepStarted{StepName: "step", Attempt: 1}, worlds.EventsCreateOptions{}),
		"a retry's start":                 start(worlds.DataStepStarted{StepName: "step", Attempt: 2}, worlds.EventsCreateOptions{StepCreatedAt: created}),
		"a start that creates its step":   start(worlds.DataStepStarted{StepName: "step", WorkflowName: "flow", Attempt: 1, Inline: true, Input: []byte("input")}, worlds.EventsCreateOptions{StepCreatedAt: created}),
	} {
		if _, sent := meta["stepCreatedAt"]; sent {
			t.Errorf("%s carries stepCreatedAt: %v", name, meta)
		}
	}
}
