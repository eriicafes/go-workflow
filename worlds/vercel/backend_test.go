package vercelworld

import (
	"bytes"
	"errors"
	"net/http"
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
		{http.StatusNotFound, "hook-not-found", worlds.EventTypeHookReceived, worlds.ErrHookNotActive},
		{http.StatusConflict, "hook-force-claimed", worlds.EventTypeHookReceived, worlds.ErrHookForceClaimed},
		{http.StatusConflict, "", worlds.EventTypeRunCreated, worlds.ErrRunAlreadyExists},
		{http.StatusConflict, "", worlds.EventTypeStepCompleted, worlds.ErrStepNotStartable},
		{http.StatusConflict, "run-terminal", worlds.EventTypeStepStarted, worlds.ErrRunTerminal},
		{http.StatusConflict, "", worlds.EventTypeRunCompleted, worlds.ErrRunTerminal},
		{http.StatusGone, "", worlds.EventTypeRunFailed, worlds.ErrRunTerminal},
		{http.StatusConflict, "", worlds.EventTypeHookCreated, worlds.ErrHookConflict},
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
