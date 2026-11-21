package worlds_test

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

func TestEventDataRoundTrips(t *testing.T) {
	at := time.Date(2026, 11, 21, 10, 0, 0, 123456789, time.UTC)
	payload := []byte{0, 1, 2, 0xff, 'd', 'e', 'v', 'l'}
	typed := func(value any) []byte {
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	for _, test := range []struct {
		name      string
		eventType worlds.EventType
		data      []byte
		// raw is a payload the encoding holds as a byte string.
		raw []byte
	}{
		{"run created", worlds.EventTypeRunCreated, typed(worlds.DataRunCreated{WorkflowName: "wf", Input: payload, Attributes: map[string]string{"a": "b"}, ExecutionContext: map[string]any{"n": 1.5, "nested": map[string]any{"x": "y"}}, StartHook: &worlds.StartHook{Token: "tok", TokenRetentionUntil: &at, Metadata: payload}}), payload},
		{"step result", worlds.EventTypeStepRetrying, typed(worlds.DataStepResult{StepName: "s", Attempt: 2, Error: payload, RetryAfter: &at}), payload},
		{"wait", worlds.EventTypeWaitCreated, typed(worlds.DataWaitCreated{ResumeAt: at}), nil},
		{"hook disposed", worlds.EventTypeHookDisposed, typed(worlds.HookDisposedEventData{ForceClaimedBy: &worlds.HookForceClaim{RunID: "wrun_a", HookID: "hook_b"}}), nil},
		{"attributes", worlds.EventTypeAttrSet, typed(worlds.DataAttrSet{Changes: []worlds.RunsAttributeChange{{Key: "k"}}, Writer: worlds.AttributeWriter{Type: "run"}}), nil},
		{"unknown field", worlds.EventTypeStepCompleted, []byte(`{"result":"AAE=","extra":{"big":9007199254740993,"f":0.5,"list":[true,null,"s"]}}`), nil},
		{"no struct", worlds.EventTypeRunCancelled, []byte(`{"reason":"stop"}`), nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			encoded, err := worlds.EncodeEventData(test.eventType, test.data)
			if err != nil {
				t.Fatal(err)
			}
			if test.raw != nil && !bytes.Contains(encoded, test.raw) {
				t.Fatalf("encoding %x does not hold the payload bytes", encoded)
			}
			decoded, err := worlds.DecodeEventData(test.eventType, encoded)
			if err != nil {
				t.Fatal(err)
			}
			var want, got any
			if err := json.Unmarshal(test.data, &want); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(decoded, &got); err != nil {
				t.Fatal(err)
			}
			if a, b := mustJSON(t, want), mustJSON(t, got); a != b {
				t.Fatalf("decoded %s, want %s", b, a)
			}
		})
	}
}

func TestEventDataKeepsLargeIntegers(t *testing.T) {
	data := []byte(`{"n":9007199254740993}`)
	encoded, err := worlds.EncodeEventData(worlds.EventTypeNoop, data)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := worlds.DecodeEventData(worlds.EventTypeNoop, encoded)
	if err != nil || string(decoded) != string(data) {
		t.Fatalf("decoded %s err=%v, want %s", decoded, err, data)
	}
}

func TestEventDataEmpty(t *testing.T) {
	if encoded, err := worlds.EncodeEventData(worlds.EventTypeRunStarted, nil); encoded != nil || err != nil {
		t.Fatalf("encoded %x err=%v", encoded, err)
	}
	if decoded, err := worlds.DecodeEventData(worlds.EventTypeRunStarted, nil); decoded != nil || err != nil {
		t.Fatalf("decoded %s err=%v", decoded, err)
	}
	if _, err := worlds.EncodeEventData(worlds.EventTypeNoop, []byte("{")); err == nil {
		t.Fatal("malformed data encoded")
	}
}

func mustJSON(t *testing.T, value any) string {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
