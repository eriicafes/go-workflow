package workflowtest

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"strings"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// conformSDKSetAttributes returns the error of RunsSetAttributes applying
// changes to a new run created with attributes.
func conformSDKSetAttributes(t *testing.T, world worlds.World, attributes map[string]string, allowReserved bool, changes ...worlds.RunsAttributeChange) (worlds.RunID, error) {
	t.Helper()
	ctx := context.Background()
	runID := conformRunID("sdk_attributes")
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "conformance", Input: []byte(`"input"`), Attributes: attributes}); err != nil {
		t.Fatalf("EventsCreateRun: %v", err)
	}
	return runID, world.RunsSetAttributes(ctx, worlds.RunsSetAttributesInput{RunID: runID, Changes: changes, AllowReservedAttributes: allowReserved})
}

// conformSDKChange returns the change that sets key to value.
func conformSDKChange(key, value string) worlds.RunsAttributeChange {
	return worlds.RunsAttributeChange{Key: key, Value: &value}
}

// conformSDKKeys returns count changes that set the keys k0 to k<count-1>
// to "v".
func conformSDKKeys(count int) []worlds.RunsAttributeChange {
	changes := make([]worlds.RunsAttributeChange, count)
	for index := range changes {
		changes[index] = conformSDKChange(fmt.Sprintf("k%d", index), "v")
	}
	return changes
}

// conformSDKAttributeLimits checks the limits a World holds attribute
// writes to: a key's length, a value's UTF-8 bytes, keys set once per write,
// the attributes a run holds after a write, and reserved keys.
//
// SDK: packages/world/src/attributes.test.ts > attribute schemas > accepts a normal key
// SDK: packages/world/src/attributes.test.ts > attribute schemas > rejects empty keys
// SDK: packages/world/src/attributes.test.ts > attribute schemas > rejects keys over the length cap
// SDK: packages/world/src/attributes.test.ts > attribute schemas > accepts keys exactly at the length cap
// SDK: packages/world/src/attributes.test.ts > attribute schemas > accepts null (unset)
// SDK: packages/world/src/attributes.test.ts > attribute schemas > accepts a normal string
// SDK: packages/world/src/attributes.test.ts > attribute schemas > rejects values over the byte cap
// SDK: packages/world/src/attributes.test.ts > attribute schemas > counts UTF-8 bytes, not characters
// SDK: packages/world/src/attributes.test.ts > attribute schemas > validates complete changes and batches
// SDK: packages/world/src/attributes.test.ts > attribute schemas > rejects batches over the per-run cap
// SDK: packages/world/src/attributes.test.ts > validateAttributeChanges > accepts a small batch of valid changes
// SDK: packages/world/src/attributes.test.ts > validateAttributeChanges > rejects duplicate keys within a single batch
// SDK: packages/world/src/attributes.test.ts > validateAttributeChanges > rejects when post-merge count exceeds the per-run cap
// SDK: packages/world/src/attributes.test.ts > validateAttributeChanges > does not let an unknown deletion offset a new attribute
// SDK: packages/world/src/attributes.test.ts > validateAttributeChanges > does not count upserts on already-present keys against the cap
// SDK: packages/world/src/attributes.test.ts > validateAttributeChanges > rejects reserved-prefix keys in a batch by default
// SDK: packages/world/src/attributes.test.ts > validateAttributeChanges > accepts reserved-prefix keys when allowReservedAttributes is set
func conformSDKAttributeLimits(t *testing.T, world worlds.World) {
	if !world.Capabilities().SetAttributes {
		t.Skip("the World does not store attribute writes")
	}
	full := map[string]string{}
	for index := range worlds.MaxAttributesPerRun {
		full[fmt.Sprintf("k%d", index)] = "v"
	}
	for _, test := range []struct {
		name          string
		attributes    map[string]string
		allowReserved bool
		changes       []worlds.RunsAttributeChange
		valid         bool
	}{
		{"a normal key", nil, false, []worlds.RunsAttributeChange{conformSDKChange("phase", "running")}, true},
		{"an empty key", nil, false, []worlds.RunsAttributeChange{conformSDKChange("", "running")}, false},
		{"a key over the length cap", nil, false, []worlds.RunsAttributeChange{conformSDKChange(strings.Repeat("k", 257), "v")}, false},
		{"a key at the length cap", nil, false, []worlds.RunsAttributeChange{conformSDKChange(strings.Repeat("k", 256), "v")}, true},
		{"an unset", nil, false, []worlds.RunsAttributeChange{{Key: "phase"}}, true},
		{"a normal string", nil, false, []worlds.RunsAttributeChange{conformSDKChange("greeting", "hello")}, true},
		{"a value over the byte cap", nil, false, []worlds.RunsAttributeChange{conformSDKChange("big", strings.Repeat("a", 257))}, false},
		// A four-byte emoji: 64 of them are 256 bytes, the cap.
		{"a value at the byte cap in UTF-8", nil, false, []worlds.RunsAttributeChange{conformSDKChange("emoji", strings.Repeat("💥", 64))}, true},
		{"a value over the byte cap in UTF-8", nil, false, []worlds.RunsAttributeChange{conformSDKChange("emoji", strings.Repeat("💥", 65))}, false},
		{"a key set twice", nil, false, []worlds.RunsAttributeChange{conformSDKChange("phase", "running"), conformSDKChange("phase", "done")}, false},
		{"a batch over the per-run cap", nil, false, conformSDKKeys(worlds.MaxAttributesPerRun + 1), false},
		{"a small batch", nil, false, []worlds.RunsAttributeChange{conformSDKChange("phase", "init"), {Key: "stale"}}, true},
		{"a batch past the cap with the run's attribute", map[string]string{"preexisting": "v"}, false, conformSDKKeys(worlds.MaxAttributesPerRun), false},
		{"a batch past the cap with an unknown deletion", nil, false, append(conformSDKKeys(worlds.MaxAttributesPerRun+1), worlds.RunsAttributeChange{Key: "not-known-to-exist"}), false},
		{"an update of a full run", full, false, []worlds.RunsAttributeChange{conformSDKChange("k0", "updated")}, true},
		{"a reserved key", nil, false, []worlds.RunsAttributeChange{conformSDKChange("phase", "init"), conformSDKChange("$framework.kind", "agent")}, false},
		{"an allowed reserved key", nil, true, []worlds.RunsAttributeChange{conformSDKChange("phase", "init"), conformSDKChange("$framework.kind", "agent")}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := conformSDKSetAttributes(t, world, test.attributes, test.allowReserved, test.changes...)
			if test.valid && err != nil {
				t.Fatalf("RunsSetAttributes: %v", err)
			}
			if !test.valid {
				conformExpect(t, err, worlds.ErrAttributeInvalid, test.name)
			}
		})
	}
}

// conformSDKAttributeChanges checks how an attribute write changes a run's
// attributes: it sets and overwrites keys, and removes the keys it unsets.
//
// SDK: packages/world/src/attributes.test.ts > applyAttributeChanges > upserts new keys
// SDK: packages/world/src/attributes.test.ts > applyAttributeChanges > overwrites existing keys
// SDK: packages/world/src/attributes.test.ts > applyAttributeChanges > removes keys when value is null
// SDK: packages/world/src/attributes.test.ts > applyAttributeChanges > applies set and unset in a single batch
// SDK: packages/world/src/attributes.test.ts > applyAttributeChanges > treats undefined existing as the empty record
func conformSDKAttributeChanges(t *testing.T, world worlds.World) {
	if !world.Capabilities().SetAttributes {
		t.Skip("the World does not store attribute writes")
	}
	for _, test := range []struct {
		name       string
		attributes map[string]string
		changes    []worlds.RunsAttributeChange
		want       map[string]string
	}{
		{"upsert", map[string]string{"a": "1"}, []worlds.RunsAttributeChange{conformSDKChange("b", "2")}, map[string]string{"a": "1", "b": "2"}},
		{"overwrite", map[string]string{"a": "1"}, []worlds.RunsAttributeChange{conformSDKChange("a", "2")}, map[string]string{"a": "2"}},
		{"remove", map[string]string{"a": "1", "b": "2"}, []worlds.RunsAttributeChange{{Key: "a"}}, map[string]string{"b": "2"}},
		{"set and unset", map[string]string{"a": "1", "stale": "x"}, []worlds.RunsAttributeChange{{Key: "stale"}, conformSDKChange("fresh", "yes")}, map[string]string{"a": "1", "fresh": "yes"}},
		{"no attributes", nil, []worlds.RunsAttributeChange{conformSDKChange("a", "1")}, map[string]string{"a": "1"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			runID, err := conformSDKSetAttributes(t, world, test.attributes, false, test.changes...)
			if err != nil {
				t.Fatalf("RunsSetAttributes: %v", err)
			}
			run, err := world.RunsGet(context.Background(), worlds.RunsGetInput{RunID: runID})
			if err != nil || !maps.Equal(run.Attributes, test.want) {
				t.Fatalf("attributes = %v err = %v, want %v", run.Attributes, err, test.want)
			}
		})
	}
}

// conformSDKAttributeEventDataSize checks that a World takes an attr_set
// whose Data is MaxAttrSetDataBytes of JSON and refuses one a byte larger,
// counting UTF-8 bytes and JSON escapes, as the Workflow SDK's
// JSON.stringify writes them.
//
// SDK: packages/world/src/attributes.test.ts > validateAttributeEventDataSize > counts %s at the JSON byte boundary
func conformSDKAttributeEventDataSize(t *testing.T, world worlds.World) {
	if !world.Capabilities().SetAttributes {
		t.Skip("the World does not store attribute writes")
	}
	type change struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	type eventData struct {
		Changes []change `json:"changes"`
		Writer  struct {
			Type string `json:"type"`
		} `json:"writer"`
	}
	encode := func(data eventData) []byte {
		encoded, err := json.Marshal(data)
		if err != nil {
			t.Fatal(err)
		}
		return encoded
	}
	for _, test := range []struct{ label, key, value string }{
		{"envelope", "key", ""},
		{"long keys", strings.Repeat("k", 180), ""},
		{"UTF-8 keys", strings.Repeat("é", 90), ""},
		{"UTF-8 values", "key", strings.Repeat("é", 64)},
		{"JSON escapes", "\n\"\\", strings.Repeat("\n\"\\", 12)},
	} {
		t.Run(test.label, func(t *testing.T) {
			data := eventData{}
			data.Writer.Type = worlds.AttributeWriterWorkflow
			for index := range 32 {
				data.Changes = append(data.Changes, change{Key: fmt.Sprintf("%d%s", index, test.key), Value: test.value})
			}
			remaining := worlds.MaxAttrSetDataBytes - len(encode(data))
			for index := range data.Changes {
				padding := min(remaining, worlds.MaxAttributeValueBytes-len(data.Changes[index].Value))
				data.Changes[index].Value += strings.Repeat("x", padding)
				remaining -= padding
			}
			if size := len(encode(data)); remaining != 0 || size != worlds.MaxAttrSetDataBytes {
				t.Fatalf("attr_set data is %d bytes, %d short", size, remaining)
			}
			changes := func() []worlds.RunsAttributeChange {
				var changes []worlds.RunsAttributeChange
				for _, change := range data.Changes {
					changes = append(changes, conformSDKChange(change.Key, change.Value))
				}
				return changes
			}
			if err := worlds.CheckAttributeChanges(nil, changes(), false); err != nil {
				t.Fatalf("changes at the limit: %v", err)
			}
			atLimit := conformCreateRun(t, world, "sdk_attr_size")
			conformAppend(t, world, atLimit, worlds.EventTypeAttrSet, "attr_size", encode(data))

			data.Changes[0].Key += "x"
			if size := len(encode(data)); size != worlds.MaxAttrSetDataBytes+1 {
				t.Fatalf("attr_set data is %d bytes, want %d", size, worlds.MaxAttrSetDataBytes+1)
			}
			if err := worlds.CheckAttributeChanges(nil, changes(), false); err != nil {
				t.Fatalf("changes over the limit: %v", err)
			}
			overLimit := conformCreateRun(t, world, "sdk_attr_size")
			_, err := world.EventsCreate(context.Background(), worlds.EventsCreateInput{RunID: overLimit, Event: conformEvent(t, worlds.EventTypeAttrSet, "attr_size", encode(data))})
			conformExpect(t, err, worlds.ErrAttributeInvalid, "attr_set over the limit")
		})
	}
}

// conformSDKRetention checks that a World deletes a finished run's data
// only when its RetentionAttribute is exactly "0": an absent attribute,
// "default", another duration, and every value that only looks like zero
// keep it.
//
// SDK: packages/world/src/run-retention.test.ts > readRunRetention > purges only on the literal "0"
// SDK: packages/world/src/run-retention.test.ts > readRunRetention > keeps the data when the attribute is absent
// SDK: packages/world/src/run-retention.test.ts > readRunRetention > keeps the data for %s
// SDK: packages/world/src/run-retention.test.ts > purgesUserDataOnFinish > is true only for the literal "0"
// SDK: packages/world/src/run-retention.test.ts > purgesUserDataOnFinish > is false for %s
func conformSDKRetention(t *testing.T, world worlds.World) {
	ctx := context.Background()
	retention := func(value string) map[string]string { return map[string]string{worlds.RetentionAttribute: value} }
	finish := func(t *testing.T, attributes map[string]string) worlds.Run {
		t.Helper()
		runID := conformRunID("sdk_retention")
		if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "conformance", Input: []byte(`"input"`), Attributes: attributes, AllowReservedAttributes: true}); err != nil {
			t.Fatalf("EventsCreateRun: %v", err)
		}
		conformAppend(t, world, runID, worlds.EventTypeRunStarted, "", nil)
		conformAppend(t, world, runID, worlds.EventTypeRunCompleted, "", worlds.DataRunCompleted{Output: []byte(`"output"`)})
		run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID})
		if err != nil {
			t.Fatal(err)
		}
		return run
	}
	t.Run("zero", func(t *testing.T) {
		if run := finish(t, retention("0")); !worlds.RunDataExpired(run, time.Now()) || run.Output != nil {
			t.Fatalf("run with retention 0 = %+v, want its data deleted", run)
		}
	})
	for _, test := range []struct {
		label      string
		attributes map[string]string
	}{
		{"absent", nil},
		{"empty attributes", map[string]string{}},
		{"other attributes", map[string]string{"tenant": "t1"}},
		{"default", retention("default")},
		{"a non-zero duration", retention("7")},
		{"a large duration", retention("86400")},
		{"a malformed word", retention("none")},
		{"a negative number", retention("-1")},
		{"a float", retention("0.0")},
		{"a zero with a unit", retention("0s")},
		{"a padded zero", retention(" 0 ")},
		{"an empty string", retention("")},
		{"a leading-zero integer", retention("007")},
		{"a signed zero", retention("-0")},
		{"a padded-digit zero", retention("00")},
	} {
		t.Run(test.label, func(t *testing.T) {
			if run := finish(t, test.attributes); run.ExpiredAt != nil || string(run.Output) != `"output"` {
				t.Fatalf("run = %+v, want its data kept", run)
			}
		})
	}
}
