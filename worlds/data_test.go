package worlds

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestCheckAttributesMatchesTheSDKLimits(t *testing.T) {
	many := map[string]string{}
	for index := range MaxAttributesPerRun + 1 {
		many[fmt.Sprint(index)] = "v"
	}
	// Keys count UTF-16 code units: an astral rune is two.
	astral := strings.Repeat("😀", MaxAttributeKeyLength/2)
	for _, test := range []struct {
		attributes    map[string]string
		allowReserved bool
		valid         bool
	}{
		{map[string]string{astral: "v"}, false, true},
		{map[string]string{astral + "x": "v"}, false, false},
		{map[string]string{"$parentRunId": "v"}, true, true},
		{map[string]string{"$parentRunId": "v"}, false, false},
		{many, false, false},
	} {
		err := CheckAttributes(test.attributes, test.allowReserved)
		if (err == nil) != test.valid || (err != nil && !errors.Is(err, ErrAttributeInvalid)) {
			t.Errorf("CheckAttributes(%d attributes, %t) = %v, want valid %t", len(test.attributes), test.allowReserved, err, test.valid)
		}
	}
}

// SDK: packages/world/src/spec-version.test.ts > requiresNewerWorld > rejects runs newer than the highest supported spec version
func TestCheckRunSpecVersion(t *testing.T) {
	if err := CheckRunSpecVersion("wrun_a", SpecVersion); err != nil {
		t.Fatalf("current spec version: %v", err)
	}
	err := CheckRunSpecVersion("wrun_a", SpecVersion+1)
	if _, ok := errors.AsType[*RunNotSupportedError](err); !ok {
		t.Fatalf("newer spec version = %v, want &RunNotSupportedError{}", err)
	}
}

func TestRunCreatedRecordsItsDeploymentAndOmitsAbsentAttributes(t *testing.T) {
	data, err := json.Marshal(DataRunCreated{WorkflowName: "wf", Input: []byte("in")})
	if err != nil {
		t.Fatal(err)
	}
	var recorded map[string]any
	if err := json.Unmarshal(data, &recorded); err != nil {
		t.Fatal(err)
	}
	if deployment, ok := recorded["deploymentId"]; !ok || deployment != "" {
		t.Fatalf("deploymentId = %#v, recorded %v", deployment, ok)
	}
	if _, ok := recorded["attributes"]; ok {
		t.Fatalf("run_created without attributes = %s", data)
	}
}

func TestHookConflictRecordsOnlyItsTokenAndHolder(t *testing.T) {
	data, err := json.Marshal(DataHookCreated{Token: "tok", Metadata: []byte("meta"), IsWebhook: true, IsSystem: true, ConflictingRunID: "wrun_holder"})
	if err != nil || string(data) != `{"token":"tok","conflictingRunId":"wrun_holder"}` {
		t.Fatalf("hook_conflict data = %s, %v", data, err)
	}
	// A hook_created keeps every field.
	data, err = json.Marshal(DataHookCreated{Token: "tok", IsSystem: true})
	if err != nil || string(data) != `{"token":"tok","isWebhook":false,"isSystem":true}` {
		t.Fatalf("hook_created data = %s, %v", data, err)
	}
}

func TestCheckAttrSetRefusesDataOverTheLimit(t *testing.T) {
	var changes []RunsAttributeChange
	for index := range 40 {
		value := strings.Repeat("v", MaxAttributeValueBytes)
		changes = append(changes, RunsAttributeChange{Key: fmt.Sprintf("key-%d", index), Value: &value})
	}
	data, err := json.Marshal(DataAttrSet{Changes: changes})
	if err != nil || len(data) <= MaxAttrSetDataBytes {
		t.Fatalf("attr_set data is %d bytes, %v", len(data), err)
	}
	if err := CheckAttrSet(nil, data); !errors.Is(err, ErrAttributeInvalid) {
		t.Fatalf("CheckAttrSet over the limit = %v", err)
	}
	if data, _ = json.Marshal(DataAttrSet{Changes: changes[:8]}); CheckAttrSet(nil, data) != nil {
		t.Fatalf("CheckAttrSet within the limit = %v", CheckAttrSet(nil, data))
	}
}

func TestAttrSetDataSizeCountsMarkupAsWritten(t *testing.T) {
	value := "<a&b>"
	data, err := json.Marshal(DataAttrSet{Changes: []RunsAttributeChange{{Key: "k", Value: &value}}})
	if err != nil {
		t.Fatal(err)
	}
	// Go writes each of <, & and > as a six-byte escape.
	if escaped, plain := len(data), AttrSetDataSize(data); escaped-plain != 3*5 {
		t.Fatalf("escaped %d bytes, plain %d", escaped, plain)
	}
}

func TestCheckAttributeChangesCountsOnlySets(t *testing.T) {
	var removals []RunsAttributeChange
	for index := range MaxAttributesPerRun + 1 {
		removals = append(removals, RunsAttributeChange{Key: fmt.Sprintf("key-%d", index)})
	}
	if err := CheckAttributeChanges(nil, removals, false); err != nil {
		t.Fatalf("removals of unknown keys = %v", err)
	}
}
