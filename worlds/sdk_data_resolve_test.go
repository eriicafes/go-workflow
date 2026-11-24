package worlds

import (
	"encoding/json"
	"testing"
)

// SDK: packages/world-local/src/storage.test.ts > stripEventDataRefs > should strip input ref from step_created, keep stepName
// SDK: packages/world-local/src/storage.test.ts > stripEventDataRefs > should strip input ref from run_created, keep workflowName and deploymentId
// SDK: packages/world-local/src/storage.test.ts > stripEventDataRefs > should strip result ref from step_completed entirely
// SDK: packages/world-local/src/storage.test.ts > stripEventDataRefs > should strip error from run_failed, keep errorCode
// SDK: packages/world-local/src/storage.test.ts > stripEventDataRefs > should strip error from step_failed, leaving no eventData when it was the only field
// SDK: packages/world-local/src/storage.test.ts > stripEventDataRefs > should not strip anything when resolveData is "all"
// SDK: packages/world-local/src/storage.test.ts > stripEventDataRefs > should pass through events with no ref fields (e.g. run_started)
// SDK: packages/world-local/src/storage.test.ts > stripEventDataRefs > should strip metadata from hook_created, keep token
func TestSDKResolveEventDataStripsPayloads(t *testing.T) {
	tests := []struct {
		name      string
		eventType EventType
		data      string
		resolve   EventsResolveData
		want      string
	}{
		{"step_created", EventTypeStepCreated, `{"stepName":"my-step","input":"AQID"}`, EventsResolveDataNone, `{"stepName":"my-step"}`},
		{"run_created", EventTypeRunCreated, `{"deploymentId":"dpl_123","workflowName":"my-workflow","input":"AQID"}`, EventsResolveDataNone, `{"deploymentId":"dpl_123","workflowName":"my-workflow"}`},
		{"step_completed", EventTypeStepCompleted, `{"result":"BAU="}`, EventsResolveDataNone, ``},
		{"run_failed", EventTypeRunFailed, `{"error":"something broke","errorCode":"TIMEOUT"}`, EventsResolveDataNone, `{"errorCode":"TIMEOUT"}`},
		{"step_failed", EventTypeStepFailed, `{"error":"AQID"}`, EventsResolveDataNone, ``},
		{"all", EventTypeStepCreated, `{"stepName":"my-step","input":"AQID"}`, EventsResolveDataAll, `{"stepName":"my-step","input":"AQID"}`},
		{"run_started", EventTypeRunStarted, ``, EventsResolveDataNone, ``},
		{"hook_created", EventTypeHookCreated, `{"token":"tok_abc","metadata":{"some":"data"}}`, EventsResolveDataNone, `{"token":"tok_abc"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var data []byte
			if test.data != "" {
				data = []byte(test.data)
			}
			got := ResolveEventData(test.eventType, data, test.resolve)
			if test.want == "" {
				if got != nil {
					t.Fatalf("data = %s, want none", got)
				}
				return
			}
			var gotValue, wantValue any
			if err := json.Unmarshal(got, &gotValue); err != nil {
				t.Fatalf("data = %q: %v", got, err)
			}
			_ = json.Unmarshal([]byte(test.want), &wantValue)
			gotJSON, _ := json.Marshal(gotValue)
			wantJSON, _ := json.Marshal(wantValue)
			if string(gotJSON) != string(wantJSON) {
				t.Fatalf("data = %s, want %s", gotJSON, wantJSON)
			}
		})
	}
}
