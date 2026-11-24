package worlds_test

import (
	"testing"

	"github.com/eriicafes/go-workflow/worlds"
)

func TestResolveEventDataKeepsMetadataWithoutPayloads(t *testing.T) {
	for _, test := range []struct {
		name      string
		eventType worlds.EventType
		data      string
		resolve   worlds.EventsResolveData
		want      string
	}{
		{"all keeps payloads", worlds.EventTypeStepCreated, `{"stepName":"pkg.step","input":1}`, worlds.EventsResolveDataAll, `{"stepName":"pkg.step","input":1}`},
		{"step input", worlds.EventTypeStepCreated, `{"position":2,"stepName":"pkg.step","input":{"Topic":"go"}}`, worlds.EventsResolveDataNone, `{"position":2,"stepName":"pkg.step"}`},
		{"step error", worlds.EventTypeStepRetrying, `{"error":{"message":"x"},"retryAfter":"2030-01-01T00:00:00Z"}`, worlds.EventsResolveDataNone, `{"retryAfter":"2030-01-01T00:00:00Z"}`},
		{"step result only", worlds.EventTypeStepCompleted, `{"result":1}`, worlds.EventsResolveDataNone, ``},
		{"run input", worlds.EventTypeRunCreated, `{"workflowName":"pkg.Flow","input":"x"}`, worlds.EventsResolveDataNone, `{"workflowName":"pkg.Flow"}`},
		{"run output", worlds.EventTypeRunCompleted, `{"output":"done"}`, worlds.EventsResolveDataNone, ``},
		{"run error", worlds.EventTypeRunFailed, `{"error":{"message":"x"}}`, worlds.EventsResolveDataNone, ``},
		{"hook metadata", worlds.EventTypeHookCreated, `{"token":"t","metadata":"bWV0YQ=="}`, worlds.EventsResolveDataNone, `{"token":"t"}`},
		{"hook payload", worlds.EventTypeHookReceived, `{"payload":{"Approved":true}}`, worlds.EventsResolveDataNone, ``},
		{"metadata only", worlds.EventTypeWaitCreated, `{"dueAt":"2030-01-01T00:00:00Z"}`, worlds.EventsResolveDataNone, `{"dueAt":"2030-01-01T00:00:00Z"}`},
		{"not an object", worlds.EventTypeStepCreated, "\xa1", worlds.EventsResolveDataNone, ``},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := worlds.ResolveEventData(test.eventType, []byte(test.data), test.resolve)
			if string(got) != test.want {
				t.Fatalf("ResolveEventData = %q, want %q", got, test.want)
			}
		})
	}
}

func TestAttrSetDataUsesWorkflowSDKShape(t *testing.T) {
	value := "eu"
	data, err := worlds.EncodeAttrSet(worlds.RunsSetAttributesInput{RunID: "wrun_a", Changes: []worlds.RunsAttributeChange{{Key: "zone", Value: &value}, {Key: "tier"}}})
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"changes":[{"key":"zone","value":"eu"},{"key":"tier","value":null}],"writer":{"type":"workflow"}}`; string(data) != want {
		t.Fatalf("AttrSetData = %s, want %s", data, want)
	}
	data, err = worlds.EncodeAttrSet(worlds.RunsSetAttributesInput{Writer: &worlds.AttributeWriter{Type: worlds.AttributeWriterStep, StepID: "step_a", Attempt: 2}})
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"changes":null,"writer":{"type":"step","stepId":"step_a","attempt":2}}`; string(data) != want {
		t.Fatalf("AttrSetData with step writer = %s, want %s", data, want)
	}
}

// A hook's resume context carries the versions of the runtime that started
// its run, from the run's execution context.
func TestHookResumeContextCopiesTheRunsRuntimeVersions(t *testing.T) {
	execution := map[string]any{worlds.ExecutionContextWorkflowCoreVersion: "5.7.1", worlds.ExecutionContextNodeVersion: "24.12.0"}
	run := worlds.Run{DeploymentID: "dpl_1", WorkflowName: "flow", EncryptionPublicKey: "key", SpecVersion: 9, ExecutionContext: execution}
	want := worlds.HookResumeContext{DeploymentID: "dpl_1", WorkflowName: "flow", EncryptionPublicKey: "key", RunSpecVersion: 9, WorkflowCoreVersion: "5.7.1", NodeVersion: "24.12.0"}
	if got := run.HookResumeContext(); got != want {
		t.Fatalf("run's resume context = %+v, want %+v", got, want)
	}
	input := worlds.EventsCreateRunInput{DeploymentID: "dpl_1", WorkflowName: "flow", EncryptionPublicKey: "key", ExecutionContext: execution}
	if got := input.HookResumeContext(); got != want {
		t.Fatalf("start hook's resume context = %+v, want %+v", got, want)
	}
	if got := (worlds.Run{}).HookResumeContext(); got.WorkflowCoreVersion != "" || got.NodeVersion != "" {
		t.Fatalf("resume context of a run without versions = %+v", got)
	}
}
