package worlds_test

import (
	"testing"

	"github.com/eriicafes/go-workflow/worlds"
)

// SDK: packages/world/src/queue.test.ts > orchestratorRunIdOf > names the run of an orchestrator delivery
func TestSDKARunsFlowsAreSerialized(t *testing.T) {
	for _, input := range []worlds.QueueInput{
		{RunID: "wrun_a"},
		{RunID: "wrun_a", RunInput: &worlds.EventsCreateRunInput{RunID: "wrun_a"}},
	} {
		if !input.SerializedFlow() {
			t.Errorf("SerializedFlow(%+v) = false", input)
		}
	}
}

// A health check names no run, so it has no run to serialize, whatever
// SerializedFlow reports.
//
// SDK: packages/world/src/queue.test.ts > orchestratorRunIdOf > leaves step messages and health checks unserialized
func TestSDKStepDeliveriesAreNotSerialized(t *testing.T) {
	step := worlds.StepID("step_1")
	if input := (worlds.QueueInput{RunID: "wrun_a", StepID: &step}); input.SerializedFlow() {
		t.Fatalf("SerializedFlow(%+v) = true", input)
	}
	health := worlds.QueueInput{SDKMessage: []byte(`{"queueName":"__wkf_workflow_health_check","message":{"__healthCheck":true,"correlationId":"c"}}`)}
	if _, ok := worlds.ParseHealthCheck(health); !ok || health.RunID != "" {
		t.Fatalf("health check %+v names a run or is not one", health)
	}
}
