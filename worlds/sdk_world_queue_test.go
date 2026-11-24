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

// A Go queue carries no health checks, so only step deliveries are left to
// check.
//
// SDK: packages/world/src/queue.test.ts > orchestratorRunIdOf > leaves step messages and health checks unserialized
func TestSDKStepDeliveriesAreNotSerialized(t *testing.T) {
	step := worlds.StepID("step_1")
	if input := (worlds.QueueInput{RunID: "wrun_a", StepID: &step}); input.SerializedFlow() {
		t.Fatalf("SerializedFlow(%+v) = true", input)
	}
}
