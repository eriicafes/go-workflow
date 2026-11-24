package vercelworld

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// SDK: packages/world/src/start-hook.test.ts > start Hook schemas > keeps the start Hook and the public key on the queued run input
// SDK: packages/world/src/queue.test.ts > RunInputSchema encryptionPublicKey > keeps the run public key, so a resilient start can forward it
func TestSDKQueuedRunInputKeepsTheStartHookAndPublicKey(t *testing.T) {
	until := time.Date(2026, time.November, 1, 0, 0, 0, 0, time.UTC)
	input := worlds.QueueInput{RunID: "wrun_1", RunInput: &worlds.EventsCreateRunInput{
		RunID:               "wrun_1",
		WorkflowName:        "wf",
		DeploymentID:        "dpl_1",
		EncryptionPublicKey: "pk",
		StartHook:           &worlds.StartHook{Token: "order:1", TokenRetentionUntil: &until},
	}}
	encoded, err := json.Marshal(newQueueMessage("msg_1", input))
	if err != nil {
		t.Fatal(err)
	}
	var message queueMessage
	if err := json.Unmarshal(encoded, &message); err != nil {
		t.Fatal(err)
	}
	run := message.input().RunInput
	if run == nil || run.EncryptionPublicKey != "pk" || run.StartHook == nil || run.StartHook.Token != "order:1" || run.StartHook.TokenRetentionUntil == nil || !run.StartHook.TokenRetentionUntil.Equal(until) {
		t.Fatalf("queued run input = %+v", run)
	}
}
