package postgresworld

import (
	"context"
	"testing"

	"github.com/eriicafes/go-workflow/worlds"
)

// TestDeleteDataFailureCode checks a run whose data could not be deleted is
// reported as failed with the code internal_error.
func TestDeleteDataFailureCode(t *testing.T) {
	t.Parallel()
	world := newTestWorld(t)
	runID := worlds.NewRunID()
	if _, err := world.EventsCreateRun(context.Background(), worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "w"}); err != nil {
		t.Fatal(err)
	}
	// A cancelled request cannot open the run's transaction.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result := world.deleteRunData(ctx, runID)
	if result.RunID != runID || result.Outcome != worlds.DeleteDataFailed || result.Code != "internal_error" || result.Retryable {
		t.Fatalf("failed deletion = %#v", result)
	}
}
