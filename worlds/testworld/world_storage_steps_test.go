package testworld_test

import (
	"context"
	"fmt"
	"testing"
	"testing/synctest"

	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/testworld"
)

func TestListsPageStablyWithEqualTimestamps(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		world := testworld.NewManual(nil)
		defer world.Close()
		// Minted in the bubble, whose clock the World checks it against.
		runID := worlds.NewRunID()
		if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "w"}); err != nil {
			t.Fatal(err)
		}
		for i := range 20 {
			if _, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeStepCreated, CorrelationID: fmt.Sprintf("step_%02d", i), Data: []byte(`{}`)}}); err != nil {
				t.Fatal(err)
			}
		}
		seen := map[worlds.StepID]bool{}
		pagination := worlds.Pagination{Limit: 5}
		for {
			page, err := world.StepsList(ctx, worlds.StepsListInput{RunID: runID, Pagination: pagination})
			if err != nil {
				t.Fatal(err)
			}
			for _, step := range page.Data {
				seen[step.StepID] = true
			}
			if !page.HasMore {
				break
			}
			pagination.Cursor = page.Cursor
		}
		if len(seen) != 20 {
			t.Fatalf("paged %d distinct steps, want 20", len(seen))
		}
	})
}
