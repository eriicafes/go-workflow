package vercelworld

import (
	"context"
	"net/http"
	"testing"

	"github.com/eriicafes/go-workflow/worlds"
)

func TestStepsListReturnsANotFound(t *testing.T) {
	world := newBackendWorld(t, func(writer http.ResponseWriter, request *http.Request) {
		writeError(writer, http.StatusNotFound, "run-not-found", "run not found")
	})
	if _, err := world.StepsList(context.Background(), worlds.StepsListInput{RunID: "wrun_1"}); statusOf(err) != http.StatusNotFound {
		t.Fatalf("steps of a missing run = %v, want the backend's 404", err)
	}
}

func TestListsKeepTheCursorOnTheLastPage(t *testing.T) {
	world := newBackendWorld(t, func(writer http.ResponseWriter, request *http.Request) {
		writeCBOR(writer, map[string]any{"data": []map[string]any{}, "cursor": "resume", "hasMore": false})
	})
	page, err := world.StepsList(context.Background(), worlds.StepsListInput{RunID: "wrun_1"})
	if err != nil {
		t.Fatal(err)
	}
	if page.Cursor != "resume" || page.HasMore {
		t.Fatalf("last page = %+v, want the backend's cursor", page)
	}

	all, _, _ := newWorlds(t, 1)
	ctx := context.Background()
	runID := worlds.NewRunID()
	if _, err := all[0].EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "cursor", Input: []byte(`"x"`)}); err != nil {
		t.Fatal(err)
	}
	events, err := all[0].EventsList(ctx, worlds.EventsListInput{RunID: runID})
	if err != nil {
		t.Fatal(err)
	}
	if events.HasMore || len(events.Data) == 0 || events.Cursor == "" {
		t.Fatalf("last events page = %+v, want the backend's cursor", events.Page)
	}
}
