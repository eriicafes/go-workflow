package localworld

import (
	"context"
	"testing"

	"github.com/eriicafes/go-workflow/worlds"
)

// TestDeleteDataDeletesAgain checks deleting the data of a run whose data
// was deleted before reports already_deleted, keeps the run's first
// ExpiredAt, and still deletes what is left of it.
func TestDeleteDataDeletesAgain(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	world := storageWorld(t, Config{})
	runID := startedRun(t, world)
	appendEvent(t, world, runID, worlds.EventTypeStepCreated, "step_a", []byte(`{"stepName":"a","input":"aW5wdXQ="}`))
	appendEvent(t, world, runID, worlds.EventTypeRunCancelled, "", nil)
	first, err := world.RunsDeleteDataMany(ctx, worlds.RunsDeleteDataManyInput{RunIDs: []worlds.RunID{runID, "wrun_a.b"}})
	if err != nil || first.Results[0].Outcome != worlds.DeleteDataDeleted {
		t.Fatalf("first deletion = %#v err=%v", first, err)
	}
	// No run is stored under an unsafe ID.
	if first.Results[1].Outcome != worlds.DeleteDataNotFound {
		t.Fatalf("deletion of an unsafe ID = %#v", first.Results[1])
	}
	deleted, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID})
	if err != nil || deleted.ExpiredAt == nil {
		t.Fatalf("deleted run = %#v err=%v", deleted, err)
	}
	// Data an earlier deletion left behind: a chunk of the run's stream.
	if _, err := world.db.write.ExecContext(ctx, "INSERT INTO stream_chunks (name, idx, run_id, data, eof) VALUES ('left', 1, ?, x'61', 0)", string(runID)); err != nil {
		t.Fatal(err)
	}
	again, err := world.RunsDeleteDataMany(ctx, worlds.RunsDeleteDataManyInput{RunIDs: []worlds.RunID{runID}})
	if err != nil || again.Results[0].Outcome != worlds.DeleteDataAlreadyDeleted {
		t.Fatalf("second deletion = %#v err=%v", again, err)
	}
	if count := scalar[int](t, world, "SELECT count(*) FROM stream_chunks WHERE run_id = ? AND eof = 0", string(runID)); count != 0 {
		t.Fatalf("chunks left after the second deletion = %d", count)
	}
	if marker := scalar[int](t, world, "SELECT idx FROM stream_chunks WHERE name = 'left' AND eof = 1"); marker != 0 {
		t.Fatalf("end marker of the stream left behind is in row %d", marker)
	}
	kept, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID})
	if err != nil || kept.ExpiredAt == nil || !kept.ExpiredAt.Equal(*deleted.ExpiredAt) {
		t.Fatalf("ExpiredAt after the second deletion = %v, want %v (err=%v)", kept.ExpiredAt, deleted.ExpiredAt, err)
	}
}
