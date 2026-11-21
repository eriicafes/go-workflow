package testworld_test

import (
	"context"
	"testing"

	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/testworld"
)

func TestStreamCursorsOutOfRangeDoNotPanic(t *testing.T) {
	ctx := context.Background()
	world := testworld.NewManual()
	defer world.Close()
	runID := runCursors
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "w"}); err != nil {
		t.Fatal(err)
	}
	if err := world.StreamsWrite(ctx, worlds.StreamsWriteInput{RunID: runID, Name: "s", Chunk: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	for _, cursor := range []string{"-3", "5", "x"} {
		if _, err := world.StreamsGetChunks(ctx, worlds.StreamsGetChunksInput{RunID: runID, Name: "s", Cursor: cursor}); err != nil {
			t.Fatalf("cursor %q: %v", cursor, err)
		}
	}
}
