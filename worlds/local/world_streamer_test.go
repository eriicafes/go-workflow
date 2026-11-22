package localworld_test

import (
	"context"
	"slices"
	"testing"

	"github.com/eriicafes/go-workflow/worlds"
	localworld "github.com/eriicafes/go-workflow/worlds/local"
)

// TestStreamsAreKeyedByName checks a stream is read, appended to, and
// described by its name alone, while StreamsList still lists a run's own
// streams.
func TestStreamsAreKeyedByName(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	world := startWorld(t, localworld.Config{Dir: t.TempDir()}, nil)
	other := worlds.NewRunID()
	if err := world.StreamsWrite(ctx, worlds.StreamsWriteInput{RunID: runA, Name: "shared", Chunk: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	// A write naming another run appends to the same stream.
	if err := world.StreamsWrite(ctx, worlds.StreamsWriteInput{RunID: other, Name: "shared", Chunk: []byte("b")}); err != nil {
		t.Fatal(err)
	}
	for _, runID := range []worlds.RunID{runA, other, worlds.NewRunID()} {
		chunks, err := world.StreamsGetChunks(ctx, worlds.StreamsGetChunksInput{RunID: runID, Name: "shared"})
		if err != nil || len(chunks.Data) != 2 || string(chunks.Data[0].Data) != "a" || string(chunks.Data[1].Data) != "b" || chunks.Data[1].Index != 1 {
			t.Fatalf("chunks read with run %s = %#v err=%v", runID, chunks, err)
		}
		if info, err := world.StreamsGetInfo(ctx, worlds.StreamsGetInfoInput{RunID: runID, Name: "shared"}); err != nil || info.TailIndex != 1 || info.Done {
			t.Fatalf("info read with run %s = %#v err=%v", runID, info, err)
		}
	}
	// Closing it under either run closes the one stream.
	if err := world.StreamsClose(ctx, worlds.StreamsCloseInput{RunID: other, Name: "shared"}); err != nil {
		t.Fatal(err)
	}
	if info, err := world.StreamsGetInfo(ctx, worlds.StreamsGetInfoInput{RunID: runA, Name: "shared"}); err != nil || !info.Done || info.TailIndex != 1 {
		t.Fatalf("info after close = %#v err=%v", info, err)
	}
	// Listing still goes by the run each chunk was written under.
	if err := world.StreamsWrite(ctx, worlds.StreamsWriteInput{RunID: runA, Name: "own", Chunk: []byte("c")}); err != nil {
		t.Fatal(err)
	}
	names, err := world.StreamsList(ctx, worlds.StreamsListInput{RunID: runA})
	if err != nil || !slices.Equal(names, []string{"own", "shared"}) {
		t.Fatalf("streams of run A = %q err=%v", names, err)
	}
	names, err = world.StreamsList(ctx, worlds.StreamsListInput{RunID: other})
	if err != nil || !slices.Equal(names, []string{"shared"}) {
		t.Fatalf("streams of the other run = %q err=%v", names, err)
	}
}
