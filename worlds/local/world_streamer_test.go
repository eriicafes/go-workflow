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

// TestPurgedStreamsEndInRowZero checks a run's streams are cut to an end
// marker in row 0 when its data is deleted, and that a stream first written
// or closed afterwards gets the same marker.
func TestPurgedStreamsEndInRowZero(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	world := startWorld(t, localworld.Config{Dir: dir}, nil)
	runID := worlds.NewRunID()
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "streams"}); err != nil {
		t.Fatal(err)
	}
	if err := world.StreamsWriteMulti(ctx, worlds.StreamsWriteMultiInput{RunID: runID, Name: "before", Chunks: [][]byte{[]byte("a"), []byte("b")}}); err != nil {
		t.Fatal(err)
	}
	if _, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeRunCancelled}}); err != nil {
		t.Fatal(err)
	}
	if result, err := world.RunsDeleteDataMany(ctx, worlds.RunsDeleteDataManyInput{RunIDs: []worlds.RunID{runID}}); err != nil || result.Results[0].Outcome != worlds.DeleteDataDeleted {
		t.Fatalf("deletion = %#v err=%v", result, err)
	}
	if err := world.StreamsWrite(ctx, worlds.StreamsWriteInput{RunID: runID, Name: "written", Chunk: []byte("c")}); err != nil {
		t.Fatal(err)
	}
	if err := world.StreamsClose(ctx, worlds.StreamsCloseInput{RunID: runID, Name: "closed"}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"before", "written", "closed"} {
		if rows := queryStrings(t, dir, "SELECT idx || ':' || eof FROM stream_chunks WHERE name = '"+name+"'"); !slices.Equal(rows, []string{"0:1"}) {
			t.Fatalf("rows of stream %q = %q, want its end marker in row 0", name, rows)
		}
		chunks, err := world.StreamsGetChunks(ctx, worlds.StreamsGetChunksInput{RunID: runID, Name: name})
		if err != nil || len(chunks.Data) != 0 || !chunks.Done || chunks.HasMore {
			t.Fatalf("stream %q = %#v err=%v", name, chunks, err)
		}
		if info, err := world.StreamsGetInfo(ctx, worlds.StreamsGetInfoInput{RunID: runID, Name: name}); err != nil || info.TailIndex != -1 || !info.Done {
			t.Fatalf("info of stream %q = %#v err=%v", name, info, err)
		}
	}
	// A stream closed while its run has its data ends after its last chunk.
	live := worlds.NewRunID()
	if err := world.StreamsWrite(ctx, worlds.StreamsWriteInput{RunID: live, Name: "live", Chunk: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	if err := world.StreamsClose(ctx, worlds.StreamsCloseInput{RunID: live, Name: "live"}); err != nil {
		t.Fatal(err)
	}
	if rows := queryStrings(t, dir, "SELECT idx || ':' || eof FROM stream_chunks WHERE name = 'live' ORDER BY idx"); !slices.Equal(rows, []string{"1:0", "2:1"}) {
		t.Fatalf("rows of a closed stream = %q", rows)
	}
}

// TestChunkCursors checks a chunks page's cursor is base64 of {"i": count},
// the count of chunks before the next one, and that a plain count is read
// too.
func TestChunkCursors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	world := startWorld(t, localworld.Config{Dir: t.TempDir()}, nil)
	runID := worlds.NewRunID()
	if err := world.StreamsWriteMulti(ctx, worlds.StreamsWriteMultiInput{RunID: runID, Name: "out", Chunks: [][]byte{[]byte("a"), []byte("b"), []byte("c")}}); err != nil {
		t.Fatal(err)
	}
	first, err := world.StreamsGetChunks(ctx, worlds.StreamsGetChunksInput{RunID: runID, Name: "out", Limit: 2})
	// base64 of {"i":2}.
	if err != nil || len(first.Data) != 2 || !first.HasMore || first.Cursor != "eyJpIjoyfQ==" {
		t.Fatalf("first page = %#v err=%v", first, err)
	}
	for _, cursor := range []string{first.Cursor, "2"} {
		rest, err := world.StreamsGetChunks(ctx, worlds.StreamsGetChunksInput{RunID: runID, Name: "out", Cursor: cursor})
		// base64 of {"i":3}.
		if err != nil || len(rest.Data) != 1 || rest.Data[0].Index != 2 || rest.HasMore || rest.Cursor != "eyJpIjozfQ==" {
			t.Fatalf("page after %q = %#v err=%v", cursor, rest, err)
		}
	}
}
