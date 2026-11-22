package postgresworld

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
	"github.com/jackc/pgx/v5"
)

func TestStreamWithALongNameNotifies(t *testing.T) {
	t.Parallel()
	world := newTestWorld(t)
	ctx := context.Background()
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runA, WorkflowName: "w"}); err != nil {
		t.Fatal(err)
	}
	name := strings.Repeat("n", 10_000)
	if err := world.StreamsWrite(ctx, worlds.StreamsWriteInput{RunID: runA, Name: name, Chunk: []byte("a")}); err != nil {
		t.Fatalf("write to a stream with a long name: %v", err)
	}
}

// TestStreamsAreKeyedByName checks a stream is read, appended to, and
// described by its name alone, while StreamsList still lists a run's own
// streams.
func TestStreamsAreKeyedByName(t *testing.T) {
	t.Parallel()
	world := newTestWorld(t)
	ctx := context.Background()
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

// TestStreamReadWithAnInvalidCursorStartsOver checks a cursor that does not
// decode reads from the start.
func TestStreamReadWithAnInvalidCursorStartsOver(t *testing.T) {
	t.Parallel()
	world := newTestWorld(t)
	ctx := context.Background()
	if err := world.StreamsWriteMulti(ctx, worlds.StreamsWriteMultiInput{RunID: runA, Name: "out", Chunks: [][]byte{[]byte("a"), []byte("b")}}); err != nil {
		t.Fatal(err)
	}
	for _, cursor := range []string{"not-a-cursor", "-1"} {
		chunks, err := world.StreamsGetChunks(ctx, worlds.StreamsGetChunksInput{RunID: runA, Name: "out", Cursor: cursor})
		if err != nil || len(chunks.Data) != 2 || chunks.Data[0].Index != 0 || chunks.Cursor != "2" {
			t.Fatalf("read with cursor %q = %#v err=%v", cursor, chunks, err)
		}
	}
	if chunks, err := world.StreamsGetChunks(ctx, worlds.StreamsGetChunksInput{RunID: runA, Name: "out", Cursor: "1"}); err != nil || len(chunks.Data) != 1 || string(chunks.Data[0].Data) != "b" {
		t.Fatalf("read with a valid cursor = %#v err=%v", chunks, err)
	}
}

// TestStreamWriteAnnouncesEachChunk checks a write of several chunks sends a
// workflow_event_chunk notification for each of them, and that a write or a
// close on a closed stream stores nothing.
func TestStreamWriteAnnouncesEachChunk(t *testing.T) {
	t.Parallel()
	world := newTestWorld(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	listener, err := world.pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Release()
	if _, err := listener.Exec(ctx, "LISTEN "+channelStreams); err != nil {
		t.Fatal(err)
	}
	defer listener.Exec(context.Background(), "UNLISTEN *")
	name := "announced-" + string(worlds.NewRunID())
	if err := world.StreamsWriteMulti(ctx, worlds.StreamsWriteMultiInput{RunID: runA, Name: name, Chunks: [][]byte{[]byte("a"), []byte("b"), []byte("c")}}); err != nil {
		t.Fatal(err)
	}
	rows, err := world.pool.Query(ctx, `SELECT id FROM workflow_stream_chunks WHERE stream_id = $1 ORDER BY id`, name)
	if err != nil {
		t.Fatal(err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil || len(ids) != 3 {
		t.Fatalf("chunk ids = %v err=%v", ids, err)
	}
	// Other tests' streams are announced on the same channel.
	var announced []string
	for len(announced) < len(ids) {
		notification, err := listener.Conn().WaitForNotification(ctx)
		if err != nil {
			t.Fatalf("announced %v of %v: %v", announced, ids, err)
		}
		var chunk chunkNotification
		if json.Unmarshal([]byte(notification.Payload), &chunk) == nil && chunk.StreamID == streamKey(name) {
			announced = append(announced, chunk.ChunkID)
		}
	}
	if !slices.Equal(announced, ids) {
		t.Fatalf("announced chunks = %v, want %v", announced, ids)
	}

	if err := world.StreamsClose(ctx, worlds.StreamsCloseInput{RunID: runA, Name: name}); err != nil {
		t.Fatal(err)
	}
	if err := world.StreamsWrite(ctx, worlds.StreamsWriteInput{RunID: runA, Name: name, Chunk: []byte("late")}); err != nil {
		t.Fatal(err)
	}
	if err := world.StreamsClose(ctx, worlds.StreamsCloseInput{RunID: runA, Name: name}); err != nil {
		t.Fatal(err)
	}
	var stored int
	if err := world.pool.QueryRow(ctx, `SELECT count(*) FROM workflow_stream_chunks WHERE stream_id = $1`, name).Scan(&stored); err != nil || stored != 4 {
		t.Fatalf("rows of a closed stream written and closed again = %d err=%v, want 3 chunks and one eof", stored, err)
	}
}
