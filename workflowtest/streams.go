package workflowtest

import (
	"context"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

func conformStreams(t *testing.T, world worlds.World) {
	ctx := context.Background()
	runID := conformCreateRun(t, world, "streams")
	session, err := world.StreamsCreateWriteSession(ctx, worlds.StreamsCreateWriteSessionInput{RunID: runID, Name: "output", WriterID: "a"})
	if err != nil {
		t.Fatal(err)
	}
	if session.ID() == "" {
		t.Fatal("session without ID")
	}
	_, err = world.StreamsCreateWriteSession(ctx, worlds.StreamsCreateWriteSessionInput{RunID: runID, Name: "output", WriterID: "b"})
	conformExpect(t, err, worlds.ErrStreamLocked, "second session")
	if err := session.Write(ctx, worlds.StreamWriteSessionWriteInput{Sequence: 0, Chunks: [][]byte{[]byte("a"), []byte("b")}}); err != nil {
		t.Fatal(err)
	}
	err = session.Write(ctx, worlds.StreamWriteSessionWriteInput{Sequence: 0, Chunks: [][]byte{[]byte("dup")}})
	conformExpect(t, err, worlds.ErrStreamLocked, "replayed sequence")
	err = session.Write(ctx, worlds.StreamWriteSessionWriteInput{Sequence: 2, Chunks: [][]byte{[]byte("gap")}})
	conformExpect(t, err, worlds.ErrStreamLocked, "skipped sequence")
	err = world.StreamsWriteSession(ctx, worlds.StreamsWriteSessionInput{RunID: runID, Name: "output", SessionID: "strm_other", Sequence: 1, Chunks: [][]byte{[]byte("x")}})
	conformExpect(t, err, worlds.ErrStreamLocked, "foreign session write")
	if err := session.Write(ctx, worlds.StreamWriteSessionWriteInput{Sequence: 1, Chunks: [][]byte{[]byte("c")}}); err != nil {
		t.Fatal(err)
	}
	err = world.StreamsWrite(ctx, worlds.StreamsWriteInput{RunID: runID, Name: "output", Chunk: []byte("direct")})
	conformExpect(t, err, worlds.ErrStreamLocked, "direct write during session")
	err = world.StreamsClose(ctx, worlds.StreamsCloseInput{RunID: runID, Name: "output"})
	conformExpect(t, err, worlds.ErrStreamLocked, "direct close during session")
	err = world.StreamsCloseWriteSession(ctx, worlds.StreamsCloseWriteSessionInput{RunID: runID, Name: "output", SessionID: "strm_other"})
	conformExpect(t, err, worlds.ErrStreamLocked, "foreign session close")
	if err := session.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := session.Release(ctx); err != nil {
		t.Fatalf("second release: %v", err)
	}
	conformExpect(t, session.Write(ctx, worlds.StreamWriteSessionWriteInput{Sequence: 2, Chunks: [][]byte{[]byte("late")}}), worlds.ErrRunTerminal, "write after release")

	next, err := world.StreamsCreateWriteSession(ctx, worlds.StreamsCreateWriteSessionInput{RunID: runID, Name: "output", WriterID: "b"})
	if err != nil {
		t.Fatalf("session after release: %v", err)
	}
	if next.ID() == session.ID() {
		t.Fatal("session ID reused")
	}
	if err := next.Write(ctx, worlds.StreamWriteSessionWriteInput{Sequence: 0, Chunks: [][]byte{[]byte("d")}}); err != nil {
		t.Fatal(err)
	}
	if err := next.Close(ctx); err != nil {
		t.Fatal(err)
	}
	_, err = world.StreamsCreateWriteSession(ctx, worlds.StreamsCreateWriteSessionInput{RunID: runID, Name: "output"})
	conformExpect(t, err, worlds.ErrRunTerminal, "session on closed stream")
	err = world.StreamsWrite(ctx, worlds.StreamsWriteInput{RunID: runID, Name: "output", Chunk: []byte("closed")})
	conformExpect(t, err, worlds.ErrStreamLocked, "direct write on closed stream")

	chunks, err := world.StreamsGetChunks(ctx, worlds.StreamsGetChunksInput{RunID: runID, Name: "output", Limit: 3})
	if err != nil || len(chunks.Data) != 3 || !chunks.HasMore || !chunks.Done || chunks.Cursor == "" {
		t.Fatalf("first chunk page = %#v err=%v", chunks, err)
	}
	for i, want := range []string{"a", "b", "c"} {
		if chunks.Data[i].Index != i || string(chunks.Data[i].Data) != want {
			t.Fatalf("chunk %d = %#v", i, chunks.Data[i])
		}
	}
	rest, err := world.StreamsGetChunks(ctx, worlds.StreamsGetChunksInput{RunID: runID, Name: "output", Cursor: chunks.Cursor})
	if err != nil || len(rest.Data) != 1 || rest.Data[0].Index != 3 || string(rest.Data[0].Data) != "d" || rest.HasMore || !rest.Done {
		t.Fatalf("second chunk page = %#v err=%v", rest, err)
	}
	tail, err := world.StreamsGetChunks(ctx, worlds.StreamsGetChunksInput{RunID: runID, Name: "output", Cursor: rest.Cursor, Wait: time.Second})
	if err != nil || len(tail.Data) != 0 || !tail.Done || tail.Cursor != rest.Cursor {
		t.Fatalf("tail of closed stream = %#v err=%v", tail, err)
	}
	info, err := world.StreamsGetInfo(ctx, worlds.StreamsGetInfoInput{RunID: runID, Name: "output"})
	if err != nil || info.TailIndex != 3 || !info.Done {
		t.Fatalf("stream info = %#v err=%v", info, err)
	}
	missing, err := world.StreamsGetInfo(ctx, worlds.StreamsGetInfoInput{RunID: runID, Name: "missing"})
	if err != nil || missing.TailIndex != -1 || missing.Done {
		t.Fatalf("missing stream info = %#v err=%v", missing, err)
	}
	empty, err := world.StreamsGetChunks(ctx, worlds.StreamsGetChunksInput{RunID: runID, Name: "missing"})
	if err != nil || len(empty.Data) != 0 || empty.Done {
		t.Fatalf("missing stream chunks = %#v err=%v", empty, err)
	}

	if events := conformAllEvents(t, world, runID); len(events) != 1 {
		t.Fatalf("streams consumed event slots: %d events", len(events))
	}
}

func conformStreamsDirect(t *testing.T, world worlds.World) {
	ctx := context.Background()
	runID := conformCreateRun(t, world, "streams_direct")
	if err := world.StreamsWrite(ctx, worlds.StreamsWriteInput{RunID: runID, Name: "b", Chunk: []byte("one")}); err != nil {
		t.Fatal(err)
	}
	if err := world.StreamsWriteMulti(ctx, worlds.StreamsWriteMultiInput{RunID: runID, Name: "b", Chunks: [][]byte{[]byte("two"), []byte("three")}}); err != nil {
		t.Fatal(err)
	}
	if err := world.StreamsWrite(ctx, worlds.StreamsWriteInput{RunID: runID, Name: "a", Chunk: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	if err := world.StreamsClose(ctx, worlds.StreamsCloseInput{RunID: runID, Name: "b"}); err != nil {
		t.Fatal(err)
	}
	chunks, err := world.StreamsGetChunks(ctx, worlds.StreamsGetChunksInput{RunID: runID, Name: "b"})
	if err != nil || len(chunks.Data) != 3 || string(chunks.Data[2].Data) != "three" || !chunks.Done || chunks.HasMore {
		t.Fatalf("direct chunks = %#v err=%v", chunks, err)
	}
	names, err := world.StreamsList(ctx, worlds.StreamsListInput{RunID: runID})
	if err != nil || len(names) != 2 || names[0] != "a" || names[1] != "b" {
		t.Fatalf("stream names = %v err=%v", names, err)
	}
	other, err := world.StreamsList(ctx, worlds.StreamsListInput{RunID: conformCreateRun(t, world, "streams_none")})
	if err != nil || len(other) != 0 {
		t.Fatalf("stream names of run without streams = %v err=%v", other, err)
	}
}

func conformStreamsWait(t *testing.T, world worlds.World) {
	ctx := context.Background()
	runID := conformCreateRun(t, world, "streams_wait")
	started := time.Now()
	empty, err := world.StreamsGetChunks(ctx, worlds.StreamsGetChunksInput{RunID: runID, Name: "live", Wait: 100 * time.Millisecond})
	if err != nil || len(empty.Data) != 0 || empty.Done {
		t.Fatalf("empty wait = %#v err=%v", empty, err)
	}
	if time.Since(started) < 90*time.Millisecond {
		t.Fatalf("wait returned after %v", time.Since(started))
	}
	go func() {
		time.Sleep(100 * time.Millisecond)
		_ = world.StreamsWrite(context.Background(), worlds.StreamsWriteInput{RunID: runID, Name: "live", Chunk: []byte("late")})
	}()
	chunks, err := world.StreamsGetChunks(ctx, worlds.StreamsGetChunksInput{RunID: runID, Name: "live", Wait: 10 * time.Second})
	if err != nil || len(chunks.Data) != 1 || string(chunks.Data[0].Data) != "late" {
		t.Fatalf("waited chunks = %#v err=%v", chunks, err)
	}
	go func() {
		time.Sleep(100 * time.Millisecond)
		_ = world.StreamsClose(context.Background(), worlds.StreamsCloseInput{RunID: runID, Name: "live"})
	}()
	done, err := world.StreamsGetChunks(ctx, worlds.StreamsGetChunksInput{RunID: runID, Name: "live", Cursor: chunks.Cursor, Wait: 10 * time.Second})
	if err != nil || len(done.Data) != 0 || !done.Done {
		t.Fatalf("wait for close = %#v err=%v", done, err)
	}
}
