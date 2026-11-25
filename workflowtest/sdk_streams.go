package workflowtest

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// sdkStreamWait is how long a read of a stream blocks for its next chunk.
const sdkStreamWait = 10 * time.Second

// sdkStream names a stream of runID. Streams are keyed by name alone, so the
// name holds the run, as worlds.UserStreamName builds it.
func sdkStream(runID worlds.RunID, name string) string {
	return worlds.UserStreamName(runID, name)
}

func sdkWrite(t *testing.T, world worlds.World, runID worlds.RunID, name string, chunks ...string) {
	t.Helper()
	data := make([][]byte, len(chunks))
	for i, chunk := range chunks {
		data[i] = []byte(chunk)
	}
	sdkWriteBytes(t, world, runID, name, data...)
}

func sdkWriteBytes(t *testing.T, world worlds.World, runID worlds.RunID, name string, chunks ...[]byte) {
	t.Helper()
	var err error
	if len(chunks) == 1 {
		err = world.StreamsWrite(context.Background(), worlds.StreamsWriteInput{RunID: runID, Name: name, Chunk: chunks[0]})
	} else {
		err = world.StreamsWriteMulti(context.Background(), worlds.StreamsWriteMultiInput{RunID: runID, Name: name, Chunks: chunks})
	}
	if err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
}

func sdkClose(t *testing.T, world worlds.World, runID worlds.RunID, name string) {
	t.Helper()
	if err := world.StreamsClose(context.Background(), worlds.StreamsCloseInput{RunID: runID, Name: name}); err != nil {
		t.Fatalf("close %s: %v", name, err)
	}
}

func sdkInfo(t *testing.T, world worlds.World, runID worlds.RunID, name string, tailIndex int, done bool) {
	t.Helper()
	info, err := world.StreamsGetInfo(context.Background(), worlds.StreamsGetInfoInput{RunID: runID, Name: name})
	if err != nil || info.TailIndex != tailIndex || info.Done != done {
		t.Fatalf("info of %s = %+v err=%v, want tail %d done %v", name, info, err, tailIndex, done)
	}
}

func sdkList(t *testing.T, world worlds.World, runID worlds.RunID, want ...string) {
	t.Helper()
	names, err := world.StreamsList(context.Background(), worlds.StreamsListInput{RunID: runID})
	slices.Sort(names)
	want = slices.Clone(want)
	slices.Sort(want)
	if err != nil || !slices.Equal(names, want) {
		t.Fatalf("streams of %s = %q err=%v, want %q", runID, names, err, want)
	}
}

// sdkReadChunks reads a stream from startIndex as a reader does: page after
// page, waiting for more, until the stream is done. It stops early after max
// chunks when max is above zero.
func sdkReadChunks(ctx context.Context, world worlds.World, runID worlds.RunID, name string, startIndex, max int) ([][]byte, error) {
	var chunks [][]byte
	cursor := ""
	deadline := time.Now().Add(2 * sdkStreamWait)
	for time.Now().Before(deadline) {
		page, err := world.StreamsGetChunks(ctx, worlds.StreamsGetChunksInput{RunID: runID, Name: name, Cursor: cursor, StartIndex: startIndex, Wait: sdkStreamWait})
		if err != nil {
			return chunks, err
		}
		for _, chunk := range page.Data {
			chunks = append(chunks, chunk.Data)
			if max > 0 && len(chunks) == max {
				return chunks, nil
			}
		}
		if page.Done && !page.HasMore {
			return chunks, nil
		}
		if page.Cursor != "" {
			cursor = page.Cursor
		}
	}
	return chunks, fmt.Errorf("stream %s did not end", name)
}

func sdkReadText(t *testing.T, world worlds.World, runID worlds.RunID, name string, startIndex int) []string {
	t.Helper()
	chunks, err := sdkReadChunks(context.Background(), world, runID, name, startIndex, 0)
	if err != nil {
		t.Fatal(err)
	}
	return sdkTexts(chunks)
}

func sdkTexts(chunks [][]byte) []string {
	texts := make([]string, len(chunks))
	for i, chunk := range chunks {
		texts[i] = string(chunk)
	}
	return texts
}

// sdkLiveReader reads a stream in the background, collecting chunks as they
// arrive, until the stream is done.
type sdkLiveReader struct {
	mu       sync.Mutex
	received []string
	err      error
	done     chan struct{}
}

func sdkCollect(world worlds.World, runID worlds.RunID, name string, startIndex int) *sdkLiveReader {
	reader := &sdkLiveReader{done: make(chan struct{})}
	go func() {
		defer close(reader.done)
		cursor := ""
		for {
			page, err := world.StreamsGetChunks(context.Background(), worlds.StreamsGetChunksInput{RunID: runID, Name: name, Cursor: cursor, StartIndex: startIndex, Wait: sdkStreamWait})
			reader.mu.Lock()
			if err != nil {
				reader.err = err
				reader.mu.Unlock()
				return
			}
			for _, chunk := range page.Data {
				reader.received = append(reader.received, string(chunk.Data))
			}
			reader.mu.Unlock()
			if page.Done && !page.HasMore {
				return
			}
			if page.Cursor != "" {
				cursor = page.Cursor
			}
		}
	}()
	return reader
}

func (r *sdkLiveReader) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.received)
}

// waitFor waits until the reader has received count chunks.
func (r *sdkLiveReader) waitFor(t *testing.T, count int) {
	t.Helper()
	deadline := time.Now().Add(sdkStreamWait)
	for r.count() < count {
		if time.Now().After(deadline) {
			t.Fatalf("reader received %d chunks, want %d", r.count(), count)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// wait waits for the stream to end and returns what the reader received.
func (r *sdkLiveReader) wait(t *testing.T) []string {
	t.Helper()
	select {
	case <-r.done:
	case <-time.After(2 * sdkStreamWait):
		t.Fatal("reader did not end")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		t.Fatal(r.err)
	}
	return r.received
}

func sdkExpectTexts(t *testing.T, label string, got []string, want ...string) {
	t.Helper()
	if want == nil {
		want = []string{}
	}
	if got == nil {
		got = []string{}
	}
	if !slices.Equal(got, want) {
		t.Fatalf("%s = %q, want %q", label, got, want)
	}
}

// SDK: packages/world-local/src/streamer.test.ts > streamer > write and read > round-trips string chunks
// SDK: packages/world-local/src/streamer.test.ts > streamer > write and read > round-trips Buffer chunks
// SDK: packages/world-local/src/streamer.test.ts > streamer > write and read > round-trips binary data byte for byte
// SDK: packages/world-local/src/streamer.test.ts > streamer > write and read > preserves multi-byte UTF-8 strings
// SDK: packages/world-local/src/streamer.test.ts > streamer > write and read > keeps streams independent
func conformSDKStreamsRoundTrip(t *testing.T, world worlds.World) {
	runID := conformCreateRun(t, world, "sdk_streams_round_trip")
	text := sdkStream(runID, "string-stream")
	sdkWrite(t, world, runID, text, "hello ")
	sdkWrite(t, world, runID, text, "world")
	sdkClose(t, world, runID, text)
	sdkExpectTexts(t, "string chunks", sdkReadText(t, world, runID, text, 0), "hello ", "world")

	buffer := sdkStream(runID, "buffer-stream")
	sdkWrite(t, world, runID, buffer, "chunk1")
	sdkWrite(t, world, runID, buffer, "chunk2")
	sdkClose(t, world, runID, buffer)
	sdkExpectTexts(t, "buffer chunks", sdkReadText(t, world, runID, buffer, 0), "chunk1", "chunk2")

	binary := sdkStream(runID, "binary-stream")
	sdkWriteBytes(t, world, runID, binary, []byte{0, 1, 2, 255})
	sdkWriteBytes(t, world, runID, binary, []byte{254, 253, 0, 6})
	sdkClose(t, world, runID, binary)
	chunks, err := sdkReadChunks(context.Background(), world, runID, binary, 0, 0)
	if err != nil || len(chunks) != 2 || !bytes.Equal(chunks[0], []byte{0, 1, 2, 255}) || !bytes.Equal(chunks[1], []byte{254, 253, 0, 6}) {
		t.Fatalf("binary chunks = %v err=%v", chunks, err)
	}

	utf8 := sdkStream(runID, "utf8-stream")
	const special = "special chars: ñáéíóú ✓ 😀"
	sdkWrite(t, world, runID, utf8, special)
	sdkClose(t, world, runID, utf8)
	sdkExpectTexts(t, "UTF-8 chunk", sdkReadText(t, world, runID, utf8, 0), special)

	one, two := sdkStream(runID, "stream1"), sdkStream(runID, "stream2")
	sdkWrite(t, world, runID, one, "data1")
	sdkWrite(t, world, runID, two, "data2")
	sdkWrite(t, world, runID, one, "data3")
	sdkClose(t, world, runID, one)
	sdkClose(t, world, runID, two)
	sdkExpectTexts(t, "stream1", sdkReadText(t, world, runID, one, 0), "data1", "data3")
	sdkExpectTexts(t, "stream2", sdkReadText(t, world, runID, two, 0), "data2")
}

// SDK: packages/world-local/src/streamer.test.ts > streamer > write and read > resumes with startIndex after a cancelled read (reproduces vibe platform bug)
func conformSDKStreamsResume(t *testing.T, world worlds.World) {
	runID := conformCreateRun(t, world, "sdk_streams_resume")
	name := sdkStream(runID, "resumption-stream")
	for i := range 4 {
		sdkWrite(t, world, runID, name, fmt.Sprintf("chunk%d", i))
	}
	// A first read, given up after two chunks while the stream is open.
	ctx, cancel := context.WithCancel(context.Background())
	first, err := sdkReadChunks(ctx, world, runID, name, 0, 2)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	sdkExpectTexts(t, "first read", sdkTexts(first), "chunk0", "chunk1")
	sdkClose(t, world, runID, name)
	sdkExpectTexts(t, "resumed read", sdkReadText(t, world, runID, name, 2), "chunk2", "chunk3")
}

// SDK: packages/world-local/src/streamer.test.ts > streamer > writeMulti > preserves chunk ordering
// SDK: packages/world-local/src/streamer.test.ts > streamer > writeMulti > writes nothing for an empty chunks array
// SDK: packages/world-local/src/streamer.test.ts > streamer > writeMulti > handles mixed string and Uint8Array chunks
func conformSDKStreamsWriteMulti(t *testing.T, world worlds.World) {
	runID := conformCreateRun(t, world, "sdk_streams_write_multi")
	ordered := sdkStream(runID, "ordered-multi-stream")
	sdkWrite(t, world, runID, ordered, "first", "second", "third")
	sdkClose(t, world, runID, ordered)
	sdkExpectTexts(t, "group", sdkReadText(t, world, runID, ordered, 0), "first", "second", "third")

	empty := conformCreateRun(t, world, "sdk_streams_write_multi_empty")
	name := sdkStream(empty, "empty-multi-stream")
	if err := world.StreamsWriteMulti(context.Background(), worlds.StreamsWriteMultiInput{RunID: empty, Name: name, Chunks: [][]byte{}}); err != nil {
		t.Fatal(err)
	}
	sdkInfo(t, world, empty, name, -1, false)
	sdkList(t, world, empty)

	mixed := sdkStream(runID, "mixed-multi-stream")
	sdkWriteBytes(t, world, runID, mixed, []byte("string-chunk"), []byte{1, 2, 3, 4}, []byte("buffer-chunk"))
	sdkClose(t, world, runID, mixed)
	chunks, err := sdkReadChunks(context.Background(), world, runID, mixed, 0, 0)
	if err != nil || len(chunks) != 3 || string(chunks[0]) != "string-chunk" || !bytes.Equal(chunks[1], []byte{1, 2, 3, 4}) || string(chunks[2]) != "buffer-chunk" {
		t.Fatalf("mixed chunks = %q err=%v", chunks, err)
	}
}

// SDK: packages/world-local/src/streamer.test.ts > streamer > writeMulti > lands a group whole and in order for a reader that opens mid-write
func conformSDKStreamsGroupForAReaderOpeningMidWrite(t *testing.T, world worlds.World) {
	runID := conformCreateRun(t, world, "sdk_streams_mid_write")
	name := sdkStream(runID, "write-multi-order")
	sdkWrite(t, world, runID, name, "zero\n")
	// The reader opens while the group is being written.
	written := make(chan error, 1)
	go func() {
		written <- world.StreamsWriteMulti(context.Background(), worlds.StreamsWriteMultiInput{RunID: runID, Name: name, Chunks: [][]byte{[]byte("first\n"), []byte("second\n"), []byte("third\n")}})
	}()
	reader := sdkCollect(world, runID, name, 0)
	if err := <-written; err != nil {
		t.Fatal(err)
	}
	sdkClose(t, world, runID, name)
	sdkExpectTexts(t, "chunks", reader.wait(t), "zero\n", "first\n", "second\n", "third\n")
}

// SDK: packages/world-local/src/streamer.test.ts > streamer > close > closes a stream with no chunks
// SDK: packages/world-local/src/streamer.test.ts > streamer > close > closes a stream with existing chunks
// SDK: packages/world-local/src/streamer.test.ts > streamer > close > ends a live reader when the stream closes
func conformSDKStreamsClose(t *testing.T, world worlds.World) {
	ctx := context.Background()
	runID := conformCreateRun(t, world, "sdk_streams_close")
	empty := sdkStream(runID, "empty-stream")
	sdkClose(t, world, runID, empty)
	sdkExpectTexts(t, "closed empty stream", sdkReadText(t, world, runID, empty, 0))
	sdkInfo(t, world, runID, empty, -1, true)
	page, err := world.StreamsGetChunks(ctx, worlds.StreamsGetChunksInput{RunID: runID, Name: empty})
	if err != nil || len(page.Data) != 0 || !page.Done {
		t.Fatalf("page of a closed empty stream = %#v err=%v", page, err)
	}

	existing := sdkStream(runID, "existing-stream")
	sdkWrite(t, world, runID, existing, "chunk1")
	sdkWrite(t, world, runID, existing, "chunk2")
	sdkClose(t, world, runID, existing)
	sdkInfo(t, world, runID, existing, 1, true)
	sdkExpectTexts(t, "closed stream", sdkReadText(t, world, runID, existing, 0), "chunk1", "chunk2")

	live := sdkStream(runID, "live-close")
	reader := sdkCollect(world, runID, live, 0)
	sdkWrite(t, world, runID, live, "one")
	reader.waitFor(t, 1)
	sdkClose(t, world, runID, live)
	sdkExpectTexts(t, "live reader", reader.wait(t), "one")
}

// SDK: packages/world-local/src/streamer.test.ts > streamer > get with startIndex > skips that many chunks from the start
// SDK: packages/world-local/src/streamer.test.ts > streamer > get with startIndex > ends at once when it starts past the tail of a finished stream
// SDK: packages/world-local/src/streamer.test.ts > streamer > get with startIndex > supports a negative startIndex to read from the end
// SDK: packages/world-local/src/streamer.test.ts > streamer > get with startIndex > resolves a negative startIndex against an open stream, then reads live
// SDK: packages/world-local/src/streamer.test.ts > streamer > get with startIndex > clamps a negative startIndex beyond the chunk count to 0
func conformSDKStreamsReadFromAnIndex(t *testing.T, world worlds.World) {
	runID := conformCreateRun(t, world, "sdk_streams_start_index")
	write := func(name string, count int, close bool) string {
		t.Helper()
		name = sdkStream(runID, name)
		for i := range count {
			sdkWrite(t, world, runID, name, fmt.Sprintf("chunk%d", i))
		}
		if close {
			sdkClose(t, world, runID, name)
		}
		return name
	}
	sdkExpectTexts(t, "from 1", sdkReadText(t, world, runID, write("positive", 4, true), 1), "chunk1", "chunk2", "chunk3")
	pastTail := write("past-tail", 2, true)
	started := time.Now()
	sdkExpectTexts(t, "past the tail", sdkReadText(t, world, runID, pastTail, 5))
	if time.Since(started) > sdkStreamWait/2 {
		t.Fatalf("a read past the tail of a finished stream waited %s", time.Since(started))
	}
	sdkExpectTexts(t, "from -2", sdkReadText(t, world, runID, write("negative", 4, true), -2), "chunk2", "chunk3")

	open := write("negative-open", 3, false)
	reader := sdkCollect(world, runID, open, -1)
	reader.waitFor(t, 1)
	sdkWrite(t, world, runID, open, "chunk3")
	sdkClose(t, world, runID, open)
	sdkExpectTexts(t, "from -1 of an open stream", reader.wait(t), "chunk2", "chunk3")

	sdkExpectTexts(t, "from -100", sdkReadText(t, world, runID, write("negative-clamped", 2, true), -100), "chunk0", "chunk1")
}

// SDK: packages/world-local/src/streamer.test.ts > streamer > get with startIndex > never delivers a chunk before startIndex, even one written live
// SDK: packages/world-postgres/src/streamer.test.ts > postgres streamer reader listener cleanup > does not count a redelivered skipped chunk against the start offset
func conformSDKStreamsLiveReadFromAnIndex(t *testing.T, world worlds.World) {
	runID := conformCreateRun(t, world, "sdk_streams_live_start_index")
	name := sdkStream(runID, "start-index-live")
	reader := sdkCollect(world, runID, name, 1)
	sdkWrite(t, world, runID, name, "c0\n")
	sdkWrite(t, world, runID, name, "c1\n")
	time.Sleep(50 * time.Millisecond)
	sdkClose(t, world, runID, name)
	sdkExpectTexts(t, "from 1", reader.wait(t), "c1\n")

	offset := sdkStream(runID, "stream-offset")
	reader = sdkCollect(world, runID, offset, 2)
	sdkWrite(t, world, runID, offset, "first")
	sdkWrite(t, world, runID, offset, "second")
	sdkWrite(t, world, runID, offset, "third")
	reader.waitFor(t, 1)
	sdkClose(t, world, runID, offset)
	sdkExpectTexts(t, "from 2", reader.wait(t), "third")
}

// SDK: packages/world-local/src/streamer.test.ts > streamer > chunk order > does not lose or duplicate chunks written while a reader opens
// SDK: packages/world-local/src/streamer.test.ts > streamer > chunk order > keeps order when chunks arrive while a reader is catching up
func conformSDKStreamsOrder(t *testing.T, world worlds.World) {
	runID := conformCreateRun(t, world, "sdk_streams_order")
	var want []string
	for i := range 10 {
		want = append(want, fmt.Sprintf("%d\n", i))
	}
	for iteration := range 3 {
		name := sdkStream(runID, fmt.Sprintf("race-%d", iteration))
		sdkWrite(t, world, runID, name, "0\n")
		sdkWrite(t, world, runID, name, "1\n")
		// Writing goes on while the reader opens.
		written := make(chan error, 1)
		go func() {
			for i := 2; i < 10; i++ {
				if err := world.StreamsWrite(context.Background(), worlds.StreamsWriteInput{RunID: runID, Name: name, Chunk: fmt.Appendf(nil, "%d\n", i)}); err != nil {
					written <- err
					return
				}
			}
			written <- world.StreamsClose(context.Background(), worlds.StreamsCloseInput{RunID: runID, Name: name})
		}()
		got := sdkReadText(t, world, runID, name, 0)
		if err := <-written; err != nil {
			t.Fatal(err)
		}
		sdkExpectTexts(t, fmt.Sprintf("iteration %d", iteration), got, want...)
	}

	name := sdkStream(runID, "ordering-test")
	for i := range 5 {
		sdkWrite(t, world, runID, name, want[i])
	}
	reader := sdkCollect(world, runID, name, 0)
	for i := 5; i < 10; i++ {
		sdkWrite(t, world, runID, name, want[i])
	}
	sdkClose(t, world, runID, name)
	if got := strings.Join(reader.wait(t), ""); got != strings.Join(want, "") {
		t.Fatalf("caught-up reader = %q", got)
	}
}

// SDK: packages/world-local/src/streamer.test.ts > streamer > reader lifecycle (vercel/workflow#2795, #2797) > reads a long backlog a bounded batch at a time, as the consumer asks
// SDK: packages/world-local/src/streamer.test.ts > streamer > reader lifecycle (vercel/workflow#2795, #2797) > reads only the stream asked for, however busy the others are
func conformSDKStreamsBacklog(t *testing.T, world worlds.World) {
	ctx := context.Background()
	runID := conformCreateRun(t, world, "sdk_streams_backlog")
	name := sdkStream(runID, "backlog")
	// 2000 chunks, written in groups small enough for every World's
	// largest write.
	chunk := make([]byte, 1024)
	for range 40 {
		group := make([][]byte, 50)
		for i := range group {
			group[i] = chunk
		}
		sdkWriteBytes(t, world, runID, name, group...)
	}
	sdkClose(t, world, runID, name)
	// A read without a limit returns a bounded page, and the reader asks for
	// the rest.
	page, err := world.StreamsGetChunks(ctx, worlds.StreamsGetChunksInput{RunID: runID, Name: name})
	if err != nil || len(page.Data) == 0 || len(page.Data) >= 2000 || !page.HasMore {
		t.Fatalf("first page of the backlog has %d chunks, more %v err=%v", len(page.Data), page.HasMore, err)
	}
	chunks, err := sdkReadChunks(ctx, world, runID, name, 0, 0)
	if err != nil || len(chunks) != 2000 {
		t.Fatalf("backlog = %d chunks err=%v, want 2000", len(chunks), err)
	}

	target, noise := sdkStream(runID, "target"), sdkStream(runID, "noise")
	sdkWrite(t, world, runID, target, "hi")
	for start := 0; start < 2000; start += 50 {
		group := make([]string, 50)
		for i := range group {
			group[i] = fmt.Sprintf("noise%d", start+i)
		}
		sdkWrite(t, world, runID, noise, group...)
	}
	sdkInfo(t, world, runID, target, 0, false)
	only, err := world.StreamsGetChunks(ctx, worlds.StreamsGetChunksInput{RunID: runID, Name: target})
	if err != nil || len(only.Data) != 1 || string(only.Data[0].Data) != "hi" {
		t.Fatalf("target chunks = %#v err=%v", only, err)
	}
}

// SDK: packages/world-local/src/streamer.test.ts > streamer > list > returns an empty array when no streams exist
// SDK: packages/world-local/src/streamer.test.ts > streamer > list > returns the streams of the run
// SDK: packages/world-local/src/streamer.test.ts > streamer > list > does not return streams of other runs
// SDK: packages/world-local/src/streamer.test.ts > streamer > list > returns each stream name once, however many chunks it has
// SDK: packages/world-local/src/streamer.test.ts > streamer > list > handles stream names with dashes
// SDK: packages/world-local/src/streamer.test.ts > streamer > list > lists a stream that was only closed
func conformSDKStreamsList(t *testing.T, world worlds.World) {
	none := conformCreateRun(t, world, "sdk_streams_list_none")
	sdkList(t, world, none)

	runID := conformCreateRun(t, world, "sdk_streams_list")
	stdout, stderr := sdkStream(runID, "my-stdout-stream"), sdkStream(runID, "my-stderr-stream")
	sdkWrite(t, world, runID, stdout, "stdout output")
	sdkWrite(t, world, runID, stderr, "stderr output")
	sdkClose(t, world, runID, stdout)
	sdkClose(t, world, runID, stderr)
	sdkList(t, world, runID, stdout, stderr)

	target, other := conformCreateRun(t, world, "sdk_streams_list_target"), conformCreateRun(t, world, "sdk_streams_list_other")
	targetName, otherName := sdkStream(target, "target-stdout"), sdkStream(other, "other-stdout")
	sdkWrite(t, world, target, targetName, "target")
	sdkWrite(t, world, other, otherName, "other")
	sdkList(t, world, target, targetName)
	sdkList(t, world, other, otherName)

	chunked := conformCreateRun(t, world, "sdk_streams_list_chunked")
	chunkedName := sdkStream(chunked, "chunked-output")
	sdkWrite(t, world, chunked, chunkedName, "chunk1")
	sdkWrite(t, world, chunked, chunkedName, "chunk2", "chunk3")
	sdkClose(t, world, chunked, chunkedName)
	sdkList(t, world, chunked, chunkedName)

	dashed := conformCreateRun(t, world, "sdk_streams_list_dashes")
	dashedName := sdkStream(dashed, "my-complex-stream-name")
	sdkWrite(t, world, dashed, dashedName, "data")
	sdkClose(t, world, dashed, dashedName)
	sdkList(t, world, dashed, dashedName)

	closed := conformCreateRun(t, world, "sdk_streams_list_closed")
	closedName := sdkStream(closed, "close-only-stream")
	sdkClose(t, world, closed, closedName)
	sdkList(t, world, closed, closedName)
}

// SDK: packages/world-local/src/streamer.test.ts > streamer > list > concurrent first writes > keeps every name when many streams of a run first write at once
// SDK: packages/world-local/src/streamer.test.ts > streamer > list > concurrent first writes > keeps every name across write, writeMulti and close
func conformSDKStreamsListConcurrentFirstWrites(t *testing.T, world worlds.World) {
	ctx := context.Background()
	runID := conformCreateRun(t, world, "sdk_streams_list_concurrent")
	var names []string
	var group sync.WaitGroup
	errs := make(chan error, 20)
	for i := range 20 {
		name := sdkStream(runID, fmt.Sprintf("stream-%d", i))
		names = append(names, name)
		group.Go(func() {
			errs <- world.StreamsWrite(ctx, worlds.StreamsWriteInput{RunID: runID, Name: name, Chunk: []byte("data")})
		})
	}
	group.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	sdkList(t, world, runID, names...)

	mixed := conformCreateRun(t, world, "sdk_streams_list_mixed")
	name := func(value string) string { return sdkStream(mixed, value) }
	writes := []func() error{
		func() error {
			return world.StreamsWrite(ctx, worlds.StreamsWriteInput{RunID: mixed, Name: name("written-0"), Chunk: []byte("a")})
		},
		func() error {
			return world.StreamsWriteMulti(ctx, worlds.StreamsWriteMultiInput{RunID: mixed, Name: name("multi-0"), Chunks: [][]byte{[]byte("a"), []byte("b")}})
		},
		func() error {
			return world.StreamsClose(ctx, worlds.StreamsCloseInput{RunID: mixed, Name: name("closed-0")})
		},
		func() error {
			return world.StreamsWrite(ctx, worlds.StreamsWriteInput{RunID: mixed, Name: name("written-1"), Chunk: []byte("a")})
		},
		func() error {
			return world.StreamsWriteMulti(ctx, worlds.StreamsWriteMultiInput{RunID: mixed, Name: name("multi-1"), Chunks: [][]byte{[]byte("a"), []byte("b")}})
		},
		func() error {
			return world.StreamsClose(ctx, worlds.StreamsCloseInput{RunID: mixed, Name: name("closed-1")})
		},
	}
	errs = make(chan error, len(writes))
	for _, write := range writes {
		group.Go(func() { errs <- write() })
	}
	group.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	sdkList(t, world, mixed, name("closed-0"), name("closed-1"), name("multi-0"), name("multi-1"), name("written-0"), name("written-1"))
}

// conformSDKStreamsGetChunks pages through streams. A page that is not the
// last reports Done once the stream is closed, and the last page keeps a
// cursor, so a reader can wait for more: those are Go's contract, where the
// SDK reports done only on the last page and ends its cursor there.
//
// SDK: packages/world-local/src/streamer.test.ts > streamer > getChunks > paginates through all chunks
// SDK: packages/world-local/src/streamer.test.ts > streamer > getChunks > reports done on a page that ends exactly at the end of the stream
// SDK: packages/world-local/src/streamer.test.ts > streamer > getChunks > continues from a cursor as an open stream grows
// SDK: packages/world-local/src/streamer.test.ts > streamer > getChunks > returns done=false for an in-progress stream
// SDK: packages/world-local/src/streamer.test.ts > streamer > getChunks > returns empty data for a nonexistent stream
// SDK: packages/world-local/src/streamer.test.ts > streamer > getChunks > restarts from the beginning on an invalid cursor
func conformSDKStreamsGetChunks(t *testing.T, world worlds.World) {
	ctx := context.Background()
	runID := conformCreateRun(t, world, "sdk_streams_get_chunks")
	get := func(input worlds.StreamsGetChunksInput) worlds.StreamChunksResult {
		t.Helper()
		input.RunID = runID
		page, err := world.StreamsGetChunks(ctx, input)
		if err != nil {
			t.Fatalf("StreamsGetChunks(%+v): %v", input, err)
		}
		return page
	}
	indexes := func(page worlds.StreamChunksResult) (string, string) {
		var index, data string
		for _, chunk := range page.Data {
			index += fmt.Sprint(chunk.Index)
			data += string(chunk.Data)
		}
		return index, data
	}

	paginated := sdkStream(runID, "paginated-stream")
	for _, chunk := range []string{"a", "b", "c"} {
		sdkWrite(t, world, runID, paginated, chunk)
	}
	sdkClose(t, world, runID, paginated)
	first := get(worlds.StreamsGetChunksInput{Name: paginated, Limit: 2})
	if index, data := indexes(first); index != "01" || data != "ab" || !first.HasMore || first.Cursor == "" {
		t.Fatalf("first page = %#v", first)
	}
	second := get(worlds.StreamsGetChunksInput{Name: paginated, Limit: 2, Cursor: first.Cursor})
	if index, data := indexes(second); index != "2" || data != "c" || second.HasMore || !second.Done {
		t.Fatalf("second page = %#v", second)
	}
	if after := get(worlds.StreamsGetChunksInput{Name: paginated, Cursor: second.Cursor}); len(after.Data) != 0 || !after.Done {
		t.Fatalf("page after the last = %#v", after)
	}

	exact := sdkStream(runID, "exact-page")
	sdkWrite(t, world, runID, exact, "a", "b")
	sdkClose(t, world, runID, exact)
	if page := get(worlds.StreamsGetChunksInput{Name: exact, Limit: 2}); len(page.Data) != 2 || page.HasMore || !page.Done {
		t.Fatalf("page ending at the end = %#v", page)
	}

	growing := sdkStream(runID, "growing")
	sdkWrite(t, world, runID, growing, "a", "b")
	head := get(worlds.StreamsGetChunksInput{Name: growing, Limit: 1})
	if !head.HasMore {
		t.Fatalf("first page of a growing stream = %#v", head)
	}
	sdkWrite(t, world, runID, growing, "c")
	sdkClose(t, world, runID, growing)
	if rest := get(worlds.StreamsGetChunksInput{Name: growing, Cursor: head.Cursor}); !rest.Done {
		t.Fatalf("rest of a growing stream = %#v", rest)
	} else if index, data := indexes(rest); index != "12" || data != "bc" {
		t.Fatalf("rest of a growing stream = %#v", rest)
	}

	progress := sdkStream(runID, "in-progress")
	sdkWrite(t, world, runID, progress, "data")
	if page := get(worlds.StreamsGetChunksInput{Name: progress}); len(page.Data) != 1 || page.HasMore || page.Done {
		t.Fatalf("page of an open stream = %#v", page)
	}

	if page := get(worlds.StreamsGetChunksInput{Name: sdkStream(runID, "nonexistent")}); len(page.Data) != 0 || page.HasMore || page.Done {
		t.Fatalf("page of a missing stream = %#v", page)
	}

	bad := sdkStream(runID, "bad-cursor")
	sdkWrite(t, world, runID, bad, "data")
	sdkClose(t, world, runID, bad)
	if page := get(worlds.StreamsGetChunksInput{Name: bad, Cursor: "not-valid-base64-json"}); len(page.Data) != 1 || page.Data[0].Index != 0 {
		t.Fatalf("page at an invalid cursor = %#v", page)
	}
}

// SDK: packages/world-local/src/streamer.test.ts > streamer > getInfo > returns tailIndex and done for a completed stream
// SDK: packages/world-local/src/streamer.test.ts > streamer > getInfo > returns tailIndex for an in-progress stream
// SDK: packages/world-local/src/streamer.test.ts > streamer > getInfo > returns -1 for a nonexistent stream
// SDK: packages/world-local/src/streamer.test.ts > streamer > getInfo > returns tailIndex 0 for a single-chunk stream
func conformSDKStreamsGetInfo(t *testing.T, world worlds.World) {
	runID := conformCreateRun(t, world, "sdk_streams_get_info")
	completed := sdkStream(runID, "info-completed")
	sdkWrite(t, world, runID, completed, "a")
	sdkWrite(t, world, runID, completed, "b")
	sdkClose(t, world, runID, completed)
	sdkInfo(t, world, runID, completed, 1, true)

	progress := sdkStream(runID, "info-progress")
	sdkWrite(t, world, runID, progress, "a")
	sdkWrite(t, world, runID, progress, "b")
	sdkInfo(t, world, runID, progress, 1, false)

	sdkInfo(t, world, runID, sdkStream(runID, "nonexistent"), -1, false)

	single := sdkStream(runID, "single-chunk")
	sdkWrite(t, world, runID, single, "only")
	sdkClose(t, world, runID, single)
	sdkInfo(t, world, runID, single, 0, true)
}

// conformSDKStreamsDropWritesAfterClose writes a stream's frames again after
// it closed, as a retried terminal write does. A closed stream takes no more
// data, so reads, from any index, end at the first close.
//
// SDK: packages/world-postgres/src/streamer.test.ts > streams.get() rows written after the first EOF > ignores rows written after the first EOF instead of erroring the stream
// SDK: packages/world-postgres/src/streamer.test.ts > streams.get() rows written after the first EOF > a start index at or past the data count (%i) closes at the first EOF instead of consuming it
// SDK: packages/world-postgres/src/streamer.test.ts > streams.get() rows written after the first EOF > a negative start index (%i) counts data chunks up to the first EOF only
func conformSDKStreamsDropWritesAfterClose(t *testing.T, world worlds.World) {
	ctx := context.Background()
	runID := conformCreateRun(t, world, "sdk_streams_after_close")
	name := sdkStream(runID, "stream-1")
	for _, text := range []string{"a", "b", "c", "d", "e"} {
		sdkWrite(t, world, runID, name, text+"\n")
	}
	sdkClose(t, world, runID, name)
	conformExpectDropped(t, world.StreamsWrite(ctx, worlds.StreamsWriteInput{RunID: runID, Name: name, Chunk: []byte("e\n")}), "write after the close")
	sdkClose(t, world, runID, name)

	sdkExpectTexts(t, "from 0", sdkReadText(t, world, runID, name, 0), "a\n", "b\n", "c\n", "d\n", "e\n")
	for _, start := range []int{5, 6} {
		sdkExpectTexts(t, fmt.Sprintf("from %d", start), sdkReadText(t, world, runID, name, start))
	}
	sdkExpectTexts(t, "from -1", sdkReadText(t, world, runID, name, -1), "e\n")
	sdkExpectTexts(t, "from -2", sdkReadText(t, world, runID, name, -2), "d\n", "e\n")
	sdkInfo(t, world, runID, name, 4, true)
}

// SDK: packages/world-local/src/streamer.test.ts > streamer > write and read > reads the same chunks from every instance on the data directory
func conformSDKSharedStreamsReadFromEveryInstance(t *testing.T, a, b worlds.World) {
	ctx := context.Background()
	runID := conformCreateRun(t, a, "sdk_shared_streams_every_instance")
	name := sdkStream(runID, "cross-instance")
	// A live reader on the second instance, opened before any write.
	live := sdkCollect(b, runID, name, 0)
	sdkWrite(t, a, runID, name, "a")
	sdkWrite(t, a, runID, name, "b", "c")
	sdkClose(t, a, runID, name)
	sdkExpectTexts(t, "live reader", live.wait(t), "a", "b", "c")

	sdkExpectTexts(t, "read", sdkReadText(t, b, runID, name, 0), "a", "b", "c")
	page, err := b.StreamsGetChunks(ctx, worlds.StreamsGetChunksInput{RunID: runID, Name: name})
	if err != nil || len(page.Data) != 3 || string(page.Data[2].Data) != "c" || !page.Done {
		t.Fatalf("page = %#v err=%v", page, err)
	}
	sdkInfo(t, b, runID, name, 2, true)
	sdkList(t, b, runID, name)
}

// SDK: packages/world-local/src/streamer.test.ts > streamer > close > closing twice is harmless
func conformSDKSharedStreamsCloseTwice(t *testing.T, a, b worlds.World) {
	ctx := context.Background()
	runID := conformCreateRun(t, a, "sdk_shared_streams_close_twice")
	name := sdkStream(runID, "double-close")
	sdkWrite(t, a, runID, name, "data")
	sdkClose(t, a, runID, name)
	sdkClose(t, a, runID, name)
	sdkClose(t, b, runID, name)
	sdkInfo(t, a, runID, name, 0, true)
	page, err := a.StreamsGetChunks(ctx, worlds.StreamsGetChunksInput{RunID: runID, Name: name})
	if err != nil || len(page.Data) != 1 || string(page.Data[0].Data) != "data" || !page.Done {
		t.Fatalf("page = %#v err=%v", page, err)
	}
	sdkExpectTexts(t, "read", sdkReadText(t, a, runID, name, 0), "data")
}

// SDK: packages/world-local/src/streamer.test.ts > streamer > writeMulti > keeps each group contiguous when instances write at once
func conformSDKSharedStreamsGroupsStayWhole(t *testing.T, a, b worlds.World) {
	ctx := context.Background()
	runID := conformCreateRun(t, a, "sdk_shared_streams_groups")
	name := sdkStream(runID, "write-multi-groups")
	reader := sdkCollect(a, runID, name, 0)
	group := func(prefix string) [][]byte {
		return [][]byte{[]byte(prefix + "0"), []byte(prefix + "1"), []byte(prefix + "2")}
	}
	var writers sync.WaitGroup
	errs := make(chan error, 3)
	// The first instance writes its groups one after the other, and the
	// second at the same time.
	writers.Go(func() {
		for _, prefix := range []string{"a", "c"} {
			if err := a.StreamsWriteMulti(ctx, worlds.StreamsWriteMultiInput{RunID: runID, Name: name, Chunks: group(prefix)}); err != nil {
				errs <- err
			}
		}
	})
	writers.Go(func() {
		if err := b.StreamsWriteMulti(ctx, worlds.StreamsWriteMultiInput{RunID: runID, Name: name, Chunks: group("b")}); err != nil {
			errs <- err
		}
	})
	writers.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	sdkClose(t, a, runID, name)
	received := reader.wait(t)
	if len(received) != 9 {
		t.Fatalf("received %q, want 9 chunks", received)
	}
	var prefixes []string
	for i := 0; i < 9; i += 3 {
		prefix := received[i][:1]
		if !slices.Equal(received[i:i+3], []string{prefix + "0", prefix + "1", prefix + "2"}) {
			t.Fatalf("received %q: groups interleave", received)
		}
		prefixes = append(prefixes, prefix)
	}
	if slices.Index(prefixes, "a") > slices.Index(prefixes, "c") {
		t.Fatalf("groups %q: one instance's groups out of order", prefixes)
	}
	slices.Sort(prefixes)
	if !slices.Equal(prefixes, []string{"a", "b", "c"}) {
		t.Fatalf("groups %q", prefixes)
	}
}

// SDK: packages/world-local/src/streamer.test.ts > streamer > chunk order > delivers writes from two instances in commit order
func conformSDKSharedStreamsCommitOrder(t *testing.T, a, b worlds.World) {
	runID := conformCreateRun(t, a, "sdk_shared_streams_commit_order")
	name := sdkStream(runID, "two-writers")
	live := sdkCollect(a, runID, name, 0)
	sdkWrite(t, a, runID, name, "0\n")
	sdkWrite(t, b, runID, name, "1\n")
	time.Sleep(50 * time.Millisecond)
	sdkWrite(t, b, runID, name, "2\n")
	sdkWrite(t, a, runID, name, "3\n")
	sdkClose(t, b, runID, name)
	sdkExpectTexts(t, "live reader", live.wait(t), "0\n", "1\n", "2\n", "3\n")
}

// SDK: packages/world-local/src/streamer.test.ts > streamer > list > concurrent first writes > keeps every name when two streamers share the data directory ($label)
func conformSDKSharedStreamsListNames(t *testing.T, a, b worlds.World) {
	ctx := context.Background()
	for i := range 10 {
		runID := conformCreateRun(t, a, fmt.Sprintf("sdk_shared_streams_names_%d", i))
		var names []string
		var writers sync.WaitGroup
		errs := make(chan error, 6)
		for _, writer := range []struct {
			world  worlds.World
			prefix string
		}{{a, "first"}, {b, "second"}} {
			for j := range 3 {
				name := sdkStream(runID, fmt.Sprintf("%s-%d", writer.prefix, j))
				names = append(names, name)
				writers.Go(func() {
					errs <- writer.world.StreamsWrite(ctx, worlds.StreamsWriteInput{RunID: runID, Name: name, Chunk: []byte(writer.prefix)})
				})
			}
		}
		writers.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatal(err)
			}
		}
		sdkList(t, a, runID, names...)
	}
}

// SDK: packages/world-local/src/streamer.test.ts > streamer > across OS processes on one data directory > reads a stream written by another process live and in order
func conformSDKSharedStreamsAnotherInstanceWrites(t *testing.T, a, b worlds.World) {
	runID := conformCreateRun(t, a, "sdk_shared_streams_other_writer")
	name := sdkStream(runID, "other-process")
	live := sdkCollect(a, runID, name, 0)
	for _, chunk := range []string{"0\n", "1\n", "2\n"} {
		sdkWrite(t, b, runID, name, chunk)
	}
	// Delivered while the stream is still open: the reader is live.
	live.waitFor(t, 3)
	sdkWrite(t, b, runID, name, "3\n", "4\n", "5\n")
	live.waitFor(t, 6)
	sdkWrite(t, b, runID, name, "6\n")
	sdkClose(t, b, runID, name)
	var want []string
	for i := range 7 {
		want = append(want, fmt.Sprintf("%d\n", i))
	}
	sdkExpectTexts(t, "live reader", live.wait(t), want...)
	sdkInfo(t, a, runID, name, 6, true)
	sdkList(t, a, runID, name)
}
