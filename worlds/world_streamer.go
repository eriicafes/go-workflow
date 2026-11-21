package worlds

import (
	"context"
	"encoding/base64"
	"errors"
	"strings"
	"sync"
	"time"
)

// UserStreamName returns the stored name of the user stream name of run
// runID: strm_ and the run ID's ULID, then _user, and for a named stream _
// and the name in unpadded base64url. The empty name is the run's default
// stream.
func UserStreamName(runID RunID, name string) string {
	stored := strings.Replace(string(runID), "wrun_", "strm_", 1) + "_user"
	if name == "" {
		return stored
	}
	return stored + "_" + base64.RawURLEncoding.EncodeToString([]byte(name))
}

// Streamer stores streams, which are named, ordered sequences of chunks that
// a run writes and readers consume. A stream is identified by its run ID and
// name. Any number of writers may append to a stream at once: each write's
// chunks stay in order, and the writes of different writers interleave in the
// order the World stores them.
type Streamer interface {
	// StreamsWrite appends Chunk. A closed stream, and a stream of a
	// run whose data was deleted, takes no more data: the write is dropped, as
	// world-local and world-postgres drop it, or refused with
	// ErrStreamClosed, as Vercel's backend refuses it.
	StreamsWrite(context.Context, StreamsWriteInput) error
	// StreamsWriteMulti appends Chunks in order, as StreamsWrite does.
	StreamsWriteMulti(context.Context, StreamsWriteMultiInput) error
	// StreamsClose marks a stream done for every writer. Closing a closed
	// stream succeeds.
	StreamsClose(context.Context, StreamsCloseInput) error
	// StreamsGetChunks returns chunks after Cursor, or from StartIndex when
	// Cursor is empty (see StreamsStartIndex). When no chunk is available
	// and the stream is not done, it blocks for up to Wait or until ctx is
	// done.
	StreamsGetChunks(context.Context, StreamsGetChunksInput) (StreamChunksResult, error)
	// StreamsGetInfo returns the index of a stream's last chunk and whether the
	// stream is done.
	StreamsGetInfo(context.Context, StreamsGetInfoInput) (StreamInfo, error)
	// StreamsList returns the names of streams created for a run.
	StreamsList(context.Context, StreamsListInput) ([]string, error)
}

// StreamWriterOpener is implemented by a World with a faster transport for a
// writer that writes a stream many times, such as a WebSocket held for the
// writer's lifetime. OpenStreamWriter uses it when the World implements it.
// It gives the writer no exclusive hold on the stream.
type StreamWriterOpener interface {
	// StreamsOpenWriter returns a writer for the stream Name of run RunID.
	StreamsOpenWriter(context.Context, StreamsOpenWriterInput) (StreamWriter, error)
}

// StreamWriter appends to one stream on behalf of one writer, which other
// writers of the stream do not wait for. After Release or CloseStream it
// accepts no more writes.
type StreamWriter interface {
	// Write appends chunks in order. A closed stream drops them, or
	// refuses them with ErrStreamClosed, as StreamsWrite does.
	Write(ctx context.Context, chunks [][]byte) error
	// Release gives up the writer and its transport, leaving the stream open
	// for other writers.
	Release(ctx context.Context) error
	// CloseStream closes the stream for every writer and gives up the
	// writer.
	CloseStream(ctx context.Context) error
}

// OpenStreamWriter returns a writer for a stream: streamer's own when it
// implements StreamWriterOpener, and otherwise one that writes through
// StreamsWriteMulti and StreamsClose.
func OpenStreamWriter(ctx context.Context, streamer Streamer, input StreamsOpenWriterInput) (StreamWriter, error) {
	if writers, ok := streamer.(StreamWriterOpener); ok {
		return writers.StreamsOpenWriter(ctx, input)
	}
	return newDirectStreamWriter(streamer, input), nil
}

// streamAppender is the part of a Streamer a direct writer uses.
type streamAppender interface {
	StreamsWriteMulti(context.Context, StreamsWriteMultiInput) error
	StreamsClose(context.Context, StreamsCloseInput) error
}

func newDirectStreamWriter(streamer streamAppender, input StreamsOpenWriterInput) *directStreamWriter {
	return &directStreamWriter{streamer: streamer, runID: input.RunID, name: input.Name}
}

// directStreamWriter writes through a Streamer's stateless methods.
type directStreamWriter struct {
	streamer streamAppender
	runID    RunID
	name     string
	mu       sync.Mutex
	done     bool
}

func (w *directStreamWriter) Write(ctx context.Context, chunks [][]byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.done {
		return errStreamWriterDone
	}
	if len(chunks) == 0 {
		return nil
	}
	return w.streamer.StreamsWriteMulti(ctx, StreamsWriteMultiInput{RunID: w.runID, Name: w.name, Chunks: chunks})
}

func (w *directStreamWriter) Release(context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.done = true
	return nil
}

func (w *directStreamWriter) CloseStream(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.streamer.StreamsClose(ctx, StreamsCloseInput{RunID: w.runID, Name: w.name}); err != nil {
		return err
	}
	w.done = true
	return nil
}

// errStreamWriterDone is returned by a StreamWriter used after Release or
// CloseStream.
var errStreamWriterDone = errors.New("workflow: stream writer is released")

type (
	// StreamsOpenWriterInput holds the arguments to StreamsOpenWriter.
	StreamsOpenWriterInput struct {
		RunID RunID
		Name  string
		// WriterID identifies the writer, for observability only. NewWriterID
		// makes one.
		WriterID string
	}
	// StreamsWriteInput holds the arguments to StreamsWrite.
	StreamsWriteInput struct {
		RunID RunID
		Name  string
		Chunk []byte
	}
	// StreamsWriteMultiInput holds the arguments to StreamsWriteMulti.
	StreamsWriteMultiInput struct {
		RunID  RunID
		Name   string
		Chunks [][]byte
	}
	// StreamsCloseInput holds the arguments to StreamsClose.
	StreamsCloseInput struct {
		RunID RunID
		Name  string
	}
	// StreamsGetChunksInput holds the arguments to StreamsGetChunks.
	StreamsGetChunksInput struct {
		RunID RunID
		Name  string
		// Cursor is the Cursor of the previous result. Empty starts at
		// StartIndex.
		Cursor string
		// StartIndex is the index of the chunk to start at when Cursor is
		// empty. A negative StartIndex counts back from the end of the stream,
		// so -3 starts at the third chunk from the end, and stops at the first
		// chunk. An index past the end waits for that chunk.
		StartIndex int
		// Limit is the maximum number of chunks to return. Zero or less uses
		// the World's default.
		Limit int
		// Wait is the longest the call blocks for a chunk. Zero returns
		// without waiting.
		Wait time.Duration
	}
	// StreamsGetInfoInput holds the arguments to StreamsGetInfo.
	StreamsGetInfoInput struct {
		RunID RunID
		Name  string
	}
	// StreamsListInput holds the arguments to StreamsList.
	StreamsListInput struct {
		RunID RunID
	}
	// StreamChunksResult is the result of StreamsGetChunks.
	StreamChunksResult struct {
		Data []StreamChunk
		// Cursor continues reading after Data when passed back in
		// StreamsGetChunksInput.
		Cursor string
		// HasMore reports whether chunks are already available after Data.
		HasMore bool
		// Done reports whether the stream is closed. Once Done is true and
		// HasMore is false, no more chunks will arrive.
		Done bool
		// RetryAfter is how long the caller should wait before polling again
		// when no chunk was returned and the stream is not done.
		RetryAfter time.Duration
	}
)

// StreamChunk is one chunk of a stream.
type StreamChunk struct {
	// Index is the chunk's zero-based position in the stream.
	Index int
	Data  []byte
}

// StreamInfo is the result of StreamsGetInfo.
type StreamInfo struct {
	// TailIndex is the index of the last chunk, or -1 when the stream has
	// no chunks.
	TailIndex int
	Done      bool
}

// StreamsStartIndex resolves input.StartIndex to the index of a chunk. A
// negative StartIndex counts back from the end of the stream, which streamer
// reports through StreamsGetInfo. It does not read the stream when
// input.Cursor is set, since the cursor takes precedence.
func StreamsStartIndex(ctx context.Context, streamer Streamer, input StreamsGetChunksInput) (int, error) {
	if input.StartIndex >= 0 || input.Cursor != "" {
		return input.StartIndex, nil
	}
	info, err := streamer.StreamsGetInfo(ctx, StreamsGetInfoInput{RunID: input.RunID, Name: input.Name})
	if err != nil {
		return 0, err
	}
	return max(info.TailIndex+1+input.StartIndex, 0), nil
}
