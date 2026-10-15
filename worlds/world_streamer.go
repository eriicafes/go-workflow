package worlds

import (
	"context"
	"time"
)

// Streamer stores streams, which are named, ordered sequences of chunks that a
// run writes and readers consume. A stream is identified by its run ID and
// name.
// Writes go either through one exclusive writer session or directly when no
// session owns the stream.
type Streamer interface {
	// StreamsCreateWriteSession creates an exclusive writer session for the
	// stream named Name in run RunID. It returns ErrStreamLocked when another
	// session owns the stream.
	StreamsCreateWriteSession(context.Context, StreamsCreateWriteSessionInput) (StreamWriteSession, error)
	// StreamsWriteSession appends Chunks for SessionID at Sequence. It returns
	// ErrStreamLocked when the session is not current or Sequence is not next.
	StreamsWriteSession(context.Context, StreamsWriteSessionInput) error
	// StreamsCloseWriteSession releases SessionID. It closes the stream when
	// Close is true. It returns ErrStreamLocked when SessionID is not current.
	StreamsCloseWriteSession(context.Context, StreamsCloseWriteSessionInput) error
	// StreamsWrite appends Chunk when no writer session owns the stream. It
	// returns ErrStreamLocked when a session owns the stream.
	StreamsWrite(context.Context, StreamsWriteInput) error
	// StreamsWriteMulti appends Chunks when no writer session owns the stream. It
	// returns ErrStreamLocked when a session owns the stream.
	StreamsWriteMulti(context.Context, StreamsWriteMultiInput) error
	// StreamsClose marks a stream done when no writer session owns it. It returns
	// ErrStreamLocked when a session owns the stream.
	StreamsClose(context.Context, StreamsCloseInput) error
	// StreamsGetChunks returns chunks after Cursor. When no chunk is available
	// and the stream is not done, it blocks for up to Wait or until ctx is
	// done.
	StreamsGetChunks(context.Context, StreamsGetChunksInput) (StreamChunksResult, error)
	// StreamsGetInfo returns the index of a stream's last chunk and whether the
	// stream is done.
	StreamsGetInfo(context.Context, StreamsGetInfoInput) (StreamInfo, error)
	// StreamsList returns the names of streams created for a run.
	StreamsList(context.Context, StreamsListInput) ([]string, error)
}

// StreamWriteSession is an exclusive writer session for one stream, created
// by StreamsCreateWriteSession. Only one session owns a stream at a time.
type StreamWriteSession interface {
	// ID returns the session ID.
	ID() StreamID
	// Write appends Chunks at Sequence. It returns ErrStreamLocked when the
	// session is no longer current or Sequence is not the next one.
	Write(context.Context, StreamWriteSessionWriteInput) error
	// Close marks the stream done and releases the session.
	Close(context.Context) error
	// Release releases the session without marking the stream done, so
	// another writer can continue it.
	Release(context.Context) error
}

type (
	// StreamsCreateWriteSessionInput holds the arguments to
	// StreamsCreateWriteSession.
	StreamsCreateWriteSessionInput struct {
		RunID RunID
		Name  string
		// WriterID identifies the writer requesting the session. A World may
		// ignore it.
		WriterID string
	}
	// StreamsWriteSessionInput holds the arguments to StreamsWriteSession.
	StreamsWriteSessionInput struct {
		RunID     RunID
		Name      string
		SessionID StreamID
		// Sequence is the number of writes this session has made before this
		// one, starting at zero.
		Sequence int
		Chunks   [][]byte
	}
	// StreamsCloseWriteSessionInput holds the arguments to
	// StreamsCloseWriteSession.
	StreamsCloseWriteSessionInput struct {
		RunID     RunID
		Name      string
		SessionID StreamID
		// Close marks the stream done as the session is released.
		Close bool
	}
	// StreamWriteSessionWriteInput holds the arguments to
	// StreamWriteSession.Write.
	StreamWriteSessionWriteInput struct {
		// Sequence is the number of writes this session has made before this
		// one, starting at zero.
		Sequence int
		Chunks   [][]byte
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
		// Cursor is the Cursor of the previous result. Empty starts at the
		// first chunk.
		Cursor string
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
