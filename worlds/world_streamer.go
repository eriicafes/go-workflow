package worlds

import (
	"context"
	"time"
)

type Streamer interface {
	// StreamsCreateWriteSession creates an exclusive writer session for RunID and
	// Name. It returns ErrStreamLocked when another session owns the stream.
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
	// StreamsGetChunks returns chunks after Cursor. It waits for up to Wait when
	// no chunk is available and the stream is not done.
	StreamsGetChunks(context.Context, StreamsGetChunksInput) (StreamChunksResult, error)
	// StreamsGetInfo returns a stream's current tail index and completion state.
	StreamsGetInfo(context.Context, StreamsGetInfoInput) (StreamInfo, error)
	// StreamsList returns the names of streams created for a run.
	StreamsList(context.Context, StreamsListInput) ([]string, error)
}

type StreamWriteSession interface {
	// ID returns this writer session's lease ID.
	ID() StreamID
	// Write appends Chunks at Sequence for this session.
	Write(context.Context, StreamWriteSessionWriteInput) error
	// Close closes the logical stream and releases this session's lease.
	Close(context.Context) error
	// Release releases this session's lease without closing the stream.
	Release(context.Context) error
}

type (
	StreamsCreateWriteSessionInput struct {
		RunID    RunID
		Name     string
		WriterID string
	}
	StreamsWriteSessionInput struct {
		RunID     RunID
		Name      string
		SessionID StreamID
		Sequence  int
		Chunks    [][]byte
	}
	StreamsCloseWriteSessionInput struct {
		RunID     RunID
		Name      string
		SessionID StreamID
		Close     bool
	}
	StreamWriteSessionWriteInput struct {
		Sequence int
		Chunks   [][]byte
	}
	StreamsWriteInput struct {
		RunID RunID
		Name  string
		Chunk []byte
	}
	StreamsWriteMultiInput struct {
		RunID  RunID
		Name   string
		Chunks [][]byte
	}
	StreamsCloseInput struct {
		RunID RunID
		Name  string
	}
	StreamsGetChunksInput struct {
		RunID  RunID
		Name   string
		Cursor string
		Limit  int
		Wait   time.Duration
	}
	StreamsGetInfoInput struct {
		RunID RunID
		Name  string
	}
	StreamsListInput struct {
		RunID RunID
	}
	StreamChunksResult struct {
		Data       []StreamChunk
		Cursor     string
		HasMore    bool
		Done       bool
		RetryAfter time.Duration
	}
)

type StreamChunk struct {
	Index int
	Data  []byte
}

type StreamInfo struct {
	TailIndex int
	Done      bool
}
