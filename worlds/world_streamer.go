package worlds

import (
	"context"
	"time"
)

type Streamer interface {
	StreamsCreateWriteSession(context.Context, StreamsCreateWriteSessionInput) (StreamWriteSession, error)
	StreamsWriteSession(context.Context, StreamsWriteSessionInput) error
	StreamsCloseWriteSession(context.Context, StreamsCloseWriteSessionInput) error
	StreamsWrite(context.Context, StreamsWriteInput) error
	StreamsWriteMulti(context.Context, StreamsWriteMultiInput) error
	StreamsClose(context.Context, StreamsCloseInput) error
	StreamsGetChunks(context.Context, StreamsGetChunksInput) (StreamChunksResult, error)
	StreamsGetInfo(context.Context, StreamsGetInfoInput) (StreamInfo, error)
	StreamsList(context.Context, StreamsListInput) ([]string, error)
}

type StreamWriteSession interface {
	ID() StreamID
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
