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
	StreamsList(context.Context, StreamsListInput) ([]StreamNamespace, error)
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
		RunID     RunID
		Namespace StreamNamespace
		WriterID  string
	}
	StreamsWriteSessionInput struct {
		RunID     RunID
		Namespace StreamNamespace
		SessionID StreamID
		Sequence  int
		Chunks    [][]byte
	}
	StreamsCloseWriteSessionInput struct {
		RunID     RunID
		Namespace StreamNamespace
		SessionID StreamID
		Close     bool
	}
	StreamWriteSessionWriteInput struct {
		Sequence int
		Chunks   [][]byte
	}
	StreamsWriteInput struct {
		RunID     RunID
		Namespace StreamNamespace
		Chunk     []byte
	}
	StreamsWriteMultiInput struct {
		RunID     RunID
		Namespace StreamNamespace
		Chunks    [][]byte
	}
	StreamsCloseInput struct {
		RunID     RunID
		Namespace StreamNamespace
	}
	StreamsGetChunksInput struct {
		RunID     RunID
		Namespace StreamNamespace
		Cursor    string
		Limit     int
		Wait      time.Duration
	}
	StreamsGetInfoInput struct {
		RunID     RunID
		Namespace StreamNamespace
	}
	StreamsListInput struct {
		RunID RunID
	}
	StreamChunksResult struct {
		Chunks     []StreamChunk
		Cursor     string
		HasMore    bool
		Closed     bool
		RetryAfter time.Duration
	}
)

type StreamChunk struct {
	Index int
	Data  []byte
}

type StreamInfo struct {
	TailIndex int
	Closed    bool
}
