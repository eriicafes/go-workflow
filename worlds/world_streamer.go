package worlds

import (
	"context"
	"time"
)

type Streamer interface {
	StreamsCreateWriteSession(context.Context, StreamsCreateWriteSessionInput) (StreamWriteSession, error)
	StreamsWrite(context.Context, StreamsWriteInput) error
	StreamsWriteMulti(context.Context, StreamsWriteMultiInput) error
	StreamsClose(context.Context, StreamsCloseInput) error
	StreamsGetChunks(context.Context, StreamsGetChunksInput) (StreamChunksResult, error)
	StreamsGetInfo(context.Context, StreamsGetInfoInput) (StreamInfo, error)
	StreamsList(context.Context, StreamsListInput) ([]StreamID, error)
}

type StreamWriteSession interface {
	Write(context.Context, StreamWriteSessionWriteInput) error
	Close(context.Context) error
}

type (
	StreamsCreateWriteSessionInput struct {
		RunID    RunID
		Name     StreamID
		WriterID string
	}
	StreamWriteSessionWriteInput struct {
		Sequence int
		Chunks   [][]byte
	}
	StreamsWriteInput struct {
		RunID RunID
		Name  StreamID
		Chunk []byte
	}
	StreamsWriteMultiInput struct {
		RunID  RunID
		Name   StreamID
		Chunks [][]byte
	}
	StreamsCloseInput struct {
		RunID RunID
		Name  StreamID
	}
	StreamsGetChunksInput struct {
		RunID  RunID
		Name   StreamID
		Cursor string
		Limit  int
		Wait   time.Duration
	}
	StreamsGetInfoInput struct {
		RunID RunID
		Name  StreamID
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
