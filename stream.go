package workflow

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

type WriterOption struct{ Name worlds.StreamID }

func writerOption(values []WriterOption) WriterOption {
	if len(values) == 0 {
		return WriterOption{}
	}
	return values[len(values)-1]
}

func Writer(ctx context.Context, options ...WriterOption) (io.WriteCloser, func() error, error) {
	execution, ok := stepExecutionFromContext(ctx)
	if !ok {
		return nil, nil, ErrNoWorkflow
	}
	stepID, _ := execution.TargetStepID()
	opts := writerOption(options)
	return newStreamWriter(ctx, NewClient(execution.World()), execution.Run().RunID, opts.Name, string(stepID))
}

type ReaderOption struct{ Name worlds.StreamID }

func readerOption(values []ReaderOption) ReaderOption {
	if len(values) == 0 {
		return ReaderOption{}
	}
	return values[len(values)-1]
}

func Reader(ctx context.Context, options ...ReaderOption) (io.Reader, error) {
	execution, ok := stepExecutionFromContext(ctx)
	if !ok {
		return nil, ErrNoWorkflow
	}
	opts := readerOption(options)
	return newStreamReader(ctx, NewClient(execution.World()), execution.Run().RunID, opts.Name, 0), nil
}

func ReaderAt(ctx context.Context, offset int64, options ...ReaderOption) (io.Reader, error) {
	execution, ok := stepExecutionFromContext(ctx)
	if !ok {
		return nil, ErrNoWorkflow
	}
	opts := readerOption(options)
	return newStreamReader(ctx, NewClient(execution.World()), execution.Run().RunID, opts.Name, offset), nil
}

type streamWriter struct {
	ctx      context.Context
	client   Client
	runID    worlds.RunID
	name     worlds.StreamID
	session  worlds.StreamWriteSession
	closed   bool
	sequence int
}

func newStreamWriter(ctx context.Context, client Client, runID worlds.RunID, name worlds.StreamID, writerID string) (io.WriteCloser, func() error, error) {
	writer := &streamWriter{ctx: ctx, client: client, runID: runID, name: name}
	if sessionClient, ok := client.(interface {
		streamsCreateWriteSession(context.Context, worlds.StreamsCreateWriteSessionInput) (worlds.StreamWriteSession, error)
	}); ok {
		session, err := sessionClient.streamsCreateWriteSession(ctx, worlds.StreamsCreateWriteSessionInput{RunID: runID, Name: name, WriterID: writerID})
		if err != nil {
			return nil, nil, err
		}
		writer.session = session
	}
	return writer, writer.release, nil
}

func (w *streamWriter) Write(data []byte) (int, error) {
	if w.closed {
		return 0, errors.New("workflow: stream writer is closed")
	}
	if w.session != nil {
		if err := w.session.Write(w.ctx, worlds.StreamWriteSessionWriteInput{Sequence: w.sequence, Chunks: [][]byte{data}}); err != nil {
			return 0, err
		}
	} else if err := w.client.StreamsWrite(w.ctx, worlds.StreamsWriteInput{RunID: w.runID, Name: w.name, Chunk: data}); err != nil {
		return 0, err
	}
	w.sequence++
	return len(data), nil
}

func (w *streamWriter) Close() error {
	if w.closed {
		return nil
	}
	if err := w.client.StreamsClose(w.ctx, worlds.StreamsCloseInput{RunID: w.runID, Name: w.name}); err != nil {
		return err
	}
	if w.session != nil {
		if err := w.session.Close(w.ctx); err != nil {
			return err
		}
	}
	w.closed = true
	return nil
}

func (w *streamWriter) release() error {
	if w.closed {
		return nil
	}
	if w.session != nil {
		if err := w.session.Close(w.ctx); err != nil {
			return err
		}
	}
	w.closed = true
	return nil
}

type streamReader struct {
	ctx    context.Context
	client Client
	runID  worlds.RunID
	name   worlds.StreamID
	cursor string
	buf    []byte
	closed bool
	skip   int64
}

func newStreamReader(ctx context.Context, client Client, runID worlds.RunID, name worlds.StreamID, offset int64) *streamReader {
	reader := &streamReader{ctx: ctx, client: client, runID: runID, name: name}
	if offset >= 0 {
		reader.skip = offset
		return reader
	}
	reader.skip = max(reader.currentLength()+offset, 0)
	return reader
}

func (r *streamReader) Read(data []byte) (int, error) {
	for len(r.buf) == 0 {
		if r.closed {
			return 0, io.EOF
		}
		chunks, err := r.client.StreamsGetChunks(r.ctx, worlds.StreamsGetChunksInput{RunID: r.runID, Name: r.name, Cursor: r.cursor, Wait: 100 * time.Millisecond})
		if err != nil {
			return 0, err
		}
		r.cursor, r.closed = chunks.Cursor, chunks.Closed
		for _, chunk := range chunks.Chunks {
			r.buf = append(r.buf, chunk.Data...)
		}
		if r.skip != 0 {
			discard := min(r.skip, int64(len(r.buf)))
			r.buf = r.buf[discard:]
			r.skip -= discard
		}
	}
	n := copy(data, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}

func (r *streamReader) currentLength() int64 {
	cursor := ""
	var length int64
	for {
		result, err := r.client.StreamsGetChunks(r.ctx, worlds.StreamsGetChunksInput{RunID: r.runID, Name: r.name, Cursor: cursor})
		if err != nil {
			return 0
		}
		for _, chunk := range result.Chunks {
			length += int64(len(chunk.Data))
		}
		if !result.HasMore {
			return length
		}
		cursor = result.Cursor
	}
}
