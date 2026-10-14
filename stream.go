package workflow

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// WriterOption selects a logical stream name within a run. A writer holds an
// exclusive lease for its name until Close or the returned release call.
type WriterOption struct{ Name string }

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

type ReaderOption struct{ Name string }

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
	name     string
	session  worlds.StreamWriteSession
	closed   bool
	sequence int
}

func newStreamWriter(ctx context.Context, client Client, runID worlds.RunID, name string, writerID string) (io.WriteCloser, func() error, error) {
	writer := &streamWriter{ctx: ctx, client: client, runID: runID, name: name}
	if sessionClient, ok := client.(interface {
		streamsCreateWriteSession(context.Context, worlds.StreamsCreateWriteSessionInput) (worlds.StreamWriteSession, error)
	}); ok {
		for {
			session, err := sessionClient.streamsCreateWriteSession(ctx, worlds.StreamsCreateWriteSessionInput{RunID: runID, Name: name, WriterID: writerID})
			if !errors.Is(err, worlds.ErrStreamLocked) {
				if err != nil {
					return nil, nil, err
				}
				writer.session = session
				break
			}
			timer := time.NewTimer(10 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, nil, ctx.Err()
			case <-timer.C:
			}
		}
	} else {
		return nil, nil, errors.New("workflow: client does not support exclusive stream sessions")
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
	}
	w.sequence++
	return len(data), nil
}

func (w *streamWriter) Close() error {
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

func (w *streamWriter) release() error {
	if w.closed {
		return nil
	}
	if w.session != nil {
		if err := w.session.Release(w.ctx); err != nil {
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
	name   string
	cursor string
	buf    []byte
	closed bool
	skip   int64
}

func newStreamReader(ctx context.Context, client Client, runID worlds.RunID, name string, offset int64) *streamReader {
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
		r.cursor, r.closed = chunks.Cursor, chunks.Done
		for _, chunk := range chunks.Data {
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
		for _, chunk := range result.Data {
			length += int64(len(chunk.Data))
		}
		if !result.HasMore {
			return length
		}
		cursor = result.Cursor
	}
}
