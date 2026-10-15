package workflow

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// WriterOption holds the options to Writer and Run.Writer.
type WriterOption struct {
	// Name is the stream name. The zero value names the run's default stream.
	Name string
}

func writerOption(values []WriterOption) WriterOption {
	if len(values) == 0 {
		return WriterOption{}
	}
	return values[len(values)-1]
}

// Writer returns a writer for a stream of the run ctx belongs to. It must be
// called in step context. It returns ErrNotInStep in workflow context and
// ErrNoWorkflow outside a run.
//
// The writer holds the stream name, so Writer waits while another writer holds
// it. Close closes the stream for all writers, while the returned release
// function only gives up this writer's hold so a retry or another step can
// continue the stream.
func Writer(ctx context.Context, options ...WriterOption) (io.WriteCloser, func() error, error) {
	execution, ok := stepExecutionFromContext(ctx)
	if !ok {
		if _, ok := workflowExecutionFromContext(ctx); ok {
			return nil, nil, ErrNotInStep
		}
		return nil, nil, ErrNoWorkflow
	}
	stepID, _ := execution.TargetStepID()
	opts := writerOption(options)
	return newStreamWriter(ctx, NewClient(execution.World()), execution.Run().RunID, opts.Name, string(stepID))
}

// ReaderOption holds the options to Reader, ReaderAt, Run.Reader, and
// Run.ReaderAt.
type ReaderOption struct {
	// Name is the stream name. The zero value names the run's default stream.
	Name string
}

func readerOption(values []ReaderOption) ReaderOption {
	if len(values) == 0 {
		return ReaderOption{}
	}
	return values[len(values)-1]
}

// Reader returns a reader for a stream of the run ctx belongs to, from its
// beginning. Reads block until data is written and return io.EOF once the
// stream is closed. It must be called in step context. It returns
// ErrNotInStep in workflow context and ErrNoWorkflow outside a run.
func Reader(ctx context.Context, options ...ReaderOption) (io.Reader, error) {
	execution, ok := stepExecutionFromContext(ctx)
	if !ok {
		if _, ok := workflowExecutionFromContext(ctx); ok {
			return nil, ErrNotInStep
		}
		return nil, ErrNoWorkflow
	}
	opts := readerOption(options)
	return newStreamReader(ctx, NewClient(execution.World()), execution.Run().RunID, opts.Name, 0), nil
}

// ReaderAt is like Reader but starts at byte offset. A negative offset is
// relative to the current end of the stream.
func ReaderAt(ctx context.Context, offset int64, options ...ReaderOption) (io.Reader, error) {
	execution, ok := stepExecutionFromContext(ctx)
	if !ok {
		if _, ok := workflowExecutionFromContext(ctx); ok {
			return nil, ErrNotInStep
		}
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
	for {
		session, err := client.StreamsCreateWriteSession(ctx, worlds.StreamsCreateWriteSessionInput{RunID: runID, Name: name, WriterID: writerID})
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
	return writer, writer.release, nil
}

func (w *streamWriter) Write(data []byte) (int, error) {
	if w.closed {
		return 0, errors.New("workflow: stream writer is closed")
	}
	if err := w.session.Write(w.ctx, worlds.StreamWriteSessionWriteInput{Sequence: w.sequence, Chunks: [][]byte{data}}); err != nil {
		return 0, err
	}
	w.sequence++
	return len(data), nil
}

func (w *streamWriter) Close() error {
	if w.closed {
		return nil
	}
	if err := w.session.Close(w.ctx); err != nil {
		return err
	}
	w.closed = true
	return nil
}

func (w *streamWriter) release() error {
	if w.closed {
		return nil
	}
	if err := w.session.Release(w.ctx); err != nil {
		return err
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
		chunks, err := r.client.StreamsGetChunks(r.ctx, worlds.StreamsGetChunksInput{RunID: r.runID, Name: r.name, Cursor: r.cursor, Wait: clientWait})
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
