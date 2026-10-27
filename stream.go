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
	opts := writerOption(options)
	return newStreamWriter(ctx, NewClient(execution.World()), execution.Run().RunID, opts.Name, StepID(ctx))
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
	backoff := 10 * time.Millisecond
	for {
		session, err := client.StreamsCreateWriteSession(ctx, worlds.StreamsCreateWriteSessionInput{RunID: runID, Name: name, WriterID: writerID})
		if !errors.Is(err, worlds.ErrStreamLocked) {
			if err != nil {
				return nil, nil, err
			}
			writer.session = session
			break
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, nil, ctx.Err()
		case <-timer.C:
		}
		backoff = min(2*backoff, 500*time.Millisecond)
	}
	return writer, writer.release, nil
}

// releaseContext returns the context a writer gives up its hold with. It
// outlives the writer's own context, so a cancelled step still frees the
// stream for the next writer.
func (w *streamWriter) releaseContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(w.ctx), 5*time.Second)
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
	ctx, cancel := w.releaseContext()
	defer cancel()
	if err := w.session.Close(ctx); err != nil {
		return err
	}
	w.closed = true
	return nil
}

func (w *streamWriter) release() error {
	if w.closed {
		return nil
	}
	ctx, cancel := w.releaseContext()
	defer cancel()
	if err := w.session.Release(ctx); err != nil {
		return err
	}
	w.closed = true
	return nil
}

// ReaderOption holds the options to Reader and Run.Reader.
type ReaderOption struct {
	// Name is the stream name. The zero value names the run's default stream.
	Name string
	// StartIndex is the index of the chunk to start reading at. Each write is
	// one chunk. A negative StartIndex counts back from the end of the
	// stream, so -3 reads the last three chunks.
	StartIndex int
}

func readerOption(values []ReaderOption) ReaderOption {
	if len(values) == 0 {
		return ReaderOption{}
	}
	return values[len(values)-1]
}

// StreamChunk is one chunk of a stream: the data of one write and its
// zero-based index in the stream.
type StreamChunk = worlds.StreamChunk

// Reader returns a reader for a stream of the run ctx belongs to, from
// ReaderOption.StartIndex. Reads block until data is written and return io.EOF
// once the stream is closed. It must be called in step context. It returns
// ErrNotInStep in workflow context and ErrNoWorkflow outside a run.
func Reader(ctx context.Context, options ...ReaderOption) (StreamReader, error) {
	execution, ok := stepExecutionFromContext(ctx)
	if !ok {
		if _, ok := workflowExecutionFromContext(ctx); ok {
			return nil, ErrNotInStep
		}
		return nil, ErrNoWorkflow
	}
	return newStreamReader(ctx, NewClient(execution.World()), execution.Run().RunID, readerOption(options)), nil
}

// StreamReader reads a run's stream. Read joins the stream's chunks into
// bytes, while Next returns one chunk at a time with its index, so a reader
// can relay whole writes and resume after the last index it saw.
type StreamReader interface {
	// Read reads the stream's bytes, blocking until data is written. It
	// returns io.EOF once the stream is closed and read.
	io.Reader
	// Next returns the next chunk, blocking until one is written. After a
	// partial Read, it returns the rest of the current chunk. It returns
	// io.EOF once the stream is closed and read.
	Next() (StreamChunk, error)
	// TailIndex returns the index of the stream's last chunk, or -1 when it
	// has none. A reconnecting client can resume from the index after the
	// last chunk it received.
	TailIndex() (int, error)
}

type streamReader struct {
	ctx        context.Context
	client     Client
	runID      worlds.RunID
	name       string
	startIndex int
	cursor     string
	chunks     []StreamChunk
	closed     bool
}

func newStreamReader(ctx context.Context, client Client, runID worlds.RunID, options ReaderOption) *streamReader {
	return &streamReader{ctx: ctx, client: client, runID: runID, name: options.Name, startIndex: options.StartIndex}
}

func (r *streamReader) Read(data []byte) (int, error) {
	if err := r.fill(); err != nil {
		return 0, err
	}
	chunk := &r.chunks[0]
	n := copy(data, chunk.Data)
	chunk.Data = chunk.Data[n:]
	if len(chunk.Data) == 0 {
		r.chunks = r.chunks[1:]
	}
	return n, nil
}

func (r *streamReader) Next() (StreamChunk, error) {
	if err := r.fill(); err != nil {
		return StreamChunk{}, err
	}
	chunk := r.chunks[0]
	r.chunks = r.chunks[1:]
	return chunk, nil
}

func (r *streamReader) TailIndex() (int, error) {
	info, err := r.client.StreamsGetInfo(r.ctx, worlds.StreamsGetInfoInput{RunID: r.runID, Name: r.name})
	if err != nil {
		return 0, err
	}
	return info.TailIndex, nil
}

// fill waits until a chunk with data is buffered, or returns io.EOF once the
// stream is closed and read.
func (r *streamReader) fill() error {
	for len(r.chunks) > 0 && len(r.chunks[0].Data) == 0 {
		r.chunks = r.chunks[1:]
	}
	for len(r.chunks) == 0 {
		if r.closed {
			return io.EOF
		}
		result, err := r.client.StreamsGetChunks(r.ctx, worlds.StreamsGetChunksInput{RunID: r.runID, Name: r.name, Cursor: r.cursor, StartIndex: r.startIndex, Wait: clientWait})
		if err != nil {
			return err
		}
		r.cursor, r.closed = result.Cursor, result.Done && !result.HasMore
		for _, chunk := range result.Data {
			if len(chunk.Data) > 0 {
				r.chunks = append(r.chunks, chunk)
			}
		}
	}
	return nil
}
