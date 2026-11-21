package workflow

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
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

// StreamWriter writes a run's stream. Writers of a stream do not wait for
// each other: each writer's chunks stay in order, and different writers'
// chunks interleave.
//
// Each Write is one chunk. Write returns once the chunk is buffered, and the
// writer stores buffered chunks together in the background, so a failed
// store is reported by a later Write, Flush, Close, or CloseStream.
type StreamWriter interface {
	// Write buffers p as one chunk. It blocks while the buffer is full.
	io.Writer
	// Close stores what is buffered and gives up the writer, leaving the
	// stream open for other writers.
	io.Closer
	// Flush waits until every buffered chunk is stored.
	Flush() error
	// CloseStream stores what is buffered, then closes the stream for every
	// writer and gives up this one.
	CloseStream() error
}

// Writer returns a writer for a stream of the run ctx belongs to. It must be
// called in step context. It returns ErrNotInStep in workflow context and
// ErrNoWorkflow outside a run.
//
// When the step function returns, the writer is closed if it is still open,
// before the step's result is recorded, so a completed step's chunks are
// stored. A failed store fails the step.
func Writer(ctx context.Context, options ...WriterOption) (StreamWriter, error) {
	execution, ok := stepExecutionFromContext(ctx)
	if !ok {
		if _, ok := workflowExecutionFromContext(ctx); ok {
			return nil, ErrNotInStep
		}
		return nil, ErrNoWorkflow
	}
	opts := writerOption(options)
	writer, err := newStreamWriter(ctx, NewClient(execution.World()), execution.Run().RunID, opts.Name, execution.StreamFrame)
	if err != nil {
		return nil, err
	}
	worlds.OnStepReturn(ctx, writer.Close)
	return writer, nil
}

const (
	// streamBufferBytes is how many bytes a writer buffers before Write
	// blocks.
	streamBufferBytes = 1 << 20
	// streamChunksPerStore is the most chunks a writer stores at once.
	streamChunksPerStore = 1000
	// streamStoreTimeout bounds one store of buffered chunks.
	streamStoreTimeout = time.Minute
)

var errStreamWriterClosed = errors.New("workflow: stream writer is closed")

type streamWriter struct {
	ctx    context.Context
	writer worlds.StreamWriter
	// frame encodes a chunk as a stream frame.
	frame func([]byte) ([]byte, error)
	mu    sync.Mutex
	cond  *sync.Cond
	// pending holds the buffered chunks, pendingBytes their size, and
	// storing reports a store in flight.
	pending      [][]byte
	pendingBytes int
	storing      bool
	err          error
	closed       bool
}

func newStreamWriter(ctx context.Context, client Client, runID worlds.RunID, name string, frame func([]byte) ([]byte, error)) (*streamWriter, error) {
	writer, err := client.StreamsOpenWriter(ctx, worlds.StreamsOpenWriterInput{RunID: runID, Name: worlds.UserStreamName(runID, name), WriterID: worlds.NewWriterID()})
	if err != nil {
		return nil, err
	}
	// Stores outlive the step's context, so a step that returns, or is
	// cancelled, still stores what it wrote.
	w := &streamWriter{ctx: context.WithoutCancel(ctx), writer: writer, frame: frame}
	w.cond = sync.NewCond(&w.mu)
	return w, nil
}

func (w *streamWriter) Write(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, errStreamWriterClosed
	}
	if w.err != nil {
		return 0, w.err
	}
	frame, err := w.frame(data)
	if err != nil {
		return 0, err
	}
	w.pending = append(w.pending, frame)
	w.pendingBytes += len(frame)
	if !w.storing {
		w.storing = true
		go w.store()
	}
	for w.pendingBytes > streamBufferBytes && w.err == nil {
		w.cond.Wait()
	}
	if w.err != nil {
		return 0, w.err
	}
	return len(data), nil
}

// store stores buffered chunks until none remain or a store fails.
func (w *streamWriter) store() {
	w.mu.Lock()
	defer w.mu.Unlock()
	for len(w.pending) > 0 && w.err == nil {
		count := min(len(w.pending), streamChunksPerStore)
		chunks := w.pending[:count]
		w.pending = w.pending[count:]
		w.mu.Unlock()
		ctx, cancel := context.WithTimeout(w.ctx, streamStoreTimeout)
		err := w.writer.Write(ctx, chunks)
		cancel()
		w.mu.Lock()
		for _, chunk := range chunks {
			w.pendingBytes -= len(chunk)
		}
		if err != nil {
			w.err = err
			w.pending, w.pendingBytes = nil, 0
		}
		w.cond.Broadcast()
	}
	w.storing = false
	w.cond.Broadcast()
}

// flushLocked waits until nothing is buffered or in flight.
func (w *streamWriter) flushLocked() error {
	for (w.storing || len(w.pending) > 0) && w.err == nil {
		w.cond.Wait()
	}
	return w.err
}

func (w *streamWriter) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.flushLocked()
}

func (w *streamWriter) Close() error {
	return w.finish(func(ctx context.Context) error { return w.writer.Release(ctx) })
}

func (w *streamWriter) CloseStream() error {
	return w.finish(func(ctx context.Context) error { return w.writer.CloseStream(ctx) })
}

// finish stores what is buffered, then gives up the writer with end. Later
// calls return nil.
func (w *streamWriter) finish(end func(context.Context) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	err := w.flushLocked()
	w.closed = true
	ctx, cancel := context.WithTimeout(w.ctx, streamStoreTimeout)
	defer cancel()
	if err != nil {
		_ = w.writer.Release(ctx)
		return err
	}
	return end(ctx)
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
	ctx    context.Context
	client Client
	// keys opens the run's stream frames, once resolved.
	keys       *worlds.RunKeys
	resolved   bool
	runID      worlds.RunID
	name       string
	startIndex int
	cursor     string
	chunks     []StreamChunk
	closed     bool
}

func newStreamReader(ctx context.Context, client Client, runID worlds.RunID, options ReaderOption) *streamReader {
	return &streamReader{ctx: ctx, client: client, runID: runID, name: worlds.UserStreamName(runID, options.Name), startIndex: options.StartIndex}
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
			if len(chunk.Data) == 0 {
				continue
			}
			if err := r.resolveKeys(); err != nil {
				return err
			}
			values, err := worlds.DecodeStreamFrames(chunk.Data, r.keys)
			if err != nil {
				return err
			}
			chunk.Data = bytes.Join(values, nil)
			if len(chunk.Data) > 0 {
				r.chunks = append(r.chunks, chunk)
			}
		}
	}
	return nil
}

// resolveKeys resolves the keys of the run's stream frames.
func (r *streamReader) resolveKeys() error {
	if r.resolved {
		return nil
	}
	run, err := r.client.RunsGet(r.ctx, worlds.RunsGetInput{RunID: r.runID, ResolveData: worlds.ResolveDataNone})
	if err != nil {
		return err
	}
	if r.keys, err = r.client.RunKeys(r.ctx, worlds.RunKeysInput{RunID: r.runID, DeploymentID: run.DeploymentID}); err != nil {
		return err
	}
	r.resolved = true
	return nil
}

// outsideStreamFrame returns how a writer outside the run writes its stream
// frames: sealed to the run's public key, or, for a run that published none,
// encrypted with its key, or plain when it has none.
func outsideStreamFrame(ctx context.Context, client Client, runID worlds.RunID) (func([]byte) ([]byte, error), error) {
	run, err := client.RunsGet(ctx, worlds.RunsGetInput{RunID: runID, ResolveData: worlds.ResolveDataNone})
	if err != nil {
		return nil, err
	}
	if publicKey := worlds.DecodePublicKey(run.EncryptionPublicKey); publicKey != nil {
		sealer, err := worlds.NewPayloadSealer(publicKey)
		if err != nil {
			return nil, err
		}
		return func(chunk []byte) ([]byte, error) { return worlds.EncodeStreamFrame(chunk, nil, sealer) }, nil
	}
	keys, err := client.RunKeys(ctx, worlds.RunKeysInput{RunID: runID, DeploymentID: run.DeploymentID})
	if err != nil {
		return nil, err
	}
	return func(chunk []byte) ([]byte, error) { return worlds.EncodeStreamFrame(chunk, keys, nil) }, nil
}
