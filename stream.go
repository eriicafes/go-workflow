package workflow

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/eriicafes/go-workflow/workflowops"
)

// WriterOption selects an independent, namespaced byte stream for a run.
// Bytes are flushed to durable storage by each Write. Use bufio.NewWriter when
// an application needs to buffer several writes before flushing.
type WriterOption struct {
	Namespace string
}

func writerOption(values []WriterOption) WriterOption {
	if len(values) == 0 {
		return WriterOption{}
	}
	return values[len(values)-1]
}

// ReaderOption selects an independent, namespaced byte stream for a run.
type ReaderOption struct {
	Namespace string
}

func readerOption(values []ReaderOption) ReaderOption {
	if len(values) == 0 {
		return ReaderOption{}
	}
	return values[len(values)-1]
}

// Reader opens the current run's stream. It is available only in a durable step.
func Reader(ctx context.Context, options ...ReaderOption) (io.Reader, error) {
	execution, ok := stepExecutionFromContext(ctx)
	if !ok {
		return nil, ErrNoWorkflow
	}
	opts := readerOption(options)
	return newReader(ctx, execution.World(), workflowops.Stream{RunID: execution.RunID(), Namespace: opts.Namespace}, 0), nil
}

// ReaderAt is Reader starting at offset. Negative offsets are relative to the
// stream's current end, so -1 starts at its last byte.
func ReaderAt(ctx context.Context, offset int64, options ...ReaderOption) (io.Reader, error) {
	execution, ok := stepExecutionFromContext(ctx)
	if !ok {
		return nil, ErrNoWorkflow
	}
	opts := readerOption(options)
	return newReader(ctx, execution.World(), workflowops.Stream{RunID: execution.RunID(), Namespace: opts.Namespace}, offset), nil
}

func newReader(ctx context.Context, client Client, ref workflowops.Stream, offset int64) *stream {
	return &stream{ctx: ctx, client: client, ref: ref, offset: offset}
}

func (value *stream) Read(data []byte) (int, error) {
	for len(value.buf) == 0 {
		if value.closed {
			return 0, io.EOF
		}
		read, err := value.client.ReadStream(value.ctx, value.ref, value.offset)
		if err != nil {
			return 0, err
		}
		value.offset, value.closed, value.buf = read.Next, read.Closed, read.Data
		if len(value.buf) != 0 || value.closed {
			continue
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-value.ctx.Done():
			timer.Stop()
			return 0, value.ctx.Err()
		case <-timer.C:
		}
	}
	n := copy(data, value.buf)
	value.buf = value.buf[n:]
	return n, nil
}

// Writer obtains the current run's stream writer. It is available only in a
// durable step. It waits for an active writer to release the stream. release
// completes this writer's lifetime without closing the stream. Close closes
// the stream for all readers.
func Writer(ctx context.Context, options ...WriterOption) (io.WriteCloser, func() error, error) {
	execution, ok := stepExecutionFromContext(ctx)
	if !ok {
		return nil, nil, ErrNoWorkflow
	}
	opts := writerOption(options)
	streams, ok := ctx.Value(stepStreamsContextKey{}).(*stepStreams)
	if !ok {
		streams = &stepStreams{}
	}
	value, err := streams.acquire(ctx, execution.World(), workflowops.Stream{
		RunID:     execution.RunID(),
		Namespace: opts.Namespace,
	})
	if err != nil {
		return nil, nil, err
	}
	return value, value.release, nil
}

type stepStreams struct {
	mu      sync.Mutex
	writers []*stream
}

func (value *stepStreams) acquire(ctx context.Context, client Client, reference workflowops.Stream) (*stream, error) {
	lock, err := streamLockToken()
	if err != nil {
		return nil, err
	}
	for {
		acquired, err := client.LockStream(ctx, reference, lock)
		if err != nil {
			return nil, err
		}
		if acquired {
			break
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	writer := &stream{ctx: ctx, client: client, ref: reference, lock: lock}
	value.mu.Lock()
	value.writers = append(value.writers, writer)
	value.mu.Unlock()
	return writer, nil
}

func (value *stepStreams) release() error {
	value.mu.Lock()
	writers := append([]*stream(nil), value.writers...)
	value.writers = nil
	value.mu.Unlock()
	var result error
	for _, writer := range writers {
		if err := writer.release(); err != nil && result == nil {
			result = err
		}
	}
	return result
}

// stream is the private provider-backed implementation returned as standard
// io interfaces. Its lock token is private to this writer instance.
type stream struct {
	ctx    context.Context
	client Client
	ref    workflowops.Stream
	lock   string
	offset int64
	buf    []byte
	closed bool
}

func (value *stream) Write(data []byte) (int, error) {
	if value.closed {
		return 0, errors.New("workflow: stream writer is closed")
	}
	if err := value.client.AppendStream(value.ctx, value.ref, value.lock, data); err != nil {
		return 0, err
	}
	return len(data), nil
}

func (value *stream) Close() error {
	if value.closed {
		return nil
	}
	value.closed = true
	return value.client.ReleaseStream(value.ctx, value.ref, value.lock, true)
}

func (value *stream) release() error {
	if value.closed {
		return nil
	}
	value.closed = true
	return value.client.ReleaseStream(value.ctx, value.ref, value.lock, false)
}

func streamLockToken() (string, error) {
	data := make([]byte, 16)
	if _, err := rand.Read(data); err != nil {
		return "", err
	}
	return hex.EncodeToString(data), nil
}
