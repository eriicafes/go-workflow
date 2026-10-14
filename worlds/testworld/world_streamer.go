package testworld

import (
	"bytes"
	"context"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

type memoryStream struct {
	chunks   [][]byte
	closed   bool
	session  worlds.StreamID
	sequence int
}

type streamWriteSession struct {
	mu     sync.Mutex
	world  *World
	runID  worlds.RunID
	name   string
	id     worlds.StreamID
	closed bool
}

func (s *streamWriteSession) ID() worlds.StreamID { return s.id }

func (w *World) StreamsCreateWriteSession(ctx context.Context, input worlds.StreamsCreateWriteSessionInput) (worlds.StreamWriteSession, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	defer w.notify()
	w.mu.Lock()
	defer w.mu.Unlock()
	stream := w.streamLocked(input.RunID, input.Name)
	if stream.closed {
		return nil, worlds.ErrRunTerminal
	}
	if stream.session != "" {
		return nil, worlds.ErrStreamLocked
	}
	id := worlds.NewStreamID()
	stream.session = id
	return &streamWriteSession{world: w, runID: input.RunID, name: input.Name, id: id}, nil
}

func (s *streamWriteSession) Write(ctx context.Context, input worlds.StreamWriteSessionWriteInput) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return worlds.ErrRunTerminal
	}
	return s.world.StreamsWriteSession(ctx, worlds.StreamsWriteSessionInput{RunID: s.runID, Name: s.name, SessionID: s.id, Sequence: input.Sequence, Chunks: input.Chunks})
}

func (s *streamWriteSession) Close(ctx context.Context) error {
	return s.close(ctx, true)
}

func (s *streamWriteSession) Release(ctx context.Context) error {
	return s.close(ctx, false)
}

func (s *streamWriteSession) close(ctx context.Context, closeStream bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	if err := s.world.StreamsCloseWriteSession(ctx, worlds.StreamsCloseWriteSessionInput{RunID: s.runID, Name: s.name, SessionID: s.id, Close: closeStream}); err != nil {
		return err
	}
	s.closed = true
	return nil
}

func (m *World) streamLocked(runID worlds.RunID, name string) *memoryStream {
	if m.streams[runID] == nil {
		m.streams[runID] = make(map[string]*memoryStream)
	}
	if m.streams[runID][name] == nil {
		m.streams[runID][name] = &memoryStream{}
	}
	return m.streams[runID][name]
}

func (m *World) StreamsWriteSession(ctx context.Context, input worlds.StreamsWriteSessionInput) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	defer m.notify()
	m.mu.Lock()
	defer m.mu.Unlock()
	stream := m.streamLocked(input.RunID, input.Name)
	if stream.closed || stream.session != input.SessionID {
		return worlds.ErrStreamLocked
	}
	if input.Sequence != stream.sequence {
		return worlds.ErrStreamLocked
	}
	for _, chunk := range input.Chunks {
		stream.chunks = append(stream.chunks, bytes.Clone(chunk))
	}
	stream.sequence++
	return nil
}

// StreamsCloseWriteSession releases a writer lease. Close controls whether the
// logical stream is also made terminal.
func (m *World) StreamsCloseWriteSession(ctx context.Context, input worlds.StreamsCloseWriteSessionInput) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	defer m.notify()
	m.mu.Lock()
	defer m.mu.Unlock()
	stream := m.streamLocked(input.RunID, input.Name)
	if stream.session != input.SessionID {
		return worlds.ErrStreamLocked
	}
	stream.closed = stream.closed || input.Close
	stream.session = ""
	stream.sequence = 0
	return nil
}

// Direct writes are retained for the application client surface. Workflow
// writers always use a session, which is the exclusive path.
func (m *World) StreamsWrite(ctx context.Context, input worlds.StreamsWriteInput) error {
	return m.StreamsWriteMulti(ctx, worlds.StreamsWriteMultiInput{RunID: input.RunID, Name: input.Name, Chunks: [][]byte{input.Chunk}})
}

func (m *World) StreamsWriteMulti(ctx context.Context, input worlds.StreamsWriteMultiInput) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	defer m.notify()
	m.mu.Lock()
	defer m.mu.Unlock()
	stream := m.streamLocked(input.RunID, input.Name)
	if stream.closed || stream.session != "" {
		return worlds.ErrStreamLocked
	}
	for _, chunk := range input.Chunks {
		stream.chunks = append(stream.chunks, bytes.Clone(chunk))
	}
	return nil
}

func (m *World) StreamsClose(_ context.Context, input worlds.StreamsCloseInput) error {
	defer m.notify()
	m.mu.Lock()
	defer m.mu.Unlock()
	stream := m.streamLocked(input.RunID, input.Name)
	if stream.session != "" {
		return worlds.ErrStreamLocked
	}
	stream.closed = true
	return nil
}

func (m *World) StreamsGetChunks(ctx context.Context, input worlds.StreamsGetChunksInput) (worlds.StreamChunksResult, error) {
	deadline := time.Now().Add(input.Wait)
	for {
		m.mu.RLock()
		stream := m.streams[input.RunID][input.Name]
		var chunks [][]byte
		closed := false
		if stream != nil {
			chunks = append(chunks, stream.chunks...)
			closed = stream.closed
		}
		m.mu.RUnlock()
		start := 0
		if input.Cursor != "" {
			start, _ = strconv.Atoi(input.Cursor)
		}
		if start < len(chunks) || closed || input.Wait <= 0 || !time.Now().Before(deadline) {
			limit := input.Limit
			if limit <= 0 {
				limit = 100
			}
			end := min(start+limit, len(chunks))
			result := worlds.StreamChunksResult{Done: closed, HasMore: end < len(chunks), RetryAfter: 50 * time.Millisecond, Cursor: strconv.Itoa(end)}
			for i, chunk := range chunks[start:end] {
				result.Data = append(result.Data, worlds.StreamChunk{Index: start + i, Data: bytes.Clone(chunk)})
			}
			return result, nil
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return worlds.StreamChunksResult{}, ctx.Err()
		case <-timer.C:
		}
	}
}

func (m *World) StreamsGetInfo(_ context.Context, input worlds.StreamsGetInfoInput) (worlds.StreamInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	stream := m.streams[input.RunID][input.Name]
	if stream == nil {
		return worlds.StreamInfo{TailIndex: -1}, nil
	}
	return worlds.StreamInfo{TailIndex: len(stream.chunks) - 1, Done: stream.closed}, nil
}

func (m *World) StreamsList(_ context.Context, input worlds.StreamsListInput) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	values := make([]string, 0, len(m.streams[input.RunID]))
	for name := range m.streams[input.RunID] {
		values = append(values, name)
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	return values, nil
}
