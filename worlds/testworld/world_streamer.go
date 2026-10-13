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
	chunks [][]byte
	closed bool
}

type streamWriteSession struct {
	mu     sync.Mutex
	world  *World
	input  worlds.StreamsCreateWriteSessionInput
	closed bool
}

func (w *World) StreamsCreateWriteSession(ctx context.Context, input worlds.StreamsCreateWriteSessionInput) (worlds.StreamWriteSession, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &streamWriteSession{world: w, input: input}, nil
}

func (s *streamWriteSession) Write(ctx context.Context, input worlds.StreamWriteSessionWriteInput) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return worlds.ErrRunTerminal
	}
	return s.world.StreamsWriteMulti(ctx, worlds.StreamsWriteMultiInput{RunID: s.input.RunID, Name: s.input.Name, Chunks: input.Chunks})
}

func (s *streamWriteSession) Close(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func (m *World) streamLocked(runID worlds.RunID, name worlds.StreamID) *memoryStream {
	if m.streams[runID] == nil {
		m.streams[runID] = make(map[worlds.StreamID]*memoryStream)
	}
	if m.streams[runID][name] == nil {
		m.streams[runID][name] = &memoryStream{}
	}
	return m.streams[runID][name]
}

func (m *World) StreamsWrite(ctx context.Context, input worlds.StreamsWriteInput) error {
	return m.StreamsWriteMulti(ctx, worlds.StreamsWriteMultiInput{RunID: input.RunID, Name: input.Name, Chunks: [][]byte{input.Chunk}})
}

func (m *World) StreamsWriteMulti(ctx context.Context, input worlds.StreamsWriteMultiInput) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	stream := m.streamLocked(input.RunID, input.Name)
	if stream.closed {
		return worlds.ErrRunTerminal
	}
	for _, chunk := range input.Chunks {
		stream.chunks = append(stream.chunks, bytes.Clone(chunk))
	}
	return nil
}

func (m *World) StreamsClose(ctx context.Context, input worlds.StreamsCloseInput) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.streamLocked(input.RunID, input.Name).closed = true
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
			if value, err := strconv.Atoi(input.Cursor); err == nil {
				start = value
			}
		}
		if start < len(chunks) || closed || input.Wait <= 0 || !time.Now().Before(deadline) {
			limit := input.Limit
			if limit <= 0 {
				limit = 100
			}
			end := start + limit
			if end > len(chunks) {
				end = len(chunks)
			}
			result := worlds.StreamChunksResult{Closed: closed, HasMore: end < len(chunks), RetryAfter: 50 * time.Millisecond, Cursor: strconv.Itoa(end)}
			for i, chunk := range chunks[start:end] {
				result.Chunks = append(result.Chunks, worlds.StreamChunk{Index: start + i, Data: bytes.Clone(chunk)})
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

func (m *World) StreamsGetInfo(ctx context.Context, input worlds.StreamsGetInfoInput) (worlds.StreamInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	stream := m.streams[input.RunID][input.Name]
	if stream == nil {
		return worlds.StreamInfo{TailIndex: -1}, nil
	}
	return worlds.StreamInfo{TailIndex: len(stream.chunks) - 1, Closed: stream.closed}, nil
}

func (m *World) StreamsList(ctx context.Context, input worlds.StreamsListInput) ([]worlds.StreamID, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	values := make([]worlds.StreamID, 0, len(m.streams[input.RunID]))
	for name := range m.streams[input.RunID] {
		values = append(values, name)
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	return values, nil
}
