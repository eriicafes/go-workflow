package testworld

import (
	"bytes"
	"context"
	"sort"
	"strconv"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

type memoryStream struct {
	chunks [][]byte
	closed bool
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
	if worlds.RunDataExpired(m.runs[input.RunID], time.Now()) {
		// A deleted run's streams take no more data.
		return worlds.ErrStreamClosed
	}
	stream := m.streamLocked(input.RunID, input.Name)
	if stream.closed {
		return worlds.ErrStreamClosed
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
	stream.closed = true
	return nil
}

func (m *World) StreamsGetChunks(ctx context.Context, input worlds.StreamsGetChunksInput) (worlds.StreamChunksResult, error) {
	start, err := worlds.StreamsStartIndex(ctx, m, input)
	if err != nil {
		return worlds.StreamChunksResult{}, err
	}
	if input.Cursor != "" {
		start = 0
		if value, err := strconv.Atoi(input.Cursor); err == nil && value > 0 {
			start = value
		}
	}
	deadline := time.Now().Add(input.Wait)
	timer := time.NewTimer(input.Wait)
	for {
		changed := m.changed()
		m.mu.RLock()
		stream := m.streams[input.RunID][input.Name]
		var chunks [][]byte
		closed := false
		if stream != nil {
			chunks = append(chunks, stream.chunks...)
			closed = stream.closed
		}
		m.mu.RUnlock()
		if start < len(chunks) || closed || input.Wait <= 0 || !time.Now().Before(deadline) {
			limit := input.Limit
			if limit <= 0 {
				limit = 100
			}
			// A start past the end returns no chunks and keeps its cursor.
			end := max(min(start+limit, len(chunks)), start)
			result := worlds.StreamChunksResult{Done: closed, HasMore: end < len(chunks), RetryAfter: 50 * time.Millisecond, Cursor: strconv.Itoa(end)}
			for i, chunk := range chunks[min(start, len(chunks)):min(end, len(chunks))] {
				result.Data = append(result.Data, worlds.StreamChunk{Index: start + i, Data: bytes.Clone(chunk)})
			}
			return result, nil
		}
		select {
		case <-ctx.Done():
			return worlds.StreamChunksResult{}, ctx.Err()
		case <-timer.C:
		case <-changed:
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
