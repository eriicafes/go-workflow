package localworld

import (
	"context"
	"fmt"
	"path"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

const streamRetryAfter = 50 * time.Millisecond

// streamMeta is the commit point for a stream. Chunk files at or past Count
// are ignored and overwritten by the next write.
type streamMeta struct {
	Name     string          `json:"name"`
	Closed   bool            `json:"closed"`
	Session  worlds.StreamID `json:"session,omitempty"`
	Sequence int             `json:"sequence"`
	Count    int             `json:"count"`
}

func (w *World) readStream(runID worlds.RunID, name string) (streamMeta, bool, error) {
	meta := streamMeta{Name: name}
	ok, err := w.readOptional(path.Join(streamDir(runID, name), "meta.json"), &meta)
	return meta, ok, err
}

func (w *World) writeStream(runID worlds.RunID, meta streamMeta) error {
	if err := writeJSON(w.root, path.Join(streamDir(runID, meta.Name), "meta.json"), meta); err != nil {
		return err
	}
	w.signal()
	return nil
}

// appendChunks writes chunk files then advances Count in meta.
func (w *World) appendChunks(runID worlds.RunID, meta *streamMeta, chunks [][]byte) error {
	for index, chunk := range chunks {
		if err := writeJSON(w.root, chunkPath(runID, meta.Name, meta.Count+index), chunk); err != nil {
			return err
		}
	}
	meta.Count += len(chunks)
	return nil
}

// updateStream applies change to the stream meta under the write lock.
func (w *World) updateStream(ctx context.Context, runID worlds.RunID, name string, change func(*streamMeta) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	unlock, err := w.writeLock()
	if err != nil {
		return err
	}
	defer unlock()
	meta, _, err := w.readStream(runID, name)
	if err != nil {
		return err
	}
	if err := change(&meta); err != nil {
		return err
	}
	return w.writeStream(runID, meta)
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

func (s *streamWriteSession) Write(ctx context.Context, input worlds.StreamWriteSessionWriteInput) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return worlds.ErrRunTerminal
	}
	return s.world.StreamsWriteSession(ctx, worlds.StreamsWriteSessionInput{RunID: s.runID, Name: s.name, SessionID: s.id, Sequence: input.Sequence, Chunks: input.Chunks})
}

func (s *streamWriteSession) Close(ctx context.Context) error   { return s.close(ctx, true) }
func (s *streamWriteSession) Release(ctx context.Context) error { return s.close(ctx, false) }

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

func (w *World) StreamsCreateWriteSession(ctx context.Context, input worlds.StreamsCreateWriteSessionInput) (worlds.StreamWriteSession, error) {
	id := worlds.NewStreamID()
	err := w.updateStream(ctx, input.RunID, input.Name, func(meta *streamMeta) error {
		if meta.Closed {
			return worlds.ErrRunTerminal
		}
		if meta.Session != "" {
			return worlds.ErrStreamLocked
		}
		meta.Session, meta.Sequence = id, 0
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &streamWriteSession{world: w, runID: input.RunID, name: input.Name, id: id}, nil
}

func (w *World) StreamsWriteSession(ctx context.Context, input worlds.StreamsWriteSessionInput) error {
	return w.updateStream(ctx, input.RunID, input.Name, func(meta *streamMeta) error {
		if meta.Closed || meta.Session != input.SessionID || input.Sequence != meta.Sequence {
			return worlds.ErrStreamLocked
		}
		meta.Sequence++
		return w.appendChunks(input.RunID, meta, input.Chunks)
	})
}

func (w *World) StreamsCloseWriteSession(ctx context.Context, input worlds.StreamsCloseWriteSessionInput) error {
	return w.updateStream(ctx, input.RunID, input.Name, func(meta *streamMeta) error {
		if meta.Session != input.SessionID {
			return worlds.ErrStreamLocked
		}
		meta.Closed = meta.Closed || input.Close
		meta.Session, meta.Sequence = "", 0
		return nil
	})
}

func (w *World) StreamsWrite(ctx context.Context, input worlds.StreamsWriteInput) error {
	return w.StreamsWriteMulti(ctx, worlds.StreamsWriteMultiInput{RunID: input.RunID, Name: input.Name, Chunks: [][]byte{input.Chunk}})
}

func (w *World) StreamsWriteMulti(ctx context.Context, input worlds.StreamsWriteMultiInput) error {
	return w.updateStream(ctx, input.RunID, input.Name, func(meta *streamMeta) error {
		if meta.Closed || meta.Session != "" {
			return worlds.ErrStreamLocked
		}
		return w.appendChunks(input.RunID, meta, input.Chunks)
	})
}

func (w *World) StreamsClose(ctx context.Context, input worlds.StreamsCloseInput) error {
	return w.updateStream(ctx, input.RunID, input.Name, func(meta *streamMeta) error {
		if meta.Session != "" {
			return worlds.ErrStreamLocked
		}
		meta.Closed = true
		return nil
	})
}

func (w *World) StreamsGetChunks(ctx context.Context, input worlds.StreamsGetChunksInput) (worlds.StreamChunksResult, error) {
	// Sleep on the change channel until a chunk past the cursor exists, the
	// stream is done, or Wait elapses.
	start := chunkCursor(input.Cursor)
	limit := input.Limit
	if limit <= 0 {
		limit = 100
	}
	deadline := time.Now().Add(input.Wait)
	timer := time.NewTimer(input.Wait)
	for {
		if err := ctx.Err(); err != nil {
			return worlds.StreamChunksResult{}, err
		}
		changed := w.changed()
		result, ready, err := w.readChunks(input.RunID, input.Name, start, limit)
		if err != nil || ready || !time.Now().Before(deadline) {
			return result, err
		}
		select {
		case <-ctx.Done():
			return worlds.StreamChunksResult{}, ctx.Err()
		case <-timer.C:
		case <-changed:
		}
	}
}

func (w *World) readChunks(runID worlds.RunID, name string, start, limit int) (worlds.StreamChunksResult, bool, error) {
	unlock, err := w.readLock()
	if err != nil {
		return worlds.StreamChunksResult{}, false, err
	}
	defer unlock()
	meta, _, err := w.readStream(runID, name)
	if err != nil {
		return worlds.StreamChunksResult{}, false, err
	}
	end := max(start, min(start+limit, meta.Count))
	result := worlds.StreamChunksResult{Data: []worlds.StreamChunk{}, Cursor: strconv.Itoa(end), HasMore: end < meta.Count, Done: meta.Closed, RetryAfter: streamRetryAfter}
	for index := start; index < end; index++ {
		var data []byte
		if err := readJSON(w.root, chunkPath(runID, name, index), &data); err != nil {
			return worlds.StreamChunksResult{}, false, err
		}
		result.Data = append(result.Data, worlds.StreamChunk{Index: index, Data: data})
	}
	return result, len(result.Data) > 0 || meta.Closed, nil
}

func (w *World) StreamsGetInfo(ctx context.Context, input worlds.StreamsGetInfoInput) (worlds.StreamInfo, error) {
	if err := ctx.Err(); err != nil {
		return worlds.StreamInfo{}, err
	}
	unlock, err := w.readLock()
	if err != nil {
		return worlds.StreamInfo{}, err
	}
	defer unlock()
	meta, _, err := w.readStream(input.RunID, input.Name)
	if err != nil {
		return worlds.StreamInfo{}, err
	}
	return worlds.StreamInfo{TailIndex: meta.Count - 1, Done: meta.Closed}, nil
}

func (w *World) StreamsList(ctx context.Context, input worlds.StreamsListInput) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	unlock, err := w.readLock()
	if err != nil {
		return nil, err
	}
	defer unlock()
	dir := path.Join("streams", fileKey(string(input.RunID)))
	keys, err := listDirs(w.root, dir)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(keys))
	for _, key := range keys {
		var meta streamMeta
		ok, err := w.readOptional(path.Join(dir, key, "meta.json"), &meta)
		if err != nil {
			return nil, err
		}
		if ok {
			names = append(names, meta.Name)
		}
	}
	slices.Sort(names)
	return names, nil
}

func streamDir(runID worlds.RunID, name string) string {
	return path.Join("streams", fileKey(string(runID)), fileKey(name))
}

func chunkPath(runID worlds.RunID, name string, index int) string {
	return path.Join(streamDir(runID, name), fmt.Sprintf("%010d.json", index))
}
