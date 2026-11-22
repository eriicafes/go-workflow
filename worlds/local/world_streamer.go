package localworld

import (
	"context"
	"errors"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

const streamRetryAfter = 50 * time.Millisecond

// A stream's chunks are rows numbered from 1, chunk index 0 being row 1. An
// end marker row follows the last chunk once the stream is closed, and
// nothing is appended after it. A run whose data was deleted keeps only its
// streams' end markers.

// streamState returns how many chunks a stream has and whether it is closed.
// A stream is keyed by its name alone: the run ID is stored with each chunk,
// for StreamsList and purges, but a read, an append, or an info of a stream
// names it only.
func (s *store) streamState(name string) (int, bool, error) {
	var count, closed int
	err := s.q.QueryRowContext(s.ctx, "SELECT coalesce(sum(1 - eof), 0), coalesce(max(eof), 0) FROM stream_chunks WHERE name = ?", name).Scan(&count, &closed)
	return count, closed == 1, err
}

// endPurgedStream writes the end marker of a new stream of a run whose data
// was deleted, in row 0 as the run's purge leaves its other streams, and
// reports whether it did.
func (s *store) endPurgedStream(runID worlds.RunID, name string) (bool, error) {
	run, err := s.getRunAnyTag(runID)
	if errors.Is(err, worlds.ErrRunNotFound) {
		return false, nil
	}
	if err != nil || !worlds.RunDataExpired(run.Run, time.Now()) {
		return false, err
	}
	return true, s.exec("INSERT INTO stream_chunks (name, idx, run_id, tag, data, eof) VALUES (?, 0, ?, ?, NULL, 1)", name, string(runID), s.tagValue())
}

func (w *World) StreamsWrite(ctx context.Context, input worlds.StreamsWriteInput) error {
	return w.StreamsWriteMulti(ctx, worlds.StreamsWriteMultiInput{RunID: input.RunID, Name: input.Name, Chunks: [][]byte{input.Chunk}})
}

func (w *World) StreamsWriteMulti(ctx context.Context, input worlds.StreamsWriteMultiInput) error {
	return w.write(ctx, func(s *store) error {
		count, closed, err := s.streamState(input.Name)
		if err != nil {
			return err
		}
		if closed {
			// A closed stream takes no more data.
			return nil
		}
		if count == 0 {
			// A deleted run's streams take no more data: its purge closed the
			// streams it had, and a new one is closed as it is first written.
			if purged, err := s.endPurgedStream(input.RunID, input.Name); err != nil || purged {
				return err
			}
		}
		for index, chunk := range input.Chunks {
			if chunk == nil {
				chunk = []byte{}
			}
			if err := s.exec("INSERT INTO stream_chunks (run_id, name, idx, tag, data, eof) VALUES (?, ?, ?, ?, ?, 0)", string(input.RunID), input.Name, count+index+1, s.tagValue(), chunk); err != nil {
				return err
			}
		}
		return nil
	})
}

func (w *World) StreamsClose(ctx context.Context, input worlds.StreamsCloseInput) error {
	return w.write(ctx, func(s *store) error {
		count, closed, err := s.streamState(input.Name)
		if err != nil || closed {
			return err
		}
		if count == 0 {
			// A deleted run's new stream is closed as its purge closed the
			// others.
			if purged, err := s.endPurgedStream(input.RunID, input.Name); err != nil || purged {
				return err
			}
		}
		return s.exec("INSERT INTO stream_chunks (run_id, name, idx, tag, data, eof) VALUES (?, ?, ?, ?, NULL, 1)", string(input.RunID), input.Name, count+1, s.tagValue())
	})
}

func (w *World) StreamsGetChunks(ctx context.Context, input worlds.StreamsGetChunksInput) (worlds.StreamChunksResult, error) {
	// Sleep on this process's commits, polling for other processes', until a
	// chunk past the cursor exists, the stream is done, or Wait elapses.
	start := parseChunkCursor(input.Cursor)
	if input.Cursor == "" {
		var err error
		if start, err = worlds.StreamsStartIndex(ctx, w, input); err != nil {
			return worlds.StreamChunksResult{}, err
		}
	}
	limit := input.Limit
	if limit <= 0 {
		limit = 100
	}
	deadline := time.Now().Add(input.Wait)
	timer := time.NewTimer(input.Wait)
	defer timer.Stop()
	for {
		changed := w.changed()
		var result worlds.StreamChunksResult
		ready := false
		err := w.read(ctx, func(s *store) error {
			var err error
			result, ready, err = s.readChunks(input.Name, start, limit)
			return err
		})
		if err != nil || ready || !time.Now().Before(deadline) {
			return result, err
		}
		if err := waitChange(ctx, changed, timer.C); err != nil {
			return worlds.StreamChunksResult{}, err
		}
	}
}

func (s *store) readChunks(name string, start, limit int) (worlds.StreamChunksResult, bool, error) {
	count, closed, err := s.streamState(name)
	if err != nil {
		return worlds.StreamChunksResult{}, false, err
	}
	end := max(start, min(start+limit, count))
	result := worlds.StreamChunksResult{Data: []worlds.StreamChunk{}, Cursor: chunkCursor(end), HasMore: end < count, Done: closed, RetryAfter: streamRetryAfter}
	if end > start {
		rows, err := s.q.QueryContext(s.ctx, "SELECT idx, data FROM stream_chunks WHERE name = ? AND eof = 0 AND idx > ? AND idx <= ? ORDER BY idx", name, start, end)
		if err != nil {
			return worlds.StreamChunksResult{}, false, err
		}
		defer rows.Close()
		for rows.Next() {
			var idx int
			var data []byte
			if err := rows.Scan(&idx, &data); err != nil {
				return worlds.StreamChunksResult{}, false, err
			}
			if data == nil {
				data = []byte{}
			}
			result.Data = append(result.Data, worlds.StreamChunk{Index: idx - 1, Data: data})
		}
		if err := rows.Err(); err != nil {
			return worlds.StreamChunksResult{}, false, err
		}
	}
	return result, len(result.Data) > 0 || closed, nil
}

func (w *World) StreamsGetInfo(ctx context.Context, input worlds.StreamsGetInfoInput) (worlds.StreamInfo, error) {
	var info worlds.StreamInfo
	err := w.read(ctx, func(s *store) error {
		count, closed, err := s.streamState(input.Name)
		info = worlds.StreamInfo{TailIndex: count - 1, Done: closed}
		return err
	})
	return info, err
}

func (w *World) StreamsList(ctx context.Context, input worlds.StreamsListInput) ([]string, error) {
	names := []string{}
	err := w.read(ctx, func(s *store) error {
		// A run's streams are listed whatever their tag.
		rows, err := s.q.QueryContext(s.ctx, "SELECT DISTINCT name FROM stream_chunks WHERE run_id = ? ORDER BY name", string(input.RunID))
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				return err
			}
			names = append(names, name)
		}
		return rows.Err()
	})
	return names, err
}
