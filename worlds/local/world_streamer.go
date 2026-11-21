package localworld

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

const streamRetryAfter = 50 * time.Millisecond

// A stream's chunks are rows numbered from 1, chunk index 0 being row 1. An
// end marker row follows the last chunk once the stream is closed, and
// nothing is appended after it. A run whose data was deleted keeps only its
// streams' end markers.

// streamState returns how many chunks a stream has and whether it is closed.
func (s *store) streamState(runID worlds.RunID, name string) (int, bool, error) {
	condition, args := s.tagged("tag")
	var count, closed int
	err := s.q.QueryRowContext(s.ctx, "SELECT coalesce(sum(1 - eof), 0), coalesce(max(eof), 0) FROM stream_chunks WHERE run_id = ? AND name = ? AND "+condition, append([]any{string(runID), name}, args...)...).Scan(&count, &closed)
	return count, closed == 1, err
}

func (w *World) StreamsWrite(ctx context.Context, input worlds.StreamsWriteInput) error {
	return w.StreamsWriteMulti(ctx, worlds.StreamsWriteMultiInput{RunID: input.RunID, Name: input.Name, Chunks: [][]byte{input.Chunk}})
}

func (w *World) StreamsWriteMulti(ctx context.Context, input worlds.StreamsWriteMultiInput) error {
	return w.write(ctx, func(s *store) error {
		count, closed, err := s.streamState(input.RunID, input.Name)
		if err != nil {
			return err
		}
		if closed {
			// A closed stream takes no more data, as world-local drops it.
			return nil
		}
		if count == 0 {
			// A deleted run's streams take no more data: its purge closed the
			// streams it had, and a new one is closed as it is first written,
			// as world-local closes it.
			run, err := s.getRunAnyTag(input.RunID)
			if err != nil && !errors.Is(err, worlds.ErrRunNotFound) {
				return err
			}
			if err == nil && worlds.RunDataExpired(run.Run, time.Now()) {
				return s.exec("INSERT INTO stream_chunks (run_id, name, idx, tag, data, eof) VALUES (?, ?, 1, ?, NULL, 1)", string(input.RunID), input.Name, s.tagValue())
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
		count, closed, err := s.streamState(input.RunID, input.Name)
		if err != nil || closed {
			return err
		}
		return s.exec("INSERT INTO stream_chunks (run_id, name, idx, tag, data, eof) VALUES (?, ?, ?, ?, NULL, 1)", string(input.RunID), input.Name, count+1, s.tagValue())
	})
}

func (w *World) StreamsGetChunks(ctx context.Context, input worlds.StreamsGetChunksInput) (worlds.StreamChunksResult, error) {
	// Sleep on this process's commits, polling for other processes', until a
	// chunk past the cursor exists, the stream is done, or Wait elapses.
	start := chunkCursor(input.Cursor)
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
			result, ready, err = s.readChunks(input.RunID, input.Name, start, limit)
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

func (s *store) readChunks(runID worlds.RunID, name string, start, limit int) (worlds.StreamChunksResult, bool, error) {
	count, closed, err := s.streamState(runID, name)
	if err != nil {
		return worlds.StreamChunksResult{}, false, err
	}
	end := max(start, min(start+limit, count))
	result := worlds.StreamChunksResult{Data: []worlds.StreamChunk{}, Cursor: strconv.Itoa(end), HasMore: end < count, Done: closed, RetryAfter: streamRetryAfter}
	if end > start {
		condition, args := s.tagged("tag")
		rows, err := s.q.QueryContext(s.ctx, "SELECT idx, data FROM stream_chunks WHERE run_id = ? AND name = ? AND eof = 0 AND idx > ? AND idx <= ? AND "+condition+" ORDER BY idx", append([]any{string(runID), name, start, end}, args...)...)
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
		count, closed, err := s.streamState(input.RunID, input.Name)
		info = worlds.StreamInfo{TailIndex: count - 1, Done: closed}
		return err
	})
	return info, err
}

func (w *World) StreamsList(ctx context.Context, input worlds.StreamsListInput) ([]string, error) {
	names := []string{}
	err := w.read(ctx, func(s *store) error {
		condition, args := s.tagged("tag")
		rows, err := s.q.QueryContext(s.ctx, "SELECT DISTINCT name FROM stream_chunks WHERE run_id = ? AND "+condition+" ORDER BY name", append([]any{string(input.RunID)}, args...)...)
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
