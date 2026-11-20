package postgresworld

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
	"github.com/jackc/pgx/v5"
)

const (
	defaultChunkLimit = 100
	streamRetryAfter  = 50 * time.Millisecond
)

func (w *World) StreamsWrite(ctx context.Context, input worlds.StreamsWriteInput) error {
	return w.StreamsWriteMulti(ctx, worlds.StreamsWriteMultiInput{RunID: input.RunID, Name: input.Name, Chunks: [][]byte{input.Chunk}})
}

func (w *World) StreamsWriteMulti(ctx context.Context, input worlds.StreamsWriteMultiInput) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return w.inTx(ctx, func(tx pgx.Tx) error {
		var closed bool
		var count int64
		err := tx.QueryRow(ctx, `INSERT INTO go_workflow_streams (run_id, name) VALUES ($1, $2)
			ON CONFLICT (run_id, name) DO UPDATE SET run_id = excluded.run_id
			RETURNING closed, chunk_count`, input.RunID, input.Name).Scan(&closed, &count)
		if err != nil {
			return err
		}
		if closed {
			return worlds.ErrStreamClosed
		}
		if err := insertChunks(ctx, tx, input.RunID, input.Name, count, input.Chunks); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE go_workflow_streams SET chunk_count = chunk_count + $3 WHERE run_id = $1 AND name = $2`,
			input.RunID, input.Name, len(input.Chunks)); err != nil {
			return err
		}
		return notifyStream(ctx, tx, input.RunID, input.Name)
	})
}

func (w *World) StreamsClose(ctx context.Context, input worlds.StreamsCloseInput) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return w.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO go_workflow_streams (run_id, name, closed) VALUES ($1, $2, true)
			ON CONFLICT (run_id, name) DO UPDATE SET closed = true`, input.RunID, input.Name); err != nil {
			return err
		}
		return notifyStream(ctx, tx, input.RunID, input.Name)
	})
}

func (w *World) StreamsGetChunks(ctx context.Context, input worlds.StreamsGetChunksInput) (worlds.StreamChunksResult, error) {
	// Read the stream row before its chunks. Since chunk_count and the chunks
	// it covers commit together, the page is consistent with Done.
	start, err := worlds.StreamsStartIndex(ctx, w, input)
	if err != nil {
		return worlds.StreamChunksResult{}, err
	}
	if input.Cursor != "" {
		value, err := strconv.Atoi(input.Cursor)
		if err != nil || value < 0 {
			return worlds.StreamChunksResult{}, errInvalidCursor
		}
		start = value
	}
	limit := input.Limit
	if limit <= 0 {
		limit = defaultChunkLimit
	}
	deadline := time.Now().Add(input.Wait)
	for {
		wake, release := w.notifier.wait(channelStreams, streamKey(input.RunID, input.Name))
		result, available, err := w.readChunks(ctx, input.RunID, input.Name, start, limit)
		remaining := time.Until(deadline)
		if err != nil || available || result.Done || input.Wait <= 0 || remaining <= 0 {
			release()
			return result, err
		}
		timer := time.NewTimer(min(remaining, w.interval))
		select {
		case <-ctx.Done():
			release()
			return worlds.StreamChunksResult{}, ctx.Err()
		case <-wake:
		case <-timer.C:
		}
		release()
	}
}

func (w *World) readChunks(ctx context.Context, runID worlds.RunID, name string, start, limit int) (worlds.StreamChunksResult, bool, error) {
	var closed bool
	var count int64
	err := w.pool.QueryRow(ctx, `SELECT closed, chunk_count FROM go_workflow_streams WHERE run_id = $1 AND name = $2`, runID, name).Scan(&closed, &count)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return worlds.StreamChunksResult{}, false, err
	}
	total := int(count)
	result := worlds.StreamChunksResult{Done: closed, RetryAfter: streamRetryAfter, Cursor: strconv.Itoa(start)}
	if start >= total {
		return result, false, nil
	}
	end := min(start+limit, total)
	rows, err := w.pool.Query(ctx, `SELECT idx, data FROM go_workflow_stream_chunks WHERE run_id = $1 AND name = $2 AND idx >= $3 AND idx < $4 ORDER BY idx`,
		runID, name, start, end)
	if err != nil {
		return worlds.StreamChunksResult{}, false, err
	}
	chunks, err := collect(rows, func(row pgx.Row) (worlds.StreamChunk, error) {
		var chunk worlds.StreamChunk
		var index int64
		err := row.Scan(&index, &chunk.Data)
		chunk.Index = int(index)
		return chunk, err
	})
	if err != nil {
		return worlds.StreamChunksResult{}, false, err
	}
	result.Data, result.HasMore, result.Cursor = chunks, end < total, strconv.Itoa(end)
	return result, true, nil
}

func (w *World) StreamsGetInfo(ctx context.Context, input worlds.StreamsGetInfoInput) (worlds.StreamInfo, error) {
	var info worlds.StreamInfo
	var count int64
	err := w.pool.QueryRow(ctx, `SELECT chunk_count, closed FROM go_workflow_streams WHERE run_id = $1 AND name = $2`, input.RunID, input.Name).Scan(&count, &info.Done)
	if errors.Is(err, pgx.ErrNoRows) {
		return worlds.StreamInfo{TailIndex: -1}, nil
	}
	if err != nil {
		return worlds.StreamInfo{}, err
	}
	info.TailIndex = int(count) - 1
	return info, nil
}

func (w *World) StreamsList(ctx context.Context, input worlds.StreamsListInput) ([]string, error) {
	rows, err := w.pool.Query(ctx, `SELECT name FROM go_workflow_streams WHERE run_id = $1 ORDER BY name COLLATE "C"`, input.RunID)
	if err != nil {
		return nil, err
	}
	return collect(rows, func(row pgx.Row) (string, error) {
		var name string
		err := row.Scan(&name)
		return name, err
	})
}

// streamKey is the notification payload for a stream. It is a hash, because
// pg_notify payloads must stay under 8000 bytes and stream names are not
// bounded.
func streamKey(runID worlds.RunID, name string) string {
	sum := sha256.Sum256([]byte(string(runID) + "/" + name))
	return hex.EncodeToString(sum[:])
}

func notifyStream(ctx context.Context, tx pgx.Tx, runID worlds.RunID, name string) error {
	_, err := tx.Exec(ctx, `SELECT pg_notify($1, $2)`, channelStreams, streamKey(runID, name))
	return err
}

func insertChunks(ctx context.Context, tx pgx.Tx, runID worlds.RunID, name string, start int64, chunks [][]byte) error {
	if len(chunks) == 0 {
		return nil
	}
	values := make([][]byte, len(chunks))
	for i, chunk := range chunks {
		values[i] = chunk
		if values[i] == nil {
			values[i] = []byte{}
		}
	}
	_, err := tx.Exec(ctx, `INSERT INTO go_workflow_stream_chunks (run_id, name, idx, data)
		SELECT $1, $2, $3 + c.ord - 1, c.data FROM unnest($4::bytea[]) WITH ORDINALITY AS c(data, ord)`, runID, name, start, values)
	return err
}
