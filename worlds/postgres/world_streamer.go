package postgresworld

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/eriicafes/go-workflow/worlds"
	"github.com/jackc/pgx/v5"
)

const (
	defaultChunkLimit = 100
	streamRetryAfter  = 50 * time.Millisecond
)

// A stream's chunks are rows ordered by their chnk_<ULID> id, as
// world-postgres stores them, keyed by the stream's run as well: a Go stream
// name is unique only within its run. An eof row with empty data closes the
// stream. A chunk's index is its position among the stream's other chunks.
// Writers of one stream take its advisory lock, so each new id follows the
// stream's last.

func (w *World) StreamsWrite(ctx context.Context, input worlds.StreamsWriteInput) error {
	return w.StreamsWriteMulti(ctx, worlds.StreamsWriteMultiInput{RunID: input.RunID, Name: input.Name, Chunks: [][]byte{input.Chunk}})
}

func (w *World) StreamsWriteMulti(ctx context.Context, input worlds.StreamsWriteMultiInput) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return w.inTx(ctx, func(tx pgx.Tx) error {
		// The run's row lock orders the write against a purge of its data,
		// and a deleted run's streams take no more data.
		var expired bool
		err := tx.QueryRow(ctx, w.q(`SELECT coalesce(expired_at <= now(), false) FROM workflow.workflow_runs WHERE id = $1 FOR SHARE`), input.RunID).Scan(&expired)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if expired {
			return worlds.ErrStreamClosed
		}
		last, closed, err := w.lockStream(ctx, tx, input.RunID, input.Name)
		if err != nil {
			return err
		}
		if closed {
			return worlds.ErrStreamClosed
		}
		if len(input.Chunks) == 0 {
			return nil
		}
		ids := chunkIDs(last, len(input.Chunks))
		values := make([][]byte, len(input.Chunks))
		for i, chunk := range input.Chunks {
			values[i] = chunk
			if values[i] == nil {
				values[i] = []byte{}
			}
		}
		if _, err := tx.Exec(ctx, w.q(`INSERT INTO workflow.workflow_stream_chunks (id, stream_id, run_id, data, eof)
			SELECT c.id, $1, $2, c.data, false FROM unnest($3::varchar[], $4::bytea[]) AS c(id, data)`), input.Name, input.RunID, ids, values); err != nil {
			return err
		}
		return w.notifyChunk(ctx, tx, input.RunID, input.Name, ids[len(ids)-1])
	})
}

func (w *World) StreamsClose(ctx context.Context, input worlds.StreamsCloseInput) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return w.inTx(ctx, func(tx pgx.Tx) error {
		last, closed, err := w.lockStream(ctx, tx, input.RunID, input.Name)
		if err != nil || closed {
			return err
		}
		id := chunkIDs(last, 1)[0]
		if _, err := tx.Exec(ctx, w.q(`INSERT INTO workflow.workflow_stream_chunks (id, stream_id, run_id, data, eof) VALUES ($1, $2, $3, '', true)`), id, input.Name, input.RunID); err != nil {
			return err
		}
		return w.notifyChunk(ctx, tx, input.RunID, input.Name, id)
	})
}

// lockStream takes the stream's write lock and returns its last chunk ID and
// whether it is closed.
func (w *World) lockStream(ctx context.Context, tx pgx.Tx, runID worlds.RunID, name string) (string, bool, error) {
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, w.prefix+streamKey(runID, name)); err != nil {
		return "", false, err
	}
	var last *string
	var closed bool
	err := tx.QueryRow(ctx, w.q(`SELECT max(id), coalesce(bool_or(eof), false) FROM workflow.workflow_stream_chunks WHERE run_id = $1 AND stream_id = $2`), runID, name).Scan(&last, &closed)
	if err != nil || last == nil {
		return "", closed, err
	}
	return *last, closed, nil
}

// chunkIDs returns count new chunk IDs, each after the one before, the first
// after last.
func chunkIDs(last string, count int) []string {
	ids := make([]string, count)
	previous, _ := ulid.ParseStrict(strings.TrimPrefix(last, "chnk_"))
	for i := range ids {
		next := ulid.Make()
		if next.Compare(previous) <= 0 {
			next = incrementULID(previous)
		}
		ids[i], previous = "chnk_"+next.String(), next
	}
	return ids
}

// incrementULID returns the ULID after id.
func incrementULID(id ulid.ULID) ulid.ULID {
	for i := len(id) - 1; i >= 0; i-- {
		id[i]++
		if id[i] != 0 {
			break
		}
	}
	return id
}

func (w *World) StreamsGetChunks(ctx context.Context, input worlds.StreamsGetChunksInput) (worlds.StreamChunksResult, error) {
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

// readChunks reads the stream's state and chunks in one transaction, so the
// page is consistent with Done.
func (w *World) readChunks(ctx context.Context, runID worlds.RunID, name string, start, limit int) (worlds.StreamChunksResult, bool, error) {
	var result worlds.StreamChunksResult
	available := false
	err := pgx.BeginTxFunc(ctx, w.pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		var closed bool
		var count int64
		// A run whose expired_at has passed reads as empty and closed, before
		// Worker deletes its chunks.
		err := tx.QueryRow(ctx, w.q(`SELECT coalesce(bool_or(c.eof), false) OR coalesce(bool_or(r.expired_at <= now()), false),
				CASE WHEN coalesce(bool_or(r.expired_at <= now()), false) THEN 0 ELSE count(*) FILTER (WHERE NOT c.eof) END
			FROM workflow.workflow_stream_chunks c LEFT JOIN workflow.workflow_runs r ON r.id = c.run_id
			WHERE c.run_id = $1 AND c.stream_id = $2`), runID, name).Scan(&closed, &count)
		if err != nil {
			return err
		}
		total := int(count)
		result = worlds.StreamChunksResult{Data: []worlds.StreamChunk{}, Done: closed, RetryAfter: streamRetryAfter, Cursor: strconv.Itoa(start)}
		if start >= total {
			return nil
		}
		end := min(start+limit, total)
		rows, err := tx.Query(ctx, w.q(`SELECT data FROM workflow.workflow_stream_chunks WHERE run_id = $1 AND stream_id = $2 AND NOT eof ORDER BY id OFFSET $3 LIMIT $4`),
			runID, name, start, end-start)
		if err != nil {
			return err
		}
		index := start
		chunks, err := collect(rows, func(row pgx.Row) (worlds.StreamChunk, error) {
			chunk := worlds.StreamChunk{Index: index}
			index++
			err := row.Scan(&chunk.Data)
			return chunk, err
		})
		if err != nil {
			return err
		}
		result.Data, result.HasMore, result.Cursor = chunks, end < total, strconv.Itoa(end)
		available = true
		return nil
	})
	return result, available, err
}

func (w *World) StreamsGetInfo(ctx context.Context, input worlds.StreamsGetInfoInput) (worlds.StreamInfo, error) {
	var info worlds.StreamInfo
	var count int64
	err := w.pool.QueryRow(ctx, w.q(`SELECT count(*) FILTER (WHERE NOT eof), coalesce(bool_or(eof), false) FROM workflow.workflow_stream_chunks WHERE run_id = $1 AND stream_id = $2`), input.RunID, input.Name).Scan(&count, &info.Done)
	if err != nil {
		return worlds.StreamInfo{}, err
	}
	info.TailIndex = int(count) - 1
	return info, nil
}

func (w *World) StreamsList(ctx context.Context, input worlds.StreamsListInput) ([]string, error) {
	rows, err := w.pool.Query(ctx, w.q(`SELECT DISTINCT stream_id COLLATE "C" FROM workflow.workflow_stream_chunks WHERE run_id = $1 ORDER BY 1`), input.RunID)
	if err != nil {
		return nil, err
	}
	return collect(rows, func(row pgx.Row) (string, error) {
		var name string
		err := row.Scan(&name)
		return name, err
	})
}

// purgeStreams deletes the chunks of a run's streams, leaving each empty and
// closed.
func (w *World) purgeStreams(ctx context.Context, tx pgx.Tx, runID worlds.RunID) error {
	rows, err := tx.Query(ctx, w.q(`SELECT DISTINCT stream_id FROM workflow.workflow_stream_chunks WHERE run_id = $1`), runID)
	if err != nil {
		return err
	}
	names, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, w.q(`DELETE FROM workflow.workflow_stream_chunks WHERE run_id = $1`), runID); err != nil {
		return err
	}
	for _, name := range names {
		if _, err := tx.Exec(ctx, w.q(`INSERT INTO workflow.workflow_stream_chunks (id, stream_id, run_id, data, eof) VALUES ($1, $2, $3, '', true)`), chunkIDs("", 1)[0], name, runID); err != nil {
			return err
		}
	}
	return nil
}

// streamKey identifies a stream in notifications. It is a hash, because
// pg_notify payloads must stay under 8000 bytes and stream names are not
// bounded.
func streamKey(runID worlds.RunID, name string) string {
	sum := sha256.Sum256([]byte(string(runID) + "/" + name))
	return hex.EncodeToString(sum[:])
}

// notifyChunk sends workflow_event_chunk for the stream's chunk, as
// world-postgres does.
func (w *World) notifyChunk(ctx context.Context, tx pgx.Tx, runID worlds.RunID, name, chunkID string) error {
	payload, err := json.Marshal(chunkNotification{StreamID: streamKey(runID, name), ChunkID: chunkID})
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `SELECT pg_notify($1, $2)`, channelStreams, string(payload))
	return err
}
