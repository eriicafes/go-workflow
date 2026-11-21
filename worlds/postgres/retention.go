package postgresworld

import (
	"context"
	"log"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/eriicafes/go-workflow/worlds"
)

const (
	// expireInterval is how often Worker deletes expired runs' data.
	expireInterval = time.Minute
	// expireBatch is the most runs one transaction deletes the data of.
	expireBatch = 100
)

// When a run's data is deleted, the run, step, event, and hook rows stay, the
// payloads in its events go, and its streams are left empty and closed. The
// run's expired_at is when its data is gone, and data_purged records that its
// rows no longer hold it. Reads hide a run's payloads once expired_at has
// passed, so a Config.Retention deadline takes effect before Worker gets to
// it.

// expireLoop deletes the data of runs whose retention has passed every
// expireInterval until ctx is done.
func (w *World) expireLoop(ctx context.Context) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		for {
			n, err := w.expireRuns(ctx)
			if err != nil {
				if ctx.Err() == nil {
					log.Printf("workflow/postgres: expire runs: %v", err)
				}
				break
			}
			if n < expireBatch {
				break
			}
		}
		timer.Reset(expireInterval)
	}
}

// expireRuns deletes the data of up to expireBatch runs whose expired_at has
// passed and whose rows still hold it, in one transaction. Runs another
// worker is purging are skipped. It returns how many runs it purged.
func (w *World) expireRuns(ctx context.Context) (int, error) {
	purged := 0
	err := w.inTx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT run_id FROM go_workflow_runs
			WHERE expired_at <= now() AND NOT data_purged
			ORDER BY expired_at LIMIT $1 FOR UPDATE SKIP LOCKED`, expireBatch)
		if err != nil {
			return err
		}
		ids, err := pgx.CollectRows(rows, pgx.RowTo[worlds.RunID])
		if err != nil {
			return err
		}
		for _, id := range ids {
			if err := purgeRunData(ctx, tx, id); err != nil {
				return err
			}
		}
		purged = len(ids)
		return nil
	})
	return purged, err
}

// retentionFor returns how long finished runs of workflow keep their data,
// or zero to keep it.
func (w *World) retentionFor(workflow string) time.Duration {
	return worlds.RetentionFor(w.retention, w.retentions, workflow)
}

// scrubEventsSQL removes the payload field from the Data of a run's events,
// and a run_created's startHook.metadata, as worlds.ScrubEventData does, and
// leaves events whose Data has nothing else empty.
var scrubEventsSQL = func() string {
	fields := worlds.EventPayloadFields()
	types := make([]string, 0, len(fields))
	for eventType := range fields {
		types = append(types, string(eventType))
	}
	slices.Sort(types)
	var field strings.Builder
	field.WriteString("CASE type")
	for _, eventType := range types {
		field.WriteString(" WHEN '" + eventType + "' THEN '" + fields[worlds.EventType(eventType)] + "'")
	}
	field.WriteString(" END")
	return `WITH events AS (
	SELECT slot, type, convert_from(data, 'UTF8')::jsonb AS data
	FROM go_workflow_events
	WHERE run_id = $1 AND data IS NOT NULL AND type IN ('` + strings.Join(types, "', '") + `')
),
scrubbed AS (
	SELECT slot, (data - (` + field.String() + `)) #- '{startHook,metadata}' AS data
	FROM events
	WHERE data ? (` + field.String() + `) OR (type = '` + string(worlds.EventTypeRunCreated) + `' AND data #> '{startHook,metadata}' IS NOT NULL)
)
UPDATE go_workflow_events e
SET data = CASE WHEN s.data = '{}'::jsonb THEN NULL ELSE convert_to(s.data::text, 'UTF8') END
FROM scrubbed s WHERE e.run_id = $1 AND e.slot = s.slot`
}()

// purgeRunData deletes a run's data in tx, which holds the run's row lock:
// the payloads in its events, which are also its run, step, and hook
// payloads, and its streams' chunks, leaving each stream empty and closed.
// It sets the run's expired_at unless it has already passed. A stream write
// takes the run's row lock to read expired_at, so it waits for the purge.
func purgeRunData(ctx context.Context, tx pgx.Tx, runID worlds.RunID) error {
	if _, err := tx.Exec(ctx, `UPDATE go_workflow_runs SET expired_at = CASE WHEN expired_at <= now() THEN expired_at ELSE now() END, data_purged = true WHERE run_id = $1`, runID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, scrubEventsSQL, runID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM go_workflow_stream_chunks WHERE run_id = $1`, runID); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `UPDATE go_workflow_streams SET chunk_count = 0, closed = true WHERE run_id = $1`, runID)
	return err
}

// runDataExpired reports whether a run's data is gone: its expired_at has
// passed. A run that does not exist has no data to hide.
func (w *World) runDataExpired(ctx context.Context, runID worlds.RunID) (bool, error) {
	var expired bool
	err := w.pool.QueryRow(ctx, `SELECT coalesce(expired_at <= now(), false) FROM go_workflow_runs WHERE run_id = $1`, runID).Scan(&expired)
	if err == pgx.ErrNoRows {
		return false, nil
	}
	return expired, err
}

// runLive is a join condition that holds while the run named by column has
// its data, so payloads joined under it read as NULL once the run's
// expired_at has passed.
func runLive(column string) string {
	return " AND NOT EXISTS (SELECT 1 FROM go_workflow_runs xr WHERE xr.run_id = " + column + " AND xr.expired_at <= now())"
}
