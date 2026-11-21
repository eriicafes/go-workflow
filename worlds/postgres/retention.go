package postgresworld

import (
	"bytes"
	"context"
	"log"
	"slices"
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
		rows, err := tx.Query(ctx, w.q(`SELECT id FROM workflow.workflow_runs
			WHERE expired_at <= now() AND NOT data_purged
			ORDER BY expired_at LIMIT $1 FOR UPDATE SKIP LOCKED`), expireBatch)
		if err != nil {
			return err
		}
		ids, err := pgx.CollectRows(rows, pgx.RowTo[worlds.RunID])
		if err != nil {
			return err
		}
		for _, id := range ids {
			if err := w.purgeRunData(ctx, tx, id); err != nil {
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

// purgeRunData deletes a run's data in tx, which holds the run's row lock:
// the payloads in its events, which are also its run, step, and hook
// payloads, and its streams' chunks, leaving each stream empty and closed.
// It sets the run's expired_at unless it has already passed. The run's row
// is updated first, so a stream write, which takes the run's row lock to read
// expired_at, waits for the purge.
func (w *World) purgeRunData(ctx context.Context, tx pgx.Tx, runID worlds.RunID) error {
	if _, err := tx.Exec(ctx, w.q(`UPDATE workflow.workflow_runs SET expired_at = CASE WHEN expired_at <= now() THEN expired_at ELSE now() END, data_purged = true WHERE id = $1`), runID); err != nil {
		return err
	}
	// Event data is CBOR, so its payloads are scrubbed here rather than in
	// SQL, keeping the metadata worlds.ScrubEventData keeps.
	fields := worlds.EventPayloadFields()
	types := make([]string, 0, len(fields))
	for eventType := range fields {
		types = append(types, string(eventType))
	}
	slices.Sort(types)
	rows, err := tx.Query(ctx, w.q(`SELECT `+eventColumns+` FROM workflow.workflow_events WHERE run_id = $1 AND payload_cbor IS NOT NULL AND type = ANY($2) FOR UPDATE`), runID, types)
	if err != nil {
		return err
	}
	events, err := collect(rows, scanEvent)
	if err != nil {
		return err
	}
	for _, event := range events {
		scrubbed := worlds.ScrubEventData(event.EventType, event.Data)
		if bytes.Equal(scrubbed, event.Data) {
			continue
		}
		data, err := worlds.EncodeEventData(event.EventType, scrubbed)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, w.q(`UPDATE workflow.workflow_events SET payload_cbor = $3, resume_payload_digest = NULL WHERE run_id = $1 AND id = $2`), runID, event.EventID, data); err != nil {
			return err
		}
	}
	return w.purgeStreams(ctx, tx, runID)
}

// runDataExpired reports whether a run's data is gone: its expired_at has
// passed. A run that does not exist has no data to hide.
func (w *World) runDataExpired(ctx context.Context, runID worlds.RunID) (bool, error) {
	var expired bool
	err := w.pool.QueryRow(ctx, w.q(`SELECT coalesce(expired_at <= now(), false) FROM workflow.workflow_runs WHERE id = $1`), runID).Scan(&expired)
	if err == pgx.ErrNoRows {
		return false, nil
	}
	return expired, err
}

// runLive is a join condition that holds while the run named by column has
// its data, so payloads joined under it read as NULL once the run's
// expired_at has passed.
func runLive(column string) string {
	return " AND NOT EXISTS (SELECT 1 FROM workflow.workflow_runs xr WHERE xr.id = " + column + " AND xr.expired_at <= now())"
}
