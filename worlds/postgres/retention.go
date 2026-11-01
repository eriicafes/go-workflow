package postgresworld

import (
	"context"
	"log"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

const (
	// expireInterval is how often Worker deletes expired runs.
	expireInterval = time.Minute
	// expireBatch is the most runs one statement deletes.
	expireBatch = 100
)

// expireLoop deletes expired runs every expireInterval until ctx is done.
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
			n, err := w.expireRuns(ctx, time.Now())
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

// expireRuns deletes up to expireBatch runs whose workflow's retention has
// passed by now, with everything recorded for them, in one statement. Runs
// another worker is deleting are skipped, and so are runs with a hook whose
// token retention has not ended. It returns how many runs it deleted.
func (w *World) expireRuns(ctx context.Context, now time.Time) (int, error) {
	workflows := make([]string, 0, len(w.retentions))
	micros := make([]int64, 0, len(w.retentions))
	for workflow, retention := range w.retentions {
		workflows = append(workflows, workflow)
		micros = append(micros, retention.Microseconds())
	}
	tag, err := w.pool.Exec(ctx, `
WITH policy AS (
	SELECT * FROM unnest($4::text[], $5::bigint[]) AS p(workflow_name, micros)
),
expired AS (
	SELECT r.run_id FROM go_workflow_runs r
	LEFT JOIN policy p ON p.workflow_name = r.workflow_name
	WHERE r.status = ANY($1) AND r.completed_at IS NOT NULL
		AND coalesce(p.micros, $3) > 0
		AND r.completed_at < $2::timestamptz - coalesce(p.micros, $3) * interval '1 microsecond'
		AND NOT EXISTS (
			SELECT 1 FROM go_workflow_hooks h
			WHERE h.run_id = r.run_id AND h.token_retention_until > now()
		)
	ORDER BY r.completed_at
	LIMIT $6
	FOR UPDATE OF r SKIP LOCKED
),
hooks AS (DELETE FROM go_workflow_hooks WHERE run_id IN (SELECT run_id FROM expired) RETURNING hook_id),
tokens AS (DELETE FROM go_workflow_hook_tokens WHERE hook_id IN (SELECT hook_id FROM hooks)),
events AS (DELETE FROM go_workflow_events WHERE run_id IN (SELECT run_id FROM expired)),
steps AS (DELETE FROM go_workflow_steps WHERE run_id IN (SELECT run_id FROM expired)),
queue AS (DELETE FROM go_workflow_queue WHERE run_id IN (SELECT run_id FROM expired)),
streams AS (DELETE FROM go_workflow_streams WHERE run_id IN (SELECT run_id FROM expired)),
chunks AS (DELETE FROM go_workflow_stream_chunks WHERE run_id IN (SELECT run_id FROM expired))
DELETE FROM go_workflow_runs WHERE run_id IN (SELECT run_id FROM expired)`,
		[]string{string(worlds.StatusCompleted), string(worlds.StatusFailed), string(worlds.StatusCancelled)},
		now, w.retention.Microseconds(), workflows, micros, expireBatch)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}
