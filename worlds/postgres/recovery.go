package postgresworld

import (
	"context"
	"log"

	"github.com/eriicafes/go-workflow/worlds"
	"github.com/jackc/pgx/v5"
)

// specVersionLegacy is the spec version of runs from before spec versions
// were recorded, which recovery never treats as parked.
const specVersionLegacy = 1

// recoveryRun is an active run startup recovery queues a flow for.
type recoveryRun struct {
	runID        worlds.RunID
	workflowName string
	deploymentID string
	specVersion  int
}

// recoverActiveRuns queues a flow for every active run when the World runs
// workers, so a run whose delivery was lost, as when its process exited
// before it queued the run's first delivery, resumes. The queue persists, so
// the flow is not a Recovery flow: the run's queued messages still arrive.
//
// A run parked on a hook or a wait is left out (see recoverableRuns), and
// each run's flow is keyed startup-recovery:<run ID>, so a start while an
// earlier start's flow is still queued or executing queues nothing more.
func (w *World) recoverActiveRuns(ctx context.Context) error {
	runs, err := w.recoverableRuns(ctx)
	if err != nil {
		return err
	}
	for _, run := range runs {
		flow := worlds.QueueInput{
			RunID:          run.runID,
			WorkflowName:   run.workflowName,
			DeploymentID:   run.deploymentID,
			RunSpecVersion: run.specVersion,
			IdempotencyKey: "startup-recovery:" + string(run.runID),
		}
		if _, err := w.Queue(ctx, flow); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			log.Printf("workflow/postgres: failed to re-enqueue run %s: %v", run.runID, err)
		}
	}
	return nil
}

// recoverableRuns lists the pending and running runs startup recovery queues
// a flow for. It leaves out a running run that is parked: one of spec 2 or
// later with a hook or an open wait, no pending or running step, no open wait
// that is due, and no wake pending. Its next wake is a hook delivery or a
// wait timer that queues its own flow, so replaying it on every start only
// adds load.
//
// The queue keeps a pending wake and a delivery that may have been cut off as
// rows: a flow of the run that is due or leased is a wake pending or a
// delivery in flight, and lists the run. A wait timer that is not due yet is
// neither.
func (w *World) recoverableRuns(ctx context.Context) ([]recoveryRun, error) {
	rows, err := w.pool.Query(ctx, w.q(`SELECT r.id, r.name, r.deployment_id, r.spec_version FROM workflow.workflow_runs r
		WHERE r.status IN ('pending', 'running')
			AND NOT (
				r.status = 'running'
				AND r.spec_version > $1
				AND (EXISTS (SELECT 1 FROM workflow.workflow_hooks h WHERE h.run_id = r.id)
					OR EXISTS (SELECT 1 FROM workflow.workflow_waits wt WHERE wt.run_id = r.id AND wt.status = 'waiting'))
				AND NOT EXISTS (SELECT 1 FROM workflow.workflow_steps s WHERE s.run_id = r.id AND s.status IN ('pending', 'running'))
				AND NOT EXISTS (SELECT 1 FROM workflow.workflow_waits wt WHERE wt.run_id = r.id AND wt.status = 'waiting' AND wt.resume_at <= now())
				AND NOT EXISTS (SELECT 1 FROM workflow.workflow_queue q WHERE q.run_id = r.id AND q.serialized
					AND (q.not_before <= now() OR q.lease_until IS NOT NULL))
			)
		ORDER BY r.id`), specVersionLegacy)
	if err != nil {
		return nil, err
	}
	return collect(rows, func(row pgx.Row) (recoveryRun, error) {
		var run recoveryRun
		var runID string
		err := row.Scan(&runID, &run.workflowName, &run.deploymentID, &run.specVersion)
		run.runID = worlds.RunID(runID)
		return run, err
	})
}
