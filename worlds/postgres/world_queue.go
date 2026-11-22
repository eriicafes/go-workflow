package postgresworld

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"sync"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/core"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/oklog/ulid/v2"
)

const (
	// backoffCap is the most failed deliveries the retry delay grows over.
	backoffCap = 10
	// maxJobAttempts is the most deliveries of one job: the core's
	// MAX_DELIVERIES_EXCEEDED delivery (49) plus 24 retries of its terminal
	// write. A row's job starts when it is queued and again on every
	// Redelivery, and a row whose job failed this many deliveries is deleted.
	maxJobAttempts = 49 + 24
	// specVersionSingleOrchestrator is the first spec version whose runs
	// have one orchestrator delivery at a time.
	specVersionSingleOrchestrator = 9
)

// storedDelivery is a queue row's payload: the delivery, and JobStart, the
// row's deliveries when its current job began. deliveries - JobStart is the
// job's attempts.
type storedDelivery struct {
	worlds.QueueInput
	JobStart int `json:"jobStart,omitempty"`
}

// Queue stores a row holding the whole delivery. A delivery with an
// IdempotencyKey is stored once while its message lives: a Queue with the key
// of a run's message that is still queued or executing stores nothing and
// returns that message's ID.
//
// A message does not expire: its row stays until a delivery succeeds or its
// job runs out of attempts, since dropping it would leave its step or flow
// with nothing to run it. QueueInput.Retention is a limit of queues that do
// expire messages, which the runtime keeps a step's retries within.
func (w *World) Queue(ctx context.Context, input worlds.QueueInput) (worlds.QueueResult, error) {
	if err := ctx.Err(); err != nil {
		return worlds.QueueResult{}, err
	}
	input.MessageID, input.Attempt, input.DeliveryCount = "", 0, 0
	// The message is created now, whatever the caller set.
	input.CreatedAt = time.Now().UTC().Truncate(time.Millisecond)
	payload, err := json.Marshal(storedDelivery{QueueInput: input})
	if err != nil {
		return worlds.QueueResult{}, fmt.Errorf("workflow/postgres: queue: %w", err)
	}
	var stepID *string
	if input.StepID != nil {
		value := string(*input.StepID)
		stepID = &value
	}
	for attempt := 1; ; attempt++ {
		var id string
		err := w.inTx(ctx, func(tx pgx.Tx) error {
			id = ""
			var inserted bool
			err := tx.QueryRow(ctx, w.q(`WITH inserted AS (
					INSERT INTO workflow.workflow_queue (id, queue_name, run_id, step_id, idempotency_key, not_before, payload, serialized)
					VALUES ($1, $2, $3, $4, $5, coalesce($6, now()), $7, $8)
					ON CONFLICT (run_id, idempotency_key) WHERE idempotency_key <> '' DO NOTHING
					RETURNING id
				)
				SELECT id, true FROM inserted
				UNION ALL
				SELECT id, false FROM workflow.workflow_queue WHERE $5 <> '' AND run_id = $3 AND idempotency_key = $5
				LIMIT 1`),
				newMessageID(), w.queueName(input), input.RunID, stepID, input.IdempotencyKey, input.NotBefore, payload, input.SerializedFlow()).Scan(&id, &inserted)
			if errors.Is(err, pgx.ErrNoRows) {
				return nil
			}
			if err != nil || !inserted {
				return err
			}
			notify(tx, channelQueue, "")
			return nil
		})
		if err != nil {
			return worlds.QueueResult{}, err
		}
		// No row means the conflicting row was committed or deleted after this
		// statement's snapshot, so try again.
		if id != "" {
			return worlds.QueueResult{MessageID: id}, nil
		}
		if attempt == txAttempts {
			return worlds.QueueResult{}, errors.New("workflow/postgres: queue: idempotency key contention")
		}
	}
}

// QueueBatch queues the items one after another, and reports an item that
// failed in its result.
func (w *World) QueueBatch(ctx context.Context, input worlds.QueueBatchInput) ([]worlds.QueueResult, error) {
	return worlds.QueueEach(ctx, w.Queue, input.Items), nil
}

// executor executes deliveries, as core.Runtime does.
type executor interface {
	Execute(context.Context, worlds.QueueInput) error
}

var _ executor = (*core.Runtime)(nil)

type delivery struct {
	id         string
	deliveries int
	// jobStart is the deliveries when the row's current job began.
	jobStart   int
	serialized bool
	input      worlds.QueueInput
}

// jobAttempts is how many times the row's current job was delivered, this
// delivery included.
func (d delivery) jobAttempts() int { return d.deliveries - d.jobStart }

// newMessageID returns a queue message ID, msg_<ULID>.
func newMessageID() string { return "msg_" + ulid.Make().String() }

// claim leases up to limit due rows that are not leased by a live worker. A
// serialized flow is claimed only while no other serialized flow of its run is running, and at
// most one per run, and the unique index on running flows turns a race
// between two workers into a retry. The deliveries counter doubles as a
// fencing token for later updates.
func (w *World) claim(ctx context.Context, limit int, queues []string) ([]delivery, error) {
	for attempt := 1; ; attempt++ {
		claimed, err := w.claimOnce(ctx, limit, queues)
		var pgErr *pgconn.PgError
		if attempt < txAttempts && errors.As(err, &pgErr) && pgErr.Code == "23505" {
			continue
		}
		if err != nil {
			return nil, err
		}
		return w.dropExhausted(ctx, claimed), nil
	}
}

// dropExhausted deletes the claimed rows whose job has no attempts left,
// as after workers that claimed it crashed, and returns the others. A row
// that fails to delete stays leased, and is dropped when it is claimed again.
func (w *World) dropExhausted(ctx context.Context, claimed []delivery) []delivery {
	kept := claimed[:0]
	for _, item := range claimed {
		if item.jobAttempts() <= maxJobAttempts {
			kept = append(kept, item)
			continue
		}
		log.Printf("workflow/postgres: delivery %s for run %s has no attempts left after %d deliveries, dropping it", item.id, item.input.RunID, item.deliveries)
		if err := w.dropDelivery(ctx, item); err != nil {
			log.Printf("workflow/postgres: drop delivery %s: %v", item.id, err)
		}
	}
	return kept
}

// dropDelivery deletes a claimed row and wakes the workers when it was a
// running flow, which may let another flow of its run be claimed.
func (w *World) dropDelivery(ctx context.Context, item delivery) error {
	if _, err := w.pool.Exec(ctx, w.q(`DELETE FROM workflow.workflow_queue WHERE id = $1 AND deliveries = $2`), item.id, item.deliveries); err != nil {
		return err
	}
	if item.serialized {
		_, err := w.pool.Exec(ctx, `SELECT pg_notify($1, '')`, channelQueue)
		return err
	}
	return nil
}

func (w *World) claimOnce(ctx context.Context, limit int, queues []string) ([]delivery, error) {
	rows, err := w.pool.Query(ctx, w.q(`WITH locked AS (
			SELECT q.id, q.run_id, q.serialized, q.not_before FROM workflow.workflow_queue q
			WHERE q.not_before <= now() AND (q.lease_until IS NULL OR q.lease_until < now())
				AND ($3::text[] IS NULL OR q.queue_name = ANY($3))
				AND (NOT q.serialized OR NOT EXISTS (
					SELECT 1 FROM workflow.workflow_queue r WHERE r.run_id = q.run_id AND r.running AND r.id <> q.id))
			ORDER BY q.not_before, q.id
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		), candidate AS (
			SELECT id FROM locked WHERE NOT serialized
			UNION ALL
			(SELECT DISTINCT ON (run_id) id FROM locked WHERE serialized ORDER BY run_id, not_before, id)
		)
		UPDATE workflow.workflow_queue q
		SET lease_until = now() + make_interval(secs => $2), deliveries = q.deliveries + 1, running = q.serialized
		FROM candidate WHERE q.id = candidate.id
		RETURNING q.id, q.deliveries, q.handoffs, q.serialized, q.queue_name, q.payload`),
		limit, w.lease.Seconds(), queuesArg(queues))
	if err != nil {
		return nil, err
	}
	return collect(rows, func(row pgx.Row) (delivery, error) {
		var value delivery
		var handoffs int
		var queueName string
		var payload []byte
		if err := row.Scan(&value.id, &value.deliveries, &handoffs, &value.serialized, &queueName, &payload); err != nil {
			return delivery{}, err
		}
		var stored storedDelivery
		if err := json.Unmarshal(payload, &stored); err != nil {
			return delivery{}, fmt.Errorf("delivery %s: %w", value.id, err)
		}
		value.input, value.jobStart = stored.QueueInput, stored.JobStart
		// A delivery names the queue it was stored on, its route included.
		value.input.QueueName = queueName
		value.input.MessageID = value.id
		value.input.DeliveryCount = value.deliveries
		value.input.Attempt = value.deliveries - handoffs
		return value, nil
	})
}

// work is the Worker loop. It claims as many due rows as there are free slots,
// executes each in its own goroutine, and sleeps until a notification, a free
// slot, or PollInterval.
func (w *World) work(ctx context.Context, runtime executor, options WorkerOptions) error {
	var group sync.WaitGroup
	defer group.Wait()
	finished := make(chan struct{}, options.Concurrency)
	inflight := 0
	for {
		wake, release := w.notifier.wait(channelQueue, "")
		claimedAll := false
		if free := options.Concurrency - inflight; free > 0 {
			deliveries, err := w.claim(ctx, free, options.Queues)
			if err != nil && ctx.Err() == nil {
				log.Printf("workflow/postgres: claim: %v", err)
			}
			for _, item := range deliveries {
				inflight++
				group.Add(1)
				go func() {
					defer group.Done()
					w.deliver(ctx, runtime, item)
					finished <- struct{}{}
				}()
			}
			claimedAll = len(deliveries) == free
		}
		if claimedAll {
			release()
			continue
		}
		timer := time.NewTimer(w.interval)
		select {
		case <-ctx.Done():
			release()
			for ; inflight > 0; inflight-- {
				<-finished
			}
			return ctx.Err()
		case <-wake:
		case <-timer.C:
		case <-finished:
			inflight--
		}
		release()
		for drained := false; !drained; {
			select {
			case <-finished:
				inflight--
			default:
				drained = true
			}
		}
	}
}

// deliver executes one claimed row, renewing its lease meanwhile. Success
// deletes the row. A Redelivery keeps it, due after its delay, and starts the
// row's next job, counting Attempt as redeliveryHandoffs describes. Failure
// schedules a retry after backoff, until the row's job has
// failed maxJobAttempts deliveries, which deletes it. A shutdown releases the
// lease so another worker takes it now. A lease lost to another worker, as
// after this worker stalled past it, cancels the execution.
func (w *World) deliver(ctx context.Context, runtime executor, item delivery) {
	executeCtx, loseLease := context.WithCancel(ctx)
	defer loseLease()
	renewCtx, stopRenew := context.WithCancel(ctx)
	renewed := make(chan struct{})
	go func() {
		defer close(renewed)
		ticker := time.NewTicker(w.lease / 3)
		for {
			select {
			case <-renewCtx.Done():
				return
			case <-ticker.C:
				tag, err := w.pool.Exec(renewCtx, w.q(`UPDATE workflow.workflow_queue SET lease_until = now() + make_interval(secs => $3) WHERE id = $1 AND deliveries = $2`),
					item.id, item.deliveries, w.lease.Seconds())
				if err != nil && renewCtx.Err() == nil {
					log.Printf("workflow/postgres: renew lease %s: %v", item.id, err)
				}
				if err == nil && tag.RowsAffected() == 0 {
					log.Printf("workflow/postgres: delivery %s lost its lease, cancelling it", item.id)
					loseLease()
					return
				}
			}
		}
	}()
	err := runtime.Execute(executeCtx, item.input)
	stopRenew()
	<-renewed

	finish, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	var updateErr error
	notify := item.serialized
	switch redelivery, redeliver := worlds.RedeliveryOf(err); {
	case err == nil:
		_, updateErr = w.pool.Exec(finish, w.q(`DELETE FROM workflow.workflow_queue WHERE id = $1 AND deliveries = $2`), item.id, item.deliveries)
	case ctx.Err() != nil:
		_, updateErr = w.pool.Exec(finish, w.q(`UPDATE workflow.workflow_queue SET lease_until = NULL, running = false WHERE id = $1 AND deliveries = $2`), item.id, item.deliveries)
	case redeliver:
		// The same row is delivered again after the delay, as the next job
		// of its message.
		if redelivery.After == 0 {
			notify = true
		}
		payload, err := json.Marshal(storedDelivery{QueueInput: item.stored(), JobStart: item.deliveries})
		if err != nil {
			updateErr = err
			break
		}
		_, updateErr = w.pool.Exec(finish, w.q(`UPDATE workflow.workflow_queue SET lease_until = NULL, running = false, not_before = now() + make_interval(secs => $3), handoffs = handoffs + $4, payload = $5 WHERE id = $1 AND deliveries = $2`),
			item.id, item.deliveries, redelivery.After.Seconds(), redeliveryHandoffs(item, redelivery.After), payload)
	case item.jobAttempts() >= maxJobAttempts:
		log.Printf("workflow/postgres: delivery %s for run %s failed (attempt %d), and has no attempts left, dropping it: %v", item.id, item.input.RunID, item.input.Attempt, err)
		updateErr = w.dropDelivery(finish, item)
		notify = false
	default:
		delay := worlds.DeliveryRetryDelay(err, backoff(item.jobAttempts()))
		log.Printf("workflow/postgres: delivery %s for run %s failed (attempt %d, retry in %v): %v", item.id, item.input.RunID, item.input.Attempt, delay, err)
		_, updateErr = w.pool.Exec(finish, w.q(`UPDATE workflow.workflow_queue SET lease_until = NULL, running = false, not_before = now() + make_interval(secs => $3) WHERE id = $1 AND deliveries = $2`),
			item.id, item.deliveries, delay.Seconds())
	}
	// A finished flow may let another flow of its run be claimed, and a
	// handoff is due now, so wake the workers.
	if updateErr == nil && notify {
		_, updateErr = w.pool.Exec(finish, `SELECT pg_notify($1, '')`, channelQueue)
	}
	if updateErr != nil {
		log.Printf("workflow/postgres: settle delivery %s: %v", item.id, updateErr)
	}
}

// redeliveryHandoffs returns how many of a redelivered row's deliveries are
// added to those not counted in Attempt, which sets its next delivery's
// Attempt. A handoff (a Redelivery of zero of a spec 9 run's message) keeps
// Attempt, and a spec 9 run's flow redelivered after a delay is a counted
// redelivery, which adds one to it. Any other Redelivery, as a step's
// in-place retry, starts Attempt over from where the row's job started it, so
// a step retried in place many times on one message is not failed by the
// core's MaxDeliveries.
func redeliveryHandoffs(item delivery, after time.Duration) int {
	single := item.input.RunSpecVersion >= specVersionSingleOrchestrator
	switch {
	case after == 0 && single:
		return 1
	case single && item.input.SerializedFlow():
		return 0
	default:
		return item.jobAttempts()
	}
}

// stored returns the delivery as Queue stored it, without what claim set.
func (d delivery) stored() worlds.QueueInput {
	input := d.input
	input.MessageID, input.Attempt, input.DeliveryCount = "", 0, 0
	return input
}

// backoff returns the retry delay after a job's given number of failed
// deliveries: e^attempts seconds, growing over the first backoffCap, so 2.7s,
// 7.4s, 20s, 55s, ... up to about 6 hours. The 24 deliveries a job has past
// the runtime's MaxDeliveries failure then retry its terminal write for about
// 6 days.
func backoff(attempts int) time.Duration {
	attempts = min(max(attempts, 1), backoffCap)
	return time.Duration(math.Exp(float64(attempts)) * float64(time.Second))
}

// queueName returns the queue a delivery is stored on: its workflow's route,
// or the queue it names.
func (w *World) queueName(input worlds.QueueInput) string {
	if queue, ok := w.routes[input.WorkflowName]; ok {
		return queue
	}
	return input.QueueName
}

// queuesArg returns the claim query's queue filter, nil for every queue.
func queuesArg(queues []string) []string {
	if len(queues) == 0 {
		return nil
	}
	return queues
}
