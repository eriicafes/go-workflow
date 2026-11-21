package postgresworld

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/core"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/oklog/ulid/v2"
)

const (
	backoffBase = time.Second
	backoffMax  = 5 * time.Minute
)

// Queue stores a row holding the whole delivery. A delivery with an
// IdempotencyKey is stored once while its message lives: a Queue with the key
// of a run's message that is still queued or executing stores nothing and
// returns that message's ID.
func (w *World) Queue(ctx context.Context, input worlds.QueueInput) (worlds.QueueResult, error) {
	if err := ctx.Err(); err != nil {
		return worlds.QueueResult{}, err
	}
	input.MessageID, input.Attempt, input.DeliveryCount = "", 0, 0
	payload, err := json.Marshal(input)
	if err != nil {
		return worlds.QueueResult{}, fmt.Errorf("workflow/postgres: queue: %w", err)
	}
	var stepID *string
	if input.StepID != nil {
		value := string(*input.StepID)
		stepID = &value
	}
	var expiresIn *float64
	if input.Retention > 0 {
		seconds := input.Retention.Seconds()
		expiresIn = &seconds
	}
	for attempt := 1; ; attempt++ {
		var id string
		err := w.inTx(ctx, func(tx pgx.Tx) error {
			id = ""
			var inserted bool
			err := tx.QueryRow(ctx, `WITH inserted AS (
					INSERT INTO go_workflow_queue (id, queue_name, run_id, step_id, idempotency_key, not_before, payload, serialized, expires_at)
					VALUES ($1, $2, $3, $4, $5, coalesce($6, now()), $7, $8, now() + make_interval(secs => $9))
					ON CONFLICT (run_id, idempotency_key) WHERE idempotency_key <> '' DO NOTHING
					RETURNING id
				)
				SELECT id, true FROM inserted
				UNION ALL
				SELECT id, false FROM go_workflow_queue WHERE $5 <> '' AND run_id = $3 AND idempotency_key = $5
				LIMIT 1`,
				ulid.Make().String(), w.queueName(input), input.RunID, stepID, input.IdempotencyKey, input.NotBefore, payload, input.SerializedFlow(), expiresIn).Scan(&id, &inserted)
			if errors.Is(err, pgx.ErrNoRows) {
				return nil
			}
			if err != nil || !inserted {
				return err
			}
			_, err = tx.Exec(ctx, `SELECT pg_notify($1, '')`, channelQueue)
			return err
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

func (w *World) QueueBatch(ctx context.Context, input worlds.QueueBatchInput) ([]worlds.QueueResult, error) {
	results := make([]worlds.QueueResult, 0, len(input.Items))
	for _, item := range input.Items {
		result, err := w.Queue(ctx, item)
		if err != nil {
			return results, err
		}
		results = append(results, result)
	}
	return results, nil
}

// executor executes deliveries, as core.Runtime does.
type executor interface {
	Execute(context.Context, worlds.QueueInput) error
}

var _ executor = (*core.Runtime)(nil)

type delivery struct {
	id         string
	deliveries int
	serialized bool
	input      worlds.QueueInput
}

// claim leases up to limit due rows that are not leased by a live worker,
// after deleting the rows whose message expired. A serialized flow is
// claimed only while no other serialized flow of its run is running, and at
// most one per run, and the unique index on running flows turns a race
// between two workers into a retry. The deliveries counter doubles as a
// fencing token for later updates.
func (w *World) claim(ctx context.Context, limit int, queues []string) ([]delivery, error) {
	if _, err := w.pool.Exec(ctx, `DELETE FROM go_workflow_queue WHERE expires_at < now() AND (lease_until IS NULL OR lease_until < now())`); err != nil {
		return nil, err
	}
	for attempt := 1; ; attempt++ {
		claimed, err := w.claimOnce(ctx, limit, queues)
		var pgErr *pgconn.PgError
		if attempt < txAttempts && errors.As(err, &pgErr) && pgErr.Code == "23505" {
			continue
		}
		return claimed, err
	}
}

func (w *World) claimOnce(ctx context.Context, limit int, queues []string) ([]delivery, error) {
	rows, err := w.pool.Query(ctx, `WITH locked AS (
			SELECT q.id, q.run_id, q.serialized, q.not_before FROM go_workflow_queue q
			WHERE q.not_before <= now() AND (q.lease_until IS NULL OR q.lease_until < now())
				AND (q.expires_at IS NULL OR q.expires_at >= now())
				AND ($3::text[] IS NULL OR q.queue_name = ANY($3))
				AND (NOT q.serialized OR NOT EXISTS (
					SELECT 1 FROM go_workflow_queue r WHERE r.run_id = q.run_id AND r.running AND r.id <> q.id))
			ORDER BY q.not_before, q.id
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		), candidate AS (
			SELECT id FROM locked WHERE NOT serialized
			UNION ALL
			(SELECT DISTINCT ON (run_id) id FROM locked WHERE serialized ORDER BY run_id, not_before, id)
		)
		UPDATE go_workflow_queue q
		SET lease_until = now() + make_interval(secs => $2), deliveries = q.deliveries + 1, running = q.serialized
		FROM candidate WHERE q.id = candidate.id
		RETURNING q.id, q.deliveries, q.handoffs, q.serialized, q.queue_name, q.payload`,
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
		if err := json.Unmarshal(payload, &value.input); err != nil {
			return delivery{}, fmt.Errorf("delivery %s: %w", value.id, err)
		}
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
// deletes the row. A Redelivery keeps it, due after its delay, and a handoff
// (a Redelivery of zero) is not counted as an attempt. Failure schedules a
// retry with capped exponential backoff. A shutdown releases the lease so another worker takes it now. A
// lease lost to another worker, as after this worker stalled past it,
// cancels the execution.
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
				tag, err := w.pool.Exec(renewCtx, `UPDATE go_workflow_queue SET lease_until = now() + make_interval(secs => $3) WHERE id = $1 AND deliveries = $2`,
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
		_, updateErr = w.pool.Exec(finish, `DELETE FROM go_workflow_queue WHERE id = $1 AND deliveries = $2`, item.id, item.deliveries)
	case ctx.Err() != nil:
		_, updateErr = w.pool.Exec(finish, `UPDATE go_workflow_queue SET lease_until = NULL, running = false WHERE id = $1 AND deliveries = $2`, item.id, item.deliveries)
	case redeliver:
		// The same row is delivered again after the delay, and a handoff
		// does not count as an attempt.
		handoff := 0
		if redelivery.After == 0 {
			handoff, notify = 1, true
		}
		_, updateErr = w.pool.Exec(finish, `UPDATE go_workflow_queue SET lease_until = NULL, running = false, not_before = now() + make_interval(secs => $3), handoffs = handoffs + $4 WHERE id = $1 AND deliveries = $2`,
			item.id, item.deliveries, redelivery.After.Seconds(), handoff)
	default:
		delay := worlds.DeliveryRetryDelay(err, backoff(item.input.Attempt))
		log.Printf("workflow/postgres: delivery %s for run %s failed (attempt %d, retry in %v): %v", item.id, item.input.RunID, item.input.Attempt, delay, err)
		_, updateErr = w.pool.Exec(finish, `UPDATE go_workflow_queue SET lease_until = NULL, running = false, not_before = now() + make_interval(secs => $3) WHERE id = $1 AND deliveries = $2`,
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

// backoff returns the retry delay after the given number of attempts, which
// is 1s, 2s, 4s, ... capped at 5 minutes.
func backoff(attempts int) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	if attempts > 20 {
		return backoffMax
	}
	return min(backoffBase<<(attempts-1), backoffMax)
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
