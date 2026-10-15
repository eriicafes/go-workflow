package postgresworld

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
	"github.com/jackc/pgx/v5"
	"github.com/oklog/ulid/v2"
)

const (
	backoffBase = time.Second
	backoffMax  = 5 * time.Minute
)

// Queue inserts a delivery row and notifies workers on commit. A non-empty
// IdempotencyKey deduplicates against an unclaimed row of the same run; once
// a worker claims a row its key is cleared, so a later Queue is never lost
// to a delivery that may already have read older state.
func (w *World) Queue(ctx context.Context, input worlds.QueueInput) (worlds.QueueResult, error) {
	if err := ctx.Err(); err != nil {
		return worlds.QueueResult{}, err
	}
	var dedupe *string
	if input.IdempotencyKey != "" {
		dedupe = &input.IdempotencyKey
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
			err := tx.QueryRow(ctx, `WITH inserted AS (
					INSERT INTO go_workflow_queue (id, queue_name, run_id, step_id, idempotency_key, dedupe_key, not_before)
					VALUES ($1, $2, $3, $4, $5, $6, coalesce($7, now()))
					ON CONFLICT (run_id, dedupe_key) WHERE dedupe_key IS NOT NULL DO NOTHING
					RETURNING id
				)
				SELECT id, true FROM inserted
				UNION ALL
				SELECT id, false FROM go_workflow_queue WHERE run_id = $3 AND dedupe_key = $6
				LIMIT 1`,
				ulid.Make().String(), input.QueueName, input.RunID, stepID, input.IdempotencyKey, dedupe, input.NotBefore).Scan(&id, &inserted)
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
		// No row means the conflicting row was committed or claimed after this
		// statement's snapshot; try again.
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

func (w *World) QueueCancel(ctx context.Context, input worlds.QueueCancelInput) error {
	tag, err := w.pool.Exec(ctx, `DELETE FROM go_workflow_queue WHERE id = $1`, input.MessageID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return worlds.ErrQueueMessageNotFound
	}
	return nil
}

type delivery struct {
	id       string
	attempts int
	input    worlds.QueueInput
}

// claim leases up to limit due rows that are not leased by a live worker.
// The attempts counter doubles as a fencing token for later updates.
func (w *World) claim(ctx context.Context, limit int) ([]delivery, error) {
	rows, err := w.pool.Query(ctx, `WITH candidate AS (
			SELECT id FROM go_workflow_queue
			WHERE not_before <= now() AND (lease_until IS NULL OR lease_until < now())
			ORDER BY not_before, id
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE go_workflow_queue q
		SET lease_until = now() + make_interval(secs => $2), attempts = q.attempts + 1, dedupe_key = NULL
		FROM candidate WHERE q.id = candidate.id
		RETURNING q.id, q.attempts, q.queue_name, q.run_id, q.step_id, q.idempotency_key`,
		limit, w.lease.Seconds())
	if err != nil {
		return nil, err
	}
	return collect(rows, func(row pgx.Row) (delivery, error) {
		var value delivery
		var stepID *string
		if err := row.Scan(&value.id, &value.attempts, &value.input.QueueName, &value.input.RunID, &stepID, &value.input.IdempotencyKey); err != nil {
			return delivery{}, err
		}
		if stepID != nil {
			id := worlds.StepID(*stepID)
			value.input.StepID = &id
		}
		return value, nil
	})
}

// work is the Start loop: claim as many due rows as there are free slots,
// execute each in its own goroutine, and sleep until a notification, a free
// slot, or PollInterval.
func (w *World) work(ctx context.Context, runtime *worlds.Runtime) error {
	var group sync.WaitGroup
	defer group.Wait()
	finished := make(chan struct{}, w.concurrency)
	inflight := 0
	for {
		wake, release := w.notifier.wait(channelQueue, "")
		claimedAll := false
		if free := w.concurrency - inflight; free > 0 {
			deliveries, err := w.claim(ctx, free)
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
			timer.Stop()
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
		timer.Stop()
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
// deletes the row; failure schedules a retry with capped exponential
// backoff; a shutdown releases the lease so another worker takes it now.
func (w *World) deliver(ctx context.Context, runtime *worlds.Runtime, item delivery) {
	renewCtx, stopRenew := context.WithCancel(ctx)
	renewed := make(chan struct{})
	go func() {
		defer close(renewed)
		ticker := time.NewTicker(w.lease / 3)
		defer ticker.Stop()
		for {
			select {
			case <-renewCtx.Done():
				return
			case <-ticker.C:
				if _, err := w.pool.Exec(renewCtx, `UPDATE go_workflow_queue SET lease_until = now() + make_interval(secs => $3) WHERE id = $1 AND attempts = $2`,
					item.id, item.attempts, w.lease.Seconds()); err != nil && renewCtx.Err() == nil {
					log.Printf("workflow/postgres: renew lease %s: %v", item.id, err)
				}
			}
		}
	}()
	err := runtime.Execute(ctx, item.input)
	stopRenew()
	<-renewed

	finish, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	var updateErr error
	switch {
	case err == nil:
		_, updateErr = w.pool.Exec(finish, `DELETE FROM go_workflow_queue WHERE id = $1 AND attempts = $2`, item.id, item.attempts)
	case ctx.Err() != nil:
		_, updateErr = w.pool.Exec(finish, `UPDATE go_workflow_queue SET lease_until = NULL WHERE id = $1 AND attempts = $2`, item.id, item.attempts)
	default:
		delay := backoff(item.attempts)
		log.Printf("workflow/postgres: delivery %s for run %s failed (attempt %d, retry in %v): %v", item.id, item.input.RunID, item.attempts, delay, err)
		_, updateErr = w.pool.Exec(finish, `UPDATE go_workflow_queue SET lease_until = NULL, not_before = now() + make_interval(secs => $3) WHERE id = $1 AND attempts = $2`,
			item.id, item.attempts, delay.Seconds())
	}
	if updateErr != nil {
		log.Printf("workflow/postgres: settle delivery %s: %v", item.id, updateErr)
	}
}

// backoff returns the retry delay after the given number of attempts:
// 1s, 2s, 4s, ... capped at 5 minutes.
func backoff(attempts int) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	if attempts > 20 {
		return backoffMax
	}
	return min(backoffBase<<(attempts-1), backoffMax)
}
