package localworld

import (
	"container/heap"
	"context"
	"log"
	"path"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/eriicafes/go-workflow/worlds"
)

const (
	retryBase = 100 * time.Millisecond
	retryMax  = 30 * time.Second
)

// queueRecord is one durable delivery. It is deleted after the runtime
// handles it successfully.
type queueRecord struct {
	ID        string            `json:"id"`
	Input     worlds.QueueInput `json:"input"`
	CreatedAt time.Time         `json:"createdAt"`
}

// queueKeyRecord deduplicates IdempotencyKey per run until delivery starts.
type queueKeyRecord struct {
	MessageID string `json:"messageId"`
}

// Queue deduplicates by IdempotencyKey. While a delivery with the same run and
// key has not started, it returns that delivery's message ID instead of
// queueing another.
func (w *World) Queue(ctx context.Context, input worlds.QueueInput) (worlds.QueueResult, error) {
	if err := ctx.Err(); err != nil {
		return worlds.QueueResult{}, err
	}
	record, created, err := w.enqueue(input)
	if err != nil {
		return worlds.QueueResult{}, err
	}
	if created {
		w.schedule(delivery{at: notBefore(record.Input), id: record.ID})
	}
	return worlds.QueueResult{MessageID: record.ID}, nil
}

func (w *World) enqueue(input worlds.QueueInput) (queueRecord, bool, error) {
	unlock, err := w.writeLock()
	if err != nil {
		return queueRecord{}, false, err
	}
	defer unlock()
	if input.IdempotencyKey != "" {
		var key queueKeyRecord
		ok, err := w.readOptional(queueKeyPath(input.RunID, input.IdempotencyKey), &key)
		if err != nil {
			return queueRecord{}, false, err
		}
		if ok {
			if pending, err := exists(w.root, queuePath(key.MessageID)); err != nil || pending {
				return queueRecord{ID: key.MessageID, Input: input}, false, err
			}
		}
	}
	record := queueRecord{ID: ulid.Make().String(), Input: input, CreatedAt: time.Now().UTC()}
	if err := writeJSON(w.root, queuePath(record.ID), record); err != nil {
		return queueRecord{}, false, err
	}
	if input.IdempotencyKey != "" {
		if err := writeJSON(w.root, queueKeyPath(input.RunID, input.IdempotencyKey), queueKeyRecord{MessageID: record.ID}); err != nil {
			return queueRecord{}, false, err
		}
	}
	return record, true, nil
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
	// Deleting the record is enough, because a scheduled timer for it finds no
	// record and is skipped.
	if err := ctx.Err(); err != nil {
		return err
	}
	unlock, err := w.writeLock()
	if err != nil {
		return err
	}
	defer unlock()
	record, ok, err := w.readQueue(input.MessageID)
	if err != nil {
		return err
	}
	if !ok {
		return worlds.ErrQueueMessageNotFound
	}
	if err := w.removeQueueKey(record); err != nil {
		return err
	}
	return remove(w.root, queuePath(record.ID))
}

func (w *World) readQueue(id string) (queueRecord, bool, error) {
	if id == "" || strings.ContainsAny(id, `/\.`) {
		return queueRecord{}, false, nil
	}
	var record queueRecord
	ok, err := w.readOptional(queuePath(id), &record)
	return record, ok, err
}

// removeQueueKey deletes the record's idempotency key if it still names it.
func (w *World) removeQueueKey(record queueRecord) error {
	if record.Input.IdempotencyKey == "" {
		return nil
	}
	name := queueKeyPath(record.Input.RunID, record.Input.IdempotencyKey)
	var key queueKeyRecord
	ok, err := w.readOptional(name, &key)
	if err != nil || !ok || key.MessageID != record.ID {
		return err
	}
	return remove(w.root, name)
}

func (w *World) loadQueueLocked() ([]delivery, error) {
	names, err := listJSON(w.root, "queue")
	if err != nil {
		return nil, err
	}
	deliveries := make([]delivery, 0, len(names))
	for _, name := range names {
		var record queueRecord
		if err := readJSON(w.root, path.Join("queue", name), &record); err != nil {
			return nil, err
		}
		deliveries = append(deliveries, delivery{at: notBefore(record.Input), id: record.ID})
	}
	return deliveries, nil
}

type delivery struct {
	at      time.Time
	id      string
	attempt int
}

type deliveryHeap []delivery

func (h deliveryHeap) Len() int           { return len(h) }
func (h deliveryHeap) Less(i, j int) bool { return h[i].at.Before(h[j].at) }
func (h deliveryHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *deliveryHeap) Push(x any)        { *h = append(*h, x.(delivery)) }
func (h *deliveryHeap) Pop() any {
	old := *h
	value := old[len(old)-1]
	*h = old[:len(old)-1]
	return value
}

func (w *World) schedule(value delivery) {
	w.queueMu.Lock()
	heap.Push(&w.pending, value)
	w.queueMu.Unlock()
	select {
	case w.queueWake <- struct{}{}:
	default:
	}
}

// due pops every delivery ready at now and returns the next pending time.
func (w *World) due(now time.Time) ([]delivery, time.Time) {
	w.queueMu.Lock()
	defer w.queueMu.Unlock()
	var ready []delivery
	for w.pending.Len() > 0 && !w.pending[0].at.After(now) {
		ready = append(ready, heap.Pop(&w.pending).(delivery))
	}
	if w.pending.Len() == 0 {
		return ready, time.Time{}
	}
	return ready, w.pending[0].at
}

// dispatch runs one timer for the earliest pending delivery and starts each
// due delivery in its own goroutine.
func (w *World) dispatch() {
	defer w.workers.Done()
	timer := time.NewTimer(time.Hour)
	for {
		ready, next := w.due(time.Now())
		for _, value := range ready {
			w.workers.Add(1)
			go w.deliver(value)
		}
		var wait <-chan time.Time
		if !next.IsZero() {
			timer.Reset(time.Until(next))
			wait = timer.C
		}
		select {
		case <-w.ctx.Done():
			return
		case <-w.queueWake:
		case <-wait:
		}
	}
}

// deliver executes one delivery. Its idempotency key is released first so a
// delivery queued while this one runs is not deduplicated away.
func (w *World) deliver(value delivery) {
	defer w.workers.Done()
	record, ok, err := w.startDelivery(value.id)
	if err == nil && !ok {
		return
	}
	if err == nil {
		input := record.Input
		input.MessageID = value.id
		err = w.runtime.Execute(w.ctx, input)
	}
	if err == nil {
		err = w.ackDelivery(value.id)
	}
	if err == nil || w.ctx.Err() != nil {
		return
	}
	value.attempt++
	backoff := min(retryBase<<min(value.attempt-1, 20), retryMax)
	value.at = time.Now().Add(backoff)
	log.Printf("workflow/local: delivery %s failed (attempt %d), retrying in %s: %v", value.id, value.attempt, backoff, err)
	w.schedule(value)
}

func (w *World) startDelivery(id string) (queueRecord, bool, error) {
	unlock, err := w.writeLock()
	if err != nil {
		return queueRecord{}, false, err
	}
	defer unlock()
	record, ok, err := w.readQueue(id)
	if err != nil || !ok {
		return queueRecord{}, false, err
	}
	return record, true, w.removeQueueKey(record)
}

func (w *World) ackDelivery(id string) error {
	unlock, err := w.writeLock()
	if err != nil {
		return err
	}
	defer unlock()
	return remove(w.root, queuePath(id))
}

func queuePath(id string) string { return path.Join("queue", id+".json") }

func queueKeyPath(runID worlds.RunID, key string) string {
	return path.Join("queue-keys", fileKey(string(runID)+"\x00"+key)+".json")
}

func notBefore(input worlds.QueueInput) time.Time {
	if input.NotBefore == nil {
		return time.Time{}
	}
	return *input.NotBefore
}
