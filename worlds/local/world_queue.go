package localworld

import (
	"container/heap"
	"context"
	"log"
	"path"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/eriicafes/go-workflow/worlds"
)

const (
	retryBase = 100 * time.Millisecond
	retryMax  = 30 * time.Second
)

// queueRecord is one durable message. It lives until a delivery of it
// succeeds, and is redelivered, with the same ID, until then.
type queueRecord struct {
	ID        string            `json:"id"`
	Input     worlds.QueueInput `json:"input"`
	CreatedAt time.Time         `json:"createdAt"`
	// Deliveries counts the deliveries that have started, and Handoffs the
	// redeliveries of zero among them, which are not attempts.
	Deliveries int `json:"deliveries,omitempty"`
	Handoffs   int `json:"handoffs,omitempty"`
	// DueAt is when the message is next delivered after a failed or
	// redelivered delivery. Nil uses Input.NotBefore.
	DueAt *time.Time `json:"dueAt,omitempty"`
}

// queueKeyRecord names the live message of an IdempotencyKey.
type queueKeyRecord struct {
	Key       string `json:"key"`
	MessageID string `json:"messageId"`
}

// Queue deduplicates by IdempotencyKey. While a message with the same key
// lives (it has not been delivered successfully), it returns that message's ID
// instead of storing another.
func (w *World) Queue(ctx context.Context, input worlds.QueueInput) (worlds.QueueResult, error) {
	if err := ctx.Err(); err != nil {
		return worlds.QueueResult{}, err
	}
	record, created, err := w.enqueue(input)
	if err != nil {
		return worlds.QueueResult{}, err
	}
	if created {
		w.schedule(delivery{at: record.due(), id: record.ID})
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
		ok, err := w.readOptional(queueKeyPath(input.IdempotencyKey), &key)
		if err != nil {
			return queueRecord{}, false, err
		}
		if ok && key.Key == input.IdempotencyKey {
			if live, err := exists(w.root, queuePath(key.MessageID)); err != nil || live {
				return queueRecord{ID: key.MessageID, Input: input}, false, err
			}
		}
	}
	// The World sets the delivery fields on each delivery.
	input.MessageID, input.Attempt, input.DeliveryCount = "", 0, 0
	record := queueRecord{ID: ulid.Make().String(), Input: input, CreatedAt: time.Now().UTC()}
	if err := writeJSON(w.root, queuePath(record.ID), record); err != nil {
		return queueRecord{}, false, err
	}
	if input.IdempotencyKey != "" {
		if err := writeJSON(w.root, queueKeyPath(input.IdempotencyKey), queueKeyRecord{Key: input.IdempotencyKey, MessageID: record.ID}); err != nil {
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
	name := queueKeyPath(record.Input.IdempotencyKey)
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
		deliveries = append(deliveries, delivery{at: record.due(), id: record.ID})
	}
	return deliveries, nil
}

type delivery struct {
	at time.Time
	id string
	// failures counts storage failures before the delivery could start.
	failures int
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

// deliver executes one delivery of a message. A serialized flow first waits
// for the flow of its run that is executing. A successful delivery deletes
// the message; a failed one, or one that asks for a redelivery, keeps it and
// schedules its next delivery.
func (w *World) deliver(value delivery) {
	defer w.workers.Done()
	record, ok, err := w.peekDelivery(value.id)
	if err != nil {
		w.retryStorage(value, err)
		return
	}
	if !ok {
		return
	}
	if record.Input.SerializedFlow() {
		if !w.flows.acquire(w.ctx, record.Input.RunID) {
			return
		}
		defer w.flows.release(record.Input.RunID)
	}
	record, ok, err = w.startDelivery(value.id)
	if err != nil {
		w.retryStorage(value, err)
		return
	}
	if !ok {
		return
	}
	input := record.Input
	input.MessageID = record.ID
	input.DeliveryCount = record.Deliveries
	input.Attempt = record.Deliveries - record.Handoffs
	err = w.execute(w.ctx, input)
	if err == nil {
		if err = w.ackDelivery(record); err == nil {
			return
		}
	}
	if w.ctx.Err() != nil {
		return
	}
	var after time.Duration
	handoff := false
	if redelivery, ok := worlds.RedeliveryOf(err); ok {
		after = redelivery.After
		handoff = after == 0
	} else {
		after = worlds.DeliveryRetryDelay(err, min(retryBase<<min(max(input.Attempt, 1)-1, 20), retryMax))
		log.Printf("workflow/local: delivery %s failed (attempt %d), retrying in %s: %v", record.ID, input.Attempt, after, err)
	}
	at := time.Now().Add(after)
	if err := w.requeue(record.ID, at, handoff); err != nil {
		log.Printf("workflow/local: delivery %s: record redelivery: %v", record.ID, err)
	}
	w.schedule(delivery{at: at, id: record.ID})
}

// retryStorage schedules a delivery that could not start because storage
// failed.
func (w *World) retryStorage(value delivery, err error) {
	if w.ctx.Err() != nil {
		return
	}
	value.failures++
	backoff := min(retryBase<<min(value.failures-1, 20), retryMax)
	value.at = time.Now().Add(backoff)
	log.Printf("workflow/local: delivery %s could not start, retrying in %s: %v", value.id, backoff, err)
	w.schedule(value)
}

func (w *World) peekDelivery(id string) (queueRecord, bool, error) {
	unlock, err := w.readLock()
	if err != nil {
		return queueRecord{}, false, err
	}
	defer unlock()
	return w.readQueue(id)
}

// startDelivery counts a delivery of message id before it executes, so a
// delivery interrupted by a crash counts too. A message past its Retention is
// deleted instead.
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
	if retention := record.Input.Retention; retention > 0 && time.Since(record.CreatedAt) > retention {
		log.Printf("workflow/local: delivery %s expired after %s, dropping it", record.ID, retention)
		return queueRecord{}, false, w.ackLocked(record)
	}
	record.Deliveries++
	record.DueAt = nil
	return record, true, writeJSON(w.root, queuePath(id), record)
}

// requeue records that message id is next delivered at at, and that the
// delivery that ended was a handoff.
func (w *World) requeue(id string, at time.Time, handoff bool) error {
	unlock, err := w.writeLock()
	if err != nil {
		return err
	}
	defer unlock()
	record, ok, err := w.readQueue(id)
	if err != nil || !ok {
		return err
	}
	at = at.UTC()
	record.DueAt = &at
	if handoff {
		record.Handoffs++
	}
	return writeJSON(w.root, queuePath(id), record)
}

func (w *World) ackDelivery(record queueRecord) error {
	unlock, err := w.writeLock()
	if err != nil {
		return err
	}
	defer unlock()
	return w.ackLocked(record)
}

// ackLocked deletes the message, then its key. A key left by a crash between
// the two names a message that no longer lives, so it deduplicates nothing.
func (w *World) ackLocked(record queueRecord) error {
	if err := remove(w.root, queuePath(record.ID)); err != nil {
		return err
	}
	return w.removeQueueKey(record)
}

func (record queueRecord) due() time.Time {
	if record.DueAt != nil {
		return *record.DueAt
	}
	return notBefore(record.Input)
}

// flowGates lets one serialized flow of a run execute at a time. A run with
// an entry has a flow executing, and its waiters execute in turn.
type flowGates struct {
	mu   sync.Mutex
	runs map[worlds.RunID]*flowGate
}

type flowGate struct {
	waiters []chan struct{}
}

// acquire waits until no other flow of runID executes. It reports false when
// ctx ends first.
func (g *flowGates) acquire(ctx context.Context, runID worlds.RunID) bool {
	g.mu.Lock()
	if g.runs == nil {
		g.runs = make(map[worlds.RunID]*flowGate)
	}
	gate, busy := g.runs[runID]
	if !busy {
		g.runs[runID] = &flowGate{}
		g.mu.Unlock()
		return true
	}
	turn := make(chan struct{})
	gate.waiters = append(gate.waiters, turn)
	g.mu.Unlock()
	select {
	case <-turn:
		return true
	case <-ctx.Done():
		g.mu.Lock()
		if index := slices.Index(gate.waiters, turn); index >= 0 {
			gate.waiters = slices.Delete(gate.waiters, index, index+1)
			g.mu.Unlock()
			return false
		}
		g.mu.Unlock()
		// The turn was handed over as ctx ended: pass it on.
		g.release(runID)
		return false
	}
}

// release hands runID's turn to its next waiting flow.
func (g *flowGates) release(runID worlds.RunID) {
	g.mu.Lock()
	defer g.mu.Unlock()
	gate := g.runs[runID]
	if len(gate.waiters) == 0 {
		delete(g.runs, runID)
		return
	}
	turn := gate.waiters[0]
	gate.waiters = gate.waiters[1:]
	close(turn)
}

func queuePath(id string) string { return path.Join("queue", id+".json") }

func queueKeyPath(key string) string {
	return path.Join("queue-keys", fileKey(key)+".json")
}

func notBefore(input worlds.QueueInput) time.Time {
	if input.NotBefore == nil {
		return time.Time{}
	}
	return *input.NotBefore
}
