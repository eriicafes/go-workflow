package localworld

import (
	"context"
	"crypto/rand"
	"log"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/eriicafes/go-workflow/worlds"
)

// The queue is a port of world-local's queue.js. It is in memory, as
// world-local's is: a restart loses its messages, and Prepare queues the
// active runs again.
const (
	// defaultConcurrency is how many deliveries execute at once.
	defaultConcurrency = 1000
	// maxDeliveries is the most deliveries of one message, handoffs and
	// re-runs aside: a guard against a message that fails forever. The
	// runtime enforces its own cap well below it.
	maxDeliveries = 256
	// maxHandoffs is the most handoffs of one message that are not counted
	// as deliveries.
	maxHandoffs = 10_000
	// maxCoalescedReruns is the most re-runs one hold of a run's gate makes
	// for the messages coalesced into it.
	maxCoalescedReruns = 32
	// linearRetries failed deliveries are retried linearRetryDelay apart,
	// before the backoff doubles up to maxRetryDelay.
	linearRetries    = 32
	linearRetryDelay = 5 * time.Second
	maxRetryDelay    = 60 * time.Second
)

// retryDelay is the delay before retrying a message whose failed-th delivery
// failed: 5s for the first 32, as Vercel Queues does, then doubling up to a
// minute.
func retryDelay(failed int) time.Duration {
	if failed <= linearRetries {
		return linearRetryDelay
	}
	return min(maxRetryDelay, linearRetryDelay<<min(failed-linearRetries, 8))
}

// queueConcurrency returns the number of delivery slots: configured,
// WORKFLOW_LOCAL_QUEUE_CONCURRENCY, or 1000.
func queueConcurrency(configured int) int {
	if configured > 0 {
		return configured
	}
	if value, err := strconv.Atoi(os.Getenv("WORKFLOW_LOCAL_QUEUE_CONCURRENCY")); err == nil && value > 0 {
		return value
	}
	return defaultConcurrency
}

type queue struct {
	w     *World
	slots chan struct{}
	gate  runGate

	mu sync.Mutex
	// inflight names the message of each idempotency key until the message
	// ends.
	inflight map[string]string
	entropy  *ulid.MonotonicEntropy
	// live counts the messages that have not ended.
	live atomic.Int64
	// retry is the delay before retrying a message after its failed-th
	// failed delivery: retryDelay, or a test's.
	retry func(failed int) time.Duration
}

func newQueue(w *World, concurrency int) *queue {
	return &queue{w: w, slots: make(chan struct{}, concurrency), inflight: map[string]string{}, entropy: ulid.Monotonic(rand.Reader, 0), retry: retryDelay}
}

// Queue deduplicates by IdempotencyKey: while a message with the same key is
// in flight, it returns that message's ID instead of queueing another.
func (w *World) Queue(ctx context.Context, input worlds.QueueInput) (worlds.QueueResult, error) {
	if err := ctx.Err(); err != nil {
		return worlds.QueueResult{}, err
	}
	if _, err := w.usable(); err != nil {
		return worlds.QueueResult{}, err
	}
	id, err := w.queue.send(input)
	return worlds.QueueResult{MessageID: id}, err
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

// message is one queued message, delivered until a delivery of it succeeds.
type message struct {
	id        string
	input     worlds.QueueInput
	createdAt time.Time
}

func (q *queue) send(input worlds.QueueInput) (string, error) {
	q.mu.Lock()
	if key := input.IdempotencyKey; key != "" {
		if id, ok := q.inflight[key]; ok {
			q.mu.Unlock()
			return id, nil
		}
	}
	id := "msg_" + ulid.MustNew(ulid.Now(), q.entropy).String()
	if input.IdempotencyKey != "" {
		q.inflight[input.IdempotencyKey] = id
	}
	q.mu.Unlock()
	// The World sets the delivery fields on each delivery.
	input.MessageID, input.Attempt, input.DeliveryCount = "", 0, 0
	m := &message{id: id, input: input, createdAt: time.Now()}
	q.live.Add(1)
	if !q.w.spawn(func(ctx context.Context) { q.run(ctx, m) }) {
		q.done(m)
		return "", ErrClosed
	}
	return id, nil
}

// done forgets a message that ended.
func (q *queue) done(m *message) {
	q.live.Add(-1)
	if key := m.input.IdempotencyKey; key != "" {
		q.mu.Lock()
		if q.inflight[key] == m.id {
			delete(q.inflight, key)
		}
		q.mu.Unlock()
	}
}

// spawn runs fn in a worker the World waits for on Close, unless the World
// is closed.
func (w *World) spawn(fn func(context.Context)) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.closed || !w.prepared {
		return false
	}
	workers, ctx := w.workers, w.ctx
	workers.Add(1)
	go func() {
		defer workers.Done()
		fn(ctx)
	}()
	return true
}

// sleep waits for d or ctx, and reports whether d passed.
func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// coalescible reports whether a serialized flow may be coalesced into the
// running delivery of its run, which re-runs to cover it: it carries nothing
// the run's log does not hold. A run's first delivery carries its creation, a
// wake names waits the log does not, and a recovery flow asks its delivery
// to send again what the queue lost, so those are never coalesced.
func coalescible(input worlds.QueueInput) bool {
	return input.RunInput == nil && len(input.CompleteWaits) == 0 && !input.Recovery
}

// run delivers a message until a delivery succeeds, it is coalesced into
// another, or the World closes.
func (q *queue) run(ctx context.Context, m *message) {
	defer q.done(m)
	// The delay is slept before a slot is taken, so a delayed message holds
	// no worker.
	if m.input.NotBefore != nil && !sleep(ctx, time.Until(*m.input.NotBefore)) {
		return
	}
	serialized := m.input.SerializedFlow()
	var lease *lease
	held := false
	release := func() {
		if held {
			<-q.slots
			held = false
		}
		if lease != nil {
			lease.release()
			lease = nil
		}
	}
	defer release()
	// acquire takes the run's gate first, so a delivery waiting for its run
	// holds no slot, then a slot. It reports false when the message was
	// coalesced or the World closed.
	acquire := func() bool {
		if serialized {
			var coalesced bool
			var err error
			lease, coalesced, err = q.gate.acquire(ctx, m.input.RunID, coalescible(m.input))
			if err != nil || coalesced {
				return false
			}
		}
		select {
		case q.slots <- struct{}{}:
			held = true
			return true
		case <-ctx.Done():
			return false
		}
	}
	deliveries, handoffs, reruns, failed, rerunsThisHold := 0, 0, 0, 0, 0
	for deliveries-handoffs-reruns < maxDeliveries {
		if !held {
			if !acquire() {
				return
			}
			rerunsThisHold = 0
		}
		if retention := m.input.Retention; retention > 0 && time.Since(m.createdAt) > retention {
			log.Printf("workflow/local: delivery %s expired after %s, dropping it", m.id, retention)
			return
		}
		deliveries++
		input := m.input
		input.MessageID, input.DeliveryCount, input.Attempt = m.id, deliveries, deliveries-handoffs-reruns
		err := q.w.execute(ctx, input)
		if ctx.Err() != nil {
			return
		}
		if err == nil {
			// An acknowledged delivery covers the run's flows that queued
			// behind it, with a re-run while it still holds the gate.
			if lease != nil && rerunsThisHold < maxCoalescedReruns && lease.takeWaiting() > 0 {
				reruns++
				rerunsThisHold++
				continue
			}
			return
		}
		if redelivery, ok := worlds.RedeliveryOf(err); ok {
			if redelivery.After == 0 && handoffs < maxHandoffs {
				// A handoff is delivered again at once and is not an attempt.
				// It keeps the run's gate, as a new hold, and covers the
				// coalescible flows waiting behind it.
				handoffs++
				if lease != nil {
					rerunsThisHold = 0
					lease.takeWaiting()
				} else {
					release()
				}
				continue
			}
			release()
			if !sleep(ctx, redelivery.After) {
				return
			}
			continue
		}
		failed++
		release()
		delay := worlds.DeliveryRetryDelay(err, q.retry(failed))
		log.Printf("workflow/local: delivery %s failed (attempt %d), retrying in %s: %v", m.id, input.Attempt, delay, err)
		if !sleep(ctx, delay) {
			return
		}
	}
	log.Printf("workflow/local: delivery %s reached %d deliveries, dropping it", m.id, maxDeliveries)
}

// runGate lets one serialized flow of a run execute at a time, in FIFO
// order, and coalesces the coalescible flows waiting behind the holder into
// its delivery.
type runGate struct {
	mu   sync.Mutex
	runs map[worlds.RunID][]*waiter
}

type waiter struct {
	coalescible bool
	// grant receives the lease, or nil when the holder coalesced the
	// waiter.
	grant chan *lease
}

type lease struct {
	gate     *runGate
	runID    worlds.RunID
	released bool
}

// acquire waits for the run's gate. It reports coalesced when the holder
// took the waiter into its delivery instead.
func (g *runGate) acquire(ctx context.Context, runID worlds.RunID, coalescible bool) (*lease, bool, error) {
	g.mu.Lock()
	if g.runs == nil {
		g.runs = map[worlds.RunID][]*waiter{}
	}
	waiters, busy := g.runs[runID]
	if !busy {
		g.runs[runID] = nil
		g.mu.Unlock()
		return &lease{gate: g, runID: runID}, false, nil
	}
	turn := &waiter{coalescible: coalescible, grant: make(chan *lease, 1)}
	g.runs[runID] = append(waiters, turn)
	g.mu.Unlock()
	select {
	case granted := <-turn.grant:
		return granted, granted == nil, nil
	case <-ctx.Done():
		g.mu.Lock()
		waiters := g.runs[runID]
		for index, value := range waiters {
			if value == turn {
				g.runs[runID] = append(waiters[:index:index], waiters[index+1:]...)
				g.mu.Unlock()
				return nil, false, ctx.Err()
			}
		}
		g.mu.Unlock()
		// The turn was handed over as ctx ended: pass it on.
		if granted := <-turn.grant; granted != nil {
			granted.release()
		}
		return nil, false, ctx.Err()
	}
}

// release hands the run's gate to its next waiter.
func (l *lease) release() {
	g := l.gate
	g.mu.Lock()
	defer g.mu.Unlock()
	if l.released {
		return
	}
	l.released = true
	waiters := g.runs[l.runID]
	if len(waiters) == 0 {
		delete(g.runs, l.runID)
		return
	}
	next := waiters[0]
	g.runs[l.runID] = waiters[1:]
	next.grant <- &lease{gate: g, runID: l.runID}
}

// takeWaiting coalesces the coalescible waiters into the holder's delivery
// and returns how many it took.
func (l *lease) takeWaiting() int {
	g := l.gate
	g.mu.Lock()
	defer g.mu.Unlock()
	if l.released {
		return 0
	}
	count := 0
	var kept []*waiter
	for _, value := range g.runs[l.runID] {
		if !value.coalescible {
			kept = append(kept, value)
			continue
		}
		count++
		value.grant <- nil
	}
	g.runs[l.runID] = kept
	return count
}
