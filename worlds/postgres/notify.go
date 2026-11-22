package postgresworld

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The channels: workflow_run_events carries the run ID of every committed
// write, workflow_run_status the run ID of a run that ended, and
// workflow_event_chunk a written chunk's {streamId, chunkId}. workflow_queue
// wakes Go's workers.
const (
	channelQueue   = "workflow_queue"
	channelRuns    = "workflow_run_status"
	channelStreams = "workflow_event_chunk"
	channelEvents  = "workflow_run_events"
)

var listenChannels = []string{channelQueue, channelRuns, channelStreams, channelEvents}

// notifier owns one LISTEN connection per World and fans notifications out
// to waiters keyed by channel and payload. Notifications are only wake-ups,
// because waiters always re-read state and also poll, so a missed one is not
// fatal.
type notifier struct {
	pool    *pgxpool.Pool
	ctx     context.Context
	cancel  context.CancelFunc
	mu      sync.Mutex
	started bool
	done    chan struct{}
	waiters map[string]*waiter
}

func newNotifier(pool *pgxpool.Pool) *notifier {
	ctx, cancel := context.WithCancel(context.Background())
	return &notifier{pool: pool, ctx: ctx, cancel: cancel, done: make(chan struct{}), waiters: make(map[string]*waiter)}
}

type waiter struct {
	ch   chan struct{}
	refs int
}

// wait registers interest in the next notification for channel and payload.
// Register before reading state so a change between the read and the wait
// still wakes the caller. release must be called once the wait is over.
func (n *notifier) wait(channel, payload string) (<-chan struct{}, func()) {
	key := notifyKey(channel, payload)
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.started && n.ctx.Err() == nil {
		n.started = true
		go n.run()
	}
	entry := n.waiters[key]
	if entry == nil {
		entry = &waiter{ch: make(chan struct{})}
		n.waiters[key] = entry
	}
	entry.refs++
	var once sync.Once
	return entry.ch, func() {
		once.Do(func() {
			n.mu.Lock()
			defer n.mu.Unlock()
			entry.refs--
			if entry.refs == 0 && n.waiters[key] == entry {
				delete(n.waiters, key)
			}
		})
	}
}

func (n *notifier) signal(key string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if entry := n.waiters[key]; entry != nil {
		close(entry.ch)
		delete(n.waiters, key)
	}
}

func (n *notifier) broadcast() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for key, entry := range n.waiters {
		close(entry.ch)
		delete(n.waiters, key)
	}
}

func (n *notifier) close() {
	n.mu.Lock()
	n.cancel()
	started := n.started
	n.mu.Unlock()
	if started {
		<-n.done
	}
	n.broadcast()
}

func (n *notifier) run() {
	defer close(n.done)
	delay := 100 * time.Millisecond
	for n.ctx.Err() == nil {
		err := n.listen()
		if n.ctx.Err() != nil {
			return
		}
		log.Printf("workflow/postgres: listen: %v", err)
		n.broadcast()
		timer := time.NewTimer(delay)
		select {
		case <-n.ctx.Done():
			return
		case <-timer.C:
		}
		delay = min(delay*2, 5*time.Second)
	}
}

// listen hijacks a pool connection so the long-lived LISTEN session does not
// occupy a pool slot, then dispatches notifications until an error occurs.
func (n *notifier) listen() error {
	pooled, err := n.pool.Acquire(n.ctx)
	if err != nil {
		return err
	}
	conn := pooled.Hijack()
	defer conn.Close(context.Background())
	for _, channel := range listenChannels {
		if _, err := conn.Exec(n.ctx, "LISTEN "+channel); err != nil {
			return err
		}
	}
	// Anything committed before LISTEN took effect was not delivered.
	n.broadcast()
	for {
		notification, err := conn.WaitForNotification(n.ctx)
		if err != nil {
			return err
		}
		payload := notification.Payload
		if notification.Channel == channelStreams {
			// A chunk's waiters wait on its stream.
			var chunk chunkNotification
			if json.Unmarshal([]byte(payload), &chunk) == nil {
				payload = chunk.StreamID
			}
		}
		n.signal(notifyKey(notification.Channel, payload))
	}
}

func notifyKey(channel, payload string) string { return channel + ":" + payload }

// notifyingTx is a write transaction of inTx, which holds the notifications
// of its writes until it commits.
type notifyingTx struct {
	pgx.Tx
	pending []notification
}

type notification struct{ channel, payload string }

// notify queues a notification on tx, sent once it commits (announce). A
// NOTIFY never runs inside the write's transaction, where its failure would
// fail the write and every notifying commit would queue on Postgres's
// notification lock.
func notify(tx pgx.Tx, channel, payload string) {
	pending := tx.(*notifyingTx)
	for _, queued := range pending.pending {
		if queued.channel == channel && queued.payload == payload {
			return
		}
	}
	pending.pending = append(pending.pending, notification{channel, payload})
}

// announce sends a committed transaction's notifications. They are best
// effort: a waiter that misses one finds the change on its next poll, so a
// failure is ignored. The write returns once a run's end (notifyRunTerminal)
// or a stream chunk (notifyStream) is announced, and does not wait for the
// other channels (announceRunEvents). Notifications sent together share one
// statement.
func (w *World) announce(ctx context.Context, pending []notification) {
	if len(pending) == 0 {
		return
	}
	channels, payloads := make([]string, len(pending)), make([]string, len(pending))
	await := false
	for i, n := range pending {
		channels[i], payloads[i] = n.channel, n.payload
		await = await || n.channel == channelRuns || n.channel == channelStreams
	}
	send := func(ctx context.Context) {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		_, _ = w.pool.Exec(ctx, `SELECT pg_notify(n.channel, n.payload) FROM unnest($1::text[], $2::text[]) AS n(channel, payload)`, channels, payloads)
	}
	if await {
		// The write has committed, so its caller's cancellation does not
		// hold the announcement back.
		send(context.WithoutCancel(ctx))
		return
	}
	w.announcing.Add(1)
	go func() {
		defer w.announcing.Done()
		send(context.Background())
	}()
}

// chunkNotification is the payload of a workflow_event_chunk notification.
type chunkNotification struct {
	StreamID string `json:"streamId"`
	ChunkID  string `json:"chunkId"`
}
