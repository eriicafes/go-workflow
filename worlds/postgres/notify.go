package postgresworld

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	channelQueue   = "go_workflow_queue"
	channelRuns    = "go_workflow_runs"
	channelStreams = "go_workflow_streams"
)

var listenChannels = []string{channelQueue, channelRuns, channelStreams}

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
			timer.Stop()
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
		n.signal(notifyKey(notification.Channel, notification.Payload))
	}
}

func notifyKey(channel, payload string) string { return channel + ":" + payload }
