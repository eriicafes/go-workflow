// Package localworld implements a World on a SQLite database in a local
// directory, for development and single-machine deployments: a port of the
// Workflow SDK's world-local. It delivers queued work in-process from an
// in-memory queue, and keeps every run across restarts. Several processes may
// share a directory: the database serializes their writes.
package localworld

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/core"
)

const (
	defaultBaseURL = "http://localhost:3000/v1/workflow"
	// defaultDir is the data directory when neither Config.Dir nor
	// WORKFLOW_LOCAL_DATA_DIR names one.
	defaultDir = ".workflow-data"
	// pollInterval is how often waiters read the database for writes made
	// by other processes.
	pollInterval = 100 * time.Millisecond
)

var (
	// ErrNotPrepared is returned by World methods called before Prepare, and by
	// Clear on a World that is not prepared.
	ErrNotPrepared = errors.New("workflow/local: world is not prepared")
	// ErrClosed is returned by World methods called after Close or Clear.
	ErrClosed = errors.New("workflow/local: world is closed")
)

// Config configures the local World.
type Config struct {
	// Dir is the data directory, which holds the database world.db. Empty
	// uses WORKFLOW_LOCAL_DATA_DIR, or .workflow-data.
	Dir string
	// Tag scopes the runs, hooks, and streams this World writes, such as a
	// test's. A tagged World sees its tag's runs and untagged ones, recovers
	// only its tag's runs, and Clear deletes only its tag's data. Steps,
	// events, hooks, and streams are read whatever their tag, as in
	// world-local. Empty is untagged.
	Tag string
	// BaseURL is the absolute public URL of HTTP(world), including
	// /v1/workflow. Webhooks created by this World use it as their URL base.
	// Empty uses http://localhost:3000/v1/workflow.
	BaseURL string
	// EncryptionKey, 32 random bytes, encrypts every run's payloads with a
	// key derived from it for the run. Empty leaves payloads unencrypted.
	// Changing it leaves earlier runs' payloads unreadable.
	EncryptionKey []byte
	// EncryptionScope is the scope run keys are derived from EncryptionKey
	// in. Worlds that share an EncryptionKey keep their runs' keys apart with
	// different scopes. Changing it leaves earlier runs' payloads
	// unreadable.
	EncryptionScope string
	// Concurrency is how many deliveries execute at once. Zero uses
	// WORKFLOW_LOCAL_QUEUE_CONCURRENCY, or 1000.
	Concurrency int
	// HookRetentionLimit caps a hook's token retention. Zero uses
	// WORKFLOW_LOCAL_HOOK_RETENTION_LIMIT_DAYS, or 30 days.
	HookRetentionLimit time.Duration
	// SkipRecovery leaves the active runs of the World's tag alone when it
	// is prepared, rather than queueing each of them again.
	SkipRecovery bool
}

// World is the local World. It stores events, run, step, wait, and hook
// views, and streams in a SQLite database, and queues deliveries in memory.
type World struct {
	// mu guards the lifecycle fields.
	mu       sync.RWMutex
	dir      string
	tag      string
	baseURL  string
	db       *database
	prepared bool
	closed   bool
	// encryptionKey and encryptionScope are Config.EncryptionKey and
	// Config.EncryptionScope.
	encryptionKey      []byte
	encryptionScope    string
	hookRetentionLimit time.Duration
	skipRecovery       bool
	// parent is Prepare's context, which ends delivery. ctx and cancel are
	// the current deliveries' context, which Clear replaces, and workers
	// counts them.
	parent  context.Context
	ctx     context.Context
	cancel  context.CancelFunc
	workers *sync.WaitGroup
	runtime *core.Runtime

	changesMu sync.Mutex
	changes   chan struct{}

	queue *queue
	// execute executes a delivery: runtime.Execute, or a test's stand-in.
	execute func(context.Context, worlds.QueueInput) error
}

// Open returns a World on config.Dir that executes deliveries for the
// workflows of runners. Open reads no state, so call Prepare before using the
// World.
func Open(config Config, runners core.RunnerSet) (*World, error) {
	if err := worlds.CheckEncryptionKey(config.EncryptionKey); err != nil {
		return nil, err
	}
	baseURL := defaultBaseURL
	if config.BaseURL != "" {
		normalized, err := worlds.NormalizeBaseURL(config.BaseURL)
		if err != nil {
			return nil, err
		}
		baseURL = normalized
	}
	dir := config.Dir
	if dir == "" {
		dir = os.Getenv("WORKFLOW_LOCAL_DATA_DIR")
	}
	if dir == "" {
		dir = defaultDir
	}
	limit, err := hookRetentionLimit(config.HookRetentionLimit)
	if err != nil {
		return nil, err
	}
	world := &World{workers: &sync.WaitGroup{}, dir: dir, tag: config.Tag, baseURL: baseURL, encryptionKey: bytes.Clone(config.EncryptionKey), encryptionScope: config.EncryptionScope, hookRetentionLimit: limit, skipRecovery: config.SkipRecovery, changes: make(chan struct{})}
	world.runtime = core.NewRuntime(world, runners)
	world.execute = world.runtime.Execute
	world.queue = newQueue(world, queueConcurrency(config.Concurrency))
	return world, nil
}

// Prepare opens the database, creating its schema in a new directory,
// begins delivering queued work in the background, and queues every active
// run of the World's tag again unless Config.SkipRecovery: the queue is in
// memory, so a restart lost their messages. Cancelling ctx stops delivery.
//
// Every other World method returns ErrNotPrepared until Prepare succeeds.
// Prepare returns ErrClosed after Close, and nil when the World is already
// prepared.
func (w *World) Prepare(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return ErrClosed
	}
	if w.prepared {
		w.mu.Unlock()
		return nil
	}
	db, err := openDatabase(ctx, w.dir)
	if err != nil {
		w.mu.Unlock()
		return err
	}
	w.db, w.prepared = db, true
	w.parent = ctx
	w.ctx, w.cancel = context.WithCancel(ctx)
	w.mu.Unlock()
	if w.skipRecovery {
		return nil
	}
	return w.recoverActiveRuns(ctx)
}

// Close stops delivery, waits for in-flight deliveries to return, and closes
// the database. Queued deliveries are lost: the next Prepare queues the
// active runs again. Later calls to World methods return ErrClosed, and
// closing twice returns nil.
func (w *World) Close() error {
	if !w.stop(false) {
		return nil
	}
	return w.release()
}

// Clear deletes the World's data: a tagged World's runs, everything they
// own, and its hooks and streams, and an untagged World's whole database.
// Deliveries in flight are cancelled first, and the World stays open for
// new work, as world-local's clear(). It returns ErrNotPrepared unless the
// World is prepared.
func (w *World) Clear(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	db, err := w.usable()
	if err != nil {
		return err
	}
	w.mu.Lock()
	cancel, workers := w.cancel, w.workers
	w.ctx, w.cancel = context.WithCancel(w.parent)
	w.workers = &sync.WaitGroup{}
	w.mu.Unlock()
	cancel()
	w.signal()
	workers.Wait()
	if err := db.update(ctx, func(tx *sqlTx) error { return clearTag(ctx, tx, w.tag) }); err != nil {
		return err
	}
	db.maintain(ctx)
	return nil
}

// stop rejects new calls, cancels delivery, and waits for workers. It reports
// false when the World was already closed, or not started when requireStarted.
func (w *World) stop(requireStarted bool) bool {
	w.mu.Lock()
	if w.closed || requireStarted && !w.prepared {
		w.mu.Unlock()
		return false
	}
	w.closed = true
	cancel, workers := w.cancel, w.workers
	w.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	w.signal()
	workers.Wait()
	return true
}

func (w *World) release() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.prepared = false
	if w.db == nil {
		return nil
	}
	err := w.db.close()
	w.db = nil
	return err
}

// usable returns the World's database while it is prepared and open.
func (w *World) usable() (*database, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	switch {
	case w.closed:
		return nil, ErrClosed
	case !w.prepared:
		return nil, ErrNotPrepared
	}
	return w.db, nil
}

// read runs fn on a snapshot of the database.
func (w *World) read(ctx context.Context, fn func(*store) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	db, err := w.usable()
	if err != nil {
		return err
	}
	return db.view(ctx, func(tx *sqlTx) error { return fn(&store{ctx: ctx, q: tx, tag: w.tag}) })
}

// write runs fn in a write transaction, which commits when fn returns nil,
// and signals waiters after a commit.
func (w *World) write(ctx context.Context, fn func(*store) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	db, err := w.usable()
	if err != nil {
		return err
	}
	if err := db.update(ctx, func(tx *sqlTx) error { return fn(&store{ctx: ctx, q: tx, tag: w.tag}) }); err != nil {
		return err
	}
	w.signal()
	return nil
}

// changed returns a channel closed by the next change this process commits,
// or after pollInterval, when another process may have committed one.
// Waiters take it before reading so a concurrent change is never missed.
func (w *World) changed() <-chan struct{} {
	w.changesMu.Lock()
	defer w.changesMu.Unlock()
	return w.changes
}

func (w *World) signal() {
	w.changesMu.Lock()
	close(w.changes)
	w.changes = make(chan struct{})
	w.changesMu.Unlock()
}

// waitChange waits for a change this process commits, pollInterval, the
// deadline's timer, or ctx.
func waitChange(ctx context.Context, changed <-chan struct{}, timer <-chan time.Time) error {
	poll := time.NewTimer(pollInterval)
	defer poll.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer:
	case <-changed:
	case <-poll.C:
	}
	return nil
}

func (w *World) BaseURL() string    { return w.baseURL }
func (*World) DeploymentID() string { return "" }
func (w *World) Analytics() worlds.Analytics {
	return analytics{LogAnalytics: worlds.LogAnalytics{World: w}, world: w}
}
func (*World) Capabilities() worlds.Capabilities {
	return worlds.Capabilities{Analytics: true, SetAttributes: true, DeleteData: true, StartHook: true, HookResumeDedup: true}
}

// HTTP returns a handler serving the client and webhook endpoints under
// /v1/workflow. Its client endpoints are unauthenticated unless options
// protect them, such as worlds.WithClientToken.
func HTTP(world *World, options ...worlds.HTTPOption) http.Handler {
	return HTTPPrefix(world, "", options...)
}

// HTTPPrefix is like HTTP but serves the endpoints under prefix, such as
// /internal/workflow. An empty prefix uses /v1/workflow. Config.BaseURL must
// end with the same prefix.
func HTTPPrefix(world *World, prefix string, options ...worlds.HTTPOption) http.Handler {
	return worlds.HTTPHandler(world, prefix, options...)
}

// RPC returns a handler serving only the RPC endpoint.
func RPC(world *World) http.Handler { return worlds.RPCHandler(world) }

// Webhook returns a handler serving only the webhook endpoint, authorized by
// the hook token in its URL.
func Webhook(world *World) http.Handler { return worlds.WebhookHandler(world) }

// EncryptionKeyForRun returns the key of a run's payloads, derived from
// Config.EncryptionKey in Config.EncryptionScope, or nil when the World has
// no key.
func (w *World) EncryptionKeyForRun(_ context.Context, runID worlds.RunID, _ string) ([]byte, error) {
	if len(w.encryptionKey) == 0 {
		return nil, nil
	}
	return worlds.DeriveRunKey(w.encryptionKey, w.encryptionScope, runID)
}
