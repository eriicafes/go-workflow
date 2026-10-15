// Package localworld provides a durable, single-process World backed by JSON
// files. Each run, event, step, hook, token, stream chunk, and queued delivery
// is its own record, and runs/<id>.json is the commit point for every append.
package localworld

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync"

	"github.com/eriicafes/go-workflow/worlds"
)

const defaultBaseURL = "http://localhost:3000/v1/workflow"

var (
	ErrNotStarted = errors.New("workflow/local: world is not started")
	ErrClosed     = errors.New("workflow/local: world is closed")
)

// Config configures a local World.
type Config struct {
	// Dir is the storage directory used by Open.
	Dir string
	// BaseURL is the public URL where HTTP is mounted. It defaults to
	// http://localhost:3000/v1/workflow.
	BaseURL string
}

// World stores workflow state as JSON records under one directory. Only one
// started World may use a directory at a time. Open owns its root; New borrows
// a root supplied by its caller.
type World struct {
	// mu guards the files and lifecycle fields. Mutations hold it exclusively,
	// so readers only ever observe committed state.
	mu        sync.RWMutex
	root      *os.Root
	ownedRoot bool
	baseURL   string
	lock      *os.File
	started   bool
	closed    bool
	broken    error
	ctx       context.Context
	cancel    context.CancelFunc
	runtime   *worlds.Runtime
	workers   sync.WaitGroup

	changesMu sync.Mutex
	changes   chan struct{}

	queueMu   sync.Mutex
	pending   deliveryHeap
	queueWake chan struct{}
}

// Open creates config.Dir with private permissions and returns a World that
// owns it. Call Start before using the World.
func Open(config Config, runners worlds.RunnerSet) (*World, error) {
	if config.Dir == "" {
		return nil, errors.New("workflow/local: storage directory is required")
	}
	if err := os.MkdirAll(config.Dir, 0700); err != nil {
		return nil, fmt.Errorf("workflow/local: create storage: %w", err)
	}
	if err := os.Chmod(config.Dir, 0700); err != nil {
		return nil, fmt.Errorf("workflow/local: protect storage: %w", err)
	}
	root, err := os.OpenRoot(config.Dir)
	if err != nil {
		return nil, fmt.Errorf("workflow/local: open storage: %w", err)
	}
	world, err := New(root, config, runners)
	if err != nil {
		_ = root.Close()
		return nil, err
	}
	world.ownedRoot = true
	return world, nil
}

// New creates a World which borrows root; the caller closes root after Close.
// config.Dir is ignored. Call Start before using the World.
func New(root *os.Root, config Config, runners worlds.RunnerSet) (*World, error) {
	if root == nil {
		return nil, errors.New("workflow/local: nil storage root")
	}
	baseURL := defaultBaseURL
	if config.BaseURL != "" {
		normalized, err := worlds.NormalizeBaseURL(config.BaseURL)
		if err != nil {
			return nil, err
		}
		baseURL = normalized
	}
	world := &World{root: root, baseURL: baseURL, changes: make(chan struct{}), queueWake: make(chan struct{}, 1)}
	world.runtime = worlds.NewRuntime(world, runners)
	return world, nil
}

// Start takes the exclusive storage lock, validates the storage version,
// recovers interrupted writes, and begins delivering persisted queue items.
// Cancelling ctx stops delivery. It returns ErrInUse when another World holds
// the directory. Every other method requires a started World.
func (w *World) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	if w.started {
		return nil
	}
	lock, err := acquireLock(w.root)
	if err != nil {
		return err
	}
	if err := validateVersion(w.root); err != nil {
		releaseLock(lock)
		return err
	}
	if err := w.recoverLocked(); err != nil {
		releaseLock(lock)
		return err
	}
	deliveries, err := w.loadQueueLocked()
	if err != nil {
		releaseLock(lock)
		return err
	}
	w.lock, w.started = lock, true
	w.ctx, w.cancel = context.WithCancel(ctx)
	for _, delivery := range deliveries {
		w.schedule(delivery)
	}
	w.workers.Add(1)
	go w.dispatch()
	return nil
}

// Close stops delivery, waits for in-flight deliveries, releases the storage
// lock, and closes a root opened by Open. Interrupted deliveries stay queued.
func (w *World) Close() error {
	if !w.stop(false) {
		return nil
	}
	return w.release()
}

// Clear stops delivery, removes all persisted state while holding the storage
// lock, writes a fresh version record, and closes the World. Open a new World
// before submitting further work.
func (w *World) Clear(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !w.stop(true) {
		return ErrNotStarted
	}
	w.mu.Lock()
	var err error
	for _, name := range []string{"runs", "events", "steps", "hooks", "tokens", "streams", "queue", "queue-keys", versionName} {
		if removeErr := w.root.RemoveAll(name); removeErr != nil {
			err = errors.Join(err, removeErr)
		}
	}
	if err == nil {
		err = writeJSON(w.root, versionName, versionRecord{Version: storeVersion})
	}
	w.mu.Unlock()
	return errors.Join(err, w.release())
}

// stop rejects new calls, cancels delivery, and waits for workers. It reports
// false when the World was already closed, or not started when requireStarted.
func (w *World) stop(requireStarted bool) bool {
	w.mu.Lock()
	if w.closed || requireStarted && !w.started {
		w.mu.Unlock()
		return false
	}
	w.closed = true
	cancel := w.cancel
	w.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	w.signal()
	w.workers.Wait()
	return true
}

func (w *World) release() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.started = false
	releaseLock(w.lock)
	w.lock = nil
	if w.ownedRoot {
		return w.root.Close()
	}
	return nil
}

// readLock holds the shared lock for a started World.
func (w *World) readLock() (func(), error) {
	w.mu.RLock()
	if err := w.usableLocked(); err != nil {
		w.mu.RUnlock()
		return nil, err
	}
	return w.mu.RUnlock, nil
}

// writeLock holds the exclusive lock for a started World.
func (w *World) writeLock() (func(), error) {
	w.mu.Lock()
	if err := w.usableLocked(); err != nil {
		w.mu.Unlock()
		return nil, err
	}
	return w.mu.Unlock, nil
}

func (w *World) usableLocked() error {
	switch {
	case w.closed:
		return ErrClosed
	case !w.started:
		return ErrNotStarted
	case w.broken != nil:
		return w.broken
	}
	return nil
}

// changed returns a channel closed by the next committed change. Waiters take
// it before reading so a concurrent change is never missed.
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

func (w *World) BaseURL() string             { return w.baseURL }
func (*World) SpecVersion() int              { return worlds.SpecVersion }
func (w *World) Analytics() worlds.Analytics { return analytics{world: w} }
func (*World) Capabilities() worlds.Capabilities {
	return worlds.Capabilities{Analytics: true, SetAttributes: true}
}

// HTTP returns the RPC and webhook endpoints under /v1/workflow.
func HTTP(world *World) http.Handler { return worlds.HTTPHandler(world, nil, "") }

// RPC returns the CBOR RPC endpoint only.
func RPC(world *World) http.Handler { return worlds.RPCHandler(world) }

// Webhook returns the token webhook endpoint only.
func Webhook(world *World) http.Handler { return worlds.WebhookHandler(world) }
