// Package localworld implements a World on JSON files in a local directory,
// for development and single-process deployments. It delivers queued work
// in-process and keeps every run across restarts. Only one process may use a
// directory at a time.
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
	// ErrNotPrepared is returned by World methods called before Prepare, and by
	// Clear on a World that is not prepared.
	ErrNotPrepared = errors.New("workflow/local: world is not prepared")
	// ErrClosed is returned by World methods called after Close or Clear.
	ErrClosed = errors.New("workflow/local: world is closed")
)

// Config configures the local World.
type Config struct {
	// Dir is the storage directory. It is required by Open and ignored by New.
	Dir string
	// BaseURL is the absolute public URL of HTTP(world), including
	// /v1/workflow. Webhooks created by this World use it as their URL base.
	// Empty uses http://localhost:3000/v1/workflow.
	BaseURL string
}

// World is the local World. It stores events, run, step, and hook views,
// streams, and queued deliveries as JSON files under one directory, and only
// one prepared World may use a directory at a time.
type World struct {
	// mu guards the files and lifecycle fields. Mutations hold it exclusively,
	// so readers only ever observe committed state.
	mu        sync.RWMutex
	root      *os.Root
	ownedRoot bool
	baseURL   string
	lock      *os.File
	prepared  bool
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

// Open creates config.Dir with private permissions if needed and returns a
// World that executes deliveries for the workflows of runners. The World owns the
// directory handle and closes it on Close. Open reads no state, so call Prepare
// before using the World.
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

// New returns a World stored in root that executes deliveries for the workflows
// in runners. The caller keeps owning root and closes it after Close.
// config.Dir is ignored. New reads no state, so call Prepare before using the
// World.
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

// Prepare locks the directory, checks its storage version, recovers writes
// interrupted by a crash, and begins delivering queued work in the background.
// It returns once delivery has begun, and cancelling ctx stops delivery.
//
// Every other World method returns ErrNotPrepared until Prepare succeeds. Prepare
// returns ErrInUse when another World, in this or another process, holds the
// directory, ErrClosed after Close, and nil when the World is already prepared.
func (w *World) Prepare(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	if w.prepared {
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
	w.lock, w.prepared = lock, true
	w.ctx, w.cancel = context.WithCancel(ctx)
	for _, delivery := range deliveries {
		w.schedule(delivery)
	}
	w.workers.Add(1)
	go w.dispatch()
	return nil
}

// Close stops delivery, waits for in-flight deliveries to return, releases the
// directory lock, and closes the directory handle when Open created it.
// Interrupted deliveries stay queued and run again after the next Prepare.
// Later calls to World methods return ErrClosed, and closing twice returns
// nil.
func (w *World) Close() error {
	if !w.stop(false) {
		return nil
	}
	return w.release()
}

// Clear stops delivery, deletes every run, hook, stream, and queued delivery in
// the directory, and closes the World. It returns ErrNotPrepared unless the
// World is prepared. Open a new World on the directory to continue.
func (w *World) Clear(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !w.stop(true) {
		return ErrNotPrepared
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
	if w.closed || requireStarted && !w.prepared {
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
	w.prepared = false
	releaseLock(w.lock)
	w.lock = nil
	if w.ownedRoot {
		return w.root.Close()
	}
	return nil
}

// readLock holds the shared lock for a prepared World.
func (w *World) readLock() (func(), error) {
	w.mu.RLock()
	if err := w.usableLocked(); err != nil {
		w.mu.RUnlock()
		return nil, err
	}
	return w.mu.RUnlock, nil
}

// writeLock holds the exclusive lock for a prepared World.
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
	case !w.prepared:
		return ErrNotPrepared
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
func (*World) DeploymentID() string          { return "" }
func (w *World) Analytics() worlds.Analytics { return analytics{world: w} }
func (*World) Capabilities() worlds.Capabilities {
	return worlds.Capabilities{Analytics: true, SetAttributes: true}
}

// HTTP returns a handler serving the RPC and webhook endpoints under
// /v1/workflow. It does not authenticate requests.
func HTTP(world *World) http.Handler { return HTTPPrefix(world, "") }

// HTTPPrefix is like HTTP but serves the endpoints under prefix, such as
// /internal/workflow. An empty prefix uses /v1/workflow. Config.BaseURL must
// end with the same prefix.
func HTTPPrefix(world *World, prefix string) http.Handler {
	return worlds.HTTPHandler(world, prefix)
}

// RPC returns a handler serving only the RPC endpoint.
func RPC(world *World) http.Handler { return worlds.RPCHandler(world) }

// Webhook returns a handler serving only the webhook endpoint, authorized by
// the hook token in its URL.
func Webhook(world *World) http.Handler { return worlds.WebhookHandler(world) }
