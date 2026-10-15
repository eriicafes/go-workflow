// Package testworld implements a World in memory, for tests. It keeps nothing
// across processes and is not meant for production use.
package testworld

import (
	"context"
	"sync"

	"github.com/eriicafes/go-workflow/worlds"
)

const baseURL = "http://localhost/v1/workflow"

// World is the in-memory World. It is safe for concurrent use, and its base
// URL is http://localhost/v1/workflow.
type World struct {
	mu      sync.RWMutex
	ctx     context.Context
	cancel  context.CancelFunc
	runtime *worlds.Runtime
	runs    map[worlds.RunID]worlds.Run
	events  map[worlds.RunID][]worlds.Event
	steps   map[worlds.RunID]map[worlds.StepID]worlds.Step
	hooks   map[worlds.HookID]worlds.Hook
	tokens  map[string]worlds.HookID
	streams map[worlds.RunID]map[string]*memoryStream
	queued  []queuedMessage
	autoRun bool

	changesMu sync.Mutex
	changes   chan struct{}
}

// Close stops delivering queued work, and deliveries already running see
// their context cancelled. State stays readable.
func (w *World) Close()                      { w.cancel() }
func (*World) BaseURL() string               { return baseURL }
func (*World) SpecVersion() int              { return worlds.SpecVersion }
func (w *World) Analytics() worlds.Analytics { return analytics{world: w} }
func (*World) Capabilities() worlds.Capabilities {
	return worlds.Capabilities{Analytics: true, SetAttributes: true}
}

// New returns a World that executes deliveries for the workflows of runners
// in-process, each in its own goroutine once it is due.
func New(runners ...worlds.Runner) *World {
	return newWorld(true, runners...)
}

// NewManual returns a World that records deliveries without executing them.
// Tests execute them through QueuedMessages and acknowledge each with
// QueueCancel.
func NewManual(runners ...worlds.Runner) *World {
	return newWorld(false, runners...)
}

func newWorld(autoRun bool, runners ...worlds.Runner) *World {
	ctx, cancel := context.WithCancel(context.Background())
	w := &World{ctx: ctx, cancel: cancel, runs: make(map[worlds.RunID]worlds.Run), events: make(map[worlds.RunID][]worlds.Event), steps: make(map[worlds.RunID]map[worlds.StepID]worlds.Step), hooks: make(map[worlds.HookID]worlds.Hook), tokens: make(map[string]worlds.HookID), streams: make(map[worlds.RunID]map[string]*memoryStream), autoRun: autoRun, changes: make(chan struct{})}
	w.runtime = worlds.NewRuntime(w, worlds.Runners(runners...))
	return w
}

// notify wakes every waiter after a state mutation.
func (w *World) notify() {
	w.changesMu.Lock()
	close(w.changes)
	w.changes = make(chan struct{})
	w.changesMu.Unlock()
}

// changed returns a channel closed by the next state mutation. Waiters take it
// before reading so a concurrent mutation is never missed. Waiting on it
// instead of polling also lets testing/synctest advance its fake clock while a
// run sleeps.
func (w *World) changed() <-chan struct{} {
	w.changesMu.Lock()
	defer w.changesMu.Unlock()
	return w.changes
}
