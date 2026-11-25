// Package testworld implements a World in memory, for tests. It keeps nothing
// across processes and is not meant for production use.
package testworld

import (
	"bytes"
	"context"
	"sync"

	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/core"
)

const baseURL = "http://localhost/v1/workflow"

// World is the in-memory World. It is safe for concurrent use, and its base
// URL is http://localhost/v1/workflow.
type World struct {
	mu sync.RWMutex
	// encryptionKey and encryptionScope are what SetEncryptionKey set.
	encryptionKey   []byte
	encryptionScope string
	ctx             context.Context
	cancel          context.CancelFunc
	executor        worlds.BoundExecutor
	runs            map[worlds.RunID]worlds.Run
	events          map[worlds.RunID][]worlds.Event
	steps           map[worlds.RunID]map[worlds.StepID]worlds.Step
	hooks           map[worlds.HookID]worlds.Hook
	// disposals holds each disposed hook's run and, after a forced claim,
	// the claimant. The hook itself is deleted.
	disposals map[worlds.HookID]hookDisposal
	tokens    map[string]worlds.HookID
	streams   map[worlds.RunID]map[string]*memoryStream
	queued    []queuedMessage
	// handedOff counts the handoffs of each message, which do not count as
	// attempts.
	handedOff map[string]int
	// resumes holds each deduplicated resume's digest and event, by run
	// and resume ID.
	resumes map[string]resumeClaim
	// startDecisions holds the refused start hook candidates.
	startDecisions map[worlds.RunID]worlds.StartHookDecision
	// flowGates lets one flow of each run execute at a time.
	flowGates map[worlds.RunID]chan struct{}
	autoRun   bool

	changesMu sync.Mutex
	changes   chan struct{}
}

// Close stops delivering queued work, and deliveries already running see
// their context cancelled. State stays readable.
func (w *World) Close()             { w.cancel() }
func (*World) BaseURL() string      { return baseURL }
func (*World) DeploymentID() string { return "" }
func (w *World) Analytics() worlds.Analytics {
	return analytics{LogAnalytics: worlds.LogAnalytics{World: w}, world: w}
}
func (w *World) Capabilities() worlds.Capabilities {
	return worlds.Capabilities{Analytics: true, SetAttributes: true, DeleteData: true, StartHook: true, HookResumeDedup: true}
}

// New returns a World that executes deliveries with executor in-process, each
// in its own goroutine once it is due: a worlds.RunnerSet runs them with Go
// workflows, and a worlds.RemoteExecutor sends them to a Workflow SDK app. A
// nil executor fails each run as unregistered.
func New(executor worlds.Executor) *World {
	return newWorld(true, executor)
}

// NewManual returns a World that records deliveries without executing them.
// Tests execute them with executor through QueuedMessages and acknowledge
// each with Acknowledge.
func NewManual(executor worlds.Executor) *World {
	return newWorld(false, executor)
}

func newWorld(autoRun bool, executor worlds.Executor) *World {
	ctx, cancel := context.WithCancel(context.Background())
	w := &World{ctx: ctx, cancel: cancel, runs: make(map[worlds.RunID]worlds.Run), events: make(map[worlds.RunID][]worlds.Event), steps: make(map[worlds.RunID]map[worlds.StepID]worlds.Step), hooks: make(map[worlds.HookID]worlds.Hook), disposals: make(map[worlds.HookID]hookDisposal), tokens: make(map[string]worlds.HookID), streams: make(map[worlds.RunID]map[string]*memoryStream), autoRun: autoRun, handedOff: make(map[string]int), resumes: make(map[string]resumeClaim), startDecisions: make(map[worlds.RunID]worlds.StartHookDecision), flowGates: make(map[worlds.RunID]chan struct{}), changes: make(chan struct{})}
	w.executor = worlds.BoundExecutor{Executor: executor, Runtime: core.NewRuntime(w)}
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

// SetEncryptionKey makes the World encrypt every run's payloads with a key
// derived from key in scope, as a World configured with an EncryptionKey and
// EncryptionScope does. Set it before starting runs.
func (w *World) SetEncryptionKey(key []byte, scope string) error {
	if err := worlds.CheckEncryptionKey(key); err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.encryptionKey, w.encryptionScope = bytes.Clone(key), scope
	return nil
}

// EncryptionKeyForRun returns the key of a run's payloads, derived from the
// key and scope SetEncryptionKey set, or nil when it set no key.
func (w *World) EncryptionKeyForRun(_ context.Context, runID worlds.RunID, _ string) ([]byte, error) {
	w.mu.RLock()
	key, scope := w.encryptionKey, w.encryptionScope
	w.mu.RUnlock()
	if len(key) == 0 {
		return nil, nil
	}
	return worlds.DeriveRunKey(key, scope, runID)
}

// resumeClaim is a deduplicated resume: its payload digest and the event it
// committed.
type resumeClaim struct {
	hookID  worlds.HookID
	digest  string
	eventID worlds.EventID
}

// hookDisposal records a disposed hook.
type hookDisposal struct {
	runID          worlds.RunID
	forceClaimedBy *worlds.HookForceClaim
}
