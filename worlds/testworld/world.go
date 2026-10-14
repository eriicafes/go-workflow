// Package testworld provides the in-memory reference World used by tests.
package testworld

import (
	"context"
	"sync"

	"github.com/eriicafes/go-workflow/worlds"
)

// World is the in-memory reference World. Production Worlds use durable
// provider storage; this implementation is reserved for tests.
type World struct {
	mu       sync.RWMutex
	ctx      context.Context
	cancel   context.CancelFunc
	runtime  *worlds.Runtime
	runs     map[worlds.RunID]worlds.Run
	events   map[worlds.RunID][]worlds.Event
	steps    map[worlds.RunID]map[worlds.StepID]worlds.Step
	hooks    map[worlds.HookID]worlds.Hook
	tokens   map[string]worlds.HookID
	streams  map[worlds.RunID]map[worlds.StreamNamespace]*memoryStream
	queued   []queuedMessage
	autoRun  bool
	onChange func(Snapshot)
}

// Snapshot is the portable event/projection state used by provider Worlds.
// Writer sessions are deliberately excluded: a process restart releases every
// lease and lets the owning step acquire a fresh one.
type Snapshot struct {
	Runs    map[worlds.RunID]worlds.Run
	Events  map[worlds.RunID][]worlds.Event
	Steps   map[worlds.RunID]map[worlds.StepID]worlds.Step
	Hooks   map[worlds.HookID]worlds.Hook
	Tokens  map[string]worlds.HookID
	Streams map[worlds.RunID]map[worlds.StreamNamespace]StreamSnapshot
	Queued  []QueueSnapshot
}

type StreamSnapshot struct {
	Chunks [][]byte
	Closed bool
}

type QueueSnapshot struct {
	ID    string
	Input worlds.QueueInput
}

func (w *World) Close()                      { w.cancel() }
func (*World) SpecVersion() int              { return worlds.SpecVersion }
func (w *World) Analytics() worlds.Analytics { return analytics{world: w} }
func (*World) Capabilities() worlds.Capabilities {
	return worlds.Capabilities{Analytics: true, SetAttributes: true}
}

func New(runners ...worlds.Runner) *World {
	return newWorld(true, nil, runners...)
}

// NewWithSnapshot restores a World and calls onChange after every state
// mutation. Provider implementations use it to durably persist the portable
// state without duplicating the reference projections.
func NewWithSnapshot(snapshot Snapshot, onChange func(Snapshot), runners ...worlds.Runner) *World {
	return newWorld(true, onChange, runners...).restore(snapshot)
}

// NewWithSnapshotAndQueue is the provider variant of NewWithSnapshot. Its
// caller owns queue delivery, so Queue records work but does not execute it in
// this process.
func NewWithSnapshotAndQueue(snapshot Snapshot, onChange func(Snapshot), runners ...worlds.Runner) *World {
	return newWorld(false, onChange, runners...).restore(snapshot)
}

func newWorld(autoRun bool, onChange func(Snapshot), runners ...worlds.Runner) *World {
	ctx, cancel := context.WithCancel(context.Background())
	w := &World{ctx: ctx, cancel: cancel, runs: make(map[worlds.RunID]worlds.Run), events: make(map[worlds.RunID][]worlds.Event), steps: make(map[worlds.RunID]map[worlds.StepID]worlds.Step), hooks: make(map[worlds.HookID]worlds.Hook), tokens: make(map[string]worlds.HookID), streams: make(map[worlds.RunID]map[worlds.StreamNamespace]*memoryStream), autoRun: autoRun, onChange: onChange}
	w.runtime = worlds.NewRuntime(w, worlds.Runners(runners...))
	return w
}

// NewFromSnapshot restores an event World from a previously captured state.
func NewFromSnapshot(snapshot Snapshot, runners ...worlds.Runner) *World {
	return newWorld(true, nil, runners...).restore(snapshot)
}

func (w *World) restore(snapshot Snapshot) *World {
	w.mu.Lock()
	defer w.mu.Unlock()
	if snapshot.Runs != nil {
		w.runs = snapshot.Runs
	}
	if snapshot.Events != nil {
		w.events = snapshot.Events
	}
	if snapshot.Steps != nil {
		w.steps = snapshot.Steps
	}
	if snapshot.Hooks != nil {
		w.hooks = snapshot.Hooks
	}
	if snapshot.Tokens != nil {
		w.tokens = snapshot.Tokens
	}
	w.streams = make(map[worlds.RunID]map[worlds.StreamNamespace]*memoryStream, len(snapshot.Streams))
	for runID, values := range snapshot.Streams {
		w.streams[runID] = make(map[worlds.StreamNamespace]*memoryStream, len(values))
		for namespace, value := range values {
			w.streams[runID][namespace] = &memoryStream{chunks: value.Chunks, closed: value.Closed}
		}
	}
	w.queued = make([]queuedMessage, len(snapshot.Queued))
	for index, value := range snapshot.Queued {
		w.queued[index] = queuedMessage{id: value.ID, input: value.Input}
	}
	return w
}

func (w *World) notify() {
	if w.onChange != nil {
		w.onChange(w.Snapshot())
	}
}

// Snapshot returns a deep copy safe to persist outside the World.
func (w *World) Snapshot() Snapshot {
	w.mu.RLock()
	defer w.mu.RUnlock()
	result := Snapshot{Runs: make(map[worlds.RunID]worlds.Run, len(w.runs)), Events: make(map[worlds.RunID][]worlds.Event, len(w.events)), Steps: make(map[worlds.RunID]map[worlds.StepID]worlds.Step, len(w.steps)), Hooks: make(map[worlds.HookID]worlds.Hook, len(w.hooks)), Tokens: make(map[string]worlds.HookID, len(w.tokens)), Streams: make(map[worlds.RunID]map[worlds.StreamNamespace]StreamSnapshot, len(w.streams)), Queued: make([]QueueSnapshot, len(w.queued))}
	for id, value := range w.runs {
		result.Runs[id] = cloneRun(value)
	}
	for id, values := range w.events {
		result.Events[id] = make([]worlds.Event, len(values))
		for i, value := range values {
			result.Events[id][i] = cloneEvent(value)
		}
	}
	for runID, values := range w.steps {
		result.Steps[runID] = make(map[worlds.StepID]worlds.Step, len(values))
		for id, value := range values {
			result.Steps[runID][id] = value
		}
	}
	for id, value := range w.hooks {
		result.Hooks[id] = cloneHook(value)
	}
	for token, id := range w.tokens {
		result.Tokens[token] = id
	}
	for runID, values := range w.streams {
		result.Streams[runID] = make(map[worlds.StreamNamespace]StreamSnapshot, len(values))
		for namespace, value := range values {
			chunks := make([][]byte, len(value.chunks))
			for i, chunk := range value.chunks {
				chunks[i] = append([]byte(nil), chunk...)
			}
			result.Streams[runID][namespace] = StreamSnapshot{Chunks: chunks, Closed: value.closed}
		}
	}
	for i, value := range w.queued {
		result.Queued[i] = QueueSnapshot{ID: value.id, Input: value.input}
	}
	return result
}
