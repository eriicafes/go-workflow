// Package testworld provides the in-memory reference World used by tests.
package testworld

import (
	"context"
	"sync"

	"github.com/eriicafes/go-workflow/worlds"
)

const baseURL = "http://localhost/v1/workflow"

// World is the in-memory reference World. Production Worlds use durable
// provider storage; this implementation is reserved for tests.
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
}

func (w *World) Close()                      { w.cancel() }
func (*World) BaseURL() string               { return baseURL }
func (*World) SpecVersion() int              { return worlds.SpecVersion }
func (w *World) Analytics() worlds.Analytics { return analytics{world: w} }
func (*World) Capabilities() worlds.Capabilities {
	return worlds.Capabilities{Analytics: true, SetAttributes: true}
}

// New returns a World that delivers queued work in-process.
func New(runners ...worlds.Runner) *World {
	return newWorld(true, runners...)
}

// NewManual returns a World whose Queue only records deliveries. Tests drive
// delivery themselves through QueuedMessages.
func NewManual(runners ...worlds.Runner) *World {
	return newWorld(false, runners...)
}

// notify marks state mutations. The in-memory World has nothing to persist.
func (w *World) notify() {}

func newWorld(autoRun bool, runners ...worlds.Runner) *World {
	ctx, cancel := context.WithCancel(context.Background())
	w := &World{ctx: ctx, cancel: cancel, runs: make(map[worlds.RunID]worlds.Run), events: make(map[worlds.RunID][]worlds.Event), steps: make(map[worlds.RunID]map[worlds.StepID]worlds.Step), hooks: make(map[worlds.HookID]worlds.Hook), tokens: make(map[string]worlds.HookID), streams: make(map[worlds.RunID]map[string]*memoryStream), autoRun: autoRun}
	w.runtime = worlds.NewRuntime(w, worlds.Runners(runners...))
	return w
}
