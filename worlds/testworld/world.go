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
	mu      sync.RWMutex
	ctx     context.Context
	cancel  context.CancelFunc
	engine  *worlds.Engine
	runs    map[worlds.RunID]worlds.Run
	events  map[worlds.RunID][]worlds.Event
	steps   map[worlds.RunID]map[worlds.StepID]worlds.Step
	hooks   map[worlds.HookID]worlds.Hook
	tokens  map[string]worlds.HookID
	streams map[worlds.RunID]map[worlds.StreamID]*memoryStream
	queued  []queuedMessage
}

func (w *World) Close()                      { w.cancel() }
func (*World) SpecVersion() int              { return worlds.SpecVersion }
func (w *World) Analytics() worlds.Analytics { return analytics{world: w} }
func (*World) Capabilities() worlds.Capabilities {
	return worlds.Capabilities{Analytics: true, SetAttributes: true}
}

func New(runners ...worlds.Runner) *World {
	ctx, cancel := context.WithCancel(context.Background())
	w := &World{ctx: ctx, cancel: cancel, runs: make(map[worlds.RunID]worlds.Run), events: make(map[worlds.RunID][]worlds.Event), steps: make(map[worlds.RunID]map[worlds.StepID]worlds.Step), hooks: make(map[worlds.HookID]worlds.Hook), tokens: make(map[string]worlds.HookID), streams: make(map[worlds.RunID]map[worlds.StreamID]*memoryStream)}
	w.engine = worlds.NewEngine(w, worlds.Runners(runners...))
	return w
}
