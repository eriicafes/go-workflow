// Package localworld provides the local event-sourced World.
package localworld

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"sync"

	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/testworld"
)

// World is the local implementation of the current Worlds contract.
//
// It is implemented around event records and materialized views.
type World struct {
	*testworld.World
	path   string
	saveMu sync.Mutex
}

// New creates an in-process local World. Use Open when state must survive a
// process restart.
func New(runners worlds.RunnerSet) *World {
	return &World{World: testworld.New(runnerValues(runners)...)}
}

// Open opens a local durable World rooted at dir. The current schema is a
// single portable event/projection snapshot and has no legacy-store fallback.
func Open(dir string, runners worlds.RunnerSet) (*World, error) {
	if dir == "" {
		return nil, errors.New("workflow/local: storage path is required")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	w := &World{path: filepath.Join(dir, "world.json")}
	snapshot, err := loadSnapshot(w.path)
	if err != nil {
		return nil, err
	}
	w.World = testworld.NewWithSnapshot(snapshot, w.save, runnerValues(runners)...)
	return w, nil
}

func loadSnapshot(path string) (testworld.Snapshot, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return testworld.Snapshot{}, nil
	}
	if err != nil {
		return testworld.Snapshot{}, err
	}
	var snapshot testworld.Snapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return testworld.Snapshot{}, errors.New("workflow/local: invalid world state: " + err.Error())
	}
	return snapshot, nil
}

func (w *World) save(snapshot testworld.Snapshot) {
	data, err := json.Marshal(snapshot)
	if err != nil {
		return
	}
	w.saveMu.Lock()
	defer w.saveMu.Unlock()
	temporary := w.path + ".tmp"
	if os.WriteFile(temporary, data, 0o600) == nil {
		_ = os.Rename(temporary, w.path)
	}
}

func runnerValues(set worlds.RunnerSet) []worlds.Runner {
	values := make([]worlds.Runner, 0, len(set))
	for _, runner := range set {
		values = append(values, runner)
	}
	return values
}

func (w *World) Close() error { w.World.Close(); return nil }
func (w *World) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	w.World.DispatchQueued(ctx)
	return nil
}
func HTTP(w *World) http.Handler { return worlds.HTTPHandler(w, nil, "") }
