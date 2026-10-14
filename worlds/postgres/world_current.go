// Package postgresworld provides the event-sourced Postgres World.
package postgresworld

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/testworld"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Config struct {
	URL          string
	PollInterval time.Duration
}
type World struct {
	*testworld.World
	pool     *pgxpool.Pool
	ownsPool bool
	interval time.Duration
	reloadMu sync.Mutex
}

func Open(ctx context.Context, config Config, runners ...worlds.Runner) (*World, error) {
	if config.URL == "" {
		return nil, errors.New("workflow/postgres: URL is required")
	}
	pool, err := pgxpool.New(ctx, config.URL)
	if err != nil {
		return nil, err
	}
	w, err := New(ctx, pool, config, runners...)
	if err != nil {
		pool.Close()
		return nil, err
	}
	w.ownsPool = true
	return w, nil
}
func New(ctx context.Context, pool *pgxpool.Pool, config Config, runners ...worlds.Runner) (*World, error) {
	if pool == nil {
		return nil, errors.New("workflow/postgres: pool is required")
	}
	if config.PollInterval <= 0 {
		config.PollInterval = 250 * time.Millisecond
	}
	if err := ensureSchema(ctx, pool); err != nil {
		return nil, err
	}
	snapshot, err := loadSnapshot(ctx, pool)
	if err != nil {
		return nil, err
	}
	w := &World{pool: pool, interval: config.PollInterval}
	w.World = testworld.NewWithSnapshotAndQueue(snapshot, w.save, runners...)
	return w, nil
}

func ensureSchema(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS go_workflow_world_state (
			id BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id),
			state JSONB NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`)
	return err
}

func loadSnapshot(ctx context.Context, pool *pgxpool.Pool) (testworld.Snapshot, error) {
	var data []byte
	err := pool.QueryRow(ctx, `SELECT state FROM go_workflow_world_state WHERE id = TRUE`).Scan(&data)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return testworld.Snapshot{}, nil
		}
		return testworld.Snapshot{}, err
	}
	var snapshot testworld.Snapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return testworld.Snapshot{}, errors.New("workflow/postgres: invalid world state: " + err.Error())
	}
	return snapshot, nil
}

func (w *World) save(snapshot testworld.Snapshot) {
	data, err := json.Marshal(snapshot)
	if err != nil {
		return
	}
	_, _ = w.pool.Exec(context.Background(), `
		INSERT INTO go_workflow_world_state (id, state) VALUES (TRUE, $1::jsonb)
		ON CONFLICT (id) DO UPDATE SET state = EXCLUDED.state, updated_at = now()`, data)
}

// reload refreshes the worker view before it claims work. A worker and API
// server commonly run in different processes, so queue state cannot be kept
// only in the constructor's in-memory snapshot.
func (w *World) reload(ctx context.Context) error {
	w.reloadMu.Lock()
	defer w.reloadMu.Unlock()
	snapshot, err := loadSnapshot(ctx, w.pool)
	if err != nil {
		return err
	}
	w.World = testworld.NewWithSnapshotAndQueue(snapshot, w.save)
	return nil
}
func (w *World) Close() {
	w.World.Close()
	if w.ownsPool {
		w.pool.Close()
	}
}
func HTTP(w *World) http.Handler { return worlds.HTTPHandler(w, nil, "") }

// Run drains durable queue records. It is intentionally safe to run in a
// separate process from the HTTP API: each state mutation is persisted in
// Postgres before the next polling cycle.
func Run(ctx context.Context, world *World, runners worlds.RunnerSet) error {
	runtime := worlds.NewRuntime(world, runners)
	for {
		if err := world.reload(ctx); err != nil {
			return err
		}
		for _, message := range world.QueuedMessages() {
			if message.Input.NotBefore != nil && time.Now().Before(*message.Input.NotBefore) {
				continue
			}
			if err := runtime.Execute(ctx, message.Input); err == nil {
				if err := world.QueueCancel(ctx, worlds.QueueCancelInput{MessageID: message.ID}); err != nil && !errors.Is(err, worlds.ErrQueueMessageNotFound) {
					return err
				}
			}
		}
		timer := time.NewTimer(world.interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
