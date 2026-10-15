// Package postgresworld provides a multi-process World backed by PostgreSQL.
//
// Every event append is one transaction that locks the run row, admits the
// event against the materialized views, inserts it at the next slot, and
// updates the views. Queue deliveries are durable rows claimed by Start
// workers with FOR UPDATE SKIP LOCKED; LISTEN/NOTIFY wakes workers and
// waiters, with polling as a fallback.
package postgresworld

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Config struct {
	// BaseURL is the absolute public URL of HTTP(world), including
	// /v1/workflow. Webhooks created by this World use it as their URL base.
	BaseURL string
	// URL is the PostgreSQL connection string used by Open. New accepts an
	// existing pool instead and ignores URL.
	URL string
	// PollInterval bounds how long waiters and Start workers sleep when no
	// notification arrives. Zero defaults to 250ms.
	PollInterval time.Duration
	// Concurrency caps the deliveries a Start worker executes at once. Zero
	// defaults to 10.
	Concurrency int
	// LeaseDuration is how long a claimed delivery stays invisible to other
	// workers. Workers renew it while executing, so it only bounds how fast a
	// crashed worker's delivery is retried. Zero defaults to 1 minute.
	LeaseDuration time.Duration
}

type World struct {
	pool        *pgxpool.Pool
	ownsPool    bool
	baseURL     string
	interval    time.Duration
	concurrency int
	lease       time.Duration
	notifier    *notifier
}

// Open connects to config.URL, migrates the schema, and returns a World that
// owns its pool.
func Open(ctx context.Context, config Config) (*World, error) {
	if config.URL == "" {
		return nil, errors.New("workflow/postgres: URL is required")
	}
	pool, err := pgxpool.New(ctx, config.URL)
	if err != nil {
		return nil, fmt.Errorf("workflow/postgres: open: %w", err)
	}
	world, err := New(ctx, pool, config)
	if err != nil {
		pool.Close()
		return nil, err
	}
	world.ownsPool = true
	return world, nil
}

// New migrates the schema and returns a World using pool. The caller keeps
// ownership of pool; Close does not close it.
func New(ctx context.Context, pool *pgxpool.Pool, config Config) (*World, error) {
	if pool == nil {
		return nil, errors.New("workflow/postgres: pool is required")
	}
	if config.BaseURL == "" {
		return nil, errors.New("workflow/postgres: base URL is required")
	}
	baseURL, err := worlds.NormalizeBaseURL(config.BaseURL)
	if err != nil {
		return nil, fmt.Errorf("workflow/postgres: invalid base URL: %w", err)
	}
	if config.PollInterval <= 0 {
		config.PollInterval = 250 * time.Millisecond
	}
	if config.Concurrency <= 0 {
		config.Concurrency = 10
	}
	if config.LeaseDuration <= 0 {
		config.LeaseDuration = time.Minute
	}
	if err := migrate(ctx, pool); err != nil {
		return nil, err
	}
	return &World{
		pool:        pool,
		baseURL:     baseURL,
		interval:    config.PollInterval,
		concurrency: config.Concurrency,
		lease:       config.LeaseDuration,
		notifier:    newNotifier(pool),
	}, nil
}

// Close stops the notification listener and closes the pool when Open
// created it.
func (w *World) Close() {
	w.notifier.close()
	if w.ownsPool {
		w.pool.Close()
	}
}

func (w *World) BaseURL() string             { return w.baseURL }
func (*World) SpecVersion() int              { return worlds.SpecVersion }
func (w *World) Analytics() worlds.Analytics { return analytics{world: w} }
func (*World) Capabilities() worlds.Capabilities {
	return worlds.Capabilities{Analytics: true, SetAttributes: true}
}

// HTTP serves the RPC and webhook endpoints under /v1/workflow.
func HTTP(world *World) http.Handler { return worlds.HTTPHandler(world, nil, "") }

// RPC serves only the CBOR RPC endpoint.
func RPC(world *World) http.Handler { return worlds.RPCHandler(world) }

// Webhook serves only the token webhook endpoint.
func Webhook(world *World) http.Handler { return worlds.WebhookHandler(world) }

// Start runs a queue worker until ctx is cancelled, then waits for in-flight
// deliveries and returns ctx.Err(). Any number of workers in any number of
// processes may run against the same database.
func Start(ctx context.Context, world *World, runners worlds.RunnerSet) error {
	return world.work(ctx, worlds.NewRuntime(world, runners))
}
