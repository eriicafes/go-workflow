// Package postgresworld implements a World on PostgreSQL, for production
// deployments where any number of application and worker processes share one
// database. Deliveries are rows in the database, executed by workers run with
// Worker.
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

// Config configures the Postgres World.
type Config struct {
	// BaseURL is the absolute public URL of HTTP(world), including
	// /v1/workflow. Webhooks created by this World use it as their URL base.
	// It is required.
	BaseURL string
	// URL is the PostgreSQL connection string. It is required by Open and
	// ignored by New.
	URL string
	// PollInterval bounds how long Worker loops and waiting reads sleep when
	// no notification arrives. Zero uses 250ms.
	PollInterval time.Duration
	// Concurrency is the most deliveries one Worker call executes at once. Zero
	// uses 10.
	Concurrency int
	// LeaseDuration is how long a claimed delivery stays hidden from other
	// workers. Workers renew it while executing, so it only bounds how soon a
	// crashed worker's delivery is retried. Zero uses 1 minute.
	LeaseDuration time.Duration
	// Routes queues the deliveries of each workflow named by workflow name,
	// as worlds.Routes builds it, on the queue it names, for workers that
	// claim that queue (WorkerOptions.Queues). Other workflows are queued on
	// DefaultQueue.
	Routes map[string]string
}

// DefaultQueue is the queue of workflows Config.Routes doesn't route.
const DefaultQueue = ""

// World is the Postgres World. It is safe for concurrent use, and any number
// of Worlds in any number of processes may share one database.
type World struct {
	pool        *pgxpool.Pool
	ownsPool    bool
	baseURL     string
	interval    time.Duration
	concurrency int
	lease       time.Duration
	notifier    *notifier
	routes      map[string]string
}

// Open connects to config.URL, migrates the schema, and returns a World that
// owns the connection pool and closes it on Close.
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

// New migrates the schema in pool's database and returns a World that uses
// pool. The caller keeps owning pool, so Close does not close it.
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
		routes:      config.Routes,
	}, nil
}

// Close releases the World's notification connection and closes the
// connection pool when Open created it. Stop Worker loops first by
// cancelling their context.
func (w *World) Close() {
	w.notifier.close()
	if w.ownsPool {
		w.pool.Close()
	}
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

// WorkerOptions configures one Worker.
type WorkerOptions struct {
	// Queues are the queues the worker claims deliveries from, such as
	// DefaultQueue and the queues Config.Routes names. Empty claims every
	// queue.
	Queues []string
	// Concurrency is the most deliveries the worker executes at once. Zero
	// uses Config.Concurrency.
	Concurrency int
}

// Worker runs the loop that executes deliveries for the workflows of runners.
// It blocks until ctx is cancelled, waits for in-flight deliveries to return,
// and returns ctx.Err(). Any number of workers in any number of processes may
// share one database, and each delivery runs on one worker at a time. The
// last options apply.
func Worker(ctx context.Context, world *World, runners worlds.RunnerSet, options ...WorkerOptions) error {
	var option WorkerOptions
	if len(options) > 0 {
		option = options[len(options)-1]
	}
	if option.Concurrency <= 0 {
		option.Concurrency = world.concurrency
	}
	return world.work(ctx, worlds.NewRuntime(world, runners), option)
}
