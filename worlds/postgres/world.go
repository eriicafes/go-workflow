// Package postgresworld implements a World on PostgreSQL, for production
// deployments where any number of application and worker processes share one
// database. Deliveries are rows in the database, executed by workers run with
// Worker.
package postgresworld

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/core"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Config configures the Postgres World.
type Config struct {
	// BaseURL is the absolute public URL of HTTP(world), including
	// /v1/workflow. Webhooks created by this World use it as their URL base.
	// It is required.
	BaseURL string
	// URL is the PostgreSQL connection string, used by Open and ignored by
	// New. Empty uses WORKFLOW_POSTGRES_URL, or DATABASE_URL.
	URL string
	// Schema is the schema the World's tables live in, created when missing.
	// Empty uses workflow.
	Schema string
	// HookRetentionLimit caps a hook's token retention. Zero uses
	// WORKFLOW_POSTGRES_HOOK_RETENTION_LIMIT_DAYS, or 30 days.
	HookRetentionLimit time.Duration
	// PollInterval bounds how long Worker loops and waiting reads sleep when
	// no notification arrives. Zero uses WORKFLOW_POSTGRES_POLL_INTERVAL_MS,
	// or 500ms.
	PollInterval time.Duration
	// Concurrency is the most deliveries one Worker call executes at once. Zero
	// uses WORKFLOW_POSTGRES_WORKER_CONCURRENCY, or 50.
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
	// Retention is how long a run's data is kept after the run completes,
	// fails, or is cancelled. The run's ExpiredAt is set when it finishes,
	// its payloads read as gone once that passes, and Worker deletes them in
	// the background: the run, step, event, and hook rows stay, and streams
	// are left empty and closed. Zero keeps the data. A run created with
	// $retention "0" loses its data as it finishes, whatever Retention is.
	Retention time.Duration
	// Retentions overrides Retention for the workflows it names, by workflow
	// name, as worlds.Retentions builds it. Zero keeps that workflow's runs
	// forever.
	Retentions map[string]time.Duration
	// EncryptionKey, 32 random bytes, encrypts every run's payloads with a
	// key derived from it for the run. Empty leaves payloads unencrypted.
	// Changing it leaves earlier runs' payloads unreadable.
	EncryptionKey []byte
	// EncryptionScope is the scope run keys are derived from EncryptionKey
	// in. Worlds that share an EncryptionKey keep their runs' keys apart with
	// different scopes. Changing it leaves earlier runs' payloads
	// unreadable.
	EncryptionScope string
	// SkipRecovery leaves the active runs alone when Worker starts, rather
	// than queueing a flow for each of them again.
	SkipRecovery bool
}

const (
	// defaultPollIntervalMS is Config.PollInterval's default, in
	// milliseconds.
	defaultPollIntervalMS = 500
	// defaultConcurrency is Config.Concurrency's default. A step that waits
	// for a child run holds its delivery meanwhile, so a low limit starves
	// workflows that fan out.
	defaultConcurrency = 50
)

// positiveIntEnv returns the positive integer the environment variable name
// holds, or zero.
func positiveIntEnv(name string) int {
	value, err := strconv.Atoi(os.Getenv(name))
	if err != nil || value <= 0 {
		return 0
	}
	return value
}

// DefaultQueue is the queue of workflows Config.Routes doesn't route.
const DefaultQueue = ""

// World is the Postgres World. It is safe for concurrent use, and any number
// of Worlds in any number of processes may share one database.
type World struct {
	qualifier
	pool     *pgxpool.Pool
	ownsPool bool
	baseURL  string
	// encryptionKey and encryptionScope are Config.EncryptionKey and
	// Config.EncryptionScope.
	encryptionKey   []byte
	encryptionScope string
	interval        time.Duration
	concurrency     int
	lease           time.Duration
	notifier        *notifier
	// announcing tracks the notifications sent after commits, which Close
	// waits for.
	announcing sync.WaitGroup
	routes     map[string]string
	retention  time.Duration
	retentions map[string]time.Duration
	// hookRetentionLimit is Config.HookRetentionLimit.
	hookRetentionLimit time.Duration
	// skipRecovery is Config.SkipRecovery.
	skipRecovery bool
	// recovery guards recovered, which is set once a Worker recovered the
	// active runs.
	recovery  sync.Mutex
	recovered bool
}

// Open connects to config.URL, migrates the schema, and returns a World that
// owns the connection pool and closes it on Close.
func Open(ctx context.Context, config Config) (*World, error) {
	if config.URL == "" {
		config.URL = cmp.Or(os.Getenv("WORKFLOW_POSTGRES_URL"), os.Getenv("DATABASE_URL"))
	}
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
	if err := worlds.CheckEncryptionKey(config.EncryptionKey); err != nil {
		return nil, err
	}
	baseURL, err := worlds.NormalizeBaseURL(config.BaseURL)
	if err != nil {
		return nil, fmt.Errorf("workflow/postgres: invalid base URL: %w", err)
	}
	if config.PollInterval <= 0 {
		config.PollInterval = time.Duration(cmp.Or(positiveIntEnv("WORKFLOW_POSTGRES_POLL_INTERVAL_MS"), defaultPollIntervalMS)) * time.Millisecond
	}
	if config.Concurrency <= 0 {
		config.Concurrency = cmp.Or(positiveIntEnv("WORKFLOW_POSTGRES_WORKER_CONCURRENCY"), defaultConcurrency)
	}
	if config.LeaseDuration <= 0 {
		config.LeaseDuration = time.Minute
	}
	if _, negative := worlds.MaxRetention(config.Retention, config.Retentions); negative {
		return nil, errors.New("workflow/postgres: retention cannot be negative")
	}
	limit, err := hookRetentionLimit(config.HookRetentionLimit)
	if err != nil {
		return nil, err
	}
	schema := newQualifier(cmp.Or(config.Schema, defaultSchema))
	if err := migrate(ctx, pool, schema); err != nil {
		return nil, err
	}
	return &World{
		qualifier:          schema,
		hookRetentionLimit: limit,
		pool:               pool,
		baseURL:            baseURL,
		encryptionKey:      bytes.Clone(config.EncryptionKey),
		encryptionScope:    config.EncryptionScope,
		interval:           config.PollInterval,
		concurrency:        config.Concurrency,
		lease:              config.LeaseDuration,
		notifier:           newNotifier(pool),
		routes:             config.Routes,
		retention:          config.Retention,
		retentions:         config.Retentions,
		skipRecovery:       config.SkipRecovery,
	}, nil
}

// Close releases the World's notification connection and closes the
// connection pool when Open created it. Stop Worker loops first by
// cancelling their context.
func (w *World) Close() {
	w.announcing.Wait()
	w.notifier.close()
	if w.ownsPool {
		w.pool.Close()
	}
}

func (w *World) BaseURL() string    { return w.baseURL }
func (*World) DeploymentID() string { return "" }
func (w *World) Analytics() worlds.Analytics {
	return analytics{LogAnalytics: worlds.LogAnalytics{World: w}, world: w}
}
func (*World) Capabilities() worlds.Capabilities {
	return worlds.Capabilities{Analytics: true, SetAttributes: true, DeleteData: true, StartHook: true, HookResumeDedup: true}
}

// HTTP returns a handler serving the client and webhook endpoints under
// /v1/workflow. Its client endpoints are unauthenticated unless options
// protect them, such as worlds.WithClientToken.
func HTTP(world *World, options ...worlds.HTTPOption) http.Handler {
	return HTTPPrefix(world, "", options...)
}

// HTTPPrefix is like HTTP but serves the endpoints under prefix, such as
// /internal/workflow. An empty prefix uses /v1/workflow. Config.BaseURL must
// end with the same prefix.
func HTTPPrefix(world *World, prefix string, options ...worlds.HTTPOption) http.Handler {
	return worlds.HTTPHandler(world, prefix, options...)
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
// share one database, and each delivery runs on one worker at a time. With
// Config.Retention set, it also deletes expired runs in the background. The
// last options apply.
//
// The World's first Worker queues a flow for every active run first, unless
// Config.SkipRecovery: a run whose process exited before it queued the run's
// delivery resumes. Runs parked on a hook or a wait are left alone, and a
// run's recovery flow is queued once while it lives, however many processes
// start. A process that runs no Worker recovers nothing.
func Worker(ctx context.Context, world *World, runners worlds.RunnerSet, options ...WorkerOptions) error {
	var option WorkerOptions
	if len(options) > 0 {
		option = options[len(options)-1]
	}
	if option.Concurrency <= 0 {
		option.Concurrency = world.concurrency
	}
	if err := world.recoverOnce(ctx); err != nil {
		return fmt.Errorf("workflow/postgres: recover active runs: %w", err)
	}
	if longest, _ := worlds.MaxRetention(world.retention, world.retentions); longest > 0 {
		done := make(chan struct{})
		defer func() { <-done }()
		go func() {
			defer close(done)
			world.expireLoop(ctx)
		}()
	}
	return world.work(ctx, core.NewRuntime(world, runners), option)
}

// recoverOnce recovers the active runs unless Config.SkipRecovery or a
// Worker of the World already did.
func (w *World) recoverOnce(ctx context.Context) error {
	if w.skipRecovery {
		return nil
	}
	w.recovery.Lock()
	defer w.recovery.Unlock()
	if w.recovered {
		return nil
	}
	if err := w.recoverActiveRuns(ctx); err != nil {
		return err
	}
	w.recovered = true
	return nil
}

// EncryptionKeyForRun returns the key of a run's payloads, derived from
// Config.EncryptionKey in Config.EncryptionScope, or nil when the World has
// no key.
func (w *World) EncryptionKeyForRun(_ context.Context, runID worlds.RunID, _ string) ([]byte, error) {
	if len(w.encryptionKey) == 0 {
		return nil, nil
	}
	return worlds.DeriveRunKey(w.encryptionKey, w.encryptionScope, runID)
}

// hookRetentionLimit returns the cap on a hook's token retention:
// configured, WORKFLOW_POSTGRES_HOOK_RETENTION_LIMIT_DAYS, or 30 days.
func hookRetentionLimit(configured time.Duration) (time.Duration, error) {
	if configured > 0 {
		return configured, nil
	}
	if raw := os.Getenv("WORKFLOW_POSTGRES_HOOK_RETENTION_LIMIT_DAYS"); raw != "" {
		days, err := strconv.ParseFloat(raw, 64)
		if err != nil || days <= 0 {
			return 0, errors.New("workflow/postgres: WORKFLOW_POSTGRES_HOOK_RETENTION_LIMIT_DAYS must be a positive number")
		}
		return time.Duration(days * float64(24*time.Hour)), nil
	}
	return 30 * 24 * time.Hour, nil
}
