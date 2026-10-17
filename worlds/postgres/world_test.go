package postgresworld

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/workflowtest"
	"github.com/eriicafes/go-workflow/worlds"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const testBaseURL = "http://localhost/v1/workflow"

var schemaSequence atomic.Int64

func TestConformance(t *testing.T) {
	t.Parallel()
	workflowtest.Conformance(t, func(t *testing.T) worlds.World { return newTestWorld(t) })
}

func TestConformanceShared(t *testing.T) {
	t.Parallel()
	workflowtest.ConformanceShared(t, func(t *testing.T) (worlds.World, worlds.World) {
		url, schema := testSchema(t)
		return testWorld(t, testPool(t, url, schema), Config{}), testWorld(t, testPool(t, url, schema), Config{})
	})
}

func TestConfigValidation(t *testing.T) {
	t.Parallel()
	if _, err := Open(context.Background(), Config{BaseURL: testBaseURL}); err == nil {
		t.Fatal("Open without URL succeeded")
	}
	if _, err := New(context.Background(), nil, Config{BaseURL: testBaseURL}); err == nil {
		t.Fatal("New without pool succeeded")
	}
	url, schema := testSchema(t)
	pool := testPool(t, url, schema)
	if _, err := New(context.Background(), pool, Config{}); err == nil {
		t.Fatal("New without base URL succeeded")
	}
	if _, err := New(context.Background(), pool, Config{BaseURL: "/relative"}); err == nil {
		t.Fatal("New with relative base URL succeeded")
	}
}

type countInput struct{ N int }
type countOutput struct{ N int }

func (countInput) Workflow(countOutput) {}

// TestStartWorkersExecuteOnce runs two workers in separate Worlds over
// separate pools and checks every step executes exactly once.
func TestStartWorkersExecuteOnce(t *testing.T) {
	t.Parallel()
	url, schema := testSchema(t)
	a := testWorld(t, testPool(t, url, schema), Config{Concurrency: 3})
	b := testWorld(t, testPool(t, url, schema), Config{Concurrency: 3})

	var mu sync.Mutex
	executions := map[int]int{}
	step := func(_ context.Context, n int) (int, error) {
		mu.Lock()
		executions[n]++
		mu.Unlock()
		return n * 2, nil
	}
	runners := worlds.Runners(workflow.Use(func(ctx context.Context, input countInput) (countOutput, error) {
		value, err := workflow.Step(ctx, step, input.N)
		return countOutput{N: value}, err
	}))
	workerCtx, stop := context.WithCancel(context.Background())
	stopped := make(chan error, 2)
	for _, world := range []*World{a, b} {
		go func() { stopped <- Worker(workerCtx, world, runners) }()
	}

	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(a)), 30*time.Second)
	defer cancel()
	const runs = 12
	handles := make([]*workflow.Run[countOutput], runs)
	for i := range handles {
		run, err := workflow.Start(ctx, countInput{N: i})
		if err != nil {
			t.Fatal(err)
		}
		handles[i] = run
	}
	for i, run := range handles {
		result, err := run.Value(ctx)
		if err != nil {
			t.Fatalf("run %d: %v", i, err)
		}
		if result.N != i*2 {
			t.Fatalf("run %d result = %d", i, result.N)
		}
	}
	// Deliveries may still be settling after the runs turned terminal.
	var pending int
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		if err := a.pool.QueryRow(ctx, `SELECT count(*) FROM go_workflow_queue`).Scan(&pending); err != nil {
			t.Fatal(err)
		}
		if pending == 0 {
			break
		}
	}
	if pending != 0 {
		t.Fatalf("queue rows after completion = %d", pending)
	}
	stop()
	for range 2 {
		if err := <-stopped; !errors.Is(err, context.Canceled) {
			t.Fatalf("Start returned %v", err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	for i := range runs {
		if executions[i] != 1 {
			t.Fatalf("step %d executed %d times", i, executions[i])
		}
	}
}

func TestStartBacksOffFailedDelivery(t *testing.T) {
	t.Parallel()
	world := newTestWorld(t)
	ctx := context.Background()
	queued, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_missing"})
	if err != nil {
		t.Fatal(err)
	}
	workerCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- Worker(workerCtx, world, worlds.Runners()) }()
	deadline := time.Now().Add(10 * time.Second)
	var attempts int
	var notBefore time.Time
	var leased bool
	for time.Now().Before(deadline) {
		if err := world.pool.QueryRow(ctx, `SELECT attempts, not_before, lease_until IS NOT NULL FROM go_workflow_queue WHERE id = $1`, queued.MessageID).Scan(&attempts, &notBefore, &leased); err != nil {
			t.Fatal(err)
		}
		if attempts == 1 && !leased {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if attempts != 1 || leased || time.Until(notBefore) < 500*time.Millisecond {
		t.Fatalf("after failure attempts=%d leased=%v notBefore in %v", attempts, leased, time.Until(notBefore))
	}
	time.Sleep(200 * time.Millisecond)
	if err := world.pool.QueryRow(ctx, `SELECT attempts FROM go_workflow_queue WHERE id = $1`, queued.MessageID).Scan(&attempts); err != nil || attempts != 1 {
		t.Fatalf("redelivered before backoff: attempts=%d err=%v", attempts, err)
	}
	stop()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Start returned %v", err)
	}
}

func TestQueueIdempotencyKey(t *testing.T) {
	t.Parallel()
	world := newTestWorld(t)
	ctx := context.Background()
	later := time.Now().Add(time.Hour)
	first, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_a", IdempotencyKey: "key", NotBefore: &later})
	if err != nil {
		t.Fatal(err)
	}
	again, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_a", IdempotencyKey: "key", NotBefore: &later})
	if err != nil || again.MessageID != first.MessageID {
		t.Fatalf("duplicate key = %#v err=%v, want %s", again, err, first.MessageID)
	}
	other, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_b", IdempotencyKey: "key", NotBefore: &later})
	if err != nil || other.MessageID == first.MessageID {
		t.Fatalf("same key in another run = %#v err=%v", other, err)
	}
	// A claimed delivery no longer deduplicates: it may have read state the
	// new message must observe.
	if _, err := world.pool.Exec(ctx, `UPDATE go_workflow_queue SET not_before = now() WHERE id = $1`, first.MessageID); err != nil {
		t.Fatal(err)
	}
	claimed, err := world.claim(ctx, 10)
	if err != nil || len(claimed) != 1 || claimed[0].id != first.MessageID || claimed[0].input.IdempotencyKey != "key" {
		t.Fatalf("claimed = %#v err=%v", claimed, err)
	}
	fresh, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_a", IdempotencyKey: "key", NotBefore: &later})
	if err != nil || fresh.MessageID == first.MessageID {
		t.Fatalf("key after claim = %#v err=%v", fresh, err)
	}
	if again, err := world.claim(ctx, 10); err != nil || len(again) != 0 {
		t.Fatalf("leased delivery claimed again: %#v err=%v", again, err)
	}
}

func TestQueueKeepsReplayDivergenceCount(t *testing.T) {
	t.Parallel()
	world := newTestWorld(t)
	ctx := context.Background()
	stepID := worlds.StepID("step_a")
	if _, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_a", StepID: &stepID, ReplayDivergenceCount: 2}); err != nil {
		t.Fatal(err)
	}

	claimed, err := world.claim(ctx, 10)
	if err != nil || len(claimed) != 1 || claimed[0].input.ReplayDivergenceCount != 2 || *claimed[0].input.StepID != stepID {
		t.Fatalf("claimed = %#v err=%v", claimed, err)
	}
}

func TestRunsWaitUsesNotifications(t *testing.T) {
	t.Parallel()
	url, schema := testSchema(t)
	waiter := testWorld(t, testPool(t, url, schema), Config{PollInterval: time.Minute})
	writer := testWorld(t, testPool(t, url, schema), Config{})
	ctx := context.Background()
	runID := worlds.NewRunID()
	if _, err := writer.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "notify"}); err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(200 * time.Millisecond)
		_, _ = writer.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeRunCompleted}})
	}()
	started := time.Now()
	run, err := waiter.RunsWaitForTerminalStatus(ctx, worlds.RunsWaitForTerminalStatusInput{RunID: runID, Timeout: 30 * time.Second})
	if err != nil || run.Status != worlds.StatusCompleted {
		t.Fatalf("run = %#v err=%v", run, err)
	}
	if elapsed := time.Since(started); elapsed > 10*time.Second {
		t.Fatalf("wait took %v; notification not delivered", elapsed)
	}

	go func() {
		time.Sleep(200 * time.Millisecond)
		_ = writer.StreamsWrite(ctx, worlds.StreamsWriteInput{RunID: runID, Name: "out", Chunk: []byte("x")})
	}()
	started = time.Now()
	chunks, err := waiter.StreamsGetChunks(ctx, worlds.StreamsGetChunksInput{RunID: runID, Name: "out", Wait: 30 * time.Second})
	if err != nil || len(chunks.Data) != 1 {
		t.Fatalf("chunks = %#v err=%v", chunks, err)
	}
	if elapsed := time.Since(started); elapsed > 10*time.Second {
		t.Fatalf("stream wait took %v; notification not delivered", elapsed)
	}
}

// TestConcurrentForcedClaims races forced claims across two instances: run
// locks are taken in run ID order, so every claim succeeds and each one after
// the first displaces exactly one live owner.
func TestConcurrentForcedClaims(t *testing.T) {
	t.Parallel()
	url, schema := testSchema(t)
	a := testWorld(t, testPool(t, url, schema), Config{})
	b := testWorld(t, testPool(t, url, schema), Config{})
	ctx := context.Background()
	const claimants = 8
	runs := make([]worlds.RunID, claimants)
	for i := range runs {
		runs[i] = worlds.NewRunID()
		if _, err := a.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runs[i], WorkflowName: "force"}); err != nil {
			t.Fatal(err)
		}
	}
	var group sync.WaitGroup
	errs := make(chan error, claimants)
	for i, runID := range runs {
		world := a
		if i%2 == 1 {
			world = b
		}
		group.Add(1)
		go func() {
			defer group.Done()
			_, err := world.EventsCreate(ctx, worlds.EventsCreateInput{
				RunID: runID,
				Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeHookCreated, CorrelationID: fmt.Sprintf("hook_%d", i), Data: []byte(`{"token":"forced","force":true}`)},
			})
			errs <- err
		}()
	}
	group.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var forced, holders int
	if err := a.pool.QueryRow(ctx, `SELECT count(*) FROM go_workflow_events WHERE type = $1`, worlds.EventTypeHookDisposed).Scan(&forced); err != nil {
		t.Fatal(err)
	}
	if err := a.pool.QueryRow(ctx, `SELECT count(*) FROM go_workflow_hook_tokens WHERE token = 'forced'`).Scan(&holders); err != nil {
		t.Fatal(err)
	}
	if forced != claimants-1 || holders != 1 {
		t.Fatalf("hook_disposed events = %d, token holders = %d", forced, holders)
	}
}

// testSchema connects to DATABASE_URL, defaulting to a local Postgres on
// port 5432, and creates an empty schema dropped at cleanup, so tests sharing
// one database run in parallel without seeing each other's rows.
func testSchema(t *testing.T) (string, string) {
	t.Helper()
	url := cmp.Or(os.Getenv("DATABASE_URL"), "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable")
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatalf("Postgres is not reachable (set DATABASE_URL): %v", err)
	}
	defer conn.Close(ctx)
	schema := fmt.Sprintf("go_workflow_test_%d_%d", time.Now().UnixNano(), schemaSequence.Add(1))
	if _, err := conn.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		conn, err := pgx.Connect(ctx, url)
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close(ctx)
		if _, err := conn.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Error(err)
		}
	})
	return url, schema
}

func testPool(t *testing.T, url, schema string) *pgxpool.Pool {
	t.Helper()
	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	config.MaxConns = 4
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func testWorld(t *testing.T, pool *pgxpool.Pool, config Config) *World {
	t.Helper()
	config.BaseURL = testBaseURL
	if config.PollInterval == 0 {
		config.PollInterval = 50 * time.Millisecond
	}
	world, err := New(context.Background(), pool, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(world.Close)
	return world
}

func newTestWorld(t *testing.T) *World {
	url, schema := testSchema(t)
	return testWorld(t, testPool(t, url, schema), Config{})
}
