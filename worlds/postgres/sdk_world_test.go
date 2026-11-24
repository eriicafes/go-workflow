package postgresworld

import (
	"context"
	"testing"
	"time"
)

// TestSDKOpenReadsTheURLFromTheEnvironment sets the environment, so it does
// not run in parallel.
//
// SDK: packages/world-postgres/src/reenqueue.test.ts > re-enqueue active runs on start > falls back to DATABASE_URL when WORKFLOW_POSTGRES_URL is unset
// SDK: packages/world-postgres/src/reenqueue.test.ts > re-enqueue active runs on start > prefers WORKFLOW_POSTGRES_URL over DATABASE_URL
func TestSDKOpenReadsTheURLFromTheEnvironment(t *testing.T) {
	url, schema := testSchema(t)
	open := func() error {
		t.Helper()
		world, err := Open(context.Background(), Config{BaseURL: testBaseURL, Schema: schema})
		if err == nil {
			world.Close()
		}
		return err
	}
	t.Setenv("WORKFLOW_POSTGRES_URL", "")
	t.Setenv("DATABASE_URL", url)
	if err := open(); err != nil {
		t.Fatalf("Open with DATABASE_URL: %v", err)
	}
	t.Setenv("WORKFLOW_POSTGRES_URL", url)
	t.Setenv("DATABASE_URL", "postgres://nobody@127.0.0.1:1/none?sslmode=disable&connect_timeout=1")
	if err := open(); err != nil {
		t.Fatalf("Open with WORKFLOW_POSTGRES_URL: %v", err)
	}
}

// TestSDKPollIntervalDefaultsAndOverride sets the environment, so it does not
// run in parallel.
//
// SDK: packages/world-postgres/src/queue.test.ts > postgres queue http execution > defaults pollInterval to 500ms and honors an override from config
func TestSDKPollIntervalDefaultsAndOverride(t *testing.T) {
	url, schema := testSchema(t)
	pool := testPool(t, url, schema)
	t.Setenv("WORKFLOW_POSTGRES_POLL_INTERVAL_MS", "")
	for configured, want := range map[time.Duration]time.Duration{0: 500 * time.Millisecond, 2 * time.Second: 2 * time.Second} {
		world, err := New(context.Background(), pool, Config{BaseURL: testBaseURL, Schema: schema, PollInterval: configured})
		if err != nil {
			t.Fatal(err)
		}
		world.Close()
		if world.interval != want {
			t.Fatalf("poll interval with %s configured = %s, want %s", configured, world.interval, want)
		}
	}
}

// SDK: packages/world-postgres/src/reenqueue.test.ts > re-enqueue active runs on start > does not close a caller-owned pool
func TestSDKCloseLeavesACallerOwnedPoolOpen(t *testing.T) {
	t.Parallel()
	url, schema := testSchema(t)
	pool := testPool(t, url, schema)
	world, err := New(context.Background(), pool, Config{BaseURL: testBaseURL, Schema: schema})
	if err != nil {
		t.Fatal(err)
	}
	world.Close()
	if err := pool.Ping(context.Background()); err != nil {
		t.Fatalf("the caller's pool was closed: %v", err)
	}
}
