package postgresworld

import (
	"context"
	"testing"

	"github.com/eriicafes/go-workflow/worlds"
)

// SDK: packages/world-postgres/src/reenqueue.test.ts > re-enqueue active runs on start > does not enqueue anything when there are no active runs
func TestSDKRecoveryQueuesNothingWithoutActiveRuns(t *testing.T) {
	t.Parallel()
	world := newTestWorld(t)
	ctx := context.Background()
	if _, err := world.pool.Exec(ctx, `INSERT INTO workflow_runs (id, status, name, spec_version) VALUES ($1, 'completed', 'wf', 9)`, worlds.NewRunID()); err != nil {
		t.Fatal(err)
	}
	if err := world.recoverActiveRuns(ctx); err != nil {
		t.Fatal(err)
	}
	if rows := queueRows(t, world); rows != 0 {
		t.Fatalf("recovery queued %d messages", rows)
	}
}

// SDK: packages/world-postgres/src/reenqueue.test.ts > re-enqueue active runs on start > pages through all active runs
func TestSDKRecoveryQueuesEveryActiveRun(t *testing.T) {
	t.Parallel()
	world := newTestWorld(t)
	ctx := context.Background()
	want := map[worlds.RunID]bool{}
	for i := range 250 {
		id := worlds.NewRunID()
		status := []string{"pending", "running"}[i%2]
		if _, err := world.pool.Exec(ctx, `INSERT INTO workflow_runs (id, status, name, spec_version) VALUES ($1, $2, 'wf', 9)`, id, status); err != nil {
			t.Fatal(err)
		}
		want[id] = true
	}
	if err := world.recoverActiveRuns(ctx); err != nil {
		t.Fatal(err)
	}
	flows := recoveryFlows(t, world)
	if len(flows) != len(want) {
		t.Fatalf("recovered %d runs, want %d", len(flows), len(want))
	}
	for id := range flows {
		if !want[id] {
			t.Fatalf("recovered unknown run %s", id)
		}
	}
}

// SDK: packages/world/src/recovery.test.ts > reenqueueActiveRuns > stamps each message with its run's spec version
func TestSDKRecoveryStampsEachFlowWithItsRunsSpecVersion(t *testing.T) {
	t.Parallel()
	world := newTestWorld(t)
	ctx := context.Background()
	// 9 is the single-orchestrator spec version, and 8 the one before it.
	versions := map[worlds.RunID]int{worlds.NewRunID(): 9, worlds.NewRunID(): 8}
	for id, version := range versions {
		if _, err := world.pool.Exec(ctx, `INSERT INTO workflow_runs (id, status, name, spec_version, deployment_id) VALUES ($1, 'running', 'myWorkflow', $2, 'dpl_1')`, id, version); err != nil {
			t.Fatal(err)
		}
	}
	if err := world.recoverActiveRuns(ctx); err != nil {
		t.Fatal(err)
	}
	flows := recoveryFlows(t, world)
	for id, version := range versions {
		if flow, ok := flows[id]; !ok || flow.RunSpecVersion != version {
			t.Errorf("recovery flow of a spec %d run = %+v, %v", version, flow, ok)
		}
	}
}
