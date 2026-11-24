package postgresworld

import (
	"context"
	"testing"

	"github.com/eriicafes/go-workflow/worlds"
)

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
