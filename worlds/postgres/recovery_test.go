package postgresworld

import (
	"context"
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/core"
)

// recoveryFixture inserts runs in every state recovery tells apart and
// returns the IDs of those it recovers.
func recoveryFixture(t *testing.T, world *World) []worlds.RunID {
	t.Helper()
	ctx := context.Background()
	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := world.pool.Exec(ctx, query, args...); err != nil {
			t.Fatal(err)
		}
	}
	type run struct {
		status   string
		spec     int
		hook     bool
		wait     string // "", "future", or "due"
		step     string // "", or the step's status
		flow     string // "", "due", "leased", or "future"
		recovers bool
	}
	runs := map[string]run{
		"pending":                 {status: "pending", spec: 9, recovers: true},
		"running":                 {status: "running", spec: 9, recovers: true},
		"parked on a hook":        {status: "running", spec: 9, hook: true},
		"parked on a wait":        {status: "running", spec: 9, wait: "future"},
		"parked with a timer":     {status: "running", spec: 9, wait: "future", flow: "future"},
		"parked, step completed":  {status: "running", spec: 9, hook: true, step: "completed"},
		"hook and a running step": {status: "running", spec: 9, hook: true, step: "running", recovers: true},
		"hook and a pending step": {status: "running", spec: 9, hook: true, step: "pending", recovers: true},
		"wait due":                {status: "running", spec: 9, wait: "due", recovers: true},
		"hook and a due flow":     {status: "running", spec: 9, hook: true, flow: "due", recovers: true},
		"hook and a leased flow":  {status: "running", spec: 9, hook: true, flow: "leased", recovers: true},
		"legacy on a hook":        {status: "running", spec: 1, hook: true, recovers: true},
		"completed":               {status: "completed", spec: 9},
		"failed":                  {status: "failed", spec: 9},
		"cancelled":               {status: "cancelled", spec: 9},
	}
	var recovered []worlds.RunID
	for name, value := range runs {
		id := worlds.NewRunID()
		exec(`INSERT INTO workflow_runs (id, status, name, spec_version, deployment_id) VALUES ($1, $2, $3, $4, 'dpl_1')`, id, value.status, name, value.spec)
		if value.hook {
			exec(`INSERT INTO workflow_hooks (run_id, hook_id, token, metadata_slot, resume_context) VALUES ($1, $2, $2, 1, '')`, id, "hook_"+string(id))
		}
		switch value.wait {
		case "future":
			exec(`INSERT INTO workflow_waits (wait_id, run_id, status, resume_at) VALUES ($1, $2, 'waiting', now() + interval '1 hour')`, "wait_"+string(id), id)
		case "due":
			exec(`INSERT INTO workflow_waits (wait_id, run_id, status, resume_at) VALUES ($1, $2, 'waiting', now() - interval '1 second')`, "wait_"+string(id), id)
		}
		if value.step != "" {
			exec(`INSERT INTO workflow_steps (run_id, step_id, step_name, status, input_slot) VALUES ($1, 'step_a', 'step', $2, 1)`, id, value.step)
		}
		switch value.flow {
		case "due":
			exec(`INSERT INTO workflow_queue (id, run_id, payload, serialized, not_before) VALUES ($1, $2, '{}', true, now())`, newMessageID(), id)
		case "leased":
			exec(`INSERT INTO workflow_queue (id, run_id, payload, serialized, not_before, lease_until, running) VALUES ($1, $2, '{}', true, now() - interval '1 minute', now() + interval '1 minute', true)`, newMessageID(), id)
		case "future":
			exec(`INSERT INTO workflow_queue (id, run_id, payload, serialized, not_before) VALUES ($1, $2, '{}', true, now() + interval '1 hour')`, newMessageID(), id)
		}
		if value.recovers {
			recovered = append(recovered, id)
		}
	}
	slices.Sort(recovered)
	return recovered
}

// recoveryFlows returns the startup recovery flows queued, by run.
func recoveryFlows(t *testing.T, world *World) map[worlds.RunID]worlds.QueueInput {
	t.Helper()
	rows, err := world.pool.Query(context.Background(), `SELECT payload FROM workflow_queue WHERE idempotency_key LIKE 'startup-recovery:%'`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	flows := map[worlds.RunID]worlds.QueueInput{}
	for rows.Next() {
		var payload []byte
		var stored storedDelivery
		if err := rows.Scan(&payload); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(payload, &stored); err != nil {
			t.Fatal(err)
		}
		if _, ok := flows[stored.RunID]; ok {
			t.Fatalf("run %s has two recovery flows", stored.RunID)
		}
		flows[stored.RunID] = stored.QueueInput
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return flows
}

// TestRecoverActiveRuns checks recovery queues one flow, keyed by its run,
// for every active run but those parked on a hook or a wait, however many
// times it runs.
func TestRecoverActiveRuns(t *testing.T) {
	t.Parallel()
	world := newTestWorld(t)
	ctx := context.Background()
	want := recoveryFixture(t, world)
	for range 2 {
		if err := world.recoverActiveRuns(ctx); err != nil {
			t.Fatal(err)
		}
	}
	flows := recoveryFlows(t, world)
	var got []worlds.RunID
	for id, flow := range flows {
		got = append(got, id)
		if flow.IdempotencyKey != "startup-recovery:"+string(id) || flow.WorkflowName == "" || flow.DeploymentID != "dpl_1" || flow.RunSpecVersion == 0 || flow.Recovery || flow.StepID != nil {
			t.Errorf("recovery flow of run %s = %#v", id, flow)
		}
	}
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Fatalf("recovered runs = %v, want %v", got, want)
	}
}

// TestWorkerRecoversActiveRuns checks Worker recovers the active runs as it
// starts, and Config.SkipRecovery leaves them alone.
func TestWorkerRecoversActiveRuns(t *testing.T) {
	t.Parallel()
	for _, skip := range []bool{false, true} {
		url, schema := testSchema(t)
		world := testWorld(t, testPool(t, url, schema), Config{SkipRecovery: skip})
		ctx := context.Background()
		id := worlds.NewRunID()
		if _, err := world.pool.Exec(ctx, `INSERT INTO workflow_runs (id, status, name, spec_version) VALUES ($1, 'running', 'wf', 9)`, id); err != nil {
			t.Fatal(err)
		}
		// The worker claims nothing: its queue has no deliveries.
		workerCtx, stop := context.WithCancel(ctx)
		stopped := make(chan error, 1)
		go func() {
			stopped <- Worker(workerCtx, world, core.Runners(), WorkerOptions{Queues: []string{"none"}})
		}()
		deadline := time.Now().Add(5 * time.Second)
		for len(recoveryFlows(t, world)) == 0 && !skip && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		if skip {
			time.Sleep(100 * time.Millisecond)
		}
		stop()
		<-stopped
		if flows := recoveryFlows(t, world); len(flows) != map[bool]int{false: 1, true: 0}[skip] {
			t.Fatalf("skip %v: recovery flows = %#v", skip, flows)
		}
	}
}
