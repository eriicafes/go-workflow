package vercelworld

import (
	"context"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

func TestLatestDeploymentIDRedisTCP(t *testing.T) { testLatestDeploymentID(t, redisTCPConfig()) }

func TestLatestDeploymentIDRedisREST(t *testing.T) { testLatestDeploymentID(t, redisRESTConfig()) }

func testLatestDeploymentID(t *testing.T, config Config) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	all, _ := newWorlds(t, config, 5)
	deploy := func(world *World, id, scope string) *World {
		world.config.DeploymentID, world.config.DeploymentScope = id, scope
		return world
	}
	d1 := deploy(all[0], "dpl_1", "production")
	d2 := deploy(all[1], "dpl_2", "production")
	d3 := deploy(all[2], "dpl_3", "production")
	preview := deploy(all[3], "dpl_preview", "preview:feature")
	unscoped := deploy(all[4], "dpl_unscoped", "")
	latest := func(world *World) string {
		t.Helper()
		id, err := world.LatestDeploymentID(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return id
	}

	if got := latest(d1); got != "dpl_1" {
		t.Fatalf("first deployment's latest = %q, want itself", got)
	}
	if got := latest(d2); got != "dpl_2" {
		t.Fatalf("second deployment's latest = %q, want itself", got)
	}
	if got := latest(d1); got != "dpl_1" {
		t.Fatalf("cached latest = %q, want dpl_1 until it expires", got)
	}
	d1.latestAt = time.Time{}
	if got := latest(d1); got != "dpl_2" {
		t.Fatalf("expired latest = %q, want dpl_2: an older deployment must not reclaim it", got)
	}

	// A deployment counts once it queues a delivery.
	if _, err := d3.Queue(ctx, worlds.QueueInput{RunID: "wrun_latest", WorkflowName: "w", IdempotencyKey: "wrun_latest"}); err != nil {
		t.Fatal(err)
	}
	d1.latestAt = time.Time{}
	if got := latest(d1); got != "dpl_3" {
		t.Fatalf("latest after dpl_3 queued = %q, want dpl_3", got)
	}

	if got := latest(preview); got != "dpl_preview" {
		t.Fatalf("preview latest = %q, want its own scope's", got)
	}
	if got := latest(unscoped); got != "" {
		t.Fatalf("unscoped latest = %q, want none", got)
	}
}

func TestDeploymentScopeFromEnv(t *testing.T) {
	for _, test := range []struct{ target, env, branch, want string }{
		{"", "production", "main", "production"},
		{"staging", "preview", "main", "staging"},
		{"", "preview", "feature", "preview:feature"},
		{"", "preview", "", ""},
		{"", "", "", ""},
	} {
		t.Setenv("VERCEL_TARGET_ENV", test.target)
		t.Setenv("VERCEL_ENV", test.env)
		t.Setenv("VERCEL_GIT_COMMIT_REF", test.branch)
		if got := deploymentScopeFromEnv(); got != test.want {
			t.Errorf("scope for %+v = %q, want %q", test, got, test.want)
		}
	}
}
