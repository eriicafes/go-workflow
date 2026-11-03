package vercelworld

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestLatestDeploymentID(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	all, _ := newWorlds(t, redisTCPConfig(), 2)
	old, production := all[0], all[1]
	old.config.DeploymentID, production.config.DeploymentID = "dpl_old", "dpl_production"
	server := httptest.NewServer(HTTP(production))
	defer server.Close()
	old.latestURL = server.URL + "/v1/workflow" + deploymentRouteSuffix
	latest := func() string {
		t.Helper()
		id, err := old.LatestDeploymentID(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return id
	}

	if got := latest(); got != "dpl_production" {
		t.Fatalf("latest = %q, want the deployment serving the route", got)
	}
	production.config.DeploymentID = "dpl_next"
	if got := latest(); got != "dpl_production" {
		t.Fatalf("cached latest = %q, want dpl_production until it expires", got)
	}
	old.latestAt = time.Time{}
	if got := latest(); got != "dpl_next" {
		t.Fatalf("expired latest = %q, want dpl_next", got)
	}

	// A failed request leaves the run on the deployment that starts it.
	missing := httptest.NewServer(http.NotFoundHandler())
	defer missing.Close()
	old.latestURL, old.latestAt = missing.URL, time.Time{}
	if got := latest(); got != "" {
		t.Fatalf("latest after a failed request = %q, want none", got)
	}
}

func TestLatestDeploymentOnlyInProduction(t *testing.T) {
	config := redisTCPConfig()
	config.DeploymentID = "dpl_1"
	for env, want := range map[string]bool{"production": true, "preview": false, "": false} {
		t.Setenv("VERCEL_ENV", env)
		world, _ := newWorlds(t, config, 1)
		if got := world[0].latestURL != ""; got != want {
			t.Errorf("VERCEL_ENV=%q: looks up the latest deployment = %v, want %v", env, got, want)
		}
	}
}
