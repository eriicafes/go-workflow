package postgresworld

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/workflowops"
)

// HTTP returns the worker API consumed by workflow.NewHTTPClient.
func HTTP(world *World) http.Handler { return workflow.NewHTTPHandler(world) }

// Run polls and executes Postgres-backed workflow tasks until ctx is canceled.
func Run(ctx context.Context, world *World, runners workflowops.RunnerSet) error {
	engine := workflowops.NewEngine(world, runners)
	var group sync.WaitGroup
	defer group.Wait()
	ticker := time.NewTicker(world.PollInterval())
	defer ticker.Stop()
	for {
		leased, err := world.ClaimRun(ctx)
		if err != nil {
			return err
		}
		if leased != nil {
			group.Go(func() { _ = engine.Execute(ctx, leased) })
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
