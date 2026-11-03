package vercelworld

import (
	"cmp"
	"context"
	"os"
	"strconv"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// latestDeploymentTTL is how long a World reuses the latest deployment it
// read before reading it again.
const latestDeploymentTTL = time.Minute

// keptDeployments is how many of a scope's newest deployments are recorded.
const keptDeployments = 20

var _ worlds.LatestDeployments = (*World)(nil)

// scriptRecordDeployment records deployment ARGV[3] in scope ARGV[2] at
// ARGV[4], unless it is recorded already, keeps the scope's newest
// deployments, and returns the newest.
var scriptRecordDeployment = newRedisScript(luaPrelude + `
local key = k('deployments', ARGV[2])
redis.call('ZADD', key, 'NX', ARGV[4], ARGV[3])
redis.call('ZREMRANGEBYRANK', key, 0, -` + strconv.Itoa(keptDeployments+1) + `)
return redis.call('ZRANGE', key, -1, -1)[1]
`)

// LatestDeploymentID returns the newest deployment of this World's
// deployment scope, or "" when the World has no deployment ID or scope.
// Deployments are ordered by when a World of theirs first used Redis for
// them, so this World's deployment counts once it has queued a delivery or
// read the latest deployment. The result is reused for a minute.
func (w *World) LatestDeploymentID(ctx context.Context) (string, error) {
	if w.config.DeploymentID == "" || w.config.DeploymentScope == "" {
		return "", nil
	}
	w.latest.Lock()
	defer w.latest.Unlock()
	if w.latestID != "" && time.Since(w.latestAt) < latestDeploymentTTL {
		return w.latestID, nil
	}
	result, err := w.redis.Run(ctx, scriptRecordDeployment, w.scripts, w.config.DeploymentScope, w.config.DeploymentID, micros(time.Now()))
	if err != nil {
		return "", err
	}
	w.latestID, w.latestAt = cmp.Or(replyString(result), w.config.DeploymentID), time.Now()
	return w.latestID, nil
}

// recordDeployment records this World's deployment the first time the
// process queues a delivery, so a new deployment counts as the latest once
// it does any work, not only once it starts a run of a workflow marked
// workflow.StartOnLatest. Errors are ignored: the next delivery tries again.
func (w *World) recordDeployment(ctx context.Context) {
	w.latest.Lock()
	recorded := w.latestID != ""
	w.latest.Unlock()
	if !recorded {
		_, _ = w.LatestDeploymentID(ctx)
	}
}

// deploymentScopeFromEnv returns the scope of the Vercel deployment this
// process runs in: its target environment, and for previews also its git
// branch, so runs move between the deployments of one branch. It returns ""
// outside Vercel and for previews without a branch.
func deploymentScopeFromEnv() string {
	env := cmp.Or(os.Getenv("VERCEL_TARGET_ENV"), os.Getenv("VERCEL_ENV"))
	if env != "preview" {
		return env
	}
	if branch := os.Getenv("VERCEL_GIT_COMMIT_REF"); branch != "" {
		return "preview:" + branch
	}
	return ""
}
