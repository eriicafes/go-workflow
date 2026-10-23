package workflowdev

import (
	"github.com/eriicafes/go-dev"
)

// Web returns a go-dev command that runs the Workflow SDK web UI through npx,
// connected to the World served by worlds.HTTPHandler at baseURL.
func Web(baseURL string) dev.Cmd {
	return dev.Cmd{
		Run:  dev.Binary("npx"),
		Args: dev.Values("workflow", "web", "--noBrowser"),
		Env: dev.OsEnv(
			dev.Pair("WORKFLOW_TARGET_WORLD", "go-workflow/world-http"),
			dev.Pair("WORKFLOW_HTTP_BASE_URL", baseURL),
		),
	}
}
