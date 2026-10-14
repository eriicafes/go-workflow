package workflowdev

import (
	"github.com/eriicafes/go-dev"
)

// Web starts the Workflow SDK web UI for baseURL through npx.
func Web(baseURL string) dev.Cmd {
	return dev.Cmd{
		Run:  dev.Binary("npx"),
		Args: dev.Values("workflow", "web", "--noBrowser"),
		Env: dev.OsEnv(
			dev.Pair("WORKFLOW_TARGET_WORLD", "@eriicafes/go-workflow/world-http"),
			dev.Pair("WORKFLOW_HTTP_BASE_URL", baseURL),
		),
	}
}
