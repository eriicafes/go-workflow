// Package sdke2e ports the Workflow SDK's workbench/example/workflows/99_e2e.ts
// fixtures to Go, so the SDK's e2e suite can run against Go workflows. The
// harness in world-http/testdata/harness runs them with HARNESS_APP=go and
// serves the manifest that maps each fixture's TypeScript name to its Go
// workflow. e2e-conformance.json lists the fixtures ported so far.
//
// Each fixture is a workflow type named after the TypeScript function, so the
// manifest can key it by the same name. Steps that take several arguments in
// TypeScript take a struct here, as a Go step has one input.
package sdke2e

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/eriicafes/go-workflow/worlds"
)

// Runners returns the fixtures' runners.
func Runners() worlds.RunnerSet { return worlds.Runners(fixtures...) }

// ManifestPath is the route the suite reads the manifest from.
const ManifestPath = "/.well-known/workflow/v1/manifest.json"

// Manifest serves the manifest the suite reads to find each fixture's
// workflow name. Fixtures are listed under workflows/99_e2e, which the suite
// matches to workflows/99_e2e.ts by its stem. It strips only .ts and .py
// extensions, so the key has none.
func Manifest() http.Handler {
	workflows := map[string]map[string]string{}
	for _, runner := range fixtures {
		name := runner.Name()
		workflows[name[strings.LastIndex(name, "//")+2:]] = map[string]string{"workflowId": name}
	}
	manifest := map[string]any{
		"version":   "1.0.0",
		"workflows": map[string]any{"workflows/99_e2e": workflows},
		"steps":     map[string]any{},
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(manifest)
	})
}
