package workflow

import (
	_ "embed"
	"encoding/json"
)

// packageJSON is the release manifest, which holds this release's version.
//
//go:embed package.json
var packageJSON []byte

// Version is the version of this release of go-workflow, which every run
// records as the version of the runtime that started it.
var Version = func() string {
	var manifest struct {
		Version string `json:"version"`
	}
	if err := json.Unmarshal(packageJSON, &manifest); err != nil || manifest.Version == "" {
		panic("workflow: package.json has no version")
	}
	return manifest.Version
}()
