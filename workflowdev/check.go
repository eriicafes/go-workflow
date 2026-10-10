// Package workflowdev integrates go-workflow's static analyzer with go-dev.
//
// Import github.com/eriicafes/go-workflow/workflowdev from a go-dev command.
package workflowdev

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/eriicafes/go-dev"
)

// Check runs the workflow analyzer before every go-dev start or reload.
// Package patterns are relative to dev.Cmd.Dir. When packages are omitted,
// Check analyzes `./...`.
//
// Example:
//
//	dev.New(dev.Cmd{Prepare: dev.Hooks(workflowdev.Check("./cmd/api"))})
func Check(packages ...string) dev.Hook {
	if len(packages) == 0 {
		packages = []string{"./..."}
	}
	return analyzer{packages: packages}
}

type analyzer struct {
	packages []string
}

func (analyzer analyzer) Prepare(ctx context.Context, config dev.Cmd) error {
	dir, err := os.MkdirTemp("", "go-workflow-check-")
	if err != nil {
		return fmt.Errorf("workflowdev: create check directory: %w", err)
	}
	defer os.RemoveAll(dir)
	tool := filepath.Join(dir, "workflowcheck")

	build := exec.CommandContext(ctx, "go", "build", "-buildvcs=false", "-o", tool, "github.com/eriicafes/go-workflow/cmd/workflowcheck")
	build.Dir = config.Dir
	build.Env = append(os.Environ(), config.BuildEnv...)
	build.Stdout, build.Stderr = os.Stdout, os.Stderr
	if err := build.Run(); err != nil {
		return fmt.Errorf("workflowdev: build analyzer: %w", err)
	}

	args := append([]string{"vet", "-vettool=" + tool}, analyzer.packages...)
	vet := exec.CommandContext(ctx, "go", args...)
	vet.Dir = config.Dir
	vet.Env = append(os.Environ(), config.BuildEnv...)
	vet.Stdout, vet.Stderr = os.Stdout, os.Stderr
	if err := vet.Run(); err != nil {
		return fmt.Errorf("workflowdev: validate workflows: %w", err)
	}
	return nil
}
