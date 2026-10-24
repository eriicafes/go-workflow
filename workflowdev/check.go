// Package workflowdev integrates go-workflow's static analyzer with go-dev.
//
// Import github.com/eriicafes/go-workflow/workflowdev from a go-dev command.
package workflowdev

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/eriicafes/go-dev"
)

const module = "github.com/eriicafes/go-workflow"

// Check runs the workflow analyzer before every go-dev start or reload.
// Package patterns are relative to dev.Cmd.Dir. When packages are omitted,
// Check analyzes `./...`.
//
// The analyzer is built from the go-workflow module that dev.Cmd.Dir's module
// requires, with that module's Go toolchain, so the application needs no
// tool dependency on it. A built analyzer is cached in the user cache
// directory.
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
	tool, err := buildAnalyzer(ctx, config)
	if err != nil {
		return err
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

// buildAnalyzer builds cmd/workflowcheck from the go-workflow module the
// application requires and returns its path. A vet tool must be built with a
// toolchain at least as new as the code it analyzes, so it uses the
// application's Go version.
func buildAnalyzer(ctx context.Context, config dev.Cmd) (string, error) {
	env := append(os.Environ(), config.BuildEnv...)
	goVersion, err := goOutput(ctx, config.Dir, env, "env", "GOVERSION")
	if err != nil {
		return "", fmt.Errorf("workflowdev: read Go version: %w", err)
	}
	listed, err := goOutput(ctx, config.Dir, env, "list", "-m", "-json", module)
	if err != nil {
		return "", fmt.Errorf("workflowdev: find %s: %w", module, err)
	}
	var source struct {
		Version string
		Dir     string
		Replace *struct {
			Version string
			Dir     string
		}
	}
	if err := json.Unmarshal([]byte(listed), &source); err != nil {
		return "", fmt.Errorf("workflowdev: find %s: %w", module, err)
	}
	dir, version := source.Dir, source.Version
	if source.Replace != nil {
		dir, version = source.Replace.Dir, source.Replace.Version
	}
	if dir == "" {
		return "", fmt.Errorf("workflowdev: %s is not downloaded, run go mod download in %s", module, config.Dir)
	}

	cache, err := os.UserCacheDir()
	if err != nil {
		cache = os.TempDir()
	}
	tool := filepath.Join(cache, "go-workflow", "workflowcheck-"+goVersion+"-"+version)
	// A published version never changes, so its build is reused. A local
	// replacement is rebuilt every time, which the Go build cache keeps fast.
	if version != "" {
		if _, err := os.Stat(tool); err == nil {
			return tool, nil
		}
	} else {
		local := filepath.Base(filepath.Dir(dir)) + "-" + filepath.Base(dir)
		tool = filepath.Join(cache, "go-workflow", "workflowcheck-"+goVersion+"-local-"+local)
	}
	build := exec.CommandContext(ctx, "go", "build", "-buildvcs=false", "-o", tool, "./cmd/workflowcheck")
	build.Dir = dir
	build.Env = append(env, "GOTOOLCHAIN="+goVersion)
	build.Stdout, build.Stderr = os.Stdout, os.Stderr
	if err := build.Run(); err != nil {
		return "", fmt.Errorf("workflowdev: build analyzer: %w", err)
	}
	return tool, nil
}

func goOutput(ctx context.Context, dir string, env []string, args ...string) (string, error) {
	command := exec.CommandContext(ctx, "go", args...)
	command.Dir = dir
	command.Env = env
	var stderr bytes.Buffer
	command.Stderr = &stderr
	output, err := command.Output()
	if err != nil {
		return "", fmt.Errorf("%w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return strings.TrimSpace(string(output)), nil
}
