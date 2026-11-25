// Package sdkdrift checks that every test of the upstream packages that
// go-workflow mirrors is accounted for: ported to a Go test that names it in
// a comment, or skipped with a reason. Each Source is one upstream
// repository: the Workflow SDK, whose ports name their tests in
// `// SDK: <file> > <test name>` comments, and the AI SDK's @ai-sdk/workflow,
// whose ports workflowai holds, in `// AI SDK: <file> > <test name>`
// comments. Its test runs the check against a checkout of each.
package sdkdrift

import (
	"bufio"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
)

// Source is an upstream repository whose tests go-workflow accounts for.
type Source struct {
	// Repository names the upstream repository, and Env the environment
	// variable that holds the path of its checkout.
	Repository string
	Env        string
	// Comment is the label of the comments that name its ported tests, as
	// in `// <Comment>: <file> > <test name>`.
	Comment string
	// TrackedDirs are the directories of the checkout whose tests are
	// accounted for.
	TrackedDirs []string
	// Skips is the glob of the skips files of its tests, in this package.
	Skips string
}

// WorkflowSDK is the Workflow SDK, vercel/workflow: the World interface and
// its schemas, the error classes, and the local and Postgres Worlds, whose
// behavior the Go Worlds share.
var WorkflowSDK = Source{
	Repository: "github.com/vercel/workflow",
	Env:        "SDK_DIR",
	Comment:    "SDK",
	TrackedDirs: []string{
		"packages/world/src",
		"packages/errors/src",
		"packages/world-local/src",
		"packages/world-postgres/src",
	},
	Skips: "skips*.txt",
}

// AISDK is the AI SDK, vercel/ai: @ai-sdk/workflow, whose WorkflowAgent
// workflowai ports.
var AISDK = Source{
	Repository:  "github.com/vercel/ai",
	Env:         "AI_SDK_DIR",
	Comment:     "AI SDK",
	TrackedDirs: []string{"packages/workflow/src"},
	Skips:       "ai-sdk-skips*.txt",
}

// Sources are the upstream repositories go-workflow accounts for.
var Sources = []Source{WorkflowSDK, AISDK}

// Test is an upstream test: its file, relative to the checkout, and its full
// name, the names of its describe blocks and its own joined with " > ".
type Test struct {
	File string
	Name string
}

// ID returns the test as a `// SDK:` comment names it: "<file> > <name>".
func (t Test) ID() string { return t.File + " > " + t.Name }

// Skip is an entry of a skips file: a file, or a test or describe block of one,
// that is not ported, and why.
type Skip struct {
	// Target is "<file>" or "<file> > <name>", where name is a test's full
	// name or a prefix of it ending at a describe block.
	Target string
	Reason string
	// File and Line are where the skip is written.
	File string
	Line int
}

// Covers reports whether the skip covers test.
func (s Skip) Covers(test Test) bool {
	id := test.ID()
	return s.Target == test.File || s.Target == id || strings.HasPrefix(id, s.Target+" > ")
}

// Report is what the check found wrong. It is empty when everything is
// accounted for.
type Report struct {
	// Unaccounted are upstream tests neither ported nor skipped.
	Unaccounted []Test
	// Stale are `// SDK:` comments naming no upstream test, as "<go file>:
	// <id>".
	Stale []string
	// UnusedSkips are skips that cover no upstream test.
	UnusedSkips []Skip
	// SkippedPorts are upstream tests both ported and skipped.
	SkippedPorts []Test
}

// Empty reports whether the check found nothing wrong.
func (r Report) Empty() bool {
	return len(r.Unaccounted) == 0 && len(r.Stale) == 0 && len(r.UnusedSkips) == 0 && len(r.SkippedPorts) == 0
}

func (r Report) String() string {
	var b strings.Builder
	section := func(title string, lines []string) {
		if len(lines) == 0 {
			return
		}
		fmt.Fprintf(&b, "%s (%d):\n", title, len(lines))
		for _, line := range lines {
			fmt.Fprintf(&b, "  %s\n", line)
		}
	}
	ids := func(tests []Test) []string {
		out := make([]string, len(tests))
		for i, test := range tests {
			out[i] = test.ID()
		}
		return out
	}
	skips := make([]string, len(r.UnusedSkips))
	for i, skip := range r.UnusedSkips {
		skips[i] = fmt.Sprintf("%s:%d: %s", skip.File, skip.Line, skip.Target)
	}
	section("upstream tests neither ported nor skipped", ids(r.Unaccounted))
	section("// SDK: comments naming no upstream test", r.Stale)
	section("skips that cover no upstream test", skips)
	section("upstream tests both ported and skipped", ids(r.SkippedPorts))
	return b.String()
}

// Check compares the upstream tests with the ported ones, a map from each
// `// SDK:` id to the Go files that name it, and the skips.
func Check(upstream []Test, ported map[string][]string, skips []Skip) Report {
	var report Report
	known := make(map[string]bool, len(upstream))
	used := make([]bool, len(skips))
	for _, test := range upstream {
		known[test.ID()] = true
		skipped := false
		for i, skip := range skips {
			if skip.Covers(test) {
				used[i], skipped = true, true
			}
		}
		_, isPorted := ported[test.ID()]
		switch {
		case isPorted && skipped:
			report.SkippedPorts = append(report.SkippedPorts, test)
		case !isPorted && !skipped:
			report.Unaccounted = append(report.Unaccounted, test)
		}
	}
	for id, files := range ported {
		if !known[id] {
			for _, file := range files {
				report.Stale = append(report.Stale, file+": "+id)
			}
		}
	}
	slices.Sort(report.Stale)
	for i, skip := range skips {
		if !used[i] {
			report.UnusedSkips = append(report.UnusedSkips, skip)
		}
	}
	return report
}

// UpstreamTests returns the tests of the tracked directories of the
// checkout of source at root.
func (source Source) UpstreamTests(root string) ([]Test, error) {
	var tests []Test
	for _, dir := range source.TrackedDirs {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() && entry.Name() == "node_modules" {
				return filepath.SkipDir
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".test.ts") {
				return nil
			}
			source, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			relative, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			for _, name := range TestNames(string(source)) {
				tests = append(tests, Test{File: filepath.ToSlash(relative), Name: name})
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return tests, nil
}

// PortedTests returns, for each upstream test a Go file under root names in
// a `// <Comment>: <file> > <test name>` comment of source, the files that
// name it.
func (source Source) PortedTests(root string) (map[string][]string, error) {
	comment := regexp.MustCompile(`^\s*//\s*` + regexp.QuoteMeta(source.Comment) + `:\s*(\S+\.test\.ts > .+?)\s*$`)
	ported := map[string][]string{}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() && (entry.Name() == "node_modules" || strings.HasPrefix(entry.Name(), ".")) && path != root {
			return filepath.SkipDir
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return nil
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()
		relative, _ := filepath.Rel(root, path)
		scanner := bufio.NewScanner(file)
		scanner.Buffer(make([]byte, 1<<20), 1<<20)
		for scanner.Scan() {
			if match := comment.FindStringSubmatch(scanner.Text()); match != nil {
				ported[match[1]] = append(ported[match[1]], filepath.ToSlash(relative))
			}
		}
		return scanner.Err()
	})
	return ported, err
}

// ReadSkips reads a skips file: one skip per line, "<target> -- <reason>", with
// blank lines and lines starting with # ignored.
func ReadSkips(path string) ([]Skip, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var skips []Skip
	for index, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		target, reason, ok := strings.Cut(line, " -- ")
		if !ok || strings.TrimSpace(reason) == "" {
			return nil, fmt.Errorf("%s:%d: a skip is \"<target> -- <reason>\"", path, index+1)
		}
		skips = append(skips, Skip{Target: strings.TrimSpace(target), Reason: strings.TrimSpace(reason), File: filepath.Base(path), Line: index + 1})
	}
	return skips, nil
}
