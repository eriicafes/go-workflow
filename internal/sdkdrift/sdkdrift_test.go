package sdkdrift

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestCheckReportsWhatIsNotAccountedFor(t *testing.T) {
	upstream := []Test{
		{File: "a.test.ts", Name: "ported"},
		{File: "a.test.ts", Name: "missing"},
		{File: "b.test.ts", Name: "group > one"},
		{File: "b.test.ts", Name: "group > two"},
		{File: "c.test.ts", Name: "whole"},
		{File: "a.test.ts", Name: "both"},
	}
	ported := map[string][]string{
		"a.test.ts > ported": {"x_test.go"},
		"a.test.ts > both":   {"x_test.go"},
		"a.test.ts > gone":   {"y_test.go"},
	}
	skips := []Skip{
		{Target: "b.test.ts > group", Line: 1},
		{Target: "c.test.ts", Line: 2},
		{Target: "a.test.ts > both", Line: 3},
		{Target: "d.test.ts", Line: 4},
	}
	report := Check(upstream, ported, skips)
	if len(report.Unaccounted) != 1 || report.Unaccounted[0].Name != "missing" {
		t.Errorf("unaccounted = %v, want only the missing test", report.Unaccounted)
	}
	if !slices.Equal(report.Stale, []string{"y_test.go: a.test.ts > gone"}) {
		t.Errorf("stale = %v", report.Stale)
	}
	if len(report.UnusedSkips) != 1 || report.UnusedSkips[0].Target != "d.test.ts" {
		t.Errorf("unused skips = %v, want only d.test.ts", report.UnusedSkips)
	}
	if len(report.SkippedPorts) != 1 || report.SkippedPorts[0].Name != "both" {
		t.Errorf("skipped ports = %v, want only both", report.SkippedPorts)
	}
}

// TestSDKTestsAreAccountedFor checks the tests of the vercel/workflow
// checkout at SDK_DIR against this repository's ported tests and the skips
// in skips*.txt.
// Without SDK_DIR it is skipped.
func TestSDKTestsAreAccountedFor(t *testing.T) {
	sdk := os.Getenv("SDK_DIR")
	if sdk == "" {
		t.Skip("set SDK_DIR to a checkout of github.com/vercel/workflow to check the ported tests")
	}
	upstream, err := UpstreamTests(sdk)
	if err != nil {
		t.Fatal(err)
	}
	if len(upstream) == 0 {
		t.Fatalf("no tests found in %s", sdk)
	}
	ported, err := PortedTests(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	// Each area keeps its skips in a file of its own, skips*.txt.
	files, err := filepath.Glob("skips*.txt")
	if err != nil {
		t.Fatal(err)
	}
	var skips []Skip
	for _, file := range files {
		read, err := ReadSkips(file)
		if err != nil {
			t.Fatal(err)
		}
		skips = append(skips, read...)
	}
	if report := Check(upstream, ported, skips); !report.Empty() {
		t.Fatalf("%d upstream tests checked:\n%s", len(upstream), report)
	}
}
