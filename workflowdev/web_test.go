package workflowdev

import (
	"slices"
	"testing"
)

func TestWeb(t *testing.T) {
	t.Parallel()

	cmd := Web("http://localhost:8081/v1/workflow")
	if cmd.Run == nil {
		t.Fatal("Web() Run = nil")
	}
	if got, want := cmd.Args, []string{"workflow", "web", "--noBrowser"}; !slices.Equal(got, want) {
		t.Errorf("Web() Args = %q, want %q", got, want)
	}
	for _, pair := range []string{
		"WORKFLOW_TARGET_WORLD=@eriicafes/go-workflow/world-http",
		"WORKFLOW_HTTP_BASE_URL=http://localhost:8081/v1/workflow",
	} {
		if !slices.Contains(cmd.Env, pair) {
			t.Errorf("Web() Env does not contain %q", pair)
		}
	}
}
