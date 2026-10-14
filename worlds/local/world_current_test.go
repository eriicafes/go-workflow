package localworld

import (
	"context"
	"testing"

	"github.com/eriicafes/go-workflow/worlds"
)

func TestOpenPersistsEvents(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	world, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = world.EventsCreateRun(context.Background(), worlds.EventsCreateRunInput{RunID: "run_1", WorkflowName: "test"})
	if err != nil {
		t.Fatal(err)
	}
	if err := world.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	run, err := reopened.RunsGet(context.Background(), worlds.RunsGetInput{RunID: "run_1"})
	if err != nil {
		t.Fatal(err)
	}
	if run.WorkflowName != "test" {
		t.Fatalf("workflow = %q, want test", run.WorkflowName)
	}
}
