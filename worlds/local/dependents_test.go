package localworld

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/eriicafes/go-workflow/workflowops"
)

func TestResolveDependentsChainsAndIgnoresDuplicateNotifications(t *testing.T) {
	world := newDependencyTestWorld(t)
	writeDependencyRun(t, world, runRecord{ID: "parent", Status: workflowops.StatusWaiting})
	writeDependencyRun(t, world, runRecord{
		ID:         "child",
		Status:     workflowops.StatusCompleted,
		Output:     json.RawMessage(`"done"`),
		Completion: 1,
	})

	first := workflowops.Task{RunID: "parent", Sequence: 1}
	second := workflowops.Task{RunID: "parent", Sequence: 2}
	writeDependencyCommand(t, world, "parent", commandRecord{
		Sequence:     first.Sequence,
		Kind:         workflowops.KindAwait,
		Dependencies: []workflowops.Task{{RunID: "child", Sequence: workflowops.RootSequence}},
		Status:       workflowops.StatusWaiting,
	})
	writeDependencyCommand(t, world, "parent", commandRecord{
		Sequence:     second.Sequence,
		Kind:         workflowops.KindAwait,
		Dependencies: []workflowops.Task{first},
		Status:       workflowops.StatusWaiting,
	})
	world.addDependentsLocked(first, []workflowops.Task{{RunID: "child", Sequence: workflowops.RootSequence}})
	world.addDependentsLocked(second, []workflowops.Task{first})

	if err := world.resolveDependentsLocked(workflowops.Task{RunID: "child", Sequence: workflowops.RootSequence}); err != nil {
		t.Fatal(err)
	}
	firstCommand, _, err := world.readCommand("parent", first.Sequence)
	if err != nil {
		t.Fatal(err)
	}
	secondCommand, _, err := world.readCommand("parent", second.Sequence)
	if err != nil {
		t.Fatal(err)
	}
	if firstCommand.Status != workflowops.StatusCompleted || secondCommand.Status != workflowops.StatusCompleted {
		t.Fatalf("chain statuses = %s, %s", firstCommand.Status, secondCommand.Status)
	}
	if firstCommand.Completion >= secondCommand.Completion {
		t.Fatalf("chain completions = %d, %d", firstCommand.Completion, secondCommand.Completion)
	}

	var version versionRecord
	if err := readJSON(world.root, "version.json", &version); err != nil {
		t.Fatal(err)
	}
	if err := world.resolveDependentsLocked(workflowops.Task{RunID: "child", Sequence: workflowops.RootSequence}); err != nil {
		t.Fatal(err)
	}
	var after versionRecord
	if err := readJSON(world.root, "version.json", &after); err != nil {
		t.Fatal(err)
	}
	if after.Completion != version.Completion {
		t.Fatalf("duplicate notification advanced completion from %d to %d", version.Completion, after.Completion)
	}

	settled := workflowops.Task{RunID: "parent", Sequence: 3}
	writeDependencyCommand(t, world, "parent", commandRecord{
		Sequence:     settled.Sequence,
		Kind:         workflowops.KindAwait,
		Dependencies: []workflowops.Task{{RunID: "child", Sequence: workflowops.RootSequence}},
		Status:       workflowops.StatusWaiting,
	})
	world.addDependentsLocked(settled, []workflowops.Task{{RunID: "child", Sequence: workflowops.RootSequence}})
	resolved, err := world.resolveWaitLocked(settled)
	if err != nil {
		t.Fatal(err)
	}
	if !resolved {
		t.Fatal("wait created after dependency settled was left pending")
	}
}

func TestCancelOperationsResolvesDependentWait(t *testing.T) {
	world := newDependencyTestWorld(t)
	writeDependencyRun(t, world, runRecord{ID: "parent", Status: workflowops.StatusWaiting})
	writeDependencyRun(t, world, runRecord{ID: "child", Status: workflowops.StatusWaiting})
	dependency := workflowops.Task{RunID: "child", Sequence: 1}
	writeDependencyCommand(t, world, "child", commandRecord{
		Sequence: dependency.Sequence,
		Kind:     workflowops.KindStep,
		Status:   workflowops.StatusQueued,
	})
	wait := workflowops.Task{RunID: "parent", Sequence: 1}
	writeDependencyCommand(t, world, "parent", commandRecord{
		Sequence:     wait.Sequence,
		Kind:         workflowops.KindAwait,
		Dependencies: []workflowops.Task{dependency},
		Status:       workflowops.StatusWaiting,
	})
	world.addDependentsLocked(wait, []workflowops.Task{dependency})

	if err := world.CancelOperations(context.Background(), dependency); err != nil {
		t.Fatal(err)
	}
	command, _, err := world.readCommand("parent", wait.Sequence)
	if err != nil {
		t.Fatal(err)
	}
	if command.Status != workflowops.StatusFailed || command.Error != workflowops.ErrCancelled.Error() {
		t.Fatalf("cancelled dependency wait = %#v", command)
	}
}

func newDependencyTestWorld(t *testing.T) *World {
	t.Helper()
	root, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.Close() })
	world, err := New(root, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeJSON(root, "version.json", versionRecord{Version: storeVersion}); err != nil {
		t.Fatal(err)
	}
	return world
}

func writeDependencyRun(t *testing.T, world *World, run runRecord) {
	t.Helper()
	if err := writeJSON(world.root, runName(run.ID), run); err != nil {
		t.Fatal(err)
	}
}

func writeDependencyCommand(t *testing.T, world *World, runID string, command commandRecord) {
	t.Helper()
	if err := writeJSON(world.root, commandName(runID, command.Sequence), command); err != nil {
		t.Fatal(err)
	}
}
