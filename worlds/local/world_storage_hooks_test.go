package localworld

import (
	"errors"
	"testing"

	"github.com/eriicafes/go-workflow/worlds"
)

// TestHookCreatedIsFencedFirst checks an in-band hook_created whose count is
// stale is refused as superseded before its run's state is checked, so a
// superseded writer learns the counters even when the run has finished.
func TestHookCreatedIsFencedFirst(t *testing.T) {
	t.Parallel()
	world := storageWorld(t, Config{})
	runID := startedRun(t, world)
	appendEvent(t, world, runID, worlds.EventTypeRunCancelled, "", nil)
	stale, current := 7, 1
	_, err := claimToken(world, runID, "hook_a", "token", worlds.DataHookCreated{}, worlds.EventsCreateOptions{InBandCount: &stale})
	superseded, ok := errors.AsType[*worlds.InBandSupersededError](err)
	if !ok || superseded.InBandCount != 1 || superseded.EventCount != 3 {
		t.Fatalf("stale hook_created on a finished run = %v", err)
	}
	_, err = claimToken(world, runID, "hook_a", "token", worlds.DataHookCreated{}, worlds.EventsCreateOptions{InBandCount: &current})
	if !errors.Is(err, worlds.ErrEntityConflict) {
		t.Fatalf("current hook_created on a finished run = %v, want ErrEntityConflict", err)
	}
}

// TestHookCreatedRefusesANewerRun checks a hook is not created on a run of a
// newer spec version.
func TestHookCreatedRefusesANewerRun(t *testing.T) {
	t.Parallel()
	world := storageWorld(t, Config{})
	runID := startedRun(t, world)
	if err := world.write(t.Context(), func(s *store) error {
		run, err := s.getRun(runID)
		if err != nil {
			return err
		}
		run.SpecVersion = worlds.SpecVersion + 1
		return s.putRun(run)
	}); err != nil {
		t.Fatal(err)
	}
	_, err := claimToken(world, runID, "hook_a", "token", worlds.DataHookCreated{}, worlds.EventsCreateOptions{})
	if _, ok := errors.AsType[*worlds.RunNotSupportedError](err); !ok {
		t.Fatalf("hook_created on a newer run = %v, want a RunNotSupportedError", err)
	}
}
