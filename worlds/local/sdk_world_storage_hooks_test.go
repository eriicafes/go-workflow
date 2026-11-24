package localworld

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// SDK: packages/world-local/src/storage.test.ts > Storage > hooks > create > rejects retention beyond the 30-day default before writing Hook state
func TestSDKHookRetentionPastTheDefaultLimit(t *testing.T) {
	ctx := context.Background()
	t.Setenv("WORKFLOW_LOCAL_HOOK_RETENTION_LIMIT_DAYS", "")
	world := storageWorld(t, Config{})
	runID := startedRun(t, world)
	before, err := world.EventsList(ctx, worlds.EventsListInput{RunID: runID})
	if err != nil {
		t.Fatal(err)
	}
	until := time.Now().Add(31 * 24 * time.Hour)
	_, err = claimToken(world, runID, "hook_over_retention_limit", "over-retention-limit", worlds.DataHookCreated{TokenRetentionUntil: &until}, worlds.EventsCreateOptions{})
	if !errors.Is(err, worlds.ErrEventInvalid) {
		t.Fatalf("hook retained for 31 days = %v, want ErrEventInvalid", err)
	}
	if _, err := world.HooksGet(ctx, worlds.HooksGetInput{HookID: "hook_over_retention_limit"}); !is[*worlds.HookNotFoundError](err) {
		t.Fatalf("refused hook = %v, want HookNotFoundError", err)
	}
	if after, err := world.EventsList(ctx, worlds.EventsListInput{RunID: runID}); err != nil || len(after.Data) != len(before.Data) || after.Snapshot != before.Snapshot {
		t.Fatalf("events after the refusal = %d, want %d (err %v)", len(after.Data), len(before.Data), err)
	}
}

// SDK: packages/world-local/src/storage.test.ts > Storage > hooks > create > accepts retention within the configured Local limit
func TestSDKHookRetentionWithinTheConfiguredLimit(t *testing.T) {
	t.Setenv("WORKFLOW_LOCAL_HOOK_RETENTION_LIMIT_DAYS", "60")
	world := storageWorld(t, Config{})
	runID := startedRun(t, world)
	until := time.Now().Add(31 * 24 * time.Hour)
	result, err := claimToken(world, runID, "hook_custom_retention_limit", "custom-retention-limit", worlds.DataHookCreated{TokenRetentionUntil: &until}, worlds.EventsCreateOptions{})
	if err != nil || result.Hook == nil || result.Hook.HookID != "hook_custom_retention_limit" {
		t.Fatalf("hook retained for 31 days = %+v err = %v", result, err)
	}
}
