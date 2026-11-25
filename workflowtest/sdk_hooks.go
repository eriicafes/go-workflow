package workflowtest

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
	"github.com/oklog/ulid/v2"
)

// sdkRequireResumeDedup skips the test unless the World deduplicates the
// resumes of the hook that holds token.
func sdkRequireResumeDedup(t *testing.T, world worlds.World, token string) {
	t.Helper()
	if world.Capabilities().HookResumeDedup {
		return
	}
	hook, err := world.HooksGetByToken(context.Background(), worlds.HooksGetByTokenInput{Token: token})
	if err != nil {
		t.Fatal(err)
	}
	if !hook.ResumeDedup {
		t.Skip("the World does not deduplicate resumes")
	}
}

// sdkHookID returns a hook ID made of name and the end of runID, so the
// hooks of different runs of one World do not share IDs.
func sdkHookID(name string, runID worlds.RunID) string {
	return name + "_" + string(runID[len(runID)-8:])
}

// sdkConflict checks that result is a hook_conflict over token naming
// holder, and that it created no hook.
func sdkConflict(t *testing.T, result worlds.EventResult, token string, holder worlds.RunID) {
	t.Helper()
	var data worlds.DataHookCreated
	if result.EventType != worlds.EventTypeHookConflict || json.Unmarshal(result.Data, &data) != nil || data.Token != token || data.ConflictingRunID != holder || result.Hook != nil {
		t.Fatalf("claim = %+v (data %s), want a hook_conflict over %q naming %s", result, result.Data, token, holder)
	}
}

// sdkClaim appends a hook_created of hookID claiming token.
func sdkClaim(t *testing.T, world worlds.World, runID worlds.RunID, hookID, token string, retention time.Duration) worlds.EventResult {
	t.Helper()
	data := worlds.DataHookCreated{Token: token}
	if retention > 0 {
		until := time.Now().Add(retention).UTC().Truncate(time.Millisecond)
		data.TokenRetentionUntil = &until
	}
	return conformAppend(t, world, runID, worlds.EventTypeHookCreated, hookID, data)
}

// sdkSameHook checks that got is the hook want, as a read returns it.
func sdkSameHook(t *testing.T, got, want worlds.Hook) {
	t.Helper()
	if got.HookID != want.HookID || got.RunID != want.RunID || got.Token != want.Token || string(got.Metadata) != string(want.Metadata) || got.IsWebhook != want.IsWebhook || !got.CreatedAt.Equal(want.CreatedAt) {
		t.Fatalf("hook = %+v, want %+v", got, want)
	}
}

// SDK: packages/world-local/src/storage.test.ts > Storage > hooks > create > should create a new hook
// SDK: packages/world-local/src/storage.test.ts > Storage > hooks > create > should return hook_conflict event when creating a hook with a duplicate token
// SDK: packages/world-local/src/storage.test.ts > Storage > hooks > create > should allow multiple hooks with different tokens for the same run
// SDK: packages/world-local/src/storage.test.ts > Storage > hooks > create > should allow the same token only after disposing the previous hook
// SDK: packages/world-local/src/storage.test.ts > Storage > hooks > create > should enforce token uniqueness across different runs within the same project
// SDK: packages/world-local/src/storage.test.ts > Storage > hooks > create > should reject concurrent creates for the same token atomically
// SDK: packages/world-local/src/storage.test.ts > Storage > hooks > get > should retrieve an existing hook by hookId
// SDK: packages/world-local/src/storage.test.ts > Storage > hooks > get > should throw error for non-existent hook
// SDK: packages/world-local/src/storage.test.ts > Storage > hooks > get > should respect resolveData option
// SDK: packages/world-local/src/storage.test.ts > Storage > hooks > getByToken > should retrieve an existing hook by token
// SDK: packages/world-local/src/storage.test.ts > Storage > hooks > getByToken > should throw error for non-existent token
// SDK: packages/world-local/src/storage.test.ts > Storage > hooks > getByToken > should find the correct hook when multiple hooks exist
func conformSDKHooks(t *testing.T, world worlds.World) {
	ctx := context.Background()
	newRun := func(t *testing.T) worlds.RunID { return sdkRun(t, world, "test-workflow", []byte(`[]`)) }
	token := func(name string, runID worlds.RunID) string { return name + ":" + string(runID) }
	getByID := func(t *testing.T, hookID worlds.HookID, resolve worlds.ResolveData) worlds.Hook {
		t.Helper()
		hook, err := world.HooksGet(ctx, worlds.HooksGetInput{HookID: hookID, ResolveData: resolve})
		if err != nil {
			t.Fatal(err)
		}
		return hook
	}
	t.Run("create", func(t *testing.T) {
		runID := newRun(t)
		result := sdkClaim(t, world, runID, sdkHookID("hook_123", runID), token("my-hook-token", runID), 0)
		if hook := result.Hook; hook == nil || hook.RunID != runID || hook.HookID != worlds.HookID(sdkHookID("hook_123", runID)) || hook.Token != token("my-hook-token", runID) || hook.CreatedAt.IsZero() {
			t.Fatalf("hook = %+v", result.Hook)
		}
	})
	t.Run("duplicate token", func(t *testing.T) {
		runID := newRun(t)
		shared := token("duplicate-test-token", runID)
		sdkClaim(t, world, runID, sdkHookID("hook_1", runID), shared, 0)
		result := sdkClaim(t, world, runID, sdkHookID("hook_2", runID), shared, 0)
		sdkConflict(t, result, shared, runID)
		if event, err := world.EventsGet(ctx, worlds.EventsGetInput{RunID: runID, EventID: result.EventID}); err != nil || event.CorrelationID != sdkHookID("hook_2", runID) {
			t.Fatalf("hook_conflict = %+v err = %v", event, err)
		}
	})
	t.Run("different tokens", func(t *testing.T) {
		runID := newRun(t)
		first := sdkClaim(t, world, runID, sdkHookID("hook_1", runID), token("token-1", runID), 0)
		second := sdkClaim(t, world, runID, sdkHookID("hook_2", runID), token("token-2", runID), 0)
		if first.Hook == nil || first.Hook.Token != token("token-1", runID) || second.Hook == nil || second.Hook.Token != token("token-2", runID) {
			t.Fatalf("hooks = %+v and %+v", first.Hook, second.Hook)
		}
	})
	t.Run("token after disposal", func(t *testing.T) {
		runID := newRun(t)
		reusable := token("reusable-token", runID)
		sdkClaim(t, world, runID, sdkHookID("hook_1", runID), reusable, time.Minute)
		sdkConflict(t, sdkClaim(t, world, runID, sdkHookID("hook_2", runID), reusable, 0), reusable, runID)
		conformAppend(t, world, runID, worlds.EventTypeHookDisposed, sdkHookID("hook_1", runID), nil)
		if claimed := sdkClaim(t, world, runID, sdkHookID("hook_2", runID), reusable, 0); claimed.Hook == nil || claimed.Hook.Token != reusable || claimed.Hook.HookID != worlds.HookID(sdkHookID("hook_2", runID)) {
			t.Fatalf("claim after disposal = %+v", claimed)
		}
	})
	t.Run("across runs", func(t *testing.T) {
		runID := newRun(t)
		other := sdkRun(t, world, "another-workflow", []byte(`[]`))
		shared := token("shared-token-across-runs", runID)
		sdkClaim(t, world, runID, sdkHookID("hook_1", runID), shared, 0)
		sdkConflict(t, sdkClaim(t, world, other, sdkHookID("hook_2", other), shared, 0), shared, runID)
	})
	t.Run("concurrent creates", func(t *testing.T) {
		runID := newRun(t)
		shared := token("concurrent-token", runID)
		results := make([]worlds.EventResult, 5)
		errs := make([]error, 5)
		var group sync.WaitGroup
		for i := range 5 {
			group.Go(func() {
				results[i], errs[i] = sdkTry(world, runID, worlds.EventTypeHookCreated, fmt.Sprintf("concurrent_hook_%d", i), worlds.DataHookCreated{Token: shared})
			})
		}
		group.Wait()
		if err := errors.Join(errs...); err != nil {
			t.Fatal(err)
		}
		created := 0
		for _, result := range results {
			if result.EventType == worlds.EventTypeHookCreated {
				created++
				continue
			}
			sdkConflict(t, result, shared, runID)
		}
		if created != 1 {
			t.Fatalf("created = %d, want 1", created)
		}
	})
	t.Run("get", func(t *testing.T) {
		runID := newRun(t)
		created := sdkClaim(t, world, runID, sdkHookID("hook_123", runID), token("test-token-123", runID), 0)
		sdkSameHook(t, getByID(t, worlds.HookID(sdkHookID("hook_123", runID)), ""), *created.Hook)
	})
	t.Run("missing hook", func(t *testing.T) {
		_, err := world.HooksGet(ctx, worlds.HooksGetInput{HookID: "nonexistent_hook"})
		conformExpectType[*worlds.HookNotFoundError](t, err, "missing hook")
	})
	t.Run("get without data", func(t *testing.T) {
		runID := newRun(t)
		conformAppend(t, world, runID, worlds.EventTypeHookCreated, sdkHookID("hook_with_response", runID), worlds.DataHookCreated{Token: token("test-token", runID), Metadata: []byte(`"meta"`)})
		created := getByID(t, worlds.HookID(sdkHookID("hook_with_response", runID)), worlds.ResolveDataAll)
		if string(created.Metadata) != `"meta"` {
			t.Fatalf("hook with data = %+v", created)
		}
		if stripped := getByID(t, worlds.HookID(sdkHookID("hook_with_response", runID)), worlds.ResolveDataNone); stripped.Metadata != nil || stripped.HookID != worlds.HookID(sdkHookID("hook_with_response", runID)) {
			t.Fatalf("hook without data = %+v", stripped)
		}
	})
	t.Run("get by token", func(t *testing.T) {
		runID := newRun(t)
		created := sdkClaim(t, world, runID, sdkHookID("hook_123", runID), token("test-token-123", runID), 0)
		hook, err := world.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: token("test-token-123", runID)})
		if err != nil {
			t.Fatal(err)
		}
		sdkSameHook(t, hook, *created.Hook)
	})
	t.Run("missing token", func(t *testing.T) {
		_, err := world.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: "nonexistent-token"})
		conformExpectType[*worlds.HookNotFoundError](t, err, "missing token")
	})
	t.Run("token among several", func(t *testing.T) {
		runID := newRun(t)
		first := sdkClaim(t, world, runID, sdkHookID("hook_1", runID), token("token-1", runID), 0)
		sdkClaim(t, world, runID, sdkHookID("hook_2", runID), token("token-2", runID), 0)
		sdkClaim(t, world, runID, sdkHookID("hook_3", runID), token("token-3", runID), 0)
		hook, err := world.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: token("token-1", runID)})
		if err != nil || hook.HookID != worlds.HookID(sdkHookID("hook_1", runID)) {
			t.Fatalf("hook = %+v err = %v", hook, err)
		}
		sdkSameHook(t, hook, *first.Hook)
	})
}

// SDK: packages/world-local/src/storage.test.ts > Storage > hooks > list > should list all hooks
// SDK: packages/world-local/src/storage.test.ts > Storage > hooks > list > should filter hooks by runId
// SDK: packages/world-local/src/storage.test.ts > Storage > hooks > list > should support pagination
// SDK: packages/world-local/src/storage.test.ts > Storage > hooks > list > should support ascending sort order
// SDK: packages/world-local/src/storage.test.ts > Storage > hooks > list > should respect resolveData option
// SDK: packages/world-local/src/storage.test.ts > Storage > hooks > list > should handle empty result set
//
// Each listing is of one run's hooks, so hooks other cases create do not
// count.
func conformSDKHooksList(t *testing.T, world worlds.World) {
	ctx := context.Background()
	list := func(t *testing.T, input worlds.HooksListInput) worlds.Page[worlds.Hook] {
		t.Helper()
		page, err := world.HooksList(ctx, input)
		if err != nil {
			t.Fatal(err)
		}
		return page
	}
	hookIDs := func(hooks []worlds.Hook) []worlds.HookID {
		ids := make([]worlds.HookID, len(hooks))
		for index, hook := range hooks {
			ids[index] = hook.HookID
		}
		return ids
	}
	two := func(t *testing.T) worlds.RunID {
		runID := sdkRun(t, world, "test-workflow", []byte(`[]`))
		sdkClaim(t, world, runID, sdkHookID("hook_1", runID), "token-1:"+string(runID), 0)
		time.Sleep(2 * time.Millisecond)
		sdkClaim(t, world, runID, sdkHookID("hook_2", runID), "token-2:"+string(runID), 0)
		return runID
	}
	t.Run("oldest first", func(t *testing.T) {
		runID := two(t)
		page := list(t, worlds.HooksListInput{RunID: runID})
		if !slices.Equal(hookIDs(page.Data), []worlds.HookID{worlds.HookID(sdkHookID("hook_1", runID)), worlds.HookID(sdkHookID("hook_2", runID))}) || page.Data[1].CreatedAt.Before(page.Data[0].CreatedAt) {
			t.Fatalf("hooks = %+v", page.Data)
		}
	})
	t.Run("by run", func(t *testing.T) {
		runID := sdkRun(t, world, "test-workflow", []byte(`[]`))
		other := sdkRun(t, world, "test-workflow-2", []byte(`[]`))
		sdkClaim(t, world, runID, sdkHookID("hook_run1", runID), "token-run1:"+string(runID), 0)
		sdkClaim(t, world, other, sdkHookID("hook_run2", other), "token-run2:"+string(other), 0)
		if page := list(t, worlds.HooksListInput{RunID: other}); len(page.Data) != 1 || page.Data[0].HookID != worlds.HookID(sdkHookID("hook_run2", other)) || page.Data[0].RunID != other {
			t.Fatalf("hooks = %+v", page.Data)
		}
	})
	t.Run("pagination", func(t *testing.T) {
		runID := sdkRun(t, world, "test-workflow", []byte(`[]`))
		for i := range 5 {
			sdkClaim(t, world, runID, sdkHookID(fmt.Sprintf("hook_%d", i), runID), fmt.Sprintf("token-%d:%s", i, runID), 0)
		}
		first := list(t, worlds.HooksListInput{RunID: runID, Pagination: worlds.Pagination{Limit: 2}})
		if len(first.Data) != 2 || first.Cursor == "" || !first.HasMore {
			t.Fatalf("first page = %+v", first)
		}
		second := list(t, worlds.HooksListInput{RunID: runID, Pagination: worlds.Pagination{Limit: 2, Cursor: first.Cursor}})
		if len(second.Data) != 2 || second.Data[0].HookID == first.Data[0].HookID {
			t.Fatalf("second page = %+v", second)
		}
	})
	t.Run("ascending", func(t *testing.T) {
		runID := two(t)
		page := list(t, worlds.HooksListInput{RunID: runID, Pagination: worlds.Pagination{SortOrder: worlds.SortOrderAscending}})
		if !slices.Equal(hookIDs(page.Data), []worlds.HookID{worlds.HookID(sdkHookID("hook_1", runID)), worlds.HookID(sdkHookID("hook_2", runID))}) {
			t.Fatalf("hooks = %+v", page.Data)
		}
	})
	t.Run("without data", func(t *testing.T) {
		runID := sdkRun(t, world, "test-workflow", []byte(`[]`))
		conformAppend(t, world, runID, worlds.EventTypeHookCreated, sdkHookID("hook_with_response", runID), worlds.DataHookCreated{Token: "token-with-response:" + string(runID), Metadata: []byte(`"meta"`)})
		if page := list(t, worlds.HooksListInput{RunID: runID, ResolveData: worlds.ResolveDataAll}); len(page.Data) != 1 {
			t.Fatalf("hooks with data = %+v", page.Data)
		}
		if page := list(t, worlds.HooksListInput{RunID: runID, ResolveData: worlds.ResolveDataNone}); len(page.Data) != 1 || page.Data[0].Metadata != nil || page.Data[0].HookID != worlds.HookID(sdkHookID("hook_with_response", runID)) {
			t.Fatalf("hooks without data = %+v", page.Data)
		}
	})
	t.Run("empty", func(t *testing.T) {
		runID := sdkRun(t, world, "test-workflow", []byte(`[]`))
		if page := list(t, worlds.HooksListInput{RunID: runID}); len(page.Data) != 0 || page.Cursor != "" || page.HasMore {
			t.Fatalf("hooks = %+v", page)
		}
	})
}

// SDK: packages/world-local/src/storage.test.ts > Storage > hooks > create > keeps a retained Hook available after %s
// SDK: packages/world-local/src/storage.test.ts > Storage > hooks > create > does not delete expired Hooks while reading
// SDK: packages/world-local/src/storage.test.ts > Storage > hooks > create > releases retention only after both its deadline and run end
func conformSDKHookRetention(t *testing.T, world worlds.World) {
	ctx := context.Background()
	ends := map[worlds.EventType]any{
		worlds.EventTypeRunCompleted: worlds.DataRunCompleted{Output: []byte(`[]`)},
		worlds.EventTypeRunFailed:    worlds.DataRunFailed{Error: []byte(`[]`)},
		worlds.EventTypeRunCancelled: nil,
	}
	for _, end := range []worlds.EventType{worlds.EventTypeRunCompleted, worlds.EventTypeRunFailed, worlds.EventTypeRunCancelled} {
		t.Run("retained after "+string(end), func(t *testing.T) {
			runID := sdkRun(t, world, "test-workflow", []byte(`[]`))
			until := time.Now().Add(time.Minute).UTC().Truncate(time.Millisecond)
			conformAppend(t, world, runID, worlds.EventTypeHookCreated, sdkHookID("hook_retained", runID), worlds.DataHookCreated{Token: "retained-token:" + string(runID), TokenRetentionUntil: &until})
			conformAppend(t, world, runID, worlds.EventTypeRunStarted, "", nil)
			conformAppend(t, world, runID, end, "", ends[end])
			page, err := world.HooksList(ctx, worlds.HooksListInput{RunID: runID})
			if err != nil || len(page.Data) != 1 || page.Data[0].HookID != worlds.HookID(sdkHookID("hook_retained", runID)) || page.Data[0].TokenRetentionUntil == nil || !page.Data[0].TokenRetentionUntil.Equal(until) {
				t.Fatalf("hooks = %+v err = %v", page.Data, err)
			}
		})
	}
	t.Run("expired hooks", func(t *testing.T) {
		runID := sdkRun(t, world, "test-workflow", []byte(`[]`))
		until := time.Now().Add(200 * time.Millisecond).UTC()
		conformAppend(t, world, runID, worlds.EventTypeHookCreated, "hook_expired", worlds.DataHookCreated{Token: "expired-hook:" + string(runID), TokenRetentionUntil: &until})
		conformAppend(t, world, runID, worlds.EventTypeRunCompleted, "", ends[worlds.EventTypeRunCompleted])
		time.Sleep(time.Until(until) + 10*time.Millisecond)
		if page, err := world.HooksList(ctx, worlds.HooksListInput{RunID: runID}); err != nil || len(page.Data) != 0 {
			t.Fatalf("hooks past their retention = %+v err = %v", page.Data, err)
		}
	})
	t.Run("deadline and run end", func(t *testing.T) {
		runID := sdkRun(t, world, "test-workflow", []byte(`[]`))
		token := "expired-retention-token:" + string(runID)
		until := time.Now().Add(200 * time.Millisecond).UTC()
		conformAppend(t, world, runID, worlds.EventTypeHookCreated, "hook_owner", worlds.DataHookCreated{Token: token, TokenRetentionUntil: &until})
		replacement := sdkRun(t, world, "replacement", []byte(`[]`))
		time.Sleep(time.Until(until) + 10*time.Millisecond)
		sdkConflict(t, sdkClaim(t, world, replacement, "hook_conflict", token, 0), token, runID)
		conformAppend(t, world, runID, worlds.EventTypeRunStarted, "", nil)
		conformAppend(t, world, runID, worlds.EventTypeRunCompleted, "", ends[worlds.EventTypeRunCompleted])
		_, err := world.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: token})
		conformExpectType[*worlds.HookNotFoundError](t, err, "token past its retention after the run ended")
		for _, run := range []worlds.RunID{runID, replacement} {
			if page, err := world.HooksList(ctx, worlds.HooksListInput{RunID: run}); err != nil || len(page.Data) != 0 {
				t.Fatalf("hooks of %s = %+v err = %v", run, page.Data, err)
			}
		}
		if claimed := sdkClaim(t, world, replacement, "hook_replacement", token, 0); claimed.Hook == nil || claimed.Hook.RunID != replacement || claimed.Hook.Token != token {
			t.Fatalf("replacement claim = %+v", claimed)
		}
	})
}

// SDK: packages/world-local/src/storage.test.ts > Storage > concurrent entity-creation races > should reject duplicate correlated workflow attr_set events
// SDK: packages/world-local/src/storage.test.ts > Storage > concurrent entity-creation races > should not claim the attr_set correlation lock when validation fails
// SDK: packages/world-local/src/storage.test.ts > Storage > concurrent entity-creation races > should reject sequential duplicate step_created calls
// SDK: packages/world-local/src/storage.test.ts > Storage > concurrent entity-creation races > should reject duplicate same-hook hook_created with EntityConflictError, not hook_conflict
// SDK: packages/world-local/src/storage.test.ts > Storage > concurrent entity-creation races > should still emit hook_conflict for a different hookId reusing the same token in the same run
// SDK: packages/world-local/src/storage.test.ts > Storage > concurrent entity-creation races > should still emit hook_conflict for the same hookId in a different run reusing the same token
// SDK: packages/world-local/src/storage.test.ts > Storage > concurrent entity-creation races > does not mutate an already-committed hook entity when a duplicate hook_created retry collides
func conformSDKRepeatedCreations(t *testing.T, world worlds.World) {
	ctx := context.Background()
	workflow := worlds.AttributeWriter{Type: worlds.AttributeWriterWorkflow}
	value := func(s string) *string { return &s }
	correlated := func(t *testing.T, runID worlds.RunID, eventType worlds.EventType, correlationID string) int {
		t.Helper()
		count := 0
		for _, event := range conformAllEvents(t, world, runID) {
			if event.EventType == eventType && event.CorrelationID == correlationID {
				count++
			}
		}
		return count
	}
	t.Run("attr_set", func(t *testing.T) {
		if !world.Capabilities().SetAttributes {
			t.Skip("the World does not store attributes")
		}
		runID := sdkStartedRun(t, world)
		running := worlds.DataAttrSet{Changes: []worlds.RunsAttributeChange{{Key: "phase", Value: value("running")}}, Writer: workflow}
		conformAppend(t, world, runID, worlds.EventTypeAttrSet, "attr_dup_1", running)
		_, err := sdkTry(world, runID, worlds.EventTypeAttrSet, "attr_dup_1", running)
		conformExpect(t, err, worlds.ErrEntityConflict, "repeated attr_set")
		_, err = sdkTry(world, runID, worlds.EventTypeAttrSet, "attr_dup_1", worlds.DataAttrSet{Changes: []worlds.RunsAttributeChange{{Key: "phase", Value: value("DIVERGED")}}, Writer: workflow})
		conformExpect(t, err, worlds.ErrEntityConflict, "repeated attr_set with other changes")
		if run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID}); err != nil || run.Attributes["phase"] != "running" {
			t.Fatalf("attributes = %v err = %v", run.Attributes, err)
		}
		if count := correlated(t, runID, worlds.EventTypeAttrSet, "attr_dup_1"); count != 1 {
			t.Fatalf("attr_set events = %d, want 1", count)
		}
	})
	t.Run("attr_set refused by validation", func(t *testing.T) {
		if !world.Capabilities().SetAttributes {
			t.Skip("the World does not store attributes")
		}
		runID := sdkStartedRun(t, world)
		_, err := sdkTry(world, runID, worlds.EventTypeAttrSet, "attr_validation_retry", worlds.DataAttrSet{Changes: []worlds.RunsAttributeChange{{Key: "$reserved", Value: value("nope")}}, Writer: workflow})
		conformExpect(t, err, worlds.ErrAttributeInvalid, "a reserved attribute without the opt-in")
		conformAppend(t, world, runID, worlds.EventTypeAttrSet, "attr_validation_retry", worlds.DataAttrSet{Changes: []worlds.RunsAttributeChange{{Key: "$reserved", Value: value("ok")}}, Writer: workflow, AllowReservedAttributes: true})
		if run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID}); err != nil || run.Attributes["$reserved"] != "ok" {
			t.Fatalf("attributes = %v err = %v", run.Attributes, err)
		}
	})
	t.Run("step_created twice", func(t *testing.T) {
		runID := sdkStartedRun(t, world)
		sdkStep(t, world, runID, "step_seq_dup", "test-step", []byte(`[]`))
		_, err := sdkTry(world, runID, worlds.EventTypeStepCreated, "step_seq_dup", worlds.DataStepCreated{StepName: "test-step", Input: []byte(`[]`)})
		conformExpect(t, err, worlds.ErrEntityConflict, "repeated step_created")
	})
	t.Run("same hook twice", func(t *testing.T) {
		runID := sdkStartedRun(t, world)
		token := "idempotent-token:" + string(runID)
		sdkClaim(t, world, runID, "hook_idem_1", token, 0)
		_, err := sdkTry(world, runID, worlds.EventTypeHookCreated, "hook_idem_1", worlds.DataHookCreated{Token: token})
		conformExpect(t, err, worlds.ErrEntityConflict, "repeated hook_created")
		if created, conflicts := correlated(t, runID, worlds.EventTypeHookCreated, "hook_idem_1"), sdkCount(t, world, runID, worlds.EventTypeHookConflict); created != 1 || conflicts != 0 {
			t.Fatalf("hook_created events = %d, hook_conflict events = %d", created, conflicts)
		}
	})
	t.Run("another hook of the run", func(t *testing.T) {
		runID := sdkStartedRun(t, world)
		token := "same-run-different-hook-token:" + string(runID)
		sdkClaim(t, world, runID, "hook_a", token, 0)
		sdkConflict(t, sdkClaim(t, world, runID, "hook_b", token, 0), token, runID)
	})
	t.Run("the same hook ID in another run", func(t *testing.T) {
		runID := sdkStartedRun(t, world)
		token := "cross-run-same-hookid-token:" + string(runID)
		sdkClaim(t, world, runID, "hook_shared_id", token, 0)
		other := sdkRun(t, world, "other-workflow", []byte(`[]`))
		sdkConflict(t, sdkClaim(t, world, other, "hook_shared_id", token, 0), token, runID)
	})
	t.Run("a repeat with other data", func(t *testing.T) {
		runID := sdkStartedRun(t, world)
		token := "no-mutate-on-duplicate-token:" + string(runID)
		first := conformAppend(t, world, runID, worlds.EventTypeHookCreated, "hook_no_mutate_on_duplicate", worlds.DataHookCreated{Token: token, Metadata: []byte(`[170]`), IsWebhook: true})
		if first.EventType != worlds.EventTypeHookCreated || first.Hook == nil {
			t.Fatalf("hook_created = %+v", first)
		}
		if hook, err := world.HooksGet(ctx, worlds.HooksGetInput{HookID: "hook_no_mutate_on_duplicate"}); err != nil || string(hook.Metadata) != `[170]` || !hook.IsWebhook {
			t.Fatalf("hook = %+v err = %v", hook, err)
		}
		_, err := sdkTry(world, runID, worlds.EventTypeHookCreated, "hook_no_mutate_on_duplicate", worlds.DataHookCreated{Token: token, Metadata: []byte(`[187]`), IsWebhook: false})
		conformExpect(t, err, worlds.ErrEntityConflict, "repeated hook_created with other data")
		if hook, err := world.HooksGet(ctx, worlds.HooksGetInput{HookID: "hook_no_mutate_on_duplicate"}); err != nil || string(hook.Metadata) != `[170]` || !hook.IsWebhook {
			t.Fatalf("hook = %+v err = %v", hook, err)
		}
		if count := correlated(t, runID, worlds.EventTypeHookCreated, "hook_no_mutate_on_duplicate"); count != 1 {
			t.Fatalf("hook_created events = %d, want 1", count)
		}
	})
}

// SDK: packages/world-local/src/storage/hook-force-claim.test.ts > local World hook force-claim > three claimers forcing the same token at once converge on one owner, each loser told
func conformSDKForceClaimRace(t *testing.T, world worlds.World) {
	ctx := context.Background()
	victim := sdkStartedRun(t, world)
	token := "channel:race:" + string(victim)
	sdkClaim(t, world, victim, "hook_victim", token, 0)
	claimers := make([]worlds.RunID, 3)
	for index := range claimers {
		claimers[index] = sdkStartedRun(t, world)
	}
	results := make([]worlds.EventResult, len(claimers))
	errs := make([]error, len(claimers))
	var group sync.WaitGroup
	for index, runID := range claimers {
		group.Go(func() {
			results[index], errs[index] = sdkTry(world, runID, worlds.EventTypeHookCreated, fmt.Sprintf("hook_claimer_%d", index+1), worlds.DataHookCreated{Token: token, Force: true})
		})
	}
	group.Wait()
	if err := errors.Join(errs...); err != nil {
		t.Fatal(err)
	}
	for _, result := range results {
		if result.EventType != worlds.EventTypeHookCreated {
			t.Fatalf("forced claim = %+v", result)
		}
	}
	owner, err := world.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: token})
	if err != nil || !slices.Contains(claimers, owner.RunID) || owner.ClaimedFrom == nil {
		t.Fatalf("owner = %+v err = %v", owner, err)
	}
	var disposal *worlds.Event
	for _, event := range conformAllEvents(t, world, victim) {
		if event.EventType == worlds.EventTypeHookDisposed {
			disposal = &event
		}
	}
	var disposed worlds.HookDisposedEventData
	if disposal == nil || json.Unmarshal(disposal.Data, &disposed) != nil || disposed.ForceClaimedBy == nil || !slices.Contains(claimers, disposed.ForceClaimedBy.RunID) {
		t.Fatalf("victim's disposal = %+v", disposal)
	}
	for _, runID := range claimers {
		want := []worlds.EventType{worlds.EventTypeRunCreated, worlds.EventTypeRunStarted, worlds.EventTypeHookCreated, worlds.EventTypeHookDisposed}
		if runID == owner.RunID {
			want = want[:3]
		}
		if types := sdkTypes(conformAllEvents(t, world, runID)); !slices.Equal(types, want) {
			t.Fatalf("claimer %s events = %v, want %v", runID, types, want)
		}
	}
}

// SDK: packages/world-local/src/storage/hook-resume-dedup.test.ts > world-local hook_received resume dedup > converges a committed resume re-ensured AFTER the hook was disposed
// SDK: packages/world-local/src/storage/hook-resume-producer-consumer.test.ts > world-local lazy hook resume: producer/consumer convergence > producer-wins: the direct write commits first; the consumer re-ensure adopts it
// SDK: packages/world-local/src/storage/hook-resume-producer-consumer.test.ts > world-local lazy hook resume: producer/consumer convergence > consumer-wins: the re-ensure commits first (producer write lost/delayed), then converges on redelivery
// SDK: packages/world-local/src/storage/hook-resume-producer-consumer.test.ts > world-local lazy hook resume: producer/consumer convergence > concurrent race: producer and consumer fire simultaneously and still commit exactly one event
// SDK: packages/world-local/src/storage/hook-resume-producer-consumer.test.ts > world-local lazy hook resume: producer/consumer convergence > distinct resumes of a reusable hook each commit their own event under load
// SDK: packages/world-local/src/storage/hook-resume-producer-consumer.test.ts > world-local lazy hook resume: producer/consumer convergence > terminal rejection: a resume against an already-completed run is rejected and writes no event
func conformSDKResumeConvergence(t *testing.T, world worlds.World) {
	ctx := context.Background()
	setup := func(t *testing.T) worlds.RunID {
		t.Helper()
		runID := sdkRun(t, world, "test-workflow", []byte(`[]`))
		token := "order:1:" + string(runID)
		sdkClaim(t, world, runID, sdkHookID("hook_1", runID), token, 0)
		sdkRequireResumeDedup(t, world, token)
		return runID
	}
	write := func(runID worlds.RunID, resumeID string, payload []byte) (worlds.EventResult, error) {
		data, err := worlds.EncodeHookReceived(payload, "test-workflow")
		if err != nil {
			return worlds.EventResult{}, err
		}
		return world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeHookReceived, CorrelationID: sdkHookID("hook_1", runID), Data: data},
			Options: worlds.EventsCreateOptions{ResumeID: resumeID, ResumePayloadDigest: worlds.ResumeDigest(payload)}})
	}
	must := func(t *testing.T, runID worlds.RunID, resumeID string, payload []byte) worlds.EventResult {
		t.Helper()
		result, err := write(runID, resumeID, payload)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	received := func(t *testing.T, runID worlds.RunID, want int) {
		t.Helper()
		if count := sdkCount(t, world, runID, worlds.EventTypeHookReceived); count != want {
			t.Fatalf("hook_received events = %d, want %d", count, want)
		}
	}
	t.Run("after the hook was disposed", func(t *testing.T) {
		runID := setup(t)
		payload := []byte(`[1,2,3]`)
		first := must(t, runID, "resume_1", payload)
		conformAppend(t, world, runID, worlds.EventTypeHookDisposed, sdkHookID("hook_1", runID), nil)
		if second := must(t, runID, "resume_1", payload); second.EventID != first.EventID {
			t.Fatalf("resume after disposal = %s, want %s", second.EventID, first.EventID)
		}
		received(t, runID, 1)
		if _, err := write(runID, "resume_2", []byte(`[9]`)); err == nil {
			t.Fatal("a new resume of a disposed hook was accepted")
		}
	})
	t.Run("producer first", func(t *testing.T) {
		runID := setup(t)
		payload := []byte(`[1,2,3]`)
		producer := must(t, runID, "r_1", payload)
		if consumer := must(t, runID, "r_1", payload); consumer.EventID != producer.EventID {
			t.Fatalf("consumer = %s, want %s", consumer.EventID, producer.EventID)
		}
		received(t, runID, 1)
	})
	t.Run("consumer first", func(t *testing.T) {
		runID := setup(t)
		payload := []byte(`[4,5,6]`)
		consumer := must(t, runID, "r_2", payload)
		redelivery := must(t, runID, "r_2", payload)
		lateProducer := must(t, runID, "r_2", payload)
		if redelivery.EventID != consumer.EventID || lateProducer.EventID != consumer.EventID {
			t.Fatalf("redelivery %s and late producer %s, want %s", redelivery.EventID, lateProducer.EventID, consumer.EventID)
		}
		received(t, runID, 1)
	})
	t.Run("concurrent", func(t *testing.T) {
		runID := setup(t)
		payload := []byte(`[7,8,9]`)
		results := make([]worlds.EventResult, 2)
		errs := make([]error, 2)
		var group sync.WaitGroup
		for index := range 2 {
			group.Go(func() { results[index], errs[index] = write(runID, "r_3", payload) })
		}
		group.Wait()
		if err := errors.Join(errs...); err != nil || results[0].EventID != results[1].EventID {
			t.Fatalf("concurrent writes = %s and %s err = %v", results[0].EventID, results[1].EventID, err)
		}
		received(t, runID, 1)
	})
	t.Run("distinct resumes", func(t *testing.T) {
		runID := setup(t)
		const resumes = 5
		results := make([]worlds.EventResult, resumes*2)
		errs := make([]error, resumes*2)
		var group sync.WaitGroup
		for index := range resumes * 2 {
			group.Go(func() {
				resume := index / 2
				results[index], errs[index] = write(runID, fmt.Sprintf("r_multi_%d", resume), []byte(fmt.Sprintf("[%d]", resume)))
			})
		}
		group.Wait()
		if err := errors.Join(errs...); err != nil {
			t.Fatal(err)
		}
		unique := map[worlds.EventID]bool{}
		for index := 0; index < len(results); index += 2 {
			if results[index].EventID != results[index+1].EventID {
				t.Fatalf("writers of resume %d = %s and %s", index/2, results[index].EventID, results[index+1].EventID)
			}
			unique[results[index].EventID] = true
		}
		if len(unique) != resumes {
			t.Fatalf("events = %d, want %d", len(unique), resumes)
		}
		received(t, runID, resumes)
	})
	t.Run("finished run", func(t *testing.T) {
		runID := setup(t)
		conformAppend(t, world, runID, worlds.EventTypeRunCompleted, "", worlds.DataRunCompleted{Output: []byte(`[]`)})
		_, err := write(runID, "r_term", []byte(`[1]`))
		if !is[*worlds.HookNotFoundError](err) && !is[*worlds.RunExpiredError](err) {
			t.Fatalf("resume of a finished run = %v, want HookNotFoundError or RunExpiredError", err)
		}
		received(t, runID, 0)
	})
}

// SDK: packages/world-local/src/storage/atomic-start-hook.test.ts > atomic start Hook (world-local) > refuses a queued candidate older than the orphan bound with a permanent code
// SDK: packages/world-local/src/storage/atomic-start-hook.test.ts > atomic start Hook (world-local) > refuses a candidate that already exists without the start Hook
func conformSDKStartHookCandidates(t *testing.T, world worlds.World) {
	if !world.Capabilities().StartHook {
		t.Skip("the World creates no start hooks")
	}
	ctx := context.Background()
	creation := func(t *testing.T, runID worlds.RunID, token string) []byte {
		t.Helper()
		data, err := worlds.RunCreatedData(worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "wf", DeploymentID: "dpl_local", Input: []byte(`[1]`), StartHook: &worlds.StartHook{Token: token}})
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	t.Run("a queued candidate past the orphan bound", func(t *testing.T) {
		dayAgo := worlds.RunID("wrun_" + ulid.MustNew(ulid.Timestamp(time.Now().Add(-25*time.Hour)), rand.Reader).String())
		_, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: dayAgo, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeRunStarted, Data: creation(t, dayAgo, "order:stale:"+string(dayAgo))}})
		conformExpect(t, err, worlds.ErrStartHookRejected, "a queued candidate minted 25 hours ago")
	})
	t.Run("an ordinary run", func(t *testing.T) {
		runID := worlds.NewRunID()
		if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "wf", DeploymentID: "dpl_local", Input: []byte(`[1]`)}); err != nil {
			t.Fatal(err)
		}
		token := "order:x:" + string(runID)
		// The run exists, so its run_started starts it as any run's does.
		if _, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeRunStarted, Data: creation(t, runID, token)}}); err != nil {
			t.Fatalf("run_started of an ordinary run carrying a start hook = %v", err)
		}
		_, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "wf", DeploymentID: "dpl_local", Input: []byte(`[1]`), StartHook: &worlds.StartHook{Token: token}})
		conformExpect(t, err, worlds.ErrStartHookRejected, "a start hook for an ordinary run")
	})
}

// conformSDKStartHook checks what a run's start hook needs and keeps: a
// non-empty token, its token's retention on the run's run_created, and its
// creation by a first delivery's run_started.
//
// SDK: packages/world/src/start-hook.test.ts > start Hook schemas > requires a non-empty string token
// SDK: packages/world/src/start-hook.test.ts > start Hook schemas > parses the start Hook on run_created, with an absolute retention
// SDK: packages/world/src/start-hook.test.ts > start Hook schemas > parses the start Hook on a resilient run_started
func conformSDKStartHook(t *testing.T, world worlds.World) {
	if !world.Capabilities().StartHook {
		t.Skip("the World does not create start hooks")
	}
	ctx := context.Background()
	t.Run("empty token", func(t *testing.T) {
		runID := conformRunID("sdk_start_empty")
		_, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "conformance", StartHook: &worlds.StartHook{Token: ""}})
		conformExpect(t, err, worlds.ErrEventInvalid, "start hook without a token")
		if _, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID}); !errors.Is(err, worlds.ErrRunNotFound) {
			t.Fatalf("refused start created a run: %v", err)
		}
	})
	t.Run("retention on run_created", func(t *testing.T) {
		runID := conformRunID("sdk_start_retention")
		token := fmt.Sprintf("order:%s", runID)
		until := time.Now().Add(time.Hour).UTC().Truncate(time.Millisecond)
		if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "conformance", StartHook: &worlds.StartHook{Token: token, TokenRetentionUntil: &until}}); err != nil {
			t.Fatal(err)
		}
		events := conformAllEvents(t, world, runID)
		start := worlds.RunStartHook(events[0].Data)
		if events[0].EventType != worlds.EventTypeRunCreated || start == nil || start.Token != token || start.TokenRetentionUntil == nil || !start.TokenRetentionUntil.Equal(until) {
			t.Fatalf("run_created start hook = %+v, want token %s retained until %s", start, token, until)
		}
	})
	t.Run("resilient run_started", func(t *testing.T) {
		runID := conformRunID("sdk_start_resilient")
		token := fmt.Sprintf("order:%s", runID)
		data, err := worlds.RunCreatedData(worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "conformance", Input: []byte(`"input"`), StartHook: &worlds.StartHook{Token: token}})
		if err != nil {
			t.Fatal(err)
		}
		one := 1
		if _, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeRunStarted, Data: data}, Options: worlds.EventsCreateOptions{InBandCount: &one}}); err != nil {
			t.Fatal(err)
		}
		hook, err := world.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: token})
		if err != nil || hook.RunID != runID || hook.HookID != worlds.StartHookID(runID) {
			t.Fatalf("start hook = %+v err = %v", hook, err)
		}
		if start := worlds.RunStartHook(conformAllEvents(t, world, runID)[0].Data); start == nil || start.Token != token {
			t.Fatalf("run_created start hook = %+v", start)
		}
	})
}
