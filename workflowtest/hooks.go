package workflowtest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

func conformHookTokens(t *testing.T, world worlds.World) {
	ctx := context.Background()
	runID := conformCreateRun(t, world, "hooks")
	other := conformCreateRun(t, world, "hooks_other")
	token := fmt.Sprintf("token-%d", time.Now().UnixNano())

	_, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: conformEvent(t, worlds.EventTypeHookCreated, "hook_x", worlds.DataHookCreated{Token: ""})})
	conformExpect(t, err, worlds.ErrEventInvalid, "claim without token")
	_, err = conformClaim(world, "wrun_missing", "hook_x", token, false)
	conformExpect(t, err, worlds.ErrRunNotFound, "claim for missing run")

	zero := 0
	claimed, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: conformEvent(t, worlds.EventTypeHookCreated, "hook_a", conformHookData(token, false, 0)), Options: worlds.EventsCreateOptions{EventCount: &zero}})
	if err != nil {
		t.Fatal(err)
	}
	if claimed.EventID != worlds.EventIDForSlot(2) || claimed.EventType != worlds.EventTypeHookCreated || claimed.AllocatedEventCount != 1 || claimed.Hook == nil || claimed.Hook.ResumeContext.WorkflowName != "conformance" || claimed.Hook.ClaimedFrom != nil || len(claimed.Events) != 1 {
		t.Fatalf("claim = %#v", claimed)
	}
	hook, err := world.HooksGet(ctx, worlds.HooksGetInput{HookID: "hook_a"})
	if err != nil || hook.RunID != runID || hook.ResumeContext.WorkflowName != "conformance" || hook.Token != token || string(hook.Metadata) != "meta" || hook.CreatedAt.IsZero() {
		t.Fatalf("hook = %#v err=%v", hook, err)
	}
	stripped, err := world.HooksGet(ctx, worlds.HooksGetInput{HookID: "hook_a", ResolveData: worlds.ResolveDataNone})
	if err != nil || stripped.Metadata != nil || stripped.ResumeContext.WorkflowName != "conformance" {
		t.Fatalf("ResolveDataNone hook = %#v err=%v", stripped, err)
	}
	byToken, err := world.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: token})
	if err != nil || byToken.HookID != "hook_a" || byToken.ResumeContext.WorkflowName != "conformance" {
		t.Fatalf("hook by token = %#v err=%v", byToken, err)
	}
	_, err = world.HooksGet(ctx, worlds.HooksGetInput{HookID: "hook_missing"})
	conformExpect(t, err, worlds.ErrHookNotFound, "missing hook")
	_, err = world.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: "token-missing"})
	conformExpect(t, err, worlds.ErrHookNotFound, "missing token")

	conflict, err := conformClaim(world, other, "hook_b", token, false)
	var data worlds.DataHookCreated
	if err != nil || conflict.EventType != worlds.EventTypeHookConflict || json.Unmarshal(conflict.Data, &data) != nil || data.ConflictingRunID != runID {
		t.Fatalf("conflict = %#v, %v", conflict, err)
	}

	second := token + "-second"
	if _, err := conformClaim(world, runID, "hook_c", second, false); err != nil {
		t.Fatal(err)
	}
	conformAppend(t, world, runID, worlds.EventTypeHookReceived, "hook_c", conformReceived(t, map[string]any{"request": map[string]any{"method": "hello"}, "response": map[string]any{"status": 42}}))
	plainHook, err := world.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: second})
	if err != nil || plainHook.DisposedAt != nil {
		t.Fatalf("ordinary payload was interpreted as webhook metadata: %#v err=%v", plainHook, err)
	}
	if _, err := conformClaim(world, other, "hook_d", token+"-other", false); err != nil {
		t.Fatal(err)
	}
	hooks, err := world.HooksList(ctx, worlds.HooksListInput{RunID: runID})
	if err != nil || len(hooks.Data) != 2 || hooks.Data[0].HookID != "hook_a" || hooks.Data[1].HookID != "hook_c" {
		t.Fatalf("hooks of run = %#v err=%v", hooks, err)
	}
	first, err := world.HooksList(ctx, worlds.HooksListInput{RunID: runID, Pagination: worlds.Pagination{Limit: 1}})
	if err != nil || len(first.Data) != 1 || !first.HasMore {
		t.Fatalf("first hook page = %#v err=%v", first, err)
	}
	next, err := world.HooksList(ctx, worlds.HooksListInput{RunID: runID, Pagination: worlds.Pagination{Limit: 1, Cursor: first.Cursor}})
	if err != nil || len(next.Data) != 1 || next.Data[0].HookID != "hook_c" || next.HasMore {
		t.Fatalf("second hook page = %#v err=%v", next, err)
	}

	conformAppend(t, world, runID, worlds.EventTypeHookDisposed, "hook_a", nil)
	_, err = world.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: token})
	conformExpect(t, err, worlds.ErrHookNotFound, "disposed token")
	if _, err := conformClaim(world, other, "hook_b", token, false); err != nil {
		t.Fatalf("claim released token: %v", err)
	}

	conformAppend(t, world, runID, worlds.EventTypeRunCompleted, "", nil)
	_, err = conformClaim(world, runID, "hook_e", token+"-terminal", false)
	conformExpect(t, err, worlds.ErrRunTerminal, "claim from terminal run")
}

func conformHooksForce(t *testing.T, world worlds.World) {
	ctx := context.Background()
	owner := conformCreateRun(t, world, "force_owner")
	claimant := conformCreateRun(t, world, "force_claimant")
	token := fmt.Sprintf("force-%d", time.Now().UnixNano())
	if _, err := conformClaim(world, owner, "hook_owner", token, false); err != nil {
		t.Fatal(err)
	}
	forced, err := conformClaim(world, claimant, "hook_claimant", token, true)
	if err != nil {
		t.Fatal(err)
	}
	if forced.Hook == nil || forced.Hook.ClaimedFrom == nil || forced.Hook.ClaimedFrom.RunID != owner || forced.EventType != worlds.EventTypeHookCreated || forced.AllocatedEventCount != 1 {
		t.Fatalf("forced claim = %#v", forced)
	}
	events := conformAllEvents(t, world, owner)
	last := events[len(events)-1]
	var disposed worlds.HookDisposedEventData
	if last.EventType != worlds.EventTypeHookDisposed || last.CorrelationID != "hook_owner" || json.Unmarshal(last.Data, &disposed) != nil ||
		disposed.ForceClaimedBy == nil || disposed.ForceClaimedBy.RunID != claimant || disposed.ForceClaimedBy.HookID != "hook_claimant" {
		t.Fatalf("owner events = %#v", events)
	}
	hook, err := world.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: token})
	if err != nil || hook.HookID != "hook_claimant" || hook.RunID != claimant {
		t.Fatalf("token owner after force = %#v err=%v", hook, err)
	}
	// A takeover within one run returns the induced disposal in the delta and
	// counts both writes, so replay cannot omit a slot or reuse a stale fence.
	seen, fence := 2, 0
	self, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: claimant, Event: conformEvent(t, worlds.EventTypeHookCreated, "hook_self", worlds.DataHookCreated{Token: token, Force: true}), Options: worlds.EventsCreateOptions{EventCount: &seen, InBandCount: &fence}})
	if err != nil || self.AllocatedEventCount != 2 || self.InBandCount != 2 || len(self.Events) != 1 || self.Events[0].EventType != worlds.EventTypeHookDisposed || self.Events[0].CorrelationID != "hook_claimant" {
		t.Fatalf("same-run takeover = %#v err=%v", self, err)
	}

	conformAppend(t, world, claimant, worlds.EventTypeRunFailed, "", nil)
	late := conformCreateRun(t, world, "force_late")
	takeover, err := conformClaim(world, late, "hook_late", token, true)
	if err != nil || takeover.EventType != worlds.EventTypeHookCreated {
		t.Fatalf("force over terminal owner = %#v err=%v", takeover, err)
	}
	if events := conformAllEvents(t, world, claimant); events[len(events)-1].EventType != worlds.EventTypeRunFailed {
		t.Fatal("terminal owner received an additional event")
	}
}

func conformHooksRetention(t *testing.T, world worlds.World) {
	ctx := context.Background()
	runID := conformCreateRun(t, world, "retention")
	plain := fmt.Sprintf("plain-%d", time.Now().UnixNano())
	if _, err := conformClaim(world, runID, "hook_plain", plain, false); err != nil {
		t.Fatal(err)
	}
	conformAppend(t, world, runID, worlds.EventTypeHookReceived, "hook_plain", conformReceived(t, []byte(`{"value":1}`)))
	hook, err := world.HooksGet(ctx, worlds.HooksGetInput{HookID: "hook_plain"})
	if err != nil || hook.ReceivedAt == nil {
		t.Fatalf("received hook = %#v err=%v", hook, err)
	}
	_, err = world.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: plain})
	if err != nil {
		t.Fatalf("active hook released on receive: %v", err)
	}

	retained := fmt.Sprintf("retained-%d", time.Now().UnixNano())
	if _, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: conformEvent(t, worlds.EventTypeHookCreated, "hook_retained", conformHookData(retained, false, 300*time.Millisecond))}); err != nil {
		t.Fatal(err)
	}
	conformAppend(t, world, runID, worlds.EventTypeHookReceived, "hook_retained", nil)
	hook, err = world.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: retained})
	if err != nil || hook.TokenRetentionUntil == nil || !hook.TokenRetentionUntil.After(time.Now()) {
		t.Fatalf("retained hook = %#v err=%v", hook, err)
	}
	other := conformCreateRun(t, world, "retention_other")
	conflict, err := conformClaim(world, other, "hook_other", retained, false)
	if err != nil || conflict.EventType != worlds.EventTypeHookConflict {
		t.Fatalf("retained conflict = %#v, %v", conflict, err)
	}
	time.Sleep(400 * time.Millisecond)
	if _, err := world.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: retained}); err != nil {
		t.Fatalf("active hook expired: %v", err)
	}
	conformAppend(t, world, runID, worlds.EventTypeRunCompleted, "", nil)
	_, err = world.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: retained})
	conformExpect(t, err, worlds.ErrHookNotFound, "expired retained token")
	if _, err := conformClaim(world, other, "hook_other", retained, false); err != nil {
		t.Fatalf("claim expired token: %v", err)
	}
}

func conformWebhooks(t *testing.T, world worlds.World) {
	ctx := context.Background()
	runID := conformCreateRun(t, world, "webhooks")
	token := fmt.Sprintf("webhook-%d", time.Now().UnixNano())
	if _, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: conformEvent(t, worlds.EventTypeHookCreated, "hook_web", conformHookData(token, true, 0))}); err != nil {
		t.Fatal(err)
	}
	hook, err := world.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: token})
	if err != nil || !hook.IsWebhook || string(hook.Metadata) != "meta" {
		t.Fatalf("webhook = %#v err=%v", hook, err)
	}

	// A webhook takes any number of requests, and its payloads are opaque to
	// the World.
	for range 2 {
		conformAppend(t, world, runID, worlds.EventTypeHookReceived, "hook_web", conformReceived(t, []byte("devl-request")))
	}
	hook, err = world.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: token})
	if err != nil || hook.ReceivedAt == nil || hook.DisposedAt != nil {
		t.Fatalf("webhook after requests = %#v err=%v", hook, err)
	}
	plain := token + "-plain"
	if _, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: conformEvent(t, worlds.EventTypeHookCreated, "hook_plain_web", conformHookData(plain, false, 0))}); err != nil {
		t.Fatal(err)
	}
	if hook, err := world.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: plain}); err != nil || hook.IsWebhook {
		t.Fatalf("plain hook = %#v err=%v", hook, err)
	}
}

func conformHookDisposal(t *testing.T, world worlds.World) {
	ctx := context.Background()
	runID := conformCreateRun(t, world, "hook_disposal")
	other := conformCreateRun(t, world, "hook_disposal_other")
	token := fmt.Sprintf("dispose-%d", time.Now().UnixNano())
	if _, err := conformClaim(world, runID, "hook_a", token, false); err != nil {
		t.Fatal(err)
	}

	_, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: other, Event: conformEvent(t, worlds.EventTypeHookDisposed, "hook_a", nil)})
	conformExpect(t, err, worlds.ErrHookNotActive, "dispose another run's hook")
	if owner, err := world.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: token}); err != nil || owner.HookID != "hook_a" {
		t.Fatalf("token owner = %#v err=%v", owner, err)
	}
	_, err = world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: conformEvent(t, worlds.EventTypeHookDisposed, "hook_missing", nil)})
	conformExpect(t, err, worlds.ErrHookNotActive, "dispose a missing hook")
	_, err = world.HooksGet(ctx, worlds.HooksGetInput{HookID: "hook_missing"})
	conformExpect(t, err, worlds.ErrHookNotFound, "hook after disposing a missing hook")

	disposed := conformAppend(t, world, runID, worlds.EventTypeHookDisposed, "hook_a", nil)
	repeat, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: conformEvent(t, worlds.EventTypeHookDisposed, "hook_a", nil)})
	if err != nil || repeat.EventID != disposed.EventID || repeat.AllocatedEventCount != 0 {
		t.Fatalf("repeated hook_disposed = %#v err=%v", repeat, err)
	}

	// Disposing a hook that lost its token to a forced claim keeps the claim.
	forced := token + "-forced"
	if _, err := conformClaim(world, runID, "hook_f", forced, false); err != nil {
		t.Fatal(err)
	}
	if _, err := conformClaim(world, other, "hook_g", forced, true); err != nil {
		t.Fatal(err)
	}
	if _, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: conformEvent(t, worlds.EventTypeHookDisposed, "hook_f", nil)}); err != nil {
		t.Fatalf("dispose a force-claimed hook: %v", err)
	}
	hook, err := world.HooksGet(ctx, worlds.HooksGetInput{HookID: "hook_f"})
	if err != nil || hook.DisposedAt == nil || hook.ForceClaimedBy == nil || hook.ForceClaimedBy.HookID != "hook_g" {
		t.Fatalf("force-claimed hook = %#v err=%v", hook, err)
	}
}

func conformStartHook(t *testing.T, world worlds.World) {
	if !world.Capabilities().StartHook {
		return
	}
	ctx := context.Background()
	token := fmt.Sprintf("conformance-start-%d", time.Now().UnixNano())
	owner := conformRunID("start_hook")
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: owner, WorkflowName: "conformance", StartHook: &worlds.StartHook{Token: token, Metadata: []byte(`"metadata"`)}}); err != nil {
		t.Fatal(err)
	}
	// The start hook exists with the run, under its run's ID.
	hook, err := world.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: token})
	if err != nil || hook.HookID != worlds.StartHookID(owner) || hook.RunID != owner || string(hook.Metadata) != `"metadata"` {
		t.Fatalf("start hook = %+v, %v", hook, err)
	}
	events := conformAllEvents(t, world, owner)
	if start := worlds.RunStartHook(events[0].Data); len(events) != 1 || start == nil || start.Token != token || start.HookID != worlds.StartHookID(owner) {
		t.Fatalf("run_created = %s", events[0].Data)
	}
	// It takes payloads while its run is still pending.
	conformAppend(t, world, owner, worlds.EventTypeHookReceived, string(worlds.StartHookID(owner)), conformReceived(t, "early"))

	// A second start with the token is refused before it becomes a run.
	duplicate := conformRunID("start_hook_duplicate")
	_, err = world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: duplicate, WorkflowName: "conformance", StartHook: &worlds.StartHook{Token: token}})
	var conflict *worlds.HookConflictError
	if !errors.Is(err, worlds.ErrHookConflict) || !errors.As(err, &conflict) || conflict.RunID != owner {
		t.Fatalf("duplicate start error = %v", err)
	}
	if _, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: duplicate}); !errors.Is(err, worlds.ErrRunNotFound) {
		t.Fatalf("refused start created a run: %v", err)
	}

	// The run's end frees the token, unless its retention has not passed.
	conformAppend(t, world, owner, worlds.EventTypeRunCancelled, "", nil)
	retained := conformRunID("start_hook_retained")
	until := time.Now().Add(time.Hour)
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: retained, WorkflowName: "conformance", StartHook: &worlds.StartHook{Token: token, TokenRetentionUntil: &until}}); err != nil {
		t.Fatalf("start after the owner ended: %v", err)
	}
	conformAppend(t, world, retained, worlds.EventTypeRunCancelled, "", nil)
	_, err = world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: conformRunID("start_hook_after_retention"), WorkflowName: "conformance", StartHook: &worlds.StartHook{Token: token}})
	conformExpect(t, err, worlds.ErrHookConflict, "start during the token's retention")
}
