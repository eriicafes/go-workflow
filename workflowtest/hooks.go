package workflowtest

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/oklog/ulid/v2"
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
	conformExpectType[*worlds.HookNotFoundError](t, err, "missing hook")
	_, err = world.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: "token-missing"})
	conformExpectType[*worlds.HookNotFoundError](t, err, "missing token")

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
	if err != nil || plainHook.HookID != "hook_c" {
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
	conformExpectType[*worlds.HookNotFoundError](t, err, "disposed token")
	// A disposed hook is deleted.
	_, err = world.HooksGet(ctx, worlds.HooksGetInput{HookID: "hook_a"})
	conformExpectType[*worlds.HookNotFoundError](t, err, "disposed hook")
	if hooks, err := world.HooksList(ctx, worlds.HooksListInput{RunID: runID}); err != nil || len(hooks.Data) != 1 || hooks.Data[0].HookID != "hook_c" {
		t.Fatalf("hooks of run after disposal = %#v err=%v", hooks, err)
	}
	if _, err := conformClaim(world, other, "hook_b", token, false); err != nil {
		t.Fatalf("claim released token: %v", err)
	}

	conformAppend(t, world, runID, worlds.EventTypeRunCompleted, "", nil)
	// The run's hooks without a retained token end with it.
	_, err = world.HooksGet(ctx, worlds.HooksGetInput{HookID: "hook_c"})
	conformExpectType[*worlds.HookNotFoundError](t, err, "hook of an ended run")
	_, err = conformClaim(world, runID, "hook_e", token+"-terminal", false)
	conformExpectType[*worlds.RunExpiredError](t, err, "claim from terminal run")
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
	// The claim names the run it took the token from by what wakes it.
	if from := forced.Hook.ClaimedFrom; from.WorkflowName != "conformance" || from.RunSpecVersion != worlds.SpecVersion {
		t.Fatalf("claimed from = %#v, want the owner run's workflow and spec version", from)
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
	seen, fence := 2, 1
	self, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: claimant, Event: conformEvent(t, worlds.EventTypeHookCreated, "hook_self", worlds.DataHookCreated{Token: token, Force: true}), Options: worlds.EventsCreateOptions{EventCount: &seen, InBandCount: &fence}})
	if err != nil || self.AllocatedEventCount != 2 || self.InBandCount != 3 || len(self.Events) != 1 || self.Events[0].EventType != worlds.EventTypeHookDisposed || self.Events[0].CorrelationID != "hook_claimant" {
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

	// A claim of a token a finished run retains names that run without its
	// workflow, so nothing wakes it, and records nothing in its log.
	retained := fmt.Sprintf("force-retained-%d", time.Now().UnixNano())
	finished := conformCreateRun(t, world, "force_finished")
	conformAppend(t, world, finished, worlds.EventTypeHookCreated, "hook_finished", conformHookData(retained, false, time.Hour))
	conformAppend(t, world, finished, worlds.EventTypeRunCompleted, "", nil)
	before := len(conformAllEvents(t, world, finished))
	taker := conformCreateRun(t, world, "force_taker")
	taken, err := conformClaim(world, taker, "hook_taker", retained, true)
	if err != nil || taken.EventType != worlds.EventTypeHookCreated || taken.Hook == nil || taken.Hook.ClaimedFrom == nil {
		t.Fatalf("force over a finished owner's retained token = %#v err=%v", taken, err)
	}
	if from := taken.Hook.ClaimedFrom; from.RunID != finished || from.HookID != "hook_finished" || from.WorkflowName != "" || from.DeploymentID != "" || from.RunSpecVersion != 0 {
		t.Fatalf("claimed from a finished owner = %#v, want only its run and hook", from)
	}
	if after := len(conformAllEvents(t, world, finished)); after != before {
		t.Fatalf("finished owner's log grew from %d to %d events", before, after)
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
	if err != nil || hook.HookID != "hook_plain" {
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
	conformExpectType[*worlds.HookNotFoundError](t, err, "expired retained token")
	// A finished run's hook is not read once its retention lapsed, by ID or
	// in a list.
	_, err = world.HooksGet(ctx, worlds.HooksGetInput{HookID: "hook_retained"})
	conformExpectType[*worlds.HookNotFoundError](t, err, "hook of an expired retained token")
	if page, err := world.HooksList(ctx, worlds.HooksListInput{RunID: runID}); err != nil || len(page.Data) != 0 {
		t.Fatalf("hooks of a finished run past their retention = %+v err = %v, want none", page.Data, err)
	}
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
	if err != nil || hook.HookID != "hook_web" {
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
	conformExpectType[*worlds.HookNotFoundError](t, err, "dispose another run's hook")
	if owner, err := world.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: token}); err != nil || owner.HookID != "hook_a" {
		t.Fatalf("token owner = %#v err=%v", owner, err)
	}
	_, err = world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: conformEvent(t, worlds.EventTypeHookDisposed, "hook_missing", nil)})
	conformExpectType[*worlds.HookNotFoundError](t, err, "dispose a missing hook")
	_, err = world.HooksGet(ctx, worlds.HooksGetInput{HookID: "hook_missing"})
	conformExpectType[*worlds.HookNotFoundError](t, err, "hook after disposing a missing hook")

	conformAppend(t, world, runID, worlds.EventTypeHookDisposed, "hook_a", nil)
	// A disposed hook is gone, so a repeated disposal or a late delivery
	// finds no hook.
	_, err = world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: conformEvent(t, worlds.EventTypeHookDisposed, "hook_a", nil)})
	conformExpectType[*worlds.HookNotFoundError](t, err, "repeated hook_disposed")
	_, err = world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: conformEvent(t, worlds.EventTypeHookReceived, "hook_a", conformReceived(t, []byte("late")))})
	conformExpectType[*worlds.HookNotFoundError](t, err, "resume a disposed hook")

	// A forced claim disposes the hook it took the token from, in its run.
	forced := token + "-forced"
	if _, err := conformClaim(world, runID, "hook_f", forced, false); err != nil {
		t.Fatal(err)
	}
	if _, err := conformClaim(world, other, "hook_g", forced, true); err != nil {
		t.Fatal(err)
	}
	_, err = world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: conformEvent(t, worlds.EventTypeHookDisposed, "hook_f", nil)})
	conformExpectType[*worlds.HookNotFoundError](t, err, "dispose a force-claimed hook")
	_, err = world.HooksGet(ctx, worlds.HooksGetInput{HookID: "hook_f"})
	conformExpectType[*worlds.HookNotFoundError](t, err, "force-claimed hook")
	if owner, err := world.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: forced}); err != nil || owner.HookID != "hook_g" {
		t.Fatalf("force-claimed token owner = %#v err=%v", owner, err)
	}
	_, err = world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: conformEvent(t, worlds.EventTypeHookReceived, "hook_f", conformReceived(t, []byte("late")))})
	conformExpectType[*worlds.HookForceClaimedError](t, err, "resume a force-claimed hook")
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
	if !errors.As(err, &conflict) || conflict.RunID != owner {
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
	conformExpectType[*worlds.HookConflictError](t, err, "start during the token's retention")
}

// conformResumeDedup checks a World that deduplicates resumes: a repeat of a
// resume returns the event it committed and appends nothing, a repeat with
// another payload is refused, and the event records its resume.
func conformResumeDedup(t *testing.T, world worlds.World) {
	ctx := context.Background()
	runID := conformCreateRun(t, world, "resume")
	conformAppend(t, world, runID, worlds.EventTypeHookCreated, "hook_resume", conformHookData("resume:"+string(runID), false, 0))
	if hook, err := world.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: "resume:" + string(runID)}); err != nil {
		t.Fatal(err)
	} else if !world.Capabilities().HookResumeDedup && !hook.ResumeDedup {
		t.Skip("the World does not deduplicate resumes")
	}
	resume := func(resumeID, payload string) (worlds.EventResult, error) {
		data, _ := worlds.EncodeHookReceived([]byte(payload), "conformance")
		return world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeHookReceived, CorrelationID: "hook_resume", Data: data},
			Options: worlds.EventsCreateOptions{ResumeID: resumeID, ResumePayloadDigest: worlds.ResumeDigest([]byte(payload))}})
	}
	first, err := resume("resume_a", `"one"`)
	if err != nil || first.AllocatedEventCount != 1 {
		t.Fatalf("resume = %+v err = %v", first, err)
	}
	repeat, err := resume("resume_a", `"one"`)
	if err != nil || repeat.AllocatedEventCount != 0 || repeat.EventID != first.EventID {
		t.Fatalf("repeated resume = %+v err = %v, want the first resume's event", repeat, err)
	}
	// A resume ID names one resume of one hook: the local World refuses
	// another as a conflict, and the Postgres World as invalid.
	_, err = resume("resume_a", `"two"`)
	conformExpectRefusedResume(t, err, "a resume repeated with another payload")
	conformAppend(t, world, runID, worlds.EventTypeHookCreated, "hook_other", conformHookData("other:"+string(runID), false, 0))
	other, _ := worlds.EncodeHookReceived([]byte(`"one"`), "conformance")
	_, err = world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeHookReceived, CorrelationID: "hook_other", Data: other},
		Options: worlds.EventsCreateOptions{ResumeID: "resume_a", ResumePayloadDigest: worlds.ResumeDigest([]byte(`"one"`))}})
	conformExpectRefusedResume(t, err, "a resume repeated for another hook")
	// A resume's digest must be its payload's.
	mismatched, _ := worlds.EncodeHookReceived([]byte(`"three"`), "conformance")
	_, err = world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeHookReceived, CorrelationID: "hook_resume", Data: mismatched},
		Options: worlds.EventsCreateOptions{ResumeID: "resume_c", ResumePayloadDigest: worlds.ResumeDigest([]byte(`"four"`))}})
	conformExpect(t, err, worlds.ErrEventInvalid, "a resume whose digest is another payload's")
	if _, err := resume("resume_b", `"one"`); err != nil {
		t.Fatalf("another resume: %v", err)
	}
	received := 0
	for _, event := range conformAllEvents(t, world, runID) {
		if event.EventType != worlds.EventTypeHookReceived {
			continue
		}
		received++
		if event.EventID == first.EventID && event.ResumeID != "resume_a" {
			t.Fatalf("event %s resume = %q, want resume_a", event.EventID, event.ResumeID)
		}
	}
	if received != 2 {
		t.Fatalf("hook_received events = %d, want 2", received)
	}
}

// conformStartHookDecisions checks that a World's decision on a start hook
// candidate is permanent: a repeat of an accepted candidate returns its run,
// which echoes its start hook, and a rejected one is refused again, naming
// the same run, even once the token is free. A candidate too old to create is
// refused, and the start hook's ID is the start hook's alone.
func conformStartHookDecisions(t *testing.T, world worlds.World) {
	if !world.Capabilities().StartHook {
		return
	}
	ctx := context.Background()
	token := fmt.Sprintf("conformance-decision-%d", time.Now().UnixNano())
	owner := worlds.NewRunID()
	created, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: owner, WorkflowName: "conformance", StartHook: &worlds.StartHook{Token: token, Metadata: []byte(`"metadata"`)}})
	if err != nil {
		t.Fatal(err)
	}
	again, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: owner, WorkflowName: "conformance", StartHook: &worlds.StartHook{Token: token}})
	if err != nil || again.EventID != created.EventID {
		t.Fatalf("repeated accepted start = %+v err = %v, want its run_created", again, err)
	}
	run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: owner})
	if err != nil || run.StartHook == nil || run.StartHook.Token != token || run.StartHook.HookID != worlds.StartHookID(owner) || run.StartHook.Metadata != nil {
		t.Fatalf("run start hook = %+v err = %v, want the echo without metadata", run.StartHook, err)
	}
	// The start hook's ID is not another hook's.
	_, err = world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: owner, Event: conformEvent(t, worlds.EventTypeHookCreated, string(worlds.StartHookID(owner)), conformHookData("other:"+token, false, 0))})
	conformExpect(t, err, worlds.ErrEventInvalid, "a hook_created under the start hook's ID")

	refused := worlds.NewRunID()
	var conflict *worlds.HookConflictError
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: refused, WorkflowName: "conformance", StartHook: &worlds.StartHook{Token: token}}); !errors.As(err, &conflict) || conflict.RunID != owner {
		t.Fatalf("conflicting start = %v", err)
	}
	// The owner frees the token, and the refused candidate stays refused.
	conformAppend(t, world, owner, worlds.EventTypeHookDisposed, string(worlds.StartHookID(owner)), nil)
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: refused, WorkflowName: "conformance", StartHook: &worlds.StartHook{Token: token}}); !errors.As(err, &conflict) || conflict.RunID != owner {
		t.Fatalf("repeated refused start = %v, want the same conflict", err)
	}
	data, _ := worlds.RunCreatedData(worlds.EventsCreateRunInput{RunID: refused, WorkflowName: "conformance", StartHook: &worlds.StartHook{Token: token}})
	if _, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: refused, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeRunStarted, Data: data}}); !errors.As(err, &conflict) || conflict.RunID != owner {
		t.Fatalf("refused candidate's run_started = %v, want the same conflict", err)
	}
	if _, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: refused}); !errors.Is(err, worlds.ErrRunNotFound) {
		t.Fatalf("refused candidate has a run: %v", err)
	}
	// A refused candidate's ID with another token, and an ordinary run's ID
	// with a start hook, are refused for good.
	_, err = world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: refused, WorkflowName: "conformance", StartHook: &worlds.StartHook{Token: token + "-other"}})
	conformExpect(t, err, worlds.ErrStartHookRejected, "a refused candidate with another token")
	ordinary := conformCreateRun(t, world, "start_hook_ordinary")
	_, err = world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: ordinary, WorkflowName: "conformance", StartHook: &worlds.StartHook{Token: token + "-ordinary"}})
	conformExpect(t, err, worlds.ErrStartHookRejected, "an ordinary run's ID with a start hook")
	_, err = world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: owner, WorkflowName: "conformance", StartHook: &worlds.StartHook{Token: token + "-other"}})
	conformExpect(t, err, worlds.ErrStartHookRejected, "an accepted candidate with another token")
	// A candidate older than any start can be is refused.
	old := worlds.RunID("wrun_" + ulid.MustNew(ulid.Timestamp(time.Now().Add(-worlds.StartHookCandidateMaxAge-time.Hour)), rand.Reader).String())
	_, err = world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: old, WorkflowName: "conformance", StartHook: &worlds.StartHook{Token: token + "-old"}})
	conformExpect(t, err, worlds.ErrStartHookRejected, "a start hook candidate past its age")
}

// conformExpectRefusedResume checks the refusal of a resume ID repeated with
// another hook or payload: ErrEntityConflict, as the local World refuses
// it, or ErrEventInvalid, as the Postgres World does.
func conformExpectRefusedResume(t *testing.T, err error, label string) {
	t.Helper()
	if !errors.Is(err, worlds.ErrEntityConflict) && !errors.Is(err, worlds.ErrEventInvalid) {
		t.Fatalf("%s: error = %v, want %v or %v", label, err, worlds.ErrEntityConflict, worlds.ErrEventInvalid)
	}
}
