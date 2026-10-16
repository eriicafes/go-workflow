package workflowtest

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

func conformHooksClaim(t *testing.T, world worlds.World) {
	ctx := context.Background()
	runID := conformCreateRun(t, world, "hooks")
	other := conformCreateRun(t, world, "hooks_other")
	token := fmt.Sprintf("token-%d", time.Now().UnixNano())

	_, err := world.HooksClaim(ctx, worlds.HooksClaimInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeWaitCreated}})
	conformExpect(t, err, worlds.ErrEventInvalid, "claim with wrong event type")
	_, err = world.HooksClaim(ctx, worlds.HooksClaimInput{RunID: runID, Event: conformEvent(t, worlds.EventTypeHookCreated, "hook_x", map[string]any{"token": ""})})
	conformExpect(t, err, worlds.ErrEventInvalid, "claim without token")
	_, err = conformClaim(world, "wrun_missing", "hook_x", token, false)
	conformExpect(t, err, worlds.ErrRunNotFound, "claim for missing run")

	zero := 0
	claimed, err := world.HooksClaim(ctx, worlds.HooksClaimInput{RunID: runID, Event: conformEvent(t, worlds.EventTypeHookCreated, "hook_a", conformHookData(token, nil, 0)), Options: worlds.EventsCreateOptions{EventCount: &zero}})
	if err != nil {
		t.Fatal(err)
	}
	if claimed.EventID != worlds.EventIDForSlot(2) || claimed.EventType != worlds.EventTypeHookCreated || claimed.AllocatedEventCount != 1 || claimed.ForcedRunID != "" || len(claimed.Events) != 1 {
		t.Fatalf("claim = %#v", claimed)
	}
	hook, err := world.HooksGet(ctx, worlds.HooksGetInput{HookID: "hook_a"})
	if err != nil || hook.RunID != runID || hook.Token != token || string(hook.Metadata) != "meta" || hook.CreatedAt.IsZero() {
		t.Fatalf("hook = %#v err=%v", hook, err)
	}
	stripped, err := world.HooksGet(ctx, worlds.HooksGetInput{HookID: "hook_a", ResolveData: worlds.ResolveDataNone})
	if err != nil || stripped.Metadata != nil {
		t.Fatalf("ResolveDataNone hook = %#v err=%v", stripped, err)
	}
	byToken, err := world.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: token})
	if err != nil || byToken.HookID != "hook_a" {
		t.Fatalf("hook by token = %#v err=%v", byToken, err)
	}
	_, err = world.HooksGet(ctx, worlds.HooksGetInput{HookID: "hook_missing"})
	conformExpect(t, err, worlds.ErrHookNotFound, "missing hook")
	_, err = world.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: "token-missing"})
	conformExpect(t, err, worlds.ErrHookNotFound, "missing token")

	_, err = conformClaim(world, other, "hook_b", token, false)
	conformExpect(t, err, worlds.ErrHookConflict, "claim held token")
	_, err = world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: other, Event: conformEvent(t, worlds.EventTypeHookCreated, "hook_b", conformHookData(token, nil, 0))})
	conformExpect(t, err, worlds.ErrHookConflict, "hook_created append with held token")

	second := token + "-second"
	if _, err := conformClaim(world, runID, "hook_c", second, false); err != nil {
		t.Fatal(err)
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
	if forced.ForcedRunID != owner || forced.EventType != worlds.EventTypeHookCreated || forced.AllocatedEventCount != 1 {
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

	conformAppend(t, world, claimant, worlds.EventTypeRunFailed, "", nil)
	late := conformCreateRun(t, world, "force_late")
	takeover, err := conformClaim(world, late, "hook_late", token, true)
	if err != nil || takeover.ForcedRunID != "" {
		t.Fatalf("force over terminal owner = %#v err=%v", takeover, err)
	}
	for _, event := range conformAllEvents(t, world, claimant) {
		if event.EventType == worlds.EventTypeHookDisposed {
			t.Fatal("terminal owner received hook_disposed")
		}
	}
}

func conformHooksRetention(t *testing.T, world worlds.World) {
	ctx := context.Background()
	runID := conformCreateRun(t, world, "retention")
	plain := fmt.Sprintf("plain-%d", time.Now().UnixNano())
	if _, err := conformClaim(world, runID, "hook_plain", plain, false); err != nil {
		t.Fatal(err)
	}
	conformAppend(t, world, runID, worlds.EventTypeHookReceived, "hook_plain", []byte(`{"value":1}`))
	hook, err := world.HooksGet(ctx, worlds.HooksGetInput{HookID: "hook_plain"})
	if err != nil || hook.ReceivedAt == nil {
		t.Fatalf("received hook = %#v err=%v", hook, err)
	}
	_, err = world.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: plain})
	conformExpect(t, err, worlds.ErrHookNotFound, "received hook without retention")

	retained := fmt.Sprintf("retained-%d", time.Now().UnixNano())
	if _, err := world.HooksClaim(ctx, worlds.HooksClaimInput{RunID: runID, Event: conformEvent(t, worlds.EventTypeHookCreated, "hook_retained", conformHookData(retained, nil, 300*time.Millisecond))}); err != nil {
		t.Fatal(err)
	}
	conformAppend(t, world, runID, worlds.EventTypeHookReceived, "hook_retained", nil)
	hook, err = world.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: retained})
	if err != nil || hook.RetainUntil == nil || hook.MinRetention != 300*time.Millisecond {
		t.Fatalf("retained hook = %#v err=%v", hook, err)
	}
	other := conformCreateRun(t, world, "retention_other")
	_, err = conformClaim(world, other, "hook_other", retained, false)
	conformExpect(t, err, worlds.ErrHookConflict, "claim retained token")
	time.Sleep(400 * time.Millisecond)
	_, err = world.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: retained})
	conformExpect(t, err, worlds.ErrHookNotFound, "expired retained token")
	if _, err := conformClaim(world, other, "hook_other", retained, false); err != nil {
		t.Fatalf("claim expired token: %v", err)
	}
}

func conformWebhooks(t *testing.T, world worlds.World) {
	ctx := context.Background()
	runID := conformCreateRun(t, world, "webhooks")
	token := fmt.Sprintf("manual-%d", time.Now().UnixNano())
	if _, err := world.HooksClaim(ctx, worlds.HooksClaimInput{RunID: runID, Event: conformEvent(t, worlds.EventTypeHookCreated, "hook_manual", conformHookData(token, &worlds.Webhook{Method: "POST", Manual: true}, 0))}); err != nil {
		t.Fatal(err)
	}
	hook, err := world.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: token})
	if err != nil || hook.Webhook == nil || !hook.Webhook.Manual || hook.Webhook.Method != "POST" {
		t.Fatalf("webhook = %#v err=%v", hook, err)
	}
	request := worlds.WebhookReceivedEventData{Request: &worlds.WebhookRequest{Method: "POST", URL: "/hook", Header: map[string][]string{"X": {"1"}}, Body: []byte("body")}}
	_, err = world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: conformEvent(t, worlds.EventTypeHookReceived, "hook_manual", worlds.WebhookReceivedEventData{Response: &worlds.WebhookResponse{Status: 200}, ResponseTo: worlds.EventIDForSlot(2)})})
	conformExpect(t, err, worlds.ErrHookNotActive, "response before request")
	received := conformAppend(t, world, runID, worlds.EventTypeHookReceived, "hook_manual", request)
	hook, err = world.HooksGet(ctx, worlds.HooksGetInput{HookID: "hook_manual"})
	if err != nil || hook.ReceivedEventID != received.EventID || hook.ReceivedAt == nil {
		t.Fatalf("manual hook after request = %#v err=%v", hook, err)
	}
	if _, err := world.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: token}); err != nil {
		t.Fatalf("manual hook released before response: %v", err)
	}
	_, err = world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: conformEvent(t, worlds.EventTypeHookReceived, "hook_manual", request)})
	conformExpect(t, err, worlds.ErrHookNotActive, "second request")
	_, err = world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: conformEvent(t, worlds.EventTypeHookReceived, "hook_manual", worlds.WebhookReceivedEventData{Response: &worlds.WebhookResponse{Status: 200}, ResponseTo: worlds.EventIDForSlot(1)})})
	conformExpect(t, err, worlds.ErrHookNotActive, "response to wrong request")
	conformAppend(t, world, runID, worlds.EventTypeHookReceived, "hook_manual", worlds.WebhookReceivedEventData{Response: &worlds.WebhookResponse{Status: 201, Body: []byte("ok")}, ResponseTo: received.EventID})
	_, err = world.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: token})
	conformExpect(t, err, worlds.ErrHookNotFound, "manual hook after response")
	second := worlds.WebhookReceivedEventData{Response: &worlds.WebhookResponse{Status: 202}, ResponseTo: received.EventID}
	_, err = world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: conformEvent(t, worlds.EventTypeHookReceived, "hook_manual", second)})
	conformExpect(t, err, worlds.ErrHookNotActive, "second response")

	// A retained webhook keeps its token after responding, and still takes
	// only one response.
	retained := token + "-retained"
	if _, err := world.HooksClaim(ctx, worlds.HooksClaimInput{RunID: runID, Event: conformEvent(t, worlds.EventTypeHookCreated, "hook_retained", conformHookData(retained, &worlds.Webhook{Method: "POST", Manual: true}, time.Hour))}); err != nil {
		t.Fatal(err)
	}
	retainedRequest := conformAppend(t, world, runID, worlds.EventTypeHookReceived, "hook_retained", request)
	conformAppend(t, world, runID, worlds.EventTypeHookReceived, "hook_retained", worlds.WebhookReceivedEventData{Response: &worlds.WebhookResponse{Status: 200}, ResponseTo: retainedRequest.EventID})
	_, err = world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: conformEvent(t, worlds.EventTypeHookReceived, "hook_retained", worlds.WebhookReceivedEventData{Response: &worlds.WebhookResponse{Status: 202}, ResponseTo: retainedRequest.EventID})})
	conformExpect(t, err, worlds.ErrHookNotActive, "second response to a retained webhook")

	automatic := token + "-auto"
	if _, err := world.HooksClaim(ctx, worlds.HooksClaimInput{RunID: runID, Event: conformEvent(t, worlds.EventTypeHookCreated, "hook_auto", conformHookData(automatic, &worlds.Webhook{Response: &worlds.WebhookResponse{Status: 202}}, 0))}); err != nil {
		t.Fatal(err)
	}
	hook, err = world.HooksGet(ctx, worlds.HooksGetInput{HookID: "hook_auto"})
	if err != nil || hook.Webhook == nil || hook.Webhook.Response == nil || hook.Webhook.Response.Status != 202 {
		t.Fatalf("automatic webhook = %#v err=%v", hook, err)
	}
	conformAppend(t, world, runID, worlds.EventTypeHookReceived, "hook_auto", request)
	_, err = world.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: automatic})
	conformExpect(t, err, worlds.ErrHookNotFound, "automatic hook after request")
}
