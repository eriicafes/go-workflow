package workflowtest

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

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
