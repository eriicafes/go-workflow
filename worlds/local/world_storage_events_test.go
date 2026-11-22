package localworld

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// TestEventsListCursor checks an events page's cursor is "key:" and its last
// event's ID, and that a bare event ID and a "<time>|<event ID>" cursor name
// the same position. A cursor that names no event starts from the first.
func TestEventsListCursor(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	world := storageWorld(t, Config{})
	runID := startedRun(t, world)
	appendEvent(t, world, runID, worlds.EventTypeWaitCreated, "wait_a", nil)
	first, err := world.EventsList(ctx, worlds.EventsListInput{RunID: runID, Pagination: worlds.Pagination{Limit: 1}})
	if err != nil || len(first.Data) != 1 || !first.HasMore {
		t.Fatalf("first page = %#v err=%v", first, err)
	}
	id := string(worlds.EventIDForSlot(1))
	if first.Cursor != "key:"+id {
		t.Fatalf("cursor = %q, want key:%s", first.Cursor, id)
	}
	for _, cursor := range []string{first.Cursor, id, "2026-01-02T03:04:05.006Z|" + id} {
		page, err := world.EventsList(ctx, worlds.EventsListInput{RunID: runID, Pagination: worlds.Pagination{Limit: 1, Cursor: cursor}})
		if err != nil || len(page.Data) != 1 || page.Data[0].EventID != worlds.EventIDForSlot(2) {
			t.Fatalf("page after %q = %#v err=%v", cursor, page, err)
		}
		correlated, err := world.EventsListByCorrelationID(ctx, worlds.EventsListByCorrelationIDInput{RunID: runID, CorrelationID: "wait_a", Pagination: worlds.Pagination{Cursor: cursor}})
		if err != nil || len(correlated.Data) != 1 || correlated.Cursor != "key:"+string(worlds.EventIDForSlot(3)) {
			t.Fatalf("correlated page after %q = %#v err=%v", cursor, correlated, err)
		}
	}
	restarted, err := world.EventsList(ctx, worlds.EventsListInput{RunID: runID, Pagination: worlds.Pagination{Limit: 1, Cursor: "not-a-cursor"}})
	if err != nil || len(restarted.Data) != 1 || restarted.Data[0].EventID != worlds.EventIDForSlot(1) {
		t.Fatalf("page after an invalid cursor = %#v err=%v", restarted, err)
	}
	// A write's SinceCursor takes a page's cursor too.
	since, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeWaitCreated, CorrelationID: "wait_b"}, Options: worlds.EventsCreateOptions{SinceCursor: "key:" + string(worlds.EventIDForSlot(2))}})
	if err != nil || len(since.Events) != 1 || since.Events[0].CorrelationID != "wait_a" {
		t.Fatalf("SinceCursor delta = %#v err=%v", since.Events, err)
	}
}

// TestUnsafeIDsAreRefused checks an ID holding ".", "/", "\", or a NUL byte
// is refused wherever a caller supplies one.
func TestUnsafeIDsAreRefused(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	world := storageWorld(t, Config{})
	runID := startedRun(t, world)
	calls := map[string]func() error{
		"create run": func() error {
			_, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: "wrun_a.b", WorkflowName: "storage"})
			return err
		},
		"append to run": func() error {
			_, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: "wrun/a", Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeRunStarted}})
			return err
		},
		"append correlation": func() error {
			_, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeWaitCreated, CorrelationID: `wait\a`}})
			return err
		},
		"create hook": func() error {
			_, err := claimToken(world, runID, "hook.a", "token", worlds.DataHookCreated{}, worlds.EventsCreateOptions{})
			return err
		},
		"batch correlation": func() error {
			_, err := world.EventsCreateBatch(ctx, worlds.EventsCreateBatchInput{RunID: runID, Events: []worlds.EventsCreateEvent{{EventType: worlds.EventTypeWaitCreated, CorrelationID: "wait\x00a"}}})
			return err
		},
		"get run": func() error {
			_, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: "../wrun"})
			return err
		},
		"get step": func() error {
			_, err := world.StepsGet(ctx, worlds.StepsGetInput{RunID: runID, StepID: "step/a"})
			return err
		},
		"list steps": func() error {
			_, err := world.StepsList(ctx, worlds.StepsListInput{RunID: "wrun.a"})
			return err
		},
		"get hook": func() error {
			_, err := world.HooksGet(ctx, worlds.HooksGetInput{HookID: "hook/a"})
			return err
		},
		"get event": func() error {
			_, err := world.EventsGet(ctx, worlds.EventsGetInput{RunID: runID, EventID: "evnt.1"})
			return err
		},
		"list events": func() error {
			_, err := world.EventsList(ctx, worlds.EventsListInput{RunID: "wrun.a"})
			return err
		},
		"list correlated events": func() error {
			_, err := world.EventsListByCorrelationID(ctx, worlds.EventsListByCorrelationIDInput{RunID: runID, CorrelationID: "a.b"})
			return err
		},
	}
	for name, call := range calls {
		if err := call(); !errors.Is(err, worlds.ErrEventInvalid) {
			t.Errorf("%s = %v, want ErrEventInvalid", name, err)
		}
	}
	if page, err := world.EventsList(ctx, worlds.EventsListInput{RunID: runID}); err != nil || len(page.Data) != 2 {
		t.Fatalf("refused writes left %d events err=%v", len(page.Data), err)
	}
}

// TestTimesAreMilliseconds checks the World records times to the
// millisecond, its own clock's and an in-band write's OccurredAt.
func TestTimesAreMilliseconds(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	world := storageWorld(t, Config{})
	runID := startedRun(t, world)
	whole := func(name string, at time.Time) {
		t.Helper()
		if at.IsZero() || !at.Equal(at.Truncate(time.Millisecond)) {
			t.Fatalf("%s = %s, want a whole millisecond", name, at.Format(time.RFC3339Nano))
		}
	}
	run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID})
	if err != nil {
		t.Fatal(err)
	}
	whole("run CreatedAt", run.CreatedAt)
	whole("run StartedAt", *run.StartedAt)
	occurred := time.Now().Add(-time.Minute).UTC().Truncate(time.Millisecond)
	one := 1
	written, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeStepCreated, CorrelationID: "step_a", Data: []byte(`{"stepName":"a"}`), OccurredAt: occurred.Add(400 * time.Microsecond)}, Options: worlds.EventsCreateOptions{InBandCount: &one}})
	if err != nil || !written.CreatedAt.Equal(occurred) {
		t.Fatalf("in-band CreatedAt = %v err=%v, want %v", written.CreatedAt, err, occurred)
	}
	step, err := world.StepsGet(ctx, worlds.StepsGetInput{RunID: runID, StepID: "step_a"})
	if err != nil || !step.CreatedAt.Equal(occurred) {
		t.Fatalf("step CreatedAt = %v err=%v, want %v", step.CreatedAt, err, occurred)
	}
	steps, err := world.StepsList(ctx, worlds.StepsListInput{RunID: runID})
	if err != nil || steps.Cursor != occurred.Format("2006-01-02T15:04:05.000Z")+"|step_a" {
		t.Fatalf("steps cursor = %q err=%v", steps.Cursor, err)
	}
}

// TestForceClaimedDeliveryNamesTheTokensOwner checks a delivery to a hook
// whose token was taken by force is redirected to the hook that holds the
// token now, with the token, however many times it was taken since.
func TestForceClaimedDeliveryNamesTheTokensOwner(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	world := storageWorld(t, Config{})
	first, second, third := startedRun(t, world), startedRun(t, world), startedRun(t, world)
	if _, err := claimToken(world, first, "hook_first", "shared-token", worlds.DataHookCreated{}, worlds.EventsCreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := claimToken(world, second, "hook_second", "shared-token", worlds.DataHookCreated{Force: true}, worlds.EventsCreateOptions{}); err != nil {
		t.Fatal(err)
	}
	deliver := func(runID worlds.RunID, hookID string) error {
		data, _ := worlds.EncodeHookReceived([]byte("payload"), "storage")
		_, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeHookReceived, CorrelationID: hookID, Data: data}})
		return err
	}
	redirect, ok := errors.AsType[*worlds.HookForceClaimedError](deliver(first, "hook_first"))
	if !ok || redirect.Token != "shared-token" || redirect.RunID != second || redirect.HookID != "hook_second" {
		t.Fatalf("delivery to the first hook = %#v", redirect)
	}
	if _, err := claimToken(world, third, "hook_third", "shared-token", worlds.DataHookCreated{Force: true}, worlds.EventsCreateOptions{}); err != nil {
		t.Fatal(err)
	}
	for runID, hookID := range map[worlds.RunID]string{first: "hook_first", second: "hook_second"} {
		redirect, ok := errors.AsType[*worlds.HookForceClaimedError](deliver(runID, hookID))
		if !ok || redirect.Token != "shared-token" || redirect.RunID != third || redirect.HookID != "hook_third" {
			t.Fatalf("delivery to %s = %#v, want a redirect to the third hook", hookID, redirect)
		}
	}
	// Once the token is free, the redirect names the hook that took it.
	appendEvent(t, world, third, worlds.EventTypeHookDisposed, "hook_third", nil)
	redirect, ok = errors.AsType[*worlds.HookForceClaimedError](deliver(first, "hook_first"))
	if !ok || redirect.Token != "shared-token" || redirect.RunID != second || redirect.HookID != "hook_second" {
		t.Fatalf("delivery after the token was released = %#v", redirect)
	}
}

// TestTokenKeepsItsRetentionFromTheClaim checks a token's row holds its
// hook's retention from the claim on, and that the token is held while its
// run is unfinished whatever the retention, then until the retention ends.
func TestTokenKeepsItsRetentionFromTheClaim(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	world := storageWorld(t, Config{})
	retained, lapsed, other := startedRun(t, world), startedRun(t, world), startedRun(t, world)
	until := time.Now().Add(time.Hour).UTC().Truncate(time.Millisecond)
	if _, err := claimToken(world, retained, "hook_retained", "retained-token", worlds.DataHookCreated{TokenRetentionUntil: &until}, worlds.EventsCreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if stored := scalar[int64](t, world, "SELECT retention_until FROM hook_tokens WHERE token = 'retained-token'"); stored != until.UnixMilli() {
		t.Fatalf("retention_until = %d, want %d", stored, until.UnixMilli())
	}
	// A retention that passes while the run is unfinished releases nothing.
	soon := time.Now().Add(20 * time.Millisecond).UTC()
	if _, err := claimToken(world, lapsed, "hook_lapsed", "lapsed-token", worlds.DataHookCreated{TokenRetentionUntil: &soon}, worlds.EventsCreateOptions{}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(40 * time.Millisecond)
	conflict, err := claimToken(world, other, "hook_other_a", "lapsed-token", worlds.DataHookCreated{}, worlds.EventsCreateOptions{})
	if err != nil || conflict.EventType != worlds.EventTypeHookConflict {
		t.Fatalf("claim of an unfinished run's token = %#v err=%v", conflict, err)
	}
	if hook, err := world.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: "lapsed-token"}); err != nil || hook.HookID != "hook_lapsed" {
		t.Fatalf("unfinished run's hook by token = %#v err=%v", hook, err)
	}
	// A start hook's token row holds its retention too.
	started := worlds.NewRunID()
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: started, WorkflowName: "storage", StartHook: &worlds.StartHook{Token: "start-token", TokenRetentionUntil: &until}}); err != nil {
		t.Fatal(err)
	}
	if stored := scalar[int64](t, world, "SELECT retention_until FROM hook_tokens WHERE token = 'start-token'"); stored != until.UnixMilli() {
		t.Fatalf("start hook retention_until = %d, want %d", stored, until.UnixMilli())
	}
	// Once the runs finish, a lapsed token is free and a retained one held.
	appendEvent(t, world, retained, worlds.EventTypeRunCancelled, "", nil)
	appendEvent(t, world, lapsed, worlds.EventTypeRunCancelled, "", nil)
	if claimed, err := claimToken(world, other, "hook_other_b", "lapsed-token", worlds.DataHookCreated{}, worlds.EventsCreateOptions{}); err != nil || claimed.EventType != worlds.EventTypeHookCreated {
		t.Fatalf("claim of a finished run's lapsed token = %#v err=%v", claimed, err)
	}
	if conflict, err := claimToken(world, other, "hook_other_c", "retained-token", worlds.DataHookCreated{}, worlds.EventsCreateOptions{}); err != nil || conflict.EventType != worlds.EventTypeHookConflict {
		t.Fatalf("claim of a retained token = %#v err=%v", conflict, err)
	}
	if hook, err := world.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: "retained-token"}); err != nil || hook.HookID != "hook_retained" {
		t.Fatalf("retained hook by token = %#v err=%v", hook, err)
	}
}
