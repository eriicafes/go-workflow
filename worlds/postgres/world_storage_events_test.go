package postgresworld

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// admission returns the decision recorded for a start hook candidate.
func admission(t *testing.T, world *World, runID worlds.RunID) (decision, conflicting string) {
	t.Helper()
	err := world.pool.QueryRow(context.Background(), `SELECT decision, coalesce(conflicting_run_id, '') FROM workflow_start_hook_admissions WHERE run_id = $1`, runID).Scan(&decision, &conflicting)
	if err != nil {
		t.Fatalf("decision of %s: %v", runID, err)
	}
	return decision, conflicting
}

func count(t *testing.T, world *World, query string, args ...any) int {
	t.Helper()
	var n int
	if err := world.pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// TestStartHookRefusalCommitsWithItsDecision checks a refused candidate's
// decision is recorded by the write that refuses it, which creates no run
// and no hook, and that an admission deletes the decisions past every
// candidate's window.
func TestStartHookRefusalCommitsWithItsDecision(t *testing.T) {
	t.Parallel()
	world := newTestWorld(t)
	ctx := context.Background()
	owner, refused := worlds.NewRunID(), worlds.NewRunID()
	start := &worlds.StartHook{Token: "start-token"}
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: owner, WorkflowName: "w", StartHook: start}); err != nil {
		t.Fatal(err)
	}
	if decision, _ := admission(t, world, owner); decision != "accepted" {
		t.Fatalf("owner's decision = %q", decision)
	}
	// Decisions older and younger than the window plus an hour.
	for id, age := range map[string]time.Duration{"wrun_lapsed": 26 * time.Hour, "wrun_kept": 24*time.Hour + 30*time.Minute} {
		if _, err := world.pool.Exec(ctx, `INSERT INTO workflow_start_hook_admissions (run_id, token, decision, created_at) VALUES ($1, 'old', 'accepted', now() - make_interval(secs => $2))`, id, age.Seconds()); err != nil {
			t.Fatal(err)
		}
	}
	_, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: refused, WorkflowName: "w", StartHook: start})
	conflict, ok := errors.AsType[*worlds.HookConflictError](err)
	if !ok || conflict.RunID != owner || conflict.Token != start.Token {
		t.Fatalf("conflicting start = %v, want a conflict naming %s", err, owner)
	}
	if decision, conflicting := admission(t, world, refused); decision != "rejected" || conflicting != string(owner) {
		t.Fatalf("refused candidate's decision = %q by %q", decision, conflicting)
	}
	if runs, hooks := count(t, world, `SELECT count(*) FROM workflow_runs WHERE id = $1`, refused), count(t, world, `SELECT count(*) FROM workflow_hooks WHERE run_id = $1`, refused); runs != 0 || hooks != 0 {
		t.Fatalf("refused candidate left %d runs and %d hooks", runs, hooks)
	}
	if lapsed, kept := count(t, world, `SELECT count(*) FROM workflow_start_hook_admissions WHERE run_id = 'wrun_lapsed'`), count(t, world, `SELECT count(*) FROM workflow_start_hook_admissions WHERE run_id = 'wrun_kept'`); lapsed != 0 || kept != 1 {
		t.Fatalf("decisions after an admission: lapsed %d, within the window %d", lapsed, kept)
	}
}

// TestConcurrentStartHookAdmissions races candidates for one token, and
// copies of each candidate, across two instances: one candidate is accepted,
// every copy of it gets its run_created, and every other candidate gets one
// refusal naming it, recorded for good.
func TestConcurrentStartHookAdmissions(t *testing.T) {
	t.Parallel()
	url, schema := testSchema(t)
	a := testWorld(t, testPool(t, url, schema), Config{})
	b := testWorld(t, testPool(t, url, schema), Config{})
	ctx := context.Background()
	const candidates, copies = 4, 3
	runs := make([]worlds.RunID, candidates)
	for i := range runs {
		runs[i] = worlds.NewRunID()
	}
	type answer struct {
		runID  worlds.RunID
		result worlds.EventResult
		err    error
	}
	answers := make(chan answer, candidates*copies)
	var group sync.WaitGroup
	for i, runID := range runs {
		for c := range copies {
			world := a
			if (i+c)%2 == 1 {
				world = b
			}
			group.Add(1)
			go func() {
				defer group.Done()
				result, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "w", StartHook: &worlds.StartHook{Token: "raced"}})
				answers <- answer{runID, result, err}
			}()
		}
	}
	group.Wait()
	close(answers)
	var winner worlds.RunID
	if err := a.pool.QueryRow(ctx, `SELECT run_id FROM workflow_hooks WHERE token = 'raced' AND claims_token`).Scan(&winner); err != nil {
		t.Fatal(err)
	}
	created := 0
	for got := range answers {
		if got.runID == winner {
			if got.err != nil || got.result.EventID != worlds.EventIDForSlot(1) {
				t.Fatalf("copy of the accepted candidate = %#v err=%v", got.result, got.err)
			}
			created += got.result.AllocatedEventCount
			continue
		}
		if conflict, ok := errors.AsType[*worlds.HookConflictError](got.err); !ok || conflict.RunID != winner {
			t.Fatalf("copy of refused candidate %s = %v, want a conflict naming %s", got.runID, got.err, winner)
		}
	}
	if created != 1 {
		t.Fatalf("copies of the accepted candidate created %d runs", created)
	}
	for _, runID := range runs {
		decision, conflicting := admission(t, a, runID)
		if want := runID == winner; (decision == "accepted") != want || !want && conflicting != string(winner) {
			t.Fatalf("decision of %s = %q by %q, winner %s", runID, decision, conflicting, winner)
		}
	}
	if n := count(t, a, `SELECT count(*) FROM workflow_runs`); n != 1 {
		t.Fatalf("runs = %d", n)
	}
}

// TestResumeOfAFinishedRun checks a resume carrying a resume ID finds its
// run finished before it looks for the hook, which went with the run, while
// a hook_received without one looks for its hook first.
func TestResumeOfAFinishedRun(t *testing.T) {
	t.Parallel()
	world := newTestWorld(t)
	ctx := context.Background()
	runID := worlds.NewRunID()
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "w"}); err != nil {
		t.Fatal(err)
	}
	for _, event := range []worlds.EventsCreateEvent{
		{EventType: worlds.EventTypeHookCreated, CorrelationID: "hook_a", Data: []byte(`{"token":"finished"}`)},
		{EventType: worlds.EventTypeRunCompleted},
	} {
		if _, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: event}); err != nil {
			t.Fatal(err)
		}
	}
	data, _ := worlds.EncodeHookReceived([]byte(`"late"`), "w")
	received := worlds.EventsCreateEvent{EventType: worlds.EventTypeHookReceived, CorrelationID: "hook_a", Data: data}
	_, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: received, Options: worlds.EventsCreateOptions{ResumeID: "resume_late"}})
	if _, ok := errors.AsType[*worlds.RunExpiredError](err); !ok {
		t.Fatalf("resume of a finished run = %v, want a *RunExpiredError", err)
	}
	_, err = world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: received})
	if _, ok := errors.AsType[*worlds.HookNotFoundError](err); !ok {
		t.Fatalf("hook_received without a resume ID on a finished run = %v, want a *HookNotFoundError", err)
	}
}

// TestResumeStoresItsPayloadDigest checks a resume sent without a digest is
// stored with its payload's, so a repeat with another payload is refused.
func TestResumeStoresItsPayloadDigest(t *testing.T) {
	t.Parallel()
	world := newTestWorld(t)
	ctx := context.Background()
	runID := worlds.NewRunID()
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "w"}); err != nil {
		t.Fatal(err)
	}
	if _, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeHookCreated, CorrelationID: "hook_a", Data: []byte(`{"token":"digest"}`)}}); err != nil {
		t.Fatal(err)
	}
	resume := func(payload string) (worlds.EventResult, error) {
		data, _ := worlds.EncodeHookReceived([]byte(payload), "w")
		return world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeHookReceived, CorrelationID: "hook_a", Data: data}, Options: worlds.EventsCreateOptions{ResumeID: "resume_a"}})
	}
	first, err := resume(`"one"`)
	if err != nil {
		t.Fatal(err)
	}
	var digest string
	if err := world.pool.QueryRow(ctx, `SELECT coalesce(resume_payload_digest, '') FROM workflow_events WHERE run_id = $1 AND id = $2`, runID, first.EventID).Scan(&digest); err != nil || digest != worlds.ResumeDigest([]byte(`"one"`)) {
		t.Fatalf("stored digest = %q err=%v", digest, err)
	}
	if repeat, err := resume(`"one"`); err != nil || repeat.EventID != first.EventID || repeat.AllocatedEventCount != 0 {
		t.Fatalf("repeated resume = %#v err=%v", repeat, err)
	}
	if _, err := resume(`"two"`); !errors.Is(err, worlds.ErrEventInvalid) {
		t.Fatalf("resume repeated with another payload = %v, want ErrEventInvalid", err)
	}
}

// TestForceClaimedRefusalNamesTheTokensHolder checks a delivery to a hook
// whose token was taken over, and taken over again, is refused naming the
// token and the hook that holds it now.
func TestForceClaimedRefusalNamesTheTokensHolder(t *testing.T) {
	t.Parallel()
	world := newTestWorld(t)
	ctx := context.Background()
	runs := []worlds.RunID{worlds.NewRunID(), worlds.NewRunID(), worlds.NewRunID()}
	for i, runID := range runs {
		if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "w"}); err != nil {
			t.Fatal(err)
		}
		event := worlds.EventsCreateEvent{EventType: worlds.EventTypeHookCreated, CorrelationID: fmt.Sprintf("hook_%d", i), Data: []byte(`{"token":"chained","force":true}`)}
		if _, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: event}); err != nil {
			t.Fatal(err)
		}
	}
	data, _ := worlds.EncodeHookReceived([]byte(`"late"`), "w")
	_, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runs[0], Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeHookReceived, CorrelationID: "hook_0", Data: data}})
	claimed, ok := errors.AsType[*worlds.HookForceClaimedError](err)
	if !ok || claimed.Token != "chained" || claimed.RunID != runs[2] || claimed.HookID != "hook_2" {
		t.Fatalf("delivery to a hook taken over twice = %#v (%v), want token chained held by hook_2 of %s", claimed, err, runs[2])
	}
	// Once the holder disposes its hook, the refusal names the claimant the
	// disposal recorded.
	if _, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runs[2], Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeHookDisposed, CorrelationID: "hook_2"}}); err != nil {
		t.Fatal(err)
	}
	_, err = world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runs[0], Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeHookReceived, CorrelationID: "hook_0", Data: data}})
	if claimed, ok := errors.AsType[*worlds.HookForceClaimedError](err); !ok || claimed.RunID != runs[1] || claimed.HookID != "hook_1" {
		t.Fatalf("delivery after the holder disposed = %#v (%v), want hook_1 of %s", claimed, err, runs[1])
	}
}
