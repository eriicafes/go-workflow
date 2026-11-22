package core

import (
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

func TestReplayTimeoutReadsItsEnvironmentVariable(t *testing.T) {
	for _, test := range []struct {
		raw  string
		want time.Duration
	}{
		{"", DefaultReplayTimeout},
		{"60000", time.Minute},
		{"45000.5", 45*time.Second + 500*time.Microsecond},
		{"1000", 30 * time.Second},
		{"9000000", 780 * time.Second},
		{"-5", DefaultReplayTimeout},
		{"soon", DefaultReplayTimeout},
	} {
		t.Setenv("WORKFLOW_REPLAY_TIMEOUT_MS", test.raw)
		if got := replayTimeout(); got != test.want {
			t.Errorf("WORKFLOW_REPLAY_TIMEOUT_MS=%q gives %s, want %s", test.raw, got, test.want)
		}
	}
	ReplayTimeout = time.Second
	defer func() { ReplayTimeout = 0 }()
	if got := replayTimeout(); got != time.Second {
		t.Errorf("ReplayTimeout gives %s, want 1s", got)
	}
}

func TestReplayTimeoutRetriesReadsItsEnvironmentVariable(t *testing.T) {
	for raw, want := range map[string]int{"": 3, "0": 0, "5": 5, "1.5": 3, "many": 3, "-2": 0} {
		t.Setenv("WORKFLOW_REPLAY_TIMEOUT_MAX_RETRIES", raw)
		if got := replayTimeoutRetries(); got != want {
			t.Errorf("WORKFLOW_REPLAY_TIMEOUT_MAX_RETRIES=%q gives %d, want %d", raw, got, want)
		}
	}
}

func TestMaxEventsForTakesTheWorldsLimitTheEnvironmentAndTheOverride(t *testing.T) {
	for _, test := range []struct {
		reported        int
		limit, override string
		want            int
	}{
		{0, "", "", 25_000},
		{0, "100", "", 100},
		{0, "0", "", 25_000},
		{0, "lots", "", 25_000},
		// A World's limit for the run comes before the environment's.
		{500, "100", "", 500},
		// The override only lowers a limit.
		{500, "", "50", 50},
		{500, "", "900", 500},
		{0, "", "10", 10},
		{0, "", "-1", 25_000},
		{0, "", "1.5", 25_000},
	} {
		t.Setenv("WORKFLOW_MAX_EVENTS", test.limit)
		t.Setenv("WORKFLOW_MAX_EVENTS_OVERRIDE", test.override)
		if got := maxEventsFor(test.reported); got != test.want {
			t.Errorf("maxEventsFor(%d) with WORKFLOW_MAX_EVENTS=%q and override %q = %d, want %d", test.reported, test.limit, test.override, got, test.want)
		}
	}
}

func TestReplayBudgetLeavesOutTheTimeItIsPaused(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ReplayTimeout = 10 * time.Second
		defer func() { ReplayTimeout = 0 }()
		budget := newReplayBudget()
		time.Sleep(6 * time.Second)
		budget.pause()
		budget.pause()
		time.Sleep(time.Hour)
		budget.resume()
		time.Sleep(3 * time.Second)
		if budget.exhausted() {
			t.Fatal("the budget counted the time it was paused")
		}
		time.Sleep(time.Second)
		if !budget.exhausted() {
			t.Fatal("the budget is not exhausted after ten seconds of replay")
		}
		if got := budget.seconds(); got != "10" {
			t.Fatalf("seconds = %q, want 10", got)
		}
	})
}

func TestMaxEventsExceededErrorIsRecordedUnderTheSDKClass(t *testing.T) {
	failure := maxEventsExceededError{count: 7, limit: 5}
	if got, want := failure.Error(), "Workflow exceeded the maximum of 5 events per run"; got != want {
		t.Fatalf("message = %q, want %q", got, want)
	}
	failure.atLeast = true
	if got, want := failure.Error(), "Workflow exceeded the maximum of 5 events per run (its event log holds at least 7 events)"; got != want {
		t.Fatalf("message = %q, want %q", got, want)
	}
	data, err := worlds.EncodeRunFailed(failure.recorded(), worlds.PayloadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if code := worlds.RunFailedErrorCode(data); code != "MAX_EVENTS_EXCEEDED" {
		t.Fatalf("error code = %q", code)
	}
	recorded := worlds.DecodeErrorPayload(worlds.DecodeRunFailed(data), nil)
	if recorded.Name != "MaxEventsExceededError" || !errors.Is(recorded, worlds.ErrMaxEventsExceeded) {
		t.Fatalf("recorded = %#v", recorded)
	}
}

func TestCountEventsLeavesOutNoops(t *testing.T) {
	events := []worlds.Event{{EventType: worlds.EventTypeRunCreated}, {EventType: worlds.EventTypeNoop}, {EventType: worlds.EventTypeRunStarted}}
	if got := countEvents(events); got != 2 {
		t.Fatalf("countEvents = %d, want 2", got)
	}
}
