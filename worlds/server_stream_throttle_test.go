package worlds

import (
	"context"
	"net/http"
	"testing"
	"time"
)

// SetStreamSocketThrottle sets the shortest wait for a stream socket's rate
// limit and the budget of one request's waits, and returns a function that
// restores them.
func SetStreamSocketThrottle(minWait, budget time.Duration) (restore func()) {
	previousWait, previousBudget := streamSocketThrottleMinWait, streamSocketThrottleBudget
	streamSocketThrottleMinWait, streamSocketThrottleBudget = minWait, budget
	return func() { streamSocketThrottleMinWait, streamSocketThrottleBudget = previousWait, previousBudget }
}

func TestSocketRetryAfterReadsSecondsAndDates(t *testing.T) {
	if got := socketRetryAfter(" 3 "); got != 3*time.Second {
		t.Errorf("seconds = %s, want 3s", got)
	}
	if got := socketRetryAfter(time.Now().Add(time.Minute).UTC().Format(http.TimeFormat)); got < 50*time.Second || got > time.Minute {
		t.Errorf("an HTTP date a minute away = %s", got)
	}
	// No wait, an unreadable one, a negative one and a date that has passed
	// name none.
	for _, value := range []string{"", "soon", "-5", time.Now().Add(-time.Minute).UTC().Format(http.TimeFormat)} {
		if got := socketRetryAfter(value); got != 0 {
			t.Errorf("retryAfter %q = %s, want 0", value, got)
		}
	}
}

func TestStreamSocketThrottleBoundsARequestsWaits(t *testing.T) {
	ctx := context.Background()
	// A refusal waits what it asks for, and at least the shortest wait.
	var throttle streamSocketThrottle
	if wait, ok := throttle.next(ctx, 0); !ok || wait != streamSocketThrottleMinWait {
		t.Fatalf("wait for a refusal that names none = %s, %t", wait, ok)
	}
	if wait, ok := throttle.next(ctx, 5*time.Second); !ok || wait != 5*time.Second {
		t.Fatalf("wait for a refusal asking for 5s = %s, %t", wait, ok)
	}
	// The waits of one request stay within the budget.
	left := streamSocketThrottleBudget - streamSocketThrottleMinWait - 5*time.Second
	if _, ok := throttle.next(ctx, left+time.Second); ok {
		t.Fatal("a wait past the budget was allowed")
	}
	if _, ok := throttle.next(ctx, left); !ok {
		t.Fatal("a wait that fills the budget was refused")
	}
	// No wait runs into the reserve before the context's deadline.
	near, cancel := context.WithTimeout(ctx, streamSocketDeadlineReserve+streamSocketThrottleMinWait/2)
	defer cancel()
	if _, ok := (&streamSocketThrottle{}).next(near, 0); ok {
		t.Fatal("a wait into the deadline's reserve was allowed")
	}
	far, cancel := context.WithTimeout(ctx, streamSocketDeadlineReserve+time.Minute)
	defer cancel()
	if _, ok := (&streamSocketThrottle{}).next(far, 0); !ok {
		t.Fatal("a wait well before the deadline was refused")
	}
}
