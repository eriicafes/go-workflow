package worlds

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// A stream socket request the server refuses with 429 did not apply, so it is
// sent again under a new request ID after the wait the refusal names: at
// least streamSocketThrottleMinWait, which is also the wait for a refusal
// that names none. One request waits streamSocketThrottleBudget in all, and
// never into the last streamSocketDeadlineReserve before its context's
// deadline. Past either, the socket is given up and the request goes through
// the writer's fallback.
var (
	streamSocketThrottleMinWait = time.Second
	streamSocketThrottleBudget  = 30 * time.Second
	streamSocketDeadlineReserve = 15 * time.Second
)

// streamSocketThrottle accounts the waits of one stream socket request for
// the server's rate limit to pass.
type streamSocketThrottle struct{ waited time.Duration }

// next returns the wait before the request is sent again after a refusal
// that asks for after, and false when the request is not to be sent on the
// socket again: the wait would take the request's waits past
// streamSocketThrottleBudget, or run into the reserve before ctx's deadline.
func (t *streamSocketThrottle) next(ctx context.Context, after time.Duration) (time.Duration, bool) {
	wait := max(after, streamSocketThrottleMinWait)
	deadline, bounded := ctx.Deadline()
	if t.waited+wait > streamSocketThrottleBudget || bounded && time.Until(deadline)-wait < streamSocketDeadlineReserve {
		return 0, false
	}
	t.waited += wait
	return wait, true
}

// socketRetryAfter reads a 429's retryAfter, a Retry-After header's value:
// seconds, or an HTTP date. It is zero when the refusal names no wait.
func socketRetryAfter(value string) time.Duration {
	value = strings.TrimSpace(value)
	if seconds, err := strconv.Atoi(value); err == nil {
		return time.Duration(max(seconds, 0)) * time.Second
	}
	if at, err := http.ParseTime(value); err == nil {
		return max(time.Until(at), 0)
	}
	return 0
}
