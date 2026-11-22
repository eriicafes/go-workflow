package core

import (
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

const (
	// DefaultReplayTimeout is how long one flow delivery may spend replaying:
	// reading the log, running the runner between steps, and writing events.
	// The time its inline steps' functions run, and the time it lingers, do
	// not count.
	DefaultReplayTimeout = 240 * time.Second
	// minReplayTimeout and maxReplayTimeout bound WORKFLOW_REPLAY_TIMEOUT_MS.
	minReplayTimeout = 30 * time.Second
	maxReplayTimeout = 780 * time.Second
	// DefaultReplayTimeoutRetries is how many deliveries of a message may run
	// out of replay time and be delivered again before the run fails.
	DefaultReplayTimeoutRetries = 3
	// DefaultMaxEvents is the most events a run may hold, unless the World
	// sets another limit for the run.
	DefaultMaxEvents = 25_000
)

var (
	// ReplayTimeout, when above zero, is the replay time of a flow delivery,
	// in place of WORKFLOW_REPLAY_TIMEOUT_MS and DefaultReplayTimeout.
	ReplayTimeout time.Duration
	// MaxEvents, when above zero, is the most events a run may hold where
	// its World sets no limit, in place of WORKFLOW_MAX_EVENTS and
	// DefaultMaxEvents.
	MaxEvents int
)

// replayTimeout returns the replay time of a flow delivery: ReplayTimeout,
// or WORKFLOW_REPLAY_TIMEOUT_MS in milliseconds, kept between 30 and 780
// seconds, or DefaultReplayTimeout when it is unset or not a positive number.
func replayTimeout() time.Duration {
	if ReplayTimeout > 0 {
		return ReplayTimeout
	}
	raw := os.Getenv("WORKFLOW_REPLAY_TIMEOUT_MS")
	if raw == "" {
		return DefaultReplayTimeout
	}
	ms, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsNaN(ms) || math.IsInf(ms, 0) || ms <= 0 {
		warnEnvOnce("WORKFLOW_REPLAY_TIMEOUT_MS", raw, "not a positive number; using the default")
		return DefaultReplayTimeout
	}
	timeout := time.Duration(ms * float64(time.Millisecond))
	if timeout < minReplayTimeout {
		warnEnvOnce("WORKFLOW_REPLAY_TIMEOUT_MS", raw, "below the minimum; using 30s")
		return minReplayTimeout
	}
	if timeout > maxReplayTimeout {
		warnEnvOnce("WORKFLOW_REPLAY_TIMEOUT_MS", raw, "above the maximum; using 780s")
		return maxReplayTimeout
	}
	return timeout
}

// replayTimeoutRetries returns how many deliveries of a message may run out
// of replay time before the run fails: WORKFLOW_REPLAY_TIMEOUT_MAX_RETRIES, a
// whole number of zero or more, or DefaultReplayTimeoutRetries.
func replayTimeoutRetries() int {
	if retries, ok := envInteger("WORKFLOW_REPLAY_TIMEOUT_MAX_RETRIES"); ok {
		return max(retries, 0)
	}
	return DefaultReplayTimeoutRetries
}

// maxEventsFor returns the most events a run may hold: reported, the limit
// its World sets for it, or else MaxEvents, WORKFLOW_MAX_EVENTS, or
// DefaultMaxEvents, and never more than WORKFLOW_MAX_EVENTS_OVERRIDE when
// that is a positive whole number.
func maxEventsFor(reported int) int {
	limit := reported
	if limit <= 0 {
		limit = MaxEvents
	}
	if limit <= 0 {
		limit = DefaultMaxEvents
		if value, ok := envInteger("WORKFLOW_MAX_EVENTS"); ok && value > 0 {
			limit = value
		}
	}
	if raw := os.Getenv("WORKFLOW_MAX_EVENTS_OVERRIDE"); raw != "" {
		if override, ok := envInteger("WORKFLOW_MAX_EVENTS_OVERRIDE"); ok && override > 0 {
			limit = min(limit, override)
		} else if ok {
			warnEnvOnce("WORKFLOW_MAX_EVENTS_OVERRIDE", raw, "not a positive whole number; ignored")
		}
	}
	return limit
}

// envInteger returns the whole number the environment variable name holds.
// It reports false, with a warning for a value that is not one, when the
// variable is unset or not a whole number.
func envInteger(name string) (int, bool) {
	raw := os.Getenv(name)
	if raw == "" {
		return 0, false
	}
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil || value != math.Trunc(value) || math.IsInf(value, 0) {
		warnEnvOnce(name, raw, "not a whole number; ignored")
		return 0, false
	}
	return int(value), true
}

var warnedEnv sync.Map

// warnEnvOnce logs, once per process for each value, that the environment
// variable name holds a value the runtime does not use as given.
func warnEnvOnce(name, raw, problem string) {
	if _, warned := warnedEnv.LoadOrStore(name+"="+raw, true); !warned {
		log.Printf("workflow: warning: %s=%q is %s", name, raw, problem)
	}
}

// replayBudget measures the replay time of one flow delivery across its
// passes. It is paused while the delivery waits on a step it runs inline or
// lingers, and is read between the runner's steps, so one long stretch of
// replay can pass the limit before it is seen.
type replayBudget struct {
	limit   time.Duration
	elapsed time.Duration
	// since is when the open interval started. It is zero while paused.
	since time.Time
}

func newReplayBudget() *replayBudget {
	return &replayBudget{limit: replayTimeout(), since: time.Now()}
}

// pause stops the count. Pausing a paused budget does nothing.
func (b *replayBudget) pause() {
	if b == nil || b.since.IsZero() {
		return
	}
	b.elapsed += time.Since(b.since)
	b.since = time.Time{}
}

// resume starts the count again.
func (b *replayBudget) resume() {
	if b != nil {
		b.since = time.Now()
	}
}

// exhausted reports whether the delivery has replayed for its limit.
func (b *replayBudget) exhausted() bool {
	if b == nil {
		return false
	}
	elapsed := b.elapsed
	if !b.since.IsZero() {
		elapsed += time.Since(b.since)
	}
	return elapsed >= b.limit
}

// seconds returns the limit in seconds, as error messages write it.
func (b *replayBudget) seconds() string {
	return strconv.FormatFloat(b.limit.Seconds(), 'f', -1, 64)
}

// errReplayBudgetExhausted ends a pass of a flow delivery that has replayed
// for its limit.
var errReplayBudgetExhausted = errors.New("workflow: replay exceeded its maximum duration")

// maxEventsExceededError ends a pass of a flow whose run's log holds limit
// events or more. atLeast is set when the log was read only up to the limit.
type maxEventsExceededError struct {
	count, limit int
	atLeast      bool
}

func (e maxEventsExceededError) Error() string {
	if e.atLeast {
		return fmt.Sprintf("Workflow exceeded the maximum of %d events per run (its event log holds at least %d events)", e.limit, e.count)
	}
	return fmt.Sprintf("Workflow exceeded the maximum of %d events per run", e.limit)
}

// recorded returns the error the run fails with, recorded as the SDK's
// MaxEventsExceededError.
func (e maxEventsExceededError) recorded() error {
	return &worlds.Error{Name: "MaxEventsExceededError", Message: e.Error(), Cause: worlds.NewError(worlds.ErrMaxEventsExceeded)}
}

// countEvents returns how many of events count toward a run's limit: all but
// the noop events a World fills abandoned positions with.
func countEvents(events []worlds.Event) int {
	count := 0
	for _, event := range events {
		if event.EventType != worlds.EventTypeNoop {
			count++
		}
	}
	return count
}

// eventLimitFractions are the shares of a run's event limit at which the
// runtime warns that the run is approaching it.
var eventLimitFractions = []float64{0.5, 0.8, 0.95}

var warnedEventLimits sync.Map

// warnApproachingEventLimit logs, once per process for each run and share,
// that a run's log has reached a share of its limit.
func warnApproachingEventLimit(runID worlds.RunID, count, limit int) {
	reached := 0.0
	for _, fraction := range eventLimitFractions {
		if count >= int(math.Ceil(float64(limit)*fraction)) {
			reached = fraction
		}
	}
	if reached == 0 {
		return
	}
	if _, warned := warnedEventLimits.LoadOrStore(fmt.Sprintf("%s:%g", runID, reached), true); !warned {
		log.Printf("workflow: warning: run %s is approaching the limit of events per run (%d of %d, %d%%), and fails with MAX_EVENTS_EXCEEDED when it reaches it", runID, count, limit, int(math.Round(reached*100)))
	}
}

// fatalError returns an error recorded as a FatalError with message, whose
// cause is cause's recorded chain, so errors.Is and errors.As reach cause
// through it. A nil cause records none.
func fatalError(message string, cause error) *worlds.Error {
	return &worlds.Error{Type: "FatalError", Message: message, Cause: worlds.NewError(cause)}
}
