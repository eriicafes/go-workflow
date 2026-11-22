package core

import (
	"context"
	"encoding/json"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// A flow delivery whose pass suspended with nothing left to run lingers while
// the run waits on work done elsewhere, a background step or an open hook, or
// on a wait due soon: every event
// another writer appends meanwhile, and a wait falling due, starts another
// pass in the same delivery, so a burst of step results or hook payloads is
// handled by one delivery instead of one queued flow each. It lingers only on
// a World that pushes the run's events: waiting on the poll alone would hold
// the delivery for a poll's latency.

var (
	// LingerDuration is how long a suspended flow delivery lingers while its
	// run waits on a background step. Zero turns lingering off.
	LingerDuration = 5 * time.Second
	// HookLingerDuration is how long it lingers while its run waits on open
	// hooks alone, and never longer than LingerDuration.
	HookLingerDuration = 5 * time.Second
	// WaitLingerDuration is how soon an open wait is due for a suspended
	// flow delivery to linger until it is and complete it, and never longer
	// than LingerDuration.
	WaitLingerDuration = 5 * time.Second
)

// waitLingerSlack is how long a linger taken for a wait runs past the wait's
// deadline, and how far past the wait window a wait may be due and count.
const waitLingerSlack = time.Second

// externalWork is what a run waits on outside its flow.
type externalWork int

const (
	noExternalWork externalWork = iota
	hookWork
	stepWork
)

// externalWork returns what the run waits on outside its flow: a step that
// runs from its own message and has not finished, or else an open hook.
func (e *Execution) externalWork() externalWork {
	openHooks := map[string]bool{}
	type stepState struct{ inline, retried, finished bool }
	steps := map[string]*stepState{}
	for _, event := range e.events {
		id := event.CorrelationID
		switch event.EventType {
		case worlds.EventTypeHookCreated:
			openHooks[id] = true
		case worlds.EventTypeHookDisposed:
			delete(openHooks, id)
		case worlds.EventTypeStepCreated:
			var data worlds.DataStepCreated
			_ = json.Unmarshal(event.Data, &data)
			if steps[id] == nil {
				steps[id] = &stepState{inline: data.Inline}
			}
		case worlds.EventTypeStepStarted:
			if steps[id] == nil && worlds.StartCreatesStep(event.Data) {
				steps[id] = &stepState{inline: true}
			}
		case worlds.EventTypeStepRetrying:
			if step := steps[id]; step != nil {
				step.retried = true
			}
		case worlds.EventTypeStepCompleted, worlds.EventTypeStepFailed:
			if step := steps[id]; step != nil {
				step.finished = true
			}
		}
	}
	for _, step := range steps {
		if !step.finished && (!step.inline || step.retried) {
			return stepWork
		}
	}
	if len(openHooks) > 0 {
		return hookWork
	}
	return noExternalWork
}

// lingerUntil returns when a suspended flow delivery stops lingering, or the
// zero time when it does not linger: the later of the window for what the
// run waits on outside its flow and, when the earliest open wait is due
// within WaitLingerDuration and before the delivery's inline deadline, that
// wait's deadline, never past the inline deadline.
func (e *Execution) lingerUntil(now time.Time) (time.Time, bool) {
	limit := max(LingerDuration, 0)
	var external time.Duration
	switch e.externalWork() {
	case stepWork:
		external = limit
	case hookWork:
		external = min(HookLingerDuration, limit)
	}
	until := now.Add(external)
	capAt := e.startedAt.Add(DefaultMaxInlineStepsDuration)
	if e.inlineDuration > 0 {
		capAt = e.startedAt.Add(e.inlineDuration)
	}
	if !e.inlineUntil.IsZero() && e.inlineUntil.Before(capAt) {
		capAt = e.inlineUntil
	}
	forWait := false
	if window := min(WaitLingerDuration, limit); window > 0 {
		var next time.Time
		for _, wait := range e.openWaitsInLog() {
			if next.IsZero() || wait.resumeAt.Before(next) {
				next = wait.resumeAt
			}
		}
		if !next.IsZero() && !next.After(capAt) && next.Add(-waitLingerSlack).Sub(now) <= window {
			if at := next.Add(waitLingerSlack); at.After(until) {
				until, forWait = at, true
			}
		}
	}
	if until.After(capAt) {
		until = capAt
	}
	if !until.After(now) {
		return time.Time{}, false
	}
	return until, forWait
}

// linger keeps a flow delivery whose pass suspended live, and reports whether
// another pass is due: another writer appended an event, or a wait fell due.
// It reports false when the time ran out without either, or the World's
// push is not, or stops being, live.
func (r *Runtime) linger(ctx context.Context, execution *Execution) bool {
	if _, ok := r.world.(worlds.EventsSubscriber); !ok {
		return false
	}
	until, _ := execution.lingerUntil(time.Now())
	if until.IsZero() {
		return false
	}
	feed := newLiveFeed(ctx, r.world, execution.run.RunID, uint64(len(execution.events)))
	defer feed.close()
	open := execution.openWaitsInLog()
	timer := time.NewTimer(time.Until(until))
	defer timer.Stop()
	for {
		var due <-chan time.Time
		var dueTimer *time.Timer
		if next := earliestWait(open); !next.IsZero() && next.Before(until) {
			dueTimer = time.NewTimer(time.Until(next))
			due = dueTimer.C
		}
		select {
		case <-feed.signal:
			if dueTimer != nil {
				dueTimer.Stop()
			}
			for _, event := range feed.take() {
				if event.EventType != worlds.EventTypeNoop {
					return true
				}
			}
		case <-due:
			return true
		case <-timer.C:
			return false
		case <-ctx.Done():
			if dueTimer != nil {
				dueTimer.Stop()
			}
			return false
		}
		if !feed.live() {
			return false
		}
	}
}

// earliestWait returns when the earliest of open is due, or the zero time.
func earliestWait(open map[string]openWait) time.Time {
	var next time.Time
	for _, wait := range open {
		if next.IsZero() || wait.resumeAt.Before(next) {
			next = wait.resumeAt
		}
	}
	return next
}
