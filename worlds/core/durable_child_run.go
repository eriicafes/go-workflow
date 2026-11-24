package core

import (
	"context"
	"strings"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// ReadFinishedRuns is the step that reports which of runIDs have finished, in
// their order. A run that cannot be found is reported as finished.
type ReadFinishedRuns func(ctx context.Context, runIDs []worlds.RunID) ([]worlds.RunID, error)

// AwaitChildRun starts waiting for child, a run this run started, through the
// run's child-run completion hook, and returns a function that reports
// whether the wait is over: the child finished, or waiting through the hook
// failed and the caller reads the child itself. readFinished is the step that
// reads which children finished.
//
// A run has one completion hook, worlds.ChildRunCompletionToken of its ID,
// which the first wait registers and which stays open for the rest of the
// run. While children are awaited, the run reads the ones awaited since its
// last read in one step, waits for payloads, each the worlds.ChildRunCompletion
// a finished child resumes the hook with, and keeps a timer open that reads
// every awaited child again when it fires, in case a resume was lost.
//
// The hook is registered before the children are read, and a child resumes
// it after its terminal event is recorded, so a resume that finds no hook
// means the read sees the child finished.
func AwaitChildRun(execution *Execution, child worlds.RunID, readFinished ReadFinishedRuns) func() bool {
	d := execution.childRuns
	if d == nil {
		d = &childRunCompletion{execution: execution, readFinished: readFinished, waiting: map[worlds.RunID]bool{}}
		execution.childRuns = d
		execution.OnAdvance(d.progress)
	}
	released := func() bool { return !d.waiting[child] }
	if d.conflicted {
		return released
	}
	if _, waiting := d.waiting[child]; !waiting {
		d.waiting[child] = true
		d.order = append(d.order, child)
	}
	d.toCheck = append(d.toCheck, child)
	switch {
	case d.parked:
		// A parked loop takes the new children once the runner waits, so
		// children awaited together are read together.
		d.nudged = true
	case !d.running:
		d.running = true
		d.start()
		d.progress()
	}
	return released
}

// childRunCompletion is a run's state of waiting for its children through
// its completion hook. Every replay rebuilds it from the same calls in the
// same order, so it drives the hook identically.
type childRunCompletion struct {
	execution    *Execution
	readFinished ReadFinishedRuns
	// waiting holds the awaited children, true while one is not released,
	// and order the awaited children in the order they were first awaited.
	waiting map[worlds.RunID]bool
	order   []worlds.RunID
	// toCheck holds the children awaited since the last read.
	toCheck []worlds.RunID
	// running is set while the loop waits for children, parked while it
	// waits for a payload or the timer, and nudged when a child is awaited
	// while it is parked. busy guards against re-entry.
	running bool
	parked  bool
	nudged  bool
	busy    bool
	// hookID is the completion hook, once created, and registered is set
	// once its registration is delivered. conflicted is set when the hook
	// cannot be used, after which every wait is over at once.
	hookID     worlds.HookID
	registered bool
	conflicted bool
	// cursor is the last hook event consumed, and armed is set while the
	// loop looks for the next payload.
	cursor worlds.EventID
	armed  bool
	// check is the read in flight.
	check worlds.StepID
	// safety is the open timer and safetyIndex the number of timers that
	// fired in this wait.
	safety      worlds.WaitID
	safetyIndex int
}

// safetyCheckDelay is how long the n-th timer of a wait runs: 5 minutes,
// then doubling, capped at 6 hours.
func safetyCheckDelay(n int) time.Duration {
	if n >= 7 {
		return 6 * time.Hour
	}
	return min(5*time.Minute<<n, 6*time.Hour)
}

// start begins a loop: it creates the completion hook on the first loop.
func (d *childRunCompletion) start() {
	e := d.execution
	if d.hookID == "" {
		hook := DurableHook[worlds.ChildRunCompletion](DurableHookOptions{Execution: e, Token: worlds.ChildRunCompletionToken(e.run.RunID), System: true})
		d.hookID = hook.HookID
		if hook.Err != nil && hook.ConflictRunID == "" {
			d.fail()
			return
		}
	}
	if d.registered && d.safety == "" {
		d.safetyIndex = 0
	}
}

// beforeWait lets a parked loop take the children awaited since it parked.
func (d *childRunCompletion) beforeWait() {
	if d.nudged {
		d.nudged = false
		d.parked = false
		d.progress()
	}
}

// progress runs the loop as far as the delivered events allow.
func (d *childRunCompletion) progress() {
	if d.busy {
		return
	}
	d.busy = true
	defer func() { d.busy = false }()
	e := d.execution
	if d.safety != "" && e.Settled(string(d.safety)) {
		// A timer that fires while the loop waits reads every awaited child
		// again. One that fires at another time is dropped.
		if d.running && d.parked {
			d.safetyIndex++
			d.parked = false
			for _, child := range d.order {
				if d.waiting[child] {
					d.toCheck = append(d.toCheck, child)
				}
			}
		}
		d.safety = ""
	}
	if d.parked {
		// A payload wakes the parked loop.
		if _, ok := hookEvent(e, d.hookID, d.cursor); ok {
			d.parked = false
		}
	}
	for d.running && !d.parked {
		if !d.registered {
			// The hook's registration is delivered as the walk passes its
			// hook_created or hook_conflict.
			_, key, _ := strings.Cut(string(d.hookID), "_")
			index, ok := e.index[key]
			if !ok || index >= e.horizon {
				return
			}
			if e.events[index].EventType == worlds.EventTypeHookConflict {
				d.fail()
				return
			}
			d.registered = true
			if d.safety == "" {
				d.safetyIndex = 0
			}
			continue
		}
		if d.check != "" {
			if !e.Settled(string(d.check)) {
				return
			}
			finished, err := StepOutcome[[]worlds.RunID](e, d.check)
			d.check = ""
			if err != nil {
				// The read failed for good: every waiting child reads its own
				// result, which reports any error of its own.
				d.toCheck = nil
				d.releaseAll()
				d.stop()
				return
			}
			for _, child := range finished {
				d.release(child)
			}
			continue
		}
		if !d.anyWaiting() {
			d.stop()
			return
		}
		if len(d.toCheck) > 0 {
			runIDs := d.toCheck
			d.toCheck = nil
			step := DurableInlineStep(e.Context(), DurableInlineStepOptions{Execution: e, Name: worlds.StepName(d.readFinished), MaxRetries: 3}, d.readFinished, runIDs)
			if step.Err != nil {
				d.releaseAll()
				d.stop()
				return
			}
			d.check = step.StepID
			continue
		}
		d.armed = true
		if event, ok := HookEvent(e, d.hookID, d.cursor); ok {
			d.cursor = event.EventID
			if event.EventType == worlds.EventTypeHookDisposed {
				d.fail()
				return
			}
			e.AdvanceClock(event.CreatedAt)
			var payload worlds.ChildRunCompletion
			if err := e.DecodePayload(worlds.DecodeHookReceived(event.Data), &payload); err != nil {
				d.fail()
				return
			}
			d.release(payload.RunID)
			continue
		}
		if d.safety == "" {
			wait := DurableWait(e.Context(), DurableWaitOptions{Execution: e, DueAt: e.Now().Add(safetyCheckDelay(d.safetyIndex))})
			d.safety = wait.WaitID
		}
		d.parked = true
	}
}

// release ends the wait for child, unless it is over.
func (d *childRunCompletion) release(child worlds.RunID) {
	if d.waiting[child] {
		d.waiting[child] = false
	}
}

func (d *childRunCompletion) releaseAll() {
	for _, child := range d.order {
		d.release(child)
	}
}

func (d *childRunCompletion) anyWaiting() bool {
	for _, child := range d.order {
		if d.waiting[child] {
			return true
		}
	}
	return false
}

// stop ends the loop. The hook stays open, but nothing looks for its
// payloads until the next loop, so it does not hold run-ahead back.
func (d *childRunCompletion) stop() {
	d.running, d.parked, d.nudged = false, false, false
	if d.armed {
		d.armed = false
		delete(d.execution.observedHooks, d.hookID)
	}
}

// fail stops using the hook: every wait, current and later, is over at once,
// and the children are read one by one.
func (d *childRunCompletion) fail() {
	d.conflicted = true
	d.toCheck = nil
	d.releaseAll()
	d.stop()
}
