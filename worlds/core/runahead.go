package core

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"sync"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// Run-ahead lets a flow hand an inline step's result to the runner as soon
// as the step returns, and write it while the runner goes on, so consecutive
// inline steps overlap their writes instead of each waiting for its own.
//
// Without it, every view of the log a flow decides from is a prefix of the
// run's log, and an event the flow has not seen lands after everything it
// read. A flow that runs ahead decides from the committed prefix plus its own
// writes in flight, and an event another writer commits can land between
// them. A later replay reads the event there, so run-ahead is allowed only
// where no such event can change a decision:
//
//   - Only inline steps' results are written ahead. They are facts, not
//     decisions, and every other write of the flow first waits for the writes
//     ahead of it to commit (drains), and so does a flow before it suspends.
//   - A flow runs ahead past a boundary only while nothing another writer can
//     append could change what the runner decides there: no hook the runner
//     waits on, no cancellation context, which a cancellation reaches through
//     a hook, no step delivered by another delivery, whose result is unknown,
//     and no timer due before the flow could finish. Otherwise it drains
//     first.
//   - Each write's answer reports the events between the position the flow
//     placed it at and its own. One that is inert there, a payload of a hook
//     nobody waits on, an attribute write, or the run's cancellation, is
//     folded in after the flow's own writes: a replay that reads it earlier
//     decides the same. Any other ends the delivery, which starts again from
//     a fresh read of the log once, and then hands its message on.
//   - A World that stores an in-band write at a time other than the one the
//     flow sent, which the runner's clock already read, ends the delivery the
//     same way and is never run ahead of again in the process.
//
// At most the run-ahead depth of results are unconfirmed past the last write
// the flow saw commit.

const (
	// DefaultRunAheadDepth is how many inline steps' results a flow hands to
	// the runner ahead of their writes, unless WithRunAheadDepth sets another
	// depth.
	DefaultRunAheadDepth = 10
	// maxRunAheadDepth caps WithRunAheadDepth.
	maxRunAheadDepth = 16
	// maxRunAheadRestarts is how often a delivery whose run-ahead stopped
	// starts again from a fresh read of the log before it hands its message
	// on.
	maxRunAheadRestarts = 1
	// openWaitSkew is how much earlier than its resumeAt a timer may fire,
	// as clocks disagree, which a flow allows for when it decides whether a
	// timer can fall due while it runs ahead.
	openWaitSkew = 30 * time.Second
)

// RuntimeOption configures a Runtime.
type RuntimeOption func(*Runtime)

// WithRunAheadDepth sets how many inline steps' results a flow hands to the
// runner ahead of their writes: zero turns run-ahead off, and other values
// are clamped to 1 through 16. The default is DefaultRunAheadDepth.
func WithRunAheadDepth(depth int) RuntimeOption {
	return func(r *Runtime) { r.runAheadDepth = min(max(depth, 0), maxRunAheadDepth) }
}

// errRunAheadStop ends a delivery whose run-ahead found an event it cannot
// fold in, or a World that does not store its writes' times.
type errRunAheadStop struct{ reason string }

func (e errRunAheadStop) Error() string { return "workflow: run-ahead stopped: " + e.reason }

// runAheadDisabled holds the Worlds found to store an in-band write at a
// time other than the one sent, which no flow of the process runs ahead of.
var runAheadDisabled sync.Map

func runAheadDisabledFor(world worlds.World) bool {
	if !reflect.TypeOf(world).Comparable() {
		return false
	}
	_, disabled := runAheadDisabled.Load(world)
	return disabled
}

func disableRunAheadFor(world worlds.World) {
	if reflect.TypeOf(world).Comparable() {
		runAheadDisabled.Store(world, true)
	}
}

// aheadWrite is a write ahead: an inline step's result, or the batch that
// creates the inline steps of a boundary, the events the runner already read
// from index in the log on, and the World's answer once done is closed.
type aheadWrite struct {
	event    worlds.EventsCreateEvent
	batch    []worlds.EventsCreateEvent
	index    int
	occurred time.Time
	done     chan struct{}
	result   worlds.EventResult
	// createdAt holds the time each event of a batch was stored at.
	createdAt []time.Time
	err       error
	// observed holds the hooks the runner waited on when it made the write,
	// which an event below it could have changed a decision for.
	observed map[worlds.HookID]bool
}

// count returns the number of events the write appends.
func (w *aheadWrite) count() int {
	if w.batch != nil {
		return len(w.batch)
	}
	return 1
}

// aheadWriter sends a flow's writes ahead one at a time, in order, each
// fenced with the count the one before it left.
type aheadWriter struct {
	writes chan *aheadWrite
}

// canRunAhead reports whether the flow may hand an inline step's result to
// the runner before its write commits.
func (e *Execution) canRunAhead() bool {
	if e.aheadDepth == 0 || e.superseded || len(e.ahead) >= e.aheadDepth || runAheadDisabledFor(e.world) {
		return false
	}
	return e.boundaryInert()
}

// boundaryInert reports whether nothing another writer can append now could
// change a decision of the runner: no hook it waits on, no cancellation
// context, no step another delivery runs, and no timer that can fall due
// before the flow could finish.
func (e *Execution) boundaryInert() bool {
	if len(e.observedHooks) > 0 {
		return false
	}
	window := time.Now().Add(openWaitSkew)
	if e.inlineUntil.IsZero() {
		window = window.Add(DefaultMaxInlineStepsDuration)
	} else {
		window = e.inlineUntil.Add(openWaitSkew)
	}
	for _, wait := range e.openWaitsInLog() {
		if wait.resumeAt.Before(window) {
			return false
		}
	}
	created := map[string]bool{}
	finished := map[string]bool{}
	for _, event := range e.events {
		switch event.EventType {
		case worlds.EventTypeHookCreated:
			var data worlds.DataHookCreated
			if json.Unmarshal(event.Data, &data) == nil && data.IsSystem {
				created[event.CorrelationID] = true
			}
		case worlds.EventTypeHookDisposed:
			finished[event.CorrelationID] = true
		case worlds.EventTypeStepCreated, worlds.EventTypeStepRetrying:
			// A step delivery runs it.
			created[event.CorrelationID] = true
		case worlds.EventTypeStepCompleted, worlds.EventTypeStepFailed:
			finished[event.CorrelationID] = true
		}
	}
	for id := range created {
		if !finished[id] {
			return false
		}
	}
	return true
}

// runAheadHazard returns why event, committed below a write the flow ran
// ahead of, could have changed a decision the runner already made, or "" when
// it is inert there.
func (e *Execution) runAheadHazard(event worlds.Event, observed map[worlds.HookID]bool) string {
	switch event.EventType {
	case worlds.EventTypeHookReceived, worlds.EventTypeHookDisposed, worlds.EventTypeHookConflict:
		if observed[worlds.HookID(event.CorrelationID)] || e.systemHook(event.CorrelationID) {
			return fmt.Sprintf("%s for a hook the runner may be waiting on", event.EventType)
		}
	case worlds.EventTypeStepStarted, worlds.EventTypeStepCompleted, worlds.EventTypeStepFailed, worlds.EventTypeStepRetrying:
		if !e.ranInline[worlds.StepID(event.CorrelationID)] {
			return fmt.Sprintf("%s of a step this delivery does not run", event.EventType)
		}
	case worlds.EventTypeWaitCompleted:
		return "wait_completed written by another writer"
	}
	return ""
}

// systemHook reports whether the log records hookID as a hook the runtime
// created for itself, such as a cancellation context's.
func (e *Execution) systemHook(hookID string) bool {
	for _, event := range e.events {
		if event.EventType == worlds.EventTypeHookCreated && event.CorrelationID == hookID {
			var data worlds.DataHookCreated
			return json.Unmarshal(event.Data, &data) == nil && data.IsSystem
		}
	}
	return false
}

// writeAhead adds result, the outcome of an inline step's attempt, to the log
// where its write is expected to land, so the runner reads it at once, and
// sends the write to land there behind the flow's other writes ahead.
func (e *Execution) writeAhead(step pendingStep, result worlds.DataStepResult) {
	eventType := worlds.EventTypeStepCompleted
	if len(result.Error) != 0 {
		eventType = worlds.EventTypeStepFailed
	}
	result.StepName, result.Attempt, result.WorkflowName = step.name, 0, ""
	if eventType == worlds.EventTypeStepCompleted {
		result.WorkflowName = e.run.WorkflowName
	} else {
		result.Attempt = step.attempt
	}
	data, err := json.Marshal(result)
	if err != nil {
		panic(e.exit(deliveryError{cause: err}))
	}
	occurred := time.Now().UTC().Truncate(time.Millisecond)
	write := &aheadWrite{
		event:    worlds.EventsCreateEvent{EventType: eventType, CorrelationID: string(step.stepID), Data: data, OccurredAt: occurred},
		index:    len(e.events),
		occurred: occurred,
		done:     make(chan struct{}),
		observed: maps.Clone(e.observedHooks),
	}
	if e.writer == nil {
		e.startWriter()
	}
	e.ahead = append(e.ahead, write)
	delete(e.ranInline, step.stepID)
	event := worlds.Event{RunID: e.run.RunID, EventID: worlds.EventIDForSlot(uint64(len(e.events) + 1)), EventType: eventType, CorrelationID: string(step.stepID), Data: data, CreatedAt: occurred}
	e.trackEvent(len(e.events), event)
	e.events = append(e.events, event)
	e.writer.writes <- write
}

// startWriter starts the goroutine that sends the flow's writes ahead, each
// fenced with the count the write before it left.
func (e *Execution) startWriter() {
	e.writer = &aheadWriter{writes: make(chan *aheadWrite, maxRunAheadDepth)}
	count := e.inBand
	go func() {
		failed := false
		for write := range e.writer.writes {
			if failed {
				write.err = errReplaySuperseded
				close(write.done)
				continue
			}
			fence, held := count, write.index
			if write.batch != nil {
				write.result, write.createdAt, write.err = e.sendBatchAhead(write.batch, held, fence)
			} else {
				write.result, write.err = e.world.EventsCreate(e.ctx, worlds.EventsCreateInput{
					RunID:   e.run.RunID,
					Event:   write.event,
					Options: worlds.EventsCreateOptions{EventCount: &held, InBandCount: &fence},
				})
				write.createdAt = []time.Time{write.result.CreatedAt}
			}
			if write.err != nil {
				failed = true
			} else {
				count = write.result.InBandCount
			}
			close(write.done)
		}
	}()
}

// confirmAhead takes the answers of the writes ahead that came back, in
// order, waiting for all of them when wait is set. A write's answer that
// reports events below it folds the inert ones in after the flow's own
// writes, and ends the delivery for any other.
func (e *Execution) confirmAhead(wait bool) {
	for len(e.ahead) > 0 {
		write := e.ahead[0]
		if !wait {
			select {
			case <-write.done:
			default:
				return
			}
		}
		<-write.done
		e.ahead = e.ahead[1:]
		switch {
		case errors.Is(write.err, worlds.ErrInBandSuperseded) || errors.Is(write.err, errReplaySuperseded):
			e.abandonAhead()
			e.supersede()
		case write.err != nil:
			e.abandonAhead()
			panic(e.exit(deliveryError{cause: write.err}))
		}
		e.inBand = write.result.InBandCount
		for _, createdAt := range write.createdAt {
			if !createdAt.Truncate(time.Millisecond).Equal(write.occurred) {
				// The runner read a time a replay never reads.
				disableRunAheadFor(e.world)
				e.abandonAhead()
				panic(e.exit(errRunAheadStop{reason: fmt.Sprintf("the World stored a write at %v, not at the %v it occurred", createdAt, write.occurred)}))
			}
		}
		e.foldBelow(write)
	}
	if len(e.ahead) == 0 && e.writer != nil {
		close(e.writer.writes)
		e.writer = nil
	}
}

// foldBelow adds the events write's answer reports below it to the log after
// the flow's own writes, and moves the flow's writes still ahead up by as
// many positions, or ends the delivery when one of them could have changed a
// decision.
func (e *Execution) foldBelow(write *aheadWrite) {
	var below []worlds.Event
	for _, event := range write.result.Events {
		// An event at a position the flow placed one of its own writes at
		// displaced that write, which landed higher.
		index, held := e.settles[string(event.EventID)]
		speculative := held && (e.isAhead(index) || index >= write.index && index < write.index+write.count())
		if !held || speculative {
			below = append(below, event)
		}
	}
	if write.result.MoreEvents {
		e.abandonAhead()
		panic(e.exit(errRunAheadStop{reason: "the report of the events below a write ahead was incomplete"}))
	}
	committed, err := worlds.EventSlot(write.result.EventID)
	// Where the write is expected to land, moved up by the events found
	// below the writes before it.
	predicted, _ := worlds.EventSlot(e.events[write.index].EventID)
	if err != nil || committed < predicted {
		e.abandonAhead()
		panic(e.exit(errRunAheadStop{reason: fmt.Sprintf("%s committed at %s, below the position it was placed at", write.event.EventType, write.result.EventID)}))
	}
	if len(below) == 0 && committed == predicted {
		return
	}
	for _, event := range below {
		if reason := e.runAheadHazard(event, write.observed); reason != "" {
			e.abandonAhead()
			panic(e.exit(errRunAheadStop{reason: reason}))
		}
	}
	// The flow's writes from this one on landed, or land, as many positions
	// higher as events were committed below them.
	shift := committed - predicted
	var moved []int
	for index := write.index; index < len(e.events); index++ {
		if e.isAhead(index) || index < write.index+write.count() {
			moved = append(moved, index)
			delete(e.settles, string(e.events[index].EventID))
		}
	}
	for _, index := range moved {
		event := &e.events[index]
		slot, _ := worlds.EventSlot(event.EventID)
		event.EventID = worlds.EventIDForSlot(slot + shift)
		e.settles[string(event.EventID)] = index
	}
	for _, event := range below {
		e.trackEvent(len(e.events), event)
		e.events = append(e.events, event)
	}
}

// isAhead reports whether the event at index is a write still ahead.
func (e *Execution) isAhead(index int) bool {
	for _, write := range e.ahead {
		if index >= write.index && index < write.index+write.count() {
			return true
		}
	}
	return false
}

// sendBatchAhead appends batch as one in-band write fenced with fence, from
// position held, and returns its answer as one write's: the first event's
// ID, the report below the block, and the time each event was stored at. A
// refused event fails the write.
func (e *Execution) sendBatchAhead(batch []worlds.EventsCreateEvent, held, fence int) (worlds.EventResult, []time.Time, error) {
	answer, err := e.world.EventsCreateBatch(e.ctx, worlds.EventsCreateBatchInput{RunID: e.run.RunID, InBandCount: &fence, EventCount: &held, Events: batch})
	if err != nil {
		return worlds.EventResult{}, nil, err
	}
	result := worlds.EventResult{Events: answer.Events, MoreEvents: answer.MoreEvents, InBandCount: answer.InBandCount}
	createdAt := make([]time.Time, len(answer.Results))
	for index, item := range answer.Results {
		if item.Err != nil {
			return worlds.EventResult{}, nil, item.Err
		}
		createdAt[index] = item.CreatedAt
	}
	if len(answer.Results) > 0 {
		result.EventID = answer.Results[0].EventID
	}
	return result, createdAt, nil
}

// writeBatchAhead adds events, a boundary's inline steps' creations, to the
// log where their write is expected to land, and sends the write behind the
// flow's other writes ahead. Every event is taken as committed.
func (e *Execution) writeBatchAhead(events []worlds.EventsCreateEvent) map[string]bool {
	occurred := time.Now().UTC().Truncate(time.Millisecond)
	write := &aheadWrite{batch: events, index: len(e.events), occurred: occurred, done: make(chan struct{}), observed: maps.Clone(e.observedHooks)}
	committed := make(map[string]bool, len(events))
	for index := range events {
		events[index].OccurredAt = occurred
		event := worlds.Event{RunID: e.run.RunID, EventID: worlds.EventIDForSlot(uint64(len(e.events) + 1)), EventType: events[index].EventType, CorrelationID: events[index].CorrelationID, Data: events[index].Data, CreatedAt: occurred}
		e.trackEvent(len(e.events), event)
		e.events = append(e.events, event)
		committed[event.CorrelationID] = true
	}
	if e.writer == nil {
		e.startWriter()
	}
	e.ahead = append(e.ahead, write)
	e.writer.writes <- write
	return committed
}

// drainAhead waits for every write ahead to commit and folds in what they
// report, so the flow decides from a prefix of the run's log again.
func (e *Execution) drainAhead() {
	if len(e.ahead) > 0 {
		e.confirmAhead(true)
	}
}

// abandonAhead waits for the writes still ahead to finish, without reading
// their answers, when the flow stops.
func (e *Execution) abandonAhead() {
	for _, write := range e.ahead {
		<-write.done
	}
	e.ahead = nil
	if e.writer != nil {
		close(e.writer.writes)
		e.writer = nil
	}
}
