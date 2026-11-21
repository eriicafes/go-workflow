package core

import (
	"context"
	"sync"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// A flow that waits for a step it runs inline takes the events other writers
// append to the run from a live feed: the World's push, when it implements
// worlds.EventsSubscriber, and a poll of the log's tail. The feed passes an
// event on only when it takes the next slot after the last one it passed on,
// so a late, repeated, or out-of-order push is dropped and the poll fills in,
// and correctness never depends on the push.
//
// Without a live push, each poll that finds nothing doubles the interval from
// LogPollInterval up to maxLogPollInterval, and a poll that finds an event, a
// failed poll, or a write of the flow itself puts it back. With one, the poll
// is only a safety net: it starts at FeedPollInterval and each clean poll
// doubles it, up to feedPollBackoff times that, while a feed error, a failed
// poll, a poll that finds an event the push missed, or a push that leaves a
// gap puts it back at LogPollInterval at once.

var (
	// LogPollInterval is how often a flow that waits for a step it runs
	// inline reads the log's tail while no live push covers the run. Zero
	// turns the poll off.
	LogPollInterval = 2 * time.Second
	// FeedPollInterval is how often it reads the log's tail while the World's
	// live push is up, and never less than LogPollInterval.
	FeedPollInterval = 10 * time.Second
)

const (
	// maxLogPollInterval caps the backoff of the poll without a live push.
	maxLogPollInterval = 10 * time.Second
	// feedPollBackoff caps the backoff of the poll with a live push, as a
	// multiple of FeedPollInterval.
	feedPollBackoff = 2
)

// liveFeed feeds the events of a run after a slot, in slot order and without
// gaps.
type liveFeed struct {
	world worlds.World
	runID worlds.RunID
	ctx   context.Context
	stop  context.CancelFunc
	done  sync.WaitGroup

	pollInterval, feedPollInterval time.Duration

	mu       sync.Mutex
	nextSlot uint64
	cursor   string
	pending  map[uint64]worlds.Event
	ready    []worlds.Event
	feedLive bool
	current  time.Duration
	dueAt    time.Time
	// signal is ready when events are; reschedule when the poll is moved up.
	signal     chan struct{}
	reschedule chan struct{}
}

// newLiveFeed starts a feed of runID's events after slot afterSlot.
func newLiveFeed(ctx context.Context, world worlds.World, runID worlds.RunID, afterSlot uint64) *liveFeed {
	ctx, stop := context.WithCancel(ctx)
	f := &liveFeed{
		world: world, runID: runID, ctx: ctx, stop: stop,
		pollInterval: LogPollInterval, feedPollInterval: max(FeedPollInterval, LogPollInterval),
		nextSlot: afterSlot + 1, pending: map[uint64]worlds.Event{},
		signal: make(chan struct{}, 1), reschedule: make(chan struct{}, 1),
	}
	if afterSlot > 0 {
		f.cursor = string(worlds.EventIDForSlot(afterSlot))
	}
	f.current = f.pollInterval
	if subscriber, ok := world.(worlds.EventsSubscriber); ok {
		f.feedLive = true
		unsubscribe := subscriber.EventsSubscribe(ctx, runID, afterSlot, func(event worlds.Event) {
			f.offer([]worlds.Event{event}, true)
		}, func(error) {
			f.mu.Lock()
			f.feedLive = false
			f.resetLocked()
			f.mu.Unlock()
		})
		f.done.Go(func() {
			<-ctx.Done()
			unsubscribe()
		})
		f.mu.Lock()
		if f.feedLive {
			f.current = f.feedPollInterval
		}
		f.mu.Unlock()
	}
	if f.pollInterval > 0 {
		f.done.Go(f.pollLoop)
	}
	return f
}

// live reports whether the World's push is up.
func (f *liveFeed) live() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.feedLive
}

// close stops the feed.
func (f *liveFeed) close() {
	f.stop()
	f.done.Wait()
}

// take returns the events the feed has ready.
func (f *liveFeed) take() []worlds.Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	ready := f.ready
	f.ready = nil
	return ready
}

// noteActivity tells the feed the flow wrote to the log: without a live push
// the poll's backoff starts over, since what the write started makes another
// writer's event likely.
func (f *liveFeed) noteActivity() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.feedLive {
		f.resetLocked()
	}
}

// resetLocked puts the poll back at the interval without a push, moving up a
// poll due later than one interval from now.
func (f *liveFeed) resetLocked() {
	f.current = f.pollInterval
	if !f.dueAt.IsZero() && f.dueAt.After(time.Now().Add(f.pollInterval)) {
		select {
		case f.reschedule <- struct{}{}:
		default:
		}
	}
}

func (f *liveFeed) pollLoop() {
	for {
		f.mu.Lock()
		wait := f.current
		f.dueAt = time.Now().Add(wait)
		f.mu.Unlock()
		timer := time.NewTimer(wait)
		select {
		case <-f.ctx.Done():
			timer.Stop()
			return
		case <-f.reschedule:
			timer.Stop()
			continue
		case <-timer.C:
		}
		f.mu.Lock()
		f.dueAt = time.Time{}
		f.mu.Unlock()
		f.poll()
	}
}

// poll reads the log's tail once and passes on what continues the sequence.
func (f *liveFeed) poll() {
	f.mu.Lock()
	clean, cursor := f.feedLive, f.cursor
	f.mu.Unlock()
	failed, delivered := false, 0
	for {
		page, err := f.world.EventsList(f.ctx, worlds.EventsListInput{RunID: f.runID, Pagination: worlds.Pagination{Cursor: cursor}})
		if err != nil {
			clean, failed = false, true
			break
		}
		if page.Cursor != "" {
			cursor = page.Cursor
		}
		// An event only the poll passed on is one the push missed.
		if offered := f.offer(page.Data, false); offered > 0 {
			clean, delivered = false, delivered+offered
		}
		if !page.HasMore {
			break
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cursor = cursor
	switch {
	case clean && f.feedLive && len(f.pending) == 0:
		f.current = min(f.current*2, f.feedPollInterval*feedPollBackoff)
	case !f.feedLive && !failed && delivered == 0 && len(f.pending) == 0:
		// Nothing new and no push: back off, up to the cap without a push.
		f.current = min(max(f.current, f.pollInterval)*2, max(f.pollInterval, maxLogPollInterval))
	default:
		f.current = f.pollInterval
	}
}

// offer takes events in and passes on what continues the slot sequence,
// returning how many it passed on.
func (f *liveFeed) offer(events []worlds.Event, push bool) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.ctx.Err() != nil {
		return 0
	}
	for _, event := range events {
		slot, err := worlds.EventSlot(event.EventID)
		if err != nil || slot < f.nextSlot {
			continue
		}
		f.pending[slot] = event
	}
	passed := 0
	for {
		event, ok := f.pending[f.nextSlot]
		if !ok {
			break
		}
		delete(f.pending, f.nextSlot)
		f.ready = append(f.ready, event)
		f.nextSlot++
		passed++
	}
	// A pushed event above a gap: the push skipped a slot.
	if push && len(f.pending) > 0 {
		f.resetLocked()
	}
	if passed > 0 {
		select {
		case f.signal <- struct{}{}:
		default:
		}
	}
	return passed
}

// startFeed starts the Execution's live feed from the highest position it
// holds confirmed, so another writer's event that takes a position a write
// ahead was placed at is still offered.
func (e *Execution) startFeed() {
	if e.feed != nil {
		return
	}
	confirmed := len(e.events)
	if len(e.ahead) > 0 {
		confirmed = e.ahead[0].index
	}
	e.feed = newLiveFeed(e.ctx, e.world, e.run.RunID, uint64(confirmed))
}

// stopFeed stops the Execution's live feed.
func (e *Execution) stopFeed() {
	if e.feed != nil {
		e.feed.close()
		e.feed = nil
	}
}

// feedSignal returns the channel that is ready when the feed has events.
func (e *Execution) feedSignal() <-chan struct{} {
	if e.feed == nil {
		return nil
	}
	return e.feed.signal
}

// noteActivity tells the live feed the flow wrote to the log.
func (e *Execution) noteActivity() {
	if e.feed != nil {
		e.feed.noteActivity()
	}
}

// takeFeed takes in the events the feed has ready. While the flow has writes
// ahead, each is checked for a hazard as a write's report is. An event at a
// position a write ahead holds is that write's own commit, which its answer
// confirms, or one that displaced it, which its answer reports: either way it
// is left out, and so is everything after it until the writes ahead settle,
// when the flow reads the log after what it holds.
func (e *Execution) takeFeed() {
	if e.feed == nil {
		return
	}
	for _, event := range e.feed.take() {
		slot, err := worlds.EventSlot(event.EventID)
		if err != nil {
			continue
		}
		if len(e.ahead) > 0 {
			if !e.isOwnOrKnown(event, slot) {
				if reason := e.feedHazard(event); reason != "" {
					e.stopRunAhead(reason, event)
				}
			}
			if index := int(slot) - 1; index < len(e.events) && e.isAhead(index) || e.feedHeld {
				e.feedHeld = true
				continue
			}
		}
		switch {
		case e.repairDue:
		case slot <= uint64(len(e.events)):
		case slot == uint64(len(e.events))+1:
			e.mergeEvents([]worlds.Event{event})
		case len(e.ahead) > 0:
			e.feedHeld = true
		default:
			e.reload()
		}
	}
	if e.feedHeld && len(e.ahead) == 0 && !e.repairDue {
		e.feedHeld = false
		e.reload()
	}
}

// isOwnOrKnown reports whether event, at slot, is the flow's own write ahead or
// one the log it holds has.
func (e *Execution) isOwnOrKnown(event worlds.Event, slot uint64) bool {
	index := int(slot) - 1
	if index < 0 || index >= len(e.events) {
		return false
	}
	held := e.events[index]
	return held.EventType == event.EventType && held.CorrelationID == event.CorrelationID
}

// feedHazard returns why event, pushed or polled while the flow has writes
// ahead, could have changed a decision made under any of them.
func (e *Execution) feedHazard(event worlds.Event) string {
	observed := map[worlds.HookID]bool{}
	for _, write := range e.ahead {
		for id := range write.observed {
			observed[id] = true
		}
	}
	return e.runAheadHazard(event, &aheadWrite{observed: observed})
}
