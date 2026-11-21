package core

import (
	"sync/atomic"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// FeedPollInterval is how often a flow that waits for a step it runs inline
// reads the log's tail while its World pushes the run's events, in case a
// push was lost. Without a feed it reads every LogPollInterval.
var FeedPollInterval = 10 * time.Second

// feed holds a subscription to the run's events of a World that pushes them.
type feed struct {
	events      chan worlds.Event
	unsubscribe func()
	// healthy is cleared when the World reports the feed stopped or a push
	// did not fit in events.
	healthy atomic.Bool
	// missed is set when the flow left a pushed event out, so it reads the
	// log once it may hold the event.
	missed bool
}

// subscribe starts the Execution's feed of the run's events after the ones it
// holds, when its World pushes them and it has none yet.
func (e *Execution) subscribe() {
	subscriber, ok := e.world.(worlds.EventsSubscriber)
	if !ok || e.feed != nil {
		return
	}
	f := &feed{events: make(chan worlds.Event, 64)}
	f.healthy.Store(true)
	f.unsubscribe = subscriber.EventsSubscribe(e.ctx, e.run.RunID, uint64(len(e.events)), func(event worlds.Event) {
		select {
		case f.events <- event:
		default:
			f.healthy.Store(false)
		}
	}, func(error) {
		f.healthy.Store(false)
	})
	e.feed = f
}

// unsubscribe stops the Execution's feed.
func (e *Execution) unsubscribe() {
	if e.feed != nil {
		e.feed.unsubscribe()
	}
}

// pollInterval is how often a flow waiting for an inline step reads the log.
func (e *Execution) pollInterval() time.Duration {
	if e.feed != nil && e.feed.healthy.Load() {
		return FeedPollInterval
	}
	return LogPollInterval
}

// feedEvents returns the channel of pushed events, nil without a feed.
func (e *Execution) feedEvents() <-chan worlds.Event {
	if e.feed == nil {
		return nil
	}
	return e.feed.events
}

// accept adds a pushed event to the events the Execution holds when it is the
// next one, and reads the log when it is past a gap. While the flow has
// writes ahead it leaves the event out: their answers report the events
// below them, and the log is read once they are confirmed.
func (e *Execution) accept(event worlds.Event) {
	slot, err := worlds.EventSlot(event.EventID)
	if err != nil {
		return
	}
	switch {
	case len(e.ahead) > 0:
		if slot > uint64(len(e.events)) {
			e.feed.missed = true
		}
	case slot == uint64(len(e.events))+1:
		e.mergeEvents([]worlds.Event{event})
	case slot > uint64(len(e.events))+1:
		e.reload()
	}
}

// catchUp reads the log when the flow left a pushed event out and holds no
// writes ahead.
func (e *Execution) catchUp() {
	if e.feed != nil && e.feed.missed && len(e.ahead) == 0 {
		e.feed.missed = false
		e.reload()
	}
}
