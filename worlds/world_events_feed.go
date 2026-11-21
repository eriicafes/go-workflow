package worlds

import (
	"context"
	"sync"
)

// EventsSubscriber is implemented by a World that pushes the events appended
// to a run as they commit. A flow that waits for a step it runs inline
// subscribes, so a payload, a cancellation, or another step's result reaches
// the runner without waiting for its next read of the log.
type EventsSubscriber interface {
	// EventsSubscribe calls onEvent with each event of runID after slot
	// afterSlot, in slot order and each at most once, until ctx is done or
	// unsubscribe is called. It is best effort: a World may stop at any time,
	// and reports why to onError. onEvent is not called after unsubscribe
	// returns, and must not block.
	EventsSubscribe(ctx context.Context, runID RunID, afterSlot uint64, onEvent func(Event), onError func(error)) (unsubscribe func())
}

// FeedEvents implements EventsSubscriber over a World's change notification:
// changed returns a channel closed by the World's next committed change to
// the run, and a function that releases it. FeedEvents takes it before it
// reads the log, so a change while it reads still wakes it, and after each
// change reads the events after the last one it delivered.
func FeedEvents(ctx context.Context, world World, runID RunID, afterSlot uint64, changed func() (<-chan struct{}, func()), onEvent func(Event), onError func(error)) (unsubscribe func()) {
	ctx, cancel := context.WithCancel(ctx)
	var done sync.WaitGroup
	done.Go(func() {
		for {
			change, release := changed()
			cursor := ""
			if afterSlot > 0 {
				cursor = string(EventIDForSlot(afterSlot))
			}
			for {
				page, err := world.EventsList(ctx, EventsListInput{RunID: runID, Pagination: Pagination{Cursor: cursor}})
				if err != nil {
					release()
					if ctx.Err() == nil {
						onError(err)
					}
					return
				}
				for _, event := range page.Data {
					if ctx.Err() != nil {
						release()
						return
					}
					onEvent(event)
					if slot, err := EventSlot(event.EventID); err == nil {
						afterSlot = slot
					}
				}
				if !page.HasMore {
					break
				}
				cursor = page.Cursor
			}
			select {
			case <-change:
				release()
			case <-ctx.Done():
				release()
				return
			}
		}
	})
	return func() {
		cancel()
		done.Wait()
	}
}
