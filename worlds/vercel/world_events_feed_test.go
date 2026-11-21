package vercelworld

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// feedRecorder records what an events feed delivers and why it stopped.
type feedRecorder struct {
	mu     sync.Mutex
	slots  []uint64
	err    error
	events chan struct{}
	failed chan struct{}
}

func newFeedRecorder() *feedRecorder {
	return &feedRecorder{events: make(chan struct{}, 64), failed: make(chan struct{}, 1)}
}

func (r *feedRecorder) onEvent(event worlds.Event) {
	slot, _ := worlds.EventSlot(event.EventID)
	r.mu.Lock()
	r.slots = append(r.slots, slot)
	r.mu.Unlock()
	r.events <- struct{}{}
}

func (r *feedRecorder) onError(err error) {
	r.mu.Lock()
	r.err = err
	r.mu.Unlock()
	r.failed <- struct{}{}
}

func TestEventsFeedFillsAGapAHeartbeatReveals(t *testing.T) {
	worlds_, _, backend := newWorlds(t, 1)
	world := worlds_[0]
	backend.heartbeat = 20 * time.Millisecond
	ctx := context.Background()
	runID := worlds.NewRunID()
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "feed", Input: []byte(`"x"`)}); err != nil {
		t.Fatal(err)
	}
	// Two events land before the subscription, which starts after the
	// first. The backend loses the push of the last, and its heartbeat,
	// naming that slot, has the feed read it from the log.
	backend.dropPush = 3
	for _, id := range []string{"wait_a", "wait_b"} {
		data, _ := json.Marshal(worlds.DataWaitCreated{ResumeAt: time.Now().Add(time.Hour)})
		if _, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeWaitCreated, CorrelationID: id, Data: data}}); err != nil {
			t.Fatal(err)
		}
	}
	recorder := newFeedRecorder()
	unsubscribe := world.EventsSubscribe(ctx, runID, 1, recorder.onEvent, recorder.onError)
	defer unsubscribe()
	for range 2 {
		select {
		case <-recorder.events:
		case <-recorder.failed:
			t.Fatalf("feed stopped: %v", recorder.err)
		case <-time.After(5 * time.Second):
			t.Fatal("no event")
		}
	}
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if len(recorder.slots) != 2 || recorder.slots[0] != 2 || recorder.slots[1] != 3 {
		t.Fatalf("slots = %v, want 2 and 3", recorder.slots)
	}
}

func TestEventsFeedReportsAnUnavailableSocket(t *testing.T) {
	worlds_, _, backend := newWorlds(t, 1)
	backend.noEventsSocket = true
	recorder := newFeedRecorder()
	unsubscribe := worlds_[0].EventsSubscribe(context.Background(), worlds.NewRunID(), 0, recorder.onEvent, recorder.onError)
	defer unsubscribe()
	select {
	case <-recorder.failed:
	case <-time.After(5 * time.Second):
		t.Fatal("the feed did not report the declined socket")
	}
}
