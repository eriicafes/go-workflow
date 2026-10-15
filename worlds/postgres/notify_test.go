package postgresworld

import (
	"testing"
	"time"
)

func TestNotifierFanOut(t *testing.T) {
	n := newNotifier(nil)
	n.started = true // no listener: exercise only the fan-out
	first, releaseFirst := n.wait(channelRuns, "a")
	second, releaseSecond := n.wait(channelRuns, "a")
	other, releaseOther := n.wait(channelRuns, "b")
	defer releaseFirst()
	defer releaseSecond()
	n.signal(notifyKey(channelRuns, "a"))
	for _, ch := range []<-chan struct{}{first, second} {
		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Fatal("waiter not signalled")
		}
	}
	select {
	case <-other:
		t.Fatal("unrelated waiter signalled")
	default:
	}
	releaseOther()
	if len(n.waiters) != 0 {
		t.Fatalf("waiters leaked: %v", n.waiters)
	}
	again, release := n.wait(channelRuns, "a")
	defer release()
	n.broadcast()
	select {
	case <-again:
	case <-time.After(time.Second):
		t.Fatal("broadcast did not wake waiter")
	}
}
