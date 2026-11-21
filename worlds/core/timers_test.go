package core

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

func TestHopDeadlineAnchorsHopsOnTheWaitsDeadline(t *testing.T) {
	now := time.Date(2030, time.January, 1, 0, 0, 0, 0, time.UTC)
	hour := time.Hour
	for _, test := range []struct {
		name     string
		resumeAt time.Time
		after    *time.Time
		maxDelay time.Duration
		want     time.Time
	}{
		{"no limit", now.Add(100 * hour), nil, 0, now.Add(100 * hour)},
		{"within the limit", now.Add(10 * hour), nil, 23 * hour, now.Add(10 * hour)},
		{"anchored on the deadline", now.Add(50 * hour), nil, 23 * hour, now.Add(50*hour - 46*hour)},
		{"the last hop", now.Add(30 * hour), nil, 23 * hour, now.Add(30*hour - 23*hour)},
		{"after the first hop", now.Add(50 * hour), ptr(now.Add(4 * hour)), 23 * hour, now.Add(27 * hour)},
		{"after the hop before the last", now.Add(50 * hour), ptr(now.Add(27 * hour)), 23 * hour, now.Add(50 * hour)},
	} {
		if got := hopDeadline(test.resumeAt, now, test.after, test.maxDelay); !got.Equal(test.want) {
			t.Errorf("%s: hop at %v, want %v", test.name, got.Sub(now), test.want.Sub(now))
		}
	}
}

func ptr[T any](value T) *T { return &value }

// queueRecorder is a World that records what is queued.
type queueRecorder struct {
	worlds.World
	queued []worlds.QueueInput
}

func (q *queueRecorder) Queue(_ context.Context, input worlds.QueueInput) (worlds.QueueResult, error) {
	q.queued = append(q.queued, input)
	return worlds.QueueResult{}, nil
}

func (*queueRecorder) Capabilities() worlds.Capabilities { return worlds.Capabilities{} }

func TestDeliveryThatMayHoldACoalescedTimerArmsNearDueWaitsByTheSecond(t *testing.T) {
	synctest.Test(t, testNearDueWaits)
}

func testNearDueWaits(t *testing.T) {
	sentNearDue = &sentKeys{limit: 10_000}
	waitEvent := func(id, creator string, resumeAt time.Time) worlds.Event {
		data, _ := json.Marshal(worlds.DataWaitCreated{ResumeAt: resumeAt, CreatorMessageID: creator})
		return worlds.Event{EventType: worlds.EventTypeWaitCreated, CorrelationID: id, Data: data}
	}
	now := time.Now()
	events := []worlds.Event{
		waitEvent("wait_near", "other", now.Add(2*time.Second)),
		waitEvent("wait_own", "msg", now.Add(time.Hour)),
		waitEvent("wait_far", "other", now.Add(time.Hour)),
	}
	keys := func(deliveryCount int) map[string]string {
		world := &queueRecorder{}
		e := &Execution{ctx: context.Background(), world: world, events: events, completedChains: map[string]bool{}, delivery: worlds.QueueInput{MessageID: "msg", DeliveryCount: deliveryCount}}
		e.armTimers()
		armed := map[string]string{}
		for _, input := range world.queued {
			armed[input.WaitContinuation.CorrelationID] = input.IdempotencyKey
		}
		return armed
	}
	second := fmt.Sprintf(":s%d", time.Now().Unix())
	// A first delivery arms only its own chain, under the hop's key.
	first := keys(1)
	if len(first) != 1 || first["wait_own"] != fmt.Sprintf("wait:wait_own:%d:%d", now.Add(time.Hour).UnixMilli(), now.Add(time.Hour).UnixMilli()) {
		t.Fatalf("first delivery armed %v", first)
	}
	// A later one also arms the near-due wait of another chain, and keys
	// both last hops by its second.
	later := keys(2)
	if len(later) != 2 || !strings.HasSuffix(later["wait_near"], second) || !strings.HasSuffix(later["wait_own"], second) {
		t.Fatalf("later delivery armed %v, want wait_near and wait_own keyed by %s", later, second)
	}
	// The process sends each near-due key once.
	if again := keys(2); len(again) != 0 {
		t.Fatalf("a delivery in the same second armed %v again", again)
	}
}

func TestHandoffArmsTheEarliestOpenWaitOfItsOwnChainWhenItCreatedIt(t *testing.T) {
	waitEvent := func(id, creator string, resumeAt time.Time) worlds.Event {
		data, _ := json.Marshal(worlds.DataWaitCreated{ResumeAt: resumeAt, CreatorMessageID: creator})
		return worlds.Event{EventType: worlds.EventTypeWaitCreated, CorrelationID: id, Data: data}
	}
	now := time.Now()
	events := []worlds.Event{
		waitEvent("wait_later", "msg", now.Add(2*time.Hour)),
		waitEvent("wait_first", "msg", now.Add(time.Hour)),
		waitEvent("wait_other", "other", now.Add(time.Minute)),
	}
	armed := func(created ...string) []worlds.QueueInput {
		world := &queueRecorder{}
		e := &Execution{ctx: context.Background(), world: world, delivery: worlds.QueueInput{MessageID: "msg", DeliveryCount: 1}, events: events, createdWaits: map[string]bool{}}
		for _, id := range created {
			e.createdWaits[id] = true
		}
		e.armCreatedWaitTimers()
		return world.queued
	}
	if queued := armed("wait_first"); len(queued) != 1 || queued[0].WaitContinuation.CorrelationID != "wait_first" {
		t.Fatalf("queued %+v, want the timer of wait_first", queued)
	}
	// An earlier delivery of the message created it and sent its timer.
	if queued := armed("wait_later"); len(queued) != 0 {
		t.Fatalf("queued %+v, want none", queued)
	}
}

func TestDeliveryArmsTheWaitItCreatesBeforeItCommitsWhenItIsItsChainsEarliest(t *testing.T) {
	data, _ := json.Marshal(worlds.DataWaitCreated{ResumeAt: time.Now().Add(2 * time.Hour), CreatorMessageID: "msg"})
	armed := func(dueIn time.Duration) []worlds.QueueInput {
		world := &queueRecorder{}
		e := &Execution{ctx: context.Background(), world: world, delivery: worlds.QueueInput{MessageID: "msg", DeliveryCount: 1},
			events: []worlds.Event{{EventType: worlds.EventTypeWaitCreated, CorrelationID: "wait_open", Data: data}}}
		e.armWaitsBeforeCreation([]pendingWait{{waitID: "wait_new", dueAt: time.Now().Add(dueIn)}})
		return world.queued
	}
	if queued := armed(time.Hour); len(queued) != 1 || queued[0].WaitContinuation.CorrelationID != "wait_new" {
		t.Fatalf("queued %+v, want the timer of wait_new", queued)
	}
	if queued := armed(3 * time.Hour); len(queued) != 0 {
		t.Fatalf("queued %+v, want none: the chain's earliest wait is open already", queued)
	}
	if queued := armed(-time.Second); len(queued) != 0 {
		t.Fatalf("queued %+v, want none for a wait already due", queued)
	}
}

func TestTimerDeliveryArmsItsWaitAgainWhenItFellDueAfterTheReplay(t *testing.T) {
	data, _ := json.Marshal(worlds.DataWaitCreated{ResumeAt: time.Now().Add(-time.Millisecond), CreatorMessageID: "msg"})
	world := &queueRecorder{}
	e := &Execution{ctx: context.Background(), world: world, completedChains: map[string]bool{},
		events:   []worlds.Event{{EventType: worlds.EventTypeWaitCreated, CorrelationID: "wait_a", Data: data}},
		delivery: worlds.QueueInput{MessageID: "timer", DeliveryCount: 1, WaitContinuation: &worlds.WaitContinuation{CorrelationID: "wait_a"}}}
	// The replay found the wait not yet due, and it is due now: the
	// delivery left it open, so it arms it again under the next attempt.
	e.armTimers()
	if len(world.queued) != 1 || world.queued[0].WaitContinuation.Attempt != 1 {
		t.Fatalf("queued %+v, want the wait's timer with attempt 1", world.queued)
	}
}
