package core

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// A wait's timer is a flow message due when the wait is. A wait further away
// than the World's MaxQueueDelay is reached in hops anchored on its
// deadline: the last hop is due when the wait is, and each earlier one a
// maximum delay before the next. Every sender of one hop computes the same
// deadline, and so the same key, wait:<id>:<resumeAtMs>:<hopAtMs>, so the
// timers they send are one message. A re-arm after a timer that came before
// its wait was due appends :a<attempt>.
//
// The waits a flow message's deliveries create form its chain, and only the
// chain's owners arm its timers, and only its earliest open wait's: the
// message's own deliveries, a flow that completes one of the chain's waits,
// which first arms the chain's next, and a redelivery of the chain's timer.

// waitTimer returns the timer of the wait waitID due at resumeAt, sent at
// now, or after the intermediate hop due at after when it is not nil.
func (e *Execution) waitTimer(waitID string, resumeAt time.Time, attempt int, after *time.Time) worlds.QueueInput {
	hopAt := hopDeadline(resumeAt, time.Now(), after, e.world.Capabilities().MaxQueueDelay)
	key := fmt.Sprintf("wait:%s:%d:%d", waitID, resumeAt.UnixMilli(), hopAt.UnixMilli())
	if attempt > 0 {
		key += fmt.Sprintf(":a%d", attempt)
	}
	continuation := &worlds.WaitContinuation{CorrelationID: waitID, Attempt: attempt}
	if hopAt.Before(resumeAt) {
		continuation.ResumeAt, continuation.HopAt = &resumeAt, &hopAt
	}
	return worlds.QueueInput{NotBefore: &hopAt, IdempotencyKey: key, WaitContinuation: continuation}
}

// hopDeadline returns the deadline of the hop a sender at now sends for a
// wait due at resumeAt: the earliest hop still ahead of it, and after the
// intermediate hop due at after when it is not nil. A zero maxDelay sends
// the last hop.
func hopDeadline(resumeAt, now time.Time, after *time.Time, maxDelay time.Duration) time.Time {
	if maxDelay <= 0 {
		return resumeAt
	}
	hopAt := resumeAt
	if remaining := resumeAt.Sub(now); remaining > maxDelay {
		hops := (remaining + maxDelay - 1) / maxDelay
		hopAt = resumeAt.Add(-(hops - 1) * maxDelay)
	}
	if after != nil {
		if next := after.Add(maxDelay); next.After(hopAt) {
			hopAt = next
		}
		if hopAt.After(resumeAt) {
			hopAt = resumeAt
		}
	}
	return hopAt
}

// NearDueWaitWindow is how close to due an open wait is when a delivery that
// may hold a coalesced timer arms it whatever chain it is in.
const NearDueWaitWindow = 5 * time.Second

// mayHoldCoalescedTimer reports whether the queue may have coalesced a timer
// into the delivery: any delivery but its message's first.
func (e *Execution) mayHoldCoalescedTimer() bool {
	return e.delivery.DeliveryCount != 1
}

// armWait queues the timer of the open wait waitID due at resumeAt, once per
// delivery. A queue can coalesce a wait's last hop that came due before the
// wait into a delivery, which then finds the wait not due while the queue
// still holds the hop's key, so a delivery that may hold one keys a last hop
// it arms afresh by its own second instead, wait:<id>:<resumeAtMs>:s<second>,
// as it does a near-due re-arm. A re-arm of a timer delivery's own wait
// carries the next attempt and keeps its hop's key.
func (e *Execution) armWait(waitID string, resumeAt time.Time, attempt int, nearDue bool) {
	if e.armedWaits[waitID] {
		return
	}
	if e.armedWaits == nil {
		e.armedWaits = map[string]bool{}
	}
	e.armedWaits[waitID] = true
	timer := e.waitTimer(waitID, resumeAt, attempt, nil)
	intermediate := timer.WaitContinuation.ResumeAt != nil
	if !nearDue && !(e.mayHoldCoalescedTimer() && !intermediate && attempt == 0) {
		e.queue(timer)
		return
	}
	now := time.Now()
	timer.IdempotencyKey = fmt.Sprintf("wait:%s:%d:s%d", waitID, resumeAt.UnixMilli(), now.Unix())
	if !sentNearDue.remember(timer.IdempotencyKey) {
		return
	}
	if _, err := e.world.Queue(e.ctx, e.addressed(timer)); err != nil {
		sentNearDue.forget(timer.IdempotencyKey)
		panic(e.exit(deliveryError{cause: err}))
	}
}

// sentNearDue holds the near-due keys this process sent, so the re-runs of
// a busy run's deliveries do not each send the re-arms the queue would drop.
var sentNearDue = &sentKeys{limit: 10_000}

type sentKeys struct {
	mu    sync.Mutex
	limit int
	keys  map[string]bool
	order []string
}

// remember reports whether key is new, and records it.
func (s *sentKeys) remember(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.keys[key] {
		return false
	}
	if s.keys == nil {
		s.keys = map[string]bool{}
	}
	s.keys[key] = true
	s.order = append(s.order, key)
	for len(s.order) > s.limit {
		delete(s.keys, s.order[0])
		s.order = s.order[1:]
	}
	return true
}

// forget drops key, so a later delivery sends it again.
func (s *sentKeys) forget(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.keys, key)
}

// continueWaitHop queues the hop after input, an intermediate hop of a
// wait's timer, which is all its delivery does.
func (r *Runtime) continueWaitHop(ctx context.Context, input worlds.QueueInput) error {
	continuation := input.WaitContinuation
	execution := &Execution{world: r.world}
	next := execution.waitTimer(continuation.CorrelationID, *continuation.ResumeAt, continuation.Attempt, continuation.HopAt)
	next.RunID, next.WorkflowName, next.DeploymentID, next.QueueName, next.RunSpecVersion = input.RunID, input.WorkflowName, input.DeploymentID, input.QueueName, worlds.SpecVersion
	_, err := r.world.Queue(ctx, next)
	return err
}

// openWait is a wait the log records and has not completed.
type openWait struct {
	resumeAt time.Time
	creator  string
}

// openWaitsInLog returns the waits the log records that have not completed.
func (e *Execution) openWaitsInLog() map[string]openWait {
	open := map[string]openWait{}
	for _, event := range e.events {
		switch event.EventType {
		case worlds.EventTypeWaitCreated:
			var data worlds.DataWaitCreated
			if json.Unmarshal(event.Data, &data) == nil {
				open[event.CorrelationID] = openWait{resumeAt: data.ResumeAt, creator: data.CreatorMessageID}
			}
		case worlds.EventTypeWaitCompleted:
			delete(open, event.CorrelationID)
		}
	}
	return open
}

// armChainBefore queues the timer of the earliest open wait, other than
// completing, of the chain completing belongs to, before completing's
// wait_completed is recorded, so the chain stays armed if the delivery stops
// right after.
func (e *Execution) armChainBefore(completing string) {
	open := e.openWaitsInLog()
	creator := open[completing].creator
	if creator == "" {
		return
	}
	e.completedChains[creator] = true
	var next string
	for id, wait := range open {
		if id != completing && wait.creator == creator && (next == "" || wait.resumeAt.Before(open[next].resumeAt)) {
			next = id
		}
	}
	if next != "" {
		e.armWait(next, open[next].resumeAt, 0, false)
	}
}

// armTimers queues the timers the flow owns: the earliest open wait of each
// chain it owns, and the re-arm of its own timer's wait when that came
// before the wait was due. A delivery that may hold a coalesced timer first
// arms every open wait due within NearDueWaitWindow, whatever its chain.
func (e *Execution) armTimers() {
	owned := map[string]bool{e.delivery.MessageID: true}
	for chain := range e.completedChains {
		owned[chain] = true
	}
	open := e.openWaitsInLog()
	timerFor := ""
	if continuation := e.delivery.WaitContinuation; continuation != nil {
		timerFor = continuation.CorrelationID
		if wait, ok := open[timerFor]; ok {
			if e.mayHoldCoalescedTimer() {
				owned[wait.creator] = true
			}
			// The timer came before its wait was due, or the wait fell due
			// after the replay found it was not: either way this delivery
			// left it open, so it arms it again.
			e.armWait(timerFor, wait.resumeAt, continuation.Attempt+1, false)
		}
	}
	if e.mayHoldCoalescedTimer() {
		horizon := time.Now().Add(NearDueWaitWindow)
		for id, wait := range open {
			if !wait.resumeAt.After(horizon) {
				e.armWait(id, wait.resumeAt, 0, true)
			}
		}
	}
	earliest := map[string]string{}
	for id, wait := range open {
		// A recovery delivery owns every chain: the queue that held their
		// timers is gone.
		if !owned[wait.creator] && !e.recovery() || id == timerFor {
			continue
		}
		if current, ok := earliest[wait.creator]; !ok || wait.resumeAt.Before(open[current].resumeAt) {
			earliest[wait.creator] = id
		}
	}
	for _, id := range earliest {
		e.armWait(id, open[id].resumeAt, 0, false)
	}
}

// armOwnChain queues the timer of the earliest open wait of the chain of
// the delivery's own message, before a handoff that a World may continue on
// a new message, whose deliveries do not own the chain.
func (e *Execution) armOwnChain() {
	open := e.openWaitsInLog()
	earliest := ""
	for id, wait := range open {
		if wait.creator == e.delivery.MessageID && (earliest == "" || wait.resumeAt.Before(open[earliest].resumeAt)) {
			earliest = id
		}
	}
	if earliest != "" {
		e.armWait(earliest, open[earliest].resumeAt, 0, false)
	}
}

// completeWaitsOnWake completes the open waits the delivery names in
// CompleteWaits, whether or not they are due.
func (e *Execution) completeWaitsOnWake() {
	if len(e.delivery.CompleteWaits) == 0 {
		return
	}
	open := e.openWaitsInLog()
	for _, id := range e.delivery.CompleteWaits {
		if wait, ok := open[id]; ok {
			e.completeWait(pendingWait{waitID: worlds.WaitID(id), dueAt: wait.resumeAt})
		}
	}
}
