package worlds

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// SetStartHookConfirmDelays sets the waits before a start hook candidate's
// creation is sent again, and returns a function that restores them.
func SetStartHookConfirmDelays(delays ...time.Duration) (restore func()) {
	previous := startHookConfirmDelays
	startHookConfirmDelays = delays
	return func() { startHookConfirmDelays = previous }
}

// scriptedStartWorld answers a start's EventsCreateRun and Queue calls with
// the errors scripted for them, in order, and then accepts them. It
// implements no other part of a World.
type scriptedStartWorld struct {
	World
	create, queue   []error
	creates, queues int
	created, queued int
}

func (w *scriptedStartWorld) EventsCreateRun(context.Context, EventsCreateRunInput) (EventResult, error) {
	w.creates++
	if w.creates <= len(w.create) && w.create[w.creates-1] != nil {
		return EventResult{}, w.create[w.creates-1]
	}
	w.created++
	return EventResult{}, nil
}

func (w *scriptedStartWorld) Queue(_ context.Context, input QueueInput) (QueueResult, error) {
	w.queues++
	if w.queues <= len(w.queue) && w.queue[w.queues-1] != nil {
		return QueueResult{}, w.queue[w.queues-1]
	}
	if input.RunInput == nil || input.RunInput.StartHook == nil || input.IdempotencyKey != string(input.RunID) {
		return QueueResult{}, errors.New("the delivery does not carry the candidate, keyed by its run")
	}
	w.queued++
	return QueueResult{}, nil
}

func TestStartRunConfirmsAStartHookCandidatesAdmission(t *testing.T) {
	defer SetStartHookConfirmDelays(time.Millisecond, time.Millisecond, time.Millisecond)()
	ctx := context.Background()
	disabled := fmt.Errorf("%w: off", ErrStartHookDisabled)
	throttled := &ThrottleError{}
	start := func(create, queue []error) (*scriptedStartWorld, RunID, error) {
		world := &scriptedStartWorld{create: create, queue: queue}
		runID := NewRunID()
		return world, runID, StartRun(ctx, world, EventsCreateRunInput{RunID: runID, WorkflowName: "flow", StartHook: &StartHook{Token: "token"}})
	}
	// An admission that is switched off, or fails retryably, is asked for
	// again: the World answers every copy of a candidate the same.
	if world, _, err := start([]error{disabled, throttled}, nil); err != nil || world.creates != 3 || world.created != 1 || world.queues != 1 {
		t.Fatalf("start = %v after %d creations and %d sends, want it admitted on the third", err, world.creates, world.queues)
	}
	// Admission that stays off leaves the start unconfirmed, with the
	// candidate's delivery queued to be admitted later.
	var unconfirmed *StartError
	world, runID, err := start([]error{disabled, disabled, disabled, disabled}, nil)
	if !errors.As(err, &unconfirmed) || unconfirmed.RunID != runID || unconfirmed.Stage != StartStageAdmission || !errors.Is(err, ErrStartHookDisabled) || world.creates != 4 || world.queued != 1 {
		t.Fatalf("start = %v after %d creations, want a StartError for the admission", err, world.creates)
	}
	// A refusal is the candidate's outcome, returned at once.
	rejected := fmt.Errorf("%w: too old", ErrStartHookRejected)
	if world, _, err := start([]error{rejected}, nil); !errors.Is(err, ErrStartHookRejected) || errors.As(err, &unconfirmed) || world.creates != 1 {
		t.Fatalf("start of a rejected candidate = %v after %d creations", err, world.creates)
	}
	// A delivery that may not have been queued is queued again once the
	// candidate is admitted, and a send nothing can have queued fails the
	// start before the candidate is decided.
	if world, _, err := start(nil, []error{throttled}); err != nil || world.queues != 2 || world.queued != 1 {
		t.Fatalf("start = %v after %d sends, want the delivery queued again", err, world.queues)
	}
	refused := errors.New("refused")
	if world, _, err := start(nil, []error{refused}); !errors.Is(err, refused) || world.creates != 0 {
		t.Fatalf("start = %v after %d creations, want the queue's refusal", err, world.creates)
	}
	// An admitted candidate whose delivery cannot be queued, and an
	// undecided one whose delivery may not be, are unconfirmed at the queue.
	if _, _, err := start(nil, []error{throttled, throttled, throttled, throttled, throttled}); !errors.As(err, &unconfirmed) || unconfirmed.Stage != StartStageQueue {
		t.Fatalf("start = %v, want a StartError for the queue", err)
	}
	if _, _, err := start([]error{disabled, disabled, disabled, disabled}, []error{throttled}); !errors.As(err, &unconfirmed) || unconfirmed.Stage != StartStageQueue {
		t.Fatalf("start = %v, want a StartError for the queue", err)
	}
}
