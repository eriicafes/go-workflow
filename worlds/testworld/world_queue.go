package testworld

import (
	"context"
	"errors"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/eriicafes/go-workflow/worlds"
)

type queuedMessage struct {
	id    string
	input worlds.QueueInput
}

// QueueMessage is a delivery that has not been acknowledged. A test that
// executes it acknowledges it by passing ID to Acknowledge.
type QueueMessage struct {
	ID    string
	Input worlds.QueueInput
}

func (w *World) Queue(ctx context.Context, input worlds.QueueInput) (worlds.QueueResult, error) {
	if err := ctx.Err(); err != nil {
		return worlds.QueueResult{}, err
	}
	w.mu.Lock()
	if input.IdempotencyKey != "" {
		// A message with a key is stored once while it lives.
		for _, message := range w.queued {
			if message.input.IdempotencyKey == input.IdempotencyKey {
				w.mu.Unlock()
				return worlds.QueueResult{MessageID: message.id}, nil
			}
		}
	}
	id := ulid.Make().String()
	w.queued = append(w.queued, queuedMessage{id: id, input: input})
	w.mu.Unlock()
	w.notify()
	if w.autoRun {
		go w.deliver(id, input)
	}
	return worlds.QueueResult{MessageID: id}, nil
}

func (w *World) deliver(id string, input worlds.QueueInput) {
	input.MessageID = id
	wait := func(until *time.Time) bool {
		if until == nil {
			return true
		}
		timer := time.NewTimer(time.Until(*until))
		defer timer.Stop()
		select {
		case <-w.ctx.Done():
			return false
		case <-timer.C:
			return true
		}
	}
	if !wait(input.NotBefore) {
		return
	}
	// A failed delivery is retried a few times, as a real queue redelivers
	// it, and a delivery that asks for its message again gets it.
	failures := 0
	for count := 1; count <= maxDeliveries; count++ {
		input.DeliveryCount = count
		input.Attempt = count - w.handoffs(id)
		err := w.execute(input)
		if err == nil {
			_ = w.Acknowledge(w.ctx, id)
			return
		}
		if redelivery, ok := worlds.RedeliveryOf(err); ok {
			if redelivery.After == 0 {
				w.addHandoff(id)
			}
			until := time.Now().Add(redelivery.After)
			if !wait(&until) {
				return
			}
			continue
		}
		if failures++; failures >= deliveryAttempts {
			return
		}
		until := time.Now().Add(time.Duration(failures) * 10 * time.Millisecond)
		if !wait(&until) {
			return
		}
	}
}

// execute executes input, one flow of a run at a time: a flow message waits
// for the run's flow that is executing to finish. Step messages do not wait.
func (w *World) execute(input worlds.QueueInput) error {
	if input.StepID == nil {
		gate := w.flowGate(input.RunID)
		select {
		case gate <- struct{}{}:
		case <-w.ctx.Done():
			return w.ctx.Err()
		}
		defer func() { <-gate }()
	}
	return w.runtime.Execute(w.ctx, input)
}

// flowGate returns the gate that lets one flow of runID execute at a time.
func (w *World) flowGate(runID worlds.RunID) chan struct{} {
	w.mu.Lock()
	defer w.mu.Unlock()
	gate, ok := w.flowGates[runID]
	if !ok {
		gate = make(chan struct{}, 1)
		w.flowGates[runID] = gate
	}
	return gate
}

const (
	// maxDeliveries caps how often an automatic World delivers one message.
	maxDeliveries = 200
	// deliveryAttempts is how often an automatic World executes a delivery
	// that fails.
	deliveryAttempts = 3
)

func (w *World) handoffs(id string) int {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.handedOff[id]
}

func (w *World) addHandoff(id string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.handedOff[id]++
}

// Redeliver puts back the delivery identified by id, which a test using a
// NewManual World calls when executing it returned a worlds.Redelivery: the
// message moves to the back of the queue, due after the redelivery's delay,
// with its DeliveryCount raised, and its Attempt raised unless the
// redelivery is a handoff. It returns ErrMessageNotFound when no such
// delivery is queued.
func (w *World) Redeliver(ctx context.Context, id string, redelivery *worlds.Redelivery) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	defer w.notify()
	w.mu.Lock()
	defer w.mu.Unlock()
	for index, message := range w.queued {
		if message.id != id {
			continue
		}
		w.queued = append(w.queued[:index], w.queued[index+1:]...)
		message.input.DeliveryCount = max(message.input.DeliveryCount, 1) + 1
		message.input.Attempt = max(message.input.Attempt, 1)
		if redelivery.After > 0 {
			message.input.Attempt++
			due := time.Now().Add(redelivery.After)
			message.input.NotBefore = &due
		}
		w.queued = append(w.queued, message)
		return nil
	}
	return ErrMessageNotFound
}

// ErrMessageNotFound is returned by Acknowledge for a message the World does
// not hold.
var ErrMessageNotFound = errors.New("testworld: queue message not found")

// Acknowledge removes the delivery identified by id, which a test using a
// NewManual World calls once it has executed the delivery. It returns
// ErrMessageNotFound when no such delivery is queued.
func (w *World) Acknowledge(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	defer w.notify()
	w.mu.Lock()
	defer w.mu.Unlock()
	for index, message := range w.queued {
		if message.id == id {
			w.queued = append(w.queued[:index], w.queued[index+1:]...)
			return nil
		}
	}
	return ErrMessageNotFound
}

func (w *World) QueueBatch(ctx context.Context, input worlds.QueueBatchInput) ([]worlds.QueueResult, error) {
	result := make([]worlds.QueueResult, 0, len(input.Items))
	for _, item := range input.Items {
		value, err := w.Queue(ctx, item)
		if err != nil {
			return result, err
		}
		result = append(result, value)
	}
	return result, nil
}

// Queued returns the inputs of the deliveries not yet acknowledged, in the
// order they were queued.
func (w *World) Queued() []worlds.QueueInput {
	w.mu.RLock()
	defer w.mu.RUnlock()
	values := make([]worlds.QueueInput, len(w.queued))
	for index, message := range w.queued {
		values[index] = message.input
	}
	return values
}

// QueuedMessages returns the deliveries not yet acknowledged, in the order they
// were queued, so a test using a NewManual World can execute and acknowledge
// them itself.
func (w *World) QueuedMessages() []QueueMessage {
	w.mu.RLock()
	defer w.mu.RUnlock()
	values := make([]QueueMessage, len(w.queued))
	for index, message := range w.queued {
		input := cloneQueueInput(message.input)
		input.MessageID = message.id
		input.DeliveryCount = max(input.DeliveryCount, 1)
		input.Attempt = max(input.Attempt, 1)
		values[index] = QueueMessage{ID: message.id, Input: input}
	}
	return values
}

func cloneQueueInput(value worlds.QueueInput) worlds.QueueInput {
	if value.StepID != nil {
		stepID := *value.StepID
		value.StepID = &stepID
	}
	if value.NotBefore != nil {
		notBefore := *value.NotBefore
		value.NotBefore = &notBefore
	}
	return value
}
