package testworld

import (
	"context"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/eriicafes/go-workflow/worlds"
)

type queuedMessage struct {
	id    string
	input worlds.QueueInput
}

// QueueMessage is an inspectable durable queue item for provider workers.
// A successful worker must acknowledge it with QueueCancel.
type QueueMessage struct {
	ID    string
	Input worlds.QueueInput
}

func (w *World) Queue(ctx context.Context, input worlds.QueueInput) (worlds.QueueResult, error) {
	if err := ctx.Err(); err != nil {
		return worlds.QueueResult{}, err
	}
	w.mu.Lock()
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
	if input.NotBefore != nil {
		timer := time.NewTimer(time.Until(*input.NotBefore))
		defer timer.Stop()
		select {
		case <-w.ctx.Done():
			return
		case <-timer.C:
		}
	}
	if w.runtime.Execute(w.ctx, input) == nil {
		_ = w.QueueCancel(w.ctx, worlds.QueueCancelInput{MessageID: id})
	}
}

func (w *World) QueueCancel(ctx context.Context, input worlds.QueueCancelInput) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	defer w.notify()
	w.mu.Lock()
	defer w.mu.Unlock()
	for index, message := range w.queued {
		if message.id == input.MessageID {
			w.queued = append(w.queued[:index], w.queued[index+1:]...)
			return nil
		}
	}
	return worlds.ErrQueueMessageNotFound
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

func (w *World) Queued() []worlds.QueueInput {
	w.mu.RLock()
	defer w.mu.RUnlock()
	values := make([]worlds.QueueInput, len(w.queued))
	for index, message := range w.queued {
		values[index] = message.input
	}
	return values
}

// QueuedMessages returns queue IDs and inputs so a test driving a NewManual
// World can execute and acknowledge deliveries itself.
func (w *World) QueuedMessages() []QueueMessage {
	w.mu.RLock()
	defer w.mu.RUnlock()
	values := make([]QueueMessage, len(w.queued))
	for index, message := range w.queued {
		values[index] = QueueMessage{ID: message.id, Input: cloneQueueInput(message.input)}
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
