package worlds

import (
	"context"
	"time"
)

type Queue interface {
	// Queue stores the delivery described by QueueInput and returns its message
	// ID. It does not deliver an item before NotBefore. QueueName lets an
	// implementation select a queue. IdempotencyKey identifies a retry for
	// implementations that support deduplicated publishing. Delivery is at least
	// once and Queue does not promise to deduplicate calls.
	Queue(context.Context, QueueInput) (QueueResult, error)
	// QueueBatch stores the supplied deliveries and returns their message IDs. It
	// may return results for items stored before an error.
	QueueBatch(context.Context, QueueBatchInput) ([]QueueResult, error)
	// QueueCancel removes the delivery identified by MessageID. It returns
	// ErrQueueMessageNotFound when the delivery does not exist.
	QueueCancel(context.Context, QueueCancelInput) error
}

type (
	QueueInput struct {
		QueueName      string
		RunID          RunID
		StepID         *StepID
		NotBefore      *time.Time
		IdempotencyKey string
	}
	QueueBatchInput struct {
		Items []QueueInput
	}
	QueueCancelInput struct {
		MessageID string
	}
	QueueResult struct {
		MessageID string
	}
)
