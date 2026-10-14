package worlds

import (
	"context"
	"time"
)

type Queue interface {
	Queue(context.Context, QueueInput) (QueueResult, error)
	QueueBatch(context.Context, QueueBatchInput) ([]QueueResult, error)
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
