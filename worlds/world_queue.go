package worlds

import (
	"context"
	"time"
)

// Queue stores deliveries for the runtime. Each delivery makes the runtime
// execute a flow or a step of a run through Runtime.Execute.
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
	// QueueInput holds the arguments to Queue and describes one delivery.
	QueueInput struct {
		RunID RunID
		// QueueName selects a queue in implementations that have more than
		// one. Empty selects the default queue.
		QueueName string
		// StepID selects the step a step delivery executes. Nil makes the
		// delivery a flow.
		StepID *StepID
		// NotBefore is the earliest time the delivery may be made. Nil
		// allows immediate delivery.
		NotBefore *time.Time
		// IdempotencyKey identifies retries of the same delivery to
		// implementations that deduplicate publishing.
		IdempotencyKey string
		// DeploymentID addresses the delivery to a deployment, normally the
		// one the run is pinned to. Worlds with deployment affinity deliver it
		// there; other Worlds ignore it. Empty uses the World's own deployment.
		DeploymentID string
		// DeploymentMismatchRetryCount counts how many times the Runtime has
		// re-routed this delivery to the run's deployment after it arrived at
		// another one.
		DeploymentMismatchRetryCount int
		// ReplayDivergenceCount counts how many times the Runtime has queued
		// this delivery again because its replay diverged from the run's
		// event log.
		ReplayDivergenceCount int
		// MessageID identifies the queued message a World delivers, as
		// QueueResult reported it, and stays the same when the message is
		// redelivered. The World sets it before Runtime.Execute, and Queue
		// ignores it.
		MessageID string
	}
	// QueueBatchInput holds the arguments to QueueBatch.
	QueueBatchInput struct {
		Items []QueueInput
	}
	// QueueCancelInput holds the arguments to QueueCancel.
	QueueCancelInput struct {
		MessageID string
	}
	// QueueResult is the result of Queue for one delivery.
	QueueResult struct {
		MessageID string
	}
)
