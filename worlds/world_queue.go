package worlds

import (
	"context"
	"errors"
	"fmt"
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
}

type (
	// QueueInput holds the arguments to Queue and describes one delivery.
	QueueInput struct {
		RunID RunID
		// QueueName selects a queue in implementations that have more than
		// one. Empty selects the default queue.
		QueueName string
		// WorkflowName is the workflow of the run the delivery belongs to,
		// so a World can route the run's deliveries to a queue of its
		// choosing. The Runtime and Client set it on every delivery they
		// queue for a run. Worlds that don't route ignore it.
		WorkflowName string
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
		// Attempt is the 1-based number of this delivery of the message,
		// which the World sets before Runtime.Execute and Queue ignores. A
		// handoff (a Redeliver of zero) does not count. Zero means the World
		// does not count deliveries. Past MaxDeliveries, Runtime.Execute
		// fails the delivery's run or step instead of executing it.
		Attempt int
		// DeliveryCount is the 1-based number of this delivery of the
		// message, which rises on every redelivery, handoffs included. The
		// World sets it before Runtime.Execute and Queue ignores it. Zero
		// means the World does not count deliveries, which the Runtime
		// treats as a possible redelivery.
		DeliveryCount int
		// StepName is the name of the step a step delivery executes.
		StepName string
		// StepAttempt is the attempt a step delivery is queued for, when it
		// is above 1: an inline step's retry handed to a step delivery.
		StepAttempt int
		// StepResent marks a step delivery queued again by a flow that could
		// not tell whether the step's first message was sent. Its delivery
		// reads the log before it runs the step.
		StepResent bool
	}
	// QueueBatchInput holds the arguments to QueueBatch.
	QueueBatchInput struct {
		Items []QueueInput
	}
	// QueueResult is the result of Queue for one delivery.
	QueueResult struct {
		MessageID string
	}
)

// Redelivery is the result of a delivery that asks its World to deliver the
// same message again, with the same MessageID and a higher DeliveryCount,
// after After. Runtime.Execute returns it as its error, and the World does
// not acknowledge the message. A Redelivery of zero is a handoff: the
// message is delivered again at once, and the redelivery does not count as
// an Attempt. Any other Redelivery counts as one.
type Redelivery struct {
	After time.Duration
}

func (r *Redelivery) Error() string {
	return fmt.Sprintf("workflow: redeliver after %s", r.After)
}

// Redeliver returns a Redelivery of after, zero or more.
func Redeliver(after time.Duration) error {
	return &Redelivery{After: max(after, 0)}
}

// RedeliveryOf reports the Redelivery err carries.
func RedeliveryOf(err error) (*Redelivery, bool) {
	var redelivery *Redelivery
	ok := errors.As(err, &redelivery)
	return redelivery, ok
}
