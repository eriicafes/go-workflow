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
	// implementation select a queue. Delivery is at least once.
	//
	// A message with an IdempotencyKey is stored once: a Queue with the key
	// of a message that lives, sent within MinDeduplicationWindow of its
	// first, stores nothing more and may return either message's ID.
	//
	// A delivery whose Runtime.Execute returns a *Redelivery is delivered
	// again, with the same MessageID and the next DeliveryCount. A World
	// whose Capabilities report MaxConcurrency delivers one serialized flow
	// of a run at a time (see QueueInput.SerializedFlow).
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
		// RunSpecVersion is the spec version of the run the delivery
		// belongs to, SpecVersion.
		RunSpecVersion int
		// CompleteWaits holds the waits a flow that wakes the run completes
		// first, whether or not they are due. See WakeRun.
		CompleteWaits []string
		// Recovery marks a flow a World queued after it lost the messages
		// it held. The flow sends again the message of every step that
		// runs from one, started or not, and arms the timer of every open
		// wait.
		Recovery bool
		// WaitContinuation marks a timer of a wait. See WaitContinuation.
		WaitContinuation *WaitContinuation
		// Retention is how long the message may live, redeliveries
		// included. A World caps it at what its queue supports, and zero
		// uses its default.
		Retention time.Duration
	}
	// WaitContinuation marks a timer of the wait CorrelationID. A wait
	// further away than the World's MaxQueueDelay is reached in hops,
	// anchored on its deadline: the last hop is due at ResumeAt, and each
	// earlier one MaxQueueDelay before the next. An intermediate hop has
	// ResumeAt and HopAt set, and its delivery only queues the next hop: it
	// is never held back for another flow of the run. The last hop, without
	// them, is an ordinary flow, which completes the wait. Attempt counts the
	// timers that came before the wait was due.
	WaitContinuation struct {
		CorrelationID string
		Attempt       int
		ResumeAt      *time.Time
		HopAt         *time.Time
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

// Intermediate reports whether input is an intermediate hop of a wait's
// timer, which a World delivers without waiting for another flow of the run.
func (input QueueInput) Intermediate() bool {
	return input.WaitContinuation != nil && input.WaitContinuation.ResumeAt != nil
}

// SerializedFlow reports whether a World with one flow of a run at a time
// holds input back while another flow of its run executes: every flow, but
// not a step delivery or an intermediate hop of a wait's timer.
func (input QueueInput) SerializedFlow() bool {
	return input.StepID == nil && !input.Intermediate()
}

const (
	// StepRetention is how long the message of a step that may retry
	// lives, and StepRetentionOnce how long one of a step that does not.
	StepRetention     = 7 * 24 * time.Hour
	StepRetentionOnce = 24 * time.Hour
	// MinDeduplicationWindow is the shortest time a World deduplicates a
	// message's IdempotencyKey for, from the message's first send, while
	// the message lives.
	MinDeduplicationWindow = 24 * time.Hour
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
