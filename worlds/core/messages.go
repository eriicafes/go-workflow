package core

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
	"unicode/utf16"

	"github.com/eriicafes/go-workflow/worlds"
)

// resendMaxAge is how old a step's last send may be for a flow to send its
// message again: an hour inside the day every World deduplicates a step's
// message key for, so a re-send is deduplicated against the step's first
// message while that message lives, and never makes a second one.
const resendMaxAge = 23 * time.Hour

// stepMessage returns the step delivery of attempt of the step stepID named
// name, keyed by the step's identity, so every send of one step's message is
// one message to a World that deduplicates. The message of a first attempt
// carries createdAt, when the step was created, and a retry's does not.
func stepMessage(stepID worlds.StepID, name string, attempt int, resent bool, createdAt time.Time) worlds.QueueInput {
	input := worlds.QueueInput{StepID: &stepID, StepName: name, IdempotencyKey: StepMessageKey(stepID, name), StepResent: resent, Retention: worlds.StepRetention}
	if attempt > 1 {
		input.StepAttempt = attempt
	} else {
		input.StepCreatedAt = createdAt
	}
	return input
}

// stepDispatchBatchSize is the most step messages one QueueBatch carries.
const stepDispatchBatchSize = 16

// queueSteps queues step messages in batches of at most
// stepDispatchBatchSize, sent at the same time. Every message must be
// stored: when one is not, the replay ends once every batch has settled, so
// the queue retries the delivery, whose flow sends the messages again under
// their keys.
func (e *Execution) queueSteps(messages []worlds.QueueInput) {
	if len(messages) == 0 {
		return
	}
	for index := range messages {
		messages[index] = e.addressed(messages[index])
	}
	failures := make([]error, (len(messages)+stepDispatchBatchSize-1)/stepDispatchBatchSize)
	var sending sync.WaitGroup
	for batch := range failures {
		start := batch * stepDispatchBatchSize
		items := messages[start:min(start+stepDispatchBatchSize, len(messages))]
		sending.Go(func() {
			_, failures[batch] = worlds.QueueAll(e.ctx, e.world, items)
		})
	}
	sending.Wait()
	for _, err := range failures {
		if err != nil {
			panic(e.exit(deliveryError{cause: err}))
		}
	}
}

// stepCreatedAt returns when the step_created of step stepID among events
// was stored, or the zero time when the step has none.
func stepCreatedAt(events []worlds.Event, stepID worlds.StepID) time.Time {
	for _, event := range events {
		if event.EventType == worlds.EventTypeStepCreated && event.CorrelationID == string(stepID) {
			return event.CreatedAt
		}
	}
	return time.Time{}
}

// StepMessageKey returns the idempotency key of the message of step stepID
// named name: the step ID and the FNV-1a hash of the name's UTF-16 code
// units, in hexadecimal.
func StepMessageKey(stepID worlds.StepID, name string) string {
	hash := uint32(0x811c9dc5)
	for _, unit := range utf16.Encode([]rune(name)) {
		hash ^= uint32(unit)
		hash *= 0x01000193
	}
	return fmt.Sprintf("%s:%08x", stepID, hash)
}

// resendSteps queues again, once per delivery, the step deliveries a
// delivery that stopped may have recorded and not queued: a step created as
// a step delivery that has not started, and an inline step whose last event
// is the step_retrying a flow handed its retry to a step delivery with. A
// flow's first delivery queues them itself, so only a redelivery, or a
// delivery of a World that does not count deliveries, re-sends. A step last
// sent more than resendMaxAge ago is left alone, since a second message past
// the World's deduplication would be a second owner of its attempts.
//
// A recovery delivery, of a World that lost its messages, sends again the
// message of every unfinished step that runs from one, started or not and
// whatever its age: the messages it lost.
func (e *Execution) resendSteps() {
	recovery := e.recovery()
	if e.delivery.DeliveryCount == 1 && !recovery {
		return
	}
	type state struct {
		name      string
		createdAt time.Time
		inline    bool
		started   bool
		finished  bool
		sentAt    time.Time
		attempt   int
		retryAt   *time.Time
		handedOff bool
		retried   bool
	}
	var order []worlds.StepID
	steps := map[worlds.StepID]*state{}
	for _, event := range e.events {
		id := worlds.StepID(event.CorrelationID)
		step := steps[id]
		switch event.EventType {
		case worlds.EventTypeStepCreated:
			if step == nil {
				var data worlds.DataStepCreated
				_ = json.Unmarshal(event.Data, &data)
				steps[id], order = &state{name: data.StepName, createdAt: event.CreatedAt, inline: data.Inline, sentAt: event.CreatedAt}, append(order, id)
			}
		case worlds.EventTypeStepStarted:
			if step == nil && worlds.StartCreatesStep(event.Data) {
				var data worlds.DataStepStarted
				_ = json.Unmarshal(event.Data, &data)
				step = &state{name: data.StepName, inline: true}
				steps[id], order = step, append(order, id)
			}
			if step != nil {
				step.started, step.handedOff, step.retryAt = true, false, nil
				step.attempt++
			}
		case worlds.EventTypeStepRetrying:
			if step != nil {
				var data worlds.DataStepResult
				_ = json.Unmarshal(event.Data, &data)
				step.handedOff, step.sentAt, step.retryAt = data.CreatorMessageID != "", event.CreatedAt, data.RetryAfter
				step.retried = true
			}
		case worlds.EventTypeStepCompleted, worlds.EventTypeStepFailed:
			if step != nil {
				step.finished = true
			}
		}
	}
	var messages []worlds.QueueInput
	for _, id := range order {
		step := steps[id]
		if step.finished {
			continue
		}
		if recovery {
			if step.inline && !step.retried {
				continue
			}
		} else if resend := !step.inline && !step.started || step.inline && step.handedOff; !resend || time.Since(step.sentAt) >= resendMaxAge {
			continue
		}
		message := stepMessage(id, step.name, step.attempt+1, true, step.createdAt)
		message.NotBefore = step.retryAt
		messages = append(messages, message)
	}
	e.queueSteps(messages)
}

// recovery reports whether the delivery is a flow a World queued after it
// lost its messages.
func (e *Execution) recovery() bool {
	return e.delivery.Recovery && e.delivery.StepID == nil
}
