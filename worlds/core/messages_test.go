package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

func TestStepMessageKeyHashesTheNamesUTF16Units(t *testing.T) {
	// The FNV-1a hash of the name's UTF-16 code units.
	if got := StepMessageKey("step_1", "a"); got != "step_1:e40c292c" {
		t.Fatalf("key = %q", got)
	}
	if got := StepMessageKey("step_1", ""); got != "step_1:811c9dc5" {
		t.Fatalf("key of an empty name = %q", got)
	}
}

func TestRecoveryDeliveryResendsEveryStepThatRunsFromItsOwnMessage(t *testing.T) {
	now := time.Now()
	event := func(eventType worlds.EventType, id string, at time.Time, data any) worlds.Event {
		encoded, _ := json.Marshal(data)
		return worlds.Event{EventType: eventType, CorrelationID: id, CreatedAt: at, Data: encoded}
	}
	old := now.Add(-48 * time.Hour)
	events := []worlds.Event{
		event(worlds.EventTypeStepCreated, "step_old", old, worlds.DataStepCreated{StepName: "old"}),
		event(worlds.EventTypeStepCreated, "step_started", now, worlds.DataStepCreated{StepName: "started"}),
		event(worlds.EventTypeStepStarted, "step_started", now, worlds.DataStepStarted{Attempt: 1}),
		event(worlds.EventTypeStepStarted, "step_inline", now, worlds.DataStepStarted{StepName: "inline", Attempt: 1, Inline: true}),
		event(worlds.EventTypeStepStarted, "step_retried", now, worlds.DataStepStarted{StepName: "retried", Attempt: 1, Inline: true}),
		event(worlds.EventTypeStepRetrying, "step_retried", now, worlds.DataStepResult{CreatorMessageID: "msg"}),
		event(worlds.EventTypeStepCreated, "step_done", now, worlds.DataStepCreated{StepName: "done"}),
		event(worlds.EventTypeStepCompleted, "step_done", now, worlds.DataStepResult{}),
	}
	resent := func(delivery worlds.QueueInput) map[worlds.StepID]int {
		world := &queueRecorder{}
		e := &Execution{ctx: context.Background(), world: world, events: events, delivery: delivery}
		e.resendSteps()
		sent := map[worlds.StepID]int{}
		for _, input := range world.queued {
			sent[*input.StepID] = input.StepAttempt
		}
		return sent
	}
	// A redelivery re-sends what may not have been sent within the day.
	if sent := resent(worlds.QueueInput{DeliveryCount: 2}); len(sent) != 1 || sent["step_retried"] != 2 {
		t.Fatalf("a redelivery resent %v, want step_retried", sent)
	}
	// A recovery delivery, even a first, re-sends every step that runs from
	// its own message, the started one with its next attempt.
	sent := resent(worlds.QueueInput{DeliveryCount: 1, Recovery: true})
	if _, ok := sent["step_old"]; !ok || len(sent) != 3 || sent["step_started"] != 2 || sent["step_retried"] != 2 {
		t.Fatalf("a recovery delivery resent %v, want step_old, step_started, and step_retried", sent)
	}
}

func TestStepMessageCarriesTheStepsCreationOnItsFirstAttemptOnly(t *testing.T) {
	created := time.Now().Add(-time.Minute)
	if message := stepMessage("step_1", "name", 1, false, created); !message.StepCreatedAt.Equal(created) || message.StepAttempt != 0 {
		t.Fatalf("first attempt's message = %+v, want the step's creation time", message)
	}
	if message := stepMessage("step_1", "name", 2, false, created); !message.StepCreatedAt.IsZero() || message.StepAttempt != 2 {
		t.Fatalf("a retry's message = %+v, want no creation time", message)
	}
	// A flow that sends a step's message again takes the time from the
	// step_created it reads.
	data, _ := json.Marshal(worlds.DataStepCreated{StepName: "name"})
	world := &queueRecorder{}
	e := &Execution{ctx: context.Background(), world: world, delivery: worlds.QueueInput{DeliveryCount: 2}, events: []worlds.Event{
		{EventType: worlds.EventTypeStepCreated, CorrelationID: "step_1", CreatedAt: created, Data: data},
	}}
	e.resendSteps()
	if len(world.queued) != 1 || !world.queued[0].StepCreatedAt.Equal(created) || !world.queued[0].StepResent {
		t.Fatalf("resent = %+v, want the step's message with its creation time", world.queued)
	}
}

// batchRecorder is a World whose QueueBatch records each batch's size and
// refuses the message of the step refuse names.
type batchRecorder struct {
	worlds.World
	mu     sync.Mutex
	sizes  []int
	refuse worlds.StepID
}

func (b *batchRecorder) QueueBatch(_ context.Context, input worlds.QueueBatchInput) ([]worlds.QueueResult, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.sizes = append(b.sizes, len(input.Items))
	results := make([]worlds.QueueResult, len(input.Items))
	for index, item := range input.Items {
		if b.refuse != "" && *item.StepID == b.refuse {
			results[index] = worlds.QueueResult{Err: errors.New("refused"), Retryable: true}
		}
	}
	return results, nil
}

func TestQueueStepsSendsBatchesAndEndsTheDeliveryWhenAMessageIsRefused(t *testing.T) {
	messages := func() []worlds.QueueInput {
		var messages []worlds.QueueInput
		for index := range 2*stepDispatchBatchSize + 1 {
			messages = append(messages, stepMessage(worlds.StepID(fmt.Sprintf("step_%d", index)), "name", 1, false, time.Time{}))
		}
		return messages
	}
	world := &batchRecorder{}
	e := &Execution{ctx: context.Background(), world: world, run: worlds.Run{RunID: "wrun_1", WorkflowName: "flow"}}
	e.queueSteps(messages())
	slices.Sort(world.sizes)
	if !slices.Equal(world.sizes, []int{1, stepDispatchBatchSize, stepDispatchBatchSize}) {
		t.Fatalf("batch sizes = %v, want two full batches and the rest", world.sizes)
	}
	// One refused message ends the delivery, after every batch was sent, so
	// its redelivery sends the messages again.
	world.sizes, world.refuse = nil, "step_3"
	var failed *worlds.QueueBatchError
	func() {
		defer func() {
			err := replayExit(recover())
			if !errors.As(err, &failed) {
				t.Fatalf("queueSteps ended with %v, want the batch's error", err)
			}
		}()
		e.queueSteps(messages())
	}()
	if failed.Failed != 1 || failed.Total != stepDispatchBatchSize || !failed.Retryable || len(world.sizes) != 3 {
		t.Fatalf("failure = %+v after %d batches", failed, len(world.sizes))
	}
}
