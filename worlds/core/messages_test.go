package core

import (
	"context"
	"encoding/json"
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
