package core

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/eriicafes/go-workflow/worlds"
)

// DurableSetAttributesOptions holds the arguments to DurableSetAttributes.
type DurableSetAttributesOptions struct {
	Execution *Execution
	// Changes are the attribute changes in the order they are recorded.
	Changes []worlds.RunsAttributeChange
	// AllowReservedAttributes permits keys with the reserved prefix.
	AllowReservedAttributes bool
}

// worldValidationFailure reports whether err is a World refusing an event
// for what it holds, which the same event meets on every attempt.
func worldValidationFailure(err error) bool {
	return errors.Is(err, worlds.ErrAttributeInvalid) || errors.Is(err, worlds.ErrEventInvalid)
}

// DurableSetAttributes draws the next attribute write's ID, attr_ and the
// run's next correlation ULID, and records an attr_set event with Changes
// written by the workflow, or on replay matches the recorded one. It returns
// once the walk reaches the event, which does not move the replay clock. A
// step delivery that reaches an unrecorded write suspends, leaving it to a
// flow. When the event recorded with its ID is not the same write, it ends
// the replay with a panic carrying a replayDivergenceError. A write the World
// refuses as invalid returns a FatalError.
func DurableSetAttributes(options DurableSetAttributesOptions) error {
	execution := options.Execution
	id := "attr_" + execution.seed.ulid()
	index, exists := execution.declared(id)
	if !exists {
		if execution.target != nil {
			panic(execution.exit(errReplaySuspended))
		}
		data, err := json.Marshal(worlds.DataAttrSet{Changes: options.Changes, Writer: worlds.AttributeWriter{Type: worlds.AttributeWriterWorkflow}, AllowReservedAttributes: options.AllowReservedAttributes})
		if err != nil {
			return err
		}
		var refusal error
		execution.CreateEvent(worlds.EventsCreateEvent{EventType: worlds.EventTypeAttrSet, CorrelationID: id, Data: data}, func(err error) error {
			if worldValidationFailure(err) {
				refusal = err
				return nil
			}
			return err
		})
		if refusal != nil {
			// The World refuses the write whenever it is made, such as one
			// past the run's attribute limit, which only the World can
			// check. A redelivery would replay into the same refusal, so the
			// workflow gets it as an error that fails the run.
			return worlds.FatalError{Cause: fmt.Errorf("workflow: SetAttributes failed World validation: %w", refusal)}
		}
		index, _ = execution.declared(id)
	}
	event := execution.events[index]
	if event.EventType != worlds.EventTypeAttrSet {
		panic(execution.exit(replayDivergenceError{CorrelationID: id, Reason: fmt.Sprintf("recorded %s, replay reached an attribute write", event.EventType)}))
	}
	var recorded worlds.DataAttrSet
	if err := json.Unmarshal(event.Data, &recorded); err != nil {
		return fmt.Errorf("workflow: decode attribute write: %w", err)
	}
	same := recorded.Writer.Type == worlds.AttributeWriterWorkflow && recorded.AllowReservedAttributes == options.AllowReservedAttributes &&
		slices.EqualFunc(recorded.Changes, options.Changes, func(a, b worlds.RunsAttributeChange) bool {
			return a.Key == b.Key && (a.Value == nil) == (b.Value == nil) && (a.Value == nil || *a.Value == *b.Value)
		})
	if !same {
		panic(execution.exit(replayDivergenceError{CorrelationID: id, Reason: "recorded attribute write differs from the one replay reached"}))
	}
	for index >= execution.horizon {
		execution.Wait()
	}
	return nil
}
