package worlds

import (
	"encoding/json"
	"fmt"
	"slices"
)

// DurableSetAttributesOptions holds the arguments to DurableSetAttributes.
type DurableSetAttributesOptions struct {
	Execution *Execution
	// Changes are the attribute changes in the order they are recorded.
	Changes []RunsAttributeChange
	// AllowReservedAttributes permits keys with the reserved prefix.
	AllowReservedAttributes bool
}

// DurableSetAttributes draws the next attribute write's ID, attr_ and the
// run's next correlation ULID, and records an attr_set event with Changes
// written by the workflow, or on replay matches the recorded one. It returns
// once the walk reaches the event, which does not move the replay clock. A
// step delivery that reaches an unrecorded write suspends, leaving it to a
// flow. When the event recorded with its ID is not the same write, it ends
// the replay with a panic carrying a replayDivergenceError.
func DurableSetAttributes(options DurableSetAttributesOptions) error {
	execution := options.Execution
	id := "attr_" + execution.seed.ulid()
	index, exists := execution.declared(id)
	if !exists {
		if execution.target != nil {
			panic(execution.exit(errReplaySuspended))
		}
		data, err := json.Marshal(DataAttrSet{Changes: options.Changes, Writer: AttributeWriter{Type: AttributeWriterWorkflow}, AllowReservedAttributes: options.AllowReservedAttributes})
		if err != nil {
			return err
		}
		execution.CreateEvent(EventsCreateEvent{EventType: EventTypeAttrSet, CorrelationID: id, Data: data}, nil)
		index, _ = execution.declared(id)
	}
	event := execution.events[index]
	if event.EventType != EventTypeAttrSet {
		panic(execution.exit(replayDivergenceError{CorrelationID: id, Reason: fmt.Sprintf("recorded %s, replay reached an attribute write", event.EventType)}))
	}
	var recorded DataAttrSet
	if err := json.Unmarshal(event.Data, &recorded); err != nil {
		return fmt.Errorf("workflow: decode attribute write: %w", err)
	}
	same := recorded.Writer.Type == AttributeWriterWorkflow && recorded.AllowReservedAttributes == options.AllowReservedAttributes &&
		slices.EqualFunc(recorded.Changes, options.Changes, func(a, b RunsAttributeChange) bool {
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
