package workflow

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/core"
)

// SetAttributesOptions holds the options for SetAttributes.
type SetAttributesOptions struct {
	// AllowReservedAttributes permits keys that start with "$", which are
	// reserved for framework and library code.
	AllowReservedAttributes bool
}

// SetAttributes changes the run's plain-text attributes as one attr_set
// event. A nil value deletes its key. Changes are recorded in key order, and
// an empty map records nothing. Attributes outside the limits of
// worlds.CheckAttributes return a FatalError, the last options apply.
//
// In workflow context the write draws its ID, attr_ and the run's next
// correlation ULID, from the run's seed, so the runner must reach its writes
// in the same order on every replay. It returns once the write is recorded
// and a FatalError when the World does not store attribute writes. In step
// context it records the write for the step's attempt at once, and a World
// that does not store attribute writes ignores it. Elsewhere it returns
// ErrNoWorkflow.
func SetAttributes(ctx context.Context, attributes map[string]*string, options ...SetAttributesOptions) error {
	var option SetAttributesOptions
	if len(options) != 0 {
		option = options[len(options)-1]
	}
	execution, inWorkflow := workflowExecutionFromContext(ctx)
	stepExecution, inStep := stepExecutionFromContext(ctx)
	if !inWorkflow && !inStep {
		return ErrNoWorkflow
	}
	keys := make([]string, 0, len(attributes))
	values := make(map[string]string, len(attributes))
	for key, value := range attributes {
		keys = append(keys, key)
		values[key] = ""
		if value != nil {
			values[key] = *value
		}
	}
	if len(keys) == 0 {
		return nil
	}
	if err := worlds.CheckAttributes(values, option.AllowReservedAttributes); err != nil {
		return FatalError(err)
	}
	slices.Sort(keys)
	changes := make([]worlds.RunsAttributeChange, len(keys))
	for index, key := range keys {
		changes[index] = worlds.RunsAttributeChange{Key: key, Value: attributes[key]}
	}
	if inStep {
		step, _ := core.StepMetadataFromContext(ctx)
		return stepExecution.World().RunsSetAttributes(ctx, worlds.RunsSetAttributesInput{
			RunID:                   stepExecution.Run().RunID,
			Changes:                 changes,
			Writer:                  &worlds.AttributeWriter{Type: worlds.AttributeWriterStep, StepID: step.ID, Attempt: step.Attempt},
			AllowReservedAttributes: option.AllowReservedAttributes,
		})
	}
	if !execution.World().Capabilities().SetAttributes {
		return FatalError(fmt.Errorf("workflow: the World does not store attribute writes: %w", errors.ErrUnsupported))
	}
	return core.DurableSetAttributes(core.DurableSetAttributesOptions{Execution: execution, Changes: changes, AllowReservedAttributes: option.AllowReservedAttributes})
}
