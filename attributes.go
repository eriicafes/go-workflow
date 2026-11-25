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
// an empty map records nothing, and a nil map returns a FatalError.
// Attributes outside the limits of worlds.CheckAttributeChanges, or an
// attr_set over worlds.MaxAttrSetDataBytes, return a FatalError with the
// SDK's message. The last options apply.
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
	if attributes == nil {
		return FatalError(errors.New("SetAttributes requires a mapping, got nil"))
	}
	keys := make([]string, 0, len(attributes))
	for key := range attributes {
		keys = append(keys, key)
	}
	if len(keys) == 0 {
		return nil
	}
	slices.Sort(keys)
	changes := make([]worlds.RunsAttributeChange, len(keys))
	for index, key := range keys {
		changes[index] = worlds.RunsAttributeChange{Key: key, Value: attributes[key]}
	}
	// The run's attributes are not known here, so only the attributes set
	// count towards the limit: a removal adds none.
	if err := worlds.CheckAttributeChanges(nil, changes, option.AllowReservedAttributes); err != nil {
		return FatalError(err)
	}
	input := worlds.RunsSetAttributesInput{Changes: changes, AllowReservedAttributes: option.AllowReservedAttributes}
	if inStep {
		step, _ := core.StepMetadataFromContext(ctx)
		input.RunID = stepExecution.Run().RunID
		input.Writer = &worlds.AttributeWriter{Type: worlds.AttributeWriterStep, StepID: step.ID, Attempt: step.Attempt}
	}
	data, err := worlds.EncodeAttrSet(input)
	if err != nil {
		return err
	}
	if err := worlds.CheckAttrSet(nil, data); err != nil {
		return FatalError(err)
	}
	if inStep {
		return stepExecution.World().RunsSetAttributes(ctx, input)
	}
	if !execution.World().Capabilities().SetAttributes {
		return FatalError(fmt.Errorf("workflow: the World does not store attribute writes: %w", errors.ErrUnsupported))
	}
	return core.DurableSetAttributes(core.DurableSetAttributesOptions{Execution: execution, Changes: changes, AllowReservedAttributes: option.AllowReservedAttributes})
}
