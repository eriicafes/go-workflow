package sdke2e

import (
	"context"
	"errors"
	"strconv"

	workflow "github.com/eriicafes/go-workflow"
)

type setAttributesWorkflow struct {
	workflow.Struct[int]
	workflow.Args
	Input int
}

func setAttributesRun(ctx context.Context, input setAttributesWorkflow) (int, error) {
	if err := workflow.SetAttributes(ctx, attributes("phase", "init", "source", "workflow-body")); err != nil {
		return 0, err
	}
	tripled := input.Input * 3
	if err := workflow.SetAttributes(ctx, attributes("phase", "done")); err != nil {
		return 0, err
	}
	// A nil value removes the key.
	if err := workflow.SetAttributes(ctx, map[string]*string{"source": nil}); err != nil {
		return 0, err
	}
	return tripled, nil
}

type setAttributesInsideStepWorkflow struct {
	workflow.Struct[int]
	workflow.Args
	Input int
}

func setAttributesInsideStepRun(ctx context.Context, input setAttributesInsideStepWorkflow) (int, error) {
	return workflow.Step(ctx, setAttributesFromStep, input.Input)
}

type setAttributesThrowsAfterWorkflow struct{ workflow.Struct[struct{}] }

func setAttributesThrowsAfterRun(ctx context.Context, _ setAttributesThrowsAfterWorkflow) (struct{}, error) {
	if err := workflow.SetAttributes(ctx, attributes("phase", "about-to-fail", "reason", "intentional")); err != nil {
		return struct{}{}, err
	}
	return struct{}{}, workflow.FatalError(errors.New("intentional failure to test attribute persistence"))
}

func setAttributesFromStep(ctx context.Context, input int) (int, error) {
	if err := workflow.SetAttributes(ctx, attributes("phase", "step-started", "source", "step-body", "input", strconv.Itoa(input))); err != nil {
		return 0, err
	}
	if err := workflow.SetAttributes(ctx, attributes("phase", "step-done")); err != nil {
		return 0, err
	}
	return input * 4, nil
}

// attributes returns the attributes of alternating keys and values.
func attributes(pairs ...string) map[string]*string {
	attrs := map[string]*string{}
	for i := 0; i+1 < len(pairs); i += 2 {
		attrs[pairs[i]] = &pairs[i+1]
	}
	return attrs
}
