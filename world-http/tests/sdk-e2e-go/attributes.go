package sdke2e

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	workflow "github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/worlds"
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

type setAttributesParallelWorkflow struct{ workflow.Struct[string] }

// A Go workflow body runs one thing at a time, so the concurrent writes of
// disjoint keys come from steps that run at once.
func setAttributesParallelRun(ctx context.Context, _ setAttributesParallelWorkflow) (string, error) {
	writes := workflow.Map(ctx, setAttributeFromStep, [][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}})
	if err := workflow.Join(ctx, workflow.MapResults(writes)...); err != nil {
		return "", err
	}
	return "done", nil
}

type setAttributesValidationWorkflow struct {
	workflow.Struct[map[string]string]
}

// A Go attribute map cannot be a non-object, so the nonObject case has no
// counterpart.
func setAttributesValidationRun(ctx context.Context, _ setAttributesValidationWorkflow) (map[string]string, error) {
	outcomes := map[string]string{}
	overCap := map[string]*string{}
	for i := 0; i <= 64; i++ {
		overCap[fmt.Sprintf("k%d", i)] = attributes("k", "v")["k"]
	}
	cases := []struct {
		label string
		attrs map[string]*string
	}{
		{"reserved", attributes("$system", "nope")},
		{"emptyKey", attributes("", "v")},
		{"keyTooLong", attributes(strings.Repeat("k", 257), "v")},
		{"valueTooLong", attributes("note", strings.Repeat("v", 257))},
		{"valueTooManyBytes", attributes("note", strings.Repeat("é", 200))},
		{"overCap", overCap},
	}
	for _, c := range cases {
		err := workflow.SetAttributes(ctx, c.attrs)
		switch {
		case err == nil:
			outcomes[c.label] = "no-error"
		case errors.As(err, new(worlds.FatalError)):
			outcomes[c.label] = "FatalError: " + err.Error()
		default:
			outcomes[c.label] = "Error: " + err.Error()
		}
	}
	// The run stays healthy after every rejected call.
	if err := workflow.SetAttributes(ctx, attributes("phase", "validated")); err != nil {
		return nil, err
	}
	return outcomes, nil
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

func setAttributeFromStep(ctx context.Context, pair [2]string) (struct{}, error) {
	return struct{}{}, workflow.SetAttributes(ctx, attributes(pair[0], pair[1]))
}

// attributes returns the attributes of alternating keys and values.
func attributes(pairs ...string) map[string]*string {
	attrs := map[string]*string{}
	for i := 0; i+1 < len(pairs); i += 2 {
		attrs[pairs[i]] = &pairs[i+1]
	}
	return attrs
}
