package sdke2e

import (
	"context"
	"errors"
	"time"

	workflow "github.com/eriicafes/go-workflow"
)

type loopTimestamps struct {
	Timestamps   []int64 `json:"timestamps"`
	TotalElapsed int64   `json:"totalElapsed"`
}

type sleepInLoopWorkflow struct {
	workflow.Struct[loopTimestamps]
}

func sleepInLoopRun(ctx context.Context, _ sleepInLoopWorkflow) (loopTimestamps, error) {
	const iterations = 3
	var timestamps []int64
	for i := range iterations {
		result, err := workflow.Step(ctx, noopStep, i)
		if err != nil {
			return loopTimestamps{}, err
		}
		timestamps = append(timestamps, result.Ts)
		if i < iterations-1 {
			if err := workflow.Sleep(ctx, 3*time.Second); err != nil {
				return loopTimestamps{}, err
			}
		}
	}
	return loopTimestamps{Timestamps: timestamps, TotalElapsed: timestamps[len(timestamps)-1] - timestamps[0]}, nil
}

type promiseRaceStressTestWorkflow struct{ workflow.Struct[[]int] }

func promiseRaceStressTestRun(ctx context.Context, _ promiseRaceStressTestWorkflow) ([]int, error) {
	pending := make([]*workflow.Result[int], 5)
	for i := range pending {
		pending[i] = workflow.Background(ctx, promiseRaceStressTestDelayStep, raceDelay{Dur: 5000 * i, Resp: i})
	}
	done := []int{}
	for len(pending) > 0 {
		winner, err := workflow.Select(ctx, workflow.MapResults(pending)...)
		if err != nil {
			return nil, err
		}
		for i, result := range pending {
			if result == winner {
				value, err := result.Value(ctx)
				if err != nil {
					return nil, err
				}
				done = append(done, value)
				pending = append(pending[:i], pending[i+1:]...)
				break
			}
		}
	}
	return done, nil
}

type branchTimes struct {
	Chain []timedStep `json:"chain"`
	Long  timedStep   `json:"long"`
}

type parallelBranchesAdvanceIndependentlyWorkflow struct {
	workflow.Struct[branchTimes]
}

func parallelBranchesAdvanceIndependentlyRun(ctx context.Context, _ parallelBranchesAdvanceIndependentlyWorkflow) (branchTimes, error) {
	chain1 := workflow.Background(ctx, timedDelayStep, delayed{500, "chain1"})
	long := workflow.Background(ctx, timedDelayStep, delayed{10_000, "long"})
	first, err := chain1.Value(ctx)
	if err != nil {
		return branchTimes{}, err
	}
	second, err := workflow.Step(ctx, timedDelayStep, delayed{500, "chain2"})
	if err != nil {
		return branchTimes{}, err
	}
	longResult, err := long.Value(ctx)
	if err != nil {
		return branchTimes{}, err
	}
	return branchTimes{Chain: []timedStep{first, second}, Long: longResult}, nil
}

type helperMetadata struct {
	Label         string `json:"label"`
	WorkflowRunID string `json:"workflowRunId"`
	StepID        string `json:"stepId"`
	Attempt       int    `json:"attempt"`
}

type metadataFromHelperWorkflow struct {
	workflow.Struct[helperMetadata]
	workflow.Args
	Label string
}

func metadataFromHelperRun(ctx context.Context, input metadataFromHelperWorkflow) (helperMetadata, error) {
	return workflow.Step(ctx, metadataHelperStep, input.Label)
}

type childResult struct {
	ChildResult   int `json:"childResult"`
	OriginalValue int `json:"originalValue"`
}

type childWorkflow struct {
	workflow.Struct[childResult]
	workflow.Args
	Value int
}

func childRun(ctx context.Context, input childWorkflow) (childResult, error) {
	doubled, err := workflow.Step(ctx, doubleValue, input.Value)
	return childResult{ChildResult: doubled, OriginalValue: input.Value}, err
}

type spawnResult struct {
	ParentInput int    `json:"parentInput"`
	ChildRunID  string `json:"childRunId"`
	ChildResult any    `json:"childResult"`
}

type spawnWorkflowFromStepWorkflow struct {
	workflow.Struct[spawnResult]
	workflow.Args
	InputValue int
}

func spawnWorkflowFromStepRun(ctx context.Context, input spawnWorkflowFromStepWorkflow) (spawnResult, error) {
	childRunID, err := workflow.Step(ctx, spawnChildWorkflow, input.InputValue)
	if err != nil {
		return spawnResult{}, err
	}
	result, err := workflow.Step(ctx, awaitWorkflowResult, childRunID)
	if err != nil {
		return spawnResult{}, err
	}
	return spawnResult{ParentInput: input.InputValue, ChildRunID: childRunID, ChildResult: result}, nil
}

type fatalRoundTrip struct {
	Caught           bool   `json:"caught"`
	IsFatal          bool   `json:"isFatal"`
	IsInstanceOf     bool   `json:"isInstanceOf"`
	Message          string `json:"message"`
	Name             string `json:"name"`
	HasFatalProp     bool   `json:"hasFatalProp"`
	CauseIsTypeError bool   `json:"causeIsTypeError"`
	CauseName        string `json:"causeName,omitempty"`
	CauseMessage     string `json:"causeMessage,omitempty"`
}

type errorStepThrowFatalRoundTrip struct {
	workflow.Struct[fatalRoundTrip]
}

func errorStepThrowFatalRoundTripRun(ctx context.Context, _ errorStepThrowFatalRoundTrip) (fatalRoundTrip, error) {
	_, err := workflow.Step(ctx, throwFatalErrorWithCause, struct{}{})
	if err == nil {
		return fatalRoundTrip{}, nil
	}
	result := fatalRoundTrip{Caught: true, IsFatal: isFatal(err), Message: err.Error()}
	var recorded *workflow.Error
	if errors.As(err, &recorded) {
		result.Name = recorded.Type
		result.IsInstanceOf = recorded.Type == "FatalError"
		result.HasFatalProp = recorded.Type == "FatalError"
		// The JavaScript cause is the first error of the chain that is not a
		// recorded Go error, past the wrapper that carries the message.
		for cause := recorded.Cause; cause != nil; cause = cause.Cause {
			if cause.Name == "" {
				result.CauseIsTypeError = cause.Type == "TypeError"
				result.CauseName, result.CauseMessage = cause.Type, cause.Message
				break
			}
		}
	}
	return result, nil
}

type errorWorkflowThrowFatalRoundTrip struct{ workflow.Struct[struct{}] }

func errorWorkflowThrowFatalRoundTripRun(context.Context, errorWorkflowThrowFatalRoundTrip) (struct{}, error) {
	return struct{}{}, workflow.FatalError(causedError{message: "workflow exploded", cause: &workflow.Error{Type: "RangeError", Message: "out of bounds"}})
}

// causedError is an error with its own message and a cause, as a
// JavaScript error with a cause property.
type causedError struct {
	message string
	cause   error
}

func (e causedError) Error() string { return e.message }
func (e causedError) Unwrap() error { return e.cause }
