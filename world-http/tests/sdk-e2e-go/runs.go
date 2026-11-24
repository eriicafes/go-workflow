package sdke2e

import (
	"context"
	"errors"
	"fmt"
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
	MaxAttempts   int    `json:"maxAttempts"`
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

type processedValue struct {
	Processed int `json:"processed"`
}

type childWorkflowWithHookSignal struct {
	workflow.Struct[processedValue]
	workflow.Args
	HookToken string
	Value     int
}

func childWithHookSignalRun(ctx context.Context, input childWorkflowWithHookSignal) (processedValue, error) {
	return workflow.Step(ctx, processAndSignalParent, hookValue{Token: input.HookToken, Value: input.Value})
}

type startFromResult struct {
	ParentInput     int            `json:"parentInput"`
	ChildRunID      string         `json:"childRunId"`
	SignalFromChild processedValue `json:"signalFromChild"`
}

type startFromWorkflow struct {
	workflow.Struct[startFromResult]
	workflow.Args
	InputValue int
}

func startFromRun(ctx context.Context, input startFromWorkflow) (startFromResult, error) {
	hook := workflow.CreateHook[processedValue](ctx)
	defer hook.Dispose(ctx)
	// The hook is registered before the child can resume it.
	if _, err := hook.GetConflict(ctx); err != nil {
		return startFromResult{}, err
	}
	child, err := workflow.Start(ctx, childWorkflowWithHookSignal{HookToken: hook.Token(), Value: input.InputValue})
	if err != nil {
		return startFromResult{}, err
	}
	signal, err := hook.Next(ctx)
	if err != nil {
		return startFromResult{}, err
	}
	return startFromResult{ParentInput: input.InputValue, ChildRunID: child.ID(), SignalFromChild: signal}, nil
}

// fibonacciWorkflow starts runs for fib(n-1) and fib(n-2) and waits for
// both, suspended on its child-run completion hook.
type fibonacciWorkflow struct {
	workflow.Struct[int]
	workflow.Args
	N int
}

func fibonacciRun(ctx context.Context, input fibonacciWorkflow) (int, error) {
	if input.N <= 1 {
		return input.N, nil
	}
	runA, err := workflow.Start(ctx, fibonacciWorkflow{N: input.N - 1})
	if err != nil {
		return 0, err
	}
	runB, err := workflow.Start(ctx, fibonacciWorkflow{N: input.N - 2})
	if err != nil {
		return 0, err
	}
	a, b := runA.ReturnValue(ctx), runB.ReturnValue(ctx)
	if err := workflow.JoinOk(ctx, a, b); err != nil {
		return 0, err
	}
	return a.Unwrap(ctx) + b.Unwrap(ctx), nil
}

// slowChildWorkflow sleeps first, so its parent reads it unfinished and
// waits on its completion hook, then doubles Value or fails.
type slowChildWorkflow struct {
	workflow.Struct[int]
	workflow.Args
	Value int
	Fail  bool
}

func slowChildRun(ctx context.Context, input slowChildWorkflow) (int, error) {
	if err := workflow.Sleep(ctx, 2*time.Second); err != nil {
		return 0, err
	}
	if input.Fail {
		return 0, workflow.FatalError(fmt.Errorf("slow child failed for %d", input.Value))
	}
	return workflow.Step(ctx, doubleValue, input.Value)
}

type childReturnValue struct {
	ChildRunID      string `json:"childRunId"`
	ChildResult     *int   `json:"childResult,omitempty"`
	ChildError      string `json:"childError,omitempty"`
	ChildErrorCause string `json:"childErrorCause,omitempty"`
}

// awaitChildReturnValueWorkflow waits for a child's output from workflow
// code. A failed child's Value is a *RunFailedError, the SDK's
// WorkflowRunFailedError, with the child's error as its cause.
type awaitChildReturnValueWorkflow struct {
	workflow.Struct[childReturnValue]
	workflow.Args
	Value int
	Fail  bool
}

func awaitChildReturnValueRun(ctx context.Context, input awaitChildReturnValueWorkflow) (childReturnValue, error) {
	child, err := workflow.Start(ctx, slowChildWorkflow{Value: input.Value, Fail: input.Fail})
	if err != nil {
		return childReturnValue{}, err
	}
	result := childReturnValue{ChildRunID: child.ID()}
	value, err := child.Value(ctx)
	var failure *workflow.RunFailedError
	switch {
	case errors.As(err, &failure):
		result.ChildError, result.ChildErrorCause = "WorkflowRunFailedError", failure.Cause.Error()
	case err != nil:
		return childReturnValue{}, err
	default:
		result.ChildResult = &value
	}
	return result, nil
}

// gatedChildWorkflow finishes once its hook, Token, is resumed, then doubles
// Value.
type gatedChildWorkflow struct {
	workflow.Struct[int]
	workflow.Args
	Token string
	Value int
}

func gatedChildRun(ctx context.Context, input gatedChildWorkflow) (int, error) {
	hook := workflow.CreateHook[map[string]any](ctx, workflow.HookOption{Token: input.Token})
	defer hook.Dispose(ctx)
	if _, err := hook.Next(ctx); err != nil {
		return 0, err
	}
	return workflow.Step(ctx, doubleValue, input.Value)
}

type fanOutResult struct {
	ChildRunIDs []string `json:"childRunIds"`
	Results     []int    `json:"results"`
}

// awaitChildrenFanOutWorkflow starts a child per value and waits for them
// all. Every child resumes the parent's one completion hook. Child i waits
// on the hook TokenPrefix:i.
type awaitChildrenFanOutWorkflow struct {
	workflow.Struct[fanOutResult]
	workflow.Args
	Values      []int
	TokenPrefix string
}

func awaitChildrenFanOutRun(ctx context.Context, input awaitChildrenFanOutWorkflow) (fanOutResult, error) {
	result := fanOutResult{ChildRunIDs: []string{}, Results: []int{}}
	var values []*workflow.Result[int]
	for i, value := range input.Values {
		child, err := workflow.Start(ctx, gatedChildWorkflow{Token: fmt.Sprintf("%s:%d", input.TokenPrefix, i), Value: value})
		if err != nil {
			return fanOutResult{}, err
		}
		result.ChildRunIDs = append(result.ChildRunIDs, child.ID())
		values = append(values, child.ReturnValue(ctx))
	}
	if err := workflow.JoinOk(ctx, workflow.MapResults(values)...); err != nil {
		return fanOutResult{}, err
	}
	for _, value := range values {
		result.Results = append(result.Results, value.Unwrap(ctx))
	}
	return result, nil
}

type workflowMetadata struct {
	WorkflowName      string           `json:"workflowName"`
	WorkflowRunID     string           `json:"workflowRunId"`
	WorkflowStartedAt string           `json:"workflowStartedAt"`
	URL               string           `json:"url"`
	Features          workflowFeatures `json:"features"`
}

type workflowFeatures struct {
	Encryption bool `json:"encryption"`
}

type stepMetadata struct {
	StepName      string `json:"stepName"`
	StepID        string `json:"stepId"`
	StepStartedAt string `json:"stepStartedAt"`
	Attempt       int    `json:"attempt"`
	MaxAttempts   int    `json:"maxAttempts"`
}

type metadataPair struct {
	StepMetadata     stepMetadata     `json:"stepMetadata"`
	WorkflowMetadata workflowMetadata `json:"workflowMetadata"`
}

type bothMetadata struct {
	WorkflowMetadata      workflowMetadata `json:"workflowMetadata"`
	StepMetadata          stepMetadata     `json:"stepMetadata"`
	InnerWorkflowMetadata workflowMetadata `json:"innerWorkflowMetadata"`
}

// workflowAndStepMetadataWorkflow reports the run's metadata from workflow
// and step context, and the step's metadata.
type workflowAndStepMetadataWorkflow struct{ workflow.Struct[bothMetadata] }

func workflowAndStepMetadataRun(ctx context.Context, _ workflowAndStepMetadataWorkflow) (bothMetadata, error) {
	outer := toWorkflowMetadata(workflow.GetWorkflowMetadata(ctx))
	inner, err := workflow.Step(ctx, stepWithMetadata, struct{}{})
	if err != nil {
		return bothMetadata{}, err
	}
	return bothMetadata{WorkflowMetadata: outer, StepMetadata: inner.StepMetadata, InnerWorkflowMetadata: inner.WorkflowMetadata}, nil
}

func stepWithMetadata(ctx context.Context, _ struct{}) (metadataPair, error) {
	step := workflow.GetStepMetadata(ctx)
	return metadataPair{
		StepMetadata:     stepMetadata{StepName: step.Name, StepID: step.ID, StepStartedAt: step.StartedAt.UTC().Format(time.RFC3339Nano), Attempt: step.Attempt, MaxAttempts: step.MaxAttempts},
		WorkflowMetadata: toWorkflowMetadata(workflow.GetWorkflowMetadata(ctx)),
	}, nil
}

func toWorkflowMetadata(metadata workflow.WorkflowMetadata) workflowMetadata {
	return workflowMetadata{
		WorkflowName:      metadata.Name,
		WorkflowRunID:     metadata.RunID,
		WorkflowStartedAt: metadata.StartedAt.UTC().Format(time.RFC3339Nano),
		URL:               metadata.URL,
		Features:          workflowFeatures{Encryption: metadata.Features.Encryption},
	}
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
