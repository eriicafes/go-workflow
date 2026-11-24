package sdke2e

import (
	"context"
	"errors"
	"time"

	workflow "github.com/eriicafes/go-workflow"
)

// The AbortController fixtures use durable cancellation contexts: a step
// started with a cancellation context sees it done when the workflow, or
// another step, cancels it, as a step sees its AbortSignal abort. Where a
// TypeScript step returns a value saying it saw the abort, a Go step attempt
// whose context is done fails with an error that wraps context.Canceled and
// the cause, and the fixture reads that error as the step's value.

type timeoutResult struct {
	Status  string `json:"status"`
	Aborted bool   `json:"aborted,omitempty"`
	Result  string `json:"result,omitempty"`
}

type abortTimeoutWorkflow struct{ workflow.Struct[timeoutResult] }

func abortTimeoutRun(ctx context.Context, _ abortTimeoutWorkflow) (timeoutResult, error) {
	cctx, cancel, err := workflow.WithCancel(ctx)
	if err != nil {
		return timeoutResult{}, err
	}
	step := workflow.Background(cctx, longStep, "")
	result, err := raceSleep(ctx, step, 3*time.Second)
	if err != nil {
		return timeoutResult{}, err
	}
	if result == "sleep" {
		if err := cancel(); err != nil {
			return timeoutResult{}, err
		}
		return timeoutResult{Status: "timed out", Aborted: cctx.Err() != nil}, nil
	}
	return timeoutResult{Status: "completed", Result: result}, nil
}

type parallelResult struct {
	Status  string   `json:"status"`
	Results []string `json:"results"`
}

type abortParallelWorkflow struct {
	workflow.Struct[parallelResult]
}

func abortParallelRun(ctx context.Context, _ abortParallelWorkflow) (parallelResult, error) {
	cctx, cancel, err := workflow.WithCancel(ctx)
	if err != nil {
		return parallelResult{}, err
	}
	var group workflow.WaitGroup
	steps := make([]*workflow.Result[string], 3)
	for i := range steps {
		steps[i] = workflow.Go(cctx, &group, longStep, "")
	}
	timer := workflow.Timer(ctx, 3*time.Second)
	winner, err := workflow.Select(ctx, &group, timer)
	if err != nil {
		return parallelResult{}, err
	}
	status := "completed"
	if winner == timer {
		if err := cancel(); err != nil {
			return parallelResult{}, err
		}
		status = "timed out"
	}
	// The steps observe the cancellation and return before the run does.
	results := make([]string, len(steps))
	for i, step := range steps {
		if results[i], err = orAborted(step.Value(ctx)); err != nil {
			return parallelResult{}, err
		}
	}
	return parallelResult{Status: status, Results: results}, nil
}

type fromStepResult struct {
	WorkflowAborted bool   `json:"workflowAborted"`
	StepSawAborted  bool   `json:"stepSawAborted"`
	LongStepResult  string `json:"longStepResult"`
}

type abortFromStepWorkflow struct {
	workflow.Struct[fromStepResult]
}

func abortFromStepRun(ctx context.Context, _ abortFromStepWorkflow) (fromStepResult, error) {
	cctx, _, err := workflow.WithCancel(ctx)
	if err != nil {
		return fromStepResult{}, err
	}
	long := workflow.Background(cctx, longStep, "")
	abort := workflow.Background(cctx, abortFromStep, 1000)
	// The group waits for both, whether they fail or not.
	var group workflow.WaitGroup
	group.Add(long, abort)
	if err := group.Wait(ctx); err != nil {
		return fromStepResult{}, err
	}
	longResult, err := orAborted(long.Value(ctx))
	if err != nil {
		return fromStepResult{}, err
	}
	state, err := checkSignal(cctx)
	if err != nil {
		return fromStepResult{}, err
	}
	return fromStepResult{WorkflowAborted: cctx.Err() != nil, StepSawAborted: state.Aborted, LongStepResult: longResult}, nil
}

type abortAlreadyAbortedWorkflow struct{ workflow.Struct[signalState] }

func abortAlreadyAbortedRun(ctx context.Context, _ abortAlreadyAbortedWorkflow) (signalState, error) {
	cctx, cancel, err := workflow.WithCancelCause(ctx)
	if err != nil {
		return signalState{}, err
	}
	if err := cancel(errors.New("pre-aborted")); err != nil {
		return signalState{}, err
	}
	return checkSignal(cctx)
}

type abortReasonWorkflow struct{ workflow.Struct[signalState] }

func abortReasonRun(ctx context.Context, _ abortReasonWorkflow) (signalState, error) {
	cctx, cancel, err := workflow.WithCancelCause(ctx)
	if err != nil {
		return signalState{}, err
	}
	result, err := raceSleep(ctx, workflow.Background(cctx, longStep, ""), 2*time.Second)
	if err != nil {
		return signalState{}, err
	}
	if result == "sleep" {
		if err := cancel(errors.New("custom timeout reason")); err != nil {
			return signalState{}, err
		}
	}
	return checkSignal(cctx)
}

type afterCompletionResult struct {
	StepSawAborted  bool `json:"stepSawAborted"`
	WorkflowAborted bool `json:"workflowAborted"`
}

type abortAfterCompletionWorkflow struct {
	workflow.Struct[afterCompletionResult]
}

func abortAfterCompletionRun(ctx context.Context, _ abortAfterCompletionWorkflow) (afterCompletionResult, error) {
	cctx, cancel, err := workflow.WithCancel(ctx)
	if err != nil {
		return afterCompletionResult{}, err
	}
	state, err := checkSignal(cctx)
	if err != nil {
		return afterCompletionResult{}, err
	}
	if err := cancel(); err != nil {
		return afterCompletionResult{}, err
	}
	return afterCompletionResult{StepSawAborted: state.Aborted, WorkflowAborted: cctx.Err() != nil}, nil
}

type cancelPayload struct {
	Reason string `json:"reason"`
}

type viaHookResult struct {
	Status string `json:"status"`
	Result string `json:"result,omitempty"`
	Reason string `json:"reason,omitempty"`
}

type abortViaHookWorkflow struct {
	workflow.Struct[viaHookResult]
	workflow.Args
	HookToken string
}

func abortViaHookRun(ctx context.Context, input abortViaHookWorkflow) (viaHookResult, error) {
	hook := workflow.CreateHook[cancelPayload](ctx, workflow.HookOption{Token: input.HookToken})
	defer hook.Dispose(ctx)
	cctx, cancel, err := workflow.WithCancelCause(ctx)
	if err != nil {
		return viaHookResult{}, err
	}
	long := workflow.Background(cctx, longStep, "")
	winner, err := workflow.Select(ctx, long, hook)
	if err != nil {
		return viaHookResult{}, err
	}
	if winner == long {
		result, err := long.Value(ctx)
		return viaHookResult{Status: "completed", Result: result}, err
	}
	payload, err := hook.Next(ctx)
	if err != nil {
		return viaHookResult{}, err
	}
	if err := cancel(errors.New(payload.Reason)); err != nil {
		return viaHookResult{}, err
	}
	return viaHookResult{Status: "cancelled", Reason: payload.Reason}, nil
}

type replayResult struct {
	BeforeAborted bool `json:"beforeAborted"`
	AfterAborted  bool `json:"afterAborted"`
	AfterReason   any  `json:"afterReason"`
}

type abortSurvivesReplayWorkflow struct{ workflow.Struct[replayResult] }

func abortSurvivesReplayRun(ctx context.Context, _ abortSurvivesReplayWorkflow) (replayResult, error) {
	cctx, cancel, err := workflow.WithCancelCause(ctx)
	if err != nil {
		return replayResult{}, err
	}
	before, err := checkSignal(cctx)
	if err != nil {
		return replayResult{}, err
	}
	if err := workflow.Sleep(ctx, time.Second); err != nil {
		return replayResult{}, err
	}
	if err := cancel(errors.New("after-replay")); err != nil {
		return replayResult{}, err
	}
	after, err := checkSignal(cctx)
	if err != nil {
		return replayResult{}, err
	}
	return replayResult{BeforeAborted: before.Aborted, AfterAborted: after.Aborted, AfterReason: after.Reason}, nil
}

type thrown struct {
	Threw   bool   `json:"threw"`
	Message string `json:"message,omitempty"`
	IsFatal bool   `json:"isFatal,omitempty"`
	Result  string `json:"result,omitempty"`
}

type abortThrowIfAbortedWorkflow struct{ workflow.Struct[thrown] }

func abortThrowIfAbortedRun(ctx context.Context, _ abortThrowIfAbortedWorkflow) (thrown, error) {
	cctx, cancel, err := workflow.WithCancelCause(ctx)
	if err != nil {
		return thrown{}, err
	}
	if err := cancel(errors.New("throw-test-reason")); err != nil {
		return thrown{}, err
	}
	if _, err := workflow.Step(cctx, stepThatThrowsIfAborted, struct{}{}); err != nil {
		return thrown{Threw: true, Message: err.Error(), IsFatal: isFatal(err)}, nil
	}
	return thrown{}, nil
}

type reasonTypes struct {
	StringReason    signalState `json:"stringReason"`
	ObjectReason    signalState `json:"objectReason"`
	UndefinedReason signalState `json:"undefinedReason"`
}

type abortReasonTypesWorkflow struct{ workflow.Struct[reasonTypes] }

func abortReasonTypesRun(ctx context.Context, _ abortReasonTypesWorkflow) (reasonTypes, error) {
	var result reasonTypes
	c1, cancel1, err := workflow.WithCancelCause(ctx)
	if err != nil {
		return result, err
	}
	if err := cancel1(errors.New("string-reason")); err != nil {
		return result, err
	}
	if result.StringReason, err = checkSignal(c1); err != nil {
		return result, err
	}
	// An object reason is an error type whose fields are the object's.
	c2, cancel2, err := workflow.WithCancelCause(ctx)
	if err != nil {
		return result, err
	}
	if err := cancel2(&reasonError{Code: "CANCELLED", Detail: "by user"}); err != nil {
		return result, err
	}
	if result.ObjectReason, err = checkSignal(c2); err != nil {
		return result, err
	}
	c3, cancel3, err := workflow.WithCancel(ctx)
	if err != nil {
		return result, err
	}
	if err := cancel3(); err != nil {
		return result, err
	}
	result.UndefinedReason, err = checkSignal(c3)
	return result, err
}

type fetchOutcome struct {
	Winner      string      `json:"winner"`
	FetchResult fetchResult `json:"fetchResult"`
}

type abortFetchInFlightWorkflow struct{ workflow.Struct[fetchOutcome] }

func abortFetchInFlightRun(ctx context.Context, _ abortFetchInFlightWorkflow) (fetchOutcome, error) {
	cctx, cancel, err := workflow.WithCancel(ctx)
	if err != nil {
		return fetchOutcome{}, err
	}
	fetch := workflow.Background(cctx, fetchWithSignal, struct{}{})
	timer := workflow.Timer(ctx, 2*time.Second)
	winner, err := workflow.Select(ctx, fetch, timer)
	if err != nil {
		return fetchOutcome{}, err
	}
	outcome := fetchOutcome{Winner: "fetch"}
	if winner == timer {
		outcome.Winner = "timeout"
		if err := cancel(); err != nil {
			return fetchOutcome{}, err
		}
	}
	outcome.FetchResult, err = fetchOrAborted(fetch.Value(ctx))
	return outcome, err
}

type abortVoidSleepTimeoutWorkflow struct{ workflow.Struct[fetchResult] }

func abortVoidSleepTimeoutRun(ctx context.Context, _ abortVoidSleepTimeoutWorkflow) (fetchResult, error) {
	// A durable timeout context is the Go counterpart of a sleep that
	// aborts a controller when it ends.
	cctx, _, err := workflow.WithTimeout(ctx, 2*time.Second)
	if err != nil {
		return fetchResult{}, err
	}
	return fetchOrAborted(workflow.Step(cctx, fetchWithSignal, struct{}{}))
}

type abortFetchUncaughtWorkflow struct{ workflow.Struct[thrown] }

func abortFetchUncaughtRun(ctx context.Context, _ abortFetchUncaughtWorkflow) (thrown, error) {
	cctx, cancel, err := workflow.WithCancel(ctx)
	if err != nil {
		return thrown{}, err
	}
	if err := cancel(); err != nil {
		return thrown{}, err
	}
	if _, err := workflow.Step(cctx, stepThatFetchesWithSignal, struct{}{}); err != nil {
		return thrown{Threw: true, Message: err.Error(), IsFatal: isFatal(err)}, nil
	}
	return thrown{}, nil
}

type branchResult struct {
	Result  string `json:"result"`
	Aborted bool   `json:"aborted"`
	Reason  any    `json:"reason"`
}

type abortDeterministicBranchWorkflow struct{ workflow.Struct[branchResult] }

func abortDeterministicBranchRun(ctx context.Context, _ abortDeterministicBranchWorkflow) (branchResult, error) {
	cctx, cancel, err := workflow.WithCancelCause(ctx)
	if err != nil {
		return branchResult{}, err
	}
	var result string
	if cctx.Err() != nil {
		result = "was aborted"
	} else {
		if err := cancel(errors.New("test")); err != nil {
			return branchResult{}, err
		}
		result = "just aborted"
	}
	state, err := checkSignal(cctx)
	if err != nil {
		return branchResult{}, err
	}
	return branchResult{Result: result, Aborted: state.Aborted, Reason: state.Reason}, nil
}

type listenerOutcome struct {
	StepResult listenerResult `json:"stepResult"`
}

type abortListenerWorkflow struct {
	workflow.Struct[listenerOutcome]
}

func abortListenerRun(ctx context.Context, _ abortListenerWorkflow) (listenerOutcome, error) {
	cctx, _, err := workflow.WithCancel(ctx)
	if err != nil {
		return listenerOutcome{}, err
	}
	listener := workflow.Background(cctx, stepWaitingOnAbortListener, struct{}{})
	abort := workflow.Background(cctx, abortFromStep, 1000)
	// The group waits for both, whether they fail or not.
	var group workflow.WaitGroup
	group.Add(listener, abort)
	if err := group.Wait(ctx); err != nil {
		return listenerOutcome{}, err
	}
	result, err := listenerOrAborted(listener.Value(ctx))
	return listenerOutcome{StepResult: result}, err
}

type abortThrowIfAbortedMidFlightWorkflow struct{ workflow.Struct[thrown] }

func abortThrowIfAbortedMidFlightRun(ctx context.Context, _ abortThrowIfAbortedMidFlightWorkflow) (thrown, error) {
	cctx, _, err := workflow.WithCancel(ctx)
	if err != nil {
		return thrown{}, err
	}
	poll := workflow.Background(cctx, stepPollingThrowIfAborted, struct{}{})
	abort := workflow.Background(cctx, abortFromStep, 1000)
	// The group waits for both, whether they fail or not.
	var group workflow.WaitGroup
	group.Add(poll, abort)
	if err := group.Wait(ctx); err != nil {
		return thrown{}, err
	}
	result, err := poll.Value(ctx)
	if err != nil {
		return thrown{Threw: true, Message: err.Error(), IsFatal: isFatal(err)}, nil
	}
	return thrown{Result: result}, nil
}

type stepBranchResult struct {
	BeforeAborted bool   `json:"beforeAborted"`
	BeforeBranch  string `json:"beforeBranch"`
	AfterAborted  bool   `json:"afterAborted"`
	AfterBranch   string `json:"afterBranch"`
}

type abortDeterministicBranchFromStepWorkflow struct {
	workflow.Struct[stepBranchResult]
}

func abortDeterministicBranchFromStepRun(ctx context.Context, _ abortDeterministicBranchFromStepWorkflow) (stepBranchResult, error) {
	cctx, _, err := workflow.WithCancel(ctx)
	if err != nil {
		return stepBranchResult{}, err
	}
	result := stepBranchResult{BeforeAborted: cctx.Err() != nil, BeforeBranch: "pre-abort"}
	if result.BeforeAborted {
		result.BeforeBranch = "unexpected-aborted"
	}
	if _, err := workflow.Step(cctx, abortFromStep, 0); err != nil && !cancelled(err) {
		return stepBranchResult{}, err
	}
	if err := workflow.Sleep(ctx, time.Second); err != nil {
		return stepBranchResult{}, err
	}
	result.AfterAborted, result.AfterBranch = cctx.Err() != nil, "post-abort"
	if !result.AfterAborted {
		result.AfterBranch = "unexpected-not-aborted"
	}
	return result, nil
}

type anyInWorkflowResult struct {
	BeforeCombinedAborted bool `json:"beforeCombinedAborted"`
	AfterCombinedAborted  bool `json:"afterCombinedAborted"`
	AfterCombinedReason   any  `json:"afterCombinedReason"`
	C1Aborted             bool `json:"c1Aborted"`
}

type abortAnyInWorkflowWorkflow struct {
	workflow.Struct[anyInWorkflowResult]
}

func abortAnyInWorkflowRun(ctx context.Context, _ abortAnyInWorkflowWorkflow) (anyInWorkflowResult, error) {
	c1, _, err := workflow.WithCancelCause(ctx)
	if err != nil {
		return anyInWorkflowResult{}, err
	}
	c2, cancel2, err := workflow.WithCancelCause(ctx)
	if err != nil {
		return anyInWorkflowResult{}, err
	}
	combined := workflow.CancelAny(ctx, c1, c2)
	result := anyInWorkflowResult{BeforeCombinedAborted: combined.Err() != nil}
	if err := cancel2(errors.New("via c2")); err != nil {
		return anyInWorkflowResult{}, err
	}
	result.AfterCombinedAborted = combined.Err() != nil
	result.AfterCombinedReason = reason(context.Cause(combined))
	result.C1Aborted = c1.Err() != nil
	return result, nil
}

type anyInStepResult struct {
	StepResult listenerResult `json:"stepResult"`
	C1Aborted  bool           `json:"c1Aborted"`
	C2Aborted  bool           `json:"c2Aborted"`
}

type abortAnyInStepWorkflow struct {
	workflow.Struct[anyInStepResult]
}

func abortAnyInStepRun(ctx context.Context, _ abortAnyInStepWorkflow) (anyInStepResult, error) {
	c1, _, err := workflow.WithCancel(ctx)
	if err != nil {
		return anyInStepResult{}, err
	}
	c2, _, err := workflow.WithCancel(ctx)
	if err != nil {
		return anyInStepResult{}, err
	}
	// The step's context is done when either source is.
	listener := workflow.Background(workflow.CancelAny(ctx, c1, c2), stepWaitingOnAbortListener, struct{}{})
	abort := workflow.Background(c2, abortFromStep, 1000)
	// The group waits for both, whether they fail or not.
	var group workflow.WaitGroup
	group.Add(listener, abort)
	if err := group.Wait(ctx); err != nil {
		return anyInStepResult{}, err
	}
	if err := workflow.Sleep(ctx, 100*time.Millisecond); err != nil {
		return anyInStepResult{}, err
	}
	result, err := listenerOrAborted(listener.Value(ctx))
	return anyInStepResult{StepResult: result, C1Aborted: c1.Err() != nil, C2Aborted: c2.Err() != nil}, err
}

// cancelled reports whether err is the failure of a step attempt whose
// cancellation context was done.
func cancelled(err error) bool { return errors.Is(err, context.Canceled) }

func orAborted(result string, err error) (string, error) {
	if cancelled(err) {
		return "aborted", nil
	}
	return result, err
}

func listenerOrAborted(result listenerResult, err error) (listenerResult, error) {
	if cancelled(err) {
		return listenerResult{Saw: true, Via: "listener"}, nil
	}
	return result, err
}

func fetchOrAborted(result fetchResult, err error) (fetchResult, error) {
	if cancelled(err) {
		return fetchResult{Aborted: true}, nil
	}
	return result, err
}

// checkSignal runs checkSignalState with ctx, and reads a cancelled
// attempt's error as the state it saw.
func checkSignal(ctx context.Context) (signalState, error) {
	state, err := workflow.Step(ctx, checkSignalState, struct{}{})
	if !cancelled(err) {
		return state, err
	}
	return signalState{Aborted: true, Reason: errorReason(err)}, nil
}

// errorReason returns the cancellation cause a cancelled attempt's error
// wraps, as reason does.
func errorReason(err error) any {
	var object *reasonError
	if errors.As(err, &object) {
		return object
	}
	var recorded *workflow.Error
	if errors.As(err, &recorded) {
		for node := recorded; node != nil; node = node.Cause {
			if node.Message != context.Canceled.Error() {
				return node.Message
			}
		}
	}
	return nil
}
