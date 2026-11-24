package sdke2e

import (
	"context"
	"errors"
	"regexp"
	"strconv"
	"strings"
	"time"

	workflow "github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/worlds"
)

var fixtures = []worlds.Runner{
	workflow.Use(addTenRun),
	workflow.Use(promiseAllRun),
	workflow.Use(promiseRaceRun),
	workflow.Use(promiseAnyRun),
	workflow.Use(nullByteRun),
	workflow.Use(sleepingRun),
	workflow.Use(parallelSleepRun),
	workflow.Use(sleepWithSequentialStepsRun),
	workflow.Use(stepWinsRaceRun),
	workflow.Use(sleepWinsRaceRun),
	workflow.Use(errorRetrySuccessRun),
	workflow.Use(errorRetryFatalRun),
	workflow.Use(errorRetryDisabledRun),
	workflow.Use(errorRetryCustomDelayRun),
	workflow.Use(errorFatalCatchableRun),
	workflow.Use(hookRun),
	workflow.Use(hookCleanupTestRun),
	workflow.Use(hookDisposeTestRun),
	workflow.Use(hookGetConflictRun),
	workflow.Use(hookGetConflictWithPriorStepRun),
	workflow.Use(hookGetConflictWithParallelStepRun),
	workflow.Use(hookGetConflictThenStepParallelRun),
	workflow.Use(hookMinRetentionRun),
	workflow.Use(hookClaimOnlyMutexRun),
	workflow.Use(hookAdoptOwnerResultRun),
	workflow.Use(hookSignalOwnerRun),
	workflow.Use(hookTokenReuseLoopRun),
	workflow.Use(hookWithSleepRun),
	workflow.Use(hookWithSleepFinalStepRun),
	workflow.Use(retainedInterleavingRun),
	workflow.Use(hookRaceAfterLostRaceRun),
	workflow.Use(hookRaceLoopAfterLostRacesRun),
	workflow.Use(hookRaceOncePromiseRun),
	workflow.Use(outputStreamRun),
	workflow.Use(outputStreamInsideStepRun),
	workflow.Use(utf8StreamRun),
	workflow.Use(writableForwardedChildRun),
	workflow.Use(writableForwardedFromWorkflowRun),
	workflow.Use(writableForwardedFromStepRun),
	workflow.Use(sleepInLoopRun),
	workflow.Use(promiseRaceStressTestRun),
	workflow.Use(parallelBranchesAdvanceIndependentlyRun),
	workflow.Use(metadataFromHelperRun),
	workflow.Use(childRun),
	workflow.Use(spawnWorkflowFromStepRun),
	workflow.Use(errorStepThrowFatalRoundTripRun),
	workflow.Use(errorWorkflowThrowFatalRoundTripRun),
	workflow.Use(setAttributesRun),
	workflow.Use(setAttributesInsideStepRun),
	workflow.Use(setAttributesThrowsAfterRun),
}

type addTenWorkflow struct {
	workflow.Struct[int]
	workflow.Args
	Input int
}

func addTenRun(ctx context.Context, input addTenWorkflow) (int, error) {
	a, err := workflow.Step(ctx, add, pair{input.Input, 2})
	if err != nil {
		return 0, err
	}
	b, err := workflow.Step(ctx, add, pair{a, 3})
	if err != nil {
		return 0, err
	}
	return workflow.Step(ctx, add, pair{b, 5})
}

type promiseAllWorkflow struct{ workflow.Struct[string] }

func promiseAllRun(ctx context.Context, _ promiseAllWorkflow) (string, error) {
	values, err := workflow.All(ctx, randomDelay, []string{"a", "b", "c"})
	return strings.Join(values, ""), err
}

type promiseRaceWorkflow struct{ workflow.Struct[string] }

func promiseRaceRun(ctx context.Context, _ promiseRaceWorkflow) (string, error) {
	a := workflow.Background(ctx, specificDelay, delayed{10000, "a"})
	b := workflow.Background(ctx, specificDelay, delayed{100, "b"})
	c := workflow.Background(ctx, specificDelay, delayed{20000, "c"})
	return first(ctx, workflow.Select, workflow.MapResults([]*workflow.Result[string]{a, b, c}), a, b, c)
}

type promiseAnyWorkflow struct{ workflow.Struct[string] }

func promiseAnyRun(ctx context.Context, _ promiseAnyWorkflow) (string, error) {
	a := workflow.Background(ctx, stepThatFails, struct{}{})
	b := workflow.Background(ctx, specificDelay, delayed{100, "b"})
	c := workflow.Background(ctx, specificDelay, delayed{6000, "c"})
	return first(ctx, workflow.SelectOk, workflow.MapResults([]*workflow.Result[string]{a, b, c}), a, b, c)
}

type nullByteWorkflow struct{ workflow.Struct[string] }

func nullByteRun(ctx context.Context, _ nullByteWorkflow) (string, error) {
	return workflow.Step(ctx, nullByteStep, struct{}{})
}

type sleepingWorkflow struct {
	workflow.Struct[span]
	workflow.Args
	DurationMs *int
}

func sleepingRun(ctx context.Context, input sleepingWorkflow) (span, error) {
	duration := 10_000
	if input.DurationMs != nil {
		duration = *input.DurationMs
	}
	start := workflow.Now(ctx).UnixMilli()
	if err := workflow.Sleep(ctx, time.Duration(duration)*time.Millisecond); err != nil {
		return span{}, err
	}
	return span{StartTime: start, EndTime: workflow.Now(ctx).UnixMilli()}, nil
}

type parallelSleepWorkflow struct{ workflow.Struct[span] }

func parallelSleepRun(ctx context.Context, _ parallelSleepWorkflow) (span, error) {
	start := workflow.Now(ctx).UnixMilli()
	timers := make([]*workflow.Result[struct{}], 10)
	for i := range timers {
		timers[i] = workflow.Timer(ctx, time.Second)
	}
	if err := workflow.Join(ctx, workflow.MapResults(timers)...); err != nil {
		return span{}, err
	}
	return span{StartTime: start, EndTime: workflow.Now(ctx).UnixMilli()}, nil
}

type sequentialSums struct {
	A            int  `json:"a"`
	B            int  `json:"b"`
	C            int  `json:"c"`
	ShouldCancel bool `json:"shouldCancel"`
}

type sleepWithSequentialStepsWorkflow struct {
	workflow.Struct[sequentialSums]
}

func sleepWithSequentialStepsRun(ctx context.Context, _ sleepWithSequentialStepsWorkflow) (sequentialSums, error) {
	// A timer nobody waits for, which must not hold the run open.
	timer := workflow.Timer(ctx, 24*time.Hour)
	a, err := workflow.Step(ctx, addNumbers, pair{1, 2})
	if err != nil {
		return sequentialSums{}, err
	}
	b, err := workflow.Step(ctx, addNumbers, pair{a, 3})
	if err != nil {
		return sequentialSums{}, err
	}
	c, err := workflow.Step(ctx, addNumbers, pair{b, 4})
	if err != nil {
		return sequentialSums{}, err
	}
	winner, err := workflow.Select(ctx, timer, workflow.NewResult(struct{}{}, nil))
	if err != nil {
		return sequentialSums{}, err
	}
	return sequentialSums{A: a, B: b, C: c, ShouldCancel: winner == timer}, nil
}

type raceResult struct {
	Winner     string `json:"winner"`
	DurationMs int64  `json:"durationMs"`
}

type stepWinsRaceWorkflow struct{ workflow.Struct[raceResult] }

func stepWinsRaceRun(ctx context.Context, _ stepWinsRaceWorkflow) (raceResult, error) {
	return raceStepAndSleep(ctx, 1_000*time.Millisecond, 10*time.Second)
}

type sleepWinsRaceWorkflow struct{ workflow.Struct[raceResult] }

func sleepWinsRaceRun(ctx context.Context, _ sleepWinsRaceWorkflow) (raceResult, error) {
	return raceStepAndSleep(ctx, 10_000*time.Millisecond, time.Second)
}

func raceStepAndSleep(ctx context.Context, stepDelay, sleep time.Duration) (raceResult, error) {
	start := workflow.Now(ctx).UnixMilli()
	step := workflow.Background(ctx, delayMsStep, delayed{int(stepDelay.Milliseconds()), "step"})
	timer := workflow.Timer(ctx, sleep)
	winner, err := workflow.Select(ctx, step, timer)
	if err != nil {
		return raceResult{}, err
	}
	label := "sleep"
	if winner == step {
		if label, err = step.Value(ctx); err != nil {
			return raceResult{}, err
		}
	}
	return raceResult{Winner: label, DurationMs: workflow.Now(ctx).UnixMilli() - start}, nil
}

type finalAttempt struct {
	FinalAttempt int `json:"finalAttempt"`
}

type errorRetrySuccess struct{ workflow.Struct[finalAttempt] }

func errorRetrySuccessRun(ctx context.Context, _ errorRetrySuccess) (finalAttempt, error) {
	attempt, err := workflow.Step(ctx, retryUntilAttempt3, struct{}{})
	return finalAttempt{FinalAttempt: attempt}, err
}

type errorRetryFatal struct{ workflow.Struct[string] }

func errorRetryFatalRun(ctx context.Context, _ errorRetryFatal) (string, error) {
	if _, err := workflow.Step(ctx, throwFatalError, struct{}{}); err != nil {
		return "", err
	}
	return "never reached", nil
}

type disabledRetry struct {
	Failed  bool `json:"failed"`
	Attempt *int `json:"attempt"`
}

type errorRetryDisabled struct{ workflow.Struct[disabledRetry] }

func errorRetryDisabledRun(ctx context.Context, _ errorRetryDisabled) (disabledRetry, error) {
	_, err := workflow.Step(ctx, throwWithNoRetries, struct{}{}, workflow.StepOptions{MaxRetries: 0})
	if err == nil {
		return disabledRetry{}, nil
	}
	match := attemptPattern.FindStringSubmatch(err.Error())
	if match == nil {
		return disabledRetry{Failed: true}, nil
	}
	attempt, _ := strconv.Atoi(match[1])
	return disabledRetry{Failed: true, Attempt: &attempt}, nil
}

var attemptPattern = regexp.MustCompile(`attempt (\d+)`)

type retriedAfter struct {
	Attempt  int   `json:"attempt"`
	Duration int64 `json:"duration"`
}

type errorRetryCustomDelay struct{ workflow.Struct[retriedAfter] }

func errorRetryCustomDelayRun(ctx context.Context, _ errorRetryCustomDelay) (retriedAfter, error) {
	return workflow.Step(ctx, throwRetryableError, struct{}{})
}

type caughtFatal struct {
	Caught  bool `json:"caught"`
	IsFatal bool `json:"isFatal"`
}

type errorFatalCatchable struct{ workflow.Struct[caughtFatal] }

func errorFatalCatchableRun(ctx context.Context, _ errorFatalCatchable) (caughtFatal, error) {
	_, err := workflow.Step(ctx, throwFatalError, struct{}{})
	if err == nil {
		return caughtFatal{}, nil
	}
	return caughtFatal{Caught: true, IsFatal: isFatal(err)}, nil
}

// first waits for results with wait, Select or SelectOk, and returns the
// winner's value.
func first[P comparable](ctx context.Context, wait func(context.Context, ...P) (P, error), promises []P, results ...*workflow.Result[string]) (string, error) {
	winner, err := wait(ctx, promises...)
	if err != nil {
		return "", err
	}
	for i, promise := range promises {
		if promise == winner {
			return results[i].Value(ctx)
		}
	}
	return "", errors.New("sdke2e: the winner is none of the results")
}

func isFatal(err error) bool {
	var recorded *workflow.Error
	return errors.As(err, &recorded) && recorded.Type == "FatalError"
}
