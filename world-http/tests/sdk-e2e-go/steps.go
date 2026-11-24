package sdke2e

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"net/http"
	"strings"
	"time"

	workflow "github.com/eriicafes/go-workflow"
)

// pair is the arguments of a step that adds two numbers.
type pair struct{ A, B int }

// delayed is the arguments of a step that waits Ms milliseconds, then
// returns Label.
type delayed struct {
	Ms    int
	Label string
}

// span is when a workflow started and ended, in Unix milliseconds.
type span struct {
	StartTime int64 `json:"startTime"`
	EndTime   int64 `json:"endTime"`
}

// box is an object argument, which a step unwraps.
type box struct {
	Value int `json:"value"`
}

// timedDelay is the arguments of a step that waits DelayMs milliseconds.
type timedDelay struct {
	Label   string
	DelayMs int
}

// timedStep is when a step started and ended, in Unix milliseconds.
type timedStep struct {
	Label     string `json:"label"`
	StartedAt int64  `json:"startedAt"`
	EndedAt   int64  `json:"endedAt"`
}

type conflictStepData struct {
	CustomData              string `json:"customData"`
	HookGetConflictStepData string `json:"hookGetConflictStepData"`
}

// tokenMessage is the arguments of a step that resumes a hook.
type tokenMessage struct {
	Token   string
	Message string
}

type processed struct {
	Processed bool   `json:"processed"`
	Type      string `json:"type"`
	ID        *int   `json:"id,omitempty"`
}

func add(_ context.Context, in pair) (int, error) { return in.A + in.B, nil }

func addNumbers(_ context.Context, in pair) (int, error) { return in.A + in.B, nil }

func randomDelay(ctx context.Context, v string) (string, error) {
	if err := wait(ctx, time.Duration(rand.Float64()*3000)*time.Millisecond); err != nil {
		return "", err
	}
	return strings.ToUpper(v), nil
}

func specificDelay(ctx context.Context, in delayed) (string, error) {
	if err := wait(ctx, time.Duration(in.Ms)*time.Millisecond); err != nil {
		return "", err
	}
	return strings.ToUpper(in.Label), nil
}

func delayMsStep(ctx context.Context, in delayed) (string, error) {
	if err := wait(ctx, time.Duration(in.Ms)*time.Millisecond); err != nil {
		return "", err
	}
	return in.Label, nil
}

func stepThatFails(context.Context, struct{}) (string, error) {
	return "", workflow.FatalError(errors.New("step failed"))
}

func nullByteStep(context.Context, struct{}) (string, error) { return "null byte \x00", nil }

func retryUntilAttempt3(ctx context.Context, _ struct{}) (int, error) {
	attempt := workflow.GetStepMetadata(ctx).Attempt
	if attempt < 3 {
		return 0, fmt.Errorf("Failed on attempt %d", attempt)
	}
	return attempt, nil
}

func throwFatalError(context.Context, struct{}) (struct{}, error) {
	return struct{}{}, workflow.FatalError(errors.New("Fatal step error"))
}

func throwWithNoRetries(ctx context.Context, _ struct{}) (struct{}, error) {
	return struct{}{}, fmt.Errorf("Failed on attempt %d", workflow.GetStepMetadata(ctx).Attempt)
}

func throwRetryableError(ctx context.Context, _ struct{}) (retriedAfter, error) {
	meta := workflow.GetStepMetadata(ctx)
	if meta.Attempt == 1 {
		return retriedAfter{}, workflow.RetryableError(errors.New("Retryable error"), 10*time.Second)
	}
	return retriedAfter{Attempt: meta.Attempt, Duration: time.Since(meta.StartedAt).Milliseconds()}, nil
}

func unwrapValue(_ context.Context, in box) (int, error) { return in.Value, nil }

func hookGetConflictStep(_ context.Context, customData string) (conflictStepData, error) {
	return conflictStepData{CustomData: customData, HookGetConflictStepData: "step_completed"}, nil
}

func hookGetConflictTimedStep(ctx context.Context, in timedDelay) (timedStep, error) {
	startedAt := workflow.GetStepMetadata(ctx).StartedAt.UnixMilli()
	if err := wait(ctx, time.Duration(in.DelayMs)*time.Millisecond); err != nil {
		return timedStep{}, err
	}
	return timedStep{Label: in.Label, StartedAt: startedAt, EndedAt: time.Now().UnixMilli()}, nil
}

func awaitWorkflowResult(ctx context.Context, runID string) (any, error) {
	run, err := workflow.Get[any](ctx, runID)
	if err != nil {
		return nil, err
	}
	return run.Value(ctx)
}

func forwardPayloadToOwner(ctx context.Context, in tokenMessage) (struct{}, error) {
	return struct{}{}, workflow.ResumeHook(ctx, in.Token, signalPayload{Message: in.Message})
}

func processPayload(_ context.Context, payload sleepHookPayload) (processed, error) {
	return processed{Processed: true, Type: payload.Type, ID: payload.ID}, nil
}

func hookRaceReminderStep(ctx context.Context, _ struct{}) (struct{}, error) {
	// Long enough for the test to resume the hook while this step runs.
	return struct{}{}, wait(ctx, 5*time.Second)
}

// raceDelay is the arguments of a step that waits Dur milliseconds, then
// returns Resp.
type raceDelay struct {
	Dur  int
	Resp int
}

type iterationTime struct {
	Iteration int   `json:"iteration"`
	Ts        int64 `json:"ts"`
}

func noopStep(_ context.Context, iteration int) (iterationTime, error) {
	return iterationTime{Iteration: iteration, Ts: time.Now().UnixMilli()}, nil
}

func promiseRaceStressTestDelayStep(ctx context.Context, in raceDelay) (int, error) {
	if err := wait(ctx, time.Duration(in.Dur)*time.Millisecond); err != nil {
		return 0, err
	}
	return in.Resp, nil
}

func timedDelayStep(ctx context.Context, in delayed) (timedStep, error) {
	startedAt := time.Now().UnixMilli()
	if err := wait(ctx, time.Duration(in.Ms)*time.Millisecond); err != nil {
		return timedStep{}, err
	}
	return timedStep{Label: in.Label, StartedAt: startedAt, EndedAt: time.Now().UnixMilli()}, nil
}

func metadataHelperStep(ctx context.Context, label string) (helperMetadata, error) {
	run, step := strictMetadata(ctx)
	return helperMetadata{Label: label, WorkflowRunID: run.RunID, StepID: step.ID, Attempt: step.Attempt}, nil
}

// strictMetadata reads the metadata from a helper outside the step body.
func strictMetadata(ctx context.Context) (workflow.WorkflowMetadata, workflow.StepMetadata) {
	return workflow.GetWorkflowMetadata(ctx), workflow.GetStepMetadata(ctx)
}

func doubleValue(_ context.Context, value int) (int, error) { return value * 2, nil }

func spawnChildWorkflow(ctx context.Context, value int) (string, error) {
	child, err := workflow.Start(ctx, childWorkflow{Value: value})
	if err != nil {
		return "", err
	}
	return child.ID(), nil
}

func throwFatalErrorWithCause(context.Context, struct{}) (struct{}, error) {
	cause := &workflow.Error{Type: "TypeError", Message: "underlying type error"}
	return struct{}{}, workflow.FatalError(causedError{message: "fatal with cause", cause: cause})
}

// signalState is whether a step's context was done, and its cause.
type signalState struct {
	Aborted bool `json:"aborted"`
	Reason  any  `json:"reason"`
}

type listenerResult struct {
	Saw bool   `json:"saw"`
	Via string `json:"via"`
}

type fetchResult struct {
	OK        bool   `json:"ok"`
	Aborted   bool   `json:"aborted"`
	Status    int    `json:"status,omitempty"`
	Error     string `json:"error,omitempty"`
	ElapsedMs int64  `json:"elapsedMs"`
}

// reasonError is a cancellation cause with an object's fields.
type reasonError struct {
	Code   string `json:"code"`
	Detail string `json:"detail"`
}

func (e *reasonError) Error() string { return e.Code + ": " + e.Detail }

// longStep runs for 30s unless its context is done first. With a token, it
// first resumes that hook to report it is running.
func longStep(ctx context.Context, readyHookToken string) (string, error) {
	if readyHookToken != "" {
		if err := workflow.ResumeHook(ctx, readyHookToken, map[string]bool{"ready": true}); err != nil {
			return "", err
		}
	}
	for range 60 {
		if ctx.Err() != nil {
			return "aborted", nil
		}
		select {
		case <-ctx.Done():
		case <-time.After(500 * time.Millisecond):
		}
	}
	return "completed", nil
}

func checkSignalState(ctx context.Context, _ struct{}) (signalState, error) {
	return signalState{Aborted: ctx.Err() != nil, Reason: reason(context.Cause(ctx))}, nil
}

// abortFromStep cancels the step's cancellation context after delayMs.
func abortFromStep(ctx context.Context, delayMs int) (struct{}, error) {
	if err := wait(ctx, time.Duration(delayMs)*time.Millisecond); err != nil {
		return struct{}{}, err
	}
	cancel, ok := workflow.CancelFunc(ctx)
	if !ok {
		return struct{}{}, errors.New("the step has no cancellation context")
	}
	return struct{}{}, cancel()
}

func stepThatThrowsIfAborted(ctx context.Context, _ struct{}) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", context.Cause(ctx)
	}
	return "should not reach here", nil
}

func stepWaitingOnAbortListener(ctx context.Context, _ struct{}) (listenerResult, error) {
	select {
	case <-ctx.Done():
		return listenerResult{Saw: true, Via: "listener"}, nil
	case <-time.After(30 * time.Second):
		return listenerResult{Saw: ctx.Err() != nil, Via: "timeout"}, nil
	}
}

func stepPollingThrowIfAborted(ctx context.Context, _ struct{}) (string, error) {
	for range 60 {
		if ctx.Err() != nil {
			return "", context.Cause(ctx)
		}
		select {
		case <-ctx.Done():
		case <-time.After(500 * time.Millisecond):
		}
	}
	return "completed", nil
}

// fetchWithSignal requests a loopback server that holds its response for
// 30s, with the step's context.
func fetchWithSignal(ctx context.Context, _ struct{}) (fetchResult, error) {
	startedAt := time.Now()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return fetchResult{}, err
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(30 * time.Second):
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"delayed":true}`))
		case <-r.Context().Done():
		}
	})}
	go func() { _ = server.Serve(listener) }()
	defer server.Close()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+listener.Addr().String()+"/delay", nil)
	if err != nil {
		return fetchResult{}, err
	}
	response, err := http.DefaultClient.Do(request)
	elapsed := time.Since(startedAt).Milliseconds()
	if err != nil {
		if ctx.Err() != nil {
			return fetchResult{Aborted: true, ElapsedMs: elapsed}, nil
		}
		return fetchResult{Error: err.Error(), ElapsedMs: elapsed}, nil
	}
	defer response.Body.Close()
	return fetchResult{OK: response.StatusCode < 300, Status: response.StatusCode, ElapsedMs: elapsed}, nil
}

func stepThatFetchesWithSignal(ctx context.Context, _ struct{}) (int, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://example.com", nil)
	if err != nil {
		return 0, err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	return response.StatusCode, nil
}

// reason returns a cancellation cause as the Workflow SDK reports an abort
// reason: an object error's fields, the message of another error, and
// nothing for a cancellation without a cause.
func reason(cause error) any {
	var object *reasonError
	switch {
	case cause == nil || errors.Is(cause, context.Canceled):
		return nil
	case errors.As(cause, &object):
		return object
	default:
		return cause.Error()
	}
}

// hookValue is the arguments of a step that resumes the hook Token with
// Value.
type hookValue struct {
	Token string
	Value int
}

func processAndSignalParent(ctx context.Context, in hookValue) (processedValue, error) {
	processed := processedValue{Processed: in.Value * 3}
	return processed, workflow.ResumeHook(ctx, in.Token, processed)
}

func signalParent(ctx context.Context, in hookValue) (struct{}, error) {
	return struct{}{}, workflow.ResumeHook(ctx, in.Token, in.Value)
}

// wait sleeps for d, or until ctx is done.
func wait(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
