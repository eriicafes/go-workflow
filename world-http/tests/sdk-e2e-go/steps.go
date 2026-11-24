package sdke2e

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
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
