package worlds

import (
	"context"
	"errors"
	"log"
	"time"
)

// A run started from a workflow, from its code or one of its steps, records
// the starting run's ID in its executionContext under
// ExecutionContextCompletionParentRunID. When the run finishes, whoever
// recorded its terminal event resumes the parent's child-run completion hook,
// whose token is ChildRunCompletionToken of the parent, with a
// ChildRunCompletion. A parent that waits for a child's output suspends on
// that hook, which all its children share.
const (
	// ChildRunCompletionTokenPrefix is the prefix of a child-run completion
	// hook's token, which the parent's run ID follows.
	ChildRunCompletionTokenPrefix = "wkf_child_run_completion:"
	// ExecutionContextCompletionParentRunID is the executionContext key naming
	// the run whose completion hook a run resumes when it finishes.
	ExecutionContextCompletionParentRunID = "completionParentRunId"
)

// ChildRunCompletion is the payload a finished run resumes its parent's
// completion hook with.
type ChildRunCompletion struct {
	RunID  RunID  `json:"runId"`
	Status Status `json:"status"`
}

// ChildRunCompletionToken returns the token of parent's child-run completion
// hook.
func ChildRunCompletionToken(parent RunID) string {
	return ChildRunCompletionTokenPrefix + string(parent)
}

// CompletionParentRunID returns the run executionContext names as the one to
// notify when the run finishes, or "".
func CompletionParentRunID(executionContext map[string]any) RunID {
	parent, _ := executionContext[ExecutionContextCompletionParentRunID].(string)
	return RunID(parent)
}

// notifyRetryDelays are the waits between attempts to resume a parent's
// completion hook after a transient failure.
var notifyRetryDelays = []time.Duration{250 * time.Millisecond, time.Second, 3 * time.Second, 6 * time.Second}

// NotifyRunEnd resumes the completion hook of the run that started runID,
// named by executionContext, with runID and status, its terminal status. Call
// it after the run's terminal event is recorded: a parent reads its children
// after it registers the hook, so a resume that finds no hook is safe only
// once that read sees the run finished. A nil executionContext is read from
// the run.
//
// A missing hook is not an error: the parent is not waiting for a child, or
// has finished. Transient failures are retried, and a failure that remains
// is logged; a waiting parent notices the run finished at its next periodic
// check.
func NotifyRunEnd(ctx context.Context, world World, runID RunID, executionContext map[string]any, status Status) {
	if executionContext == nil {
		run, err := world.RunsGet(ctx, RunsGetInput{RunID: runID, ResolveData: ResolveDataNone})
		if err != nil {
			log.Printf("workflow: read run %s to notify its parent: %v", runID, err)
			return
		}
		executionContext = run.ExecutionContext
	}
	parent := CompletionParentRunID(executionContext)
	if parent == "" {
		return
	}
	resumeID := NewResumeID()
	for attempt := 0; ; attempt++ {
		err := resumeRunCompletion(ctx, world, parent, ChildRunCompletion{RunID: runID, Status: status}, resumeID)
		if err == nil || is[*HookNotFoundError](err) || is[*RunExpiredError](err) || errors.Is(err, ErrHookNotActive) {
			return
		}
		if attempt == len(notifyRetryDelays) || !errors.Is(err, ErrEntityConflict) && !IsRetryableWorldError(err) {
			log.Printf("workflow: resume the completion hook of run %s for run %s: %v", parent, runID, err)
			return
		}
		timer := time.NewTimer(notifyRetryDelays[attempt])
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func resumeRunCompletion(ctx context.Context, world World, parent RunID, payload ChildRunCompletion, resumeID string) error {
	hook, err := world.HooksGetByToken(ctx, HooksGetByTokenInput{Token: ChildRunCompletionToken(parent)})
	if err != nil {
		return err
	}
	options, err := HookPayloadOptions(ctx, NewClient(world), hook)
	if err != nil {
		return err
	}
	data, err := EncodePayload(payload, options)
	if err != nil {
		return err
	}
	_, err = ResumeHook(ctx, world, hook, data, resumeID)
	return err
}
