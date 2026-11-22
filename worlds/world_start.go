package worlds

import (
	"context"
	"errors"
	"time"
)

// startHookConfirmDelays are the waits before a start hook candidate's
// creation, or the queueing of an admitted one, is sent again.
var startHookConfirmDelays = []time.Duration{100 * time.Millisecond, 300 * time.Millisecond, 900 * time.Millisecond}

// startCandidate starts a run with a start hook, as StartRun says.
func startCandidate(ctx context.Context, world World, input EventsCreateRunInput) error {
	wait := func(attempt int) bool {
		if attempt >= len(startHookConfirmDelays) {
			return false
		}
		timer := time.NewTimer(startHookConfirmDelays[attempt])
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return false
		case <-timer.C:
			return true
		}
	}
	_, queueErr := world.Queue(ctx, StartQueueInput(input))
	if queueErr != nil && !IsRetryableWorldError(queueErr) {
		// Nothing was queued.
		return queueErr
	}
	// A delivery that may have been queued decides the candidate too, so
	// the candidate is decided here either way.
	for attempt := 0; ; attempt++ {
		_, err := world.EventsCreateRun(ctx, input)
		if err == nil {
			break
		}
		// A switched off admission recorded nothing, so it is no decision.
		if !errors.Is(err, ErrStartHookDisabled) && !IsRetryableWorldError(err) {
			return err
		}
		if !wait(attempt) {
			if queueErr != nil {
				return &StartError{RunID: input.RunID, Stage: StartStageQueue, Cause: queueErr}
			}
			return &StartError{RunID: input.RunID, Stage: StartStageAdmission, Cause: err}
		}
	}
	// The candidate is admitted. A delivery that may not have been queued is
	// queued again, so the run starts.
	for attempt := 0; queueErr != nil; attempt++ {
		if _, queueErr = world.Queue(ctx, StartQueueInput(input)); queueErr == nil {
			break
		}
		if !IsRetryableWorldError(queueErr) || !wait(attempt) {
			return &StartError{RunID: input.RunID, Stage: StartStageQueue, Cause: queueErr}
		}
	}
	return nil
}
