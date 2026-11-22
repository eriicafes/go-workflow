package core

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

func TestEncodeRunFailedSetsTheSDKErrorCode(t *testing.T) {
	for _, test := range []struct {
		err  error
		want string
	}{
		{errors.New("boom"), "USER_ERROR"},
		{worlds.FatalError{Cause: fmt.Errorf("%w (48 attempts)", worlds.ErrMaxDeliveriesExceeded)}, "MAX_DELIVERIES_EXCEEDED"},
		{fatalError("Workflow exceeded maximum queue deliveries (49/48)", worlds.ErrMaxDeliveriesExceeded), "MAX_DELIVERIES_EXCEEDED"},
		{fatalError("Workflow replay exceeded maximum duration (240s) after 4 attempts", worlds.ErrReplayTimeout), "REPLAY_TIMEOUT"},
		{maxEventsExceededError{count: 5, limit: 5}.recorded(), "MAX_EVENTS_EXCEEDED"},
		{corruptedEventLogError{CorrelationID: "step_x"}, "CORRUPTED_EVENT_LOG"},
		{replayDivergenceError{CorrelationID: "step_x"}, "REPLAY_DIVERGENCE"},
		{deploymentMismatchError{}, "DEPLOYMENT_MISMATCH"},
		{fmt.Errorf("%w: wf", worlds.ErrWorkflowNotRegistered), "WORKFLOW_NOT_REGISTERED"},
		{DeliveryError(errors.New("backend 503")), "WORLD_CONTRACT_ERROR"},
	} {
		data, err := worlds.EncodeRunFailed(test.err, worlds.PayloadOptions{})
		var failed worlds.DataRunFailed
		if err == nil {
			err = json.Unmarshal(data, &failed)
		}
		if err != nil || failed.ErrorCode != test.want {
			t.Errorf("EncodeRunFailed(%v) code = %q, want %q (err=%v)", test.err, failed.ErrorCode, test.want, err)
		}
	}
}

func TestStepFailuresAreRecordedAsFatalErrorsThatKeepTheirCause(t *testing.T) {
	cause := errors.New("boom")
	started := time.Date(2026, 3, 4, 5, 6, 7, 89_000_000, time.UTC)
	for _, test := range []struct {
		err  error
		want string
	}{
		{retriesExhaustedError("charge", 3, cause), `Step "charge" failed after 3 retries: boom`},
		{retriesExhaustedError("charge", 1, cause), `Step "charge" failed after 1 retry: boom`},
		{retriesExhaustedError("charge", 0, cause), `Step "charge" failed after 0 retries: boom`},
		{attemptsUsedError("charge", 3, 4, time.Time{}), `Step "charge" exceeded max retries (3 retries)`},
		{attemptsUsedError("charge", 1, 2, started), `Step "charge" exceeded max retries (1 retry): attempt 2 started at 2026-03-04T05:06:07.089Z and never recorded a result. The invocation running it likely crashed, ran out of memory, hit its timeout, or was stopped before it could record the result. Check that invocation's logs for the cause.`},
		{deliveriesExceededStepError("charge", 49), `Step "charge" failed with MAX_DELIVERIES_EXCEEDED: its queue message was delivered 49 times (more than 48) without the step starting`},
	} {
		payload, err := worlds.EncodeErrorPayload(test.err, worlds.PayloadOptions{})
		if err != nil {
			t.Fatal(err)
		}
		recorded := worlds.DecodeErrorPayload(payload, nil)
		var fatal worlds.FatalError
		if recorded.Type != "FatalError" || recorded.Message != test.want || !errors.As(recorded, &fatal) {
			t.Errorf("recorded %#v, want a FatalError saying %q", recorded, test.want)
		}
	}
	// The error a step ran out of retries with is the recorded error's
	// cause, with nothing between them.
	payload, _ := worlds.EncodeErrorPayload(retriesExhaustedError("charge", 3, cause), worlds.PayloadOptions{})
	recorded := worlds.DecodeErrorPayload(payload, nil)
	var fatal worlds.FatalError
	if !errors.Is(recorded, cause) || !errors.As(recorded, &fatal) || fatal.Cause == nil || fatal.Cause.Error() != "boom" || recorded.Cause == nil || recorded.Cause.Cause != nil {
		t.Fatalf("recorded = %#v, cause %#v", recorded, recorded.Cause)
	}
	payload, _ = worlds.EncodeErrorPayload(deliveriesExceededStepError("charge", 49), worlds.PayloadOptions{})
	if recorded := worlds.DecodeErrorPayload(payload, nil); !errors.Is(recorded, worlds.ErrMaxDeliveriesExceeded) {
		t.Fatalf("recorded %#v does not match ErrMaxDeliveriesExceeded", recorded)
	}
}
