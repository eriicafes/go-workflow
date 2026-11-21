package core

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/eriicafes/go-workflow/worlds"
)

func TestEncodeRunFailedSetsTheSDKErrorCode(t *testing.T) {
	for _, test := range []struct {
		err  error
		want string
	}{
		{errors.New("boom"), "USER_ERROR"},
		{worlds.FatalError{Cause: fmt.Errorf("%w (48 attempts)", worlds.ErrMaxDeliveriesExceeded)}, "MAX_DELIVERIES_EXCEEDED"},
		{corruptedEventLogError{CorrelationID: "step_x"}, "CORRUPTED_EVENT_LOG"},
		{replayDivergenceError{CorrelationID: "step_x"}, "REPLAY_DIVERGENCE"},
		{deploymentMismatchError{}, "DEPLOYMENT_MISMATCH"},
		{fmt.Errorf("%w: wf", worlds.ErrWorkflowNotFound), "WORKFLOW_NOT_REGISTERED"},
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
