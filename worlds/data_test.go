package worlds

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestEncodeRunFailedSetsTheSDKErrorCode(t *testing.T) {
	for _, test := range []struct {
		err  error
		want string
	}{
		{errors.New("boom"), "USER_ERROR"},
		{FatalError{Cause: fmt.Errorf("%w (48 attempts)", ErrMaxDeliveriesExceeded)}, "MAX_DELIVERIES_EXCEEDED"},
		{corruptedEventLogError{CorrelationID: "step_x"}, "CORRUPTED_EVENT_LOG"},
		{replayDivergenceError{CorrelationID: "step_x"}, "REPLAY_DIVERGENCE"},
		{deploymentMismatchError{}, "DEPLOYMENT_MISMATCH"},
		{fmt.Errorf("%w: wf", ErrWorkflowNotFound), "WORKFLOW_NOT_REGISTERED"},
		{DeliveryError(errors.New("backend 503")), "WORLD_CONTRACT_ERROR"},
	} {
		data, err := EncodeRunFailed(test.err, PayloadOptions{})
		var failed DataRunFailed
		if err == nil {
			err = json.Unmarshal(data, &failed)
		}
		if err != nil || failed.ErrorCode != test.want {
			t.Errorf("EncodeRunFailed(%v) code = %q, want %q (err=%v)", test.err, failed.ErrorCode, test.want, err)
		}
	}
}

func TestCheckAttributesMatchesTheSDKLimits(t *testing.T) {
	many := map[string]string{}
	for index := range MaxAttributesPerRun + 1 {
		many[fmt.Sprint(index)] = "v"
	}
	// Keys count UTF-16 code units: an astral rune is two.
	astral := strings.Repeat("😀", MaxAttributeKeyLength/2)
	for _, test := range []struct {
		attributes    map[string]string
		allowReserved bool
		valid         bool
	}{
		{map[string]string{astral: "v"}, false, true},
		{map[string]string{astral + "x": "v"}, false, false},
		{map[string]string{"$parentRunId": "v"}, true, true},
		{map[string]string{"$parentRunId": "v"}, false, false},
		{many, false, false},
	} {
		err := CheckAttributes(test.attributes, test.allowReserved)
		if (err == nil) != test.valid || (err != nil && !errors.Is(err, ErrAttributeInvalid)) {
			t.Errorf("CheckAttributes(%d attributes, %t) = %v, want valid %t", len(test.attributes), test.allowReserved, err, test.valid)
		}
	}
}
