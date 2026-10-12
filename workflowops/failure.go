package workflowops

import (
	"encoding/json"
	"errors"
	"strings"
)

const failurePrefix = "go-workflow/failure/v1:"

type failure struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// EncodeFailure returns the durable representation of cause. Known runtime
// failures retain a stable code; arbitrary application failures retain only
// their message.
func EncodeFailure(cause error) string {
	if cause == nil {
		return ""
	}
	value := failure{Message: cause.Error()}
	switch {
	case errors.Is(cause, ErrHookForceClaimed):
		value.Code = "hook_force_claimed"
	default:
		return value.Message
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return value.Message
	}
	return failurePrefix + string(encoded)
}

// DecodeFailure reconstructs a known runtime failure from its durable code.
// Messages without a workflow failure envelope are application errors.
func DecodeFailure(value string) error {
	if !strings.HasPrefix(value, failurePrefix) {
		return errors.New(value)
	}
	var failure failure
	if err := json.Unmarshal([]byte(strings.TrimPrefix(value, failurePrefix)), &failure); err != nil {
		return errors.New(value)
	}
	switch failure.Code {
	case "hook_force_claimed":
		return ErrHookForceClaimed
	default:
		return errors.New(failure.Message)
	}
}
