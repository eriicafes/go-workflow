package workflowai

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/digitallysavvy/go-ai/pkg/ai"
)

// ToolApprovalSecret names the environment variable that holds the secret
// the agent signs tool approval requests with, using HMAC. The agent verifies
// the signature before it executes an approved call from client-supplied
// messages, so a client cannot approve a call it was not asked about or
// change an approved call's input. Only the variable's name enters the run's
// steps; the secret is read inside them and never recorded.
type ToolApprovalSecret struct {
	EnvironmentVariable string `json:"environmentVariable"`
}

// toolApprovalSignatureInput is the input of signWorkflowToolApproval and
// verifyWorkflowToolApprovalSignature.
type toolApprovalSignatureInput struct {
	Secret     ToolApprovalSecret `json:"secret"`
	Signature  string             `json:"signature,omitempty"`
	ApprovalID string             `json:"approvalId"`
	ToolCallID string             `json:"toolCallId"`
	ToolName   string             `json:"toolName"`
	Input      json.RawMessage    `json:"input"`
}

// signWorkflowToolApproval returns the signature of an approval request.
func signWorkflowToolApproval(ctx context.Context, input toolApprovalSignatureInput) (string, error) {
	secret, err := input.Secret.value()
	if err != nil {
		return "", err
	}
	value, err := decodeInput(input.Input)
	if err != nil {
		return "", err
	}
	return ai.SignToolApproval(secret, input.ApprovalID, input.ToolCallID, input.ToolName, value)
}

// verifyWorkflowToolApprovalSignature reports whether an approval request's
// signature is valid.
func verifyWorkflowToolApprovalSignature(ctx context.Context, input toolApprovalSignatureInput) (bool, error) {
	secret, err := input.Secret.value()
	if err != nil {
		return false, err
	}
	value, err := decodeInput(input.Input)
	if err != nil {
		return false, err
	}
	return ai.VerifyToolApprovalSignature(secret, input.Signature, input.ApprovalID, input.ToolCallID, input.ToolName, value)
}

// value returns the secret the environment variable holds.
func (s ToolApprovalSecret) value() ([]byte, error) {
	secret, ok := os.LookupEnv(s.EnvironmentVariable)
	if !ok {
		return nil, fmt.Errorf("Tool approval secret environment variable %q is not set", s.EnvironmentVariable) //nolint:staticcheck // the Workflow SDK's error text
	}
	return []byte(secret), nil
}
