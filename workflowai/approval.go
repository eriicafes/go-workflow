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

// ToolApprovalFunc decides whether a tool call runs, is denied, or waits for
// the user's approval. It runs in workflow context, like NeedsApproval, so it
// must be deterministic.
type ToolApprovalFunc func(ctx context.Context, call ToolCall, options ToolApprovalOptions) (ToolApprovalStatus, error)

// ToolApprovalOptions describes the call a ToolApprovalFunc decides on.
type ToolApprovalOptions struct {
	// Messages is the conversation up to the step that made the call.
	Messages []ModelMessage
	// ToolContext is the tool's entry in ToolsContext.
	ToolContext    any
	ToolsContext   map[string]any
	RuntimeContext any
}

// ToolApprovalStatus is a ToolApprovalFunc's decision. Reason explains a
// denial to the model.
type ToolApprovalStatus struct {
	Type   ToolApprovalType
	Reason string
}

// ToolApprovalType is the kind of a ToolApprovalStatus.
type ToolApprovalType string

const (
	// ToolApprovalNotApplicable leaves the call to its tool's NeedsApproval.
	// It is the zero value.
	ToolApprovalNotApplicable ToolApprovalType = ""
	// ToolApprovalApproved runs the call without asking the user.
	ToolApprovalApproved ToolApprovalType = "approved"
	// ToolApprovalDenied reports the call to the model as denied without
	// running it.
	ToolApprovalDenied ToolApprovalType = "denied"
	// ToolApprovalUserApproval ends the turn with an approval request.
	ToolApprovalUserApproval ToolApprovalType = "user-approval"
)

// resolveApproval returns the approval status of a call, as the Workflow SDK
// resolves it: the agent's ToolApproval decides when it is set, otherwise the
// tool's entry in ToolApprovals, otherwise the tool's NeedsApproval.
func (r *run) resolveApproval(ctx context.Context, call ToolCall, messages []ModelMessage) (ToolApprovalStatus, error) {
	options := ToolApprovalOptions{Messages: messages, ToolContext: r.toolsContext[call.ToolName], ToolsContext: r.toolsContext, RuntimeContext: r.runtimeContext}
	if r.toolApproval != nil {
		return r.toolApproval(ctx, call, options)
	}
	if decide := r.toolApprovals[call.ToolName]; decide != nil {
		return decide(ctx, call, options)
	}
	tool := r.tools[call.ToolName]
	if tool.NeedsApproval == nil {
		return ToolApprovalStatus{}, nil
	}
	needsApproval, err := tool.NeedsApproval(ctx, call.Input, r.toolCallOptions(call, messages))
	if err != nil || !needsApproval {
		return ToolApprovalStatus{}, err
	}
	return ToolApprovalStatus{Type: ToolApprovalUserApproval}, nil
}

// automaticApproval returns the approval request and response parts that a
// call the approval policy approved or denied adds to its step's content.
func automaticApproval(call ToolCall, status ToolApprovalStatus) (request, response ContentPart) {
	id := "approval-" + call.ToolCallID
	request = ContentPart{Type: PartToolApprovalRequest, ApprovalID: id, ToolCallID: call.ToolCallID, Automatic: true}
	response = ContentPart{Type: PartToolApprovalResponse, ApprovalID: id, ToolCallID: call.ToolCallID, Approved: status.Type == ToolApprovalApproved, Reason: status.Reason, ProviderExecuted: call.ProviderExecuted}
	return request, response
}
