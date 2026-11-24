package workflowai

import (
	"context"
	"encoding/json"

	"github.com/eriicafes/go-workflow"
)

// The steps below write the agent's own parts to a call's stream. Streams can
// only be written in step context, so each runs as a step, named and ordered
// like the Workflow SDK's WorkflowAgent steps.

// writeToolResultsInput is the input of writeToolResults.
type writeToolResultsInput struct {
	Writer  workflow.WriterOption `json:"writer"`
	Results []ToolResult          `json:"results"`
	// WriteStepBoundary follows the results with finish-step and start-step
	// parts, which separate the tool call step from the next model call in
	// the UI message.
	WriteStepBoundary bool `json:"writeStepBoundary,omitempty"`
}

// writeToolResults writes a tool-result part for each result, or a
// tool-error part for each failed one.
func writeToolResults(ctx context.Context, input writeToolResultsInput) (struct{}, error) {
	parts := make([]ModelCallStreamPart, 0, len(input.Results)+2)
	for _, result := range input.Results {
		parts = append(parts, resultPart(result))
	}
	if input.WriteStepBoundary {
		parts = append(parts, ModelCallStreamPart{Type: "finish-step"}, ModelCallStreamPart{Type: "start-step"})
	}
	return struct{}{}, writeParts(ctx, input.Writer, parts)
}

// writeApprovalToolResultsInput is the input of writeApprovalToolResults.
type writeApprovalToolResultsInput struct {
	Writer          workflow.WriterOption `json:"writer"`
	ApprovedResults []ToolResult          `json:"approvedResults"`
	DeniedResults   []deniedResult        `json:"deniedResults"`
}

// writeApprovalToolResults writes the results of the approved calls a turn
// resumes, as tool-result parts even when they failed, then a
// tool-output-denied part for each denied call, then a step boundary.
func writeApprovalToolResults(ctx context.Context, input writeApprovalToolResultsInput) (struct{}, error) {
	parts := make([]ModelCallStreamPart, 0, len(input.ApprovedResults)+len(input.DeniedResults)+2)
	for _, result := range input.ApprovedResults {
		parts = append(parts, ModelCallStreamPart{Type: "tool-result", ToolCallID: result.ToolCallID, ToolName: result.ToolName, Input: result.Input, Output: rawOrNull(result.Output)})
	}
	for _, denied := range input.DeniedResults {
		parts = append(parts, ModelCallStreamPart{Type: "tool-output-denied", ToolCallID: denied.ToolCallID})
	}
	parts = append(parts, ModelCallStreamPart{Type: "finish-step"}, ModelCallStreamPart{Type: "start-step"})
	return struct{}{}, writeParts(ctx, input.Writer, parts)
}

// writeApprovalRequestsInput is the input of writeApprovalRequests.
type writeApprovalRequestsInput struct {
	Writer           workflow.WriterOption `json:"writer"`
	ApprovalRequests []approvalRequest     `json:"approvalRequests"`
}

// writeApprovalRequests writes a tool-approval-request part for each request,
// which a chat client shows to the user.
func writeApprovalRequests(ctx context.Context, input writeApprovalRequestsInput) (struct{}, error) {
	parts := make([]ModelCallStreamPart, 0, len(input.ApprovalRequests))
	for _, request := range input.ApprovalRequests {
		parts = append(parts, ModelCallStreamPart{Type: "tool-approval-request", ApprovalID: request.ApprovalID, ToolCallID: request.ToolCallID, Signature: request.Signature})
	}
	return struct{}{}, writeParts(ctx, input.Writer, parts)
}

// writeToolOutputDeniedInput is the input of writeToolOutputDenied.
type writeToolOutputDeniedInput struct {
	Writer        workflow.WriterOption `json:"writer"`
	DeniedResults []deniedResult        `json:"deniedResults"`
}

// writeToolOutputDenied writes a tool-output-denied part for each call the
// approval policy denied.
func writeToolOutputDenied(ctx context.Context, input writeToolOutputDeniedInput) (struct{}, error) {
	parts := make([]ModelCallStreamPart, 0, len(input.DeniedResults))
	for _, denied := range input.DeniedResults {
		parts = append(parts, ModelCallStreamPart{Type: "tool-output-denied", ToolCallID: denied.ToolCallID})
	}
	return struct{}{}, writeParts(ctx, input.Writer, parts)
}

// writeDeferredToolCallsInput is the input of writeDeferredToolCalls.
type writeDeferredToolCallsInput struct {
	Writer    workflow.WriterOption `json:"writer"`
	ToolCalls []ToolCall            `json:"toolCalls"`
}

// writeDeferredToolCalls writes the tool-call parts a model call step leaves
// to the workflow, each invalid one followed by its tool-error part. The
// Workflow SDK defers the calls of tools with their own input validator,
// which Go tools do not have, so the agent runs it with no calls after every
// streamed model call, as the SDK does.
func writeDeferredToolCalls(ctx context.Context, input writeDeferredToolCallsInput) (struct{}, error) {
	parts := make([]ModelCallStreamPart, 0, len(input.ToolCalls))
	for _, call := range input.ToolCalls {
		parts = append(parts, ModelCallStreamPart{Type: "tool-call", ToolCallID: call.ToolCallID, ToolName: call.ToolName, Input: call.Input, Invalid: call.Invalid, Error: call.Error, ProviderExecuted: call.ProviderExecuted})
		if call.Invalid && !call.ProviderExecuted {
			parts = append(parts, ModelCallStreamPart{Type: "tool-error", ToolCallID: call.ToolCallID, ToolName: call.ToolName, Input: call.Input, Error: call.Error})
		}
	}
	return struct{}{}, writeParts(ctx, input.Writer, parts)
}

// closeStreamInput is the input of closeStream.
type closeStreamInput struct {
	Writer       workflow.WriterOption `json:"writer"`
	PreventClose bool                  `json:"preventClose,omitempty"`
	SendFinish   bool                  `json:"sendFinish,omitempty"`
}

// closeStream writes a finish part when SendFinish is set, and closes the
// stream unless PreventClose is set.
func closeStream(ctx context.Context, input closeStreamInput) (struct{}, error) {
	writer, err := workflow.Writer(ctx, input.Writer)
	if err != nil {
		return struct{}{}, err
	}
	defer writer.Close()
	if input.SendFinish {
		if err := writePart(writer, ModelCallStreamPart{Type: "finish"}); err != nil {
			return struct{}{}, err
		}
	}
	if !input.PreventClose {
		return struct{}{}, writer.CloseStream()
	}
	return struct{}{}, writer.Close()
}

// approvalRequest is an approval request a paused turn writes.
type approvalRequest struct {
	ApprovalID string `json:"approvalId"`
	ToolCallID string `json:"toolCallId"`
	Signature  string `json:"signature,omitempty"`
}

// deniedResult names a call whose approval was denied.
type deniedResult struct {
	ToolCallID string `json:"toolCallId"`
}

// writeParts writes parts to the stream writer names.
func writeParts(ctx context.Context, option workflow.WriterOption, parts []ModelCallStreamPart) error {
	writer, err := workflow.Writer(ctx, option)
	if err != nil {
		return err
	}
	defer writer.Close()
	for _, part := range parts {
		if err := writePart(writer, part); err != nil {
			return err
		}
	}
	return writer.Close()
}

// resultPart returns the stream part of a tool result: a tool-error part for
// a failed call, and a tool-result part otherwise.
func resultPart(result ToolResult) ModelCallStreamPart {
	if result.IsError {
		var message string
		if json.Unmarshal(result.Output, &message) != nil {
			message = string(result.Output)
		}
		return ModelCallStreamPart{Type: "tool-error", ToolCallID: result.ToolCallID, ToolName: result.ToolName, Input: result.Input, Error: message}
	}
	return ModelCallStreamPart{Type: "tool-result", ToolCallID: result.ToolCallID, ToolName: result.ToolName, Input: result.Input, Output: rawOrNull(result.Output)}
}
