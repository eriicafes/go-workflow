package workflowai

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"

	"github.com/digitallysavvy/go-ai/pkg/ai"
	"github.com/digitallysavvy/go-ai/pkg/provider/types"
)

// Message roles.
const (
	RoleSystem    = "system"
	RoleUser      = "user"
	RoleAssistant = "assistant"
	RoleTool      = "tool"
)

// Content part types.
const (
	PartText                 = "text"
	PartReasoning            = "reasoning"
	PartFile                 = "file"
	PartToolCall             = "tool-call"
	PartToolResult           = "tool-result"
	PartToolApprovalRequest  = "tool-approval-request"
	PartToolApprovalResponse = "tool-approval-response"
)

// ModelMessage is a message of the conversation an agent runs on. Unlike
// go-ai's types.Message, it decodes from JSON, so it can cross step
// boundaries and be returned from a workflow.
type ModelMessage struct {
	Role            string         `json:"role"`
	Content         []ContentPart  `json:"content"`
	ProviderOptions map[string]any `json:"providerOptions,omitempty"`
}

// ContentPart is one part of a ModelMessage. Type selects which fields are
// set.
type ContentPart struct {
	Type string `json:"type"`

	// Text holds the text of text and reasoning parts.
	Text string `json:"text,omitempty"`
	// Signature holds a reasoning signature, or the HMAC signature of a
	// tool-approval-request.
	Signature        string `json:"signature,omitempty"`
	RedactedData     string `json:"redactedData,omitempty"`
	EncryptedContent string `json:"encryptedContent,omitempty"`

	// Data holds a file's base64 content, or its URL.
	Data      string `json:"data,omitempty"`
	MediaType string `json:"mediaType,omitempty"`
	Filename  string `json:"filename,omitempty"`

	ToolCallID       string            `json:"toolCallId,omitempty"`
	ToolName         string            `json:"toolName,omitempty"`
	Input            json.RawMessage   `json:"input,omitempty"`
	Output           *ToolResultOutput `json:"output,omitempty"`
	ProviderExecuted bool              `json:"providerExecuted,omitempty"`
	ThoughtSignature string            `json:"thoughtSignature,omitempty"`

	// ApprovalID identifies tool-approval-request and tool-approval-response
	// parts. Approved and Reason are set on responses.
	ApprovalID string `json:"approvalId,omitempty"`
	Approved   bool   `json:"approved,omitempty"`
	Reason     string `json:"reason,omitempty"`

	ProviderOptions  map[string]any  `json:"providerOptions,omitempty"`
	ProviderMetadata json.RawMessage `json:"providerMetadata,omitempty"`
}

// ToolResultOutput is the output of a tool-result part:
// "text" and "error-text" carry a string Value, "json" and "error-json" a JSON
// Value, and "execution-denied" an optional Reason.
type ToolResultOutput struct {
	Type   string          `json:"type"`
	Value  json.RawMessage `json:"value,omitempty"`
	Reason string          `json:"reason,omitempty"`
}

// ConvertToModelMessages converts UI messages from a chat client into model
// messages, keeping tool approval requests and responses so Stream can resume
// the tool calls they approve or deny. It is deterministic, so it can run in
// workflow context.
func ConvertToModelMessages(ctx context.Context, messages []ai.UIMessage) ([]ModelMessage, error) {
	converted, err := ai.ConvertToModelMessages(ctx, messages)
	if err != nil {
		return nil, err
	}
	return FromMessages(converted), nil
}

// FromMessages converts go-ai messages into model messages. Content types
// that have no ModelMessage equivalent are dropped.
func FromMessages(messages []types.Message) []ModelMessage {
	out := make([]ModelMessage, 0, len(messages))
	for _, message := range messages {
		converted := ModelMessage{Role: string(message.Role), ProviderOptions: message.ProviderOptions}
		for _, content := range message.Content {
			if part, ok := fromContentPart(content); ok {
				converted.Content = append(converted.Content, part)
			}
		}
		out = append(out, converted)
	}
	return out
}

func fromContentPart(content types.ContentPart) (ContentPart, bool) {
	switch c := content.(type) {
	case types.TextContent:
		return ContentPart{Type: PartText, Text: c.Text, ProviderOptions: c.ProviderOptions, ProviderMetadata: c.ProviderMetadata}, true
	case types.ReasoningContent:
		return ContentPart{
			Type:             PartReasoning,
			Text:             c.Text,
			Signature:        c.Signature,
			RedactedData:     c.RedactedData,
			EncryptedContent: c.EncryptedContent,
			ProviderOptions:  c.ProviderOptions,
			ProviderMetadata: c.ProviderMetadata,
		}, true
	case types.ImageContent:
		data := c.URL
		if data == "" {
			data = base64.StdEncoding.EncodeToString(c.Image)
		}
		return ContentPart{Type: PartFile, Data: data, MediaType: c.MimeType, ProviderOptions: c.ProviderOptions}, true
	case types.FileContent:
		part := ContentPart{Type: PartFile, MediaType: c.MediaType, Filename: c.Filename, ProviderOptions: c.ProviderOptions}
		if part.MediaType == "" {
			part.MediaType = c.MimeType
		}
		switch {
		case c.FileData.URL != "":
			part.Data = c.FileData.URL
		case len(c.FileData.Data) > 0:
			part.Data = base64.StdEncoding.EncodeToString(c.FileData.Data)
		case c.URL != "":
			part.Data = c.URL
		default:
			part.Data = base64.StdEncoding.EncodeToString(c.Data)
		}
		if part.MediaType == "" {
			part.MediaType = c.FileData.MediaType
		}
		return part, true
	case types.ToolCallContent:
		input := json.RawMessage(c.Input)
		if len(input) == 0 {
			input = marshalRaw(c.Arguments)
		}
		return ContentPart{
			Type:             PartToolCall,
			ToolCallID:       c.ToolCallID,
			ToolName:         c.ToolName,
			Input:            input,
			ProviderExecuted: c.ProviderExecuted,
			ThoughtSignature: c.ThoughtSignature,
			ProviderOptions:  c.ProviderOptions,
			ProviderMetadata: c.ProviderMetadata,
		}, true
	case types.ToolResultContent:
		part := ContentPart{
			Type:             PartToolResult,
			ToolCallID:       c.ToolCallID,
			ToolName:         c.ToolName,
			ProviderExecuted: c.ProviderExecuted,
			ProviderOptions:  c.ProviderOptions,
		}
		switch {
		case c.Output != nil:
			part.Output = &ToolResultOutput{Type: string(c.Output.Type), Reason: c.Output.Reason}
			if c.Output.Value != nil {
				part.Output.Value = marshalRaw(c.Output.Value)
			}
		case c.Error != "":
			part.Output = &ToolResultOutput{Type: "error-text", Value: marshalRaw(c.Error)}
		default:
			part.Output = jsonOutput(c.Result)
		}
		return part, true
	case types.ToolApprovalRequestContent:
		toolCallID := c.ToolCallID
		if toolCallID == "" {
			toolCallID = c.ToolCall.ID
		}
		return ContentPart{Type: PartToolApprovalRequest, ApprovalID: c.ApprovalID, ToolCallID: toolCallID, Signature: c.Signature}, true
	case types.ToolApprovalResponseContent:
		return ContentPart{
			Type:             PartToolApprovalResponse,
			ApprovalID:       c.ApprovalID,
			ToolCallID:       c.ToolCallID,
			Approved:         c.Approved,
			Reason:           c.Reason,
			ProviderExecuted: c.ProviderExecuted,
		}, true
	}
	return ContentPart{}, false
}

func toContentPart(part ContentPart) (types.ContentPart, bool) {
	switch part.Type {
	case PartText:
		return types.TextContent{Text: part.Text, ProviderOptions: part.ProviderOptions, ProviderMetadata: part.ProviderMetadata}, true
	case PartReasoning:
		return types.ReasoningContent{
			Text:             part.Text,
			Signature:        part.Signature,
			RedactedData:     part.RedactedData,
			EncryptedContent: part.EncryptedContent,
			ProviderOptions:  part.ProviderOptions,
			ProviderMetadata: part.ProviderMetadata,
		}, true
	case PartFile:
		file := types.FileContent{MediaType: part.MediaType, Filename: part.Filename, ProviderOptions: part.ProviderOptions}
		if isURL(part.Data) {
			file.FileData = types.FileData{Type: types.FileDataTypeURL, URL: part.Data, MediaType: part.MediaType}
		} else if data, err := base64.StdEncoding.DecodeString(part.Data); err == nil {
			file.Data = data
		} else {
			file.Text = part.Data
		}
		return file, true
	case PartToolCall:
		var arguments map[string]any
		_ = json.Unmarshal(part.Input, &arguments)
		input := string(part.Input)
		if input == "" {
			input = "{}"
		}
		return types.ToolCallContent{
			ToolCallID:       part.ToolCallID,
			ToolName:         part.ToolName,
			Input:            input,
			Arguments:        arguments,
			ProviderExecuted: part.ProviderExecuted,
			ThoughtSignature: part.ThoughtSignature,
			ProviderOptions:  part.ProviderOptions,
			ProviderMetadata: part.ProviderMetadata,
		}, true
	case PartToolResult:
		result := types.ToolResultContent{
			ToolCallID:       part.ToolCallID,
			ToolName:         part.ToolName,
			ProviderExecuted: part.ProviderExecuted,
			ProviderOptions:  part.ProviderOptions,
		}
		if part.Output != nil {
			output := &types.ToolResultOutput{Type: types.ToolResultOutputType(part.Output.Type), Reason: part.Output.Reason}
			if len(part.Output.Value) > 0 {
				var value any
				_ = json.Unmarshal(part.Output.Value, &value)
				output.Value = value
			}
			result.Output = output
		}
		return result, true
	case PartToolApprovalRequest:
		return types.ToolApprovalRequestContent{ApprovalID: part.ApprovalID, ToolCallID: part.ToolCallID, Signature: part.Signature}, true
	case PartToolApprovalResponse:
		return types.ToolApprovalResponseContent{
			ApprovalID:       part.ApprovalID,
			ToolCallID:       part.ToolCallID,
			Approved:         part.Approved,
			Reason:           part.Reason,
			ProviderExecuted: part.ProviderExecuted,
		}, true
	}
	return nil, false
}

func isURL(value string) bool {
	return strings.HasPrefix(value, "http://") || strings.HasPrefix(value, "https://") || strings.HasPrefix(value, "data:")
}

func marshalRaw(value any) json.RawMessage {
	data, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage("null")
	}
	return data
}

// jsonOutput returns the model output of a tool's result: text for a string
// and JSON for anything else.
func jsonOutput(value any) *ToolResultOutput {
	if text, ok := value.(string); ok {
		return &ToolResultOutput{Type: "text", Value: marshalRaw(text)}
	}
	if raw, ok := value.(json.RawMessage); ok {
		return &ToolResultOutput{Type: "json", Value: raw}
	}
	return &ToolResultOutput{Type: "json", Value: marshalRaw(value)}
}

// toProviderMessages converts model messages to go-ai messages.
func toProviderMessages(messages []ModelMessage) []types.Message {
	out := make([]types.Message, 0, len(messages))
	for _, message := range messages {
		converted := types.Message{Role: types.MessageRole(message.Role), ProviderOptions: message.ProviderOptions}
		for _, part := range message.Content {
			content, ok := toContentPart(part)
			if !ok {
				continue
			}
			converted.Content = append(converted.Content, content)
			if call, isCall := content.(types.ToolCallContent); isCall && !call.ProviderExecuted {
				converted.ToolCalls = append(converted.ToolCalls, types.ToolCall{
					ID:               call.ToolCallID,
					ToolName:         call.ToolName,
					Arguments:        call.Arguments,
					ThoughtSignature: call.ThoughtSignature,
				})
			}
		}
		if len(converted.Content) > 0 {
			out = append(out, converted)
		}
	}
	return out
}
