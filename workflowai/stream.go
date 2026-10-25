package workflowai

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/digitallysavvy/go-ai/pkg/ai"
	"github.com/digitallysavvy/go-ai/pkg/provider/types"
	"github.com/eriicafes/go-workflow"
)

// ModelCallStreamPart is one part of the raw stream an agent writes to its
// Writer. Each part is one stream chunk, encoded as JSON.
// Read the stream through UIMessageChunks to send it to a chat client.
type ModelCallStreamPart struct {
	Type string `json:"type"`

	// ID identifies text, reasoning, tool input, and source parts.
	ID string `json:"id,omitempty"`
	// Text holds the text of a text-delta or reasoning-delta.
	Text string `json:"text,omitempty"`
	// Delta holds the input text of a tool-input-delta.
	Delta string `json:"delta,omitempty"`

	ToolCallID       string          `json:"toolCallId,omitempty"`
	ToolName         string          `json:"toolName,omitempty"`
	Input            json.RawMessage `json:"input,omitempty"`
	Output           json.RawMessage `json:"output,omitempty"`
	Invalid          bool            `json:"invalid,omitempty"`
	ProviderExecuted bool            `json:"providerExecuted,omitempty"`

	// Error holds the message of tool-error, tool-call parts that are
	// invalid, and error parts.
	Error string `json:"error,omitempty"`

	ApprovalID string `json:"approvalId,omitempty"`
	Signature  string `json:"signature,omitempty"`

	// MediaType and Data hold a file's media type and base64 data.
	MediaType string `json:"mediaType,omitempty"`
	Data      string `json:"data,omitempty"`

	SourceType string `json:"sourceType,omitempty"`
	URL        string `json:"url,omitempty"`
	Title      string `json:"title,omitempty"`
	Filename   string `json:"filename,omitempty"`

	FinishReason string       `json:"finishReason,omitempty"`
	Usage        *types.Usage `json:"usage,omitempty"`

	ProviderMetadata json.RawMessage `json:"providerMetadata,omitempty"`

	// RawValue holds an unprocessed provider chunk of a raw part, written
	// when CallOptions.IncludeRawChunks is set.
	RawValue json.RawMessage `json:"rawValue,omitempty"`
}

// ToUIMessageChunk converts a ModelCallStreamPart to the UI message chunk a
// chat client receives. It returns false for parts that have no UI chunk.
func ToUIMessageChunk(part ModelCallStreamPart) (ai.UIMessageChunk, bool) {
	chunk := ai.UIMessageChunk{"type": part.Type}
	withMetadata := func() {
		if len(part.ProviderMetadata) > 0 {
			chunk["providerMetadata"] = part.ProviderMetadata
		}
	}
	switch part.Type {
	case "reset-step", "start-step", "finish-step":
	case "text-start", "text-end", "reasoning-start", "reasoning-end":
		chunk["id"] = part.ID
		withMetadata()
	case "text-delta", "reasoning-delta":
		chunk["id"] = part.ID
		chunk["delta"] = part.Text
		withMetadata()
	case "file":
		chunk["mediaType"] = part.MediaType
		chunk["url"] = fmt.Sprintf("data:%s;base64,%s", part.MediaType, part.Data)
	case "source":
		switch part.SourceType {
		case "url":
			chunk = ai.UIMessageChunk{"type": "source-url", "sourceId": part.ID, "url": part.URL}
		case "document":
			chunk = ai.UIMessageChunk{"type": "source-document", "sourceId": part.ID, "mediaType": part.MediaType}
			if part.Filename != "" {
				chunk["filename"] = part.Filename
			}
		default:
			return nil, false
		}
		if part.Title != "" {
			chunk["title"] = part.Title
		}
		withMetadata()
	case "tool-input-start":
		chunk["toolCallId"] = part.ID
		chunk["toolName"] = part.ToolName
		if part.ProviderExecuted {
			chunk["providerExecuted"] = true
		}
	case "tool-input-delta":
		chunk["toolCallId"] = part.ID
		chunk["inputTextDelta"] = part.Delta
	case "tool-call":
		chunk["toolCallId"] = part.ToolCallID
		chunk["toolName"] = part.ToolName
		chunk["input"] = rawOrNull(part.Input)
		if part.Invalid {
			chunk["type"] = "tool-input-error"
			chunk["errorText"] = part.Error
			break
		}
		chunk["type"] = "tool-input-available"
		if part.ProviderExecuted {
			chunk["providerExecuted"] = true
		}
		withMetadata()
	case "tool-result":
		chunk = ai.UIMessageChunk{"type": "tool-output-available", "toolCallId": part.ToolCallID, "output": rawOrNull(part.Output)}
		if part.ProviderExecuted {
			chunk["providerExecuted"] = true
		}
	case "tool-error":
		chunk = ai.UIMessageChunk{"type": "tool-output-error", "toolCallId": part.ToolCallID, "errorText": part.Error}
		if part.ProviderExecuted {
			chunk["providerExecuted"] = true
		}
	case "tool-output-denied":
		chunk["toolCallId"] = part.ToolCallID
	case "tool-approval-request":
		chunk["approvalId"] = part.ApprovalID
		chunk["toolCallId"] = part.ToolCallID
		if part.Signature != "" {
			chunk["signature"] = part.Signature
		}
	case "error":
		chunk["errorText"] = part.Error
	default:
		// tool-input-end, model-call-start, model-call-end, and finish have
		// no UI chunk.
		return nil, false
	}
	return chunk, true
}

// UIMessageChunks reads the ModelCallStreamParts of a run stream and converts
// them to UI message chunks, wrapping them in start and finish chunks, like
// createModelCallToUIChunkTransform. The channel closes when the stream
// closes, or without the finish chunks when reading fails, so a
// WorkflowChatTransport client reconnects.
//
// Parts and UI chunks are not one to one, so a reconnecting client's chunk
// index is not a stream index: read the stream from index 0 and pass the
// number of UI chunks the client already received as uiStartIndex.
func UIMessageChunks(ctx context.Context, reader workflow.StreamReader, uiStartIndex int) <-chan ai.UIMessageChunk {
	chunks := make(chan ai.UIMessageChunk)
	go func() {
		defer close(chunks)
		index := 0
		send := func(chunk ai.UIMessageChunk) bool {
			index++
			if index <= uiStartIndex {
				return true
			}
			select {
			case chunks <- chunk:
				return true
			case <-ctx.Done():
				return false
			}
		}
		if !send(ai.UIMessageChunk{"type": "start"}) || !send(ai.UIMessageChunk{"type": "start-step"}) {
			return
		}
		for {
			data, err := reader.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return
			}
			var part ModelCallStreamPart
			if err := json.Unmarshal(data.Data, &part); err != nil {
				return
			}
			if chunk, ok := ToUIMessageChunk(part); ok && !send(chunk) {
				return
			}
		}
		if send(ai.UIMessageChunk{"type": "finish-step"}) {
			send(ai.UIMessageChunk{"type": "finish"})
		}
	}()
	return chunks
}

// writePart writes part to w as one chunk.
func writePart(w io.Writer, part ModelCallStreamPart) error {
	data, err := json.Marshal(part)
	if err != nil {
		return err
	}
	_, err = w.Write(data)
	return err
}

func rawOrNull(value json.RawMessage) json.RawMessage {
	if len(value) == 0 {
		return json.RawMessage("null")
	}
	return value
}
