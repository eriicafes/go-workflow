package workflowai_test

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/digitallysavvy/go-ai/pkg/ai"
	"github.com/digitallysavvy/go-ai/pkg/provider"
	"github.com/digitallysavvy/go-ai/pkg/provider/types"
	"github.com/eriicafes/go-workflow/workflowai"
)

// approvalTurn runs a turn that stops for approval of a bookFlight call.
func approvalTurn(t *testing.T) workflowai.Result {
	t.Helper()
	result, chunks, err := chat(t, []workflowai.ModelMessage{user("book LH123")})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"start", "start-step", "reset-step", "tool-input-available", "tool-approval-request", "finish-step", "finish"}
	if got := chunkTypes(chunks); !slices.Equal(got, want) {
		t.Fatalf("chunks = %v, want %v", got, want)
	}
	if chunks[4]["approvalId"] != "approval-c1" || chunks[4]["toolCallId"] != "c1" {
		t.Fatalf("approval chunk = %v", chunks[4])
	}
	if len(result.ToolCalls) != 1 || len(result.ToolResults) != 0 {
		t.Fatalf("result = %+v", result)
	}
	return result
}

// respond returns the UI messages a useChat client sends after the user
// responds to the approval request of the first turn.
func respond(t *testing.T, signature string, approved bool, input string) []workflowai.ModelMessage {
	t.Helper()
	approval := map[string]any{"id": "approval-c1", "approved": approved, "reason": "changed my mind"}
	if signature != "" {
		approval["signature"] = signature
	}
	data, _ := json.Marshal([]map[string]any{
		{"id": "u1", "role": "user", "parts": []any{map[string]any{"type": "text", "text": "book LH123"}}},
		{"id": "a1", "role": "assistant", "parts": []any{
			map[string]any{"type": "step-start"},
			map[string]any{"type": "tool-bookFlight", "toolCallId": "c1", "state": "approval-responded", "input": json.RawMessage(input), "approval": approval},
		}},
	})
	var messages []ai.UIMessage
	if err := json.Unmarshal(data, &messages); err != nil {
		t.Fatal(err)
	}
	converted, err := workflowai.ConvertToModelMessages(t.Context(), messages)
	if err != nil {
		t.Fatal(err)
	}
	return converted
}

func TestStreamApprovedToolCall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		setup(toolCalls(call("c1", "bookFlight", `{"flightId":"LH123"}`)), text("Booked!"))
		testOptions.ToolApprovalSecret = []byte("0123456789abcdef0123456789abcdef")
		first := approvalTurn(t)
		var signature string
		for _, part := range first.Messages[len(first.Messages)-1].Content {
			if part.Type == workflowai.PartToolApprovalRequest {
				signature = part.Signature
			}
		}
		if signature == "" {
			t.Fatal("approval request is not signed")
		}

		result, chunks, err := chat(t, respond(t, signature, true, `{"flightId":"LH123"}`))
		if err != nil {
			t.Fatal(err)
		}
		want := []string{"start", "start-step", "tool-output-available", "finish-step", "start-step", "reset-step", "text-start", "text-delta", "text-end", "finish-step", "finish"}
		if got := chunkTypes(chunks); !slices.Equal(got, want) {
			t.Fatalf("chunks = %v, want %v", got, want)
		}
		if chunks[2]["output"] == nil || string(chunks[2]["output"].(json.RawMessage)) != `"booked LH123"` {
			t.Fatalf("output chunk = %v", chunks[2])
		}
		if result.Text != "Booked!" {
			t.Fatalf("text = %q", result.Text)
		}
		// The model sees the executed result and no approval parts.
		prompt := testModel.prompts()[1]
		last := prompt[len(prompt)-1]
		if last.Role != types.RoleTool || len(last.Content) != 1 {
			t.Fatalf("prompt = %+v", prompt)
		}
	})
}

func TestStreamDeniedToolCall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		setup(toolCalls(call("c1", "bookFlight", `{"flightId":"LH123"}`)), text("Okay, not booked."))
		approvalTurn(t)
		_, chunks, err := chat(t, respond(t, "", false, `{"flightId":"LH123"}`))
		if err != nil {
			t.Fatal(err)
		}
		if got := chunkTypes(chunks)[2]; got != "tool-output-denied" {
			t.Fatalf("chunks = %v", chunkTypes(chunks))
		}
		prompt := testModel.prompts()[1]
		result := prompt[len(prompt)-1].Content[0].(types.ToolResultContent)
		if result.Output.Type != types.ToolResultOutputExecutionDenied || result.Output.Reason != "changed my mind" {
			t.Fatalf("result = %+v", result.Output)
		}
	})
}

func TestStreamRejectsTamperedApproval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		setup(toolCalls(call("c1", "bookFlight", `{"flightId":"LH123"}`)), text("Something went wrong."))
		testOptions.ToolApprovalSecret = []byte("0123456789abcdef0123456789abcdef")
		first := approvalTurn(t)
		signature := first.Messages[len(first.Messages)-1].Content[1].Signature

		// The client changes the approved input.
		_, chunks, err := chat(t, respond(t, signature, true, `{"flightId":"XX999"}`))
		if err != nil {
			t.Fatal(err)
		}
		if chunks[2]["type"] != "tool-output-error" || !strings.Contains(chunks[2]["errorText"].(string), "invalid signature") {
			t.Fatalf("chunk = %v", chunks[2])
		}
	})
}

func TestStreamForwardsProviderApprovals(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		providerCall := types.ToolCall{ID: "p1", ToolName: "mcpSearch", RawArguments: `{"query":"flights"}`, ProviderExecuted: true}
		setup(func() ([]provider.StreamChunk, error) {
			return []provider.StreamChunk{
				{Type: provider.ChunkTypeToolCall, ToolCall: &providerCall},
				{Type: provider.ChunkTypeToolApprovalRequest, ToolApprovalRequest: &types.ToolApprovalRequestContent{ApprovalID: "mcp-1", ToolCallID: "p1"}},
				{Type: provider.ChunkTypeFinish, FinishReason: types.FinishReasonToolCalls},
			}, nil
		}, text("Found flights."))

		// The provider's approval request reaches the client and ends the
		// turn.
		_, chunks, err := chat(t, []workflowai.ModelMessage{user("search flights")})
		if err != nil {
			t.Fatal(err)
		}
		want := []string{"start", "start-step", "reset-step", "tool-input-available", "tool-approval-request", "finish-step", "finish"}
		if got := chunkTypes(chunks); !slices.Equal(got, want) {
			t.Fatalf("chunks = %v, want %v", got, want)
		}
		if chunks[4]["approvalId"] != "mcp-1" || chunks[4]["toolCallId"] != "p1" {
			t.Fatalf("approval chunk = %v", chunks[4])
		}

		// The user's response is sent to the provider with its request.
		data, _ := json.Marshal([]map[string]any{
			{"id": "u1", "role": "user", "parts": []any{map[string]any{"type": "text", "text": "search flights"}}},
			{"id": "a1", "role": "assistant", "parts": []any{
				map[string]any{"type": "step-start"},
				map[string]any{"type": "tool-mcpSearch", "toolCallId": "p1", "state": "approval-responded", "providerExecuted": true, "input": map[string]any{"query": "flights"}, "approval": map[string]any{"id": "mcp-1", "approved": true}},
			}},
		})
		var messages []ai.UIMessage
		if err := json.Unmarshal(data, &messages); err != nil {
			t.Fatal(err)
		}
		converted, err := workflowai.ConvertToModelMessages(t.Context(), messages)
		if err != nil {
			t.Fatal(err)
		}
		result, chunks, err := chat(t, converted)
		if err != nil {
			t.Fatal(err)
		}
		if result.Text != "Found flights." || slices.Contains(chunkTypes(chunks), "tool-output-denied") {
			t.Fatalf("result = %+v, chunks = %v", result, chunkTypes(chunks))
		}
		var request *types.ToolApprovalRequestContent
		var response *types.ToolApprovalResponseContent
		for _, message := range testModel.prompts()[1] {
			for _, part := range message.Content {
				switch part := part.(type) {
				case types.ToolApprovalRequestContent:
					request = &part
				case types.ToolApprovalResponseContent:
					response = &part
				}
			}
		}
		if request == nil || request.ApprovalID != "mcp-1" {
			t.Fatalf("approval request = %+v", request)
		}
		if response == nil || response.ApprovalID != "mcp-1" || !response.Approved || !response.ProviderExecuted {
			t.Fatalf("approval response = %+v", response)
		}
	})
}

func TestStreamProviderApprovalPausesAfterLocalCalls(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		providerCall := types.ToolCall{ID: "p1", ToolName: "mcpSearch", RawArguments: `{}`, ProviderExecuted: true}
		localCall := call("c1", "getWeather", `{"city":"Lagos"}`)
		setup(func() ([]provider.StreamChunk, error) {
			return []provider.StreamChunk{
				{Type: provider.ChunkTypeToolCall, ToolCall: &localCall},
				{Type: provider.ChunkTypeToolCall, ToolCall: &providerCall},
				{Type: provider.ChunkTypeToolApprovalRequest, ToolApprovalRequest: &types.ToolApprovalRequestContent{ApprovalID: "mcp-1", ToolCallID: "p1"}},
				{Type: provider.ChunkTypeFinish, FinishReason: types.FinishReasonToolCalls},
			}, nil
		}, text("unused"))
		_, chunks, err := chat(t, []workflowai.ModelMessage{user("weather and search")})
		if err != nil {
			t.Fatal(err)
		}
		// The local call executes, then the turn waits for the user instead
		// of calling the model again.
		want := []string{"start", "start-step", "reset-step", "tool-input-available", "tool-input-available", "tool-approval-request", "tool-output-available", "finish-step", "finish"}
		if got := chunkTypes(chunks); !slices.Equal(got, want) {
			t.Fatalf("chunks = %v, want %v", got, want)
		}
		if weatherRuns != 1 || len(testModel.calls) != 1 {
			t.Fatalf("weather ran %d times, %d model calls", weatherRuns, len(testModel.calls))
		}
	})
}
