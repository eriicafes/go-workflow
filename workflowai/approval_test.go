package workflowai_test

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/digitallysavvy/go-ai/pkg/provider/types"
	"github.com/eriicafes/go-workflow/workflowai"
)

func approve(context.Context, workflowai.ToolCall, workflowai.ToolApprovalOptions) (workflowai.ToolApprovalStatus, error) {
	return workflowai.ToolApprovalStatus{Type: workflowai.ToolApprovalApproved}, nil
}

func deny(context.Context, workflowai.ToolCall, workflowai.ToolApprovalOptions) (workflowai.ToolApprovalStatus, error) {
	return workflowai.ToolApprovalStatus{Type: workflowai.ToolApprovalDenied, Reason: "no weather today"}, nil
}

// The approval policy runs approved calls without asking the user, even when
// their tool needs approval, and reports denied calls to the model.
func TestStreamToolApprovalPolicy(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		setup(toolCalls(call("c1", "bookFlight", `{"flightId":"LH123"}`), call("c2", "getWeather", `{"city":"Lagos"}`)), text("Booked."))
		testOptions.ToolApprovals = map[string]workflowai.ToolApprovalFunc{"bookFlight": approve, "getWeather": deny}
		result, chunks, steps, err := chatWithSteps(t, []workflowai.ModelMessage{user("book LH123 and check the weather")})
		if err != nil {
			t.Fatal(err)
		}
		wantSteps := []string{"doStreamStep", "writeDeferredToolCalls", "bookFlight", "writeToolOutputDenied", "writeToolResults", "doStreamStep", "writeDeferredToolCalls", "closeStream"}
		if !slices.Equal(steps, wantSteps) {
			t.Fatalf("steps = %v, want %v", steps, wantSteps)
		}
		if weatherRuns != 0 || result.Text != "Booked." {
			t.Fatalf("weather ran %d times, result = %+v", weatherRuns, result)
		}
		var denied, output bool
		for _, chunk := range chunks {
			denied = denied || chunk["type"] == "tool-output-denied" && chunk["toolCallId"] == "c2"
			output = output || chunk["type"] == "tool-output-available" && chunk["toolCallId"] == "c1"
		}
		if !denied || !output {
			t.Fatalf("chunks = %v", chunkTypes(chunks))
		}
		// The decisions are recorded in the step's content as automatic
		// approvals.
		var approvals []string
		for _, part := range result.Steps[0].Content {
			switch part.Type {
			case workflowai.PartToolApprovalRequest:
				if part.Automatic {
					approvals = append(approvals, "request "+part.ToolCallID)
				}
			case workflowai.PartToolApprovalResponse:
				approvals = append(approvals, "response "+part.ToolCallID+" "+map[bool]string{true: "approved", false: "denied"}[part.Approved])
			}
		}
		if want := []string{"request c1", "request c2", "response c1 approved", "response c2 denied"}; !slices.Equal(approvals, want) {
			t.Fatalf("approvals = %v, want %v", approvals, want)
		}
		var sent string
		for _, message := range testModel.prompts()[1] {
			for _, part := range message.Content {
				if part, ok := part.(types.ToolResultContent); ok && part.ToolCallID == "c2" {
					data, _ := json.Marshal(part.Output)
					sent = string(data)
				}
			}
		}
		if !strings.Contains(sent, "execution-denied") || !strings.Contains(sent, "no weather today") {
			t.Fatalf("denied result = %s", sent)
		}
	})
}

// ToolApproval decides every call in place of the tools' NeedsApproval.
func TestStreamToolApprovalAsksTheUser(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		setup(toolCalls(call("c1", "getWeather", `{"city":"Lagos"}`)), text("unused"))
		testOptions.ToolApproval = func(context.Context, workflowai.ToolCall, workflowai.ToolApprovalOptions) (workflowai.ToolApprovalStatus, error) {
			return workflowai.ToolApprovalStatus{Type: workflowai.ToolApprovalUserApproval}, nil
		}
		_, chunks, steps, err := chatWithSteps(t, []workflowai.ModelMessage{user("weather in Lagos?")})
		if err != nil {
			t.Fatal(err)
		}
		if want := []string{"doStreamStep", "writeDeferredToolCalls", "writeApprovalRequests", "closeStream"}; !slices.Equal(steps, want) {
			t.Fatalf("steps = %v, want %v", steps, want)
		}
		if weatherRuns != 0 || !slices.Contains(chunkTypes(chunks), "tool-approval-request") {
			t.Fatalf("weather ran %d times, chunks = %v", weatherRuns, chunkTypes(chunks))
		}
	})
}

// The approval policy can deny a call the user approved.
func TestStreamToolApprovalPolicyDeniesApprovedCall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		setup(toolCalls(call("c1", "bookFlight", `{"flightId":"LH123"}`)), text("Not booked."))
		approvalTurn(t)
		testOptions.ToolApprovals = map[string]workflowai.ToolApprovalFunc{"bookFlight": func(context.Context, workflowai.ToolCall, workflowai.ToolApprovalOptions) (workflowai.ToolApprovalStatus, error) {
			return workflowai.ToolApprovalStatus{Type: workflowai.ToolApprovalDenied, Reason: "bookings are closed"}, nil
		}}
		_, _, steps, err := chatWithSteps(t, respond(t, "", true, `{"flightId":"LH123"}`))
		if err != nil {
			t.Fatal(err)
		}
		if want := []string{"writeApprovalToolResults", "doStreamStep", "writeDeferredToolCalls", "closeStream"}; !slices.Equal(steps, want) {
			t.Fatalf("steps = %v, want %v", steps, want)
		}
		var sent string
		for _, message := range testModel.prompts()[1] {
			for _, part := range message.Content {
				if part, ok := part.(types.ToolResultContent); ok && part.ToolCallID == "c1" {
					data, _ := json.Marshal(part.Output)
					sent = string(data)
				}
			}
		}
		if !strings.Contains(sent, "bookings are closed") {
			t.Fatalf("result = %s", sent)
		}
	})
}
