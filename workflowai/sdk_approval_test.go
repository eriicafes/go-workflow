package workflowai_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/digitallysavvy/go-ai/pkg/ai"
	"github.com/digitallysavvy/go-ai/pkg/provider"
	"github.com/digitallysavvy/go-ai/pkg/provider/types"
	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/workflowai"
	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/testworld"
)

const (
	sdkApprovalSecret         = "approval-test-secret"
	sdkApprovalSecretVariable = "WORKFLOW_TOOL_APPROVAL_SECRET"
)

// sdkApprovalModes are the agent calls the ports run each test with.
var sdkApprovalModes = []string{"generate", "stream"}

// AI SDK: packages/workflow/src/workflow-agent-approval.test.ts > issues signed data and resumes without a writable (approved: %s)
func TestSDKApprovalSignedResume(t *testing.T) {
	for _, mode := range sdkApprovalModes {
		for _, approved := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/approved=%v", mode, approved), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					sdkApprovalSetup(sdkApprovalResponse("", sdkApprovalCall()), sdkApprovalResponse("Done"))
					testOptions.Tools = workflowai.ToolSet{"action": sdkApprovalTool("action", "executed", workflowai.NeedsApprovalAlways)}
					sdkApprovalSign(t)
					issued := sdkApprovalRun(t, mode, sdkApprovalPrompt())
					if issued.err != nil {
						t.Fatal(issued.err)
					}
					request, ok := sdkApprovalFind(sdkApprovalContent(issued.result), workflowai.PartToolApprovalRequest)
					if !ok || request.ApprovalID != "approval-call-1" || request.Signature == "" || request.ToolCallID != "call-1" {
						t.Fatalf("approval request = %+v", request)
					}
					if runs := sdkApprovalExecutions.count("action"); runs != 0 {
						t.Fatalf("action ran %d times", runs)
					}
					data, _ := json.Marshal(issued.result)
					if strings.Contains(string(data), sdkApprovalSecret) {
						t.Fatal("the result holds the secret")
					}
					valid, err := ai.VerifyToolApprovalSignature([]byte(sdkApprovalSecret), request.Signature, request.ApprovalID, "call-1", "action", map[string]any{"value": "requested"})
					if err != nil || !valid {
						t.Fatalf("signature valid = %v, %v", valid, err)
					}

					resumed := sdkApprovalRun(t, mode, sdkApprovalRespond(issued.result.Messages, "approval-call-1", approved))
					if resumed.err != nil {
						t.Fatal(resumed.err)
					}
					if resumed.result.Text != "Done" {
						t.Fatalf("text = %q", resumed.result.Text)
					}
					if runs, want := sdkApprovalExecutions.count("action"), map[bool]int{true: 1, false: 0}[approved]; runs != want {
						t.Fatalf("action ran %d times, want %d", runs, want)
					}
					if mode == "generate" {
						results := sdkApprovalToolResultParts(resumed.result.Messages)
						if len(results) != 1 {
							t.Fatalf("tool results = %+v", results)
						}
						output := results[0].Output
						if approved && (output.Type != "text" || string(output.Value) != `"executed"`) || !approved && output.Type != "execution-denied" {
							t.Fatalf("output = %+v", output)
						}
					}
				})
			})
		}
	}
}

// AI SDK: packages/workflow/src/workflow-agent-approval.test.ts > resumes only the first of repeated approval responses (approved: %s)
func TestSDKApprovalRepeatedResponses(t *testing.T) {
	for _, mode := range sdkApprovalModes {
		for _, approved := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/approved=%v", mode, approved), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					sdkApprovalSetup(sdkApprovalResponse("", sdkApprovalCall()), sdkApprovalResponse("Done"))
					testOptions.Tools = workflowai.ToolSet{"action": sdkApprovalTool("action", "executed", workflowai.NeedsApprovalAlways)}
					issued := sdkApprovalRun(t, mode, sdkApprovalPrompt())
					if issued.err != nil {
						t.Fatal(issued.err)
					}
					response := workflowai.ContentPart{Type: workflowai.PartToolApprovalResponse, ApprovalID: "approval-call-1", Approved: approved}
					flipped := response
					flipped.Approved = !approved
					messages := append(slices.Clone(issued.result.Messages), workflowai.ModelMessage{
						Role:    workflowai.RoleTool,
						Content: []workflowai.ContentPart{response, response, flipped},
					})
					resumed := sdkApprovalRun(t, mode, messages)
					if resumed.err != nil {
						t.Fatal(resumed.err)
					}
					if runs, want := sdkApprovalExecutions.count("action"), map[bool]int{true: 1, false: 0}[approved]; runs != want {
						t.Fatalf("action ran %d times, want %d", runs, want)
					}
					if mode == "generate" {
						results := sdkApprovalToolResultParts(resumed.result.Messages)
						if len(results) != 1 || results[0].ToolCallID != "call-1" {
							t.Fatalf("tool results = %+v", results)
						}
						output := results[0].Output
						if approved && (output.Type != "text" || string(output.Value) != `"executed"`) || !approved && output.Type != "execution-denied" {
							t.Fatalf("output = %+v", output)
						}
					}
				})
			})
		}
	}
}

// AI SDK: packages/workflow/src/workflow-agent-approval.test.ts > rejects tampered approval input before executing the tool
func TestSDKApprovalRejectsTamperedInput(t *testing.T) {
	for _, mode := range sdkApprovalModes {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				sdkApprovalSetup(sdkApprovalResponse("", sdkApprovalCall()), sdkApprovalResponse("Done"))
				testOptions.Tools = workflowai.ToolSet{"action": sdkApprovalTool("action", "executed", workflowai.NeedsApprovalAlways)}
				sdkApprovalSign(t)
				issued := sdkApprovalRun(t, mode, sdkApprovalPrompt())
				if issued.err != nil {
					t.Fatal(issued.err)
				}
				messages := sdkApprovalTamper(sdkApprovalRespond(issued.result.Messages, "approval-call-1", true))
				if resumed := sdkApprovalRun(t, mode, messages); resumed.err != nil {
					t.Fatal(resumed.err)
				}
				if runs := sdkApprovalExecutions.count("action"); runs != 0 {
					t.Fatalf("action ran %d times", runs)
				}
			})
		})
	}
}

// AI SDK: packages/workflow/src/workflow-agent-approval.test.ts > does not execute an approved tool after its %s changes
func TestSDKApprovalToolChanges(t *testing.T) {
	for _, mode := range sdkApprovalModes {
		for _, change := range []string{"schema", "policy", "removed"} {
			t.Run(mode+"/"+change, func(t *testing.T) {
				if change == "removed" {
				}
				synctest.Test(t, func(t *testing.T) {
					sdkApprovalSetup(sdkApprovalResponse("", sdkApprovalCall()), sdkApprovalResponse("Done"))
					original := sdkApprovalTool("action", "executed", workflowai.NeedsApprovalAlways)
					testOptions.Tools = workflowai.ToolSet{"action": original}
					issued := sdkApprovalRun(t, mode, sdkApprovalPrompt())
					if issued.err != nil {
						t.Fatal(issued.err)
					}
					switch change {
					case "removed":
						testOptions.Tools = workflowai.ToolSet{}
					case "schema":
						changed := original
						changed.InputSchema = map[string]any{"type": "object", "properties": map[string]any{"value": map[string]any{"type": "number"}}, "required": []any{"value"}}
						testOptions.Tools = workflowai.ToolSet{"action": changed}
					case "policy":
						changed := original
						changed.NeedsApproval = nil
						testOptions.Tools = workflowai.ToolSet{"action": changed}
					}
					resumed := sdkApprovalRun(t, mode, sdkApprovalRespond(issued.result.Messages, "approval-call-1", true))
					if change == "removed" {
						if resumed.err == nil || !strings.Contains(resumed.err.Error(), "Tool result is missing") {
							t.Fatalf("err = %v", resumed.err)
						}
					} else if resumed.err != nil {
						t.Fatal(resumed.err)
					}
					if runs := sdkApprovalExecutions.count("action"); runs != 0 {
						t.Fatalf("action ran %d times", runs)
					}
				})
			})
		}
	}
}

// AI SDK: packages/workflow/src/workflow-agent-approval.test.ts > retains deferred provider results and local tool errors
func TestSDKApprovalDeferredProviderResultsAndToolErrors(t *testing.T) {
	for _, mode := range sdkApprovalModes {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				remote := types.ToolCall{ID: "remote-call", ToolName: "remote", RawArguments: `{"value":"requested"}`, ProviderExecuted: true}
				local := sdkApprovalCall()
				sdkApprovalSetup(
					func() ([]provider.StreamChunk, error) {
						return []provider.StreamChunk{
							{Type: provider.ChunkTypeToolCall, ToolCall: &remote},
							{Type: provider.ChunkTypeToolCall, ToolCall: &local},
							{Type: provider.ChunkTypeFinish, FinishReason: types.FinishReasonToolCalls},
						}, nil
					},
					func() ([]provider.StreamChunk, error) {
						chunks := []provider.StreamChunk{{Type: provider.ChunkTypeToolResult, ToolResult: &types.ToolResult{
							ToolCallID: "remote-call", ToolName: "remote", Result: map[string]any{"answer": 42}, ProviderExecuted: true,
						}}}
						done, _ := sdkApprovalResponse("Done")()
						return append(chunks, done...), nil
					},
				)
				testOptions.Tools = workflowai.ToolSet{
					"remote": {Provider: &types.Tool{Name: "remote", ProviderExecuted: true, SupportsDeferredResults: true}},
					"action": {
						InputSchema: sdkApprovalSchema(),
						Execute: workflowai.ExecuteFunc(func(context.Context, json.RawMessage, workflowai.ToolCallOptions) (any, error) {
							return nil, errors.New("local failure")
						}),
					},
				}
				run := sdkApprovalRun(t, mode, sdkApprovalPrompt())
				if run.err != nil {
					t.Fatal(run.err)
				}
				if run.result.Text != "Done" {
					t.Fatalf("text = %q", run.result.Text)
				}
				if !slices.ContainsFunc(sdkApprovalToolResults(run.result), func(result workflowai.ToolResult) bool {
					return result.ToolCallID == "remote-call" && string(result.Output) == `{"answer":42}`
				}) {
					t.Fatalf("tool results = %+v", sdkApprovalToolResults(run.result))
				}
				if !slices.ContainsFunc(sdkApprovalContent(run.result), func(part workflowai.ContentPart) bool {
					return part.ToolCallID == "call-1" && (part.Type == "tool-error" || part.Type == workflowai.PartToolResult && part.Output != nil && strings.HasPrefix(part.Output.Type, "error"))
				}) {
					t.Fatalf("content = %+v", sdkApprovalContent(run.result))
				}
			})
		})
	}
}

// AI SDK: packages/workflow/src/workflow-agent-approval.test.ts > retains executable siblings and unresolved client tools with approval data
func TestSDKApprovalRetainsSiblingsAndClientTools(t *testing.T) {
	for _, mode := range sdkApprovalModes {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				sdkApprovalSetup(sdkApprovalResponse("", sdkApprovalCall(), call("sibling", "sibling", `{"value":"requested"}`), call("client", "client", `{"value":"requested"}`)))
				testOptions.Tools = workflowai.ToolSet{
					"action":  sdkApprovalTool("action", "executed", workflowai.NeedsApprovalAlways),
					"sibling": sdkApprovalTool("sibling", "completed", nil),
					"client":  {InputSchema: sdkApprovalSchema()},
				}
				issued := sdkApprovalRun(t, mode, sdkApprovalPrompt())
				if issued.err != nil {
					t.Fatal(issued.err)
				}
				if sdkApprovalExecutions.count("sibling") != 1 || sdkApprovalExecutions.count("action") != 0 {
					t.Fatalf("sibling ran %d times, action %d times", sdkApprovalExecutions.count("sibling"), sdkApprovalExecutions.count("action"))
				}
				results := sdkApprovalToolResults(issued.result)
				if len(results) != 1 || results[0].ToolCallID != "sibling" || string(results[0].Output) != `"completed"` {
					t.Fatalf("tool results = %+v", results)
				}
				calls := 0
				for _, part := range sdkApprovalContent(issued.result) {
					if part.Type == workflowai.PartToolCall {
						calls++
					}
				}
				if calls != 3 {
					t.Fatalf("content has %d tool calls", calls)
				}
				assistant := issued.result.Messages[1]
				if assistant.Role != workflowai.RoleAssistant || !slices.ContainsFunc(assistant.Content, func(part workflowai.ContentPart) bool {
					return part.Type == workflowai.PartToolApprovalRequest && part.ApprovalID == "approval-call-1" && part.ToolCallID == "call-1"
				}) {
					t.Fatalf("assistant message = %+v", assistant)
				}
				if mode == "stream" {
					types := sdkApprovalPartTypes(issued.parts)
					if slices.Index(types, "tool-result") > slices.Index(types, "tool-approval-request") || types[len(types)-1] != "finish" {
						t.Fatalf("parts = %v", types)
					}
				}
			})
		})
	}
}

// AI SDK: packages/workflow/src/workflow-agent-approval.test.ts > forwards provider approval responses without local execution (%s)
func TestSDKApprovalForwardsProviderResponses(t *testing.T) {
	for _, mode := range sdkApprovalModes {
		for _, approved := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/approved=%v", mode, approved), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					providerCall := sdkApprovalCall()
					providerCall.ProviderExecuted = true
					sdkApprovalSetup(func() ([]provider.StreamChunk, error) {
						return []provider.StreamChunk{
							{Type: provider.ChunkTypeToolCall, ToolCall: &providerCall},
							{Type: provider.ChunkTypeToolApprovalRequest, ToolApprovalRequest: &types.ToolApprovalRequestContent{ApprovalID: "provider-approval", ToolCallID: "call-1"}},
							{Type: provider.ChunkTypeFinish, FinishReason: types.FinishReasonToolCalls},
						}, nil
					}, sdkApprovalResponse("Done"))
					testOptions.Tools = workflowai.ToolSet{"action": sdkApprovalTool("action", "executed", nil)}
					issued := sdkApprovalRun(t, mode, sdkApprovalPrompt())
					if issued.err != nil {
						t.Fatal(issued.err)
					}
					if !slices.ContainsFunc(sdkApprovalContent(issued.result), func(part workflowai.ContentPart) bool {
						return part.Type == workflowai.PartToolApprovalRequest && part.ApprovalID == "provider-approval"
					}) {
						t.Fatalf("content = %+v", sdkApprovalContent(issued.result))
					}
					resumed := sdkApprovalRun(t, mode, sdkApprovalRespond(issued.result.Messages, "provider-approval", approved))
					if resumed.err != nil {
						t.Fatal(resumed.err)
					}
					if runs := sdkApprovalExecutions.count("action"); runs != 0 {
						t.Fatalf("action ran %d times", runs)
					}
					if len(testModel.calls) != 2 {
						t.Fatalf("%d model calls", len(testModel.calls))
					}
					var found bool
					for _, message := range testModel.calls[1].Prompt.Messages {
						for _, part := range message.Content {
							if response, ok := part.(types.ToolApprovalResponseContent); ok && message.Role == types.RoleTool && response.ApprovalID == "provider-approval" && response.Approved == approved {
								found = true
							}
						}
					}
					if !found {
						t.Fatalf("prompt = %+v", testModel.calls[1].Prompt.Messages)
					}
				})
			})
		}
	}
}

// AI SDK: packages/workflow/src/workflow-agent-approval.integration.test.ts > durable generate approvals > round trips signed approval data without a writable (%s)
func TestSDKApprovalDurableSignedRoundTrip(t *testing.T) {
	const secret = "phase4-runtime-secret-not-for-workflow-history"
	for _, approved := range []bool{true, false} {
		t.Run(fmt.Sprintf("approved=%v", approved), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				sdkApprovalSetup(sdkApprovalResponse("", call("call-1", "action", `{"value":"approved-action"}`)), sdkApprovalResponse("Finished."))
				testOptions.Tools = workflowai.ToolSet{"action": {
					NeedsApproval: workflowai.NeedsApprovalAlways,
					Execute:       workflowai.ExecuteStep(sdkApprovalPerformApprovedAction),
				}}
				t.Setenv(sdkApprovalSecretVariable, secret)
				testOptions.ToolApprovalSecret = &workflowai.ToolApprovalSecret{EnvironmentVariable: sdkApprovalSecretVariable}

				issued := sdkApprovalRun(t, "generate", sdkApprovalPrompt())
				if issued.err != nil {
					t.Fatal(issued.err)
				}
				messages := issued.result.Messages[1:]
				if len(messages) != 1 || messages[0].Role != workflowai.RoleAssistant || len(messages[0].Content) != 2 ||
					messages[0].Content[0].Type != workflowai.PartToolCall || messages[0].Content[1].Type != workflowai.PartToolApprovalRequest || messages[0].Content[1].Signature == "" {
					t.Fatalf("messages = %+v", messages)
				}
				signing, ok := sdkApprovalStep(issued.steps, "signWorkflowToolApproval")
				if !ok {
					t.Fatalf("steps = %v", sdkApprovalStepNames(issued.steps))
				}
				want := `{"approvalId":"approval-call-1","input":{"value":"approved-action"},"secret":{"environmentVariable":"WORKFLOW_TOOL_APPROVAL_SECRET"},"toolCallId":"call-1","toolName":"action"}`
				if got := sdkApprovalStepArgument(t, signing); got != want {
					t.Fatalf("signing input = %s, want %s", got, want)
				}
				if sdkApprovalStepsContain(t, issued.steps, secret) {
					t.Fatal("the steps hold the secret")
				}
				if data, _ := json.Marshal(issued.result.Messages); strings.Contains(string(data), secret) {
					t.Fatal("the messages hold the secret")
				}

				resumed := sdkApprovalRun(t, "generate", sdkApprovalRespond(issued.result.Messages, "approval-call-1", approved))
				if resumed.err != nil {
					t.Fatal(resumed.err)
				}
				if resumed.result.Text != "Finished." {
					t.Fatalf("text = %q", resumed.result.Text)
				}
				data, _ := json.Marshal(resumed.result.Messages)
				if want := map[bool]string{true: "performed:approved-action", false: "execution-denied"}[approved]; !strings.Contains(string(data), want) {
					t.Fatalf("messages = %s, want %q", data, want)
				}
				if sdkApprovalStepsContain(t, resumed.steps, secret) {
					t.Fatal("the steps hold the secret")
				}
				_, performed := sdkApprovalStep(resumed.steps, "PerformApprovedAction")
				if performed != approved {
					t.Fatalf("steps = %v", sdkApprovalStepNames(resumed.steps))
				}
				if approved {
					verification, ok := sdkApprovalStep(resumed.steps, "verifyWorkflowToolApprovalSignature")
					if !ok || !strings.Contains(sdkApprovalStepArgument(t, verification), `"secret":{"environmentVariable":"WORKFLOW_TOOL_APPROVAL_SECRET"}`) {
						t.Fatalf("steps = %v", sdkApprovalStepNames(resumed.steps))
					}
				}
			})
		})
	}
}

// AI SDK: packages/workflow/src/workflow-agent-approval.integration.test.ts > durable generate approvals > resumes a hook tool while retaining completed model and tool steps
func TestSDKApprovalDurableHookTool(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const token = "sdk-approval-wait-for-input"
		sdkApprovalSetup(sdkApprovalResponse("", call("c1", "completedWork", `{}`)), sdkApprovalResponse("", call("c2", "waitForInput", `{}`)), sdkApprovalResponse("Resumed."))
		testOptions.Tools = workflowai.ToolSet{
			"completedWork": {Execute: workflowai.ExecuteStep(sdkApprovalCompletedWork)},
			"waitForInput": {Execute: workflowai.ExecuteFunc(func(ctx context.Context, _ json.RawMessage, _ workflowai.ToolCallOptions) (any, error) {
				hook := workflow.CreateHook[string](ctx, workflow.HookOption{Token: token})
				defer hook.Dispose(ctx)
				return hook.Next(ctx)
			})},
		}
		world := testworld.New(worlds.Runners(workflow.Use(sdkApprovalRunner)))
		defer world.Close()
		ctx := workflow.NewContext(t.Context(), workflow.NewClient(world))
		run, err := workflow.Start(ctx, sdkApprovalChat{Messages: []workflowai.ModelMessage{user("Do work, then wait.")}, Generate: true})
		if err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		var before []worlds.Step
		for _, step := range sdkApprovalListSteps(t, world, run.ID()) {
			if step.Status == worlds.StatusCompleted {
				before = append(before, step)
			}
		}
		if names := sdkApprovalStepNames(before); sdkApprovalCountNamed(names, "doGenerateStep") != 2 || sdkApprovalCountNamed(names, "CompletedWork") != 1 {
			t.Fatalf("completed steps = %v", names)
		}
		if err := workflow.ResumeHook(ctx, token, "resumed input"); err != nil {
			t.Fatal(err)
		}
		result, err := run.Value(ctx)
		if err != nil {
			t.Fatal(err)
		}
		after := sdkApprovalListSteps(t, world, run.ID())
		if names := sdkApprovalStepNames(after); sdkApprovalCountNamed(names, "doGenerateStep") != 3 {
			t.Fatalf("steps = %v", names)
		}
		for _, completed := range before {
			i := slices.IndexFunc(after, func(step worlds.Step) bool { return step.StepID == completed.StepID })
			if i < 0 {
				t.Fatalf("step %s is gone", completed.StepName)
			}
			a, _ := json.Marshal(completed)
			b, _ := json.Marshal(after[i])
			if string(a) != string(b) {
				t.Fatalf("step %s changed from %s to %s", completed.StepName, a, b)
			}
		}
		var outputs []string
		for _, toolResult := range result.ToolResults {
			outputs = append(outputs, string(toolResult.Output))
		}
		if result.Text != "Resumed." || !slices.Equal(outputs, []string{`"completed before suspension"`, `"resumed input"`}) {
			t.Fatalf("text = %q, outputs = %v", result.Text, outputs)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-tool-approval-config.test.ts > requests approval for a tool without needsApproval using %s
func TestSDKApprovalConfigRequestsApproval(t *testing.T) {
	configurations := []struct {
		name  string
		apply func()
	}{
		{"a per-tool status", func() {
			testOptions.ToolApprovals = map[string]workflowai.ToolApprovalFunc{"action": sdkApprovalStatus(workflowai.ToolApprovalUserApproval)}
		}},
		{"a per-tool function", func() {
			testOptions.ToolApprovals = map[string]workflowai.ToolApprovalFunc{"action": func(context.Context, workflowai.ToolCall, workflowai.ToolApprovalOptions) (workflowai.ToolApprovalStatus, error) {
				return workflowai.ToolApprovalStatus{Type: workflowai.ToolApprovalUserApproval}, nil
			}}
		}},
		{"a generic function", func() {
			testOptions.ToolApproval = func(context.Context, workflowai.ToolCall, workflowai.ToolApprovalOptions) (workflowai.ToolApprovalStatus, error) {
				return workflowai.ToolApprovalStatus{Type: workflowai.ToolApprovalUserApproval}, nil
			}
		}},
	}
	for _, mode := range sdkApprovalModes {
		for _, configuration := range configurations {
			t.Run(mode+"/"+configuration.name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					sdkApprovalSetup(sdkApprovalActionResponse())
					testOptions.Tools = workflowai.ToolSet{"action": sdkApprovalTool("action", "executed", nil)}
					configuration.apply()
					run := sdkApprovalRun(t, mode, sdkApprovalPrompt())
					if run.err != nil {
						t.Fatal(run.err)
					}
					if runs := sdkApprovalExecutions.count("action"); runs != 0 {
						t.Fatalf("action ran %d times", runs)
					}
					if len(testModel.calls) != 1 || len(run.result.Steps) != 1 {
						t.Fatalf("%d model calls, %d steps", len(testModel.calls), len(run.result.Steps))
					}
					if got := sdkApprovalRequestIDs(run.result); !slices.Equal(got, []string{"call-1"}) {
						t.Fatalf("approval requests = %v", got)
					}
					sdkApprovalExpectShapes(t, run.result.Messages[1:], "assistant:text,tool-call,tool-approval-request")
				})
			})
		}
	}
}

// AI SDK: packages/workflow/src/workflow-agent-tool-approval-config.test.ts > runs an approved call and continues like ToolLoopAgent
// AI SDK: packages/workflow/src/workflow-agent-tool-approval-config.test.ts > does not run a denied call
func TestSDKApprovalConfigResumes(t *testing.T) {
	for _, mode := range sdkApprovalModes {
		for _, approved := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/approved=%v", mode, approved), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					sdkApprovalSetup(sdkApprovalActionResponse(), sdkApprovalResponse("Done."))
					testOptions.Tools = workflowai.ToolSet{"action": sdkApprovalTool("action", "executed", nil)}
					testOptions.ToolApprovals = map[string]workflowai.ToolApprovalFunc{"action": sdkApprovalStatus(workflowai.ToolApprovalUserApproval)}
					issued := sdkApprovalRun(t, mode, sdkApprovalPrompt())
					if issued.err != nil {
						t.Fatal(issued.err)
					}
					sdkApprovalExpectShapes(t, issued.result.Messages[1:], "assistant:text,tool-call,tool-approval-request")
					requests := sdkApprovalRequests(issued.result)
					if len(requests) != 1 {
						t.Fatalf("approval requests = %+v", requests)
					}
					resumed := sdkApprovalRun(t, mode, sdkApprovalRespond(issued.result.Messages, requests[0].ApprovalID, approved))
					if resumed.err != nil {
						t.Fatal(resumed.err)
					}
					if runs, want := sdkApprovalExecutions.count("action"), map[bool]int{true: 1, false: 0}[approved]; runs != want {
						t.Fatalf("action ran %d times, want %d", runs, want)
					}
					sdkApprovalExpectShapes(t, resumed.result.Messages[len(resumed.result.Messages)-2:], "tool:tool-result", "assistant:text")
					results := sdkApprovalToolResultParts(resumed.result.Messages)
					if len(results) != 1 || approved && string(results[0].Output.Value) != `"executed"` || !approved && results[0].Output.Type != "execution-denied" {
						t.Fatalf("tool results = %+v", results)
					}
					data, _ := json.Marshal(resumed.result.Messages)
					if strings.Contains(string(data), `"executed"`) != approved {
						t.Fatalf("messages = %s", data)
					}
				})
			})
		}
	}
}

// AI SDK: packages/workflow/src/workflow-agent-tool-approval-config.test.ts > applies the configuration over needsApproval: true
func TestSDKApprovalConfigOverNeedsApproval(t *testing.T) {
	for _, mode := range sdkApprovalModes {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				sdkApprovalSetup(sdkApprovalActionResponse(), sdkApprovalResponse("Done."))
				testOptions.Tools = workflowai.ToolSet{"action": sdkApprovalTool("action", "executed", workflowai.NeedsApprovalAlways)}
				testOptions.ToolApprovals = map[string]workflowai.ToolApprovalFunc{"action": sdkApprovalStatus(workflowai.ToolApprovalNotApplicable)}
				run := sdkApprovalRun(t, mode, sdkApprovalPrompt())
				if run.err != nil {
					t.Fatal(run.err)
				}
				if runs := sdkApprovalExecutions.count("action"); runs != 1 || len(testModel.calls) != 2 || len(sdkApprovalRequests(run.result)) != 0 {
					t.Fatalf("action ran %d times, %d model calls, approval requests %+v", runs, len(testModel.calls), sdkApprovalRequests(run.result))
				}
				sdkApprovalExpectShapes(t, run.result.Messages[1:], "assistant:text,tool-call", "tool:tool-result", "assistant:text")
			})
		})
	}
}

// AI SDK: packages/workflow/src/workflow-agent-tool-approval-config.test.ts > keeps needsApproval for tools without a configuration entry
func TestSDKApprovalConfigKeepsNeedsApproval(t *testing.T) {
	for _, mode := range sdkApprovalModes {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				sdkApprovalSetup(sdkApprovalActionResponse())
				testOptions.Tools = workflowai.ToolSet{
					"action": sdkApprovalTool("action", "executed", workflowai.NeedsApprovalAlways),
					"other":  {InputSchema: sdkApprovalSchema()},
				}
				testOptions.ToolApprovals = map[string]workflowai.ToolApprovalFunc{"other": sdkApprovalStatus(workflowai.ToolApprovalNotApplicable)}
				run := sdkApprovalRun(t, mode, sdkApprovalPrompt())
				if run.err != nil {
					t.Fatal(run.err)
				}
				if runs := sdkApprovalExecutions.count("action"); runs != 0 || len(testModel.calls) != 1 {
					t.Fatalf("action ran %d times, %d model calls", runs, len(testModel.calls))
				}
				if got := sdkApprovalRequestIDs(run.result); !slices.Equal(got, []string{"call-1"}) {
					t.Fatalf("approval requests = %v", got)
				}
				sdkApprovalExpectShapes(t, run.result.Messages[1:], "assistant:text,tool-call,tool-approval-request")
			})
		})
	}
}

// AI SDK: packages/workflow/src/workflow-agent-tool-approval-config.test.ts > lets a per-call configuration override the constructor value
func TestSDKApprovalConfigCallOverride(t *testing.T) {
	status := func(kind workflowai.ToolApprovalType) map[string]workflowai.ToolApprovalFunc {
		return map[string]workflowai.ToolApprovalFunc{"action": sdkApprovalStatus(kind)}
	}
	for _, mode := range sdkApprovalModes {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				sdkApprovalSetup(sdkApprovalActionResponse())
				testOptions.Tools = workflowai.ToolSet{"action": sdkApprovalTool("action", "executed", workflowai.NeedsApprovalAlways)}
				testOptions.ToolApprovals = status(workflowai.ToolApprovalNotApplicable)
				testCallOptions.ToolApprovals = status(workflowai.ToolApprovalUserApproval)
				requestsApproval := sdkApprovalRun(t, mode, sdkApprovalPrompt())
				if requestsApproval.err != nil {
					t.Fatal(requestsApproval.err)
				}
				if runs := sdkApprovalExecutions.count("action"); runs != 0 || len(testModel.calls) != 1 {
					t.Fatalf("action ran %d times, %d model calls", runs, len(testModel.calls))
				}
				if got := sdkApprovalRequestIDs(requestsApproval.result); !slices.Equal(got, []string{"call-1"}) {
					t.Fatalf("approval requests = %v", got)
				}

				sdkApprovalSetup(sdkApprovalActionResponse(), sdkApprovalResponse("Done."))
				testOptions.Tools = workflowai.ToolSet{"action": sdkApprovalTool("action", "executed", workflowai.NeedsApprovalAlways)}
				testOptions.ToolApprovals = status(workflowai.ToolApprovalUserApproval)
				testCallOptions.ToolApprovals = status(workflowai.ToolApprovalNotApplicable)
				runsWithoutApproval := sdkApprovalRun(t, mode, sdkApprovalPrompt())
				if runsWithoutApproval.err != nil {
					t.Fatal(runsWithoutApproval.err)
				}
				if runs := sdkApprovalExecutions.count("action"); runs != 1 || len(testModel.calls) != 2 || len(sdkApprovalRequests(runsWithoutApproval.result)) != 0 {
					t.Fatalf("action ran %d times, %d model calls", runs, len(testModel.calls))
				}
			})
		})
	}
}

// AI SDK: packages/workflow/src/workflow-agent-tool-approval-config.test.ts > signs configuration approval requests and rejects tampered responses
func TestSDKApprovalConfigSignsRequests(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkApprovalSetup(sdkApprovalActionResponse(), sdkApprovalResponse("Done."))
		testOptions.Tools = workflowai.ToolSet{"action": sdkApprovalTool("action", "executed", nil)}
		testOptions.ToolApprovals = map[string]workflowai.ToolApprovalFunc{"action": sdkApprovalStatus(workflowai.ToolApprovalUserApproval)}
		sdkApprovalSign(t)
		issued := sdkApprovalRun(t, "generate", sdkApprovalPrompt())
		if issued.err != nil {
			t.Fatal(issued.err)
		}
		requests := sdkApprovalRequests(issued.result)
		if len(requests) != 1 || requests[0].ApprovalID != "approval-call-1" || requests[0].Signature == "" {
			t.Fatalf("approval requests = %+v", requests)
		}
		if runs := sdkApprovalExecutions.count("action"); runs != 0 {
			t.Fatalf("action ran %d times", runs)
		}
		valid, err := ai.VerifyToolApprovalSignature([]byte(sdkApprovalSecret), requests[0].Signature, requests[0].ApprovalID, "call-1", "action", map[string]any{"value": "requested"})
		if err != nil || !valid {
			t.Fatalf("signature valid = %v, %v", valid, err)
		}
		tampered := sdkApprovalTamper(issued.result.Messages)
		if resumed := sdkApprovalRun(t, "generate", sdkApprovalRespond(tampered, "approval-call-1", true)); resumed.err != nil {
			t.Fatal(resumed.err)
		}
		if runs := sdkApprovalExecutions.count("action"); runs != 0 {
			t.Fatalf("action ran %d times", runs)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-tool-approval-config.test.ts > continues after a call denied by %s like ToolLoopAgent
func TestSDKApprovalConfigDenied(t *testing.T) {
	for _, mode := range sdkApprovalModes {
		for _, reason := range []string{"", "policy"} {
			t.Run(fmt.Sprintf("%s/reason=%q", mode, reason), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					sdkApprovalSetup(sdkApprovalActionResponse(), sdkApprovalResponse("Done."))
					testOptions.Tools = workflowai.ToolSet{"action": sdkApprovalTool("action", "executed", nil)}
					testOptions.ToolApprovals = map[string]workflowai.ToolApprovalFunc{"action": workflowai.ToolApprovalAlways(workflowai.ToolApprovalStatus{Type: workflowai.ToolApprovalDenied, Reason: reason})}
					run := sdkApprovalRun(t, mode, sdkApprovalPrompt())
					if run.err != nil {
						t.Fatal(run.err)
					}
					if runs := sdkApprovalExecutions.count("action"); runs != 0 || len(testModel.calls) != 2 {
						t.Fatalf("action ran %d times, %d model calls", runs, len(testModel.calls))
					}
					sdkApprovalExpectContentTypes(t, run.result, "text,tool-call,tool-approval-request,tool-approval-response", "text")
					sdkApprovalExpectSecondCallToolResult(t, "execution-denied", "", reason)
				})
			})
		}
	}
}

// AI SDK: packages/workflow/src/workflow-agent-tool-approval-config.test.ts > runs a call approved by the configuration without pausing
func TestSDKApprovalConfigApproved(t *testing.T) {
	for _, mode := range sdkApprovalModes {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				sdkApprovalSetup(sdkApprovalActionResponse(), sdkApprovalResponse("Done."))
				testOptions.Tools = workflowai.ToolSet{"action": sdkApprovalTool("action", "executed", nil)}
				testOptions.ToolApprovals = map[string]workflowai.ToolApprovalFunc{"action": sdkApprovalStatus(workflowai.ToolApprovalApproved)}
				run := sdkApprovalRun(t, mode, sdkApprovalPrompt())
				if run.err != nil {
					t.Fatal(run.err)
				}
				if runs := sdkApprovalExecutions.count("action"); runs != 1 || len(testModel.calls) != 2 {
					t.Fatalf("action ran %d times, %d model calls", runs, len(testModel.calls))
				}
				sdkApprovalExpectSecondCallToolResult(t, "text", `"executed"`, "")
				sdkApprovalExpectContentTypes(t, run.result, "text,tool-call,tool-approval-request,tool-approval-response,tool-result", "text")
			})
		})
	}
}

// AI SDK: packages/workflow/src/workflow-agent-tool-approval-config.test.ts > denies, asks, and runs calls in one step like ToolLoopAgent
func TestSDKApprovalConfigMixedDecisions(t *testing.T) {
	for _, mode := range sdkApprovalModes {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				sdkApprovalSetup(sdkApprovalResponse("Checking the actions.",
					call("call-denied", "deniedAction", `{"value":"denied"}`),
					call("call-asked", "askedAction", `{"value":"asked"}`),
					call("call-plain", "plainAction", `{"value":"plain"}`),
				))
				testOptions.Tools = workflowai.ToolSet{
					"deniedAction": sdkApprovalTool("deniedAction", "denied ran", nil),
					"askedAction":  sdkApprovalTool("askedAction", "asked ran", nil),
					"plainAction":  sdkApprovalTool("plainAction", "plain ran", nil),
				}
				testOptions.ToolApprovals = map[string]workflowai.ToolApprovalFunc{
					"deniedAction": sdkApprovalStatus(workflowai.ToolApprovalDenied),
					"askedAction":  sdkApprovalStatus(workflowai.ToolApprovalUserApproval),
				}
				run := sdkApprovalRun(t, mode, sdkApprovalPrompt())
				if run.err != nil {
					t.Fatal(run.err)
				}
				if got := []int{sdkApprovalExecutions.count("deniedAction"), sdkApprovalExecutions.count("askedAction"), sdkApprovalExecutions.count("plainAction")}; !slices.Equal(got, []int{0, 0, 1}) {
					t.Fatalf("executions = %v", got)
				}
				if len(testModel.calls) != 1 {
					t.Fatalf("%d model calls", len(testModel.calls))
				}
				var requests []string
				for _, request := range sdkApprovalRequests(run.result) {
					requests = append(requests, fmt.Sprintf("%s automatic=%v", request.ToolCallID, request.Automatic))
				}
				if want := []string{"call-denied automatic=true", "call-asked automatic=false"}; !slices.Equal(requests, want) {
					t.Fatalf("approval requests = %v, want %v", requests, want)
				}
				var types []string
				for _, part := range run.result.Steps[0].Content {
					types = append(types, part.Type)
				}
				slices.Sort(types)
				want := []string{"text", "tool-approval-request", "tool-approval-request", "tool-approval-response", "tool-call", "tool-call", "tool-call", "tool-result"}
				if !slices.Equal(types, want) {
					t.Fatalf("content types = %v, want %v", types, want)
				}
			})
		})
	}
}

// AI SDK: packages/workflow/src/workflow-agent-tool-approval-config.test.ts > denies a call when prepareStep revokes the permission
// AI SDK: packages/workflow/src/workflow-agent-tool-approval-config.test.ts > requires approval when prepareStep revokes the permission
func TestSDKApprovalConfigPrepareStepRevokes(t *testing.T) {
	for _, mode := range sdkApprovalModes {
		for _, revoked := range []workflowai.ToolApprovalType{workflowai.ToolApprovalDenied, workflowai.ToolApprovalUserApproval} {
			t.Run(mode+"/"+string(revoked), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					sdkApprovalSetup(sdkApprovalActionResponse(), sdkApprovalResponse("Done."))
					testOptions.Tools = workflowai.ToolSet{"action": sdkApprovalTool("action", "executed", nil)}
					testOptions.RuntimeContext = map[string]any{"role": "admin"}
					testOptions.ToolApproval = func(_ context.Context, _ workflowai.ToolCall, options workflowai.ToolApprovalOptions) (workflowai.ToolApprovalStatus, error) {
						if options.RuntimeContext.(map[string]any)["role"] == "admin" {
							return workflowai.ToolApprovalStatus{Type: workflowai.ToolApprovalApproved}, nil
						}
						return workflowai.ToolApprovalStatus{Type: revoked}, nil
					}
					seen := &sdkApprovalRecorder{}
					testOptions.PrepareStep = func(_ context.Context, info workflowai.PrepareStepInfo) (workflowai.PrepareStepResult, error) {
						seen.record(fmt.Sprint(info.StepNumber), info.RuntimeContext.(map[string]any)["role"])
						return workflowai.PrepareStepResult{RuntimeContext: map[string]any{"role": "guest"}}, nil
					}
					run := sdkApprovalRun(t, mode, sdkApprovalPrompt())
					if run.err != nil {
						t.Fatal(run.err)
					}
					if runs := sdkApprovalExecutions.count("action"); runs != 0 {
						t.Fatalf("action ran %d times", runs)
					}
					if revoked == workflowai.ToolApprovalDenied {
						if len(testModel.calls) != 2 {
							t.Fatalf("%d model calls", len(testModel.calls))
						}
						// The revoked context carries into the step after the
						// denial.
						if got := seen.all(); !slices.Equal(got, []any{"admin", "guest"}) {
							t.Fatalf("seen contexts = %v", got)
						}
						sdkApprovalExpectContentTypes(t, run.result, "text,tool-call,tool-approval-request,tool-approval-response", "text")
						sdkApprovalExpectSecondCallToolResult(t, "execution-denied", "", "")
						if data, _ := json.Marshal(run.result.Messages); strings.Contains(string(data), `"executed"`) {
							t.Fatalf("messages = %s", data)
						}
						return
					}
					if len(testModel.calls) != 1 {
						t.Fatalf("%d model calls", len(testModel.calls))
					}
					if got := sdkApprovalRequestIDs(run.result); !slices.Equal(got, []string{"call-1"}) || slices.ContainsFunc(sdkApprovalRequests(run.result), func(part workflowai.ContentPart) bool { return part.Automatic }) {
						t.Fatalf("approval requests = %+v", sdkApprovalRequests(run.result))
					}
					if got := seen.all(); !slices.Equal(got, []any{"admin"}) {
						t.Fatalf("seen contexts = %v", got)
					}
					sdkApprovalExpectShapes(t, run.result.Messages[1:], "assistant:text,tool-call,tool-approval-request")
				})
			})
		}
	}
}

// AI SDK: packages/workflow/src/workflow-agent-tool-approval-config.test.ts > does not authorize action from a %s sibling
func TestSDKApprovalConfigSiblingDoesNotAuthorize(t *testing.T) {
	for _, mode := range sdkApprovalModes {
		for _, sibling := range []string{"alone", "denied", "pending", "invalid", "unknown", "reversed"} {
			t.Run(mode+"/"+sibling, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					action := sdkApprovalCall()
					attestInput := `{"value":"attestation"}`
					if sibling == "invalid" {
						attestInput = `{}`
					}
					attest := call("attest-call", "attest", attestInput)
					calls := []types.ToolCall{action, attest}
					switch sibling {
					case "alone":
						calls = calls[:1]
					case "reversed":
						calls = []types.ToolCall{attest, action}
					}
					sdkApprovalSetup(sdkApprovalResponse("", calls...), sdkApprovalResponse("Done."))
					testOptions.Tools = workflowai.ToolSet{"action": sdkApprovalTool("action", "protected output", nil)}
					if sibling != "unknown" {
						testOptions.Tools["attest"] = sdkApprovalTool("attest", "attested", nil)
					}
					approvalMessages := &sdkApprovalRecorder{}
					testOptions.ToolApproval = func(_ context.Context, call workflowai.ToolCall, options workflowai.ToolApprovalOptions) (workflowai.ToolApprovalStatus, error) {
						approvalMessages.record(call.ToolCallID, options.Messages)
						if call.ToolName == "attest" {
							if sibling == "pending" {
								return workflowai.ToolApprovalStatus{Type: workflowai.ToolApprovalUserApproval}, nil
							}
							return workflowai.ToolApprovalStatus{Type: workflowai.ToolApprovalDenied}, nil
						}
						if names := sdkApprovalToolCallNames(options.Messages); len(names) > 0 && names[len(names)-1] == "attest" {
							return workflowai.ToolApprovalStatus{Type: workflowai.ToolApprovalApproved}, nil
						}
						return workflowai.ToolApprovalStatus{Type: workflowai.ToolApprovalUserApproval}, nil
					}
					run := sdkApprovalRun(t, mode, sdkApprovalPrompt())
					if run.err != nil {
						t.Fatal(run.err)
					}
					if sdkApprovalExecutions.count("action") != 0 || sdkApprovalExecutions.count("attest") != 0 {
						t.Fatalf("action ran %d times, attest %d times", sdkApprovalExecutions.count("action"), sdkApprovalExecutions.count("attest"))
					}
					if approvalMessages.len() == 0 {
						t.Fatal("the approval policy was not called")
					}
					prompt := sdkApprovalJSON(workflowai.FromMessages(testModel.calls[0].Prompt.Messages))
					for _, messages := range approvalMessages.all() {
						if got := sdkApprovalJSON(messages); got != prompt {
							t.Fatalf("approval messages = %s, want %s", got, prompt)
						}
					}
					if !slices.Contains(sdkApprovalRequestIDs(run.result), "call-1") {
						t.Fatalf("approval requests = %v", sdkApprovalRequestIDs(run.result))
					}
					if results := sdkApprovalToolResults(run.result); len(results) != 0 {
						t.Fatalf("tool results = %+v", results)
					}
				})
			})
		}
	}
}

// AI SDK: packages/workflow/src/workflow-agent-tool-approval-config.test.ts > uses prepared pre-response history for %s, preserving execution context
func TestSDKApprovalConfigPreparedHistory(t *testing.T) {
	history := []workflowai.ModelMessage{
		user("Perform the action."),
		{Role: workflowai.RoleAssistant, Content: []workflowai.ContentPart{{Type: workflowai.PartToolCall, ToolCallID: "prior", ToolName: "attest", Input: json.RawMessage(`{"value":"attestation"}`)}}},
		{Role: workflowai.RoleTool, Content: []workflowai.ContentPart{{Type: workflowai.PartToolResult, ToolCallID: "prior", ToolName: "attest", Output: &workflowai.ToolResultOutput{Type: "text", Value: json.RawMessage(`"attested"`)}}}},
	}
	for _, mode := range sdkApprovalModes {
		for _, configuration := range []string{"generic", "per-tool", "needsApproval"} {
			t.Run(mode+"/"+configuration, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					sdkApprovalSetup(sdkApprovalActionResponse(), sdkApprovalResponse("Done."))
					testOptions.Instructions = "Do not include instructions in approval messages."
					testOptions.PrepareStep = func(_ context.Context, info workflowai.PrepareStepInfo) (workflowai.PrepareStepResult, error) {
						if info.StepNumber != 0 {
							return workflowai.PrepareStepResult{}, nil
						}
						system := workflowai.ModelMessage{Role: workflowai.RoleSystem, Content: []workflowai.ContentPart{{Type: workflowai.PartText, Text: "Prepared instructions."}}}
						return workflowai.PrepareStepResult{Messages: append([]workflowai.ModelMessage{system}, history...)}, nil
					}
					approvalMessages := &sdkApprovalRecorder{}
					hasAttestation := func(call string, messages []workflowai.ModelMessage) bool {
						approvalMessages.record(call, messages)
						names := sdkApprovalToolCallNames(messages)
						return len(names) > 0 && names[len(names)-1] == "attest"
					}
					executionToolNames := &sdkApprovalRecorder{}
					action := workflowai.Tool{
						InputSchema: sdkApprovalSchema(),
						Execute: workflowai.ExecuteFunc(func(ctx context.Context, input json.RawMessage, options workflowai.ToolCallOptions) (any, error) {
							executionToolNames.record(options.ToolCallID, sdkApprovalToolCallNames(options.Messages))
							return workflow.Step(ctx, sdkApprovalExecute, sdkApprovalExecution{Tool: "action", Output: "protected output", Input: input})
						}),
					}
					policy := func(_ context.Context, call workflowai.ToolCall, options workflowai.ToolApprovalOptions) (workflowai.ToolApprovalStatus, error) {
						if hasAttestation(call.ToolCallID, options.Messages) {
							return workflowai.ToolApprovalStatus{Type: workflowai.ToolApprovalApproved}, nil
						}
						return workflowai.ToolApprovalStatus{Type: workflowai.ToolApprovalUserApproval}, nil
					}
					switch configuration {
					case "generic":
						testOptions.ToolApproval = policy
					case "per-tool":
						testOptions.ToolApprovals = map[string]workflowai.ToolApprovalFunc{"action": policy}
					case "needsApproval":
						action.NeedsApproval = func(_ context.Context, _ json.RawMessage, options workflowai.ToolCallOptions) (bool, error) {
							return !hasAttestation(options.ToolCallID, options.Messages), nil
						}
					}
					testOptions.Tools = workflowai.ToolSet{"action": action}
					if run := sdkApprovalRun(t, mode, sdkApprovalPrompt()); run.err != nil {
						t.Fatal(run.err)
					}
					if runs := sdkApprovalExecutions.count("action"); runs != 1 {
						t.Fatalf("action ran %d times", runs)
					}
					// Tool execution receives the prepared history without the
					// response that contained the call.
					if got := executionToolNames.all(); len(got) != 1 || !slices.Equal(got[0].([]string), []string{"attest"}) {
						t.Fatalf("execution tool names = %v", got)
					}
					if got := approvalMessages.all(); len(got) != 1 || sdkApprovalJSON(got[0]) != sdkApprovalJSON(history) {
						t.Fatalf("approval messages = %v", got)
					}
				})
			})
		}
	}
}

// AI SDK: packages/workflow/src/workflow-agent-tool-approval-config.test.ts > offers a call and result that completed in an earlier step
func TestSDKApprovalConfigEarlierStepHistory(t *testing.T) {
	for _, mode := range sdkApprovalModes {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				sdkApprovalSetup(sdkApprovalResponse("", call("attest-call", "attest", `{"value":"attestation"}`)), sdkApprovalActionResponse(), sdkApprovalResponse("Done."))
				testOptions.Instructions = "Instructions are never approval messages."
				executedFor := &sdkApprovalRecorder{}
				testOptions.Tools = workflowai.ToolSet{
					"attest": sdkApprovalTool("attest", "attested", nil),
					"action": {
						InputSchema: sdkApprovalSchema(),
						Execute: workflowai.ExecuteFunc(func(ctx context.Context, input json.RawMessage, options workflowai.ToolCallOptions) (any, error) {
							executedFor.record(options.ToolCallID, sdkApprovalToolCallNames(options.Messages))
							return workflow.Step(ctx, sdkApprovalExecute, sdkApprovalExecution{Tool: "action", Output: "protected output", Input: input})
						}),
					},
				}
				approvalMessages := &sdkApprovalRecorder{}
				testOptions.ToolApproval = func(_ context.Context, call workflowai.ToolCall, options workflowai.ToolApprovalOptions) (workflowai.ToolApprovalStatus, error) {
					if call.ToolName != "action" {
						return workflowai.ToolApprovalStatus{}, nil
					}
					approvalMessages.record(call.ToolCallID, options.Messages)
					names := sdkApprovalToolCallNames(options.Messages)
					if len(names) > 0 && names[len(names)-1] == "attest" && slices.ContainsFunc(sdkApprovalToolResultParts(options.Messages), func(part workflowai.ContentPart) bool { return part.ToolName == "attest" }) {
						return workflowai.ToolApprovalStatus{Type: workflowai.ToolApprovalApproved}, nil
					}
					return workflowai.ToolApprovalStatus{Type: workflowai.ToolApprovalUserApproval}, nil
				}
				if run := sdkApprovalRun(t, mode, sdkApprovalPrompt()); run.err != nil {
					t.Fatal(run.err)
				}
				if sdkApprovalExecutions.count("attest") != 1 || sdkApprovalExecutions.count("action") != 1 {
					t.Fatalf("attest ran %d times, action %d times", sdkApprovalExecutions.count("attest"), sdkApprovalExecutions.count("action"))
				}
				if got := executedFor.all(); len(got) != 1 || !slices.Equal(got[0].([]string), []string{"attest"}) {
					t.Fatalf("executed for tool names = %v", got)
				}
				recorded := approvalMessages.all()
				if len(recorded) != 1 {
					t.Fatalf("approval messages = %v", recorded)
				}
				messages := recorded[0].([]workflowai.ModelMessage)
				// The attest call and its result are real history and stay
				// visible.
				if names := sdkApprovalToolCallNames(messages); !slices.Equal(names, []string{"attest"}) {
					t.Fatalf("tool call names = %v", names)
				}
				results := sdkApprovalToolResultParts(messages)
				if len(results) != 1 || results[0].ToolName != "attest" || results[0].Output.Type != "text" || string(results[0].Output.Value) != `"attested"` {
					t.Fatalf("tool results = %+v", results)
				}
				// The model received instructions, the policy does not.
				if testModel.calls[1].Prompt.System == "" {
					t.Fatal("the model call has no instructions")
				}
				if slices.ContainsFunc(messages, func(message workflowai.ModelMessage) bool { return message.Role == workflowai.RoleSystem }) {
					t.Fatalf("approval messages = %+v", messages)
				}
				// Apart from the system messages, the policy sees the model
				// input.
				var prompt []workflowai.ModelMessage
				for _, message := range workflowai.FromMessages(testModel.calls[1].Prompt.Messages) {
					if message.Role != workflowai.RoleSystem {
						prompt = append(prompt, message)
					}
				}
				if got, want := sdkApprovalJSON(messages), sdkApprovalJSON(prompt); got != want {
					t.Fatalf("approval messages = %s, want %s", got, want)
				}
			})
		})
	}
}

// AI SDK: packages/workflow/src/workflow-agent-tool-approval-config.test.ts > stream workflow agent automatic approval decisions > writes a denied tool output and no approval request for a denied call
func TestSDKApprovalConfigStreamDenied(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkApprovalSetup(sdkApprovalActionResponse(), sdkApprovalResponse("Done."))
		testOptions.Tools = workflowai.ToolSet{"action": sdkApprovalTool("action", "executed", nil)}
		testOptions.ToolApprovals = map[string]workflowai.ToolApprovalFunc{"action": sdkApprovalStatus(workflowai.ToolApprovalDenied)}
		run := sdkApprovalRun(t, "stream", sdkApprovalPrompt())
		if run.err != nil {
			t.Fatal(run.err)
		}
		if len(run.result.Steps) != 2 || sdkApprovalExecutions.count("action") != 0 {
			t.Fatalf("%d steps, action ran %d times", len(run.result.Steps), sdkApprovalExecutions.count("action"))
		}
		var denied, requested bool
		for _, part := range run.parts {
			denied = denied || part.Type == "tool-output-denied" && part.ToolCallID == "call-1"
			requested = requested || part.Type == "tool-approval-request" && part.ToolCallID == "call-1"
		}
		if !denied || requested {
			t.Fatalf("parts = %v", sdkApprovalPartTypes(run.parts))
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-tool-metadata-approval.test.ts > resumed approvals receive the server-defined tool fields > runs an approved dynamic call that the policy still requires approval for
func TestSDKApprovalResumedPolicyStillRequiresApproval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkApprovalSetup(sdkApprovalResponse("", call("call-1", "deleteRecords", `{"table":"users"}`)), sdkApprovalResponse("Done"))
		testOptions.Tools = workflowai.ToolSet{"deleteRecords": sdkApprovalRecordsTool()}
		seen := &sdkApprovalRecorder{}
		testOptions.ToolApproval = func(_ context.Context, call workflowai.ToolCall, _ workflowai.ToolApprovalOptions) (workflowai.ToolApprovalStatus, error) {
			seen.record(call.ToolCallID, string(call.Input))
			return workflowai.ToolApprovalStatus{Type: workflowai.ToolApprovalUserApproval}, nil
		}
		issued := sdkApprovalRun(t, "generate", []workflowai.ModelMessage{user("Delete the records.")})
		if issued.err != nil {
			t.Fatal(issued.err)
		}
		requests := sdkApprovalRequests(issued.result)
		if sdkApprovalExecutions.count("deleteRecords") != 0 || len(requests) != 1 {
			t.Fatalf("approval requests = %+v", requests)
		}
		seen.reset()
		resumed := sdkApprovalRun(t, "generate", sdkApprovalRespond(issued.result.Messages, requests[0].ApprovalID, true))
		if resumed.err != nil {
			t.Fatal(resumed.err)
		}
		if seen.len() == 0 {
			t.Fatal("the approval policy was not called")
		}
		for _, input := range seen.all() {
			if input != `{"table":"users"}` {
				t.Fatalf("policy input = %v", input)
			}
		}
		if runs := sdkApprovalExecutions.count("deleteRecords"); runs != 1 {
			t.Fatalf("deleteRecords ran %d times", runs)
		}
		if data, _ := json.Marshal(resumed.result.Messages); !strings.Contains(string(data), `"deleted"`) {
			t.Fatalf("messages = %s", data)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-tool-metadata-approval.test.ts > resumed approvals receive the server-defined tool fields > does not run an approved call that the policy reports as not needing approval
func TestSDKApprovalResumedPolicyNotApplicable(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkApprovalSetup(sdkApprovalResponse("", call("call-1", "deleteRecords", `{"table":"users"}`)), sdkApprovalResponse("Done"))
		testOptions.Tools = workflowai.ToolSet{"deleteRecords": sdkApprovalRecordsTool()}
		testOptions.ToolApproval = sdkApprovalStatus(workflowai.ToolApprovalUserApproval)
		issued := sdkApprovalRun(t, "generate", []workflowai.ModelMessage{user("Delete the records.")})
		if issued.err != nil {
			t.Fatal(issued.err)
		}
		requests := sdkApprovalRequests(issued.result)
		if sdkApprovalExecutions.count("deleteRecords") != 0 || len(requests) != 1 {
			t.Fatalf("approval requests = %+v", requests)
		}
		testOptions.ToolApproval = sdkApprovalStatus(workflowai.ToolApprovalNotApplicable)
		resumed := sdkApprovalRun(t, "generate", sdkApprovalRespond(issued.result.Messages, requests[0].ApprovalID, true))
		if resumed.err != nil {
			t.Fatal(resumed.err)
		}
		if runs := sdkApprovalExecutions.count("deleteRecords"); runs != 0 {
			t.Fatalf("deleteRecords ran %d times", runs)
		}
		results := sdkApprovalToolResultParts(resumed.result.Messages)
		if len(results) != 1 || results[0].ToolCallID != "call-1" || results[0].Output.Type != "error-text" || !strings.Contains(string(results[0].Output.Value), "does not require approval") {
			t.Fatalf("tool results = %+v", results)
		}
	})
}

// sdkApprovalChat is a turn that runs Stream, or Generate when Generate is
// set.
type sdkApprovalChat struct {
	Messages []workflowai.ModelMessage `json:"messages"`
	Generate bool                      `json:"generate"`
}

func (sdkApprovalChat) Workflow(workflowai.Result) {}

func sdkApprovalRunner(ctx context.Context, input sdkApprovalChat) (workflowai.Result, error) {
	options := testOptions
	options.Model = sdkApprovalModel{testModel}
	agent := workflowai.NewWorkflowAgent(options)
	callOptions := testCallOptions
	callOptions.Messages = input.Messages
	if input.Generate {
		return agent.Generate(ctx, callOptions)
	}
	return agent.Stream(ctx, callOptions)
}

// sdkApprovalModel is a fakeModel whose generated responses also carry
// provider tool results and approval requests.
type sdkApprovalModel struct{ *fakeModel }

func (m sdkApprovalModel) DoGenerate(_ context.Context, options *provider.GenerateOptions) (*types.GenerateResult, error) {
	chunks, err := m.next(options)
	if err != nil {
		return nil, err
	}
	result := &types.GenerateResult{}
	for _, chunk := range chunks {
		switch chunk.Type {
		case provider.ChunkTypeText:
			result.Text += chunk.Text
			result.Content = append(result.Content, types.TextContent{Text: chunk.Text})
		case provider.ChunkTypeToolCall:
			result.ToolCalls = append(result.ToolCalls, *chunk.ToolCall)
		case provider.ChunkTypeToolResult:
			r := chunk.ToolResult
			result.Content = append(result.Content, types.ToolResultContent{ToolCallID: r.ToolCallID, ToolName: r.ToolName, Result: r.Result, ProviderExecuted: r.ProviderExecuted})
		case provider.ChunkTypeToolApprovalRequest:
			result.Content = append(result.Content, *chunk.ToolApprovalRequest)
		case provider.ChunkTypeFinish:
			result.FinishReason = chunk.FinishReason
		}
	}
	return result, nil
}

// sdkApprovalTurn is the outcome of one turn.
type sdkApprovalTurn struct {
	result workflowai.Result
	// parts are the raw stream parts of a Stream turn.
	parts []workflowai.ModelCallStreamPart
	steps []worlds.Step
	err   error
}

// sdkApprovalRun runs one turn with Stream, or with Generate when mode is
// "generate".
func sdkApprovalRun(t *testing.T, mode string, messages []workflowai.ModelMessage) sdkApprovalTurn {
	t.Helper()
	world := testworld.New(worlds.Runners(workflow.Use(sdkApprovalRunner)))
	defer world.Close()
	ctx := workflow.NewContext(t.Context(), workflow.NewClient(world))
	run, err := workflow.Start(ctx, sdkApprovalChat{Messages: messages, Generate: mode == "generate"})
	if err != nil {
		t.Fatal(err)
	}
	var turn sdkApprovalTurn
	if mode != "generate" {
		turn.parts = sdkApprovalReadParts(t, run)
	}
	turn.result, turn.err = run.Value(ctx)
	turn.steps = sdkApprovalListSteps(t, world, run.ID())
	return turn
}

// sdkApprovalReadParts reads the raw stream parts of a run.
func sdkApprovalReadParts(t *testing.T, run *workflow.Run[workflowai.Result]) []workflowai.ModelCallStreamPart {
	t.Helper()
	reader, err := run.Reader(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var parts []workflowai.ModelCallStreamPart
	for {
		chunk, err := reader.Next()
		if errors.Is(err, io.EOF) {
			return parts
		}
		if err != nil {
			t.Fatal(err)
		}
		var part workflowai.ModelCallStreamPart
		if err := json.Unmarshal(chunk.Data, &part); err != nil {
			t.Fatal(err)
		}
		parts = append(parts, part)
	}
}

// sdkApprovalListSteps returns the steps of run runID.
func sdkApprovalListSteps(t *testing.T, world worlds.World, runID string) []worlds.Step {
	t.Helper()
	var steps []worlds.Step
	var cursor worlds.Pagination
	for {
		page, err := world.StepsList(t.Context(), worlds.StepsListInput{RunID: worlds.RunID(runID), Pagination: cursor})
		if err != nil {
			t.Fatal(err)
		}
		steps = append(steps, page.Data...)
		if !page.HasMore {
			return steps
		}
		cursor = worlds.Pagination{Cursor: page.Cursor}
	}
}

func sdkApprovalStepNames(steps []worlds.Step) []string {
	var names []string
	for _, step := range steps {
		names = append(names, step.StepName[strings.LastIndex(step.StepName, "//")+2:])
	}
	return names
}

// sdkApprovalStep returns the step whose name contains name.
func sdkApprovalStep(steps []worlds.Step, name string) (worlds.Step, bool) {
	for _, step := range steps {
		if strings.Contains(step.StepName, name) {
			return step, true
		}
	}
	return worlds.Step{}, false
}

func sdkApprovalCountNamed(names []string, name string) int {
	count := 0
	for _, n := range names {
		if strings.Contains(n, name) {
			count++
		}
	}
	return count
}

// sdkApprovalStepArgument returns the JSON of a step's argument.
func sdkApprovalStepArgument(t *testing.T, step worlds.Step) string {
	t.Helper()
	var input any
	if err := worlds.DecodePayload(step.Input, nil, &input); err != nil {
		t.Fatal(err)
	}
	if args, ok := input.([]any); ok && len(args) == 1 {
		input = args[0]
	}
	if wrapped, ok := input.(map[string]any); ok {
		if args, ok := wrapped["args"].([]any); ok && len(args) == 1 {
			input = args[0]
		}
	}
	data, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// sdkApprovalStepsContain reports whether the inputs or outputs of steps
// contain value.
func sdkApprovalStepsContain(t *testing.T, steps []worlds.Step, value string) bool {
	t.Helper()
	for _, step := range steps {
		for _, payload := range [][]byte{step.Input, step.Output} {
			var decoded any
			if err := worlds.DecodePayload(payload, nil, &decoded); err != nil {
				t.Fatal(err)
			}
			data, _ := json.Marshal(decoded)
			if strings.Contains(string(data), value) || strings.Contains(string(payload), value) {
				return true
			}
		}
	}
	return false
}

// sdkApprovalSetup sets up a test like setup and resets the tool executions.
func sdkApprovalSetup(responses ...func() ([]provider.StreamChunk, error)) {
	setup(responses...)
	sdkApprovalExecutions.reset()
}

// sdkApprovalSign makes the agent sign its approval requests.
func sdkApprovalSign(t *testing.T) {
	t.Setenv(sdkApprovalSecretVariable, sdkApprovalSecret)
	testOptions.ToolApprovalSecret = &workflowai.ToolApprovalSecret{EnvironmentVariable: sdkApprovalSecretVariable}
}

func sdkApprovalPrompt() []workflowai.ModelMessage {
	return []workflowai.ModelMessage{user("Perform the action.")}
}

// sdkApprovalCall is the call of the action tool the upstream tests make.
func sdkApprovalCall() types.ToolCall {
	return call("call-1", "action", `{"value":"requested"}`)
}

// sdkApprovalResponse is a model response with text, when it is not empty,
// followed by calls.
func sdkApprovalResponse(text string, calls ...types.ToolCall) func() ([]provider.StreamChunk, error) {
	return func() ([]provider.StreamChunk, error) {
		var chunks []provider.StreamChunk
		if text != "" {
			chunks = append(chunks,
				provider.StreamChunk{Type: provider.ChunkTypeTextStart, ID: "text"},
				provider.StreamChunk{Type: provider.ChunkTypeText, ID: "text", Text: text},
				provider.StreamChunk{Type: provider.ChunkTypeTextEnd, ID: "text"},
			)
		}
		for _, call := range calls {
			chunks = append(chunks, provider.StreamChunk{Type: provider.ChunkTypeToolCall, ToolCall: &call})
		}
		finish := types.FinishReasonStop
		if len(calls) > 0 {
			finish = types.FinishReasonToolCalls
		}
		return append(chunks, provider.StreamChunk{Type: provider.ChunkTypeFinish, FinishReason: finish}), nil
	}
}

// sdkApprovalSchema is the input schema of the upstream tests' tools.
func sdkApprovalSchema() map[string]any {
	return map[string]any{"type": "object", "properties": map[string]any{"value": map[string]any{"type": "string"}}, "required": []any{"value"}}
}

// sdkApprovalTool is a tool whose calls run a step that counts the call
// under name and returns output.
func sdkApprovalTool(name, output string, needsApproval workflowai.NeedsApprovalFunc) workflowai.Tool {
	return workflowai.Tool{
		InputSchema:   sdkApprovalSchema(),
		NeedsApproval: needsApproval,
		Execute: workflowai.ExecuteFunc(func(ctx context.Context, input json.RawMessage, _ workflowai.ToolCallOptions) (any, error) {
			return workflow.Step(ctx, sdkApprovalExecute, sdkApprovalExecution{Tool: name, Output: output, Input: input})
		}),
	}
}

// sdkApprovalExecution is the input of sdkApprovalExecute.
type sdkApprovalExecution struct {
	Tool   string          `json:"tool"`
	Output string          `json:"output"`
	Input  json.RawMessage `json:"input"`
}

// sdkApprovalExecute counts a tool call and returns its output.
func sdkApprovalExecute(_ context.Context, execution sdkApprovalExecution) (string, error) {
	sdkApprovalExecutions.add(execution.Tool)
	return execution.Output, nil
}

// sdkApprovalExecutions counts the executions of the tools of
// sdkApprovalTool.
var sdkApprovalExecutions = &sdkApprovalCounter{}

type sdkApprovalCounter struct {
	mu     sync.Mutex
	counts map[string]int
}

func (c *sdkApprovalCounter) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counts = map[string]int{}
}

func (c *sdkApprovalCounter) add(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counts[name]++
}

func (c *sdkApprovalCounter) count(name string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.counts[name]
}

func sdkApprovalPerformApprovedAction(_ context.Context, input struct {
	Value string `json:"value"`
}) (string, error) {
	return "performed:" + input.Value, nil
}

func sdkApprovalCompletedWork(context.Context, struct{}) (string, error) {
	return "completed before suspension", nil
}

// sdkApprovalRespond returns messages followed by a tool message with the
// user's response to approval request approvalID.
func sdkApprovalRespond(messages []workflowai.ModelMessage, approvalID string, approved bool) []workflowai.ModelMessage {
	return append(slices.Clone(messages), workflowai.ModelMessage{
		Role:    workflowai.RoleTool,
		Content: []workflowai.ContentPart{{Type: workflowai.PartToolApprovalResponse, ApprovalID: approvalID, Approved: approved}},
	})
}

// sdkApprovalTamper returns messages with the input of their tool calls
// changed.
func sdkApprovalTamper(messages []workflowai.ModelMessage) []workflowai.ModelMessage {
	out := slices.Clone(messages)
	for i, message := range out {
		if message.Role != workflowai.RoleAssistant {
			continue
		}
		out[i].Content = slices.Clone(message.Content)
		for j, part := range out[i].Content {
			if part.Type == workflowai.PartToolCall {
				out[i].Content[j].Input = json.RawMessage(`{"value":"tampered"}`)
			}
		}
	}
	return out
}

// sdkApprovalContent returns the content of every step of result.
func sdkApprovalContent(result workflowai.Result) []workflowai.ContentPart {
	var content []workflowai.ContentPart
	for _, step := range result.Steps {
		content = append(content, step.Content...)
	}
	return content
}

// sdkApprovalToolResults returns the tool results of every step of result.
func sdkApprovalToolResults(result workflowai.Result) []workflowai.ToolResult {
	var results []workflowai.ToolResult
	for _, step := range result.Steps {
		results = append(results, step.ToolResults...)
	}
	return results
}

// sdkApprovalToolResultParts returns the tool-result parts of the tool
// messages in messages.
func sdkApprovalToolResultParts(messages []workflowai.ModelMessage) []workflowai.ContentPart {
	var parts []workflowai.ContentPart
	for _, message := range messages {
		if message.Role != workflowai.RoleTool {
			continue
		}
		for _, part := range message.Content {
			if part.Type == workflowai.PartToolResult {
				parts = append(parts, part)
			}
		}
	}
	return parts
}

// sdkApprovalFind returns the first part of content of type kind.
func sdkApprovalFind(content []workflowai.ContentPart, kind string) (workflowai.ContentPart, bool) {
	i := slices.IndexFunc(content, func(part workflowai.ContentPart) bool { return part.Type == kind })
	if i < 0 {
		return workflowai.ContentPart{}, false
	}
	return content[i], true
}

func sdkApprovalPartTypes(parts []workflowai.ModelCallStreamPart) []string {
	var out []string
	for _, part := range parts {
		out = append(out, part.Type)
	}
	return out
}

// sdkApprovalActionResponse is a model response with text and a call of the
// action tool.
func sdkApprovalActionResponse() func() ([]provider.StreamChunk, error) {
	return sdkApprovalResponse("Checking the action.", sdkApprovalCall())
}

// sdkApprovalStatus is an approval policy that decides every call with kind.
func sdkApprovalStatus(kind workflowai.ToolApprovalType) workflowai.ToolApprovalFunc {
	return workflowai.ToolApprovalAlways(workflowai.ToolApprovalStatus{Type: kind})
}

// sdkApprovalRecordsTool is the deleteRecords tool of the metadata tests,
// whose schema has no required fields.
func sdkApprovalRecordsTool() workflowai.Tool {
	tool := sdkApprovalTool("deleteRecords", "deleted", nil)
	tool.Description = "Delete records. Admin only."
	tool.InputSchema = map[string]any{"type": "object", "properties": map[string]any{"table": map[string]any{"type": "string"}}}
	return tool
}

// sdkApprovalRequests returns the approval requests in the content of every
// step of result.
func sdkApprovalRequests(result workflowai.Result) []workflowai.ContentPart {
	var requests []workflowai.ContentPart
	for _, part := range sdkApprovalContent(result) {
		if part.Type == workflowai.PartToolApprovalRequest {
			requests = append(requests, part)
		}
	}
	return requests
}

// sdkApprovalRequestIDs returns the tool call IDs of the approval requests
// of result.
func sdkApprovalRequestIDs(result workflowai.Result) []string {
	var ids []string
	for _, request := range sdkApprovalRequests(result) {
		ids = append(ids, request.ToolCallID)
	}
	return ids
}

// sdkApprovalExpectShapes checks the roles and part types of messages, each
// written as "role:type,type".
func sdkApprovalExpectShapes(t *testing.T, messages []workflowai.ModelMessage, want ...string) {
	t.Helper()
	var got []string
	for _, message := range messages {
		var types []string
		for _, part := range message.Content {
			types = append(types, part.Type)
		}
		got = append(got, message.Role+":"+strings.Join(types, ","))
	}
	if !slices.Equal(got, want) {
		t.Fatalf("messages = %v, want %v", got, want)
	}
}

// sdkApprovalExpectContentTypes checks the part types of the content of each
// step of result, each written as "type,type".
func sdkApprovalExpectContentTypes(t *testing.T, result workflowai.Result, want ...string) {
	t.Helper()
	var got []string
	for _, step := range result.Steps {
		var types []string
		for _, part := range step.Content {
			types = append(types, part.Type)
		}
		got = append(got, strings.Join(types, ","))
	}
	if !slices.Equal(got, want) {
		t.Fatalf("content types = %v, want %v", got, want)
	}
}

// sdkApprovalExpectSecondCallToolResult checks that the tool messages of the
// second model call hold one part, the result of call-1 with the given
// output.
func sdkApprovalExpectSecondCallToolResult(t *testing.T, outputType, value, reason string) {
	t.Helper()
	var results []types.ToolResultContent
	var parts int
	for _, message := range testModel.calls[1].Prompt.Messages {
		if message.Role != types.RoleTool {
			continue
		}
		for _, part := range message.Content {
			parts++
			if result, ok := part.(types.ToolResultContent); ok {
				results = append(results, result)
			}
		}
	}
	if parts != 1 || len(results) != 1 || results[0].ToolCallID != "call-1" || results[0].Output == nil || string(results[0].Output.Type) != outputType || results[0].Output.Reason != reason {
		t.Fatalf("tool messages = %+v", testModel.calls[1].Prompt.Messages)
	}
	if value != "" {
		if got, _ := json.Marshal(results[0].Output.Value); string(got) != value {
			t.Fatalf("output = %s, want %s", got, value)
		}
	}
}

// sdkApprovalToolCallNames returns the names of the tool calls in the
// assistant messages of messages.
func sdkApprovalToolCallNames(messages []workflowai.ModelMessage) []string {
	var names []string
	for _, message := range messages {
		if message.Role != workflowai.RoleAssistant {
			continue
		}
		for _, part := range message.Content {
			if part.Type == workflowai.PartToolCall {
				names = append(names, part.ToolName)
			}
		}
	}
	return names
}

func sdkApprovalJSON(value any) string {
	data, _ := json.Marshal(value)
	return string(data)
}

// sdkApprovalRecorder records the last value seen for each key, since
// replays call workflow code, such as approval policies, again.
type sdkApprovalRecorder struct {
	mu       sync.Mutex
	keys     []string
	recorded map[string]any
}

func (r *sdkApprovalRecorder) record(key string, value any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.recorded == nil {
		r.recorded = map[string]any{}
	}
	if _, ok := r.recorded[key]; !ok {
		r.keys = append(r.keys, key)
	}
	r.recorded[key] = value
}

func (r *sdkApprovalRecorder) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.keys, r.recorded = nil, nil
}

func (r *sdkApprovalRecorder) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.keys)
}

// all returns the recorded values in the order their keys were first seen.
func (r *sdkApprovalRecorder) all() []any {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []any
	for _, key := range r.keys {
		out = append(out, r.recorded[key])
	}
	return out
}
