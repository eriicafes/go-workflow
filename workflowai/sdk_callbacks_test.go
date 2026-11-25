package workflowai_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/digitallysavvy/go-ai/pkg/ai"
	"github.com/digitallysavvy/go-ai/pkg/provider"
	"github.com/digitallysavvy/go-ai/pkg/provider/types"
	"github.com/digitallysavvy/go-ai/pkg/telemetry"
	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/workflowai"
	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/testworld"
)

// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > stream > should use prepareCall
func TestSDKCallbacksPrepareCallSettings(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkCallbacksSetup(sdkCallbacksSimple())
		none := types.ReasoningNone
		testOptions.PrepareCall = func(context.Context, workflowai.PrepareCallOptions) (workflowai.PrepareCallResult, error) {
			return workflowai.PrepareCallResult{GenerationSettings: workflowai.GenerationSettings{
				Reasoning:       &none,
				ProviderOptions: map[string]any{"test": map[string]any{"value": "from-prepareCall"}},
			}}, nil
		}
		if _, _, err := sdkCallbacksRun(t, user("Hello, world!")); err != nil {
			t.Fatal(err)
		}
		options := testModel.calls[0]
		want := map[string]any{"test": map[string]any{"value": "from-prepareCall"}}
		if !reflect.DeepEqual(map[string]any(options.ProviderOptions), want) {
			t.Fatalf("provider options = %v", options.ProviderOptions)
		}
		if options.Reasoning == nil || *options.Reasoning != types.ReasoningNone {
			t.Fatalf("reasoning = %v", options.Reasoning)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > stream > should pass reasoning to streamText
func TestSDKCallbacksCallReasoning(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkCallbacksSetup(sdkCallbacksSimple())
		low, none := types.ReasoningLow, types.ReasoningNone
		testOptions.Reasoning = &low
		testCallOptions.Reasoning = &none
		if _, _, err := sdkCallbacksRun(t, user("Hello, world!")); err != nil {
			t.Fatal(err)
		}
		if reasoning := testModel.calls[0].Reasoning; reasoning == nil || *reasoning != types.ReasoningNone {
			t.Fatalf("reasoning = %v", reasoning)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > stream > should allow prepareStep to override reasoning
func TestSDKCallbacksPrepareStepReasoning(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkCallbacksSetup(sdkCallbacksSimple())
		none, high := types.ReasoningNone, types.ReasoningHigh
		testOptions.Reasoning = &none
		testOptions.PrepareStep = func(context.Context, workflowai.PrepareStepInfo) (workflowai.PrepareStepResult, error) {
			var result workflowai.PrepareStepResult
			result.Reasoning = &high
			return result, nil
		}
		if _, _, err := sdkCallbacksRun(t, user("Hello, world!")); err != nil {
			t.Fatal(err)
		}
		if reasoning := testModel.calls[0].Reasoning; reasoning == nil || *reasoning != types.ReasoningHigh {
			t.Fatalf("reasoning = %v", reasoning)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > stream > should abort before calling the model when the timeout is already elapsed
func TestSDKCallbacksElapsedTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkCallbacksSetup(sdkCallbacksSimple())
		// A zero Timeout has no limit, so the shortest one stands for a
		// timeout that has already elapsed.
		testCallOptions.Timeout = time.Nanosecond
		var abortSteps []workflowai.StepResult
		aborted := false
		testCallOptions.OnAbort = func(_ context.Context, event workflowai.AbortEvent) {
			sdkCallbacksRecord("abort")
			aborted, abortSteps = true, event.Steps
		}
		testCallOptions.OnError = func(context.Context, error) { sdkCallbacksRecord("error") }
		testCallOptions.OnEnd = func(context.Context, workflowai.EndEvent) { sdkCallbacksRecord("end") }
		_, _, _ = sdkCallbacksRun(t, user("Hello, world!"))
		if len(testModel.calls) != 0 {
			t.Fatalf("model called %d times", len(testModel.calls))
		}
		if got := sdkCallbacksCalls(); !aborted || len(abortSteps) != 0 || !slices.Equal(got, []string{"abort"}) {
			t.Fatalf("callbacks = %v, aborted steps = %v", got, abortSteps)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > stream > should pass string instructions
// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > stream > should pass system message instructions
// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > stream > should pass array of system message instructions
func TestSDKCallbacksInstructions(t *testing.T) {
	system := func(text, value string) workflowai.ModelMessage {
		return workflowai.ModelMessage{
			Role:            workflowai.RoleSystem,
			Content:         []workflowai.ContentPart{{Type: workflowai.PartText, Text: text}},
			ProviderOptions: map[string]any{"test": map[string]any{"value": value}},
		}
	}
	tests := []struct {
		name         string
		instructions string
		messages     []workflowai.ModelMessage
		// wantSystem lists the system messages sent before the user message,
		// as text and provider options value.
		wantSystem [][2]string
	}{
		{name: "string", instructions: "INSTRUCTIONS"},
		{name: "system message", messages: []workflowai.ModelMessage{system("INSTRUCTIONS", "test")}, wantSystem: [][2]string{{"INSTRUCTIONS", "test"}}},
		{
			name:       "array of system messages",
			messages:   []workflowai.ModelMessage{system("INSTRUCTIONS_1", "test1"), system("INSTRUCTIONS_2", "test2")},
			wantSystem: [][2]string{{"INSTRUCTIONS_1", "test1"}, {"INSTRUCTIONS_2", "test2"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				sdkCallbacksSetup(sdkCallbacksSimple())
				testOptions.Instructions = tt.instructions
				testOptions.InstructionMessages = tt.messages
				if _, _, err := sdkCallbacksRun(t, user("Hello, world!")); err != nil {
					t.Fatal(err)
				}
				prompt := testModel.calls[0].Prompt
				if prompt.System != tt.instructions {
					t.Fatalf("system = %q", prompt.System)
				}
				if len(prompt.Messages) != len(tt.wantSystem)+1 {
					t.Fatalf("prompt = %+v", prompt.Messages)
				}
				for i, want := range tt.wantSystem {
					message := prompt.Messages[i]
					value := message.ProviderOptions["test"].(map[string]any)["value"]
					if message.Role != types.RoleSystem || sdkCallbacksPromptParts(message)[0] != "text:"+want[0] || value != want[1] {
						t.Fatalf("system message %d = %+v", i, message)
					}
				}
				last := prompt.Messages[len(prompt.Messages)-1]
				if last.Role != types.RoleUser || !slices.Equal(sdkCallbacksPromptParts(last), []string{"text:Hello, world!"}) {
					t.Fatalf("user message = %+v", last)
				}
			})
		})
	}
}

// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > stream > should expose finishReason and totalUsage on the stream result
func TestSDKCallbacksFinishReasonAndTotalUsage(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkCallbacksSetup(sdkCallbacksSimple())
		result, _, err := sdkCallbacksRun(t, user("test"))
		if err != nil {
			t.Fatal(err)
		}
		if result.FinishReason != "stop" || sdkCallbacksTokens(result.TotalUsage) != [2]int64{3, 10} {
			t.Fatalf("finish reason = %q, usage = %v", result.FinishReason, sdkCallbacksTokens(result.TotalUsage))
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > stream > should retain partial assistant content when the output length limit is reached
func TestSDKCallbacksLengthKeepsPartialContent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkCallbacksSetup(sdkCallbacksChunks(
			provider.StreamChunk{Type: provider.ChunkTypeTextStart, ID: "text-1"},
			provider.StreamChunk{Type: provider.ChunkTypeText, ID: "text-1", Text: "A partial answer."},
			provider.StreamChunk{Type: provider.ChunkTypeTextEnd, ID: "text-1"},
			sdkCallbacksFinish(types.FinishReasonLength),
		))
		var ended workflowai.Result
		testCallOptions.OnEnd = func(_ context.Context, result workflowai.EndEvent) { ended = result.Result }
		result, _, err := sdkCallbacksRun(t, user("Write an answer."))
		if err != nil {
			t.Fatal(err)
		}
		if result.FinishReason != "length" || result.Steps[0].Text != "A partial answer." {
			t.Fatalf("result = %+v", result)
		}
		last := result.Messages[len(result.Messages)-1]
		if last.Role != workflowai.RoleAssistant || !slices.Equal(sdkCallbacksParts(last), []string{"text:A partial answer."}) {
			t.Fatalf("last message = %+v", last)
		}
		if sdkCallbacksJSON(result.Steps[0].Response.Messages) != sdkCallbacksJSON([]workflowai.ModelMessage{last}) {
			t.Fatalf("response messages = %+v", result.Steps[0].Response.Messages)
		}
		if ended.FinishReason != "length" || ended.Text != "A partial answer." || sdkCallbacksJSON(ended.Messages) != sdkCallbacksJSON(result.Messages) {
			t.Fatalf("OnEnd result = %+v", ended)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > stream > should keep provider-executed tool results paired when the output length limit is reached
func TestSDKCallbacksLengthKeepsProviderResultsPaired(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkCallbacksSetup(sdkCallbacksChunks(
			sdkCallbacksProviderCall("call-1", "webSearch", `{"query":"test"}`),
			sdkCallbacksProviderResult("call-1", "webSearch", map[string]any{"hits": 3}, nil),
			provider.StreamChunk{Type: provider.ChunkTypeTextStart, ID: "text-1"},
			provider.StreamChunk{Type: provider.ChunkTypeText, ID: "text-1", Text: "A partial answer."},
			provider.StreamChunk{Type: provider.ChunkTypeTextEnd, ID: "text-1"},
			sdkCallbacksFinish(types.FinishReasonLength),
		))
		result, _, err := sdkCallbacksRun(t, user("Write an answer."))
		if err != nil {
			t.Fatal(err)
		}
		want := []string{"tool-call:call-1", "tool-result:call-1", "text:A partial answer."}
		last := result.Messages[len(result.Messages)-1]
		if result.FinishReason != "length" || last.Role != workflowai.RoleAssistant || !slices.Equal(sdkCallbacksParts(last), want) {
			t.Fatalf("finish reason = %q, last message = %v", result.FinishReason, sdkCallbacksParts(last))
		}
		if response := result.Steps[0].Response.Messages; len(response) != 1 || !slices.Equal(sdkCallbacksParts(response[0]), want) {
			t.Fatalf("response messages = %+v", response)
		}

		sdkCallbacksSetup(sdkCallbacksShort()...)
		if _, _, err := sdkCallbacksRun(t, append(slices.Clone(result.Messages), user("Continue."))...); err != nil {
			t.Fatal(err)
		}
		prompt := testModel.prompts()[0]
		if len(prompt) != 3 || prompt[1].Role != types.RoleAssistant || !slices.Equal(sdkCallbacksPromptParts(prompt[1]), want) {
			t.Fatalf("continued prompt = %+v", prompt)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > stream > should preserve $description tool errors alongside provider results on length completion
func TestSDKCallbacksLengthKeepsToolErrors(t *testing.T) {
	tests := []struct {
		description string
		toolName    string
		input       string
	}{
		{"truncated JSON", "local", `{"value":`},
		{"invalid input", "local", `{"value":123}`},
		{"unknown tool", "unknown", `{"value":"test"}`},
	}
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				sdkCallbacksSetup(sdkCallbacksChunks(
					sdkCallbacksProviderCall("provider-call", "webSearch", `{"query":"test"}`),
					sdkCallbacksProviderResult("provider-call", "webSearch", map[string]any{"hits": 3}, nil),
					provider.StreamChunk{Type: provider.ChunkTypeToolCall, ToolCall: &types.ToolCall{ID: "invalid-call", ToolName: tt.toolName, RawArguments: tt.input}},
					sdkCallbacksFinish(types.FinishReasonLength),
				))
				executed, approvals := 0, 0
				testOptions.Tools = workflowai.ToolSet{"local": {
					InputSchema: map[string]any{
						"type":       "object",
						"properties": map[string]any{"value": map[string]any{"type": "string"}},
						"required":   []any{"value"},
					},
					NeedsApproval: func(context.Context, json.RawMessage, workflowai.ToolCallOptions) (bool, error) {
						approvals++
						return true, nil
					},
					Execute: workflowai.ExecuteFunc(func(context.Context, json.RawMessage, workflowai.ToolCallOptions) (any, error) {
						executed++
						return "should-not-run", nil
					}),
				}}
				result, chunks, err := sdkCallbacksRun(t, user("Write an answer."))
				if err != nil {
					t.Fatal(err)
				}
				if result.FinishReason != "length" || len(testModel.calls) != 1 || executed != 0 || approvals != 0 {
					t.Fatalf("finish reason = %q after %d model calls, %d executions, %d approvals", result.FinishReason, len(testModel.calls), executed, approvals)
				}
				if !slices.ContainsFunc(chunks, func(chunk ai.UIMessageChunk) bool {
					return chunk["type"] == "tool-output-error" && chunk["toolCallId"] == "invalid-call"
				}) {
					t.Fatalf("chunks = %v", chunkTypes(chunks))
				}
				wantAssistant := []string{"tool-call:provider-call", "tool-result:provider-call", "tool-call:invalid-call"}
				check := func(messages []workflowai.ModelMessage) {
					t.Helper()
					if len(messages) != 2 || messages[0].Role != workflowai.RoleAssistant || !slices.Equal(sdkCallbacksParts(messages[0]), wantAssistant) {
						t.Fatalf("messages = %+v", messages)
					}
					errorResult := messages[1]
					if errorResult.Role != workflowai.RoleTool || len(errorResult.Content) != 1 || errorResult.Content[0].ToolCallID != "invalid-call" ||
						errorResult.Content[0].ToolName != tt.toolName || errorResult.Content[0].Output == nil || errorResult.Content[0].Output.Type != "error-text" {
						t.Fatalf("error message = %+v", errorResult)
					}
				}
				check(result.Messages[1:])
				check(result.Steps[0].Response.Messages)

				sdkCallbacksSetup(sdkCallbacksShort()...)
				if _, _, err := sdkCallbacksRun(t, append(slices.Clone(result.Messages), user("Continue."))...); err != nil {
					t.Fatal(err)
				}
				prompt := testModel.prompts()[0]
				if len(prompt) != 4 || !slices.Equal(sdkCallbacksPromptParts(prompt[1]), wantAssistant) || prompt[2].Role != types.RoleTool {
					t.Fatalf("continued prompt = %+v", prompt)
				}
			})
		})
	}
}

// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > stream > should retain assistant content when the stream ends with %s
func TestSDKCallbacksKeepsContentForEveryFinish(t *testing.T) {
	tests := []struct {
		name   string
		reason types.FinishReason
	}{
		{"content-filter", types.FinishReasonContentFilter},
		{"error", types.FinishReasonError},
		{"interrupted", types.FinishReason("interrupted")},
		{"other", types.FinishReasonOther},
		{"unknown", types.FinishReason("unknown")},
		{"no finish part", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				chunks := []provider.StreamChunk{
					{Type: provider.ChunkTypeReasoningStart, ID: "reasoning-1"},
					{Type: provider.ChunkTypeReasoning, ID: "reasoning-1", Reasoning: "Thinking."},
					{Type: provider.ChunkTypeReasoningEnd, ID: "reasoning-1"},
					{Type: provider.ChunkTypeTextStart, ID: "text-1"},
					{Type: provider.ChunkTypeText, ID: "text-1", Text: "A partial answer."},
					{Type: provider.ChunkTypeTextEnd, ID: "text-1"},
				}
				if tt.reason != "" {
					chunks = append(chunks, sdkCallbacksFinish(tt.reason))
				}
				sdkCallbacksSetup(sdkCallbacksChunks(chunks...))
				var ended *workflowai.Result
				testCallOptions.OnEnd = func(_ context.Context, result workflowai.EndEvent) { ended = &result.Result }
				result, _, err := sdkCallbacksRun(t, user("Write an answer."))
				if err != nil {
					t.Fatal(err)
				}
				want := []string{"reasoning:Thinking.", "text:A partial answer."}
				last := result.Messages[len(result.Messages)-1]
				if last.Role != workflowai.RoleAssistant || !slices.Equal(sdkCallbacksParts(last), want) {
					t.Fatalf("last message = %v", sdkCallbacksParts(last))
				}
				if sdkCallbacksJSON(result.Steps[0].Response.Messages) != sdkCallbacksJSON([]workflowai.ModelMessage{last}) {
					t.Fatalf("response messages = %+v", result.Steps[0].Response.Messages)
				}
				if ended == nil || ended.Text != "A partial answer." || sdkCallbacksJSON(ended.Messages) != sdkCallbacksJSON(result.Messages) {
					t.Fatalf("OnEnd result = %+v", ended)
				}
			})
		})
	}
}

// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > experimental_onStart > stream > should call experimental_onStart from constructor
// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > experimental_onStart > stream > should call experimental_onStart from stream method
// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > experimental_onStart > stream > should call both constructor and method experimental_onStart in correct order
func TestSDKCallbacksOnStartOrder(t *testing.T) {
	sdkCallbacksCheckOrder(t, func(callbacks *workflowai.Callbacks, name string) {
		callbacks.OnStart = func(context.Context, workflowai.StartEvent) { sdkCallbacksRecord(name) }
	}, sdkCallbacksShort()...)
}

// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > experimental_onStart > stream > should pass correct event information
func TestSDKCallbacksOnStartEvent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkCallbacksSetup(sdkCallbacksShort()...)
		temperature, maxOutputTokens := 0.7, 500
		testOptions.Instructions = "You are a helpful assistant"
		testOptions.Temperature = &temperature
		testOptions.MaxOutputTokens = &maxOutputTokens
		testOptions.RuntimeContext = map[string]any{"userId": "test-user"}
		var event workflowai.StartEvent
		testCallOptions.OnStart = func(_ context.Context, e workflowai.StartEvent) { event = e }
		if _, _, err := sdkCallbacksRun(t, user("Hello, world!")); err != nil {
			t.Fatal(err)
		}
		if sdkCallbacksJSON(event.Messages) != sdkCallbacksJSON([]workflowai.ModelMessage{user("Hello, world!")}) {
			t.Fatalf("messages = %+v", event.Messages)
		}
		if event.Model != provider.LanguageModel(testModel) {
			t.Errorf("model = %v", event.Model)
		}
		if !reflect.DeepEqual(event.RuntimeContext, map[string]any{"userId": "test-user"}) {
			t.Errorf("runtime context = %v", event.RuntimeContext)
		}
		if len(event.ToolsContext) != 0 {
			t.Errorf("tools context = %v", event.ToolsContext)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > stable start callbacks > uses stable callbacks in preference to experimental aliases in each option scope
func TestSDKCallbacksStartCallbacksOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkCallbacksSetup(sdkCallbacksShort()...)
		testOptions.OnStart = func(context.Context, workflowai.StartEvent) { sdkCallbacksRecord("constructor onStart") }
		testOptions.OnStepStart = func(context.Context, workflowai.StepStartEvent) { sdkCallbacksRecord("constructor onStepStart") }
		testCallOptions.OnStart = func(context.Context, workflowai.StartEvent) { sdkCallbacksRecord("stream onStart") }
		testCallOptions.OnStepStart = func(context.Context, workflowai.StepStartEvent) { sdkCallbacksRecord("stream onStepStart") }
		if _, _, err := sdkCallbacksRun(t, user("Hello, world!")); err != nil {
			t.Fatal(err)
		}
		want := []string{"constructor onStart", "stream onStart", "constructor onStepStart", "stream onStepStart"}
		if got := sdkCallbacksCalls(); !slices.Equal(got, want) {
			t.Fatalf("calls = %v, want %v", got, want)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > experimental_onStepStart > stream > should call experimental_onStepStart from constructor
// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > experimental_onStepStart > stream > should call experimental_onStepStart from stream method
// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > experimental_onStepStart > stream > should call both constructor and method experimental_onStepStart in correct order
func TestSDKCallbacksOnStepStartOrder(t *testing.T) {
	sdkCallbacksCheckOrder(t, func(callbacks *workflowai.Callbacks, name string) {
		callbacks.OnStepStart = func(context.Context, workflowai.StepStartEvent) { sdkCallbacksRecord(name) }
	}, sdkCallbacksShort()...)
}

// The SDK reports 3 messages because its event shares the messages array
// that the step's response is appended to later. The event holds the
// conversation the step starts from.
//
// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > experimental_onStepStart > stream > should pass correct event information
func TestSDKCallbacksOnStepStartEvent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkCallbacksSetup(sdkCallbacksShort()...)
		testOptions.Instructions = "You are a helpful assistant"
		testOptions.RuntimeContext = map[string]any{"userId": "test-user"}
		var event workflowai.StepStartEvent
		testCallOptions.OnStepStart = func(_ context.Context, e workflowai.StepStartEvent) { event = e }
		if _, _, err := sdkCallbacksRun(t, user("Hello, world!")); err != nil {
			t.Fatal(err)
		}
		if event.StepNumber != 0 || len(event.Messages) != 1 {
			t.Fatalf("event = %+v", event)
		}
		if event.Model != provider.LanguageModel(testModel) {
			t.Errorf("model = %v", event.Model)
		}
		if len(event.Steps) != 0 {
			t.Errorf("steps = %v", event.Steps)
		}
		if !reflect.DeepEqual(event.RuntimeContext, map[string]any{"userId": "test-user"}) {
			t.Errorf("runtime context = %v", event.RuntimeContext)
		}
		if len(event.ToolsContext) != 0 {
			t.Errorf("tools context = %v", event.ToolsContext)
		}
	})
}

// OnStepEnd is also the Go name of the SDK's deprecated onStepFinish.
//
// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > onStepEnd > stream > should call onStepEnd from constructor and stream method in correct order
// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > onStepEnd > stream > should prefer onStepEnd over deprecated onStepFinish
// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > onStepFinish > stream > should call onStepFinish from constructor
// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > onStepFinish > stream > should call onStepFinish from stream method
// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > onStepFinish > stream > should call both constructor and method onStepFinish in correct order
func TestSDKCallbacksOnStepEndOrder(t *testing.T) {
	sdkCallbacksCheckOrder(t, func(callbacks *workflowai.Callbacks, name string) {
		callbacks.OnStepEnd = func(context.Context, workflowai.StepResult) { sdkCallbacksRecord(name) }
	}, sdkCallbacksSimple())
}

// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > completed step tool results > exposes static tool results to step consumers
func TestSDKCallbacksToolResultsForStepConsumers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkCallbacksSetup(sdkCallbacksToolCall(`{ "value": "test" }`), sdkCallbacksText("done"))
		testOptions.Tools = sdkCallbacksTools(sdkCallbacksTestTool())
		var prepared, ended [][]workflowai.ToolResult
		testOptions.PrepareStep = func(_ context.Context, info workflowai.PrepareStepInfo) (workflowai.PrepareStepResult, error) {
			if info.StepNumber > 0 {
				prepared = append(prepared, info.Steps[0].ToolResults)
			}
			return workflowai.PrepareStepResult{}, nil
		}
		testOptions.OnStart = func(context.Context, workflowai.StartEvent) { prepared, ended = nil, nil }
		testOptions.OnStepEnd = func(_ context.Context, step workflowai.StepResult) { ended = append(ended, step.ToolResults) }
		result, _, err := sdkCallbacksRun(t, user("test"))
		if err != nil {
			t.Fatal(err)
		}
		want := []map[string]any{{"toolCallId": "call-1", "toolName": "testTool", "input": map[string]any{"value": "test"}, "output": "test-result"}}
		if len(prepared) != 1 || !reflect.DeepEqual(sdkCallbacksResults(prepared[0]), want) {
			t.Fatalf("PrepareStep tool results = %v", prepared)
		}
		if len(ended) == 0 || !reflect.DeepEqual(sdkCallbacksResults(ended[0]), want) {
			t.Fatalf("OnStepEnd tool results = %v", ended)
		}
		if got := sdkCallbacksResults(result.Steps[0].ToolResults); !reflect.DeepEqual(got, want) {
			t.Fatalf("tool results = %v", got)
		}
		if !slices.ContainsFunc(result.Steps[0].Content, func(part workflowai.ContentPart) bool {
			return part.Type == workflowai.PartToolResult && part.ToolCallID == "call-1" && part.ToolName == "testTool" &&
				part.Output != nil && sdkCallbacksDecode(part.Output.Value) == "test-result"
		}) {
			t.Fatalf("content = %v", sdkCallbacksContent(result.Steps[0].Content))
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > completed step tool results > makes tool outputs available to stop conditions
func TestSDKCallbacksStopWhenSeesToolOutputs(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkCallbacksSetup(sdkCallbacksToolCall(`{ "value": "test" }`), sdkCallbacksText("done"))
		testOptions.Tools = sdkCallbacksTools(sdkCallbacksTestTool())
		testOptions.StopWhen = []workflowai.StopCondition{func(steps []workflowai.StepResult) bool {
			return slices.ContainsFunc(steps[len(steps)-1].ToolResults, func(result workflowai.ToolResult) bool {
				return sdkCallbacksDecode(result.Output) == "test-result"
			})
		}}
		result, _, err := sdkCallbacksRun(t, user("test"))
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Steps) != 1 || sdkCallbacksDecode(result.Steps[0].ToolResults[0].Output) != "test-result" {
			t.Fatalf("steps = %+v", result.Steps)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > completed step tool results > exposes local tool failures as errors without successful results
func TestSDKCallbacksLocalToolFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkCallbacksSetup(sdkCallbacksToolCall(`{ "value": "test" }`), sdkCallbacksText("done"))
		testOptions.Tools = sdkCallbacksTools(workflowai.Tool{Execute: workflowai.ExecuteStep(sdkCallbacksFailingTool)})
		testOptions.StopWhen = []workflowai.StopCondition{sdkCallbacksHasToolError}
		result, _, err := sdkCallbacksRun(t, user("test"))
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Steps) != 1 {
			t.Fatalf("steps = %+v", result.Steps)
		}
		if !slices.ContainsFunc(result.Steps[0].Content, func(part workflowai.ContentPart) bool {
			return part.Type == "tool-error" && part.ToolCallID == "call-1" && part.ToolName == "testTool" &&
				reflect.DeepEqual(sdkCallbacksDecode(part.Input), map[string]any{"value": "test"}) && !part.ProviderExecuted
		}) {
			t.Fatalf("content = %v", sdkCallbacksContent(result.Steps[0].Content))
		}
		if len(result.Steps[0].ToolResults) != 0 {
			t.Fatalf("tool results = %+v", result.Steps[0].ToolResults)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > completed step tool results > exposes provider tool failures as errors without successful results
func TestSDKCallbacksProviderToolFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkCallbacksSetup(sdkCallbacksChunks(
			sdkCallbacksMetadata("id-0"),
			sdkCallbacksProviderCall("call-1", "webSearch", `{ "query": "test" }`),
			sdkCallbacksProviderResult("call-1", "webSearch", "provider failed", errors.New("provider failed")),
			sdkCallbacksFinish(types.FinishReasonToolCalls),
		))
		testOptions.StopWhen = []workflowai.StopCondition{sdkCallbacksHasToolError}
		result, _, err := sdkCallbacksRun(t, user("test"))
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Steps) != 1 {
			t.Fatalf("steps = %+v", result.Steps)
		}
		if !slices.ContainsFunc(result.Steps[0].Content, func(part workflowai.ContentPart) bool {
			return part.Type == "tool-error" && part.ToolCallID == "call-1" && part.ToolName == "webSearch" &&
				reflect.DeepEqual(sdkCallbacksDecode(part.Input), map[string]any{"query": "test"}) && part.ProviderExecuted
		}) {
			t.Fatalf("content = %v", sdkCallbacksContent(result.Steps[0].Content))
		}
		if len(result.Steps[0].ToolResults) != 0 {
			t.Fatalf("tool results = %+v", result.Steps[0].ToolResults)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > completed step tool results > preserves terminal provider tool results in stream order
func TestSDKCallbacksProviderResultsInStreamOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		resultA := sdkCallbacksProviderResult("call-a", "providerA", map[string]any{"value": "a"}, nil)
		resultA.ToolResult.ProviderMetadata = map[string]any{"testProvider": map[string]any{"resultId": "result-a"}}
		sdkCallbacksSetup(sdkCallbacksChunks(
			sdkCallbacksProviderCall("call-a", "providerA", `{}`),
			sdkCallbacksProviderCall("call-b", "providerB", `{}`),
			resultA,
			sdkCallbacksProviderResult("call-b", "providerB", map[string]any{"value": "b"}, nil),
			sdkCallbacksFinish(types.FinishReasonStop),
		))
		result, _, err := sdkCallbacksRun(t, user("test"))
		if err != nil {
			t.Fatal(err)
		}
		want := []string{"tool-call:call-a", "tool-call:call-b", "tool-result:call-a", "tool-result:call-b"}
		if len(testModel.calls) != 1 || !slices.Equal(sdkCallbacksContent(result.Steps[0].Content), want) {
			t.Fatalf("content = %v after %d model calls", sdkCallbacksContent(result.Steps[0].Content), len(testModel.calls))
		}
		if !slices.ContainsFunc(result.Steps[0].ToolResults, func(result workflowai.ToolResult) bool {
			return result.ToolCallID == "call-a" && result.ToolName == "providerA" &&
				reflect.DeepEqual(sdkCallbacksDecode(result.Input), map[string]any{}) &&
				reflect.DeepEqual(sdkCallbacksDecode(result.Output), map[string]any{"value": "a"}) &&
				result.ProviderExecuted &&
				sdkCallbacksJSON(result.ProviderMetadata) == `{"testProvider":{"resultId":"result-a"}}`
		}) {
			t.Fatalf("tool results = %+v", result.Steps[0].ToolResults)
		}
		var assistant workflowai.ModelMessage
		for _, message := range result.Messages {
			if message.Role == workflowai.RoleAssistant {
				assistant = message
				break
			}
		}
		if got := sdkCallbacksParts(assistant); !slices.Equal(got, want) {
			t.Fatalf("assistant message = %v", got)
		}
		if sdkCallbacksJSON(result.Steps[0].Response.Messages) != sdkCallbacksJSON(result.Messages[1:]) {
			t.Fatalf("response messages = %+v", result.Steps[0].Response.Messages)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > completed step tool results > replays deferred provider results emitted in a later response
func TestSDKCallbacksDeferredProviderResult(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		deferred := sdkCallbacksProviderResult("program-call", "program", map[string]any{"status": "done"}, nil)
		deferred.ToolResult.ProviderMetadata = map[string]any{"testProvider": map[string]any{"resultId": "deferred-result"}}
		sdkCallbacksSetup(
			sdkCallbacksChunks(
				sdkCallbacksProviderCall("program-call", "program", `{"code":"run()"}`),
				provider.StreamChunk{Type: provider.ChunkTypeToolCall, ToolCall: &types.ToolCall{ID: "client-call", ToolName: "clientTool", RawArguments: `{}`}},
				sdkCallbacksFinish(types.FinishReasonToolCalls),
			),
			sdkCallbacksChunks(
				deferred,
				provider.StreamChunk{Type: provider.ChunkTypeTextStart, ID: "text-1"},
				provider.StreamChunk{Type: provider.ChunkTypeText, ID: "text-1", Text: "Done."},
				provider.StreamChunk{Type: provider.ChunkTypeTextEnd, ID: "text-1"},
				sdkCallbacksFinish(types.FinishReasonStop),
			),
		)
		testOptions.Tools = workflowai.ToolSet{"clientTool": {Execute: workflowai.ExecuteStep(sdkCallbacksClientTool)}}
		result, _, err := sdkCallbacksRun(t, user("run the program"))
		if err != nil {
			t.Fatal(err)
		}
		if len(testModel.calls) != 2 || len(result.Steps) != 2 {
			t.Fatalf("%d model calls, steps = %+v", len(testModel.calls), result.Steps)
		}
		if !slices.ContainsFunc(result.Steps[1].Content, func(part workflowai.ContentPart) bool {
			return part.Type == workflowai.PartToolResult && part.ToolCallID == "program-call" && part.ToolName == "program" && part.ProviderExecuted &&
				part.Output != nil && reflect.DeepEqual(sdkCallbacksDecode(part.Output.Value), map[string]any{"status": "done"})
		}) {
			t.Fatalf("content = %v", sdkCallbacksContent(result.Steps[1].Content))
		}
		if !slices.ContainsFunc(result.Steps[1].ToolResults, func(result workflowai.ToolResult) bool {
			return result.ToolCallID == "program-call" && result.ToolName == "program" &&
				reflect.DeepEqual(sdkCallbacksDecode(result.Output), map[string]any{"status": "done"}) &&
				result.ProviderExecuted &&
				sdkCallbacksJSON(result.ProviderMetadata) == `{"testProvider":{"resultId":"deferred-result"}}`
		}) {
			t.Fatalf("tool results = %+v", result.Steps[1].ToolResults)
		}
		last := result.Messages[len(result.Messages)-1]
		if last.Role != workflowai.RoleAssistant || !slices.Equal(sdkCallbacksParts(last), []string{"tool-result:program-call", "text:Done."}) {
			t.Fatalf("last message = %v", sdkCallbacksParts(last))
		}
		var responses []workflowai.ModelMessage
		for _, step := range result.Steps {
			responses = append(responses, step.Response.Messages...)
		}
		if sdkCallbacksJSON(responses) != sdkCallbacksJSON(result.Messages[1:]) {
			t.Fatalf("response messages = %+v", responses)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > completed step tool results > preserves provider result order and response messages when a client tool pauses the loop
func TestSDKCallbacksClientToolPauseKeepsProviderResults(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkCallbacksSetup(sdkCallbacksChunks(
			sdkCallbacksProviderCall("provider-call", "providerTool", `{}`),
			provider.StreamChunk{Type: provider.ChunkTypeToolCall, ToolCall: &types.ToolCall{ID: "client-call", ToolName: "clientTool", RawArguments: `{}`}},
			sdkCallbacksProviderResult("provider-call", "providerTool", map[string]any{"value": "provider result"}, nil),
			sdkCallbacksFinish(types.FinishReasonToolCalls),
		))
		testOptions.Tools = workflowai.ToolSet{"clientTool": {InputSchema: map[string]any{"type": "object"}}}
		result, _, err := sdkCallbacksRun(t, user("test"))
		if err != nil {
			t.Fatal(err)
		}
		if len(testModel.calls) != 1 {
			t.Fatalf("model called %d times", len(testModel.calls))
		}
		var callIDs, resultIDs []string
		for _, call := range result.ToolCalls {
			callIDs = append(callIDs, call.ToolCallID)
		}
		for _, result := range result.ToolResults {
			resultIDs = append(resultIDs, result.ToolCallID)
		}
		if !slices.Equal(callIDs, []string{"provider-call", "client-call"}) || !slices.Equal(resultIDs, []string{"provider-call"}) {
			t.Fatalf("tool calls = %v, tool results = %v", callIDs, resultIDs)
		}
		response := result.Steps[0].Response.Messages
		if sdkCallbacksJSON(response) != sdkCallbacksJSON(result.Messages[1:]) {
			t.Fatalf("response messages = %+v", response)
		}
		var parts []string
		for _, message := range response {
			if message.Role == workflowai.RoleAssistant {
				parts = append(parts, sdkCallbacksParts(message)...)
			}
		}
		if want := []string{"tool-call:provider-call", "tool-call:client-call", "tool-result:provider-call"}; !slices.Equal(parts, want) {
			t.Fatalf("assistant parts = %v, want %v", parts, want)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > onStepFinish > stream > should pass stepResult to onStepFinish callback
func TestSDKCallbacksOnStepEndStepResult(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkCallbacksSetup(sdkCallbacksSimple())
		var step workflowai.StepResult
		testCallOptions.OnStepEnd = func(_ context.Context, s workflowai.StepResult) { step = s }
		if _, _, err := sdkCallbacksRun(t, user("Hello, world!")); err != nil {
			t.Fatal(err)
		}
		if step.FinishReason != "stop" || step.StepNumber != 0 || step.Text != "Hello, world!" || sdkCallbacksTokens(step.Usage) != [2]int64{3, 10} {
			t.Fatalf("step = %+v", step)
		}
		if sdkCallbacksJSON(step.ProviderMetadata) != `{"testProvider":{"testKey":"testValue"}}` {
			t.Fatalf("provider metadata = %v", step.ProviderMetadata)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > onToolExecutionStart > stream > should call onToolExecutionStart from constructor
// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > onToolExecutionStart > stream > should call onToolExecutionStart from stream method
// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > onToolExecutionStart > stream > should call both constructor and method in correct order
func TestSDKCallbacksOnToolExecutionStartOrder(t *testing.T) {
	sdkCallbacksCheckOrder(t, func(callbacks *workflowai.Callbacks, name string) {
		callbacks.OnToolExecutionStart = func(context.Context, workflowai.ToolExecutionStartEvent) { sdkCallbacksRecord(name) }
	}, sdkCallbacksToolLoop()...)
}

// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > onToolExecutionStart > stream > should pass correct event information
func TestSDKCallbacksOnToolExecutionStartEvent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkCallbacksSetup(sdkCallbacksToolLoop()...)
		testOptions.Tools = sdkCallbacksTools(sdkCallbacksTestTool())
		var event workflowai.ToolExecutionStartEvent
		testCallOptions.OnToolExecutionStart = func(_ context.Context, e workflowai.ToolExecutionStartEvent) { event = e }
		if _, _, err := sdkCallbacksRun(t, user("test")); err != nil {
			t.Fatal(err)
		}
		if event.ToolCall.ToolName != "testTool" || event.ToolCall.ToolCallID != "call-1" ||
			!reflect.DeepEqual(sdkCallbacksDecode(event.ToolCall.Input), map[string]any{"value": "test"}) {
			t.Fatalf("tool call = %+v", event.ToolCall)
		}
		if len(event.Messages) != 1 {
			t.Errorf("messages = %v", event.Messages)
		}
		if event.ToolContext != nil {
			t.Errorf("tool context = %v", event.ToolContext)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > onToolExecutionEnd > stream > should call onToolExecutionEnd from constructor
// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > onToolExecutionEnd > stream > should call onToolExecutionEnd from stream method
// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > onToolExecutionEnd > stream > should call both constructor and method in correct order
func TestSDKCallbacksOnToolExecutionEndOrder(t *testing.T) {
	sdkCallbacksCheckOrder(t, func(callbacks *workflowai.Callbacks, name string) {
		callbacks.OnToolExecutionEnd = func(context.Context, workflowai.ToolExecutionEndEvent) { sdkCallbacksRecord(name) }
	}, sdkCallbacksToolLoop()...)
}

// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > onToolExecutionEnd > stream > should pass correct event information on success
func TestSDKCallbacksOnToolExecutionEndEvent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkCallbacksSetup(sdkCallbacksToolCall(`{ "value": "hello" }`), sdkCallbacksText("done"))
		testOptions.Tools = sdkCallbacksTools(sdkCallbacksTestTool())
		var event workflowai.ToolExecutionEndEvent
		testCallOptions.OnToolExecutionEnd = func(_ context.Context, e workflowai.ToolExecutionEndEvent) { event = e }
		if _, _, err := sdkCallbacksRun(t, user("test")); err != nil {
			t.Fatal(err)
		}
		if event.ToolCall.ToolName != "testTool" || event.ToolCall.ToolCallID != "call-1" ||
			!reflect.DeepEqual(sdkCallbacksDecode(event.ToolCall.Input), map[string]any{"value": "hello"}) {
			t.Fatalf("tool call = %+v", event.ToolCall)
		}
		if event.Error != nil || event.Output != "hello-result" {
			t.Fatalf("output = %v, error = %v", event.Output, event.Error)
		}
		if event.DurationMs < 0 {
			t.Errorf("duration = %v", event.DurationMs)
		}
		if !event.Success {
			t.Errorf("success = %v", event.Success)
		}
		if len(event.Messages) != 1 {
			t.Errorf("messages = %v", event.Messages)
		}
		if event.ToolContext != nil {
			t.Errorf("tool context = %v", event.ToolContext)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > onEnd > stream > should call onFinish from constructor
// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > onEnd > stream > should call onFinish from stream method
// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > onEnd > stream > should call both constructor and method in correct order
func TestSDKCallbacksOnEndOrder(t *testing.T) {
	sdkCallbacksCheckOrder(t, func(callbacks *workflowai.Callbacks, name string) {
		callbacks.OnEnd = func(context.Context, workflowai.EndEvent) { sdkCallbacksRecord(name) }
	}, sdkCallbacksSimple())
}

// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > onEnd > stream > should pass correct event information
func TestSDKCallbacksOnEndEvent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkCallbacksSetup(sdkCallbacksSimple())
		var ended workflowai.Result
		testCallOptions.OnEnd = func(_ context.Context, result workflowai.EndEvent) { ended = result.Result }
		if _, _, err := sdkCallbacksRun(t, user("test")); err != nil {
			t.Fatal(err)
		}
		if ended.Text != "Hello, world!" || ended.FinishReason != "stop" || len(ended.Steps) != 1 || sdkCallbacksTokens(ended.TotalUsage) != [2]int64{3, 10} {
			t.Fatalf("OnEnd result = %+v", ended)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > telemetry integrations > stream > should call per-call integration listeners for all lifecycle events
func TestSDKCallbacksTelemetryLifecycle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkCallbacksSetup(sdkCallbacksToolLoop()...)
		testOptions.Tools = sdkCallbacksTools(sdkCallbacksTestTool())
		testOptions.Telemetry = &telemetry.Options{Integrations: []telemetry.TelemetryIntegration{&sdkCallbacksIntegration{}}}
		if _, _, err := sdkCallbacksRun(t, user("test")); err != nil {
			t.Fatal(err)
		}
		want := []string{"onStart", "onStepStart", "onToolExecutionStart", "onToolExecutionEnd", "onStepEnd", "onStepStart", "onStepEnd", "onEnd"}
		// The integration also records its ExecuteTool calls and the outcome
		// of each tool execution, which other tests check.
		got := slices.DeleteFunc(sdkCallbacksEvents(), func(event string) bool { return event == "executeTool" || strings.Contains(event, ":") })
		if !slices.Equal(got, want) {
			t.Fatalf("events = %v, want %v", got, want)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > telemetry integrations > stream > should include only configured runtime and tools context fields
func TestSDKCallbacksTelemetryContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkCallbacksSetup(sdkCallbacksToolLoop()...)
		testOptions.Tools = sdkCallbacksTools(sdkCallbacksTestTool())
		testOptions.RuntimeContext = map[string]any{"requestId": "request-123", "secret": "runtime-secret"}
		testOptions.ToolsContext = map[string]any{"testTool": map[string]any{"requestId": "tool-request-123", "secret": "tool-secret"}}
		integration := &sdkCallbacksIntegration{}
		testOptions.Telemetry = &telemetry.Options{
			IncludeRuntimeContext: map[string]bool{"requestId": true},
			IncludeToolsContext:   map[string]map[string]bool{"testTool": {"requestId": true}},
			Integrations:          []telemetry.TelemetryIntegration{integration},
		}
		if _, _, err := sdkCallbacksRun(t, user("test")); err != nil {
			t.Fatal(err)
		}
		integration.mu.Lock()
		defer integration.mu.Unlock()
		if len(integration.starts) == 0 || len(integration.toolStarts) == 0 || len(integration.ends) == 0 {
			t.Fatalf("%d start, %d tool start, %d end events", len(integration.starts), len(integration.toolStarts), len(integration.ends))
		}
		start, toolStart, end := integration.starts[0], integration.toolStarts[0], integration.ends[len(integration.ends)-1]
		if !reflect.DeepEqual(start.RuntimeContext, map[string]any{"requestId": "request-123"}) {
			t.Errorf("start runtime context = %v", start.RuntimeContext)
		}
		if !reflect.DeepEqual(start.ToolsContext, map[string]any{"testTool": map[string]any{"requestId": "tool-request-123"}}) {
			t.Errorf("start tools context = %v", start.ToolsContext)
		}
		if !reflect.DeepEqual(toolStart.ToolContext, map[string]any{"requestId": "tool-request-123"}) {
			t.Errorf("tool context = %v", toolStart.ToolContext)
		}
		if !reflect.DeepEqual(end.RuntimeContext, map[string]any{"requestId": "request-123"}) {
			t.Errorf("end runtime context = %v", end.RuntimeContext)
		}
		events := fmt.Sprintf("%+v %+v %+v", start, toolStart, end)
		if strings.Contains(events, "runtime-secret") || strings.Contains(events, "tool-secret") {
			t.Errorf("events = %s", events)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > telemetry integrations > stream > should call globally registered integration listeners
func TestSDKCallbacksTelemetryGlobalIntegration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkCallbacksSetup(sdkCallbacksText("Hello!"))
		telemetry.AddTelemetryIntegration(&sdkCallbacksIntegration{prefix: "global-"})
		defer telemetry.ClearTelemetryIntegrations()
		if _, _, err := sdkCallbacksRun(t, user("test")); err != nil {
			t.Fatal(err)
		}
		want := []string{"global-onStart", "global-onStepEnd", "global-onEnd"}
		if got := slices.DeleteFunc(sdkCallbacksEvents(), func(event string) bool { return event == "global-onStepStart" }); !slices.Equal(got, want) {
			t.Fatalf("events = %v, want %v", got, want)
		}
	})
}

// A Go integration fails by panicking.
//
// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > telemetry integrations > stream > should not break streaming when an integration listener throws
func TestSDKCallbacksTelemetryPanickingIntegration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkCallbacksSetup(sdkCallbacksText("Hello!"))
		testOptions.Telemetry = &telemetry.Options{Integrations: []telemetry.TelemetryIntegration{&sdkCallbacksIntegration{panics: true}}}
		result, _, err := sdkCallbacksRun(t, user("test"))
		if err != nil {
			t.Fatal(err)
		}
		if result.Text != "Hello!" {
			t.Fatalf("result = %+v", result)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > tool approval > stream > should pause agent when tool has needsApproval: true
// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > tool approval > stream > should support needsApproval as a function
func TestSDKCallbacksNeedsApprovalPauses(t *testing.T) {
	var approvalInput any
	tests := []struct {
		name          string
		needsApproval workflowai.NeedsApprovalFunc
		wantInput     any
	}{
		{"always", workflowai.NeedsApprovalAlways, nil},
		{"function", func(_ context.Context, input json.RawMessage, _ workflowai.ToolCallOptions) (bool, error) {
			approvalInput = sdkCallbacksDecode(input)
			return true, nil
		}, map[string]any{"value": "test"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				approvalInput = nil
				sdkCallbacksSetup(sdkCallbacksToolLoop()...)
				tool := sdkCallbacksTestTool()
				tool.NeedsApproval = tt.needsApproval
				testOptions.Tools = sdkCallbacksTools(tool)
				result, _, err := sdkCallbacksRun(t, user("test"))
				if err != nil {
					t.Fatal(err)
				}
				if len(result.ToolCalls) != 1 || result.ToolCalls[0].ToolName != "testTool" || len(result.ToolResults) != 0 {
					t.Fatalf("tool calls = %+v, tool results = %+v", result.ToolCalls, result.ToolResults)
				}
				if !reflect.DeepEqual(approvalInput, tt.wantInput) {
					t.Fatalf("approval input = %v", approvalInput)
				}
			})
		})
	}
}

// AI SDK: packages/workflow/src/workflow-agent-compat.test.ts > WorkflowAgent (ToolLoopAgent compat) > tool approval > stream > should emit telemetry when an approved tool resumes
func TestSDKCallbacksTelemetryApprovedToolResumes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkCallbacksSetup(sdkCallbacksSimple())
		tool := sdkCallbacksTestTool()
		tool.NeedsApproval = workflowai.NeedsApprovalAlways
		testOptions.Tools = sdkCallbacksTools(tool)
		testOptions.Telemetry = &telemetry.Options{Integrations: []telemetry.TelemetryIntegration{&sdkCallbacksIntegration{}}}
		messages := []workflowai.ModelMessage{
			user("test"),
			{Role: workflowai.RoleAssistant, Content: []workflowai.ContentPart{
				{Type: workflowai.PartToolCall, ToolCallID: "call-1", ToolName: "testTool", Input: json.RawMessage(`{"value":"test"}`)},
				{Type: workflowai.PartToolApprovalRequest, ApprovalID: "approval-call-1", ToolCallID: "call-1"},
			}},
			{Role: workflowai.RoleTool, Content: []workflowai.ContentPart{
				{Type: workflowai.PartToolApprovalResponse, ApprovalID: "approval-call-1", Approved: true},
			}},
		}
		if _, _, err := sdkCallbacksRun(t, messages...); err != nil {
			t.Fatal(err)
		}
		tools := slices.DeleteFunc(sdkCallbacksEvents(), func(event string) bool {
			return !strings.HasPrefix(event, "onToolExecution") && event != "executeTool" || event == "onToolExecutionEnd"
		})
		if want := []string{"onToolExecutionStart", "executeTool", "onToolExecutionEnd:success"}; !slices.Equal(tools, want) {
			t.Fatalf("events = %v, want %v", tools, want)
		}
	})
}

// SDKCallbacksChat runs the agent like Chat, and clears the recorded
// callback calls at the start of every execution, since replays call the
// callbacks again.
type SDKCallbacksChat struct {
	Messages []workflowai.ModelMessage
}

func (SDKCallbacksChat) Workflow(workflowai.Result) {}

func SDKCallbacksChatRunner(ctx context.Context, input SDKCallbacksChat) (workflowai.Result, error) {
	sdkCallbacksState.mu.Lock()
	sdkCallbacksState.calls = nil
	sdkCallbacksState.mu.Unlock()
	return ChatRunner(ctx, Chat(input))
}

// sdkCallbacksState records callback calls, cleared for every execution of
// a run, and events, kept for the whole run.
var sdkCallbacksState struct {
	mu     sync.Mutex
	calls  []string
	events []string
}

func sdkCallbacksRecord(name string) {
	sdkCallbacksState.mu.Lock()
	defer sdkCallbacksState.mu.Unlock()
	sdkCallbacksState.calls = append(sdkCallbacksState.calls, name)
}

func sdkCallbacksCalls() []string {
	sdkCallbacksState.mu.Lock()
	defer sdkCallbacksState.mu.Unlock()
	return slices.Clone(sdkCallbacksState.calls)
}

func sdkCallbacksEvent(name string) {
	sdkCallbacksState.mu.Lock()
	defer sdkCallbacksState.mu.Unlock()
	sdkCallbacksState.events = append(sdkCallbacksState.events, name)
}

// sdkCallbacksEventOnce records an event unless it was recorded before.
func sdkCallbacksEventOnce(name string) {
	sdkCallbacksState.mu.Lock()
	defer sdkCallbacksState.mu.Unlock()
	if !slices.Contains(sdkCallbacksState.events, name) {
		sdkCallbacksState.events = append(sdkCallbacksState.events, name)
	}
}

func sdkCallbacksEvents() []string {
	sdkCallbacksState.mu.Lock()
	defer sdkCallbacksState.mu.Unlock()
	return slices.Clone(sdkCallbacksState.events)
}

// sdkCallbacksSetup scripts the model like setup, for an agent with no
// instructions or tools.
func sdkCallbacksSetup(responses ...func() ([]provider.StreamChunk, error)) {
	setup(responses...)
	testOptions = workflowai.WorkflowAgentOptions{}
	sdkCallbacksState.mu.Lock()
	sdkCallbacksState.calls, sdkCallbacksState.events = nil, nil
	sdkCallbacksState.mu.Unlock()
}

// sdkCallbacksRun runs one turn with SDKCallbacksChatRunner and returns its
// result and the UI chunks of its stream.
func sdkCallbacksRun(t *testing.T, messages ...workflowai.ModelMessage) (workflowai.Result, []ai.UIMessageChunk, error) {
	t.Helper()
	world := testworld.New(worlds.Runners(workflow.Use(SDKCallbacksChatRunner)))
	defer world.Close()
	ctx := workflow.NewContext(t.Context(), workflow.NewClient(world))
	run, err := workflow.Start(ctx, SDKCallbacksChat{Messages: messages})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := run.Reader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var chunks []ai.UIMessageChunk
	for chunk := range workflowai.UIMessageChunks(ctx, reader, 0) {
		chunks = append(chunks, chunk)
	}
	result, err := run.Value(ctx)
	return result, chunks, err
}

// sdkCallbacksCheckOrder checks that a callback set on the agent, on the
// call, or on both is called once per execution, the agent's first.
func sdkCallbacksCheckOrder(t *testing.T, set func(callbacks *workflowai.Callbacks, name string), responses ...func() ([]provider.StreamChunk, error)) {
	tests := []struct {
		name   string
		agent  bool
		method bool
		want   []string
	}{
		{"constructor", true, false, []string{"constructor"}},
		{"method", false, true, []string{"method"}},
		{"both", true, true, []string{"constructor", "method"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				sdkCallbacksSetup(responses...)
				testOptions.Tools = sdkCallbacksTools(sdkCallbacksTestTool())
				if tt.agent {
					set(&testOptions.Callbacks, "constructor")
				}
				if tt.method {
					set(&testCallOptions.Callbacks, "method")
				}
				if _, _, err := sdkCallbacksRun(t, user("Hello, world!")); err != nil {
					t.Fatal(err)
				}
				if got := sdkCallbacksCalls(); !slices.Equal(got, tt.want) {
					t.Fatalf("calls = %v, want %v", got, tt.want)
				}
			})
		})
	}
}

type sdkCallbacksInput struct {
	Value string `json:"value"`
}

func sdkCallbacksEcho(_ context.Context, input sdkCallbacksInput) (string, error) {
	return input.Value + "-result", nil
}

func sdkCallbacksFailingTool(context.Context, sdkCallbacksInput) (string, error) {
	return "", errors.New("tool failed")
}

func sdkCallbacksClientTool(context.Context, struct{}) (string, error) {
	return "client result", nil
}

func sdkCallbacksTestTool() workflowai.Tool {
	return workflowai.Tool{Execute: workflowai.ExecuteStep(sdkCallbacksEcho)}
}

func sdkCallbacksTools(tool workflowai.Tool) workflowai.ToolSet {
	return workflowai.ToolSet{"testTool": tool}
}

// sdkCallbacksHasToolError stops after a step with a tool-error part.
func sdkCallbacksHasToolError(steps []workflowai.StepResult) bool {
	return slices.ContainsFunc(steps[len(steps)-1].Content, func(part workflowai.ContentPart) bool { return part.Type == "tool-error" })
}

// sdkCallbacksIntegration records the telemetry events it gets, prefixed,
// and panics in OnStart, OnStepEnd and OnEnd when panics is set.
type sdkCallbacksIntegration struct {
	telemetry.NoopTelemetryIntegration
	prefix     string
	panics     bool
	mu         sync.Mutex
	starts     []telemetry.TelemetryStartEvent
	toolStarts []telemetry.TelemetryToolCallStartEvent
	ends       []telemetry.TelemetryFinishEvent
}

func (i *sdkCallbacksIntegration) OnStart(ctx context.Context, event telemetry.TelemetryStartEvent) context.Context {
	i.mu.Lock()
	i.starts = append(i.starts, event)
	i.mu.Unlock()
	i.record("onStart")
	return ctx
}

func (i *sdkCallbacksIntegration) OnStepStart(ctx context.Context, _ telemetry.TelemetryStepStartEvent) context.Context {
	sdkCallbacksEvent(i.prefix + "onStepStart")
	return ctx
}

func (i *sdkCallbacksIntegration) OnToolExecutionStart(ctx context.Context, event telemetry.TelemetryToolCallStartEvent) context.Context {
	i.mu.Lock()
	i.toolStarts = append(i.toolStarts, event)
	i.mu.Unlock()
	sdkCallbacksEvent(i.prefix + "onToolExecutionStart")
	return ctx
}

func (i *sdkCallbacksIntegration) OnToolExecutionEnd(_ context.Context, event telemetry.TelemetryToolCallFinishEvent) {
	outcome := "success"
	if event.Error != nil {
		outcome = "error"
	}
	sdkCallbacksEvent(i.prefix + "onToolExecutionEnd")
	sdkCallbacksEvent(i.prefix + "onToolExecutionEnd:" + outcome)
}

func (i *sdkCallbacksIntegration) OnStepEnd(context.Context, telemetry.TelemetryStepEndEvent) {
	i.record("onStepEnd")
}

func (i *sdkCallbacksIntegration) OnEnd(_ context.Context, event telemetry.TelemetryFinishEvent) {
	i.mu.Lock()
	i.ends = append(i.ends, event)
	i.mu.Unlock()
	i.record("onEnd")
}

func (i *sdkCallbacksIntegration) ExecuteTool(ctx context.Context, _ string, args map[string]any, execute func(context.Context, map[string]any) (any, error)) (any, error) {
	sdkCallbacksEvent(i.prefix + "executeTool")
	return execute(ctx, args)
}

func (i *sdkCallbacksIntegration) record(name string) {
	if i.panics {
		panic("integration error")
	}
	sdkCallbacksEvent(i.prefix + name)
}

func sdkCallbacksUsage() *types.Usage {
	input, output, total := int64(3), int64(10), int64(13)
	return &types.Usage{InputTokens: &input, OutputTokens: &output, TotalTokens: &total}
}

func sdkCallbacksTokens(usage types.Usage) [2]int64 {
	var tokens [2]int64
	if usage.InputTokens != nil {
		tokens[0] = *usage.InputTokens
	}
	if usage.OutputTokens != nil {
		tokens[1] = *usage.OutputTokens
	}
	return tokens
}

func sdkCallbacksChunks(chunks ...provider.StreamChunk) func() ([]provider.StreamChunk, error) {
	return func() ([]provider.StreamChunk, error) { return slices.Clone(chunks), nil }
}

func sdkCallbacksFinish(reason types.FinishReason) provider.StreamChunk {
	return provider.StreamChunk{Type: provider.ChunkTypeFinish, FinishReason: reason, Usage: sdkCallbacksUsage()}
}

func sdkCallbacksMetadata(id string) provider.StreamChunk {
	return provider.StreamChunk{Type: provider.ChunkTypeResponseMetadata, ResponseMetadata: &provider.ResponseMetadata{ID: id, ModelID: "mock-model-id"}}
}

// sdkCallbacksSimple streams "Hello, world!" in three deltas, with usage and
// provider metadata.
func sdkCallbacksSimple() func() ([]provider.StreamChunk, error) {
	finish := sdkCallbacksFinish(types.FinishReasonStop)
	finish.ProviderMetadata = json.RawMessage(`{"testProvider":{"testKey":"testValue"}}`)
	return sdkCallbacksChunks(
		sdkCallbacksMetadata("id-0"),
		provider.StreamChunk{Type: provider.ChunkTypeTextStart, ID: "1"},
		provider.StreamChunk{Type: provider.ChunkTypeText, ID: "1", Text: "Hello"},
		provider.StreamChunk{Type: provider.ChunkTypeText, ID: "1", Text: ", "},
		provider.StreamChunk{Type: provider.ChunkTypeText, ID: "1", Text: "world!"},
		provider.StreamChunk{Type: provider.ChunkTypeTextEnd, ID: "1"},
		finish,
	)
}

// sdkCallbacksShort streams "Hello".
func sdkCallbacksShort() []func() ([]provider.StreamChunk, error) {
	return []func() ([]provider.StreamChunk, error){sdkCallbacksText("Hello")}
}

func sdkCallbacksText(text string) func() ([]provider.StreamChunk, error) {
	return sdkCallbacksChunks(
		sdkCallbacksMetadata("id-1"),
		provider.StreamChunk{Type: provider.ChunkTypeTextStart, ID: "1"},
		provider.StreamChunk{Type: provider.ChunkTypeText, ID: "1", Text: text},
		provider.StreamChunk{Type: provider.ChunkTypeTextEnd, ID: "1"},
		sdkCallbacksFinish(types.FinishReasonStop),
	)
}

// sdkCallbacksToolCall calls testTool with input as call-1.
func sdkCallbacksToolCall(input string) func() ([]provider.StreamChunk, error) {
	return sdkCallbacksChunks(
		sdkCallbacksMetadata("id-0"),
		provider.StreamChunk{Type: provider.ChunkTypeToolCall, ToolCall: &types.ToolCall{ID: "call-1", ToolName: "testTool", RawArguments: input}},
		sdkCallbacksFinish(types.FinishReasonToolCalls),
	)
}

// sdkCallbacksToolLoop calls testTool and then answers "done".
func sdkCallbacksToolLoop() []func() ([]provider.StreamChunk, error) {
	return []func() ([]provider.StreamChunk, error){sdkCallbacksToolCall(`{ "value": "test" }`), sdkCallbacksText("done")}
}

func sdkCallbacksProviderCall(id, name, input string) provider.StreamChunk {
	return provider.StreamChunk{Type: provider.ChunkTypeToolCall, ToolCall: &types.ToolCall{ID: id, ToolName: name, RawArguments: input, ProviderExecuted: true}}
}

func sdkCallbacksProviderResult(id, name string, result any, err error) provider.StreamChunk {
	return provider.StreamChunk{Type: provider.ChunkTypeToolResult, ToolResult: &types.ToolResult{
		ToolCallID: id, ToolName: name, Result: result, Error: err, ProviderExecuted: true,
	}}
}

// sdkCallbacksParts describes the parts of a message as "type:text" for
// text and reasoning, and "type:toolCallId" for the others.
func sdkCallbacksParts(message workflowai.ModelMessage) []string {
	return sdkCallbacksContent(message.Content)
}

func sdkCallbacksContent(parts []workflowai.ContentPart) []string {
	var out []string
	for _, part := range parts {
		switch part.Type {
		case workflowai.PartText, workflowai.PartReasoning:
			out = append(out, part.Type+":"+part.Text)
		default:
			out = append(out, part.Type+":"+part.ToolCallID)
		}
	}
	return out
}

// sdkCallbacksPromptParts describes the parts of a prompt message like
// sdkCallbacksParts.
func sdkCallbacksPromptParts(message types.Message) []string {
	var out []string
	for _, part := range message.Content {
		switch part := part.(type) {
		case types.TextContent:
			out = append(out, "text:"+part.Text)
		case types.ReasoningContent:
			out = append(out, "reasoning:"+part.Text)
		case types.ToolCallContent:
			out = append(out, "tool-call:"+part.ToolCallID)
		case types.ToolResultContent:
			out = append(out, "tool-result:"+part.ToolCallID)
		default:
			out = append(out, fmt.Sprintf("%T", part))
		}
	}
	return out
}

// sdkCallbacksResults returns tool results with their input and output
// decoded.
func sdkCallbacksResults(results []workflowai.ToolResult) []map[string]any {
	var out []map[string]any
	for _, result := range results {
		out = append(out, map[string]any{
			"toolCallId": result.ToolCallID,
			"toolName":   result.ToolName,
			"input":      sdkCallbacksDecode(result.Input),
			"output":     sdkCallbacksDecode(result.Output),
		})
	}
	return out
}

func sdkCallbacksDecode(raw json.RawMessage) any {
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil
	}
	return value
}

func sdkCallbacksJSON(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		return err.Error()
	}
	return string(data)
}
