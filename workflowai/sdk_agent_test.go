package workflowai_test

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"reflect"
	"slices"
	"strconv"
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
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > id property > should expose id when provided in constructor
// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > id property > should be undefined when not provided
func TestSDKAgentID(t *testing.T) {
	if id := workflowai.NewWorkflowAgent(workflowai.WorkflowAgentOptions{Model: &fakeModel{}, ID: "my-agent"}).ID(); id != "my-agent" {
		t.Fatalf("id = %q", id)
	}
	if id := workflowai.NewWorkflowAgent(workflowai.WorkflowAgentOptions{Model: &fakeModel{}}).ID(); id != "" {
		t.Fatalf("id = %q", id)
	}
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > user-agent > should tag the user-agent with the agent identifier
// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > user-agent > should preserve a caller-provided user-agent
func TestSDKAgentUserAgent(t *testing.T) {
	tests := []struct {
		name    string
		headers map[string]string
		want    []string
	}{
		{"default", nil, []string{"ai-sdk-agent/workflow"}},
		{"caller user-agent", map[string]string{"user-agent": "my-app/1.0"}, []string{"my-app/1.0", "ai-sdk-agent/workflow"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				sdkAgentSetup(text("Hi!"))
				testOptions.Headers = tt.headers
				if _, _, _, err := sdkAgentStream(t, []workflowai.ModelMessage{user("test")}); err != nil {
					t.Fatal(err)
				}
				header := testModel.calls[0].Headers["user-agent"]
				for _, want := range tt.want {
					if !strings.Contains(header, want) {
						t.Fatalf("user-agent = %q, want it to contain %q", header, want)
					}
				}
			})
		})
	}
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > prepareCall > applies maxRetries to generation settings
func TestSDKAgentPrepareCallMaxRetries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkAgentSetup(failWith(retryableError{}), failWith(retryableError{}), failWith(retryableError{}), text("Hi!"))
		testOptions.MaxRetries = sdkAgentPtr(1)
		testOptions.PrepareCall = func(context.Context, workflowai.PrepareCallOptions) (workflowai.PrepareCallResult, error) {
			return workflowai.PrepareCallResult{GenerationSettings: workflowai.GenerationSettings{MaxRetries: sdkAgentPtr(5)}}, nil
		}
		result, _, err := sdkAgentSteps(t, []workflowai.ModelMessage{user("test")})
		if err != nil {
			t.Fatal(err)
		}
		// The prepared limit of 5 retries lets the fourth attempt succeed.
		if result.Text != "Hi!" || len(testModel.calls) != 4 {
			t.Fatalf("text = %q after %d calls", result.Text, len(testModel.calls))
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > prepareCall > applies stopWhen, activeTools, and experimental_download
func TestSDKAgentPrepareCallSettings(t *testing.T) {
	t.Run("stopWhen and activeTools", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			sdkAgentSetup(toolCalls(call("c1", "second", `{}`)), text("unused"))
			testOptions.Tools = workflowai.ToolSet{"first": sdkAgentTool("first", nil), "second": sdkAgentTool("second", nil)}
			var mu sync.Mutex
			var streamStops, preparedStops int
			streamStop := func([]workflowai.StepResult) bool {
				mu.Lock()
				defer mu.Unlock()
				streamStops++
				return false
			}
			preparedStop := func([]workflowai.StepResult) bool {
				mu.Lock()
				defer mu.Unlock()
				preparedStops++
				return true
			}
			testCallOptions.StopWhen = []workflowai.StopCondition{streamStop}
			testCallOptions.ActiveTools = []string{"first"}
			var got workflowai.PrepareCallOptions
			testOptions.PrepareCall = func(_ context.Context, options workflowai.PrepareCallOptions) (workflowai.PrepareCallResult, error) {
				got = options
				return workflowai.PrepareCallResult{StopWhen: []workflowai.StopCondition{preparedStop}, ActiveTools: []string{"second"}}, nil
			}
			if _, _, err := sdkAgentSteps(t, []workflowai.ModelMessage{user("test")}); err != nil {
				t.Fatal(err)
			}
			if len(got.StopWhen) != 1 || !slices.Equal(got.ActiveTools, []string{"first"}) {
				t.Fatalf("prepare call options = %+v", got)
			}
			if names := sdkAgentToolNames(testModel.calls[0]); !slices.Equal(names, []string{"second"}) {
				t.Fatalf("tools = %v", names)
			}
			// The prepared stop condition ends the loop after the first step.
			mu.Lock()
			defer mu.Unlock()
			if len(testModel.calls) != 1 || preparedStops == 0 || streamStops != 0 {
				t.Fatalf("%d model calls, prepared stops = %d, stream stops = %d", len(testModel.calls), preparedStops, streamStops)
			}
		})
	})
	t.Run("download", func(t *testing.T) {
		// PrepareCall would receive the call's Download and replace it with
		// its own, which the model call step then uses.
	})
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > prepareCall > passes URL-backed image and file parts to model steps without downloading them
func TestSDKAgentURLFileParts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkAgentSetup(text("Nice picture."))
		var mu sync.Mutex
		var requested []string
		testOptions.Download = func(_ context.Context, requests []ai.DownloadRequest) ([]*ai.DownloadResult, error) {
			mu.Lock()
			defer mu.Unlock()
			for _, request := range requests {
				requested = append(requested, request.URL)
			}
			return make([]*ai.DownloadResult, len(requests)), nil
		}
		messages := []workflowai.ModelMessage{{Role: workflowai.RoleUser, Content: []workflowai.ContentPart{
			{Type: workflowai.PartFile, Data: "https://example.com/image.png", MediaType: "image/png"},
			{Type: workflowai.PartFile, Data: "https://example.com/file.pdf", MediaType: "application/pdf"},
		}}}
		if _, _, err := sdkAgentSteps(t, messages); err != nil {
			t.Fatal(err)
		}
		// The workflow passes the URLs to the model call step, whose Download
		// leaves them as they are.
		prompt := testModel.prompts()[0]
		if len(prompt) != 1 || len(prompt[0].Content) != 2 {
			t.Fatalf("prompt = %+v", prompt)
		}
		want := []struct{ url, mediaType string }{{"https://example.com/image.png", "image/png"}, {"https://example.com/file.pdf", "application/pdf"}}
		for i, part := range prompt[0].Content {
			file, ok := part.(types.FileContent)
			if !ok || file.FileData.Type != types.FileDataTypeURL || file.FileData.URL != want[i].url || file.MediaType != want[i].mediaType {
				t.Fatalf("part %d = %+v", i, part)
			}
		}
		mu.Lock()
		defer mu.Unlock()
		if !slices.Equal(requested, []string{"https://example.com/image.png", "https://example.com/file.pdf"}) {
			t.Fatalf("download requests = %v", requested)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > tool execution error handling > should convert FatalError to tool error result
func TestSDKAgentToolFatalError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkAgentSetup(toolCalls(call("test-call-id", "testTool", `{}`)), text("Sorry."))
		testOptions.Tools = workflowai.ToolSet{"testTool": {
			Description: "A test tool",
			Execute: workflowai.ExecuteFunc(func(context.Context, json.RawMessage, workflowai.ToolCallOptions) (any, error) {
				return nil, workflow.FatalError(errors.New("This is a fatal error"))
			}),
		}}
		if _, _, err := sdkAgentSteps(t, []workflowai.ModelMessage{user("test")}); err != nil {
			t.Fatal(err)
		}
		results := sdkAgentToolResults(testModel.prompts()[1], types.RoleTool)
		if len(results) != 1 || results[0].ToolCallID != "test-call-id" || results[0].ToolName != "testTool" {
			t.Fatalf("tool results = %+v", results)
		}
		if output := results[0].Output; output.Type != types.ToolResultOutputErrorText || !strings.Contains(output.Value.(string), "This is a fatal error") {
			t.Fatalf("output = %+v", output)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > tool execution error handling > should convert non-FatalError to tool error result
// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > callbacks > should convert tool execution error to error-text result instead of failing stream
func TestSDKAgentToolError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkAgentSetup(toolCalls(call("test-call-id", "testTool", `{}`)), text("Sorry."))
		testOptions.Tools = workflowai.ToolSet{"testTool": {
			Description: "A test tool",
			Execute: workflowai.ExecuteFunc(func(context.Context, json.RawMessage, workflowai.ToolCallOptions) (any, error) {
				return nil, errors.New("This is a generic error")
			}),
		}}
		_, parts, _, err := sdkAgentStream(t, []workflowai.ModelMessage{user("test")})
		if err != nil {
			t.Fatal(err)
		}
		results := sdkAgentToolResults(testModel.prompts()[1], types.RoleTool)
		if len(results) != 1 || results[0].ToolCallID != "test-call-id" || results[0].ToolName != "testTool" {
			t.Fatalf("tool results = %+v", results)
		}
		if output := results[0].Output; output.Type != types.ToolResultOutputErrorText || !strings.Contains(output.Value.(string), "This is a generic error") {
			t.Fatalf("output = %+v", output)
		}
		// The stream reports the failure with a tool-error part and no
		// tool-result part.
		var toolErrors int
		for _, part := range parts {
			switch {
			case part.Type == "tool-result" && part.ToolCallID == "test-call-id":
				t.Fatalf("tool-result part = %+v", part)
			case part.Type == "tool-error" && part.ToolCallID == "test-call-id":
				if part.ToolName != "testTool" || string(part.Input) != `{}` || !strings.Contains(part.Error, "This is a generic error") {
					t.Fatalf("tool-error part = %+v", part)
				}
				toolErrors++
			}
		}
		if toolErrors != 1 {
			t.Fatalf("parts = %+v", parts)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > tool execution error handling > should successfully execute tools that return normally
func TestSDKAgentToolResult(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkAgentSetup(toolCalls(call("test-call-id", "testTool", `{}`)), text("Done."))
		testOptions.Tools = workflowai.ToolSet{"testTool": sdkAgentTool("testTool", map[string]any{"success": true, "data": "test result"})}
		if _, _, err := sdkAgentSteps(t, []workflowai.ModelMessage{user("test")}); err != nil {
			t.Fatal(err)
		}
		results := sdkAgentToolResults(testModel.prompts()[1], types.RoleTool)
		if len(results) != 1 || results[0].ToolCallID != "test-call-id" || results[0].ToolName != "testTool" {
			t.Fatalf("tool results = %+v", results)
		}
		// Object results reach the model as JSON values.
		output := results[0].Output
		if value, ok := output.Value.(map[string]any); output.Type != types.ToolResultOutputJSON || !ok || value["success"] != true || value["data"] != "test result" {
			t.Fatalf("output = %+v", output)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > tool execution error handling > should use toModelOutput for local tool results while preserving raw output
func TestSDKAgentToolModelOutput(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkAgentSetup(toolCalls(call("test-call-id", "testTool", `{"query":"test query"}`)), text("Done."))
		tool := sdkAgentTool("testTool", map[string]any{"public": "visible to user", "secret": "hide me"})
		tool.ToModelOutput = sdkAgentModelSees
		testOptions.Tools = workflowai.ToolSet{"testTool": tool}
		result, parts, _, err := sdkAgentStream(t, []workflowai.ModelMessage{user("test")})
		if err != nil {
			t.Fatal(err)
		}
		results := sdkAgentToolResults(testModel.prompts()[1], types.RoleTool)
		if len(results) != 1 || results[0].Output.Type != types.ToolResultOutputText || results[0].Output.Value != "model sees: visible to user" {
			t.Fatalf("tool results = %+v", results)
		}
		raw := `{"public":"visible to user","secret":"hide me"}`
		if got := result.Steps[0].ToolResults; len(got) != 1 || string(got[0].Input) != `{"query":"test query"}` || string(got[0].Output) != raw {
			t.Fatalf("step tool results = %+v", got)
		}
		if !slices.ContainsFunc(parts, func(part workflowai.ModelCallStreamPart) bool {
			return part.Type == "tool-result" && part.ToolCallID == "test-call-id" && string(part.Output) == raw
		}) {
			t.Fatalf("parts = %+v", parts)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > tool execution error handling > should skip local execution for provider-executed tools
func TestSDKAgentProviderToolSkipsLocalExecution(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		search := types.ToolCall{ID: "provider-call-id", ToolName: "WebSearch", RawArguments: `{"query":"test query"}`, ProviderExecuted: true}
		sdkAgentSetup(func() ([]provider.StreamChunk, error) {
			return []provider.StreamChunk{
				{Type: provider.ChunkTypeToolCall, ToolCall: &search},
				sdkAgentProviderResult("provider-call-id", "WebSearch", "Search results for: test query", nil),
				{Type: provider.ChunkTypeFinish, FinishReason: types.FinishReasonToolCalls},
			}, nil
		}, text("Done."))
		testOptions.Tools = workflowai.ToolSet{"localTool": sdkAgentTool("localTool", nil)}
		result, _, err := sdkAgentSteps(t, []workflowai.ModelMessage{user("test")})
		if err != nil {
			t.Fatal(err)
		}
		if runs := sdkAgentRuns("localTool"); runs != 0 {
			t.Fatalf("local tool ran %d times", runs)
		}
		// The provider's result is the call's result.
		got := result.Steps[0].ToolResults
		if len(got) != 1 || got[0].ToolCallID != "provider-call-id" || got[0].ToolName != "WebSearch" || string(got[0].Output) != `"Search results for: test query"` {
			t.Fatalf("step tool results = %+v", got)
		}
		part := sdkAgentContentResult(result.Steps[0].Content, "provider-call-id")
		if part == nil || part.Output.Type != "text" || string(part.Output.Value) != `"Search results for: test query"` {
			t.Fatalf("content = %+v", result.Steps[0].Content)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > tool execution error handling > should defer missing results for provider tools that support them
func TestSDKAgentDeferredProviderResults(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		program := types.ToolCall{ID: "program-call", ToolName: "program", RawArguments: `{"code":"run()"}`, ProviderExecuted: true}
		sdkAgentSetup(func() ([]provider.StreamChunk, error) {
			return []provider.StreamChunk{
				{Type: provider.ChunkTypeToolCall, ToolCall: &program},
				{Type: provider.ChunkTypeFinish, FinishReason: types.FinishReasonToolCalls},
			}, nil
		}, func() ([]provider.StreamChunk, error) {
			return []provider.StreamChunk{
				sdkAgentProviderResult("program-call", "program", map[string]any{"status": "done"}, nil),
				{Type: provider.ChunkTypeFinish, FinishReason: types.FinishReasonStop},
			}, nil
		})
		testOptions.Tools = workflowai.ToolSet{"program": {Provider: &types.Tool{Name: "program", ProviderExecuted: true, SupportsDeferredResults: true}}}
		result, _, err := sdkAgentSteps(t, []workflowai.ModelMessage{user("Run the program.")})
		if err != nil {
			t.Fatal(err)
		}
		// The call gets no stand-in result, and the loop continues with no
		// results until the provider delivers it.
		if len(result.Steps) != 2 || len(result.Steps[0].ToolResults) != 0 || sdkAgentContentResult(result.Steps[0].Content, "program-call") != nil {
			t.Fatalf("steps = %+v", result.Steps)
		}
		if len(sdkAgentToolResults(testModel.prompts()[1], types.RoleTool)) != 0 {
			t.Fatalf("prompt = %+v", testModel.prompts()[1])
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > tool execution error handling > should use toModelOutput for provider-executed tool results while preserving raw output
func TestSDKAgentProviderToolModelOutput(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		search := types.ToolCall{ID: "provider-call-id", ToolName: "WebSearch", RawArguments: `{"query":"test query"}`, ProviderExecuted: true}
		sdkAgentSetup(func() ([]provider.StreamChunk, error) {
			return []provider.StreamChunk{
				{Type: provider.ChunkTypeToolCall, ToolCall: &search},
				sdkAgentProviderResult("provider-call-id", "WebSearch", map[string]any{"public": "provider result", "secret": "hide me"}, map[string]any{"openai": map[string]any{"itemId": "provider-result-item"}}),
				{Type: provider.ChunkTypeFinish, FinishReason: types.FinishReasonToolCalls},
			}, nil
		}, text("Done."))
		webSearch := sdkAgentTool("WebSearch", nil)
		webSearch.ToModelOutput = sdkAgentModelSees
		testOptions.Tools = workflowai.ToolSet{"WebSearch": webSearch}
		result, _, err := sdkAgentSteps(t, []workflowai.ModelMessage{user("test")})
		if err != nil {
			t.Fatal(err)
		}
		if runs := sdkAgentRuns("WebSearch"); runs != 0 {
			t.Fatalf("WebSearch ran %d times", runs)
		}
		// The step's content holds the result the model sees in later calls.
		sent := sdkAgentContentResult(result.Steps[0].Content, "provider-call-id")
		if sent == nil || sent.Output.Type != "text" || string(sent.Output.Value) != `"model sees: provider result"` || sent.ProviderOptions["openai"] == nil {
			t.Fatalf("provider result = %+v", sent)
		}
		got := result.Steps[0].ToolResults
		if len(got) != 1 || string(got[0].Input) != `{"query":"test query"}` || string(got[0].Output) != `{"public":"provider result","secret":"hide me"}` {
			t.Fatalf("step tool results = %+v", got)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > tool execution error handling > should handle mixed provider-executed and local tools
func TestSDKAgentMixedProviderAndLocalTools(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		local := call("local-call-id", "localTool", `{}`)
		search := types.ToolCall{ID: "provider-call-id", ToolName: "WebSearch", RawArguments: `{"query":"test"}`, ProviderExecuted: true}
		sdkAgentSetup(func() ([]provider.StreamChunk, error) {
			return []provider.StreamChunk{
				{Type: provider.ChunkTypeToolCall, ToolCall: &local},
				{Type: provider.ChunkTypeToolCall, ToolCall: &search},
				sdkAgentProviderResult("provider-call-id", "WebSearch", map[string]any{"searchResults": []any{"result1", "result2"}}, nil),
				{Type: provider.ChunkTypeFinish, FinishReason: types.FinishReasonToolCalls},
			}, nil
		}, text("Done."))
		testOptions.Tools = workflowai.ToolSet{"localTool": sdkAgentTool("localTool", map[string]any{"local": "result"})}
		result, _, err := sdkAgentSteps(t, []workflowai.ModelMessage{user("test")})
		if err != nil {
			t.Fatal(err)
		}
		if runs := sdkAgentRuns("localTool"); runs != 1 {
			t.Fatalf("local tool ran %d times", runs)
		}
		var ids []string
		for _, result := range result.Steps[0].ToolResults {
			ids = append(ids, result.ToolCallID)
		}
		if !slices.Equal(ids, []string{"local-call-id", "provider-call-id"}) {
			t.Fatalf("step tool results = %v", ids)
		}
		// The next model call gets both results as JSON values.
		prompt := testModel.prompts()[1]
		local1 := sdkAgentToolResults(prompt, types.RoleTool)
		if len(local1) != 1 || local1[0].ToolCallID != "local-call-id" || local1[0].Output.Type != types.ToolResultOutputJSON || local1[0].Output.Value.(map[string]any)["local"] != "result" {
			t.Fatalf("local result = %+v", local1)
		}
		provider1 := sdkAgentToolResults(prompt, types.RoleAssistant)
		if len(provider1) != 1 || provider1[0].ToolCallID != "provider-call-id" || provider1[0].ToolName != "WebSearch" || provider1[0].Output.Type != types.ToolResultOutputJSON {
			t.Fatalf("provider result = %+v", provider1)
		}
		if results, _ := provider1[0].Output.Value.(map[string]any)["searchResults"].([]any); len(results) != 2 {
			t.Fatalf("provider output = %+v", provider1[0].Output)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > tool execution error handling > should handle provider-executed tool errors with isError flag
func TestSDKAgentProviderToolError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		search := types.ToolCall{ID: "provider-call-id", ToolName: "WebSearch", RawArguments: `{"query":"test query"}`, ProviderExecuted: true}
		sdkAgentSetup(func() ([]provider.StreamChunk, error) {
			result := sdkAgentProviderResult("provider-call-id", "WebSearch", "Search failed: Rate limit exceeded", nil)
			result.ToolResult.Error = errors.New("Search failed: Rate limit exceeded")
			return []provider.StreamChunk{
				{Type: provider.ChunkTypeToolCall, ToolCall: &search},
				result,
				{Type: provider.ChunkTypeFinish, FinishReason: types.FinishReasonToolCalls},
			}, nil
		}, text("Done."))
		testOptions.Tools = workflowai.ToolSet{}
		result, parts, _, err := sdkAgentStream(t, []workflowai.ModelMessage{user("test")})
		if err != nil {
			t.Fatal(err)
		}
		// The assistant message keeps the error as an error-json result for
		// the provider.
		part := sdkAgentContentResult(result.Steps[0].Response.Messages[0].Content, "provider-call-id")
		if part == nil || part.ToolName != "WebSearch" || part.Output.Type != "error-json" || string(part.Output.Value) != `"Search failed: Rate limit exceeded"` {
			t.Fatalf("assistant message = %+v", result.Steps[0].Response.Messages[0].Content)
		}
		if !slices.ContainsFunc(parts, func(part workflowai.ModelCallStreamPart) bool {
			return part.Type == "tool-error" && part.ToolCallID == "provider-call-id" && part.ToolName == "WebSearch" &&
				string(part.Input) == `{"query":"test query"}` && part.Error == "Search failed: Rate limit exceeded"
		}) {
			t.Fatalf("parts = %+v", parts)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > tool execution error handling > should warn and return empty result when provider-executed tool result is missing
func TestSDKAgentMissingProviderResult(t *testing.T) {
	var logs bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&logs)
	defer log.SetOutput(previous)
	synctest.Test(t, func(t *testing.T) {
		search := types.ToolCall{ID: "missing-result-id", ToolName: "WebSearch", RawArguments: `{"query":"test query"}`, ProviderExecuted: true}
		sdkAgentSetup(func() ([]provider.StreamChunk, error) {
			return []provider.StreamChunk{
				{Type: provider.ChunkTypeToolCall, ToolCall: &search},
				{Type: provider.ChunkTypeFinish, FinishReason: types.FinishReasonToolCalls},
			}, nil
		}, text("Done."))
		testOptions.Tools = workflowai.ToolSet{}
		result, _, err := sdkAgentSteps(t, []workflowai.ModelMessage{user("test")})
		if err != nil {
			t.Fatal(err)
		}
		if output := logs.String(); !strings.Contains(output, `Provider-executed tool "WebSearch"`) || !strings.Contains(output, "missing-result-id") {
			t.Fatalf("logs = %q", output)
		}
		part := sdkAgentContentResult(result.Steps[0].Content, "missing-result-id")
		if part == nil || part.ToolName != "WebSearch" || part.Output.Type != "text" || string(part.Output.Value) != `""` {
			t.Fatalf("content = %+v", result.Steps[0].Content)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > tool execution error handling > should keep invalid tool calls on the error path without executing them
func TestSDKAgentInvalidToolCall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkAgentSetup(toolCalls(call("invalid-call-id", "testTool", `{"cities":"San Francisco"}`)), text("done"))
		tool := sdkAgentTool("testTool", nil)
		tool.InputSchema = sdkAgentObject("city")
		testOptions.Tools = workflowai.ToolSet{"testTool": tool}
		result, parts, _, err := sdkAgentStream(t, []workflowai.ModelMessage{user("test")})
		if err != nil {
			t.Fatal(err)
		}
		if runs := sdkAgentRuns("testTool"); runs != 0 {
			t.Fatalf("tool ran %d times", runs)
		}
		results := sdkAgentToolResults(testModel.prompts()[1], types.RoleTool)
		if len(results) != 1 || results[0].ToolCallID != "invalid-call-id" || results[0].ToolName != "testTool" ||
			results[0].Output.Type != types.ToolResultOutputErrorText || !strings.Contains(results[0].Output.Value.(string), "Invalid input for tool testTool") {
			t.Fatalf("tool results = %+v", results)
		}
		// The model call step follows the invalid call with its error, and the
		// agent writes no result for it.
		errors := 0
		for _, part := range parts {
			if part.ToolCallID != "invalid-call-id" {
				continue
			}
			switch part.Type {
			case "tool-error":
				errors++
			case "tool-result":
				t.Fatalf("part = %+v", part)
			}
		}
		if errors != 1 {
			t.Fatalf("%d tool-error parts", errors)
		}
		step := result.Steps[0]
		if len(step.ToolCalls) != 1 || step.ToolCalls[0].ToolCallID != "invalid-call-id" || string(step.ToolCalls[0].Input) != `{"cities":"San Francisco"}` || len(step.ToolResults) != 0 {
			t.Fatalf("step = %+v", step)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > client-side tools (tools without execute) > should stop the loop and return unresolved toolCalls for tools without execute
func TestSDKAgentClientTool(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkAgentSetup(toolCalls(call("ask-user-call-id", "askUser", `{"question":"What is your name?"}`)), text("unused"))
		testOptions.Tools = workflowai.ToolSet{"askUser": {Description: "Ask the user a question", InputSchema: sdkAgentObject("question")}}
		result, _, err := sdkAgentSteps(t, []workflowai.ModelMessage{user("test")})
		if err != nil {
			t.Fatal(err)
		}
		if len(testModel.calls) != 1 {
			t.Fatalf("%d model calls", len(testModel.calls))
		}
		if len(result.ToolCalls) != 1 || result.ToolCalls[0].ToolCallID != "ask-user-call-id" || result.ToolCalls[0].ToolName != "askUser" ||
			string(result.ToolCalls[0].Input) != `{"question":"What is your name?"}` || len(result.ToolResults) != 0 {
			t.Fatalf("result = %+v", result)
		}
		// The messages end with the call, so the client can resume.
		if len(result.Messages) != 2 || result.Messages[1].Role != workflowai.RoleAssistant || result.Messages[1].Content[0].Type != workflowai.PartToolCall {
			t.Fatalf("messages = %+v", result.Messages)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > client-side tools (tools without execute) > should handle mixed executable and client-side tools in the same step
func TestSDKAgentMixedClientTools(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkAgentSetup(toolCalls(call("server-call-id", "serverTool", `{}`), call("client-call-id", "clientTool", `{"prompt":"confirm action"}`)), text("unused"))
		testOptions.Tools = workflowai.ToolSet{
			"serverTool": sdkAgentTool("serverTool", map[string]any{"data": "from-server"}),
			"clientTool": {Description: "A client-side tool", InputSchema: sdkAgentObject("prompt")},
		}
		result, parts, _, err := sdkAgentStream(t, []workflowai.ModelMessage{user("test")})
		if err != nil {
			t.Fatal(err)
		}
		if runs := sdkAgentRuns("serverTool"); runs != 1 || len(testModel.calls) != 1 {
			t.Fatalf("server tool ran %d times, %d model calls", runs, len(testModel.calls))
		}
		if len(result.ToolCalls) != 2 || result.ToolCalls[0].ToolCallID != "server-call-id" || result.ToolCalls[1].ToolCallID != "client-call-id" ||
			string(result.ToolCalls[1].Input) != `{"prompt":"confirm action"}` {
			t.Fatalf("tool calls = %+v", result.ToolCalls)
		}
		if len(result.ToolResults) != 1 || result.ToolResults[0].ToolCallID != "server-call-id" || string(result.ToolResults[0].Output) != `{"data":"from-server"}` {
			t.Fatalf("tool results = %+v", result.ToolResults)
		}
		if !slices.ContainsFunc(parts, func(part workflowai.ModelCallStreamPart) bool {
			return part.Type == "tool-result" && part.ToolCallID == "server-call-id" && part.ToolName == "serverTool" && string(part.Input) == `{}` && string(part.Output) == `{"data":"from-server"}`
		}) {
			t.Fatalf("parts = %+v", parts)
		}
		sdkAgentNoStartStep(t, parts)
		last := result.Messages[len(result.Messages)-1]
		if last.Role != workflowai.RoleTool || len(last.Content) != 1 || last.Content[0].ToolCallID != "server-call-id" {
			t.Fatalf("messages = %+v", result.Messages)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > client-side tools (tools without execute) > should stream executable tool results when a sibling needs approval
func TestSDKAgentApprovalSiblingResults(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkAgentSetup(toolCalls(call("server-call-id", "serverTool", `{}`), call("approval-call-id", "approvalTool", `{}`)), text("unused"))
		approval := sdkAgentTool("approvalTool", nil)
		approval.NeedsApproval = workflowai.NeedsApprovalAlways
		testOptions.Tools = workflowai.ToolSet{"serverTool": sdkAgentTool("serverTool", map[string]any{"data": "from-server"}), "approvalTool": approval}
		result, parts, _, err := sdkAgentStream(t, []workflowai.ModelMessage{user("test")})
		if err != nil {
			t.Fatal(err)
		}
		if sdkAgentRuns("serverTool") != 1 || sdkAgentRuns("approvalTool") != 0 {
			t.Fatalf("server tool ran %d times, approval tool %d times", sdkAgentRuns("serverTool"), sdkAgentRuns("approvalTool"))
		}
		if !slices.ContainsFunc(result.ToolResults, func(result workflowai.ToolResult) bool {
			return result.ToolCallID == "server-call-id" && result.ToolName == "serverTool" && string(result.Input) == `{}` && string(result.Output) == `{"data":"from-server"}`
		}) {
			t.Fatalf("tool results = %+v", result.ToolResults)
		}
		if !slices.ContainsFunc(parts, func(part workflowai.ModelCallStreamPart) bool {
			return part.Type == "tool-result" && part.ToolCallID == "server-call-id" && string(part.Output) == `{"data":"from-server"}`
		}) || !slices.ContainsFunc(parts, func(part workflowai.ModelCallStreamPart) bool {
			return part.Type == "tool-approval-request" && part.ApprovalID == "approval-approval-call-id" && part.ToolCallID == "approval-call-id" && part.Signature == ""
		}) {
			t.Fatalf("parts = %+v", parts)
		}
		sdkAgentNoStartStep(t, parts)
	})
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > client-side tools (tools without execute) > should sign approval requests without writing the secret to the durable stream
func TestSDKAgentSignedApprovalRequest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const secret = "workflow-tool-approval-secret"
		t.Setenv("WORKFLOW_TOOL_APPROVAL_SECRET", secret)
		sdkAgentSetup(toolCalls(call("approval-call-id", "approvalTool", `{"value":"test"}`)), text("unused"))
		approval := sdkAgentTool("approvalTool", nil)
		approval.NeedsApproval = workflowai.NeedsApprovalAlways
		testOptions.Tools = workflowai.ToolSet{"approvalTool": approval}
		testOptions.ToolApprovalSecret = &workflowai.ToolApprovalSecret{EnvironmentVariable: "WORKFLOW_TOOL_APPROVAL_SECRET"}
		_, parts, _, err := sdkAgentStream(t, []workflowai.ModelMessage{user("test")})
		if err != nil {
			t.Fatal(err)
		}
		index := slices.IndexFunc(parts, func(part workflowai.ModelCallStreamPart) bool { return part.Type == "tool-approval-request" })
		if index < 0 || parts[index].ApprovalID != "approval-approval-call-id" || parts[index].ToolCallID != "approval-call-id" || parts[index].Signature == "" {
			t.Fatalf("parts = %+v", parts)
		}
		data, _ := json.Marshal(parts)
		if strings.Contains(string(data), secret) {
			t.Fatalf("stream contains the secret: %s", data)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > client-side tools (tools without execute) > should call onFinish when stopping for client-side tools
func TestSDKAgentOnEndForClientTools(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkAgentSetup(toolCalls(call("ask-id", "askUser", `{"question":"confirm?"}`)), text("unused"))
		testOptions.Tools = workflowai.ToolSet{"askUser": {Description: "Ask the user a question", InputSchema: sdkAgentObject("question")}}
		var mu sync.Mutex
		var ended *workflowai.Result
		testCallOptions.OnEnd = func(_ context.Context, result workflowai.EndEvent) {
			mu.Lock()
			defer mu.Unlock()
			ended = &result.Result
		}
		if _, _, err := sdkAgentSteps(t, []workflowai.ModelMessage{user("test")}); err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		defer mu.Unlock()
		if ended == nil || len(ended.Steps) != 1 || len(ended.Messages) == 0 {
			t.Fatalf("OnEnd result = %+v", ended)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > client-side tools (tools without execute) > should have empty toolCalls when all tools complete normally
func TestSDKAgentNoToolCallsAfterCompletion(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkAgentSetup(toolCalls(call("server-call-id", "serverTool", `{}`)), text("Done."))
		testOptions.Tools = workflowai.ToolSet{"serverTool": sdkAgentTool("serverTool", nil)}
		result, _, err := sdkAgentSteps(t, []workflowai.ModelMessage{user("test")})
		if err != nil {
			t.Fatal(err)
		}
		// The result's calls and results are the last step's, which called no
		// tools.
		if len(result.ToolCalls) != 0 || len(result.ToolResults) != 0 || len(result.Steps) != 2 {
			t.Fatalf("result = %+v", result)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > prepareStep callback > should pass prepareStep callback to modelCallIterator
func TestSDKAgentPrepareStep(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkAgentSetup(text("Hi!"))
		testOptions.Tools = workflowai.ToolSet{}
		var mu sync.Mutex
		var calls []int
		testCallOptions.PrepareStep = func(_ context.Context, info workflowai.PrepareStepInfo) (workflowai.PrepareStepResult, error) {
			mu.Lock()
			defer mu.Unlock()
			calls = append(calls, info.StepNumber)
			return workflowai.PrepareStepResult{}, nil
		}
		if _, _, err := sdkAgentSteps(t, []workflowai.ModelMessage{user("test")}); err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		defer mu.Unlock()
		if len(calls) == 0 || slices.ContainsFunc(calls, func(step int) bool { return step != 0 }) {
			t.Fatalf("prepareStep calls = %v", calls)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > prepareStep callback > should pass stream instructions and initial messages to prepareStep
func TestSDKAgentPrepareStepInstructions(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkAgentSetup(text("Hi!"))
		testOptions.Tools = workflowai.ToolSet{}
		testOptions.Instructions = "constructor instructions"
		testCallOptions.Instructions = "stream instructions"
		var mu sync.Mutex
		var messages []workflowai.ModelMessage
		testCallOptions.PrepareStep = func(_ context.Context, info workflowai.PrepareStepInfo) (workflowai.PrepareStepResult, error) {
			mu.Lock()
			defer mu.Unlock()
			messages = info.Messages
			return workflowai.PrepareStepResult{}, nil
		}
		if _, _, err := sdkAgentSteps(t, []workflowai.ModelMessage{user("test")}); err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		defer mu.Unlock()
		if len(messages) != 1 || messages[0].Role != workflowai.RoleUser || messages[0].Content[0].Text != "test" {
			t.Fatalf("prepareStep messages = %+v", messages)
		}
		options := testModel.calls[0]
		if options.Prompt.System != "stream instructions" || len(options.Prompt.Messages) != 1 || options.Prompt.Messages[0].Role != types.RoleUser {
			t.Fatalf("prompt = %+v", options.Prompt)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > prepareStep callback > should allow prepareStep to modify messages
func TestSDKAgentPrepareStepMessages(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkAgentSetup(text("Hi!"))
		testOptions.Tools = workflowai.ToolSet{}
		testCallOptions.PrepareStep = func(_ context.Context, info workflowai.PrepareStepInfo) (workflowai.PrepareStepResult, error) {
			return workflowai.PrepareStepResult{Messages: append(slices.Clone(info.Messages), user("injected message"))}, nil
		}
		if _, _, err := sdkAgentSteps(t, []workflowai.ModelMessage{user("test")}); err != nil {
			t.Fatal(err)
		}
		prompt := testModel.prompts()[0]
		if len(prompt) != 2 || prompt[1].Content[0].(types.TextContent).Text != "injected message" {
			t.Fatalf("prompt = %+v", prompt)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > prepareStep callback > should allow prepareStep to change model dynamically
func TestSDKAgentPrepareStepModel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkAgentSetup(toolCalls(call("c1", "testTool", `{}`)), text("unused"))
		other := &fakeModel{responses: []func() ([]provider.StreamChunk, error){text("From the other model.")}}
		testOptions.Tools = workflowai.ToolSet{"testTool": sdkAgentTool("testTool", nil)}
		testCallOptions.PrepareStep = func(_ context.Context, info workflowai.PrepareStepInfo) (workflowai.PrepareStepResult, error) {
			if info.StepNumber > 0 {
				return workflowai.PrepareStepResult{Model: other}, nil
			}
			return workflowai.PrepareStepResult{}, nil
		}
		result, _, err := sdkAgentSteps(t, []workflowai.ModelMessage{user("test")})
		if err != nil {
			t.Fatal(err)
		}
		if len(testModel.calls) != 1 || len(other.calls) != 1 || result.Text != "From the other model." {
			t.Fatalf("%d calls, %d calls to the other model, text = %q", len(testModel.calls), len(other.calls), result.Text)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > prepareStep callback > should provide step information to prepareStep callback
func TestSDKAgentPrepareStepInfo(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkAgentSetup(toolCalls(call("c1", "testTool", `{}`)), text("Done."))
		testOptions.Tools = workflowai.ToolSet{"testTool": sdkAgentTool("testTool", nil)}
		var mu sync.Mutex
		infos := map[int]workflowai.PrepareStepInfo{}
		testCallOptions.PrepareStep = func(_ context.Context, info workflowai.PrepareStepInfo) (workflowai.PrepareStepResult, error) {
			mu.Lock()
			defer mu.Unlock()
			infos[info.StepNumber] = info
			return workflowai.PrepareStepResult{}, nil
		}
		if _, _, err := sdkAgentSteps(t, []workflowai.ModelMessage{user("test")}); err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		defer mu.Unlock()
		first, second := infos[0], infos[1]
		if len(infos) != 2 || first.Model.ModelID() != "fake-model" || len(first.Steps) != 0 || len(first.Messages) != 1 {
			t.Fatalf("first step info = %+v", first)
		}
		if second.Model.ModelID() != "fake-model" || len(second.Steps) != 1 || len(second.Messages) != 3 {
			t.Fatalf("second step info = %+v", second)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > tool execution with messages > should pass conversation messages to tool execute function
func TestSDKAgentToolMessages(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkAgentSetup(toolCalls(call("test-call-id", "testTool", `{"query":"weather"}`)), text("Done."))
		testOptions.Tools = workflowai.ToolSet{"testTool": sdkAgentTool("testTool", map[string]any{"result": "success"})}
		if _, _, err := sdkAgentSteps(t, []workflowai.ModelMessage{user("What is the weather?")}); err != nil {
			t.Fatal(err)
		}
		// Like in go-ai, the messages end before the assistant message with
		// the call.
		options := sdkAgentOptions("test-call-id")
		if options.ToolCallID != "test-call-id" {
			t.Fatalf("options = %+v", options)
		}
		if got := sdkAgentRoles(options.Messages); !slices.Equal(got, []string{workflowai.RoleUser}) || options.Messages[0].Content[0].Text != "What is the weather?" {
			t.Fatalf("messages = %+v", options.Messages)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > tool execution with messages > should pass messages to multiple tools in parallel execution
func TestSDKAgentParallelToolMessages(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkAgentSetup(toolCalls(call("weather-call", "weatherTool", `{"city":"NYC"}`), call("news-call", "newsTool", `{"topic":"tech"}`)), text("Done."))
		testOptions.Tools = workflowai.ToolSet{
			"weatherTool": sdkAgentTool("weatherTool", map[string]any{"temp": 72}),
			"newsTool":    sdkAgentTool("newsTool", map[string]any{"headlines": []any{"News 1"}}),
		}
		if _, _, err := sdkAgentSteps(t, []workflowai.ModelMessage{user("Weather and news please")}); err != nil {
			t.Fatal(err)
		}
		for _, id := range []string{"weather-call", "news-call"} {
			messages := sdkAgentOptions(id).Messages
			if got := sdkAgentRoles(messages); !slices.Equal(got, []string{workflowai.RoleUser}) || messages[0].Content[0].Text != "Weather and news please" {
				t.Fatalf("%s messages = %+v", id, messages)
			}
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > tool execution with messages > should pass updated messages on subsequent tool call rounds
func TestSDKAgentToolMessagesPerRound(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkAgentSetup(toolCalls(call("search-1", "searchTool", `{"query":"cats"}`)), toolCalls(call("search-2", "searchTool", `{"query":"dogs"}`)), text("Done."))
		testOptions.Tools = workflowai.ToolSet{"searchTool": sdkAgentTool("searchTool", map[string]any{"found": true})}
		if _, _, err := sdkAgentSteps(t, []workflowai.ModelMessage{user("Search for cats")}); err != nil {
			t.Fatal(err)
		}
		if got := sdkAgentRoles(sdkAgentOptions("search-1").Messages); !slices.Equal(got, []string{workflowai.RoleUser}) {
			t.Fatalf("first round roles = %v", got)
		}
		second := sdkAgentOptions("search-2").Messages
		if got := sdkAgentRoles(second); !slices.Equal(got, []string{workflowai.RoleUser, workflowai.RoleAssistant, workflowai.RoleTool}) {
			t.Fatalf("second round roles = %v", got)
		}
		if second[1].Content[0].ToolCallID != "search-1" || second[2].Content[0].ToolCallID != "search-1" {
			t.Fatalf("second round messages = %+v", second)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > generation settings > should pass generation settings from constructor to modelCallIterator
func TestSDKAgentGenerationSettings(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkAgentSetup(text("Hi!"))
		testOptions.Tools = workflowai.ToolSet{}
		low := types.ReasoningLow
		testOptions.GenerationSettings = workflowai.GenerationSettings{
			Temperature:     sdkAgentPtr(0.7),
			MaxOutputTokens: sdkAgentPtr(1000),
			TopP:            sdkAgentPtr(0.9),
			Seed:            sdkAgentPtr(42),
			Reasoning:       &low,
		}
		if _, _, err := sdkAgentSteps(t, []workflowai.ModelMessage{user("test")}); err != nil {
			t.Fatal(err)
		}
		options := testModel.calls[0]
		if *options.Temperature != 0.7 || *options.MaxTokens != 1000 || *options.TopP != 0.9 || *options.Seed != 42 || *options.Reasoning != types.ReasoningLow {
			t.Fatalf("options = %+v", options)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > generation settings > should allow stream options to override constructor generation settings
func TestSDKAgentCallGenerationSettings(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkAgentSetup(text("Hi!"))
		testOptions.Tools = workflowai.ToolSet{}
		testOptions.Temperature = sdkAgentPtr(0.7)
		none := types.ReasoningNone
		testCallOptions.Temperature = sdkAgentPtr(0.3)
		testCallOptions.MaxOutputTokens = sdkAgentPtr(500)
		testCallOptions.Reasoning = &none
		if _, _, err := sdkAgentSteps(t, []workflowai.ModelMessage{user("test")}); err != nil {
			t.Fatal(err)
		}
		options := testModel.calls[0]
		if *options.Temperature != 0.3 || options.MaxTokens == nil || *options.MaxTokens != 500 || options.Reasoning == nil || *options.Reasoning != types.ReasoningNone {
			t.Fatalf("options = %+v", options)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > toolChoice > should pass toolChoice from constructor to modelCallIterator
// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > toolChoice > should allow stream options to override constructor toolChoice
func TestSDKAgentToolChoice(t *testing.T) {
	required, auto, none := types.RequiredToolChoice(), types.AutoToolChoice(), types.NoneToolChoice()
	tests := []struct {
		name              string
		agent, callChoice *types.ToolChoice
		want              types.ToolChoiceType
	}{
		{"constructor", &required, nil, types.ToolChoiceRequired},
		{"stream override", &auto, &none, types.ToolChoiceNone},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				sdkAgentSetup(text("Hi!"))
				testOptions.Tools = workflowai.ToolSet{}
				testOptions.ToolChoice = tt.agent
				testCallOptions.ToolChoice = tt.callChoice
				if _, _, err := sdkAgentSteps(t, []workflowai.ModelMessage{user("test")}); err != nil {
					t.Fatal(err)
				}
				if got := testModel.calls[0].ToolChoice.Type; got != tt.want {
					t.Fatalf("tool choice = %q, want %q", got, tt.want)
				}
			})
		})
	}
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > activeTools > should filter tools when activeTools is specified
// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > activeTools > should pass no tools when %s activeTools is empty
// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > constructor-level defaults for stream-only parameters > should use constructor activeTools when not specified in stream()
func TestSDKAgentActiveTools(t *testing.T) {
	tests := []struct {
		name              string
		agent, callActive []string
		want              []string
	}{
		{"stream", nil, []string{"tool1", "tool3"}, []string{"tool1", "tool3"}},
		{"constructor empty", []string{}, nil, nil},
		{"stream empty", nil, []string{}, nil},
		{"constructor", []string{"tool1"}, nil, []string{"tool1"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				sdkAgentSetup(text("Hi!"))
				testOptions.Tools = workflowai.ToolSet{"tool1": sdkAgentTool("tool1", nil), "tool2": sdkAgentTool("tool2", nil), "tool3": sdkAgentTool("tool3", nil)}
				testOptions.ActiveTools = tt.agent
				testCallOptions.ActiveTools = tt.callActive
				if _, _, err := sdkAgentSteps(t, []workflowai.ModelMessage{user("test")}); err != nil {
					t.Fatal(err)
				}
				if got := sdkAgentToolNames(testModel.calls[0]); !slices.Equal(got, tt.want) {
					t.Fatalf("tools = %v, want %v", got, tt.want)
				}
			})
		})
	}
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > constructor-level defaults for stream-only parameters > should use constructor stopWhen when not specified in stream()
// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > constructor-level defaults for stream-only parameters > should allow stream options to override constructor stopWhen
func TestSDKAgentStopWhen(t *testing.T) {
	tests := []struct {
		name           string
		agentStops     bool
		callStopWhen   bool
		wantAgentCalls bool
	}{
		{"constructor", true, false, true},
		{"stream override", false, true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				sdkAgentSetup(toolCalls(call("c1", "testTool", `{}`)), text("unused"))
				testOptions.Tools = workflowai.ToolSet{"testTool": sdkAgentTool("testTool", nil)}
				var mu sync.Mutex
				var agentCalls int
				testOptions.StopWhen = []workflowai.StopCondition{func([]workflowai.StepResult) bool {
					mu.Lock()
					defer mu.Unlock()
					agentCalls++
					return tt.agentStops
				}}
				if tt.callStopWhen {
					testCallOptions.StopWhen = []workflowai.StopCondition{workflowai.IsStepCount(1)}
				}
				if _, _, err := sdkAgentSteps(t, []workflowai.ModelMessage{user("test")}); err != nil {
					t.Fatal(err)
				}
				mu.Lock()
				defer mu.Unlock()
				if len(testModel.calls) != 1 || (agentCalls > 0) != tt.wantAgentCalls {
					t.Fatalf("%d model calls, agent stop condition called %d times", len(testModel.calls), agentCalls)
				}
			})
		})
	}
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > constructor-level defaults for stream-only parameters > should use constructor repairToolCall when not specified in stream()
// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > tool call repair > should pass repairToolCall through to modelCallIterator
func TestSDKAgentRepairToolCall(t *testing.T) {
	for _, source := range []string{"constructor", "stream"} {
		t.Run(source, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				sdkAgentSetup(toolCalls(call("c1", "testTool", `{"town":1}`)), text("Done."))
				tool := sdkAgentTool("testTool", nil)
				tool.InputSchema = sdkAgentObject("city")
				testOptions.Tools = workflowai.ToolSet{"testTool": tool}
				var mu sync.Mutex
				var repaired []string
				repair := func(_ context.Context, options ai.ToolCallRepairOptions) (*types.ToolCall, error) {
					mu.Lock()
					defer mu.Unlock()
					repaired = append(repaired, options.ToolCall.ID)
					fixed := options.ToolCall
					fixed.RawArguments = `{"city":"Lagos"}`
					fixed.Arguments = map[string]any{"city": "Lagos"}
					return &fixed, nil
				}
				if source == "constructor" {
					testOptions.RepairToolCall = repair
				} else {
					testCallOptions.RepairToolCall = repair
				}
				result, _, err := sdkAgentSteps(t, []workflowai.ModelMessage{user("test")})
				if err != nil {
					t.Fatal(err)
				}
				mu.Lock()
				defer mu.Unlock()
				if !slices.Equal(repaired, []string{"c1"}) || sdkAgentRuns("testTool") != 1 || string(result.Steps[0].ToolCalls[0].Input) != `{"city":"Lagos"}` {
					t.Fatalf("repaired = %v, step = %+v", repaired, result.Steps[0])
				}
			})
		})
	}
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > callbacks > should keep onError handling at the WorkflowAgent boundary
func TestSDKAgentOnError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkAgentSetup(failWith(errors.New("model down")))
		testOptions.Tools = workflowai.ToolSet{}
		var mu sync.Mutex
		var got error
		testCallOptions.OnError = func(_ context.Context, err error) {
			mu.Lock()
			defer mu.Unlock()
			got = err
		}
		_, _, err := sdkAgentSteps(t, []workflowai.ModelMessage{user("test")})
		if err == nil || !strings.Contains(err.Error(), "model down") {
			t.Fatalf("err = %v", err)
		}
		mu.Lock()
		defer mu.Unlock()
		if got == nil || !strings.Contains(got.Error(), "model down") {
			t.Fatalf("OnError error = %v", got)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > callbacks > should call onFinish with steps and messages when streaming completes
// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > runtimeContext and toolsContext > should pass runtimeContext to onFinish
func TestSDKAgentOnEnd(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkAgentSetup(text("Hello"))
		testOptions.Tools = workflowai.ToolSet{}
		testCallOptions.RuntimeContext = map[string]any{"tenantId": "tenant_123"}
		testCallOptions.ToolsContext = map[string]any{"weather": map[string]any{"unit": "fahrenheit"}}
		var mu sync.Mutex
		var ended *workflowai.EndEvent
		testOptions.OnEnd = func(_ context.Context, event workflowai.EndEvent) {
			mu.Lock()
			defer mu.Unlock()
			ended = &event
		}
		if _, _, err := sdkAgentSteps(t, []workflowai.ModelMessage{user("test")}); err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		defer mu.Unlock()
		if ended == nil || len(ended.Steps) != 1 || len(ended.Messages) != 2 {
			t.Fatalf("OnEnd result = %+v", ended)
		}
		if !reflect.DeepEqual(ended.RuntimeContext, map[string]any{"tenantId": "tenant_123"}) ||
			!reflect.DeepEqual(ended.ToolsContext, map[string]any{"weather": map[string]any{"unit": "fahrenheit"}}) {
			t.Fatalf("OnEnd contexts = %v, %v", ended.RuntimeContext, ended.ToolsContext)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > callbacks > should call onAbort when abort signal is already aborted
func TestSDKAgentAbortedBeforeStart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkAgentSetup(text("unused"))
		testOptions.Tools = workflowai.ToolSet{}
		sdkAgentContext = func(ctx context.Context) (context.Context, error) {
			ctx, cancel, err := workflow.WithCancel(ctx)
			if err != nil {
				return nil, err
			}
			return ctx, cancel()
		}
		aborts := sdkAgentAbortCallbacks()
		_, _, err := sdkAgentSteps(t, []workflowai.ModelMessage{user("test")})
		if err != nil {
			t.Fatal(err)
		}
		if steps := aborts.steps(); !slices.Equal(steps, []int{0}) || len(testModel.calls) != 0 {
			t.Fatalf("aborted with steps %v after %d model calls", steps, len(testModel.calls))
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > callbacks > should handle an aborted model step without calling error or end callbacks
func TestSDKAgentAbortedModelStep(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkAgentSetup(hang)
		testOptions.Tools = workflowai.ToolSet{}
		// The call's context is cancelled while the model call runs.
		sdkAgentContext = func(ctx context.Context) (context.Context, error) {
			ctx, _, err := workflow.WithTimeoutCause(ctx, 30*time.Second, context.Canceled)
			return ctx, err
		}
		aborts := sdkAgentAbortCallbacks()
		_, _, err := sdkAgentSteps(t, []workflowai.ModelMessage{user("test")})
		if err != nil {
			t.Fatal(err)
		}
		if steps := aborts.steps(); len(steps) == 0 || steps[len(steps)-1] != 0 || aborts.errors() != 0 || aborts.ends() != 0 {
			t.Fatalf("aborts = %v, errors = %d, ends = %d", steps, aborts.errors(), aborts.ends())
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > callbacks > should classify timeout errors as aborts
func TestSDKAgentTimeoutIsAbort(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkAgentSetup(hang)
		testOptions.Tools = workflowai.ToolSet{}
		testCallOptions.Timeout = 30 * time.Second
		aborts := sdkAgentAbortCallbacks()
		_, _, err := sdkAgentSteps(t, []workflowai.ModelMessage{user("test")})
		// Like the SDK's stream, an aborted Stream resolves.
		if err != nil {
			t.Fatalf("err = %v", err)
		}
		if steps := aborts.steps(); len(steps) == 0 || steps[len(steps)-1] != 0 || aborts.errors() != 0 || aborts.ends() != 0 {
			t.Fatalf("aborts = %v, errors = %d, ends = %d", steps, aborts.errors(), aborts.ends())
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > callbacks > should pass stepNumber to onToolExecutionStart and use discriminated union in onToolExecutionEnd
// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > callbacks > should pass success: false in onToolExecutionEnd when tool errors
func TestSDKAgentToolExecutionCallbacks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkAgentSetup(toolCalls(call("call-1", "testTool", `{}`), call("call-2", "failTool", `{}`)), text("Done."))
		testOptions.Tools = workflowai.ToolSet{
			"testTool": sdkAgentTool("testTool", map[string]any{"result": "ok"}),
			"failTool": {Execute: workflowai.ExecuteFunc(func(context.Context, json.RawMessage, workflowai.ToolCallOptions) (any, error) {
				return nil, errors.New("tool failed")
			})},
		}
		var mu sync.Mutex
		starts := map[string]workflowai.ToolExecutionStartEvent{}
		ends := map[string]workflowai.ToolExecutionEndEvent{}
		testOptions.OnToolExecutionStart = func(_ context.Context, event workflowai.ToolExecutionStartEvent) {
			mu.Lock()
			defer mu.Unlock()
			starts[event.ToolCall.ToolCallID] = event
		}
		testOptions.OnToolExecutionEnd = func(_ context.Context, event workflowai.ToolExecutionEndEvent) {
			mu.Lock()
			defer mu.Unlock()
			ends[event.ToolCall.ToolCallID] = event
		}
		if _, _, err := sdkAgentSteps(t, []workflowai.ModelMessage{user("test")}); err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		defer mu.Unlock()
		if start := starts["call-1"]; start.StepNumber != 0 || start.ToolCall.ToolName != "testTool" {
			t.Fatalf("start = %+v", start)
		}
		if end := ends["call-1"]; end.StepNumber != 0 || end.Error != nil || !end.Success || end.DurationMs < 0 || end.Output.(map[string]any)["result"] != "ok" || end.ToolCall.ToolName != "testTool" {
			t.Fatalf("end = %+v", end)
		}
		if end := ends["call-2"]; end.StepNumber != 0 || end.Success || end.Error == nil || end.Error.Error() != "tool failed" {
			t.Fatalf("end = %+v", end)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > callbacks > should pass steps to onStepStart callback
func TestSDKAgentOnStepStart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkAgentSetup(toolCalls(call("c1", "testTool", `{}`)), text("Done."))
		testOptions.Tools = workflowai.ToolSet{"testTool": sdkAgentTool("testTool", nil)}
		var mu sync.Mutex
		started := map[int]int{}
		testOptions.OnStepStart = func(_ context.Context, event workflowai.StepStartEvent) {
			mu.Lock()
			defer mu.Unlock()
			started[event.StepNumber] = len(event.Messages)
		}
		if _, _, err := sdkAgentSteps(t, []workflowai.ModelMessage{user("test")}); err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		defer mu.Unlock()
		if len(started) != 2 || started[0] != 1 || started[1] != 3 {
			t.Fatalf("step starts = %v", started)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > runtimeContext and toolsContext > should pass per-tool toolsContext entry as the tool execute context
// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > runtimeContext and toolsContext > should pass undefined context when no toolsContext entry exists
func TestSDKAgentToolsContext(t *testing.T) {
	tests := []struct {
		name         string
		toolsContext map[string]any
		want         any
	}{
		{"entry", map[string]any{"weather": map[string]any{"weatherApiKey": "secret-key", "defaultUnit": "celsius"}}, map[string]any{"weatherApiKey": "secret-key", "defaultUnit": "celsius"}},
		{"no entry", nil, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				sdkAgentSetup(toolCalls(call("call-1", "weather", `{}`)), text("Done."))
				testOptions.Tools = workflowai.ToolSet{"weather": sdkAgentTool("weather", map[string]any{"result": "ok"})}
				testCallOptions.ToolsContext = tt.toolsContext
				if _, _, err := sdkAgentSteps(t, []workflowai.ModelMessage{user("test")}); err != nil {
					t.Fatal(err)
				}
				got, _ := json.Marshal(sdkAgentOptions("call-1").Context)
				want, _ := json.Marshal(tt.want)
				if string(got) != string(want) {
					t.Fatalf("context = %s, want %s", got, want)
				}
			})
		})
	}
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > runtimeContext and toolsContext > should validate %s per-tool context against contextSchema
func TestSDKAgentToolContextSchema(t *testing.T) {
	tests := []struct {
		name         string
		toolsContext map[string]any
	}{
		{"invalid", map[string]any{"weather": map[string]any{}}},
		{"missing", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				sdkAgentSetup(toolCalls(call("call-1", "weather", `{}`)), text("Done."))
				weather := sdkAgentTool("weather", map[string]any{"result": "ok"})
				weather.ContextSchema = map[string]any{
					"type":       "object",
					"properties": map[string]any{"apiKey": map[string]any{"type": "string"}},
					"required":   []any{"apiKey"},
				}
				testOptions.Tools = workflowai.ToolSet{"weather": weather}
				testCallOptions.ToolsContext = tt.toolsContext
				if _, _, err := sdkAgentSteps(t, []workflowai.ModelMessage{user("test")}); err == nil {
					t.Fatal("expected an error")
				}
			})
		})
	}
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > stream result > should return messages and steps in result
func TestSDKAgentResult(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkAgentSetup(text("Hello"))
		testOptions.Tools = workflowai.ToolSet{}
		result, _, err := sdkAgentSteps(t, []workflowai.ModelMessage{user("test")})
		if err != nil {
			t.Fatal(err)
		}
		if got := sdkAgentRoles(result.Messages); !slices.Equal(got, []string{workflowai.RoleUser, workflowai.RoleAssistant}) || result.Messages[1].Content[0].Text != "Hello" {
			t.Fatalf("messages = %+v", result.Messages)
		}
		if len(result.Steps) != 1 || result.Steps[0].Text != "Hello" || result.Steps[0].FinishReason != "stop" || len(result.Steps[0].ToolCalls) != 0 {
			t.Fatalf("steps = %+v", result.Steps)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > prompt parameter > should accept a string prompt instead of messages
func TestSDKAgentPrompt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkAgentSetup(text("Sunny."))
		testOptions.Tools = workflowai.ToolSet{}
		testCallOptions.Prompt = "What is the weather?"
		if _, _, err := sdkAgentSteps(t, nil); err != nil {
			t.Fatal(err)
		}
		prompt := testModel.prompts()[0]
		if len(prompt) != 1 || prompt[0].Role != types.RoleUser || prompt[0].Content[0].(types.TextContent).Text != "What is the weather?" {
			t.Fatalf("prompt = %+v", prompt)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > allowSystemInMessages > should reject system messages in messages by default
// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > allowSystemInMessages > should allow system messages when allowSystemInMessages is true
func TestSDKAgentSystemMessages(t *testing.T) {
	messages := []workflowai.ModelMessage{
		{Role: workflowai.RoleSystem, Content: []workflowai.ContentPart{{Type: workflowai.PartText, Text: "You are a helpful assistant."}}},
		user("What is the weather?"),
	}
	for _, allow := range []bool{false, true} {
		t.Run(map[bool]string{false: "default", true: "allowed"}[allow], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				sdkAgentSetup(text("Sunny."))
				testOptions.Tools = workflowai.ToolSet{}
				testOptions.AllowSystemInMessages = allow
				_, _, err := sdkAgentSteps(t, messages)
				if allow {
					if err != nil || len(testModel.calls) != 1 {
						t.Fatalf("err = %v after %d model calls", err, len(testModel.calls))
					}
					return
				}
				if err == nil || len(testModel.calls) != 0 {
					t.Fatalf("err = %v after %d model calls", err, len(testModel.calls))
				}
			})
		})
	}
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > includeRawChunks > should pass includeRawChunks to modelCallIterator
func TestSDKAgentIncludeRawChunks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkAgentSetup(text("Hi!"))
		testOptions.Tools = workflowai.ToolSet{}
		testCallOptions.IncludeRawChunks = true
		if _, _, err := sdkAgentSteps(t, []workflowai.ModelMessage{user("test")}); err != nil {
			t.Fatal(err)
		}
		if !testModel.calls[0].IncludeRawChunks {
			t.Fatal("model call does not include raw chunks")
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > telemetry > should pass telemetry settings from constructor to modelCallIterator
// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > telemetry > should allow stream options to override constructor telemetry
func TestSDKAgentTelemetry(t *testing.T) {
	for _, override := range []bool{false, true} {
		t.Run(map[bool]string{false: "constructor", true: "stream override"}[override], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				exporter := tracetest.NewInMemoryExporter()
				tracer := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter)).Tracer("test")
				integrations := []telemetry.TelemetryIntegration{telemetry.NewOpenTelemetry(telemetry.OpenTelemetryOptions{Tracer: tracer})}
				sdkAgentSetup(text("Hi!"))
				testOptions.Tools = workflowai.ToolSet{}
				testOptions.Telemetry = &telemetry.Options{IsEnabled: telemetry.Bool(true), FunctionID: "test-agent", Integrations: integrations}
				if override {
					testCallOptions.Telemetry = &telemetry.Options{IsEnabled: telemetry.Bool(false), FunctionID: "stream-id", Integrations: integrations}
				}
				if _, _, err := sdkAgentSteps(t, []workflowai.ModelMessage{user("test")}); err != nil {
					t.Fatal(err)
				}
				if spans := exporter.GetSpans(); (len(spans) > 0) == override {
					t.Fatalf("recorded %d spans", len(spans))
				}
			})
		})
	}
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > tool approval resumption > should execute approved tools with conversation context and lifecycle callbacks
func TestSDKAgentApprovedToolExecution(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkAgentSetup(text("It is 72 degrees."))
		testOptions.Tools = sdkAgentApprovalTools()
		var mu sync.Mutex
		var callbacks []string
		record := func(name string) {
			mu.Lock()
			defer mu.Unlock()
			callbacks = append(callbacks, name)
		}
		testOptions.OnToolExecutionStart = func(context.Context, workflowai.ToolExecutionStartEvent) { record("constructor-start") }
		testOptions.OnToolExecutionEnd = func(context.Context, workflowai.ToolExecutionEndEvent) { record("constructor-end") }
		testCallOptions.OnToolExecutionStart = func(context.Context, workflowai.ToolExecutionStartEvent) { record("stream-start") }
		testCallOptions.OnToolExecutionEnd = func(context.Context, workflowai.ToolExecutionEndEvent) { record("stream-end") }
		messages := sdkAgentApprovalMessages(sdkAgentApproval{})
		if _, _, err := sdkAgentSteps(t, messages); err != nil {
			t.Fatal(err)
		}
		if runs := sdkAgentRuns("getWeather"); runs != 1 || len(testModel.calls) != 1 {
			t.Fatalf("getWeather ran %d times, %d model calls", runs, len(testModel.calls))
		}
		options := sdkAgentOptions("call-1")
		if options.ToolCallID != "call-1" || !slices.Equal(sdkAgentRoles(options.Messages), sdkAgentRoles(messages)) {
			t.Fatalf("options = %+v", options)
		}
		// Replays call the callbacks again, so check the last execution's.
		mu.Lock()
		defer mu.Unlock()
		want := []string{"constructor-start", "stream-start", "constructor-end", "stream-end"}
		if len(callbacks) < 4 || !slices.Equal(callbacks[len(callbacks)-4:], want) {
			t.Fatalf("callbacks = %v", callbacks)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > tool approval resumption > should execute a tool when the replayed approval signature is valid
func TestSDKAgentValidApprovalSignature(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		t.Setenv("WORKFLOW_TOOL_APPROVAL_SECRET", sdkAgentApprovalSecret)
		t.Setenv("OTHER_TOOL_APPROVAL_SECRET", "incorrect-secret")
		sdkAgentSetup(text("Done."))
		testOptions.Tools = sdkAgentApprovalTools()
		testOptions.ToolApprovalSecret = &workflowai.ToolApprovalSecret{EnvironmentVariable: "OTHER_TOOL_APPROVAL_SECRET"}
		testCallOptions.ToolApprovalSecret = &workflowai.ToolApprovalSecret{EnvironmentVariable: "WORKFLOW_TOOL_APPROVAL_SECRET"}
		messages := sdkAgentApprovalMessages(sdkAgentApproval{Signature: sdkAgentSign(t)})
		if _, _, err := sdkAgentSteps(t, messages); err != nil {
			t.Fatal(err)
		}
		if runs := sdkAgentRuns("getWeather"); runs != 1 {
			t.Fatalf("getWeather ran %d times", runs)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > tool approval resumption > should reject a replay with $name
func TestSDKAgentRejectedApprovalReplay(t *testing.T) {
	tests := []struct {
		name   string
		replay func(approval *sdkAgentApproval)
	}{
		{"missing signature", func(approval *sdkAgentApproval) { approval.Signature = "" }},
		{"tampered signature", func(approval *sdkAgentApproval) { approval.Signature = "tampered-signature" }},
		{"different approval id", func(approval *sdkAgentApproval) { approval.ApprovalID = "approval-call-2" }},
		{"different tool call id", func(approval *sdkAgentApproval) { approval.ToolCallID = "call-2" }},
		{"different tool name", func(approval *sdkAgentApproval) { approval.ToolName = "getForecast" }},
		{"different schema-valid input", func(approval *sdkAgentApproval) { approval.Input = `{"city":"Paris"}` }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				t.Setenv("WORKFLOW_TOOL_APPROVAL_SECRET", sdkAgentApprovalSecret)
				sdkAgentSetup(text("Not done."))
				testOptions.Tools = sdkAgentApprovalTools()
				testOptions.ToolApprovalSecret = &workflowai.ToolApprovalSecret{EnvironmentVariable: "WORKFLOW_TOOL_APPROVAL_SECRET"}
				approval := sdkAgentApproval{Signature: sdkAgentSign(t)}
				tt.replay(&approval)
				if _, _, err := sdkAgentSteps(t, sdkAgentApprovalMessages(approval)); err != nil {
					t.Fatal(err)
				}
				if sdkAgentRuns("getWeather") != 0 || sdkAgentRuns("getForecast") != 0 {
					t.Fatalf("getWeather ran %d times, getForecast %d times", sdkAgentRuns("getWeather"), sdkAgentRuns("getForecast"))
				}
			})
		})
	}
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > tool approval resumption > should continue with a model-visible error when approved input does not match the schema
func TestSDKAgentApprovedInvalidInput(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkAgentSetup(text("Sorry."))
		testOptions.Tools = sdkAgentApprovalTools()
		if _, _, err := sdkAgentSteps(t, sdkAgentApprovalMessages(sdkAgentApproval{Input: `{"city":42}`})); err != nil {
			t.Fatal(err)
		}
		if runs := sdkAgentRuns("getWeather"); runs != 0 || len(testModel.calls) != 1 {
			t.Fatalf("getWeather ran %d times, %d model calls", runs, len(testModel.calls))
		}
		prompt := testModel.prompts()[0]
		last := prompt[len(prompt)-1]
		if last.Role != types.RoleTool || len(last.Content) != 1 {
			t.Fatalf("prompt = %+v", prompt)
		}
		result, ok := last.Content[0].(types.ToolResultContent)
		if !ok || result.ToolCallID != "call-1" || result.ToolName != "getWeather" || result.Output.Type != types.ToolResultOutputErrorText ||
			!strings.Contains(result.Output.Value.(string), "Invalid input for tool getWeather") {
			t.Fatalf("tool result = %+v", last.Content[0])
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > tool approval resumption > should not execute an approved tool when it does not declare needsApproval
func TestSDKAgentApprovalWithoutNeedsApproval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkAgentSetup(text("Done."))
		tools := sdkAgentApprovalTools()
		weather := tools["getWeather"]
		weather.NeedsApproval = nil
		testOptions.Tools = workflowai.ToolSet{"getWeather": weather}
		if _, _, err := sdkAgentSteps(t, sdkAgentApprovalMessages(sdkAgentApproval{})); err != nil {
			t.Fatal(err)
		}
		if runs := sdkAgentRuns("getWeather"); runs != 0 {
			t.Fatalf("getWeather ran %d times", runs)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > tool approval resumption > should validate each approval independently (one forged, one valid)
func TestSDKAgentApprovalsValidatedIndependently(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkAgentSetup(text("Done."))
		validTool := sdkAgentTool("validTool", map[string]any{"ok": true})
		validTool.InputSchema = sdkAgentObject("city")
		validTool.NeedsApproval = workflowai.NeedsApprovalAlways
		forgedTool := sdkAgentTool("forgedTool", map[string]any{"ok": true})
		forgedTool.InputSchema = sdkAgentObject("city")
		forgedTool.NeedsApproval = workflowai.NeedsApprovalAlways
		testOptions.Tools = workflowai.ToolSet{"validTool": validTool, "forgedTool": forgedTool}
		messages := []workflowai.ModelMessage{
			user("do both"),
			{Role: workflowai.RoleAssistant, Content: []workflowai.ContentPart{
				{Type: workflowai.PartToolCall, ToolCallID: "call-valid", ToolName: "validTool", Input: json.RawMessage(`{"city":"London"}`)},
				{Type: workflowai.PartToolApprovalRequest, ApprovalID: "approval-valid", ToolCallID: "call-valid"},
				{Type: workflowai.PartToolCall, ToolCallID: "call-forged", ToolName: "forgedTool", Input: json.RawMessage(`{"city":42}`)},
				{Type: workflowai.PartToolApprovalRequest, ApprovalID: "approval-forged", ToolCallID: "call-forged"},
			}},
			{Role: workflowai.RoleTool, Content: []workflowai.ContentPart{
				{Type: workflowai.PartToolApprovalResponse, ApprovalID: "approval-valid", Approved: true},
				{Type: workflowai.PartToolApprovalResponse, ApprovalID: "approval-forged", Approved: true},
			}},
		}
		if _, _, err := sdkAgentSteps(t, messages); err != nil {
			t.Fatal(err)
		}
		if sdkAgentRuns("validTool") != 1 || sdkAgentRuns("forgedTool") != 0 {
			t.Fatalf("validTool ran %d times, forgedTool %d times", sdkAgentRuns("validTool"), sdkAgentRuns("forgedTool"))
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > tool approval resumption > should use toModelOutput for approved tool results while preserving raw stream output
func TestSDKAgentApprovedToolModelOutput(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkAgentSetup(text("Done."))
		testOptions.Tools = sdkAgentApprovalTools()
		weather := testOptions.Tools["getWeather"]
		weather.ToModelOutput = sdkAgentModelSees
		weather.Execute = sdkAgentTool("getWeather", map[string]any{"public": "weather summary", "secret": "hide me"}).Execute
		testOptions.Tools["getWeather"] = weather
		_, parts, _, err := sdkAgentStream(t, sdkAgentApprovalMessages(sdkAgentApproval{}))
		if err != nil {
			t.Fatal(err)
		}
		var sent *types.ToolResultContent
		for _, result := range sdkAgentToolResults(testModel.prompts()[0], types.RoleTool) {
			if result.ToolCallID == "call-1" {
				sent = &result
			}
		}
		if sent == nil || sent.ToolName != "getWeather" || sent.Output.Type != types.ToolResultOutputText || sent.Output.Value != "model sees: weather summary" {
			t.Fatalf("tool result = %+v", sent)
		}
		if !slices.ContainsFunc(parts, func(part workflowai.ModelCallStreamPart) bool {
			return part.Type == "tool-result" && part.ToolCallID == "call-1" && string(part.Output) == `{"public":"weather summary","secret":"hide me"}`
		}) {
			t.Fatalf("parts = %+v", parts)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > tool approval resumption > should create denial results for denied tools and continue
func TestSDKAgentDeniedApproval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkAgentSetup(text("Not deleted."))
		testOptions.Tools = sdkAgentDeleteFileTools()
		if _, _, err := sdkAgentSteps(t, sdkAgentDenialMessages(false)); err != nil {
			t.Fatal(err)
		}
		if runs := sdkAgentRuns("deleteFile"); runs != 0 || len(testModel.calls) != 1 {
			t.Fatalf("deleteFile ran %d times, %d model calls", runs, len(testModel.calls))
		}
		results := sdkAgentToolResults(testModel.prompts()[0], types.RoleTool)
		if len(results) != 1 || results[0].Output.Type != types.ToolResultOutputExecutionDenied || results[0].Output.Reason != "Too dangerous" {
			t.Fatalf("tool results = %+v", results)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > tool approval resumption > should reuse an existing denial result while streaming the denial event
func TestSDKAgentExistingDenialResult(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkAgentSetup(text("Not deleted."))
		testOptions.Tools = sdkAgentDeleteFileTools()
		_, parts, _, err := sdkAgentStream(t, sdkAgentDenialMessages(true))
		if err != nil {
			t.Fatal(err)
		}
		if runs := sdkAgentRuns("deleteFile"); runs != 0 {
			t.Fatalf("deleteFile ran %d times", runs)
		}
		var results int
		for _, result := range sdkAgentToolResults(testModel.prompts()[0], types.RoleTool) {
			if result.ToolCallID == "call-1" {
				results++
			}
		}
		if results != 1 {
			t.Fatalf("prompt has %d results for the call: %+v", results, testModel.prompts()[0])
		}
		if !slices.ContainsFunc(parts, func(part workflowai.ModelCallStreamPart) bool {
			return part.Type == "tool-output-denied" && part.ToolCallID == "call-1"
		}) {
			t.Fatalf("parts = %+v", parts)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > tool approval resumption > should pass through messages without approval responses unchanged
func TestSDKAgentMessagesWithoutApprovals(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkAgentSetup(text("Hi!"))
		testOptions.Tools = sdkAgentApprovalTools()
		if _, _, err := sdkAgentSteps(t, []workflowai.ModelMessage{user("Hello")}); err != nil {
			t.Fatal(err)
		}
		prompt := testModel.prompts()[0]
		if len(prompt) != 1 || prompt[0].Role != types.RoleUser || prompt[0].Content[0].(types.TextContent).Text != "Hello" {
			t.Fatalf("prompt = %+v", prompt)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > tool approval resumption > should forward an approved provider-executed approval to the provider
// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > tool approval resumption > should forward a denied provider-executed approval to the provider
func TestSDKAgentForwardsProviderApprovals(t *testing.T) {
	for _, approved := range []bool{true, false} {
		t.Run(map[bool]string{true: "approved", false: "denied"}[approved], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				sdkAgentSetup(text("Found the docs."))
				testOptions.Tools = workflowai.ToolSet{}
				messages := []workflowai.ModelMessage{
					user("Search the docs."),
					{Role: workflowai.RoleAssistant, Content: []workflowai.ContentPart{
						{Type: workflowai.PartToolCall, ToolCallID: "call-1", ToolName: "mcp_search", Input: json.RawMessage(`{"query":"docs"}`), ProviderExecuted: true},
						{Type: workflowai.PartToolApprovalRequest, ApprovalID: "approval-call-1", ToolCallID: "call-1"},
					}},
					{Role: workflowai.RoleTool, Content: []workflowai.ContentPart{
						{Type: workflowai.PartToolApprovalResponse, ApprovalID: "approval-call-1", Approved: approved},
					}},
				}
				if _, _, err := sdkAgentSteps(t, messages); err != nil {
					t.Fatal(err)
				}
				if got := sdkAgentApprovalResponses(testModel.prompts()[0]); !slices.Contains(got, sdkAgentApprovalResponse{"approval-call-1", approved}) {
					t.Fatalf("approval responses = %v", got)
				}
			})
		})
	}
}

// AI SDK: packages/workflow/src/workflow-agent.test.ts > WorkflowAgent > tool approval resumption > should execute local approvals while forwarding provider-executed ones
func TestSDKAgentLocalAndProviderApprovals(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkAgentSetup(text("Done."))
		testOptions.Tools = sdkAgentApprovalTools()
		messages := []workflowai.ModelMessage{
			user("Weather in London and search the docs."),
			{Role: workflowai.RoleAssistant, Content: []workflowai.ContentPart{
				{Type: workflowai.PartToolCall, ToolCallID: "local-1", ToolName: "getWeather", Input: json.RawMessage(`{"city":"London"}`)},
				{Type: workflowai.PartToolApprovalRequest, ApprovalID: "approval-local-1", ToolCallID: "local-1"},
				{Type: workflowai.PartToolCall, ToolCallID: "provider-1", ToolName: "mcp_search", Input: json.RawMessage(`{"query":"docs"}`), ProviderExecuted: true},
				{Type: workflowai.PartToolApprovalRequest, ApprovalID: "approval-provider-1", ToolCallID: "provider-1"},
			}},
			{Role: workflowai.RoleTool, Content: []workflowai.ContentPart{
				{Type: workflowai.PartToolApprovalResponse, ApprovalID: "approval-local-1", Approved: true},
				{Type: workflowai.PartToolApprovalResponse, ApprovalID: "approval-provider-1", Approved: true},
			}},
		}
		if _, _, err := sdkAgentSteps(t, messages); err != nil {
			t.Fatal(err)
		}
		if runs := sdkAgentRuns("getWeather"); runs != 1 || sdkAgentOptions("local-1").ToolCallID != "local-1" {
			t.Fatalf("getWeather ran %d times", runs)
		}
		// Only the provider's approval is forwarded; the local one is replaced
		// by the call's result.
		got := sdkAgentApprovalResponses(testModel.prompts()[0])
		if !slices.Contains(got, sdkAgentApprovalResponse{"approval-provider-1", true}) || slices.Contains(got, sdkAgentApprovalResponse{"approval-local-1", true}) {
			t.Fatalf("approval responses = %v", got)
		}
	})
}

// SDKAgentChat runs one Stream turn of the agent the tests in this file
// configure.
type SDKAgentChat struct {
	Messages []workflowai.ModelMessage
}

func (SDKAgentChat) Workflow(workflowai.Result) {}

// sdkAgentContext changes the context SDKAgentChatRunner calls Stream with.
var sdkAgentContext func(ctx context.Context) (context.Context, error)

func SDKAgentChatRunner(ctx context.Context, input SDKAgentChat) (workflowai.Result, error) {
	if sdkAgentContext != nil {
		var err error
		if ctx, err = sdkAgentContext(ctx); err != nil {
			return workflowai.Result{}, err
		}
	}
	options := testOptions
	options.Model = testModel
	callOptions := testCallOptions
	callOptions.Messages = input.Messages
	return workflowai.NewWorkflowAgent(options).Stream(ctx, callOptions)
}

// sdkAgentSetup scripts the model's responses and resets the agent and the
// tools of this file.
func sdkAgentSetup(responses ...func() ([]provider.StreamChunk, error)) {
	setup(responses...)
	sdkAgentContext = nil
	sdkAgentTools.mu.Lock()
	defer sdkAgentTools.mu.Unlock()
	sdkAgentTools.runs = map[string]int{}
	sdkAgentTools.options = map[string]workflowai.ToolCallOptions{}
	sdkAgentTools.outputs = map[string]map[string]any{}
}

// sdkAgentStream runs one turn and returns its result, the parts of its
// stream, and the names of its steps.
func sdkAgentStream(t *testing.T, messages []workflowai.ModelMessage) (workflowai.Result, []workflowai.ModelCallStreamPart, []string, error) {
	t.Helper()
	world := testworld.New(worlds.Runners(workflow.Use(SDKAgentChatRunner)))
	defer world.Close()
	ctx := workflow.NewContext(t.Context(), workflow.NewClient(world))
	run, err := workflow.Start(ctx, SDKAgentChat{Messages: messages})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := run.Reader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var parts []workflowai.ModelCallStreamPart
	for {
		chunk, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
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
	result, err := run.Value(ctx)
	return result, parts, stepNames(t, world, run.ID()), err
}

// sdkAgentSteps runs one turn without reading its stream, for turns that may
// fail before they write to it, and returns its result and the names of its
// steps.
func sdkAgentSteps(t *testing.T, messages []workflowai.ModelMessage) (workflowai.Result, []string, error) {
	t.Helper()
	world := testworld.New(worlds.Runners(workflow.Use(SDKAgentChatRunner)))
	defer world.Close()
	ctx := workflow.NewContext(t.Context(), workflow.NewClient(world))
	run, err := workflow.Start(ctx, SDKAgentChat{Messages: messages})
	if err != nil {
		t.Fatal(err)
	}
	result, err := run.Value(ctx)
	return result, stepNames(t, world, run.ID()), err
}

// sdkAgentTools records the executions of the tools sdkAgentTool returns.
var sdkAgentTools struct {
	mu      sync.Mutex
	runs    map[string]int
	options map[string]workflowai.ToolCallOptions
	outputs map[string]map[string]any
}

// sdkAgentTool returns a tool that executes its calls in a step returning
// output, recording the ToolCallOptions of each call by ID.
// sdkAgentModelSees shows the model "model sees: " and the output's public
// field.
func sdkAgentModelSees(_ context.Context, options workflowai.ToolModelOutputOptions) (workflowai.ToolResultOutput, error) {
	public := options.Output.(map[string]any)["public"]
	return workflowai.ToolResultOutput{Type: "text", Value: json.RawMessage(strconv.Quote("model sees: " + public.(string)))}, nil
}

func sdkAgentTool(name string, output map[string]any) workflowai.Tool {
	sdkAgentTools.mu.Lock()
	defer sdkAgentTools.mu.Unlock()
	sdkAgentTools.outputs[name] = output
	return workflowai.Tool{Description: "The " + name + " tool", Execute: sdkAgentExecutor(name)}
}

type sdkAgentExecutor string

func (name sdkAgentExecutor) Execute(ctx context.Context, input json.RawMessage, options workflowai.ToolCallOptions) *workflow.Result[any] {
	sdkAgentTools.mu.Lock()
	sdkAgentTools.options[options.ToolCallID] = options
	sdkAgentTools.mu.Unlock()
	result := workflow.Background(ctx, sdkAgentToolStep, sdkAgentToolInput{Tool: string(name), Input: input})
	return workflow.Then(ctx, result, func(output map[string]any, err error) *workflow.Result[any] {
		return workflow.NewResult[any](output, err)
	})
}

type sdkAgentToolInput struct {
	Tool  string          `json:"tool"`
	Input json.RawMessage `json:"input"`
}

// sdkAgentToolStep counts an execution of a tool and returns its output.
func sdkAgentToolStep(_ context.Context, input sdkAgentToolInput) (map[string]any, error) {
	sdkAgentTools.mu.Lock()
	defer sdkAgentTools.mu.Unlock()
	sdkAgentTools.runs[input.Tool]++
	return sdkAgentTools.outputs[input.Tool], nil
}

// sdkAgentRuns returns how many times the tool named name executed.
func sdkAgentRuns(name string) int {
	sdkAgentTools.mu.Lock()
	defer sdkAgentTools.mu.Unlock()
	return sdkAgentTools.runs[name]
}

// sdkAgentOptions returns the ToolCallOptions of the call with id toolCallID.
func sdkAgentOptions(toolCallID string) workflowai.ToolCallOptions {
	sdkAgentTools.mu.Lock()
	defer sdkAgentTools.mu.Unlock()
	return sdkAgentTools.options[toolCallID]
}

// sdkAgentObject returns the schema of an object with the named required
// string properties.
func sdkAgentObject(names ...string) map[string]any {
	properties := map[string]any{}
	required := []any{}
	for _, name := range names {
		properties[name] = map[string]any{"type": "string"}
		required = append(required, name)
	}
	return map[string]any{"type": "object", "properties": properties, "required": required}
}

// sdkAgentProviderResult returns the stream chunk of a provider-executed
// tool's result.
func sdkAgentProviderResult(id, name string, result any, metadata map[string]any) provider.StreamChunk {
	return provider.StreamChunk{Type: provider.ChunkTypeToolResult, ToolResult: &types.ToolResult{
		ToolCallID: id, ToolName: name, Result: result, ProviderExecuted: true, ProviderMetadata: metadata,
	}}
}

// sdkAgentContentResult returns the tool-result part of content for the call
// with id toolCallID, or nil.
func sdkAgentContentResult(content []workflowai.ContentPart, toolCallID string) *workflowai.ContentPart {
	for _, part := range content {
		if part.Type == workflowai.PartToolResult && part.ToolCallID == toolCallID {
			return &part
		}
	}
	return nil
}

// sdkAgentToolResults returns the tool results in the messages of prompt with
// role.
func sdkAgentToolResults(prompt []types.Message, role types.MessageRole) []types.ToolResultContent {
	var results []types.ToolResultContent
	for _, message := range prompt {
		if message.Role != role {
			continue
		}
		for _, part := range message.Content {
			if result, ok := part.(types.ToolResultContent); ok {
				results = append(results, result)
			}
		}
	}
	return results
}

// sdkAgentToolNames returns the names of the tools a model call got.
func sdkAgentToolNames(options *provider.GenerateOptions) []string {
	var names []string
	for _, tool := range options.Tools {
		names = append(names, tool.Name)
	}
	return names
}

// sdkAgentRoles returns the roles of messages.
func sdkAgentRoles(messages []workflowai.ModelMessage) []string {
	var roles []string
	for _, message := range messages {
		roles = append(roles, message.Role)
	}
	return roles
}

// sdkAgentNoStartStep fails when the agent wrote a start-step part, which only
// separates a tool step from a following model call.
func sdkAgentNoStartStep(t *testing.T, parts []workflowai.ModelCallStreamPart) {
	t.Helper()
	if slices.ContainsFunc(parts, func(part workflowai.ModelCallStreamPart) bool { return part.Type == "start-step" }) {
		t.Fatalf("parts = %+v", parts)
	}
}

// sdkAgentAborts records the OnAbort, OnError and OnEnd calls of a turn.
type sdkAgentAborts struct {
	mu          sync.Mutex
	abortSteps  []int
	errs, endsN int
}

// sdkAgentAbortCallbacks sets the call's OnAbort, OnError and OnEnd to record
// their calls.
func sdkAgentAbortCallbacks() *sdkAgentAborts {
	aborts := &sdkAgentAborts{}
	testCallOptions.OnAbort = func(_ context.Context, event workflowai.AbortEvent) {
		aborts.mu.Lock()
		defer aborts.mu.Unlock()
		aborts.abortSteps = append(aborts.abortSteps, len(event.Steps))
	}
	testCallOptions.OnError = func(context.Context, error) {
		aborts.mu.Lock()
		defer aborts.mu.Unlock()
		aborts.errs++
	}
	testCallOptions.OnEnd = func(context.Context, workflowai.EndEvent) {
		aborts.mu.Lock()
		defer aborts.mu.Unlock()
		aborts.endsN++
	}
	return aborts
}

// steps returns the number of steps of each OnAbort call, with replays
// repeating them.
func (a *sdkAgentAborts) steps() []int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Compact(slices.Clone(a.abortSteps))
}

func (a *sdkAgentAborts) errors() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.errs
}

func (a *sdkAgentAborts) ends() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.endsN
}

const sdkAgentApprovalSecret = "workflow-tool-approval-secret"

// sdkAgentApproval describes the approved getWeather call of
// sdkAgentApprovalMessages. Zero fields take the defaults of the signed call.
type sdkAgentApproval struct {
	ApprovalID, ToolCallID, ToolName, Input, Signature string
}

// sdkAgentApprovalMessages returns a conversation whose last message approves
// a call.
func sdkAgentApprovalMessages(approval sdkAgentApproval) []workflowai.ModelMessage {
	approvalID := cmp.Or(approval.ApprovalID, "approval-call-1")
	toolCallID := cmp.Or(approval.ToolCallID, "call-1")
	return []workflowai.ModelMessage{
		user("What's the weather in London?"),
		{Role: workflowai.RoleAssistant, Content: []workflowai.ContentPart{
			{Type: workflowai.PartToolCall, ToolCallID: toolCallID, ToolName: cmp.Or(approval.ToolName, "getWeather"), Input: json.RawMessage(cmp.Or(approval.Input, `{"city":"London"}`))},
			{Type: workflowai.PartToolApprovalRequest, ApprovalID: approvalID, ToolCallID: toolCallID, Signature: approval.Signature},
		}},
		{Role: workflowai.RoleTool, Content: []workflowai.ContentPart{
			{Type: workflowai.PartToolApprovalResponse, ApprovalID: approvalID, Approved: true},
		}},
	}
}

// sdkAgentSign returns the signature of the default approval of
// sdkAgentApprovalMessages.
func sdkAgentSign(t *testing.T) string {
	t.Helper()
	signature, err := ai.SignToolApproval([]byte(sdkAgentApprovalSecret), "approval-call-1", "call-1", "getWeather", map[string]any{"city": "London"})
	if err != nil {
		t.Fatal(err)
	}
	return signature
}

// sdkAgentApprovalTools returns the getWeather and getForecast tools, which
// need approval.
func sdkAgentApprovalTools() workflowai.ToolSet {
	tools := workflowai.ToolSet{}
	for name, output := range map[string]map[string]any{"getWeather": {"city": "London", "temperature": 72}, "getForecast": {"ok": true}} {
		tool := sdkAgentTool(name, output)
		tool.InputSchema = sdkAgentObject("city")
		tool.NeedsApproval = workflowai.NeedsApprovalAlways
		tools[name] = tool
	}
	return tools
}

// sdkAgentDeleteFileTools returns the deleteFile tool, which needs approval.
func sdkAgentDeleteFileTools() workflowai.ToolSet {
	tool := sdkAgentTool("deleteFile", nil)
	tool.InputSchema = sdkAgentObject("path")
	tool.NeedsApproval = workflowai.NeedsApprovalAlways
	return workflowai.ToolSet{"deleteFile": tool}
}

// sdkAgentDenialMessages returns a conversation whose last message denies a
// deleteFile call, with the denial's result when withResult is set.
func sdkAgentDenialMessages(withResult bool) []workflowai.ModelMessage {
	response := []workflowai.ContentPart{{Type: workflowai.PartToolApprovalResponse, ApprovalID: "approval-call-1", Reason: "Too dangerous"}}
	if withResult {
		response = append(response, workflowai.ContentPart{Type: workflowai.PartToolResult, ToolCallID: "call-1", ToolName: "deleteFile", Output: &workflowai.ToolResultOutput{Type: "execution-denied", Reason: "Too dangerous"}})
	}
	return []workflowai.ModelMessage{
		user("Delete /etc/passwd"),
		{Role: workflowai.RoleAssistant, Content: []workflowai.ContentPart{
			{Type: workflowai.PartToolCall, ToolCallID: "call-1", ToolName: "deleteFile", Input: json.RawMessage(`{"path":"/etc/passwd"}`)},
			{Type: workflowai.PartToolApprovalRequest, ApprovalID: "approval-call-1", ToolCallID: "call-1"},
		}},
		{Role: workflowai.RoleTool, Content: response},
	}
}

type sdkAgentApprovalResponse struct {
	ApprovalID string
	Approved   bool
}

// sdkAgentApprovalResponses returns the approval responses in the tool
// messages of prompt.
func sdkAgentApprovalResponses(prompt []types.Message) []sdkAgentApprovalResponse {
	var responses []sdkAgentApprovalResponse
	for _, message := range prompt {
		if message.Role != types.RoleTool {
			continue
		}
		for _, part := range message.Content {
			if response, ok := part.(types.ToolApprovalResponseContent); ok {
				responses = append(responses, sdkAgentApprovalResponse{response.ApprovalID, response.Approved})
			}
		}
	}
	return responses
}

func sdkAgentPtr[T any](value T) *T { return &value }
