package workflowai_test

import (
	"cmp"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/digitallysavvy/go-ai/pkg/ai"
	"github.com/digitallysavvy/go-ai/pkg/provider"
	providererrors "github.com/digitallysavvy/go-ai/pkg/provider/errors"
	"github.com/digitallysavvy/go-ai/pkg/provider/types"
	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/workflowai"
	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/testworld"
)

// AI SDK: packages/workflow/src/workflow-agent-generate.test.ts > WorkflowAgent.generate > matches core multi-step results using only doGenerate
func TestSDKRunGenerateMultiStep(t *testing.T) {
	var result workflowai.Result
	var reasoning types.ReasoningContent
	synctest.Test(t, func(t *testing.T) {
		model := sdkRunNewModel(
			sdkRunGenerateResponse(types.FinishReasonToolCalls,
				types.ReasoningContent{Text: "thinking", ProviderMetadata: json.RawMessage(`{"fixture":{"thought":1}}`)},
				types.TextContent{Text: "Checking."},
				sdkRunLookupCall("lookup-1", `{"city":"London"}`),
			),
			sdkRunGenerateResponse(types.FinishReasonStop, types.TextContent{Text: "Sunny."}),
		)
		sdkRunSetup(model)
		testOptions.Tools = sdkRunLookupTools()
		out := sdkRunGenerate(t, "Find the weather.")
		if out.err != nil {
			t.Fatal(out.err)
		}
		result = out.result
		if generate, stream := model.counts(); generate != 2 || stream != 0 {
			t.Fatalf("doGenerate calls = %d, doStream calls = %d", generate, stream)
		}
		// Like core's generate, the tool calls are those of every step.
		if result.Text != "Sunny." || result.FinishReason != "stop" || len(result.Steps) != 2 || len(result.ToolCalls) != 1 {
			t.Fatalf("result = %+v", result)
		}
		if got := sdkRunRoles(result.Messages[1:]); !slices.Equal(got, []string{"assistant", "tool", "assistant"}) {
			t.Fatalf("response roles = %v", got)
		}
		if *result.TotalUsage.InputTokens != 10 || *result.TotalUsage.OutputTokens != 6 {
			t.Fatalf("total usage = %+v", result.TotalUsage)
		}
		results := result.Steps[0].ToolResults
		if len(results) != 1 || string(results[0].Output) != `"weather in London"` {
			t.Fatalf("tool results = %+v", results)
		}
		// The second model call gets the first step's reasoning with its
		// provider metadata as provider options, its text and its tool call.
		prompt := model.all()[1].Prompt.Messages
		if len(prompt) != 3 || prompt[0].Role != types.RoleUser || prompt[1].Role != types.RoleAssistant || prompt[2].Role != types.RoleTool {
			t.Fatalf("prompt = %+v", prompt)
		}
		var kinds []string
		for _, part := range prompt[1].Content {
			kinds = append(kinds, part.ContentType())
		}
		if !slices.Equal(kinds, []string{"reasoning", "text", "tool-call"}) {
			t.Fatalf("assistant content = %v", kinds)
		}
		reasoning = prompt[1].Content[0].(types.ReasoningContent)
	})
	t.Run("reasoning provider options", func(t *testing.T) {
		if fixture, _ := reasoning.ProviderOptions["fixture"].(map[string]any); fixture["thought"] != float64(1) {
			t.Fatalf("reasoning = %+v", reasoning)
		}
	})
	t.Run("default text output", func(t *testing.T) {
		if result.Output != "Sunny." {
			t.Fatalf("output = %#v", result.Output)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-generate.test.ts > WorkflowAgent.generate > preserves ordered files, sources, reasoning and provider-executed results
func TestSDKRunGenerateOrderedContent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		model := sdkRunNewModel(sdkRunGenerateResponse(types.FinishReasonStop,
			types.TextContent{Text: "Answer"},
			types.FileContent{MediaType: "text/plain", Data: []byte("hello")},
			types.SourceContent{SourceType: "url", ID: "source", URL: "https://example.com"},
			types.ToolCallContent{ToolCallID: "server", ToolName: "lookup", Input: `{"city":"London"}`, ProviderExecuted: true},
			types.ReasoningContent{Text: "Reasoning"},
			types.ToolResultContent{ToolCallID: "server", ToolName: "lookup", Result: map[string]any{"found": true}},
		))
		sdkRunSetup(model)
		testOptions.Tools = sdkRunLookupTools()
		out := sdkRunGenerate(t, "Find the weather.")
		if out.err != nil {
			t.Fatal(out.err)
		}
		step := out.result.Steps[0]
		var kinds []string
		for _, part := range step.Content {
			kinds = append(kinds, part.Type)
		}
		if want := []string{"text", "file", "source", "tool-call", "reasoning", "tool-result"}; !slices.Equal(kinds, want) {
			t.Fatalf("content = %v, want %v", kinds, want)
		}
		if step.Content[1].Data != base64.StdEncoding.EncodeToString([]byte("hello")) {
			t.Fatalf("file = %+v", step.Content[1])
		}
		if len(step.ToolResults) != 1 || step.ToolResults[0].ToolCallID != "server" || string(step.ToolResults[0].Output) != `{"found":true}` {
			t.Fatalf("tool results = %+v", step.ToolResults)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-generate.test.ts > WorkflowAgent.generate > shares option precedence and tool lifecycle context without stream deltas
func TestSDKRunGenerateOptionPrecedence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		model := sdkRunNewModel(
			sdkRunGenerateResponse(types.FinishReasonToolCalls, sdkRunLookupCall("lookup-1", `{"city":"London"}`)),
			sdkRunGenerateResponse(types.FinishReasonStop, types.TextContent{Text: "Done"}),
		)
		sdkRunSetup(model)
		low, prepared := 0.1, 0.2
		testOptions.Temperature = &low
		testOptions.PrepareCall = func(_ context.Context, options workflowai.PrepareCallOptions) (workflowai.PrepareCallResult, error) {
			return workflowai.PrepareCallResult{GenerationSettings: workflowai.GenerationSettings{Temperature: &prepared}}, nil
		}
		testOptions.RuntimeContext = map[string]any{"userId": "user"}
		testOptions.Tools = workflowai.ToolSet{"lookup": {
			InputSchema: sdkRunCitySchema(),
			Execute: workflowai.ExecuteFunc(func(_ context.Context, _ json.RawMessage, options workflowai.ToolCallOptions) (any, error) {
				sdkRunEvents.add(fmt.Sprintf("execute:%v", options.Context.(map[string]any)["key"]))
				return 42, nil
			}),
		}}
		testOptions.ToolsContext = map[string]any{"lookup": map[string]any{"key": "lookup-key"}}
		testOptions.OnEnd = func(context.Context, workflowai.EndEvent) { sdkRunEvents.add("constructor-end") }
		testCallOptions.OnEnd = func(context.Context, workflowai.EndEvent) { sdkRunEvents.add("call-end") }
		out := sdkRunGenerate(t, "Find the weather.")
		if out.err != nil {
			t.Fatal(out.err)
		}
		// Generate does not stream, so no tool input delta is reported.
		if got, want := sdkRunEvents.list(), []string{"execute:lookup-key", "constructor-end", "call-end"}; !slices.Equal(got, want) {
			t.Fatalf("events = %v, want %v", got, want)
		}
		if temperature := model.all()[0].Temperature; temperature == nil || *temperature != prepared {
			t.Fatalf("temperature = %v", temperature)
		}
	})
	t.Run("call settings", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			model := sdkRunNewModel(sdkRunGenerateResponse(types.FinishReasonStop, types.TextContent{Text: "Done"}))
			sdkRunSetup(model)
			prepared, call := 0.2, 0.3
			testOptions.PrepareCall = func(context.Context, workflowai.PrepareCallOptions) (workflowai.PrepareCallResult, error) {
				return workflowai.PrepareCallResult{GenerationSettings: workflowai.GenerationSettings{Temperature: &prepared}}, nil
			}
			testOptions.RuntimeContext = map[string]any{"userId": "user"}
			testCallOptions.Temperature = &call
			out := sdkRunGenerate(t, "Find the weather.")
			if out.err != nil {
				t.Fatal(out.err)
			}
			if temperature := model.all()[0].Temperature; temperature == nil || *temperature != call {
				t.Fatalf("temperature = %v", temperature)
			}
			if steps := out.result.Steps; !reflect.DeepEqual(steps[len(steps)-1].RuntimeContext, map[string]any{"userId": "user"}) {
				t.Fatalf("runtime context = %v", steps[len(steps)-1].RuntimeContext)
			}
		})
	})
	t.Run("tool input lifecycle", func(t *testing.T) {
		t.Skip("workflowai tools have no onInputStart and onInputAvailable hooks")
	})
}

// The SDK stubs the global fetch its default download uses; the port stubs
// go-ai's DefaultDownload, which refuses loopback test servers.
//
// AI SDK: packages/workflow/src/workflow-agent-generate.test.ts > WorkflowAgent.generate > downloads URL-backed files that the generate model does not support
func TestSDKRunGenerateDownloadsUnsupportedURLs(t *testing.T) {
	defaultDownload := ai.DefaultDownload
	t.Cleanup(func() { ai.DefaultDownload = defaultDownload })
	var requests []ai.DownloadRequest
	ai.DefaultDownload = func(_ context.Context, batch []ai.DownloadRequest) ([]*ai.DownloadResult, error) {
		requests = append(requests, batch...)
		return []*ai.DownloadResult{{Data: []byte{1, 2, 3}, MediaType: "application/pdf"}}, nil
	}
	synctest.Test(t, func(t *testing.T) {
		model := sdkRunNewModel(sdkRunGenerateResponse(types.FinishReasonStop, types.TextContent{Text: "Done"}))
		sdkRunSetup(model)
		out := sdkRunDo(t, sdkRunInput{Generate: true, Messages: []workflowai.ModelMessage{sdkRunFileMessage("https://example.com/file.pdf")}})
		if out.err != nil {
			t.Fatal(out.err)
		}
		if len(requests) != 1 || requests[0].URL != "https://example.com/file.pdf" {
			t.Fatalf("download requests = %+v", requests)
		}
		if file := sdkRunPromptFile(t, model.all()[0]); !slices.Equal(file.FileData.Data, []byte{1, 2, 3}) {
			t.Fatalf("file = %+v", file)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-generate.test.ts > WorkflowAgent.generate > preserves URL-backed files supported by the generate model without fetching
func TestSDKRunGenerateKeepsSupportedURLs(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		model := sdkRunNewModel(sdkRunGenerateResponse(types.FinishReasonStop, types.TextContent{Text: "Done"}))
		model.supportedURLs = map[string][]string{"application/pdf": {".*"}}
		sdkRunSetup(model)
		out := sdkRunDo(t, sdkRunInput{Generate: true, Messages: []workflowai.ModelMessage{sdkRunFileMessage("https://example.com/file.pdf")}})
		if out.err != nil {
			t.Fatal(out.err)
		}
		file := sdkRunPromptFile(t, model.all()[0])
		if sdkRunFileURL(file) != "https://example.com/file.pdf" || len(file.FileData.Data) != 0 || file.MediaType != "application/pdf" {
			t.Fatalf("file = %+v", file)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-generate.test.ts > WorkflowAgent.generate > honors a custom pass-through decision for unsupported URLs
func TestSDKRunGenerateCustomDownloadPassThrough(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		model := sdkRunNewModel(sdkRunGenerateResponse(types.FinishReasonStop, types.TextContent{Text: "Done"}))
		sdkRunSetup(model)
		var mu sync.Mutex
		var requests [][]ai.DownloadRequest
		testOptions.Download = func(_ context.Context, batch []ai.DownloadRequest) ([]*ai.DownloadResult, error) {
			mu.Lock()
			defer mu.Unlock()
			requests = append(requests, batch)
			return make([]*ai.DownloadResult, len(batch)), nil
		}
		out := sdkRunDo(t, sdkRunInput{Generate: true, Messages: []workflowai.ModelMessage{sdkRunFileMessage("https://example.com/file.pdf")}})
		if out.err != nil {
			t.Fatal(out.err)
		}
		mu.Lock()
		defer mu.Unlock()
		if len(requests) != 1 || len(requests[0]) != 1 || requests[0][0] != (ai.DownloadRequest{URL: "https://example.com/file.pdf"}) {
			t.Fatalf("download requests = %+v", requests)
		}
		if file := sdkRunPromptFile(t, model.all()[0]); sdkRunFileURL(file) != "https://example.com/file.pdf" || len(file.FileData.Data) != 0 {
			t.Fatalf("file = %+v", file)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-generate.test.ts > WorkflowAgent.generate > preserves generated history when prepareStep replaces the conversation
func TestSDKRunGeneratePrepareStepKeepsHistory(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkRunSetup(sdkRunNewModel(
			sdkRunGenerateResponse(types.FinishReasonToolCalls, sdkRunLookupCall("lookup-1", `{"city":"London"}`)),
			sdkRunGenerateResponse(types.FinishReasonStop, types.TextContent{Text: "Done"}),
		))
		testOptions.Tools = sdkRunLookupTools()
		testOptions.PrepareStep = func(_ context.Context, info workflowai.PrepareStepInfo) (workflowai.PrepareStepResult, error) {
			if info.StepNumber == 1 {
				return workflowai.PrepareStepResult{Messages: []workflowai.ModelMessage{user("Replacement")}}, nil
			}
			return workflowai.PrepareStepResult{}, nil
		}
		out := sdkRunGenerate(t, "Find the weather.")
		if out.err != nil {
			t.Fatal(out.err)
		}
		response := out.result.Messages[1:]
		if got := sdkRunRoles(response); !slices.Equal(got, []string{"assistant", "tool", "assistant"}) {
			t.Fatalf("response roles = %v", got)
		}
		if response[0].Content[0].ToolCallID != "lookup-1" {
			t.Fatalf("first response message = %+v", response[0])
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-generate.test.ts > WorkflowAgent.generate > defaults to 20 steps and honors an explicit limit
func TestSDKRunGenerateDefaultStepLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := sdkRunGenerateResponse(types.FinishReasonToolCalls, sdkRunLookupCall("lookup-1", `{"city":"London"}`))
		model := sdkRunNewModel()
		model.repeat = &calls
		sdkRunSetup(model)
		testOptions.Tools = sdkRunLookupTools()
		out := sdkRunGenerate(t, "Find the weather.")
		if out.err != nil || len(out.result.Steps) != 20 || out.result.Output != nil {
			t.Fatalf("err = %v, %d steps, output = %#v", out.err, len(out.result.Steps), out.result.Output)
		}

		model = sdkRunNewModel()
		model.repeat = &calls
		sdkRunSetup(model)
		testOptions.Tools = sdkRunLookupTools()
		testCallOptions.StopWhen = []workflowai.StopCondition{workflowai.IsStepCount(2)}
		out = sdkRunGenerate(t, "Find the weather.")
		if out.err != nil || len(out.result.Steps) != 2 {
			t.Fatalf("err = %v, %d steps", out.err, len(out.result.Steps))
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-generate.test.ts > WorkflowAgent.generate > returns client tools and invokes step-end before end without parsing tool-call output
func TestSDKRunGenerateClientTools(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkRunSetup(sdkRunNewModel(sdkRunGenerateResponse(types.FinishReasonToolCalls, types.TextContent{Text: "{"}, sdkRunLookupCall("lookup-1", `{"city":"London"}`))))
		testOptions.Tools = workflowai.ToolSet{"lookup": {InputSchema: sdkRunCitySchema()}}
		testOptions.Output = sdkRunValueOutput()
		testOptions.OnStepEnd = func(context.Context, workflowai.StepResult) { sdkRunEvents.add("step") }
		testOptions.OnEnd = func(context.Context, workflowai.EndEvent) { sdkRunEvents.add("end") }
		out := sdkRunGenerate(t, "Find the weather.")
		if out.err != nil {
			t.Fatal(out.err)
		}
		if len(out.result.ToolCalls) != 1 || len(out.result.ToolResults) != 0 || out.result.Output != nil {
			t.Fatalf("result = %+v", out.result)
		}
		if got := sdkRunEvents.list(); !slices.Equal(got, []string{"step", "end"}) {
			t.Fatalf("events = %v", got)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-generate.test.ts > WorkflowAgent.generate > matches core output and response messages for finish reason %s
func TestSDKRunGenerateFinishReasons(t *testing.T) {
	for _, reason := range []types.FinishReason{types.FinishReasonStop, types.FinishReasonLength, types.FinishReasonContentFilter, types.FinishReasonError, types.FinishReasonOther} {
		t.Run(string(reason), func(t *testing.T) {
			var result workflowai.Result
			synctest.Test(t, func(t *testing.T) {
				sdkRunSetup(sdkRunNewModel(sdkRunGenerateResponse(reason, types.TextContent{Text: "Answer"})))
				out := sdkRunGenerate(t, "Find the weather.")
				if out.err != nil {
					t.Fatal(out.err)
				}
				response := out.result.Messages[1:]
				if len(response) != 1 || response[0].Role != "assistant" || response[0].Content[0].Text != "Answer" || out.result.FinishReason != string(reason) {
					t.Fatalf("result = %+v", out.result)
				}
				result = out.result
			})
			t.Run("default text output", func(t *testing.T) {
				if result.Output != "Answer" {
					t.Fatalf("output = %#v", result.Output)
				}
			})
		})
	}
}

// AI SDK: packages/workflow/src/workflow-agent-generate.test.ts > WorkflowAgent.generate > parses empty stop output and runs onEnd before parsing
func TestSDKRunGenerateParsesEmptyOutputAfterEnd(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkRunSetup(sdkRunNewModel(sdkRunGenerateResponse(types.FinishReasonStop)))
		testOptions.Output = sdkRunRecordingOutput{}
		testOptions.OnEnd = func(context.Context, workflowai.EndEvent) { sdkRunEvents.add("end") }
		out := sdkRunGenerate(t, "Find the weather.")
		if out.err != nil {
			t.Fatal(out.err)
		}
		if out.result.Output != "" {
			t.Fatalf("output = %#v", out.result.Output)
		}
		if got := sdkRunEvents.list(); !slices.Equal(got, []string{"end", "parse"}) {
			t.Fatalf("events = %v", got)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-generate.test.ts > WorkflowAgent.generate > runs onEnd before an output parse failure
func TestSDKRunGenerateEndsBeforeParseFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkRunSetup(sdkRunNewModel(sdkRunGenerateResponse(types.FinishReasonStop, types.TextContent{Text: "{"})))
		testOptions.Output = sdkRunValueOutput()
		testOptions.OnEnd = func(context.Context, workflowai.EndEvent) { sdkRunEvents.add("end") }
		out := sdkRunGenerate(t, "Find the weather.")
		if out.err == nil {
			t.Fatal("expected a parse error")
		}
		if got := sdkRunEvents.list(); !slices.Equal(got, []string{"end"}) {
			t.Fatalf("events = %v", got)
		}
	})
}

// The SDK also rejects with undefined and false, which Go errors cannot be.
//
// AI SDK: packages/workflow/src/workflow-agent-generate.test.ts > WorkflowAgent.generate > rejects model failure %s without firing onEnd
func TestSDKRunGenerateModelFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkRunSetup(sdkRunNewModel(sdkRunResponse{err: errors.New("model failed")}))
		maxRetries := 0
		testOptions.MaxRetries = &maxRetries
		testOptions.OnEnd = func(context.Context, workflowai.EndEvent) { sdkRunEvents.add("end") }
		out := sdkRunGenerate(t, "Find the weather.")
		if out.err == nil || !strings.Contains(out.err.Error(), "model failed") {
			t.Fatalf("err = %v", out.err)
		}
		if got := sdkRunEvents.list(); len(got) != 0 {
			t.Fatalf("events = %v", got)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-generate.test.ts > WorkflowAgent.generate > preserves unknown usage counts and token details
func TestSDKRunGenerateUnknownUsage(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		response := sdkRunGenerateResponse(types.FinishReasonStop, types.TextContent{Text: "Answer"})
		response.usage = types.Usage{}
		sdkRunSetup(sdkRunNewModel(response))
		out := sdkRunGenerate(t, "Find the weather.")
		if out.err != nil {
			t.Fatal(out.err)
		}
		usage := out.result.Steps[0].Usage
		if usage.InputTokens != nil || usage.OutputTokens != nil || usage.TotalTokens != nil || out.result.TotalUsage.TotalTokens != nil {
			t.Fatalf("usage = %+v, total = %+v", usage, out.result.TotalUsage)
		}
	})
}

// The SDK also keeps the response's timestamp and headers, which StepResult
// does not have.
//
// AI SDK: packages/workflow/src/workflow-agent-generate.test.ts > WorkflowAgent.generate > retains metadata with opt-in bodies and omits them by default
func TestSDKRunGenerateIncludeBodies(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkRunSetup(sdkRunNewModel(sdkRunGenerateResponse(types.FinishReasonStop)))
		testCallOptions.Include = &ai.IncludeOptions{RequestBody: true, ResponseBody: true, RequestMessages: true}
		out := sdkRunGenerate(t, "Find the weather.")
		if out.err != nil {
			t.Fatal(out.err)
		}
		step := out.result.Steps[0]
		if step.Request.Body != "request-body" || len(step.Request.Messages) != 1 || step.Request.Messages[0].Content[0].Text != "Find the weather." {
			t.Fatalf("request = %+v", step.Request)
		}
		if step.Response.ID != "response-id" || step.Response.ModelID != "response-model" || step.Response.Body != "response-body" {
			t.Fatalf("response = %+v", step.Response)
		}

		sdkRunSetup(sdkRunNewModel(sdkRunGenerateResponse(types.FinishReasonStop)))
		out = sdkRunGenerate(t, "Find the weather.")
		if out.err != nil {
			t.Fatal(out.err)
		}
		if step := out.result.Steps[0]; step.Request.Body != nil || step.Response.Body != nil {
			t.Fatalf("step = %+v", step)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-generate.test.ts > WorkflowAgent.generate > uses per-call output overrides
func TestSDKRunGenerateCallOutput(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkRunSetup(sdkRunNewModel(sdkRunGenerateResponse(types.FinishReasonStop, types.TextContent{Text: "Plain"})))
		testOptions.Output = sdkRunValueOutput()
		testCallOptions.Output = workflowai.OutputFrom(ai.TextOutput())
		out := sdkRunGenerate(t, "Find the weather.")
		if out.err != nil || out.result.Output != "Plain" {
			t.Fatalf("err = %v, output = %#v", out.err, out.result.Output)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-generate.test.ts > WorkflowAgent.generate > retains invalid calls and tool errors, then continues
func TestSDKRunGenerateInvalidCall(t *testing.T) {
	var steps []workflowai.StepResult
	synctest.Test(t, func(t *testing.T) {
		sdkRunSetup(sdkRunNewModel(
			sdkRunGenerateResponse(types.FinishReasonToolCalls, sdkRunLookupCall("lookup-1", `{"city":123}`)),
			sdkRunGenerateResponse(types.FinishReasonStop, types.TextContent{Text: "Done"}),
		))
		testOptions.Tools = sdkRunLookupTools()
		out := sdkRunGenerate(t, "Find the weather.")
		if out.err != nil {
			t.Fatal(out.err)
		}
		steps = out.result.Steps
		if len(steps) != 2 || len(steps[0].ToolCalls) != 1 || !steps[0].ToolCalls[0].Invalid || steps[1].Text != "Done" {
			t.Fatalf("steps = %+v", steps)
		}
		// The model gets the invalid call's error as its result.
		tool := out.result.Messages[2]
		if tool.Role != "tool" || tool.Content[0].ToolCallID != "lookup-1" || tool.Content[0].Output.Type != "error-text" {
			t.Fatalf("tool message = %+v", tool)
		}
	})
	t.Run("tool-error content", func(t *testing.T) {
		var kinds []string
		for _, part := range steps[0].Content {
			kinds = append(kinds, part.Type)
		}
		if !slices.Equal(kinds, []string{"tool-call", "tool-error"}) {
			t.Fatalf("content = %v", kinds)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-generate.test.ts > WorkflowAgent.generate > rejects an already-aborted invocation before model dispatch
func TestSDKRunGenerateAlreadyAborted(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		model := sdkRunNewModel(sdkRunGenerateResponse(types.FinishReasonStop))
		sdkRunSetup(model)
		sdkRunCancelBefore(nil)
		out := sdkRunGenerate(t, "Find the weather.")
		if !sdkRunAborted(out.err) {
			t.Fatalf("err = %v", out.err)
		}
		if len(model.all()) != 0 {
			t.Fatalf("%d model calls", len(model.all()))
		}
	})
}

// workflowai has no constructor abort signal, so the call's context is
// cancelled with the reason.
//
// AI SDK: packages/workflow/src/workflow-agent-generate.test.ts > WorkflowAgent.generate > preserves an already-aborted constructor signal reason
func TestSDKRunGenerateAbortReason(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		model := sdkRunNewModel(sdkRunGenerateResponse(types.FinishReasonStop))
		sdkRunSetup(model)
		sdkRunCancelBefore(errors.New("cancelled by caller"))
		out := sdkRunGenerate(t, "Find the weather.")
		if out.err == nil || !strings.Contains(out.err.Error(), "cancelled by caller") {
			t.Fatalf("err = %v", out.err)
		}
		if len(model.all()) != 0 {
			t.Fatalf("%d model calls", len(model.all()))
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-contract.test.ts > WorkflowAgent stream and core generate contracts > uses equivalent model responses while distinguishing final-step and all-step tools
func TestSDKRunStreamAndGenerateContract(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var results [2]workflowai.Result
		for i, generate := range []bool{false, true} {
			model := sdkRunNewModel(sdkRunToolResponse("weather-call"), sdkRunTextResponse("It is 18°C."))
			sdkRunSetup(model)
			testOptions.Tools = sdkRunWeatherTools()
			out := sdkRunDo(t, sdkRunInput{Generate: generate, Prompt: "What is the weather in London?"})
			if out.err != nil {
				t.Fatal(out.err)
			}
			if generateCalls, streamCalls := model.counts(); generate && (generateCalls != 2 || streamCalls != 0) || !generate && (generateCalls != 0 || streamCalls != 2) {
				t.Fatalf("generate %v: doGenerate calls = %d, doStream calls = %d", generate, generateCalls, streamCalls)
			}
			results[i] = out.result
		}
		streamed, generated := results[0], results[1]
		for _, result := range results {
			var texts []string
			for _, step := range result.Steps {
				texts = append(texts, step.Text)
			}
			if !slices.Equal(texts, []string{"Checking the weather.", "It is 18°C."}) || result.Text != "It is 18°C." || result.FinishReason != "stop" {
				t.Fatalf("result = %+v", result)
			}
			calls := result.Steps[0].ToolCalls
			if len(calls) != 1 || calls[0].ToolCallID != "weather-call" || calls[0].ToolName != "weather" || string(calls[0].Input) != `{"city":"London"}` {
				t.Fatalf("tool calls = %+v", calls)
			}
			toolResults := result.Steps[0].ToolResults
			if len(toolResults) != 1 || toolResults[0].ToolCallID != "weather-call" || string(toolResults[0].Input) != `{"city":"London"}` || string(toolResults[0].Output) != `{"temperature":18}` {
				t.Fatalf("tool results = %+v", toolResults)
			}
			usage := result.TotalUsage
			if *usage.InputTokens != 6 || *usage.OutputTokens != 4 || *usage.TotalTokens != 10 {
				t.Fatalf("total usage = %+v", usage)
			}
			if result.Messages[0].Role != "user" || !slices.Equal(sdkRunRoles(result.Messages[1:]), []string{"assistant", "tool", "assistant"}) {
				t.Fatalf("messages = %v", sdkRunRoles(result.Messages))
			}
		}
		// The stream reports the tools of its last step, which made no tool
		// calls, and generate the tools of every step.
		if len(streamed.ToolCalls) != 0 || len(streamed.ToolResults) != 0 {
			t.Fatalf("stream tools = %+v, %+v", streamed.ToolCalls, streamed.ToolResults)
		}
		if !reflect.DeepEqual(generated.ToolCalls, streamed.Steps[0].ToolCalls) || !reflect.DeepEqual(generated.ToolResults, streamed.Steps[0].ToolResults) {
			t.Fatalf("generate tools = %+v, %+v", generated.ToolCalls, generated.ToolResults)
		}
		streamedMessages, _ := json.Marshal(streamed.Messages)
		generatedMessages, _ := json.Marshal(generated.Messages)
		if string(streamedMessages) != string(generatedMessages) {
			t.Fatalf("stream messages = %s, generate messages = %s", streamedMessages, generatedMessages)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-contract.test.ts > WorkflowAgent stream and core generate contracts > aggregates stream usage like core, keeping token details
func TestSDKRunStreamUsageDetails(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkRunSetup(sdkRunNewModel(sdkRunToolResponse("weather-call"), sdkRunTextResponse("It is 18°C.")))
		testOptions.Tools = sdkRunWeatherTools()
		var mu sync.Mutex
		var endUsage types.Usage
		testCallOptions.OnEnd = func(_ context.Context, result workflowai.EndEvent) {
			mu.Lock()
			defer mu.Unlock()
			endUsage = result.TotalUsage
		}
		out := sdkRunStream(t, "What is the weather in London?")
		if out.err != nil {
			t.Fatal(out.err)
		}
		want := sdkRunUsage(6, 4, 2, 4, 4)
		for _, usage := range []types.Usage{out.result.TotalUsage, endUsage} {
			got, _ := json.Marshal(usage)
			wanted, _ := json.Marshal(want)
			if string(got) != string(wanted) {
				t.Fatalf("usage = %s, want %s", got, wanted)
			}
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-contract.test.ts > WorkflowAgent stream and core generate contracts > keeps unknown stream usage counts undefined like core
func TestSDKRunStreamUnknownUsage(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		response := sdkRunTextResponse("Done.")
		response.usage = types.Usage{}
		sdkRunSetup(sdkRunNewModel(response))
		out := sdkRunStream(t, "What is the weather in London?")
		if out.err != nil {
			t.Fatal(out.err)
		}
		usage := out.result.TotalUsage
		if usage.InputTokens != nil || usage.OutputTokens != nil || usage.TotalTokens != nil {
			t.Fatalf("total usage = %+v", usage)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-contract.test.ts > WorkflowAgent stream and core generate contracts > returns unresolved client tool calls in both contracts
func TestSDKRunClientToolCalls(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, generate := range []bool{false, true} {
			sdkRunSetup(sdkRunNewModel(sdkRunToolResponse("weather-call")))
			testOptions.Tools = workflowai.ToolSet{"weather": {InputSchema: sdkRunCitySchema()}}
			out := sdkRunDo(t, sdkRunInput{Generate: generate, Prompt: "What is the weather in London?"})
			if out.err != nil {
				t.Fatal(out.err)
			}
			result := out.result
			if len(result.Steps) != 1 || len(result.ToolResults) != 0 {
				t.Fatalf("generate %v: result = %+v", generate, result)
			}
			if calls := result.ToolCalls; len(calls) != 1 || calls[0].ToolCallID != "weather-call" || calls[0].ToolName != "weather" || string(calls[0].Input) != `{"city":"London"}` {
				t.Fatalf("generate %v: tool calls = %+v", generate, calls)
			}
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-contract.test.ts > WorkflowAgent stream and core generate contracts > passes the same conversation scope to tool execution as core
func TestSDKRunToolConversationScope(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, generate := range []bool{false, true} {
			sdkRunSetup(sdkRunNewModel(sdkRunToolResponse("weather-call"), sdkRunToolResponse("weather-call-2"), sdkRunTextResponse("It is 18°C.")))
			testOptions.Instructions = "You are a weather assistant."
			testOptions.Tools = workflowai.ToolSet{"weather": {
				InputSchema: sdkRunCitySchema(),
				Execute: workflowai.ExecuteFunc(func(_ context.Context, _ json.RawMessage, options workflowai.ToolCallOptions) (any, error) {
					sdkRunEvents.add(strings.Join(sdkRunRoles(options.Messages), ","))
					return map[string]any{"temperature": 18}, nil
				}),
			}}
			out := sdkRunDo(t, sdkRunInput{Generate: generate, Prompt: "What is the weather in London?"})
			if out.err != nil {
				t.Fatal(out.err)
			}
			// No system prompt, and no assistant message with the current call.
			if got, want := sdkRunEvents.list(), []string{"user", "user,assistant,tool"}; !slices.Equal(got, want) {
				t.Fatalf("generate %v: tool messages = %v, want %v", generate, got, want)
			}
		}
	})
	t.Run("tool execution start messages", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			for _, generate := range []bool{false, true} {
				sdkRunSetup(sdkRunNewModel(sdkRunToolResponse("weather-call"), sdkRunToolResponse("weather-call-2"), sdkRunTextResponse("It is 18°C.")))
				testOptions.Instructions = "You are a weather assistant."
				testOptions.Tools = sdkRunWeatherTools()
				testOptions.OnToolExecutionStart = func(_ context.Context, event workflowai.ToolExecutionStartEvent) {
					sdkRunEvents.add(event.ToolCall.ToolCallID + ":" + strings.Join(sdkRunRoles(event.Messages), ","))
				}
				out := sdkRunDo(t, sdkRunInput{Generate: generate, Prompt: "What is the weather in London?"})
				if out.err != nil {
					t.Fatal(out.err)
				}
				// Replays call the callback again, so check each call's last event.
				last := map[string]string{}
				for _, event := range sdkRunEvents.list() {
					id, roles, _ := strings.Cut(event, ":")
					last[id] = roles
				}
				if last["weather-call"] != "user" || last["weather-call-2"] != "user,assistant,tool" {
					t.Fatalf("generate %v: tool start messages = %v", generate, last)
				}
			}
		})
	})
}

// AI SDK: packages/workflow/src/workflow-agent-contract.test.ts > WorkflowAgent stream and core generate contracts > distinguishes the core 20-step default from the unbounded Workflow stream default
func TestSDKRunStepLimitDefaults(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		responses := func() []sdkRunResponse {
			var responses []sdkRunResponse
			for i := range 21 {
				responses = append(responses, sdkRunToolResponse(fmt.Sprintf("weather-%d", i)))
			}
			return append(responses, sdkRunTextResponse("Done."))
		}
		sdkRunSetup(sdkRunNewModel(responses()...))
		testOptions.Tools = sdkRunWeatherTools()
		generated := sdkRunGenerate(t, "What is the weather in London?")
		if generated.err != nil || len(generated.result.Steps) != 20 || generated.result.FinishReason != "tool-calls" {
			t.Fatalf("generate: err = %v, %d steps, finish reason %q", generated.err, len(generated.result.Steps), generated.result.FinishReason)
		}
		sdkRunSetup(sdkRunNewModel(responses()...))
		testOptions.Tools = sdkRunWeatherTools()
		streamed := sdkRunStream(t, "What is the weather in London?")
		if streamed.err != nil || len(streamed.result.Steps) != 22 || streamed.result.Text != "Done." {
			t.Fatalf("stream: err = %v, %d steps, text %q", streamed.err, len(streamed.result.Steps), streamed.result.Text)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-contract.test.ts > WorkflowAgent stream and core generate contracts > applies prepareStep setting overrides to one step only, like core
func TestSDKRunPrepareStepOverridesOneStep(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, generate := range []bool{false, true} {
			model := sdkRunNewModel(sdkRunToolResponse("weather-call"), sdkRunTextResponse("It is 18°C."))
			sdkRunSetup(model)
			tools := sdkRunWeatherTools()
			tools["other"] = workflowai.Tool{InputSchema: map[string]any{"type": "object"}, Execute: workflowai.ExecuteFunc(func(context.Context, json.RawMessage, workflowai.ToolCallOptions) (any, error) {
				return "unused", nil
			})}
			testOptions.Tools = tools
			temperature := 0.1
			testOptions.Temperature = &temperature
			required := types.RequiredToolChoice()
			testOptions.PrepareStep = func(_ context.Context, info workflowai.PrepareStepInfo) (workflowai.PrepareStepResult, error) {
				if info.StepNumber == 0 {
					return workflowai.PrepareStepResult{ToolChoice: &required, ActiveTools: []string{"weather"}}, nil
				}
				return workflowai.PrepareStepResult{}, nil
			}
			out := sdkRunDo(t, sdkRunInput{Generate: generate, Prompt: "What is the weather in London?"})
			if out.err != nil {
				t.Fatal(out.err)
			}
			calls := model.all()
			wantTools := [][]string{{"weather"}, {"other", "weather"}}
			wantChoice := []types.ToolChoiceType{types.ToolChoiceRequired, types.ToolChoiceAuto}
			for i, call := range calls {
				var names []string
				for _, tool := range call.Tools {
					names = append(names, tool.Name)
				}
				slices.Sort(names)
				if !slices.Equal(names, wantTools[i]) || call.ToolChoice.Type != wantChoice[i] || call.Temperature == nil || *call.Temperature != 0.1 {
					t.Fatalf("generate %v: call %d tools = %v, choice = %+v, temperature = %v", generate, i, names, call.ToolChoice, call.Temperature)
				}
			}
		}
	})
	t.Run("temperature and provider options", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			for _, generate := range []bool{false, true} {
				model := sdkRunNewModel(sdkRunToolResponse("weather-call"), sdkRunTextResponse("It is 18°C."))
				sdkRunSetup(model)
				testOptions.Tools = sdkRunWeatherTools()
				low, high := 0.1, 0.9
				testOptions.Temperature = &low
				testOptions.PrepareStep = func(_ context.Context, info workflowai.PrepareStepInfo) (workflowai.PrepareStepResult, error) {
					if info.StepNumber == 0 {
						return workflowai.PrepareStepResult{GenerationSettings: workflowai.GenerationSettings{
							Temperature:     &high,
							ProviderOptions: map[string]any{"mock": map[string]any{"firstStep": true}},
						}}, nil
					}
					return workflowai.PrepareStepResult{}, nil
				}
				out := sdkRunDo(t, sdkRunInput{Generate: generate, Prompt: "What is the weather in London?"})
				if out.err != nil {
					t.Fatal(out.err)
				}
				calls := model.all()
				if len(calls) != 2 || *calls[0].Temperature != high || !reflect.DeepEqual(map[string]any(calls[0].ProviderOptions), map[string]any{"mock": map[string]any{"firstStep": true}}) ||
					*calls[1].Temperature != low || len(calls[1].ProviderOptions) != 0 {
					t.Fatalf("generate %v: calls = %+v", generate, calls)
				}
			}
		})
	})
}

// AI SDK: packages/workflow/src/workflow-agent-contract.test.ts > WorkflowAgent stream and core generate contracts > uses a prepareStep model override for one step only, like core
func TestSDKRunPrepareStepModelOneStep(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, generate := range []bool{false, true} {
			model := sdkRunNewModel(sdkRunTextResponse("It is 18°C."))
			first := sdkRunNewModel(sdkRunToolResponse("weather-call"))
			sdkRunSetup(model)
			testOptions.Tools = sdkRunWeatherTools()
			testOptions.PrepareStep = func(_ context.Context, info workflowai.PrepareStepInfo) (workflowai.PrepareStepResult, error) {
				if info.StepNumber == 0 {
					return workflowai.PrepareStepResult{Model: first}, nil
				}
				return workflowai.PrepareStepResult{}, nil
			}
			out := sdkRunDo(t, sdkRunInput{Generate: generate, Prompt: "What is the weather in London?"})
			if out.err != nil {
				t.Fatal(out.err)
			}
			if len(first.all()) != 1 || len(model.all()) != 1 {
				t.Fatalf("generate %v: first step model calls = %d, model calls = %d", generate, len(first.all()), len(model.all()))
			}
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-contract.test.ts > WorkflowAgent stream and core generate contracts > applies stream settings after prepareCall and constructor defaults
func TestSDKRunStreamSettingsPrecedence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		model := sdkRunNewModel(sdkRunTextResponse("Done."))
		sdkRunSetup(model)
		temperature, topP, maxTokens := 0.1, 0.9, 10
		testOptions.Temperature, testOptions.TopP, testOptions.MaxOutputTokens = &temperature, &topP, &maxTokens
		testOptions.PrepareCall = func(_ context.Context, options workflowai.PrepareCallOptions) (workflowai.PrepareCallResult, error) {
			sdkRunEvents.add(fmt.Sprint(*options.Temperature))
			prepared, preparedMax := 0.2, 20
			return workflowai.PrepareCallResult{GenerationSettings: workflowai.GenerationSettings{Temperature: &prepared, MaxOutputTokens: &preparedMax}}, nil
		}
		out := sdkRunStream(t, "What is the weather in London?")
		if out.err != nil {
			t.Fatal(out.err)
		}
		if got := sdkRunEvents.list(); !slices.Equal(got, []string{"0.1"}) {
			t.Fatalf("prepared temperatures = %v", got)
		}
		call := model.all()[0]
		if *call.MaxTokens != 20 || *call.TopP != 0.9 {
			t.Fatalf("model call = %+v", call)
		}
	})
	t.Run("call temperature", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			model := sdkRunNewModel(sdkRunTextResponse("Done."))
			sdkRunSetup(model)
			prepared, call := 0.2, 0.3
			testOptions.PrepareCall = func(context.Context, workflowai.PrepareCallOptions) (workflowai.PrepareCallResult, error) {
				return workflowai.PrepareCallResult{GenerationSettings: workflowai.GenerationSettings{Temperature: &prepared}}, nil
			}
			testCallOptions.Temperature = &call
			if out := sdkRunStream(t, "What is the weather in London?"); out.err != nil {
				t.Fatal(out.err)
			}
			if temperature := model.all()[0].Temperature; temperature == nil || *temperature != call {
				t.Fatalf("temperature = %v", temperature)
			}
		})
	})
}

// AI SDK: packages/workflow/src/workflow-agent-contract.test.ts > WorkflowAgent stream and core generate contracts > composes constructor and call callbacks around model and tool execution
func TestSDKRunComposedCallbacks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkRunSetup(sdkRunNewModel(sdkRunToolResponse("weather-call"), sdkRunTextResponse("Done.")))
		testOptions.Tools = workflowai.ToolSet{"weather": {
			InputSchema: sdkRunCitySchema(),
			Execute: workflowai.ExecuteFunc(func(context.Context, json.RawMessage, workflowai.ToolCallOptions) (any, error) {
				sdkRunEvents.add("execute")
				return map[string]any{"temperature": 18}, nil
			}),
		}}
		testOptions.Callbacks = sdkRunCallbacks("constructor")
		testCallOptions.Callbacks = sdkRunCallbacks("call")
		out := sdkRunStream(t, "What is the weather in London?")
		if out.err != nil {
			t.Fatal(out.err)
		}
		want := []string{
			"constructor:start", "call:start",
			"constructor:step-start", "call:step-start",
			"constructor:tool-start", "call:tool-start",
			"execute",
			"constructor:tool-end", "call:tool-end",
			"constructor:step-end", "call:step-end",
			"constructor:step-start", "call:step-start",
			"constructor:step-end", "call:step-end",
			"constructor:end", "call:end",
		}
		if got := sdkRunEvents.list(); !slices.Equal(got, want) {
			t.Fatalf("events = %v, want %v", got, want)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-contract.test.ts > WorkflowAgent error and abort callbacks > calls a constructor onError callback like ToolLoopAgent, before the call callback
func TestSDKRunErrorCallbacks(t *testing.T) {
	var messages []string
	synctest.Test(t, func(t *testing.T) {
		for _, generate := range []bool{true, false} {
			sdkRunSetup(sdkRunNewModel(sdkRunResponse{err: errors.New("model exploded")}))
			maxRetries := 0
			testOptions.MaxRetries = &maxRetries
			testOptions.OnError = func(_ context.Context, err error) { sdkRunEvents.add("constructor: " + err.Error()) }
			testCallOptions.OnError = func(_ context.Context, err error) { sdkRunEvents.add("call: " + err.Error()) }
			out := sdkRunDo(t, sdkRunInput{Generate: generate, Prompt: "What is the weather in London?"})
			if out.err == nil || !strings.Contains(out.err.Error(), "model exploded") {
				t.Fatalf("generate %v: err = %v", generate, out.err)
			}
			got := sdkRunEvents.list()
			if len(got) != 2 || !strings.HasPrefix(got[0], "constructor: ") || !strings.HasPrefix(got[1], "call: ") || !strings.Contains(got[1], "model exploded") {
				t.Fatalf("generate %v: onError calls = %v", generate, got)
			}
			messages = append(messages, got...)
		}
	})
	t.Run("model error", func(t *testing.T) {
		for _, message := range messages {
			if !strings.HasSuffix(message, ": model exploded") || strings.Count(message, ":") != 1 {
				t.Fatalf("onError calls = %v", messages)
			}
		}
	})
}

// Callbacks return nothing in Go, so they cannot throw; the abort callbacks
// of an already cancelled call are checked here.
//
// AI SDK: packages/workflow/src/workflow-agent-contract.test.ts > WorkflowAgent error and abort callbacks > calls constructor and call onAbort callbacks and ignores errors they throw
func TestSDKRunAbortCallbacks(t *testing.T) {
	var err error
	synctest.Test(t, func(t *testing.T) {
		sdkRunSetup(sdkRunNewModel(sdkRunTextResponse("Unused.")))
		sdkRunCancelBefore(nil)
		testOptions.OnAbort = func(_ context.Context, event workflowai.AbortEvent) {
			sdkRunEvents.add(fmt.Sprintf("constructor: %d steps", len(event.Steps)))
		}
		testCallOptions.OnAbort = func(_ context.Context, event workflowai.AbortEvent) {
			sdkRunEvents.add(fmt.Sprintf("call: %d steps", len(event.Steps)))
		}
		out := sdkRunStream(t, "What is the weather in London?")
		err = out.err
		if got := sdkRunEvents.list(); !slices.Equal(got, []string{"constructor: 0 steps", "call: 0 steps"}) {
			t.Fatalf("onAbort calls = %v", got)
		}
		if len(out.result.Steps) != 0 {
			t.Fatalf("steps = %+v", out.result.Steps)
		}
	})
	sdkRunResolvesAbort(t, err)
}

// AI SDK: packages/workflow/src/workflow-agent-contract.test.ts > WorkflowAgent stream completion contract > forwards raw chunks when requested: %s
func TestSDKRunRawChunks(t *testing.T) {
	for _, include := range []bool{false, true} {
		t.Run(fmt.Sprint(include), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				chunks := []provider.StreamChunk{
					{Type: provider.ChunkTypeRaw, Raw: map[string]any{"provider": "fixture", "delta": "Done."}},
					{Type: provider.ChunkTypeTextStart, ID: "text"},
					{Type: provider.ChunkTypeText, ID: "text", Text: "Done."},
					{Type: provider.ChunkTypeTextEnd, ID: "text"},
					{Type: provider.ChunkTypeFinish, FinishReason: types.FinishReasonStop},
				}
				// A provider sends raw chunks only when asked to.
				if !include {
					chunks = chunks[1:]
				}
				model := sdkRunNewModel(sdkRunResponse{chunks: chunks})
				sdkRunSetup(model)
				testCallOptions.IncludeRawChunks = include
				out := sdkRunStream(t, "What is the weather in London?")
				if out.err != nil {
					t.Fatal(out.err)
				}
				if model.all()[0].IncludeRawChunks != include {
					t.Fatalf("includeRawChunks = %v", model.all()[0].IncludeRawChunks)
				}
				var raw []string
				var text []string
				for _, part := range out.parts {
					switch part.Type {
					case "raw":
						raw = append(raw, string(part.RawValue))
					case "text-start", "text-delta", "text-end", "finish":
						text = append(text, part.Type)
					}
				}
				wantRaw := []string(nil)
				if include {
					wantRaw = []string{`{"delta":"Done.","provider":"fixture"}`}
				}
				if !slices.Equal(raw, wantRaw) || !slices.Equal(text, []string{"text-start", "text-delta", "text-end", "finish"}) {
					t.Fatalf("raw = %v, parts = %v", raw, out.partTypes())
				}
			})
		})
	}
}

// AI SDK: packages/workflow/src/workflow-agent-contract.test.ts > WorkflowAgent stream completion contract > paused=${paused}, sendFinish=${sendFinish}, preventClose=${preventClose}
func TestSDKRunStreamCompletion(t *testing.T) {
	for _, paused := range []bool{false, true} {
		for _, sendFinish := range []bool{false, true} {
			for _, preventClose := range []bool{false, true} {
				t.Run(fmt.Sprintf("paused=%v,sendFinish=%v,preventClose=%v", paused, sendFinish, preventClose), func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						response := sdkRunTextResponse("Done.")
						if paused {
							response = sdkRunToolResponse("weather-call")
						}
						sdkRunSetup(sdkRunNewModel(response))
						testOptions.Tools = workflowai.ToolSet{"weather": {InputSchema: sdkRunCitySchema()}}
						testCallOptions.SendFinish = &sendFinish
						testCallOptions.PreventClose = preventClose
						out := sdkRunStream(t, "What is the weather in London?")
						if out.err != nil {
							t.Fatal(out.err)
						}
						finishes := 0
						for _, part := range out.parts {
							if part.Type == "finish" {
								finishes++
							}
						}
						if want := map[bool]int{false: 0, true: 1}[sendFinish]; finishes != want {
							t.Fatalf("%d finish parts in %v", finishes, out.partTypes())
						}
						if sendFinish && out.parts[len(out.parts)-1].Type != "finish" {
							t.Fatalf("parts = %v", out.partTypes())
						}
						if out.closed != !preventClose {
							t.Fatalf("closed = %v", out.closed)
						}
					})
				})
			}
		}
	}
}

// workflowai has no separate system setting, so Instructions stands for both.
//
// AI SDK: packages/workflow/src/workflow-agent-prepare-step.test.ts > WorkflowAgent prepareStep results > instructions > applies string instructions returned at step 0, like ToolLoopAgent
// AI SDK: packages/workflow/src/workflow-agent-prepare-step.test.ts > WorkflowAgent prepareStep results > instructions > applies system returned at step 0, like ToolLoopAgent
func TestSDKRunPrepareStepInstructions(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := sdkRunPrepareStepCalls(t, func() {
			testOptions.Instructions = "Initial instructions"
			testOptions.PrepareStep = sdkRunAtStepZero(workflowai.PrepareStepResult{Instructions: "Step instructions"})
		})
		for mode, calls := range calls {
			if len(calls) != 2 {
				t.Fatalf("%s: %d model calls", mode, len(calls))
			}
			for i, call := range calls {
				if got := sdkRunSystem(call); !slices.Equal(got, []string{"Step instructions"}) {
					t.Fatalf("%s: call %d system = %v", mode, i, got)
				}
			}
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-prepare-step.test.ts > WorkflowAgent prepareStep results > instructions > applies system message instructions with providerOptions returned at step 0, like ToolLoopAgent
func TestSDKRunPrepareStepSystemMessage(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := sdkRunPrepareStepCalls(t, func() {
			testOptions.Instructions = "Initial instructions"
			testOptions.PrepareStep = sdkRunAtStepZero(workflowai.PrepareStepResult{InstructionMessages: []workflowai.ModelMessage{workflowai.ModelMessage{Role: workflowai.RoleSystem, Content: []workflowai.ContentPart{{Type: workflowai.PartText, Text: "Step instructions"}}, ProviderOptions: map[string]any{"test": map[string]any{"value": "step"}}}}})
		})
		for mode, calls := range calls {
			for i, call := range calls {
				system := call.Prompt.Messages[0]
				if call.Prompt.System != "" || system.Role != types.RoleSystem || system.Content[0].(types.TextContent).Text != "Step instructions" || system.ProviderOptions["test"] == nil {
					t.Fatalf("%s: call %d prompt = %+v", mode, i, call.Prompt)
				}
			}
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-prepare-step.test.ts > WorkflowAgent prepareStep results > instructions > applies array instructions returned at step 0, like ToolLoopAgent
func TestSDKRunPrepareStepInstructionArray(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := sdkRunPrepareStepCalls(t, func() {
			testOptions.Instructions = "Initial instructions"
			testOptions.PrepareStep = sdkRunAtStepZero(workflowai.PrepareStepResult{InstructionMessages: []workflowai.ModelMessage{
				{Role: workflowai.RoleSystem, Content: []workflowai.ContentPart{{Type: workflowai.PartText, Text: "Step instructions 1"}}, ProviderOptions: map[string]any{"test": map[string]any{"value": "step 1"}}},
				{Role: workflowai.RoleSystem, Content: []workflowai.ContentPart{{Type: workflowai.PartText, Text: "Step instructions 2"}}},
			}})
		})
		for mode, calls := range calls {
			for i, call := range calls {
				if got := sdkRunSystem(call); !slices.Equal(got, []string{"Step instructions 1", "Step instructions 2"}) {
					t.Fatalf("%s: call %d system = %v", mode, i, got)
				}
			}
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-prepare-step.test.ts > WorkflowAgent prepareStep results > instructions > replaces initial instructions that are an array of messages, like ToolLoopAgent
func TestSDKRunPrepareStepReplacesInstructionMessages(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := sdkRunPrepareStepCalls(t, func() {
			testOptions.InstructionMessages = []workflowai.ModelMessage{
				{Role: workflowai.RoleSystem, Content: []workflowai.ContentPart{{Type: workflowai.PartText, Text: "Initial instructions 1"}}, ProviderOptions: map[string]any{"test": map[string]any{"value": "initial 1"}}},
				{Role: workflowai.RoleSystem, Content: []workflowai.ContentPart{{Type: workflowai.PartText, Text: "Initial instructions 2"}}},
			}
			testOptions.PrepareStep = sdkRunAtStepZero(workflowai.PrepareStepResult{InstructionMessages: []workflowai.ModelMessage{
				{Role: workflowai.RoleSystem, Content: []workflowai.ContentPart{{Type: workflowai.PartText, Text: "Step instructions 1"}}},
				{Role: workflowai.RoleSystem, Content: []workflowai.ContentPart{{Type: workflowai.PartText, Text: "Step instructions 2"}}},
			}})
		})
		for mode, calls := range calls {
			for i, call := range calls {
				if got := sdkRunSystem(call); !slices.Equal(got, []string{"Step instructions 1", "Step instructions 2"}) {
					t.Fatalf("%s: call %d system = %v", mode, i, got)
				}
			}
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-prepare-step.test.ts > WorkflowAgent prepareStep results > instructions > passes the instructions in effect to prepareStep like ToolLoopAgent
func TestSDKRunPrepareStepSeesInstructions(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkRunPrepareStepCalls(t, func() {
			testOptions.Instructions = "Initial instructions"
			testOptions.PrepareStep = func(_ context.Context, info workflowai.PrepareStepInfo) (workflowai.PrepareStepResult, error) {
				sdkRunEvents.add(fmt.Sprintf("%d:%s", info.StepNumber, info.Instructions))
				if info.StepNumber == 0 {
					return workflowai.PrepareStepResult{Instructions: "Step instructions"}, nil
				}
				return workflowai.PrepareStepResult{}, nil
			}
		})
		// Replays call PrepareStep again, so check each step's last call.
		seen := map[string]string{}
		for _, event := range sdkRunEvents.list() {
			step, instructions, _ := strings.Cut(event, ":")
			seen[step] = instructions
		}
		if seen["0"] != "Initial instructions" || seen["1"] != "Step instructions" {
			t.Fatalf("instructions = %v", seen)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-prepare-step.test.ts > WorkflowAgent prepareStep results > providerOptions > merges providerOptions returned at step 0, like ToolLoopAgent
func TestSDKRunPrepareStepProviderOptions(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := sdkRunPrepareStepCalls(t, func() {
			testOptions.ProviderOptions = map[string]any{
				"mock":      map[string]any{"fromAgent": "agent", "shared": "agent"},
				"agentOnly": map[string]any{"value": "agent"},
			}
			testOptions.PrepareStep = sdkRunAtStepZero(workflowai.PrepareStepResult{GenerationSettings: workflowai.GenerationSettings{
				ProviderOptions: map[string]any{"mock": map[string]any{"fromStep": "step", "shared": "step"}},
			}})
		})
		want := []string{
			`{"agentOnly":{"value":"agent"},"mock":{"fromAgent":"agent","fromStep":"step","shared":"step"}}`,
			`{"agentOnly":{"value":"agent"},"mock":{"fromAgent":"agent","shared":"agent"}}`,
		}
		for mode, calls := range calls {
			for i, call := range calls {
				if got, _ := json.Marshal(call.ProviderOptions); string(got) != want[i] {
					t.Fatalf("%s: call %d provider options = %s, want %s", mode, i, got, want[i])
				}
			}
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-prepare-step.test.ts > WorkflowAgent prepareStep results > headers > merges headers returned at step 0 over the call headers
func TestSDKRunPrepareStepHeaders(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := sdkRunPrepareStepCalls(t, func() {
			testOptions.Headers = map[string]string{"x-agent": "agent"}
			testOptions.PrepareStep = sdkRunAtStepZero(workflowai.PrepareStepResult{GenerationSettings: workflowai.GenerationSettings{Headers: map[string]string{"x-step": "step"}}})
		})
		for mode, calls := range calls {
			first, second := calls[0].Headers, calls[1].Headers
			if first["x-agent"] != "agent" || first["x-step"] != "step" || !strings.Contains(first["user-agent"], "ai-sdk-agent/workflow") {
				t.Fatalf("%s: first headers = %v", mode, first)
			}
			if _, ok := second["x-step"]; ok || second["x-agent"] != "agent" || !strings.Contains(second["user-agent"], "ai-sdk-agent/workflow") {
				t.Fatalf("%s: second headers = %v", mode, second)
			}
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-prepare-step.test.ts > WorkflowAgent prepareStep results > headers > keeps the agent user-agent when prepareStep returns a user-agent
func TestSDKRunPrepareStepUserAgent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := sdkRunPrepareStepCalls(t, func() {
			testOptions.Headers = map[string]string{"x-agent": "agent"}
			testOptions.PrepareStep = sdkRunAtStepZero(workflowai.PrepareStepResult{GenerationSettings: workflowai.GenerationSettings{Headers: map[string]string{"user-agent": "custom"}}})
		})
		for mode, calls := range calls {
			headers := calls[0].Headers
			if !strings.Contains(headers["user-agent"], "custom") || !strings.Contains(headers["user-agent"], "ai-sdk-agent/workflow") || headers["x-agent"] != "agent" {
				t.Fatalf("%s: headers = %v", mode, headers)
			}
		}
	})
}

// The warning is logged by the logDefaultStepLimitWarning step, so the tests
// count those steps.
//
// AI SDK: packages/workflow/src/workflow-agent-default-step-limit.test.ts > default step limit warning > warns once when both agents stop at the default limit
// AI SDK: packages/workflow/src/workflow-agent-default-step-limit.integration.test.ts > stops generate() at the default step limit inside a workflow
func TestSDKRunDefaultStepLimitWarning(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkRunStepLimitSetup()
		out := sdkRunGenerate(t, "Keep calling the weather tool.")
		if out.err != nil || len(out.result.Steps) != 20 || out.count("logDefaultStepLimitWarning") != 1 {
			t.Fatalf("err = %v, %d steps, steps = %v", out.err, len(out.result.Steps), out.steps)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-default-step-limit.test.ts > default step limit warning > does not warn when the model finishes naturally on step 20
func TestSDKRunDefaultStepLimitNaturalFinish(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkRunStepLimitSetup()
		model := testOptions.Model.(*sdkRunModel)
		model.repeat = nil
		for range 19 {
			model.responses = append(model.responses, sdkRunToolResponse("weather-call"))
		}
		model.responses = append(model.responses, sdkRunTextResponse("Done."))
		out := sdkRunGenerate(t, "Keep calling the weather tool.")
		if out.err != nil || len(out.result.Steps) != 20 || out.count("logDefaultStepLimitWarning") != 0 {
			t.Fatalf("err = %v, %d steps, steps = %v", out.err, len(out.result.Steps), out.steps)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-default-step-limit.test.ts > default step limit warning > does not warn when step 20 has only a provider-executed tool call
func TestSDKRunDefaultStepLimitProviderCall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkRunStepLimitSetup()
		model := testOptions.Model.(*sdkRunModel)
		model.repeat = nil
		for range 19 {
			model.responses = append(model.responses, sdkRunToolResponse("weather-call"))
		}
		provided := sdkRunTextResponse("Checked by the provider.")
		provided.content = []types.ContentPart{
			types.ToolCallContent{ToolCallID: "provider-call", ToolName: "weather", Input: `{"city":"London"}`, ProviderExecuted: true},
			types.ToolResultContent{ToolCallID: "provider-call", ToolName: "weather", Result: map[string]any{"temperature": 18}, ProviderExecuted: true},
		}
		provided.finish = types.FinishReasonToolCalls
		model.responses = append(model.responses, provided)
		out := sdkRunGenerate(t, "Keep calling the weather tool.")
		if out.err != nil || len(out.result.Steps) != 20 || out.count("logDefaultStepLimitWarning") != 0 {
			t.Fatalf("err = %v, %d steps, steps = %v", out.err, len(out.result.Steps), out.steps)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-default-step-limit.test.ts > default step limit warning > does not warn for an explicit default limit in the constructor
func TestSDKRunDefaultStepLimitExplicitConstructor(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkRunStepLimitSetup()
		testOptions.StopWhen = []workflowai.StopCondition{workflowai.IsStepCount(20)}
		out := sdkRunGenerate(t, "Keep calling the weather tool.")
		if out.err != nil || len(out.result.Steps) != 20 || out.count("logDefaultStepLimitWarning") != 0 {
			t.Fatalf("err = %v, %d steps, steps = %v", out.err, len(out.result.Steps), out.steps)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-default-step-limit.test.ts > default step limit warning > does not warn for an explicit default limit on the call
func TestSDKRunDefaultStepLimitExplicitCall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkRunStepLimitSetup()
		testCallOptions.StopWhen = []workflowai.StopCondition{workflowai.IsStepCount(20)}
		out := sdkRunGenerate(t, "Keep calling the weather tool.")
		if out.err != nil || len(out.result.Steps) != 20 || out.count("logDefaultStepLimitWarning") != 0 {
			t.Fatalf("err = %v, %d steps, steps = %v", out.err, len(out.result.Steps), out.steps)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-default-step-limit.test.ts > default step limit warning > does not warn when prepareCall provides a stop condition
func TestSDKRunDefaultStepLimitPrepareCall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkRunStepLimitSetup()
		testOptions.PrepareCall = func(context.Context, workflowai.PrepareCallOptions) (workflowai.PrepareCallResult, error) {
			return workflowai.PrepareCallResult{StopWhen: []workflowai.StopCondition{workflowai.IsStepCount(20)}}, nil
		}
		out := sdkRunGenerate(t, "Keep calling the weather tool.")
		if out.err != nil || len(out.result.Steps) != 20 || out.count("logDefaultStepLimitWarning") != 0 {
			t.Fatalf("err = %v, %d steps, steps = %v", out.err, len(out.result.Steps), out.steps)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-default-step-limit.test.ts > default step limit warning > warns when prepareCall returns the default stop condition it received
func TestSDKRunDefaultStepLimitPrepareCallDefault(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkRunStepLimitSetup()
		testOptions.PrepareCall = func(_ context.Context, options workflowai.PrepareCallOptions) (workflowai.PrepareCallResult, error) {
			return workflowai.PrepareCallResult{StopWhen: options.StopWhen}, nil
		}
		out := sdkRunGenerate(t, "Keep calling the weather tool.")
		if out.err != nil || len(out.result.Steps) != 20 || out.count("logDefaultStepLimitWarning") != 1 {
			t.Fatalf("err = %v, %d steps, steps = %v", out.err, len(out.result.Steps), out.steps)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-default-step-limit.test.ts > default step limit warning > does not warn when stream sets an explicit limit
func TestSDKRunDefaultStepLimitStream(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkRunStepLimitSetup()
		testCallOptions.StopWhen = []workflowai.StopCondition{workflowai.IsStepCount(25)}
		out := sdkRunStream(t, "Keep calling the weather tool.")
		if out.err != nil || len(out.result.Steps) != 25 || out.count("logDefaultStepLimitWarning") != 0 {
			t.Fatalf("err = %v, %d steps, steps = %v", out.err, len(out.result.Steps), out.steps)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-concurrency.test.ts > WorkflowAgent.${mode} toolCallConcurrency > ${path}: limits execution to %s and preserves result order
func TestSDKRunToolCallConcurrencyOrder(t *testing.T) {
	for _, mode := range []string{"generate", "stream"} {
		for _, path := range []string{"normal", "paused", "approved"} {
			for _, concurrency := range []int{0, 1, 2} {
				t.Run(fmt.Sprintf("%s/%s/%d", mode, path, concurrency), func(t *testing.T) {
					if path == "approved" {
					}
					synctest.Test(t, func(t *testing.T) {
						var content []types.ContentPart
						if path != "approved" {
							content = sdkRunActionCalls()
						}
						if path == "paused" {
							content = append(content, sdkRunCall("paused", "paused", `{}`))
						}
						sdkRunSetup(sdkRunNewModel(sdkRunResponse{content: content, finish: types.FinishReasonStop}))
						sdkRunGates.reset()
						action := workflowai.Tool{Execute: workflowai.ExecuteStep(sdkRunGatedAction)}
						if path == "approved" {
							action.NeedsApproval = workflowai.NeedsApprovalAlways
						}
						testOptions.Tools = workflowai.ToolSet{"action": action, "paused": {InputSchema: map[string]any{"type": "object"}}}
						testOptions.ToolCallConcurrency = concurrency
						testOptions.StopWhen = []workflowai.StopCondition{workflowai.IsStepCount(1)}
						input := sdkRunInput{Generate: mode == "generate", Messages: []workflowai.ModelMessage{user("Run the tools.")}}
						if path == "approved" {
							input.Messages = sdkRunApprovalMessages()
						}

						world := testworld.New(worlds.Runners(workflow.Use(sdkRunRunner)))
						defer world.Close()
						ctx := workflow.NewContext(t.Context(), workflow.NewClient(world))
						run, err := workflow.Start(ctx, input)
						if err != nil {
							t.Fatal(err)
						}
						var result workflowai.Result
						done := make(chan struct{})
						go func() {
							defer close(done)
							result, err = run.Value(ctx)
						}()
						initial := concurrency
						if initial == 0 {
							initial = len(sdkRunIDs)
						}
						sdkRunGates.waitStarted(t, sdkRunIDs[:initial])
						// Finish the most recently started call first. Each
						// completion opens one slot, and the results keep the
						// model's order.
						if initial == 1 {
							sdkRunGates.release("first")
							sdkRunGates.waitStarted(t, sdkRunIDs[:2])
						}
						sdkRunGates.release("second")
						sdkRunGates.waitStarted(t, sdkRunIDs)
						sdkRunGates.release("third")
						sdkRunGates.release("first")
						<-done
						if err != nil {
							t.Fatal(err)
						}
						var outputs []string
						for _, message := range result.Messages {
							for _, part := range message.Content {
								if message.Role == workflowai.RoleTool && part.Type == workflowai.PartToolResult {
									outputs = append(outputs, part.ToolCallID)
								}
							}
						}
						if !slices.Equal(outputs, sdkRunIDs) {
							t.Fatalf("results = %v", outputs)
						}
					})
				})
			}
		}
	}
}

// AI SDK: packages/workflow/src/workflow-agent-concurrency.test.ts > WorkflowAgent.${mode} toolCallConcurrency > resolves constructor < prepareCall < call: %j
func TestSDKRunToolCallConcurrencyPrecedence(t *testing.T) {
	tests := []struct{ constructor, prepared, call, expected int }{
		{1, 0, 0, 1},
		{1, 2, 0, 2},
		{1, 2, 3, 3},
	}
	for _, mode := range []string{"generate", "stream"} {
		for _, tt := range tests {
			t.Run(fmt.Sprintf("%s/%+v", mode, tt), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					sdkRunSetup(sdkRunNewModel(sdkRunResponse{content: sdkRunActionCalls(), finish: types.FinishReasonStop}))
					sdkRunPeak.reset()
					testOptions.Tools = workflowai.ToolSet{"action": {Execute: workflowai.ExecuteStep(sdkRunPeakAction)}}
					testOptions.StopWhen = []workflowai.StopCondition{workflowai.IsStepCount(1)}
					testOptions.ToolCallConcurrency = tt.constructor
					testOptions.PrepareCall = func(_ context.Context, options workflowai.PrepareCallOptions) (workflowai.PrepareCallResult, error) {
						sdkRunEvents.add(fmt.Sprint(options.ToolCallConcurrency))
						return workflowai.PrepareCallResult{ToolCallConcurrency: tt.prepared}, nil
					}
					testCallOptions.ToolCallConcurrency = tt.call
					out := sdkRunDo(t, sdkRunInput{Generate: mode == "generate", Prompt: "Run tools."})
					if out.err != nil {
						t.Fatal(out.err)
					}
					want := cmp.Or(tt.call, tt.constructor)
					if got := sdkRunEvents.list(); !slices.Equal(got, []string{fmt.Sprint(want)}) {
						t.Fatalf("prepareCall saw %v, want [%d]", got, want)
					}
					if peak := sdkRunPeak.max(); peak != tt.expected {
						t.Fatalf("peak = %d, want %d", peak, tt.expected)
					}
				})
			})
		}
	}
}

// ToolCallConcurrency is an int, so the SDK's 1.5, NaN and Infinity cannot be
// set, and 0 is Go's unset value, which allows any number.
//
// AI SDK: packages/workflow/src/workflow-agent-concurrency.test.ts > WorkflowAgent.${mode} toolCallConcurrency > rejects invalid concurrency %s before executing approved tools or calling the model
func TestSDKRunToolCallConcurrencyInvalid(t *testing.T) {
	for _, mode := range []string{"generate", "stream"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				model := sdkRunNewModel()
				sdkRunSetup(model)
				sdkRunGates.reset()
				testOptions.Tools = workflowai.ToolSet{"action": {NeedsApproval: workflowai.NeedsApprovalAlways, Execute: workflowai.ExecuteStep(sdkRunGatedAction)}}
				testOptions.ToolCallConcurrency = -1
				out := sdkRunDo(t, sdkRunInput{Generate: mode == "generate", Messages: sdkRunApprovalMessages()})
				if out.err == nil || !strings.Contains(out.err.Error(), "ToolCallConcurrency") {
					t.Fatalf("err = %v", out.err)
				}
				if started := sdkRunGates.list(); len(started) != 0 || len(model.all()) != 0 {
					t.Fatalf("started = %v after %d model calls", started, len(model.all()))
				}
			})
		})
	}
}

// AI SDK: packages/workflow/src/workflow-agent-concurrency.test.ts > WorkflowAgent.${mode} toolCallConcurrency > continues queued calls after a tool error
func TestSDKRunToolCallConcurrencyAfterError(t *testing.T) {
	for _, mode := range []string{"generate", "stream"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				sdkRunSetup(sdkRunNewModel(sdkRunResponse{content: sdkRunActionCalls(), finish: types.FinishReasonStop}))
				testOptions.Tools = workflowai.ToolSet{"action": {Execute: workflowai.ExecuteFunc(func(_ context.Context, input json.RawMessage, _ workflowai.ToolCallOptions) (any, error) {
					var action struct{ ID string }
					if err := json.Unmarshal(input, &action); err != nil {
						return nil, err
					}
					sdkRunEvents.add(action.ID)
					if action.ID == "first" {
						return nil, errors.New("tool failed")
					}
					return action.ID, nil
				})}}
				testOptions.StopWhen = []workflowai.StopCondition{workflowai.IsStepCount(1)}
				testOptions.ToolCallConcurrency = 1
				out := sdkRunDo(t, sdkRunInput{Generate: mode == "generate", Prompt: "Run tools."})
				if out.err != nil {
					t.Fatal(out.err)
				}
				if got := sdkRunEvents.list(); !slices.Equal(got, sdkRunIDs) {
					t.Fatalf("started = %v", got)
				}
			})
		})
	}
}

// AI SDK: packages/workflow/src/workflow-agent-concurrency.test.ts > WorkflowAgent.${mode} toolCallConcurrency > does not start queued calls after abort
func TestSDKRunToolCallConcurrencyAfterAbort(t *testing.T) {
	for _, mode := range []string{"generate", "stream"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				sdkRunSetup(sdkRunNewModel(sdkRunResponse{content: sdkRunActionCalls(), finish: types.FinishReasonStop}, sdkRunTextResponse("unused")))
				var cancel func(error) error
				sdkRunBefore = func(ctx context.Context) (context.Context, error) {
					cancelCtx, cancelFunc, err := workflow.WithCancelCause(ctx)
					cancel = cancelFunc
					return cancelCtx, err
				}
				testOptions.Tools = workflowai.ToolSet{"action": {Execute: workflowai.ExecuteFunc(func(_ context.Context, input json.RawMessage, _ workflowai.ToolCallOptions) (any, error) {
					var action struct{ ID string }
					if err := json.Unmarshal(input, &action); err != nil {
						return nil, err
					}
					sdkRunEvents.add(action.ID)
					return action.ID, cancel(nil)
				})}}
				testOptions.ToolCallConcurrency = 1
				out := sdkRunDo(t, sdkRunInput{Generate: mode == "generate", Prompt: "Run tools."})
				if !sdkRunAborted(out.err) {
					t.Fatalf("err = %v", out.err)
				}
				if got := sdkRunEvents.list(); !slices.Equal(got, []string{"first"}) {
					t.Fatalf("started = %v", got)
				}
			})
		})
	}
}

// A model call cannot cancel the call's context with a custom reason, so the
// call's context is cancelled with the reason before it starts.
//
// AI SDK: packages/workflow/src/workflow-agent-abort-reason.test.ts > WorkflowAgent abort reasons > reports a generate() abort with a custom reason as an abort
// AI SDK: packages/workflow/src/workflow-agent-abort-reason.test.ts > WorkflowAgent abort reasons > agrees with ToolLoopAgent for a generate() abort with a custom reason
func TestSDKRunGenerateAbortWithReason(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		model := sdkRunNewModel(sdkRunTextResponse("Unused."))
		sdkRunSetup(model)
		sdkRunCancelBefore(errors.New("user cancelled"))
		sdkRunAbortCallbacks()
		out := sdkRunGenerate(t, "What is the weather in London?")
		if out.err == nil || !strings.Contains(out.err.Error(), "user cancelled") {
			t.Fatalf("err = %v", out.err)
		}
		if got := sdkRunEvents.list(); !slices.Equal(got, []string{"constructor abort: 0 steps", "call abort: 0 steps"}) {
			t.Fatalf("callbacks = %v", got)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-abort-reason.test.ts > WorkflowAgent abort reasons > reports a generate() abort without a reason as an abort
func TestSDKRunGenerateAbortWithoutReason(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkRunSetup(sdkRunNewModel(sdkRunResponse{before: sdkRunCancelInCall}))
		sdkRunCancelable()
		sdkRunAbortCallbacks()
		out := sdkRunGenerate(t, "What is the weather in London?")
		if !errors.Is(out.err, context.Canceled) {
			t.Fatalf("err = %v", out.err)
		}
		if got := sdkRunEvents.list(); !slices.Equal(got, []string{"constructor abort: 0 steps", "call abort: 0 steps"}) {
			t.Fatalf("callbacks = %v", got)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-abort-reason.test.ts > WorkflowAgent abort reasons > reports a model error without an abort as a failure
func TestSDKRunGenerateModelErrorIsFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkRunSetup(sdkRunNewModel(sdkRunResponse{err: errors.New("provider unavailable")}))
		sdkRunCancelable()
		maxRetries := 0
		testOptions.MaxRetries = &maxRetries
		sdkRunAbortCallbacks()
		out := sdkRunGenerate(t, "What is the weather in London?")
		if out.err == nil || !strings.Contains(out.err.Error(), "provider unavailable") || sdkRunAborted(out.err) {
			t.Fatalf("err = %v", out.err)
		}
		got := sdkRunEvents.list()
		if len(got) != 2 || !strings.HasPrefix(got[0], "constructor error: ") || !strings.HasPrefix(got[1], "call error: ") {
			t.Fatalf("callbacks = %v", got)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-abort-reason.test.ts > WorkflowAgent abort reasons > agrees with ToolLoopAgent for a stream() abort with a custom reason
// AI SDK: packages/workflow/src/workflow-agent-abort-reason.test.ts > WorkflowAgent abort reasons > resolves a stream() abort with a custom reason like an abort without a reason
func TestSDKRunStreamAbortWithReason(t *testing.T) {
	var errs [2]error
	var events [2][]string
	synctest.Test(t, func(t *testing.T) {
		for i, reason := range []error{errors.New("user cancelled"), nil} {
			sdkRunSetup(sdkRunNewModel(sdkRunTextResponse("Unused.")))
			sdkRunCancelBefore(reason)
			sdkRunAbortCallbacks()
			out := sdkRunStream(t, "What is the weather in London?")
			errs[i], events[i] = out.err, sdkRunEvents.list()
			if len(out.result.Steps) != 0 {
				t.Fatalf("steps = %+v", out.result.Steps)
			}
		}
		if !slices.Equal(events[0], []string{"constructor abort: 0 steps", "call abort: 0 steps"}) || !slices.Equal(events[0], events[1]) {
			t.Fatalf("callbacks = %v", events)
		}
	})
	sdkRunResolvesAbort(t, errors.Join(errs[:]...))
}

// AI SDK: packages/workflow/src/workflow-agent-timeout-contract.test.ts > WorkflowAgent timeout configuration > stream() with totalMs behaves like a numeric timeout
func TestSDKRunStreamTotalTimeout(t *testing.T) {
	var err error
	synctest.Test(t, func(t *testing.T) {
		sdkRunSetup(sdkRunNewModel(sdkRunStalledResponse()))
		testCallOptions.Timeout = 100 * time.Millisecond
		sdkRunAbortCallbacks()
		start := time.Now()
		out := sdkRunStream(t, "What is the weather in London?")
		err = out.err
		if elapsed := time.Since(start); elapsed >= 1500*time.Millisecond {
			t.Fatalf("aborted after %v", elapsed)
		}
		if len(out.result.Steps) != 0 {
			t.Fatalf("steps = %+v", out.result.Steps)
		}
		if got := sdkRunEvents.list(); !slices.Equal(got, []string{"constructor abort: 0 steps", "call abort: 0 steps"}) {
			t.Fatalf("callbacks = %v", got)
		}
	})
	sdkRunResolvesAbort(t, err)
}

// AI SDK: packages/workflow/src/workflow-agent-timeout-contract.test.ts > WorkflowAgent timeout configuration > ends when the provider ignores the abort signal
func TestSDKRunStreamTimeoutIgnoredAbort(t *testing.T) {

	synctest.Test(t, func(t *testing.T) {
		response := sdkRunStalledResponse()
		response.hang = false
		response.before = func(context.Context, *provider.GenerateOptions) error { return nil }
		model := sdkRunNewModel(response)
		model.ignoreAbort = time.Hour
		sdkRunSetup(model)
		testCallOptions.Timeout = 100 * time.Millisecond
		sdkRunAbortCallbacks()
		start := time.Now()
		out := sdkRunStream(t, "What is the weather in London?")
		if elapsed := time.Since(start); elapsed >= 1500*time.Millisecond {
			t.Fatalf("aborted after %v", elapsed)
		}
		if len(out.result.Steps) != 0 || !slices.Contains(sdkRunEvents.list(), "call abort: 0 steps") {
			t.Fatalf("steps = %+v, callbacks = %v", out.result.Steps, sdkRunEvents.list())
		}
		// The stream's own read ends only when it stops ignoring the abort.
		time.Sleep(model.ignoreAbort)
	})
}

// The SDK rejects with a TimeoutError, and Generate with
// context.DeadlineExceeded.
//
// AI SDK: packages/workflow/src/workflow-agent-timeout-contract.test.ts > WorkflowAgent timeout configuration > rejects generate() with a total timeout in both agents
func TestSDKRunGenerateTotalTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkRunSetup(sdkRunNewModel(sdkRunResponse{hang: true}))
		testCallOptions.Timeout = 100 * time.Millisecond
		out := sdkRunGenerate(t, "What is the weather in London?")
		if !errors.Is(out.err, context.DeadlineExceeded) {
			t.Fatalf("err = %v", out.err)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-timeout-contract.test.ts > WorkflowAgent timeout configuration > ends stream() quickly when an external signal aborts before a long step timeout
func TestSDKRunStreamExternalAbortBeforeTimeout(t *testing.T) {
	var err error
	synctest.Test(t, func(t *testing.T) {
		response := sdkRunStalledResponse()
		response.hang = false
		response.before = func(ctx context.Context, options *provider.GenerateOptions) error {
			time.Sleep(50 * time.Millisecond)
			return sdkRunCancelInCall(ctx, options)
		}
		sdkRunSetup(sdkRunNewModel(response))
		sdkRunCancelable()
		testCallOptions.Timeout = 5 * time.Second
		sdkRunAbortCallbacks()
		start := time.Now()
		out := sdkRunStream(t, "What is the weather in London?")
		err = out.err
		if elapsed := time.Since(start); elapsed >= 1500*time.Millisecond {
			t.Fatalf("aborted after %v", elapsed)
		}
		if len(out.result.Steps) != 0 || !slices.Equal(sdkRunEvents.list(), []string{"constructor abort: 0 steps", "call abort: 0 steps"}) {
			t.Fatalf("steps = %+v, callbacks = %v", out.result.Steps, sdkRunEvents.list())
		}
	})
	sdkRunResolvesAbort(t, err)
}

// AI SDK: packages/workflow/src/workflow-agent-timeout-contract.test.ts > WorkflowAgent timeout configuration > timeout validation > treats %s as a deadline that has passed
func TestSDKRunNegativeTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		model := sdkRunNewModel(sdkRunTextResponse("Unused."))
		sdkRunSetup(model)
		testCallOptions.Timeout = -time.Millisecond
		sdkRunAbortCallbacks()
		out := sdkRunStream(t, "What is the weather in London?")
		if len(out.result.Steps) != 0 || len(model.all()) != 0 || !slices.Contains(sdkRunEvents.list(), "call abort: 0 steps") {
			t.Fatalf("steps = %+v, model calls = %d, callbacks = %v", out.result.Steps, len(model.all()), sdkRunEvents.list())
		}

		model = sdkRunNewModel(sdkRunTextResponse("Unused."))
		sdkRunSetup(model)
		testCallOptions.Timeout = -time.Millisecond
		out = sdkRunGenerate(t, "What is the weather in London?")
		if !errors.Is(out.err, context.DeadlineExceeded) || len(model.all()) != 0 {
			t.Fatalf("err = %v after %d model calls", out.err, len(model.all()))
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-timeout-contract.test.ts > WorkflowAgent timeout configuration > timeout validation > accepts a fractional total timeout in stream()
func TestSDKRunFractionalTimeout(t *testing.T) {
	var err error
	synctest.Test(t, func(t *testing.T) {
		sdkRunSetup(sdkRunNewModel(sdkRunStalledResponse()))
		testCallOptions.Timeout = 100500 * time.Microsecond
		sdkRunAbortCallbacks()
		out := sdkRunStream(t, "What is the weather in London?")
		err = out.err
		if len(out.result.Steps) != 0 || !slices.Equal(sdkRunEvents.list(), []string{"constructor abort: 0 steps", "call abort: 0 steps"}) {
			t.Fatalf("steps = %+v, callbacks = %v", out.result.Steps, sdkRunEvents.list())
		}
	})
	sdkRunResolvesAbort(t, err)
}

// AI SDK: packages/workflow/src/workflow-agent-e2e.integration.test.ts > core > generates through durable model and tool steps (repair: %s)
func TestSDKRunE2EGenerate(t *testing.T) {
	for _, repair := range []bool{false, true} {
		t.Run(fmt.Sprint(repair), func(t *testing.T) {
			var result workflowai.Result
			synctest.Test(t, func(t *testing.T) {
				input := `{"a":3,"b":7}`
				if repair {
					input = "{"
				}
				sdkRunSetup(sdkRunNewModel(sdkRunE2EResponse(sdkRunCall("call-1", "addNumbers", input)), sdkRunE2EResponse(types.TextContent{Text: "The sum is 10"})))
				testOptions.Tools = sdkRunAddTools()
				testOptions.OnStepEnd = func(context.Context, workflowai.StepResult) { sdkRunEvents.add("step") }
				testOptions.OnEnd = func(context.Context, workflowai.EndEvent) { sdkRunEvents.add("end") }
				testCallOptions.RepairToolCall = sdkRunRepairAdd
				out := sdkRunGenerate(t, "Add 3 and 7")
				if out.err != nil {
					t.Fatal(out.err)
				}
				result = out.result
				if result.Text != "The sum is 10" || len(result.Steps) != 2 || string(result.Steps[0].ToolResults[0].Output) != "10" {
					t.Fatalf("result = %+v", result)
				}
				if got := sdkRunEvents.list(); !slices.Equal(got, []string{"step", "step", "end"}) {
					t.Fatalf("events = %v", got)
				}
				if got := sdkRunRoles(result.Messages[1:]); !slices.Equal(got, []string{"assistant", "tool", "assistant"}) {
					t.Fatalf("response roles = %v", got)
				}
				if *result.TotalUsage.InputTokens != 10 || *result.TotalUsage.OutputTokens != 20 {
					t.Fatalf("usage = %+v", result.TotalUsage)
				}
			})
			t.Run("default text output", func(t *testing.T) {
				if result.Output != "The sum is 10" {
					t.Fatalf("output = %#v", result.Output)
				}
			})
		})
	}
}

// AI SDK: packages/workflow/src/workflow-agent-e2e.integration.test.ts > core > basic text response
// AI SDK: packages/workflow/src/workflow-agent-e2e.integration.test.ts > instructions > string instructions are passed to the model
func TestSDKRunE2EText(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		model := sdkRunNewModel(sdkRunE2EResponse(types.TextContent{Text: "Echo: hello world"}))
		sdkRunSetup(model)
		testOptions.Instructions = "You are a pirate."
		out := sdkRunStream(t, "hello world")
		if out.err != nil || len(out.result.Steps) != 1 || out.result.Steps[0].Text != "Echo: hello world" {
			t.Fatalf("err = %v, result = %+v", out.err, out.result)
		}
		if system := model.all()[0].Prompt.System; system != "You are a pirate." {
			t.Fatalf("system = %q", system)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-e2e.integration.test.ts > core > retries model calls within one durable step attempt
// AI SDK: packages/workflow/src/workflow-agent-boundaries.integration.test.ts > generate runtime boundaries > retries the SDK call within one Workflow step attempt
func TestSDKRunRetriesWithinStepAttempt(t *testing.T) {
	for _, mode := range []string{"stream", "generate"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				failure := sdkRunAttemptResponse(sdkRunResponse{err: sdkRunRetryable("0")})
				success := sdkRunAttemptResponse(sdkRunE2EResponse(types.TextContent{Text: "Recovered."}))
				sdkRunSetup(sdkRunNewModel(failure, failure, success))
				maxRetries := 2
				testOptions.MaxRetries = &maxRetries
				out := sdkRunDo(t, sdkRunInput{Generate: mode == "generate", Prompt: "retry the model call"})
				if out.err != nil || out.result.Text != "Recovered." {
					t.Fatalf("err = %v, result = %+v", out.err, out.result)
				}
				if got := sdkRunEvents2.list(); !slices.Equal(got, []string{"attempt 1", "attempt 1", "attempt 1"}) {
					t.Fatalf("model calls = %v", got)
				}
				if steps := out.count("doStreamStep") + out.count("doGenerateStep"); steps != 1 {
					t.Fatalf("steps = %v", out.steps)
				}
			})
		})
	}
}

// AI SDK: packages/workflow/src/workflow-agent-e2e.integration.test.ts > core > surfaces stream error data without retrying the model step
func TestSDKRunE2EStreamError(t *testing.T) {
	var result workflowai.Result
	synctest.Test(t, func(t *testing.T) {
		model := sdkRunNewModel(sdkRunResponse{chunks: []provider.StreamChunk{{Type: provider.ChunkTypeError, Text: "safe-terminal-classification"}}})
		sdkRunSetup(model)
		testCallOptions.OnError = func(_ context.Context, err error) { sdkRunEvents.add(err.Error()) }
		out := sdkRunStream(t, "trigger the terminal error")
		if out.err != nil {
			t.Fatal(out.err)
		}
		result = out.result
		if result.Error != "safe-terminal-classification" || len(result.Steps) != 1 || len(model.all()) != 1 {
			t.Fatalf("result = %+v after %d model calls", result, len(model.all()))
		}
		if got := sdkRunEvents.list(); !slices.Equal(got, []string{"safe-terminal-classification"}) {
			t.Fatalf("onError calls = %v", got)
		}
		errorParts := 0
		for _, part := range out.parts {
			if part.Type == "error" {
				errorParts++
			}
		}
		if errorParts != 1 {
			t.Fatalf("parts = %v", out.partTypes())
		}
	})
	t.Run("finish reason", func(t *testing.T) {
		if result.FinishReason != "error" {
			t.Fatalf("finish reason = %q", result.FinishReason)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-e2e.integration.test.ts > core > single tool call
// AI SDK: packages/workflow/src/workflow-agent-e2e.integration.test.ts > tool input schema serialization > tools with zod input schemas work across step boundaries
func TestSDKRunE2EToolCall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// A step tool derives its schema from its input type, and a
		// workflow-context tool declares it.
		inline := workflowai.ToolSet{"addNumbers": {
			Description: "Add two numbers",
			InputSchema: map[string]any{"type": "object", "properties": map[string]any{"a": map[string]any{"type": "number"}, "b": map[string]any{"type": "number"}}, "required": []any{"a", "b"}},
			Execute: workflowai.ExecuteFunc(func(_ context.Context, input json.RawMessage, _ workflowai.ToolCallOptions) (any, error) {
				var add sdkRunAddInput
				err := json.Unmarshal(input, &add)
				return add.A + add.B, err
			}),
		}}
		for _, tools := range []workflowai.ToolSet{sdkRunAddTools(), inline} {
			model := sdkRunNewModel(sdkRunE2EResponse(sdkRunCall("call-1", "addNumbers", `{"a":3,"b":7}`)), sdkRunE2EResponse(types.TextContent{Text: "The sum is 10"}))
			sdkRunSetup(model)
			testOptions.Tools = tools
			testOptions.Instructions = "You are a calculator assistant."
			out := sdkRunStream(t, "Add 3 and 7")
			if out.err != nil || len(out.result.Steps) != 2 || out.result.Text != "The sum is 10" {
				t.Fatalf("err = %v, result = %+v", out.err, out.result)
			}
			if schema := model.all()[0].Tools[0].Parameters; schema == nil {
				t.Fatal("tool schema not sent")
			}
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-e2e.integration.test.ts > core > multiple sequential tool calls
func TestSDKRunE2EMultiStep(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var responses []sdkRunResponse
		for step := 1; step <= 3; step++ {
			responses = append(responses, sdkRunE2EResponse(sdkRunCall(fmt.Sprintf("call-%d", step), "echoStep", fmt.Sprintf(`{"step":%d}`, step))))
		}
		sdkRunSetup(sdkRunNewModel(append(responses, sdkRunE2EResponse(types.TextContent{Text: "All done!"}))...))
		testOptions.Tools = workflowai.ToolSet{"echoStep": {Description: "Echo the step number", Execute: workflowai.ExecuteStep(sdkRunEchoStep)}}
		out := sdkRunStream(t, "Run 3 steps")
		if out.err != nil || len(out.result.Steps) != 4 || out.result.Text != "All done!" || out.count("sdkRunEchoStep") != 3 {
			t.Fatalf("err = %v, result = %+v, steps = %v", out.err, out.result, out.steps)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-e2e.integration.test.ts > core > tool error recovery
func TestSDKRunE2EToolError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkRunSetup(sdkRunNewModel(sdkRunE2EResponse(sdkRunCall("call-1", "throwingTool", `{}`)), sdkRunE2EResponse(types.TextContent{Text: "Tool failed but I recovered."})))
		testOptions.Tools = workflowai.ToolSet{"throwingTool": {Description: "A tool that always fails", Execute: workflowai.ExecuteStep(sdkRunThrowingStep)}}
		out := sdkRunStream(t, "Call the throwing tool")
		failed := slices.ContainsFunc(out.result.Steps[0].Content, func(part workflowai.ContentPart) bool {
			return part.Type == workflowai.PartToolError && part.ToolCallID == "call-1"
		})
		if out.err != nil || len(out.result.Steps) != 2 || out.result.Text != "Tool failed but I recovered." || !failed {
			t.Fatalf("err = %v, result = %+v", out.err, out.result)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-e2e.integration.test.ts > repairToolCall > callback survives serialization and repairs malformed tool input
func TestSDKRunE2ERepairToolCall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkRunSetup(sdkRunNewModel(sdkRunE2EResponse(sdkRunCall("call-1", "addNumbers", `{"a": 3, "b": 7`)), sdkRunE2EResponse(types.TextContent{Text: "The sum is 10"})))
		testOptions.Tools = sdkRunAddTools()
		testCallOptions.RepairToolCall = sdkRunRepairAdd
		out := sdkRunStream(t, "add 3 and 7")
		if out.err != nil || len(out.result.Steps) != 2 || out.result.Text != "The sum is 10" || string(out.result.Steps[0].ToolResults[0].Output) != "10" {
			t.Fatalf("err = %v, result = %+v", out.err, out.result)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-e2e.integration.test.ts > onStepFinish > fires constructor + stream callbacks in order with step data
// AI SDK: packages/workflow/src/workflow-agent-e2e.integration.test.ts > onFinish > fires constructor + stream callbacks in order with event data
func TestSDKRunE2EFinishCallbacks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkRunSetup(sdkRunNewModel(sdkRunE2EResponse(types.TextContent{Text: "hello"})))
		testOptions.OnStepEnd = func(context.Context, workflowai.StepResult) { sdkRunEvents.add("step constructor") }
		testCallOptions.OnStepEnd = func(_ context.Context, step workflowai.StepResult) {
			sdkRunEvents.add(fmt.Sprintf("step method %s %s %d", step.Text, step.FinishReason, step.StepNumber))
		}
		testOptions.OnEnd = func(context.Context, workflowai.EndEvent) { sdkRunEvents.add("end constructor") }
		testCallOptions.OnEnd = func(_ context.Context, result workflowai.EndEvent) {
			sdkRunEvents.add(fmt.Sprintf("end method %s %s %d %v %v", result.Text, result.FinishReason, len(result.Steps), len(result.Messages) > 0, result.TotalUsage.TotalTokens != nil))
		}
		out := sdkRunStream(t, "test")
		if out.err != nil || len(out.result.Steps) != 1 {
			t.Fatalf("err = %v, result = %+v", out.err, out.result)
		}
		want := []string{"step constructor", "step method hello stop 0", "end constructor", "end method hello stop 1 true true"}
		if got := sdkRunEvents.list(); !slices.Equal(got, want) {
			t.Fatalf("events = %v, want %v", got, want)
		}
	})
}

// The SDK checks that no sleep is left pending; here the run ends at once,
// without waiting for the Timeout.
//
// AI SDK: packages/workflow/src/workflow-agent-e2e.integration.test.ts > timeout > completes within timeout without leaving a pending sleep
func TestSDKRunE2ETimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkRunSetup(sdkRunNewModel(sdkRunE2EResponse(types.TextContent{Text: "fast response"})))
		testCallOptions.Timeout = 30 * time.Second
		start := time.Now()
		out := sdkRunStream(t, "test")
		if out.err != nil || len(out.result.Steps) != 1 || out.result.Text != "fast response" {
			t.Fatalf("err = %v, result = %+v", out.err, out.result)
		}
		if elapsed := time.Since(start); elapsed != 0 {
			t.Fatalf("run took %v", elapsed)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-e2e.integration.test.ts > experimental_onStart > calls constructor and stream callbacks
// AI SDK: packages/workflow/src/workflow-agent-e2e.integration.test.ts > experimental_onStepStart > calls constructor and stream callbacks
// AI SDK: packages/workflow/src/workflow-agent-e2e.integration.test.ts > onToolExecutionStart > calls constructor and stream callbacks
// AI SDK: packages/workflow/src/workflow-agent-e2e.integration.test.ts > onToolExecutionEnd > calls constructor and stream callbacks with the result
func TestSDKRunE2EStartCallbacks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkRunSetup(sdkRunNewModel(sdkRunE2EResponse(sdkRunCall("call-1", "addNumbers", `{"a":1,"b":2}`)), sdkRunE2EResponse(types.TextContent{Text: "done"})))
		testOptions.Tools = sdkRunAddTools()
		for _, scope := range []string{"constructor", "method"} {
			callbacks := workflowai.Callbacks{
				OnStart: func(context.Context, workflowai.StartEvent) { sdkRunEvents.add("start " + scope) },
				OnStepStart: func(_ context.Context, event workflowai.StepStartEvent) {
					sdkRunEvents.add(fmt.Sprintf("step-start %d %s", event.StepNumber, scope))
				},
				OnToolExecutionStart: func(context.Context, workflowai.ToolExecutionStartEvent) { sdkRunEvents.add("tool-start " + scope) },
				OnToolExecutionEnd: func(_ context.Context, event workflowai.ToolExecutionEndEvent) {
					sdkRunEvents.add(fmt.Sprintf("tool-end %s %s %v %v", scope, event.ToolCall.ToolName, event.Error == nil, event.Output))
				},
			}
			if scope == "constructor" {
				testOptions.Callbacks = callbacks
			} else {
				testCallOptions.Callbacks = callbacks
			}
		}
		out := sdkRunStream(t, "test")
		if out.err != nil {
			t.Fatal(out.err)
		}
		want := []string{
			"start constructor", "start method",
			"step-start 0 constructor", "step-start 0 method",
			"tool-start constructor", "tool-start method",
			"tool-end constructor addNumbers true 3", "tool-end method addNumbers true 3",
			"step-start 1 constructor", "step-start 1 method",
		}
		if got := sdkRunEvents.list(); !slices.Equal(got, want) {
			t.Fatalf("events = %v, want %v", got, want)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-e2e.integration.test.ts > prepareCall (GAP) > completes but prepareCall is not applied (GAP)
func TestSDKRunE2EPrepareCall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkRunSetup(sdkRunNewModel(sdkRunE2EResponse(types.TextContent{Text: "ok"})))
		testOptions.PrepareCall = func(context.Context, workflowai.PrepareCallOptions) (workflowai.PrepareCallResult, error) {
			return workflowai.PrepareCallResult{GenerationSettings: workflowai.GenerationSettings{ProviderOptions: map[string]any{"test": map[string]any{"value": nil}}}}, nil
		}
		out := sdkRunStream(t, "test")
		if out.err != nil || len(out.result.Steps) != 1 || out.result.Text != "ok" {
			t.Fatalf("err = %v, result = %+v", out.err, out.result)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-e2e.integration.test.ts > tool approval > pauses before executing a tool that needs approval
func TestSDKRunE2EToolApproval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkRunSetup(sdkRunNewModel(sdkRunE2EResponse(sdkRunCall("call-1", "riskyTool", `{"action":"delete"}`)), sdkRunE2EResponse(types.TextContent{Text: "done"})))
		testOptions.Tools = sdkRunRiskyTools()
		out := sdkRunStream(t, "do something risky")
		result := out.result
		if out.err != nil || len(result.Steps) != 1 || len(result.ToolCalls) != 1 || len(result.ToolResults) != 0 || result.ToolCalls[0].ToolName != "riskyTool" {
			t.Fatalf("err = %v, result = %+v", out.err, result)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-e2e.integration.test.ts > tool approval > signs an approval and verifies it in a later workflow run
func TestSDKRunE2ESignedApproval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const secret = "workflow-tool-approval-secret-for-tests"
		t.Setenv("WORKFLOW_TOOL_APPROVAL_SECRET", secret)
		configure := func(responses ...sdkRunResponse) {
			sdkRunSetup(sdkRunNewModel(responses...))
			testOptions.Tools = sdkRunRiskyTools()
			testOptions.ToolApprovalSecret = &workflowai.ToolApprovalSecret{EnvironmentVariable: "WORKFLOW_TOOL_APPROVAL_SECRET"}
		}
		configure(sdkRunE2EResponse(sdkRunCall("call-1", "riskyTool", `{"action":"delete"}`)))
		issued := sdkRunStream(t, "do something risky")
		if issued.err != nil || len(issued.result.ToolCalls) != 1 {
			t.Fatalf("err = %v, result = %+v", issued.err, issued.result)
		}
		var request *workflowai.ModelCallStreamPart
		for _, part := range issued.parts {
			if part.Type == "tool-approval-request" {
				request = &part
			}
		}
		if request == nil || request.ApprovalID != "approval-call-1" || request.ToolCallID != "call-1" || request.Signature == "" {
			t.Fatalf("parts = %+v", issued.parts)
		}
		if data, _ := json.Marshal(issued.parts); strings.Contains(string(data), secret) {
			t.Fatal("the stream contains the secret")
		}

		configure(sdkRunE2EResponse(types.TextContent{Text: "approved action done"}))
		resumed := sdkRunDo(t, sdkRunInput{Messages: []workflowai.ModelMessage{
			user("do something risky"),
			{Role: workflowai.RoleAssistant, Content: []workflowai.ContentPart{
				{Type: workflowai.PartToolCall, ToolCallID: "call-1", ToolName: "riskyTool", Input: json.RawMessage(`{"action":"delete"}`)},
				{Type: workflowai.PartToolApprovalRequest, ApprovalID: "approval-call-1", ToolCallID: "call-1", Signature: request.Signature},
			}},
			{Role: workflowai.RoleTool, Content: []workflowai.ContentPart{{Type: workflowai.PartToolApprovalResponse, ApprovalID: "approval-call-1", Approved: true}}},
		}})
		if resumed.err != nil || resumed.result.Text != "approved action done" {
			t.Fatalf("err = %v, result = %+v", resumed.err, resumed.result)
		}
		found := false
		for _, message := range resumed.result.Messages {
			for _, part := range message.Content {
				found = found || part.Type == workflowai.PartToolResult && part.ToolCallID == "call-1" && string(part.Output.Value) == `"executed:delete"`
			}
		}
		if !found {
			t.Fatalf("messages = %+v", resumed.result.Messages)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-e2e.integration.test.ts > runtimeContext + toolsContext > flows through prepareStep, tool execute, and onFinish
func TestSDKRunE2ERuntimeAndToolsContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkRunSetup(sdkRunNewModel(sdkRunE2EResponse(sdkRunCall("call-1", "lookupCustomer", `{"customerId":"cust_123"}`)), sdkRunE2EResponse(types.TextContent{Text: "Customer cust_123 is eligible."})))
		testOptions.Instructions = "You look up customers."
		testOptions.Tools = workflowai.ToolSet{"lookupCustomer": {
			Description: "Look up customer account details.",
			InputSchema: map[string]any{"type": "object"},
			Execute: workflowai.ExecuteFunc(func(_ context.Context, _ json.RawMessage, options workflowai.ToolCallOptions) (any, error) {
				data, _ := json.Marshal(options.Context)
				sdkRunEvents.add("tool " + string(data))
				return map[string]any{"customerId": "cust_123", "eligible": true}, nil
			}),
		}}
		testOptions.RuntimeContext = map[string]any{"tenantId": "tenant_123", "requestId": "req_abc"}
		testOptions.ToolsContext = map[string]any{"lookupCustomer": map[string]any{"apiKey": "sk-test-key", "region": "us"}}
		testOptions.PrepareStep = func(_ context.Context, info workflowai.PrepareStepInfo) (workflowai.PrepareStepResult, error) {
			runtime := maps.Clone(info.RuntimeContext.(map[string]any))
			runtime["lastStep"] = info.StepNumber
			data, _ := json.Marshal(runtime)
			sdkRunEvents.add("prepareStep " + string(data))
			return workflowai.PrepareStepResult{RuntimeContext: runtime}, nil
		}
		out := sdkRunStream(t, "Is customer cust_123 eligible for support?")
		if out.err != nil || len(out.result.Steps) != 2 || out.result.Text != "Customer cust_123 is eligible." {
			t.Fatalf("err = %v, result = %+v", out.err, out.result)
		}
		want := []string{
			`prepareStep {"lastStep":0,"requestId":"req_abc","tenantId":"tenant_123"}`,
			`tool {"apiKey":"sk-test-key","region":"us"}`,
			`prepareStep {"lastStep":1,"requestId":"req_abc","tenantId":"tenant_123"}`,
		}
		if got := sdkRunEvents.list(); !slices.Equal(got, want) {
			t.Fatalf("events = %v, want %v", got, want)
		}
	})
	t.Run("onEnd contexts", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			sdkRunSetup(sdkRunNewModel(sdkRunE2EResponse(sdkRunCall("call-1", "lookupCustomer", `{"customerId":"cust_123"}`)), sdkRunE2EResponse(types.TextContent{Text: "Customer cust_123 is eligible."})))
			testOptions.Tools = workflowai.ToolSet{"lookupCustomer": {InputSchema: map[string]any{"type": "object"}, Execute: workflowai.ExecuteFunc(func(context.Context, json.RawMessage, workflowai.ToolCallOptions) (any, error) {
				return map[string]any{"eligible": true}, nil
			})}}
			testOptions.RuntimeContext = map[string]any{"tenantId": "tenant_123", "requestId": "req_abc"}
			toolsContext := map[string]any{"lookupCustomer": map[string]any{"apiKey": "sk-test-key", "region": "us"}}
			testOptions.ToolsContext = toolsContext
			testOptions.PrepareStep = func(_ context.Context, info workflowai.PrepareStepInfo) (workflowai.PrepareStepResult, error) {
				runtime := maps.Clone(info.RuntimeContext.(map[string]any))
				runtime["lastStep"] = info.StepNumber
				return workflowai.PrepareStepResult{RuntimeContext: runtime}, nil
			}
			var ended workflowai.EndEvent
			testOptions.OnEnd = func(_ context.Context, event workflowai.EndEvent) { ended = event }
			if out := sdkRunStream(t, "Is customer cust_123 eligible for support?"); out.err != nil {
				t.Fatal(out.err)
			}
			want := map[string]any{"tenantId": "tenant_123", "requestId": "req_abc", "lastStep": 1}
			if !reflect.DeepEqual(ended.RuntimeContext, want) || !reflect.DeepEqual(ended.ToolsContext, toolsContext) {
				t.Fatalf("OnEnd contexts = %v, %v", ended.RuntimeContext, ended.ToolsContext)
			}
		})
	})
}

// AI SDK: packages/workflow/src/workflow-agent-boundaries.integration.test.ts > generate runtime boundaries > expires before model dispatch
func TestSDKRunBoundaryExpiresBeforeDispatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		model := sdkRunNewModel(sdkRunAttemptResponse(sdkRunE2EResponse(types.TextContent{Text: "Recovered."})))
		sdkRunSetup(model)
		testCallOptions.Timeout = -time.Millisecond
		out := sdkRunGenerate(t, "Test generation.")
		if !errors.Is(out.err, context.DeadlineExceeded) || len(sdkRunEvents2.list()) != 0 || out.count("doGenerateStep") != 1 {
			t.Fatalf("err = %v, model calls = %v, steps = %v", out.err, sdkRunEvents2.list(), out.steps)
		}
	})
}

// The SDK reports an AbortError, and Generate the Timeout's
// context.DeadlineExceeded.
//
// AI SDK: packages/workflow/src/workflow-agent-boundaries.integration.test.ts > generate runtime boundaries > does not dispatch a retry beyond the absolute deadline
func TestSDKRunBoundaryRetryBeyondDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		failure := sdkRunAttemptResponse(sdkRunResponse{err: sdkRunRetryable("10000")})
		sdkRunSetup(sdkRunNewModel(failure, failure, failure))
		maxRetries := 2
		testOptions.MaxRetries = &maxRetries
		testCallOptions.Timeout = 5 * time.Second
		out := sdkRunGenerate(t, "Test generation.")
		if !errors.Is(out.err, context.DeadlineExceeded) {
			t.Fatalf("err = %v", out.err)
		}
		if got := sdkRunEvents2.list(); !slices.Equal(got, []string{"attempt 1"}) || out.count("doGenerateStep") != 1 {
			t.Fatalf("model calls = %v, steps = %v", got, out.steps)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-boundaries.integration.test.ts > generate runtime boundaries > does not retry provider aborts at either retry layer
func TestSDKRunBoundaryProviderAbort(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		failure := sdkRunAttemptResponse(sdkRunResponse{err: context.Canceled})
		sdkRunSetup(sdkRunNewModel(failure, failure, failure))
		maxRetries := 2
		testOptions.MaxRetries = &maxRetries
		out := sdkRunGenerate(t, "Test generation.")
		if !errors.Is(out.err, context.Canceled) {
			t.Fatalf("err = %v", out.err)
		}
		if got := sdkRunEvents2.list(); !slices.Equal(got, []string{"attempt 1"}) || out.count("doGenerateStep") != 1 {
			t.Fatalf("model calls = %v, steps = %v", got, out.steps)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-boundaries.integration.test.ts > generate runtime boundaries > keeps a hook suspended past the model deadline and blocks model dispatch after resume
func TestSDKRunBoundaryHookPastDeadline(t *testing.T) {

	synctest.Test(t, func(t *testing.T) {
		sdkRunSetup(sdkRunNewModel(sdkRunAttemptResponse(sdkRunE2EResponse(sdkRunCall("wait-call", "wait", `{}`))), sdkRunAttemptResponse(sdkRunE2EResponse(types.TextContent{Text: "unused"}))))
		testOptions.Tools = sdkRunHookTools()
		testCallOptions.Timeout = 5 * time.Second
		world := testworld.New(worlds.Runners(workflow.Use(sdkRunRunner)))
		defer world.Close()
		ctx := workflow.NewContext(t.Context(), workflow.NewClient(world))
		run, err := workflow.Start(ctx, sdkRunInput{Generate: true, Prompt: "Wait for the user."})
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(6 * time.Second)
		synctest.Wait()
		if status, err := run.Status(ctx); err != nil || status != workflow.StatusRunning {
			t.Fatalf("status = %v, err = %v", status, err)
		}
		if steps := stepNames(t, world, run.ID()); slices.Index(steps, "doGenerateStep") < 0 || slices.Contains(steps[slices.Index(steps, "doGenerateStep")+1:], "doGenerateStep") {
			t.Fatalf("steps = %v", steps)
		}
		if err := workflow.ResumeHook(ctx, "wait-hook", "continue"); err != nil {
			t.Fatal(err)
		}
		if _, err := run.Value(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err = %v", err)
		}
		count := 0
		for _, step := range stepNames(t, world, run.ID()) {
			if step == "doGenerateStep" {
				count++
			}
		}
		if got := sdkRunEvents2.list(); count != 2 || !slices.Equal(got, []string{"attempt 1"}) {
			t.Fatalf("%d model call steps, model calls = %v", count, got)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-boundaries.integration.test.ts > generate runtime boundaries > supports cancelling the workflow run while a tool waits on a hook
func TestSDKRunBoundaryCancelWhileHookWaits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkRunSetup(sdkRunNewModel(sdkRunE2EResponse(sdkRunCall("wait-call", "wait", `{}`)), sdkRunE2EResponse(types.TextContent{Text: "unused"})))
		testOptions.Tools = sdkRunHookTools()
		world := testworld.New(worlds.Runners(workflow.Use(sdkRunRunner)))
		defer world.Close()
		ctx := workflow.NewContext(t.Context(), workflow.NewClient(world))
		run, err := workflow.Start(ctx, sdkRunInput{Generate: true, Prompt: "Wait for the user."})
		if err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		before := stepNames(t, world, run.ID())
		if err := run.Cancel(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := run.Value(ctx); err == nil {
			t.Fatal("expected the cancelled run to fail")
		}
		if status, err := run.Status(ctx); err != nil || status != workflow.StatusCancelled {
			t.Fatalf("status = %v, err = %v", status, err)
		}
		if after := stepNames(t, world, run.ID()); !slices.Equal(after, before) {
			t.Fatalf("steps = %v, before = %v", after, before)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-boundaries.integration.test.ts > generate runtime boundaries > returns selected rich result fields after suspension (include: %s)
func TestSDKRunBoundaryRichResult(t *testing.T) {
	for _, include := range []bool{true, false} {
		t.Run(fmt.Sprint(include), func(t *testing.T) {
			var result workflowai.Result
			synctest.Test(t, func(t *testing.T) {
				rich := sdkRunAttemptResponse(sdkRunResponse{
					content: []types.ContentPart{
						types.ReasoningContent{Text: "Reasoning.", ProviderMetadata: json.RawMessage(`{"fixture":{"reasoning":true}}`)},
						types.TextContent{Text: "Answer."},
						types.FileContent{MediaType: "text/plain", Data: []byte("hi")},
						types.SourceContent{SourceType: "url", ID: "source", URL: "https://example.com/source"},
					},
					finish: types.FinishReasonStop,
					usage:  sdkRunUsage(3, 2, 1, 2, 1),
				})
				sdkRunSetup(sdkRunNewModel(rich))
				testCallOptions.Include = &ai.IncludeOptions{RequestBody: include, ResponseBody: include, RequestMessages: include}
				sdkRunAfter = func(ctx context.Context) error {
					hook := workflow.CreateHook[string](ctx, workflow.HookOption{Token: "rich-hook"})
					defer hook.Dispose(ctx)
					_, err := hook.Next(ctx)
					return err
				}
				world := testworld.New(worlds.Runners(workflow.Use(sdkRunRunner)))
				defer world.Close()
				ctx := workflow.NewContext(t.Context(), workflow.NewClient(world))
				run, err := workflow.Start(ctx, sdkRunInput{Generate: true, Prompt: "Return rich content."})
				if err != nil {
					t.Fatal(err)
				}
				synctest.Wait()
				before := stepNames(t, world, run.ID())
				if err := workflow.ResumeHook(ctx, "rich-hook", "continue"); err != nil {
					t.Fatal(err)
				}
				if result, err = run.Value(ctx); err != nil {
					t.Fatal(err)
				}
				if after := stepNames(t, world, run.ID()); slices.Index(after, "doGenerateStep") != slices.Index(before, "doGenerateStep") || strings.Count(strings.Join(after, ","), "doGenerateStep") != 1 {
					t.Fatalf("steps = %v, before = %v", after, before)
				}
				if got := sdkRunEvents2.list(); !slices.Equal(got, []string{"attempt 1"}) {
					t.Fatalf("model calls = %v", got)
				}
				step := result.Steps[0]
				file := step.Content[slices.IndexFunc(step.Content, func(part workflowai.ContentPart) bool { return part.Type == workflowai.PartFile })]
				if result.Text != "Answer." || file.Data != "aGk=" || file.MediaType != "text/plain" {
					t.Fatalf("result = %+v", result)
				}
				if step.Response.ID != "response-id" || step.Response.ModelID != "response-model" {
					t.Fatalf("response = %+v", step.Response)
				}
				usage := result.TotalUsage
				if *usage.InputTokens != 3 || *usage.OutputTokens != 2 || *usage.TotalTokens != 5 || *step.Usage.InputDetails.CacheReadTokens != 1 {
					t.Fatalf("usage = %+v", usage)
				}
				if include != (step.Request.Body == "request-body") || include != (step.Response.Body == "response-body") || include != (len(step.Request.Messages) == 1) {
					t.Fatalf("request = %+v, response body = %v", step.Request, step.Response.Body)
				}
				if len(result.Messages[1:]) != 1 {
					t.Fatalf("response messages = %+v", result.Messages[1:])
				}
			})
			t.Run("sources, output and metadata", func(t *testing.T) {
				var kinds []string
				for _, part := range result.Steps[0].Content {
					kinds = append(kinds, part.Type)
				}
				if !slices.Equal(kinds, []string{"reasoning", "text", "file", "source"}) || result.Output != "Answer." {
					t.Fatalf("content = %v, output = %#v", kinds, result.Output)
				}
			})
		})
	}
}

// AI SDK: packages/workflow/src/workflow-agent-response-format.test.ts > WorkflowAgent.stream structured output > forwards the response format and parses the structured result
func TestSDKRunStreamResponseFormat(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		model := sdkRunNewModel(sdkRunTextResponse(`{"ok":true}`))
		sdkRunSetup(model)
		testOptions.Output = workflowai.OutputFrom(ai.ObjectOutput[sdkRunOK](ai.ObjectOutputOptions{Schema: ai.SchemaFor[sdkRunOK]()}))
		out := sdkRunStream(t, "Return the structured result.")
		if out.err != nil {
			t.Fatal(out.err)
		}
		if output, ok := out.result.Output.(map[string]any); !ok || output["ok"] != true {
			t.Fatalf("output = %#v", out.result.Output)
		}
		format := model.all()[0].ResponseFormat
		if format == nil || format.Type != "json" {
			t.Fatalf("response format = %+v", format)
		}
		schema, _ := json.Marshal(format.Schema)
		var decoded struct {
			Type       string                       `json:"type"`
			Properties map[string]map[string]string `json:"properties"`
			Required   []string                     `json:"required"`
		}
		if err := json.Unmarshal(schema, &decoded); err != nil || decoded.Type != "object" || decoded.Properties["ok"]["type"] != "boolean" || !slices.Equal(decoded.Required, []string{"ok"}) {
			t.Fatalf("schema = %s", schema)
		}
	})
}

// sdkRunOK is the structured output of the SDK's response format test.
type sdkRunOK struct {
	OK bool `json:"ok"`
}

// sdkRunInput is the input of sdkRunRunner. Generate selects Generate over
// Stream, and a non-empty Prompt starts the call from it instead of Messages.
type sdkRunInput struct {
	Generate bool                      `json:"generate,omitempty"`
	Prompt   string                    `json:"prompt,omitempty"`
	Messages []workflowai.ModelMessage `json:"messages,omitempty"`
}

func (sdkRunInput) Workflow(workflowai.Result) {}

var (
	// sdkRunBefore wraps the runner's context before the call, for example to
	// cancel it, and sdkRunAfter runs after the call, for example to wait on
	// a hook.
	sdkRunBefore func(ctx context.Context) (context.Context, error)
	sdkRunAfter  func(ctx context.Context) error
	// sdkRunEvents records what callbacks and workflow-context tools see. The
	// runner resets it, so it holds the events of the run's last replay.
	sdkRunEvents sdkRunLog
)

// sdkRunRunner runs the agent testOptions configures with testCallOptions.
func sdkRunRunner(ctx context.Context, input sdkRunInput) (workflowai.Result, error) {
	sdkRunEvents.reset()
	if sdkRunBefore != nil {
		var err error
		if ctx, err = sdkRunBefore(ctx); err != nil {
			return workflowai.Result{}, err
		}
	}
	agent := workflowai.NewWorkflowAgent(testOptions)
	options := testCallOptions
	options.Prompt = input.Prompt
	if input.Messages != nil {
		options.Messages = input.Messages
	}
	call := agent.Stream
	if input.Generate {
		call = agent.Generate
	}
	result, err := call(ctx, options)
	if err == nil && sdkRunAfter != nil {
		err = sdkRunAfter(ctx)
	}
	return result, err
}

// sdkRunSetup resets the agent options, the call options and the runner's
// hooks for a run with model.
func sdkRunSetup(model *sdkRunModel) {
	testOptions = workflowai.WorkflowAgentOptions{Model: model}
	testCallOptions = workflowai.CallOptions{}
	sdkRunBefore, sdkRunAfter = nil, nil
	sdkRunEvents.reset()
	sdkRunEvents2.reset()
}

// sdkRunOutcome is what a run produced.
type sdkRunOutcome struct {
	result workflowai.Result
	err    error
	// steps are the names of the run's steps in order.
	steps []string
	// parts are the stream parts of a Stream, and closed reports whether the
	// stream was closed.
	parts  []workflowai.ModelCallStreamPart
	closed bool
}

// count returns how many of the run's steps are named name.
func (o sdkRunOutcome) count(name string) int {
	n := 0
	for _, step := range o.steps {
		if step == name {
			n++
		}
	}
	return n
}

// partTypes returns the types of the run's stream parts.
func (o sdkRunOutcome) partTypes() []string {
	var types []string
	for _, part := range o.parts {
		types = append(types, part.Type)
	}
	return types
}

// sdkRunDo runs sdkRunRunner with input in a new world and returns what it
// produced. It reads the stream of a Stream after the run ends.
func sdkRunDo(t *testing.T, input sdkRunInput) sdkRunOutcome {
	t.Helper()
	world := testworld.New(worlds.Runners(workflow.Use(sdkRunRunner)))
	defer world.Close()
	ctx := workflow.NewContext(t.Context(), workflow.NewClient(world))
	run, err := workflow.Start(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	var out sdkRunOutcome
	out.result, out.err = run.Value(ctx)
	out.steps = stepNames(t, world, run.ID())
	if !input.Generate {
		out.parts, out.closed = sdkRunReadParts(t, ctx, run)
	}
	return out
}

// sdkRunStream runs a Stream from prompt.
func sdkRunStream(t *testing.T, prompt string) sdkRunOutcome {
	t.Helper()
	return sdkRunDo(t, sdkRunInput{Prompt: prompt})
}

// sdkRunGenerate runs a Generate from prompt.
func sdkRunGenerate(t *testing.T, prompt string) sdkRunOutcome {
	t.Helper()
	return sdkRunDo(t, sdkRunInput{Generate: true, Prompt: prompt})
}

// sdkRunReadParts reads the parts of a finished run's default stream and
// reports whether the stream was closed. It must run in a synctest bubble.
func sdkRunReadParts(t *testing.T, ctx context.Context, run *workflow.Run[workflowai.Result]) ([]workflowai.ModelCallStreamPart, bool) {
	t.Helper()
	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	reader, err := run.Reader(readCtx)
	if err != nil {
		t.Fatal(err)
	}
	tail, err := reader.TailIndex()
	if err != nil {
		t.Fatal(err)
	}
	var parts []workflowai.ModelCallStreamPart
	for range tail + 1 {
		chunk, err := reader.Next()
		if err != nil {
			t.Fatal(err)
		}
		var part workflowai.ModelCallStreamPart
		if err := json.Unmarshal(chunk.Data, &part); err != nil {
			t.Fatal(err)
		}
		parts = append(parts, part)
	}
	// A closed stream ends after its last chunk, and an open one blocks until
	// the read is cancelled.
	done := make(chan error, 1)
	go func() {
		_, err := reader.Next()
		done <- err
	}()
	synctest.Wait()
	select {
	case err := <-done:
		return parts, errors.Is(err, io.EOF)
	default:
		cancel()
		<-done
		return parts, false
	}
}

// sdkRunLog records events in order.
type sdkRunLog struct {
	mu     sync.Mutex
	events []string
}

func (l *sdkRunLog) add(event string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, event)
}

func (l *sdkRunLog) reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = nil
}

func (l *sdkRunLog) list() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.events)
}

// sdkRunResponse is a scripted model response. Generate returns its content
// as a GenerateResult, and Stream as the equivalent chunks.
type sdkRunResponse struct {
	content []types.ContentPart
	finish  types.FinishReason
	usage   types.Usage
	// err fails the call.
	err error
	// chunks replace the chunks a Stream sends.
	chunks []provider.StreamChunk
	// hang keeps a call open after its chunks until its context is done.
	hang bool
	// before runs at the start of the call. An error it returns fails the
	// call.
	before func(ctx context.Context, options *provider.GenerateOptions) error
}

// sdkRunModel answers each model call with the next scripted response.
type sdkRunModel struct {
	mu        sync.Mutex
	responses []sdkRunResponse
	// repeat answers every call after the scripted responses.
	repeat        *sdkRunResponse
	generateCalls []*provider.GenerateOptions
	streamCalls   []*provider.GenerateOptions
	calls         []*provider.GenerateOptions
	supportedURLs map[string][]string
	// ignoreAbort makes streams ignore cancellation: after their chunks
	// they wait this long and end.
	ignoreAbort time.Duration
}

func sdkRunNewModel(responses ...sdkRunResponse) *sdkRunModel {
	return &sdkRunModel{responses: responses}
}

func (m *sdkRunModel) SpecificationVersion() string { return "v3" }

func (m *sdkRunModel) Provider() string { return "mock-provider" }

func (m *sdkRunModel) ModelID() string { return "mock-model-id" }

func (m *sdkRunModel) SupportsTools() bool { return true }

func (m *sdkRunModel) SupportsStructuredOutput() bool { return true }

func (m *sdkRunModel) SupportsImageInput() bool { return false }

func (m *sdkRunModel) SupportedURLs() map[string][]string { return m.supportedURLs }

func (m *sdkRunModel) next(options *provider.GenerateOptions, streaming bool) (sdkRunResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	index := len(m.calls)
	m.calls = append(m.calls, options)
	if streaming {
		m.streamCalls = append(m.streamCalls, options)
	} else {
		m.generateCalls = append(m.generateCalls, options)
	}
	switch {
	case index < len(m.responses):
		return m.responses[index], nil
	case m.repeat != nil:
		return *m.repeat, nil
	}
	return sdkRunResponse{}, errors.New("unexpected model call")
}

// counts returns the number of Generate and Stream model calls.
func (m *sdkRunModel) counts() (generate, stream int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.generateCalls), len(m.streamCalls)
}

// all returns the options of every model call in order.
func (m *sdkRunModel) all() []*provider.GenerateOptions {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.calls)
}

func (m *sdkRunModel) DoGenerate(ctx context.Context, options *provider.GenerateOptions) (*types.GenerateResult, error) {
	response, err := m.next(options, false)
	if err != nil {
		return nil, err
	}
	if response.before != nil {
		if err := response.before(ctx, options); err != nil {
			return nil, err
		}
	}
	if response.err != nil {
		return nil, response.err
	}
	if response.hang {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	result := &types.GenerateResult{
		FinishReason:     response.finish,
		Usage:            response.usage,
		ResponseMetadata: &types.ResponseMetadata{ID: "response-id", ModelID: "response-model"},
		RawRequest:       "request-body",
		RawResponse:      "response-body",
	}
	// Like providers that report tool calls in order, such as OpenAI's
	// Responses API, the content keeps the tool calls too.
	for _, part := range response.content {
		if call, ok := part.(types.ToolCallContent); ok {
			result.ToolCalls = append(result.ToolCalls, types.ToolCall{ID: call.ToolCallID, ToolName: call.ToolName, RawArguments: call.Input, ProviderExecuted: call.ProviderExecuted})
		}
		result.Content = append(result.Content, part)
	}
	return result, nil
}

func (m *sdkRunModel) DoStream(ctx context.Context, options *provider.GenerateOptions) (provider.TextStream, error) {
	response, err := m.next(options, true)
	if err != nil {
		return nil, err
	}
	if response.before != nil {
		if err := response.before(ctx, options); err != nil {
			return nil, err
		}
	}
	if response.err != nil {
		return nil, response.err
	}
	chunks := response.chunks
	if chunks == nil {
		chunks = sdkRunChunks(response)
	}
	return &sdkRunTextStream{ctx: ctx, chunks: chunks, hang: response.hang, ignore: m.ignoreAbort}, nil
}

// sdkRunChunks returns the stream chunks equivalent to a response.
func sdkRunChunks(response sdkRunResponse) []provider.StreamChunk {
	chunks := []provider.StreamChunk{{Type: provider.ChunkTypeResponseMetadata, ResponseMetadata: &provider.ResponseMetadata{ID: "response-id", ModelID: "response-model"}}}
	for i, part := range response.content {
		id := fmt.Sprintf("part-%d", i)
		switch part := part.(type) {
		case types.TextContent:
			chunks = append(chunks,
				provider.StreamChunk{Type: provider.ChunkTypeTextStart, ID: id},
				provider.StreamChunk{Type: provider.ChunkTypeText, ID: id, Text: part.Text},
				provider.StreamChunk{Type: provider.ChunkTypeTextEnd, ID: id},
			)
		case types.ReasoningContent:
			chunks = append(chunks,
				provider.StreamChunk{Type: provider.ChunkTypeReasoningStart, ID: id, ProviderMetadata: part.ProviderMetadata},
				provider.StreamChunk{Type: provider.ChunkTypeReasoning, ID: id, Reasoning: part.Text},
				provider.StreamChunk{Type: provider.ChunkTypeReasoningEnd, ID: id},
			)
		case types.ToolCallContent:
			chunks = append(chunks, provider.StreamChunk{Type: provider.ChunkTypeToolCall, ToolCall: &types.ToolCall{ID: part.ToolCallID, ToolName: part.ToolName, RawArguments: part.Input, ProviderExecuted: part.ProviderExecuted}})
		case types.ToolResultContent:
			chunks = append(chunks, provider.StreamChunk{Type: provider.ChunkTypeToolResult, ToolResult: &types.ToolResult{ToolCallID: part.ToolCallID, ToolName: part.ToolName, Result: part.Result, ProviderExecuted: true}})
		case types.FileContent:
			chunks = append(chunks, provider.StreamChunk{Type: provider.ChunkTypeFile, GeneratedFileContent: &types.GeneratedFileContent{MediaType: part.MediaType, Data: part.Data}})
		case types.SourceContent:
			chunks = append(chunks, provider.StreamChunk{Type: provider.ChunkTypeSource, SourceContent: &part})
		}
	}
	if response.finish != "" {
		usage := response.usage
		chunks = append(chunks, provider.StreamChunk{Type: provider.ChunkTypeFinish, FinishReason: response.finish, Usage: &usage})
	}
	return chunks
}

// sdkRunTextStream sends its chunks, then ends, or with hang waits until its
// context is done.
type sdkRunTextStream struct {
	ctx    context.Context
	chunks []provider.StreamChunk
	hang   bool
	ignore time.Duration
}

func (s *sdkRunTextStream) Next() (*provider.StreamChunk, error) {
	if len(s.chunks) == 0 {
		if s.ignore > 0 {
			time.Sleep(s.ignore)
			return nil, io.EOF
		}
		if s.hang {
			<-s.ctx.Done()
			return nil, s.ctx.Err()
		}
		return nil, io.EOF
	}
	chunk := s.chunks[0]
	s.chunks = s.chunks[1:]
	return &chunk, nil
}

func (s *sdkRunTextStream) Err() error { return nil }

func (s *sdkRunTextStream) Close() error { return nil }

// sdkRunUsage returns a usage with the token counts and details of the SDK's
// fixtures.
func sdkRunUsage(input, noCache, cacheRead, output, text int64) types.Usage {
	total := input + output
	return types.Usage{
		InputTokens:   &input,
		InputDetails:  &types.InputTokenDetails{NoCacheTokens: &noCache, CacheReadTokens: &cacheRead},
		OutputTokens:  &output,
		OutputDetails: &types.OutputTokenDetails{TextTokens: &text},
		TotalTokens:   &total,
	}
}

// sdkRunTextResponse is the text response of the SDK's contract fixtures.
func sdkRunTextResponse(text string) sdkRunResponse {
	return sdkRunResponse{
		content: []types.ContentPart{types.TextContent{Text: text}},
		finish:  types.FinishReasonStop,
		usage:   sdkRunUsage(3, 2, 1, 2, 2),
	}
}

// sdkRunToolResponse is the weather tool call response of the SDK's contract
// fixtures.
func sdkRunToolResponse(id string) sdkRunResponse {
	response := sdkRunTextResponse("Checking the weather.")
	response.content = append(response.content, sdkRunCall(id, "weather", `{"city":"London"}`))
	response.finish = types.FinishReasonToolCalls
	return response
}

func sdkRunCall(id, name, input string) types.ToolCallContent {
	return types.ToolCallContent{ToolCallID: id, ToolName: name, Input: input}
}

// sdkRunWeatherTools returns the weather tool of the SDK's fixtures. It runs
// in workflow context, so the runner's replays run it again.
func sdkRunWeatherTools() workflowai.ToolSet {
	return workflowai.ToolSet{"weather": {
		InputSchema: sdkRunCitySchema(),
		Execute: workflowai.ExecuteFunc(func(context.Context, json.RawMessage, workflowai.ToolCallOptions) (any, error) {
			return map[string]any{"temperature": 18}, nil
		}),
	}}
}

func sdkRunCitySchema() map[string]any {
	return map[string]any{
		"type":       "object",
		"properties": map[string]any{"city": map[string]any{"type": "string"}},
		"required":   []any{"city"},
	}
}

// sdkRunRoles returns the roles of messages.
func sdkRunRoles(messages []workflowai.ModelMessage) []string {
	var roles []string
	for _, message := range messages {
		roles = append(roles, message.Role)
	}
	return roles
}

// sdkRunSystem returns the system prompt of a model call and the contents
// of its system messages.
func sdkRunSystem(options *provider.GenerateOptions) []string {
	var system []string
	if options.Prompt.System != "" {
		system = append(system, options.Prompt.System)
	}
	for _, message := range options.Prompt.Messages {
		if message.Role != types.RoleSystem {
			break
		}
		for _, part := range message.Content {
			if text, ok := part.(types.TextContent); ok {
				system = append(system, text.Text)
			}
		}
	}
	return system
}

// sdkRunAborted reports whether err is a cancellation or timeout.
func sdkRunAborted(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// sdkRunResolvesAbort checks that an aborted Stream resolved, as the SDK's
// stream() does.
func sdkRunResolvesAbort(t *testing.T, err error) {
	t.Helper()
	t.Run("stream resolves", func(t *testing.T) {
		if err != nil {
			t.Fatalf("err = %v", err)
		}
	})
}

// sdkRunCancelBefore makes the runner cancel its context with cause before
// the call.
func sdkRunCancelBefore(cause error) {
	sdkRunBefore = func(ctx context.Context) (context.Context, error) {
		cancelCtx, cancel, err := workflow.WithCancelCause(ctx)
		if err != nil {
			return ctx, err
		}
		return cancelCtx, cancel(cause)
	}
}

// sdkRunCancelInCall cancels the call's durable context from inside a model
// call and waits until it is done, like a provider seeing an external abort.
func sdkRunCancelInCall(ctx context.Context, _ *provider.GenerateOptions) error {
	cancel, ok := workflow.CancelFunc(ctx)
	if !ok {
		return errors.New("no cancel func")
	}
	if err := cancel(); err != nil {
		return err
	}
	<-ctx.Done()
	return ctx.Err()
}

// sdkRunCancelable gives the call a durable context that model calls can
// cancel with workflow.CancelFunc.
func sdkRunCancelable() {
	sdkRunBefore = func(ctx context.Context) (context.Context, error) {
		cancelCtx, _, err := workflow.WithCancel(ctx)
		return cancelCtx, err
	}
}

// sdkRunGenerateResponse is a response of the SDK's generate fixtures.
func sdkRunGenerateResponse(finish types.FinishReason, content ...types.ContentPart) sdkRunResponse {
	usage := sdkRunUsage(5, 2, 3, 3, 1)
	reasoning := int64(2)
	usage.OutputDetails.ReasoningTokens = &reasoning
	return sdkRunResponse{content: content, finish: finish, usage: usage}
}

func sdkRunLookupCall(id, input string) types.ToolCallContent {
	return sdkRunCall(id, "lookup", input)
}

// sdkRunLookupTools returns the lookup tool of the SDK's generate fixtures.
func sdkRunLookupTools() workflowai.ToolSet {
	return workflowai.ToolSet{"lookup": {
		InputSchema: sdkRunCitySchema(),
		Execute: workflowai.ExecuteFunc(func(_ context.Context, input json.RawMessage, _ workflowai.ToolCallOptions) (any, error) {
			var city struct{ City string }
			if err := json.Unmarshal(input, &city); err != nil {
				return nil, err
			}
			return "weather in " + city.City, nil
		}),
	}}
}

// sdkRunValue is the object output of the SDK's generate fixtures.
type sdkRunValue struct {
	Value float64 `json:"value"`
}

func sdkRunValueOutput() workflowai.Output {
	return workflowai.OutputFrom(ai.ObjectOutput[sdkRunValue](ai.ObjectOutputOptions{Schema: ai.SchemaFor[sdkRunValue]()}))
}

// sdkRunRecordingOutput parses the text as it is and records "parse".
type sdkRunRecordingOutput struct{}

func (sdkRunRecordingOutput) ResponseFormat(context.Context) (*provider.ResponseFormat, error) {
	return &provider.ResponseFormat{Type: "text"}, nil
}

func (sdkRunRecordingOutput) ParseOutput(_ context.Context, options ai.ParseCompleteOutputOptions) (any, error) {
	sdkRunEvents.add("parse")
	return options.Text, nil
}

// sdkRunFileMessage is a user message with a PDF file at url.
func sdkRunFileMessage(url string) workflowai.ModelMessage {
	return workflowai.ModelMessage{Role: workflowai.RoleUser, Content: []workflowai.ContentPart{{Type: workflowai.PartFile, Data: url, MediaType: "application/pdf"}}}
}

// sdkRunPromptFile returns the file part of a model call's only message.
func sdkRunPromptFile(t *testing.T, options *provider.GenerateOptions) types.FileContent {
	t.Helper()
	messages := options.Prompt.Messages
	if len(messages) != 1 || len(messages[0].Content) != 1 {
		t.Fatalf("prompt = %+v", messages)
	}
	file, ok := messages[0].Content[0].(types.FileContent)
	if !ok {
		t.Fatalf("content = %#v", messages[0].Content[0])
	}
	return file
}

// sdkRunFileURL returns the URL of a file part.
func sdkRunFileURL(file types.FileContent) string {
	if file.FileData.URL != "" {
		return file.FileData.URL
	}
	return file.URL
}

// sdkRunCallbacks returns callbacks that record their calls with scope.
func sdkRunCallbacks(scope string) workflowai.Callbacks {
	return workflowai.Callbacks{
		OnStart:              func(context.Context, workflowai.StartEvent) { sdkRunEvents.add(scope + ":start") },
		OnStepStart:          func(context.Context, workflowai.StepStartEvent) { sdkRunEvents.add(scope + ":step-start") },
		OnToolExecutionStart: func(context.Context, workflowai.ToolExecutionStartEvent) { sdkRunEvents.add(scope + ":tool-start") },
		OnToolExecutionEnd:   func(context.Context, workflowai.ToolExecutionEndEvent) { sdkRunEvents.add(scope + ":tool-end") },
		OnStepEnd:            func(context.Context, workflowai.StepResult) { sdkRunEvents.add(scope + ":step-end") },
		OnEnd:                func(context.Context, workflowai.EndEvent) { sdkRunEvents.add(scope + ":end") },
	}
}

// sdkRunPrepareStepCalls runs a Generate and a Stream of the weather tool
// call and answer that configure sets up, and returns their model calls by
// mode.
func sdkRunPrepareStepCalls(t *testing.T, configure func()) map[string][]*provider.GenerateOptions {
	t.Helper()
	calls := map[string][]*provider.GenerateOptions{}
	for mode, generate := range map[string]bool{"generate": true, "stream": false} {
		model := sdkRunNewModel(sdkRunToolResponse("weather-call"), sdkRunTextResponse("It is 18°C."))
		sdkRunSetup(model)
		testOptions.Tools = sdkRunWeatherTools()
		configure()
		out := sdkRunDo(t, sdkRunInput{Generate: generate, Prompt: "What is the weather in London?"})
		if out.err != nil {
			t.Fatalf("%s: %v", mode, out.err)
		}
		calls[mode] = model.all()
	}
	return calls
}

// sdkRunAtStepZero returns a PrepareStep that returns result for step 0.
func sdkRunAtStepZero(result workflowai.PrepareStepResult) workflowai.PrepareStepFunc {
	return func(_ context.Context, info workflowai.PrepareStepInfo) (workflowai.PrepareStepResult, error) {
		if info.StepNumber == 0 {
			return result, nil
		}
		return workflowai.PrepareStepResult{}, nil
	}
}

// sdkRunStepLimitSetup sets up an agent whose model calls the weather tool
// on every call.
func sdkRunStepLimitSetup() {
	response := sdkRunToolResponse("weather-call")
	model := sdkRunNewModel()
	model.repeat = &response
	sdkRunSetup(model)
	testOptions.Tools = sdkRunWeatherTools()
}

// sdkRunIDs are the ids of the action calls of the SDK's concurrency
// fixtures.
var sdkRunIDs = []string{"first", "second", "third"}

// sdkRunActionCalls returns a call of the action tool for each id.
func sdkRunActionCalls() []types.ContentPart {
	var calls []types.ContentPart
	for _, id := range sdkRunIDs {
		calls = append(calls, sdkRunCall(id, "action", fmt.Sprintf(`{"id":%q}`, id)))
	}
	return calls
}

// sdkRunApprovalMessages returns a conversation whose last message approves
// the action call of each id, with the responses in reverse order.
func sdkRunApprovalMessages() []workflowai.ModelMessage {
	assistant := workflowai.ModelMessage{Role: workflowai.RoleAssistant}
	tool := workflowai.ModelMessage{Role: workflowai.RoleTool}
	for _, id := range sdkRunIDs {
		assistant.Content = append(assistant.Content,
			workflowai.ContentPart{Type: workflowai.PartToolCall, ToolCallID: id, ToolName: "action", Input: json.RawMessage(fmt.Sprintf(`{"id":%q}`, id))},
			workflowai.ContentPart{Type: workflowai.PartToolApprovalRequest, ToolCallID: id, ApprovalID: "approval-" + id},
		)
	}
	for _, id := range slices.Backward(sdkRunIDs) {
		tool.Content = append(tool.Content, workflowai.ContentPart{Type: workflowai.PartToolApprovalResponse, ApprovalID: "approval-" + id, Approved: true})
	}
	return []workflowai.ModelMessage{user("Run the tools."), assistant, tool}
}

// sdkRunActionInput is the input of the action tool.
type sdkRunActionInput struct {
	ID string `json:"id"`
}

// sdkRunGates records the action calls that started and holds each until it
// is released.
var sdkRunGates sdkRunGateSet

type sdkRunGateSet struct {
	mu      sync.Mutex
	started []string
	gates   map[string]chan struct{}
}

func (g *sdkRunGateSet) reset() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.started = nil
	g.gates = map[string]chan struct{}{}
	for _, id := range sdkRunIDs {
		g.gates[id] = make(chan struct{})
	}
}

func (g *sdkRunGateSet) list() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return slices.Clone(g.started)
}

// release lets the call with id finish, once it starts.
func (g *sdkRunGateSet) release(id string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	select {
	case <-g.gates[id]:
	default:
		close(g.gates[id])
	}
}

// waitStarted waits until the run is blocked and checks the calls that
// started. Calls that start together start in any order. It reports a
// mismatch without stopping the test, so the test still releases the calls.
func (g *sdkRunGateSet) waitStarted(t *testing.T, want []string) {
	t.Helper()
	synctest.Wait()
	got := g.list()
	slices.Sort(got)
	want = slices.Sorted(slices.Values(want))
	if !slices.Equal(got, want) {
		t.Errorf("started = %v, want %v", got, want)
	}
}

// sdkRunGatedAction records that its call started and returns its id once
// the call's gate is released.
func sdkRunGatedAction(_ context.Context, input sdkRunActionInput) (string, error) {
	sdkRunGates.mu.Lock()
	sdkRunGates.started = append(sdkRunGates.started, input.ID)
	gate := sdkRunGates.gates[input.ID]
	sdkRunGates.mu.Unlock()
	<-gate
	return input.ID, nil
}

// sdkRunPeak records how many action calls run at once.
var sdkRunPeak sdkRunCounter

type sdkRunCounter struct {
	mu     sync.Mutex
	active int
	peak   int
}

func (c *sdkRunCounter) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.active, c.peak = 0, 0
}

func (c *sdkRunCounter) max() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.peak
}

// sdkRunPeakAction runs for a second while it counts as active.
func sdkRunPeakAction(_ context.Context, input sdkRunActionInput) (string, error) {
	sdkRunPeak.mu.Lock()
	sdkRunPeak.active++
	sdkRunPeak.peak = max(sdkRunPeak.peak, sdkRunPeak.active)
	sdkRunPeak.mu.Unlock()
	time.Sleep(time.Second)
	sdkRunPeak.mu.Lock()
	sdkRunPeak.active--
	sdkRunPeak.mu.Unlock()
	return "done", nil
}

// sdkRunAbortCallbacks makes the agent and the call record their OnAbort,
// OnError and OnEnd calls.
func sdkRunAbortCallbacks() {
	for _, scope := range []string{"constructor", "call"} {
		callbacks := workflowai.Callbacks{
			OnAbort: func(_ context.Context, event workflowai.AbortEvent) {
				sdkRunEvents.add(fmt.Sprintf("%s abort: %d steps", scope, len(event.Steps)))
			},
			OnError: func(_ context.Context, err error) { sdkRunEvents.add(fmt.Sprintf("%s error: %v", scope, err)) },
			OnEnd:   func(context.Context, workflowai.EndEvent) { sdkRunEvents.add(scope + " end") },
		}
		if scope == "constructor" {
			testOptions.Callbacks = callbacks
		} else {
			testCallOptions.Callbacks = callbacks
		}
	}
}

// sdkRunStalledResponse starts a text and then stays open until the call's
// context is done.
func sdkRunStalledResponse() sdkRunResponse {
	return sdkRunResponse{
		chunks: []provider.StreamChunk{
			{Type: provider.ChunkTypeTextStart, ID: "text-1"},
			{Type: provider.ChunkTypeText, ID: "text-1", Text: "Hello"},
		},
		hang: true,
	}
}

// sdkRunE2EResponse is a response of the SDK's mock providers: it finishes
// with tool-calls when it calls a tool.
func sdkRunE2EResponse(content ...types.ContentPart) sdkRunResponse {
	finish := types.FinishReasonStop
	for _, part := range content {
		if _, ok := part.(types.ToolCallContent); ok {
			finish = types.FinishReasonToolCalls
		}
	}
	input, output, total := int64(5), int64(10), int64(15)
	return sdkRunResponse{content: content, finish: finish, usage: types.Usage{InputTokens: &input, OutputTokens: &output, TotalTokens: &total}}
}

// sdkRunEvents2 records model call attempts. The runner does not reset it,
// since model calls run in steps.
var sdkRunEvents2 sdkRunLog

// sdkRunAttemptResponse records the step attempt of the model call that
// answers with response.
func sdkRunAttemptResponse(response sdkRunResponse) sdkRunResponse {
	before := response.before
	response.before = func(ctx context.Context, options *provider.GenerateOptions) error {
		sdkRunEvents2.add(fmt.Sprintf("attempt %d", workflow.GetStepMetadata(ctx).Attempt))
		if before != nil {
			return before(ctx, options)
		}
		return nil
	}
	return response
}

// sdkRunRetryable is a retryable provider error whose retry-after-ms header
// is ms.
func sdkRunRetryable(ms string) error {
	return &providererrors.ProviderError{StatusCode: 503, Message: "Retryable fixture failure", ResponseHeaders: map[string]string{"retry-after-ms": ms}}
}

type sdkRunAddInput struct {
	A int `json:"a"`
	B int `json:"b"`
}

func sdkRunAdd(_ context.Context, input sdkRunAddInput) (int, error) {
	return input.A + input.B, nil
}

func sdkRunAddTools() workflowai.ToolSet {
	return workflowai.ToolSet{"addNumbers": {Description: "Add two numbers", Execute: workflowai.ExecuteStep(sdkRunAdd)}}
}

// sdkRunRepairAdd repairs a call to addNumbers with malformed input.
func sdkRunRepairAdd(_ context.Context, options ai.ToolCallRepairOptions) (*types.ToolCall, error) {
	call := options.ToolCall
	call.RawArguments = `{"a": 3, "b": 7}`
	call.Arguments = nil
	return &call, nil
}

func sdkRunEchoStep(_ context.Context, input struct {
	Step int `json:"step"`
}) (string, error) {
	return fmt.Sprintf("step-%d-done", input.Step), nil
}

func sdkRunThrowingStep(context.Context, struct{}) (string, error) {
	return "", workflow.FatalError(errors.New("Tool execution failed fatally"))
}

func sdkRunExecuteApprovedAction(_ context.Context, input struct {
	Action string `json:"action"`
}) (string, error) {
	return "executed:" + input.Action, nil
}

// sdkRunRiskyTools returns a tool that needs approval.
func sdkRunRiskyTools() workflowai.ToolSet {
	return workflowai.ToolSet{"riskyTool": {
		Description:   "A dangerous tool that needs approval",
		NeedsApproval: workflowai.NeedsApprovalAlways,
		Execute:       workflowai.ExecuteStep(sdkRunExecuteApprovedAction),
	}}
}

// sdkRunHookTools returns a tool that waits for the wait-hook hook.
func sdkRunHookTools() workflowai.ToolSet {
	return workflowai.ToolSet{"wait": {
		InputSchema: map[string]any{"type": "object"},
		Execute: workflowai.ExecuteFunc(func(ctx context.Context, _ json.RawMessage, _ workflowai.ToolCallOptions) (any, error) {
			hook := workflow.CreateHook[string](ctx, workflow.HookOption{Token: "wait-hook"})
			defer hook.Dispose(ctx)
			return hook.Next(ctx)
		}),
	}}
}
