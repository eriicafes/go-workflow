package workflowai_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
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
	providererrors "github.com/digitallysavvy/go-ai/pkg/provider/errors"
	"github.com/digitallysavvy/go-ai/pkg/provider/types"
	promptutils "github.com/digitallysavvy/go-ai/pkg/providerutils/prompt"
	"github.com/digitallysavvy/go-ai/pkg/telemetry"
	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/workflowai"
	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/testworld"
)

// AI SDK: packages/workflow/src/do-stream-step.test.ts > doStreamStep > resets the current UI step before streaming model output
func TestSDKModelCallResetsStepBeforeOutput(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkModelCallSetup(sdkModelCallChunks(
			provider.StreamChunk{Type: provider.ChunkTypeStreamStart},
			provider.StreamChunk{Type: provider.ChunkTypeTextStart, ID: "text-1"},
			provider.StreamChunk{Type: provider.ChunkTypeText, ID: "text-1", Text: "Retried output"},
			provider.StreamChunk{Type: provider.ChunkTypeTextEnd, ID: "text-1"},
			sdkModelCallFinish(types.FinishReasonStop),
		))
		run := sdkModelCallRunAgent(t, false, []workflowai.ModelMessage{user("Retry.")})
		if run.err != nil {
			t.Fatal(run.err)
		}
		if len(run.parts) == 0 || run.parts[0]["type"] != "reset-step" {
			t.Fatalf("parts = %v", run.parts)
		}
		if !slices.ContainsFunc(run.parts, func(part map[string]any) bool {
			return part["type"] == "text-delta" && part["id"] == "text-1" && part["text"] == "Retried output"
		}) {
			t.Fatalf("parts = %v", run.parts)
		}
	})
}

// AI SDK: packages/workflow/src/do-stream-step.test.ts > doStreamStep > uses model URL support when invoking a custom downloader
// AI SDK: packages/workflow/src/do-generate-step.test.ts > doGenerateStep > honors a custom pass-through decision for $name URLs
func TestSDKModelCallDownloadPassThrough(t *testing.T) {
	tests := []struct {
		name      string
		generate  bool
		urls      map[string][]string
		supported bool
	}{
		{"stream supported", false, map[string][]string{"application/pdf": {".*"}}, true},
		{"generate unsupported", true, map[string][]string{}, false},
		{"generate supported", true, map[string][]string{"application/pdf": {".*"}}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				sdkModelCallSetup(sdkModelCallText("done"))
				sdkModelCallLM.urls = tt.urls
				var mu sync.Mutex
				var requests []promptutils.DownloadRequest
				testOptions.Download = func(_ context.Context, batch []promptutils.DownloadRequest) ([]*promptutils.DownloadResult, error) {
					mu.Lock()
					defer mu.Unlock()
					requests = append(requests, batch...)
					return make([]*promptutils.DownloadResult, len(batch)), nil
				}
				run := sdkModelCallRunAgent(t, tt.generate, []workflowai.ModelMessage{sdkModelCallFileMessage()})
				if run.err != nil {
					t.Fatal(run.err)
				}
				mu.Lock()
				defer mu.Unlock()
				want := promptutils.DownloadRequest{URL: sdkModelCallFileURL, IsURLSupportedByModel: tt.supported}
				if len(requests) == 0 || requests[0] != want {
					t.Fatalf("requests = %+v, want %+v", requests, want)
				}
				file, ok := sdkModelCallLM.prompt(0)[0].Content[0].(types.FileContent)
				if !ok || file.FileData.URL != sdkModelCallFileURL || len(file.Data) != 0 {
					t.Fatalf("prompt file = %#v", sdkModelCallLM.prompt(0)[0].Content[0])
				}
			})
		})
	}
}

// AI SDK: packages/workflow/src/do-stream-step.test.ts > doStreamStep > preserves reasoning text and provider metadata as one content part
func TestSDKModelCallReasoningContentPart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		metadata := json.RawMessage(`{"anthropic":{"signature":"reasoning-signature"},"openai":{"reasoningEncryptedContent":"encrypted-reasoning"}}`)
		sdkModelCallSetup(sdkModelCallChunks(
			provider.StreamChunk{Type: provider.ChunkTypeStreamStart},
			provider.StreamChunk{Type: provider.ChunkTypeReasoningStart, ID: "reasoning-1", ProviderMetadata: metadata},
			provider.StreamChunk{Type: provider.ChunkTypeReasoning, ID: "reasoning-1", Reasoning: "Inspect the configuration."},
			provider.StreamChunk{Type: provider.ChunkTypeReasoningEnd, ID: "reasoning-1"},
			sdkModelCallFinish(types.FinishReasonStop),
		))
		run := sdkModelCallRunAgent(t, false, sdkModelCallPrompt())
		if run.err != nil {
			t.Fatal(run.err)
		}
		content := run.result.Steps[0].Content
		if len(content) != 1 || content[0].Type != workflowai.PartReasoning || content[0].Text != "Inspect the configuration." || !sdkModelCallJSONEqual(content[0].ProviderMetadata, metadata) {
			t.Fatalf("content = %+v", content)
		}
	})
}

// AI SDK: packages/workflow/src/do-stream-step.test.ts > doStreamStep > preserves measured model call performance
// AI SDK: packages/workflow/src/model-call-iterator.test.ts > modelCallIterator > reports measured stream performance in step results and callbacks
func TestSDKModelCallPerformance(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkModelCallSetup(func() ([]provider.StreamChunk, error) {
			time.Sleep(100 * time.Millisecond)
			return []provider.StreamChunk{
				{Type: provider.ChunkTypeStreamStart},
				{Type: provider.ChunkTypeTextStart, ID: "text-1"},
				{Type: provider.ChunkTypeText, ID: "text-1", Text: "Measured output"},
				{Type: provider.ChunkTypeTextEnd, ID: "text-1"},
				sdkModelCallFinish(types.FinishReasonStop),
			}, nil
		})
		var ended workflowai.StepResult
		testOptions.OnStepEnd = func(_ context.Context, step workflowai.StepResult) { ended = step }
		run := sdkModelCallRunAgent(t, false, sdkModelCallPrompt())
		if run.err != nil {
			t.Fatal(run.err)
		}
		for _, step := range []workflowai.StepResult{run.result.Steps[0], ended} {
			var got map[string]any
			_ = json.Unmarshal(sdkModelCallMarshal(t, step.Performance), &got)
			for _, key := range []string{"responseTimeMs", "effectiveOutputTokensPerSecond", "outputTokensPerSecond", "inputTokensPerSecond", "effectiveTotalTokensPerSecond", "timeToFirstOutputMs"} {
				if value, ok := got[key].(float64); !ok || value <= 0 {
					t.Fatalf("performance = %v", got)
				}
			}
		}
	})
}

// AI SDK: packages/workflow/src/do-stream-step.test.ts > doStreamStep > preserves provider metadata on provider-executed tool results
func TestSDKModelCallProviderResultMetadata(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkModelCallSetup(sdkModelCallChunks(
			provider.StreamChunk{Type: provider.ChunkTypeStreamStart},
			provider.StreamChunk{Type: provider.ChunkTypeToolCall, ToolCall: &types.ToolCall{ID: "provider-call", ToolName: "providerTool", RawArguments: `{}`, ProviderExecuted: true, Dynamic: true}},
			provider.StreamChunk{Type: provider.ChunkTypeToolResult, ToolResult: &types.ToolResult{
				ToolCallID: "provider-call", ToolName: "providerTool", Result: map[string]any{"value": "result"}, ProviderExecuted: true,
				ProviderMetadata: map[string]any{"openai": map[string]any{"itemId": "provider-result-item"}},
			}},
			sdkModelCallFinish(types.FinishReasonToolCalls),
		))
		run := sdkModelCallRunAgent(t, false, sdkModelCallPrompt())
		if run.err != nil {
			t.Fatal(run.err)
		}
		step := run.result.Steps[0]
		index := slices.IndexFunc(step.Content, func(part workflowai.ContentPart) bool { return part.Type == workflowai.PartToolResult })
		if index < 0 {
			t.Fatalf("content = %+v", step.Content)
		}
		result := step.Content[index]
		if result.ToolCallID != "provider-call" || result.ToolName != "providerTool" || !result.ProviderExecuted ||
			!sdkModelCallJSONEqual(sdkModelCallMarshal(t, result.ProviderOptions), json.RawMessage(`{"openai":{"itemId":"provider-result-item"}}`)) {
			t.Fatalf("tool result = %+v", result)
		}
		if len(step.ToolResults) != 1 || step.ToolResults[0].IsError || !sdkModelCallJSONEqual(step.ToolResults[0].Output, json.RawMessage(`{"value":"result"}`)) {
			t.Fatalf("tool results = %+v", step.ToolResults)
		}
	})
}

// AI SDK: packages/workflow/src/do-stream-step.test.ts > doStreamStep > $setting makes $expectedAttempts model attempt(s)
func TestSDKModelCallRetryAttempts(t *testing.T) {
	two := 2
	zero := 0
	tests := []struct {
		name       string
		maxRetries *int
		attempts   int
	}{
		{"zero retries", &zero, 1},
		{"two retries", &two, 3},
		{"the default retry count", nil, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var responses []func() ([]provider.StreamChunk, error)
				for range 4 {
					responses = append(responses, failWith(sdkModelCallAPIError(500, nil)))
				}
				sdkModelCallSetup(responses...)
				testOptions.MaxRetries = tt.maxRetries
				run := sdkModelCallRunAgent(t, false, sdkModelCallPrompt())
				if run.err == nil || !strings.Contains(run.err.Error(), "model call failed with status 500") {
					t.Fatalf("err = %v", run.err)
				}
				if got := sdkModelCallLM.callCount(); got != tt.attempts {
					t.Fatalf("%d attempts, want %d", got, tt.attempts)
				}
			})
		})
	}
}

// AI SDK: packages/workflow/src/do-stream-step.test.ts > doStreamStep > retries rate-limit errors
func TestSDKModelCallRetriesRateLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkModelCallSetup(failWith(sdkModelCallAPIError(429, nil)), failWith(sdkModelCallAPIError(429, nil)), sdkModelCallText("unused"))
		one := 1
		testOptions.MaxRetries = &one
		run := sdkModelCallRunAgent(t, false, sdkModelCallPrompt())
		if run.err == nil || !strings.Contains(run.err.Error(), "model call failed with status 429") {
			t.Fatalf("err = %v", run.err)
		}
		if got := sdkModelCallLM.callCount(); got != 2 {
			t.Fatalf("%d attempts", got)
		}
	})
}

// AI SDK: packages/workflow/src/do-stream-step.test.ts > doStreamStep > does not retry non-retryable status %i errors
func TestSDKModelCallNonRetryableStatus(t *testing.T) {
	for _, status := range []int{400, 401, 403} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				sdkModelCallSetup(failWith(sdkModelCallAPIError(status, nil)), sdkModelCallText("unused"))
				two := 2
				testOptions.MaxRetries = &two
				run := sdkModelCallRunAgent(t, false, sdkModelCallPrompt())
				if run.err == nil || !strings.Contains(run.err.Error(), "model call failed with status "+strconv.Itoa(status)) {
					t.Fatalf("err = %v", run.err)
				}
				if got := sdkModelCallLM.callCount(); got != 1 {
					t.Fatalf("%d attempts", got)
				}
			})
		})
	}
}

// AI SDK: packages/workflow/src/do-stream-step.test.ts > doStreamStep > respects retry-after response headers
func TestSDKModelCallRetryAfterHeader(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls []time.Time
		record := func(respond func() ([]provider.StreamChunk, error)) func() ([]provider.StreamChunk, error) {
			return func() ([]provider.StreamChunk, error) {
				calls = append(calls, time.Now())
				return respond()
			}
		}
		sdkModelCallSetup(record(failWith(sdkModelCallAPIError(429, map[string]string{"retry-after-ms": "25"}))), record(sdkModelCallChunks(sdkModelCallFinish(types.FinishReasonStop))))
		one := 1
		testOptions.MaxRetries = &one
		run := sdkModelCallRunAgent(t, false, sdkModelCallPrompt())
		if run.err != nil {
			t.Fatal(run.err)
		}
		if run.result.FinishReason != "stop" || len(calls) != 2 {
			t.Fatalf("finish reason = %q after %d calls", run.result.FinishReason, len(calls))
		}
		if waited := calls[1].Sub(calls[0]); waited != 25*time.Millisecond {
			t.Fatalf("waited %v", waited)
		}
	})
}

// The model call retries opening the model itself, so a failed model call
// step is not retried by the workflow on top of the model call's retries.
//
// AI SDK: packages/workflow/src/do-stream-step.test.ts > doStreamStep > disables workflow step retries to avoid stacking retry layers
func TestSDKModelCallStepNotRetried(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var responses []func() ([]provider.StreamChunk, error)
		for range 4 {
			responses = append(responses, failWith(sdkModelCallAPIError(500, nil)))
		}
		sdkModelCallSetup(responses...)
		zero := 0
		testOptions.MaxRetries = &zero
		run := sdkModelCallRunAgent(t, false, sdkModelCallPrompt())
		if run.err == nil {
			t.Fatal("expected an error")
		}
		if got := sdkModelCallLM.callCount(); got != 1 {
			t.Fatalf("%d attempts", got)
		}
		if got := slices.DeleteFunc(slices.Clone(run.steps), func(name string) bool { return name != "doStreamStep" }); len(got) != 1 {
			t.Fatalf("steps = %v", run.steps)
		}
	})
}

// A Timeout that passes while a tool runs aborts the call before its next
// model call, which never reaches the model.
//
// AI SDK: packages/workflow/src/do-stream-step.test.ts > doStreamStep > does not call the model after the absolute deadline has elapsed
// AI SDK: packages/workflow/src/do-generate-step.test.ts > doGenerateStep > rejects an expired deadline without dispatching the model and disables Workflow retries
func TestSDKModelCallExpiredDeadline(t *testing.T) {
	for _, generate := range []bool{false, true} {
		t.Run(sdkModelCallMode(generate), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				sdkModelCallSetup(toolCalls(call("c1", "sleep", `{"seconds":20}`)), sdkModelCallText("unused"))
				testOptions.Tools = workflowai.ToolSet{"sleep": {Execute: workflowai.ExecuteStep(sdkModelCallSleep)}}
				testOptions.Timeout = 10 * time.Second
				aborted := false
				testOptions.OnAbort = func(context.Context, workflowai.AbortEvent) { aborted = true }
				run := sdkModelCallRunAgent(t, generate, sdkModelCallPrompt())
				// Generate fails with the abort, and the stream resolves.
				if generate && !errors.Is(run.err, context.Canceled) && !errors.Is(run.err, context.DeadlineExceeded) || !generate && run.err != nil {
					t.Fatalf("err = %v", run.err)
				}
				if got := sdkModelCallLM.callCount(); got != 1 || !aborted {
					t.Fatalf("%d model calls, aborted = %v", got, aborted)
				}
			})
		})
	}
}

// AI SDK: packages/workflow/src/do-stream-step.test.ts > doStreamStep > returns an aborted result when the deadline aborts the model call
// AI SDK: packages/workflow/src/model-call-iterator.test.ts > modelCallIterator > generation settings > returns an aborted result without reporting an error
func TestSDKModelCallDeadlineAbortsModelCall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkModelCallSetup(hang)
		testOptions.Timeout = 5 * time.Second
		testOptions.OnError = func(ctx context.Context, err error) { sdkModelCallRecordOnce(ctx, "error") }
		testOptions.OnAbort = func(ctx context.Context, _ workflowai.AbortEvent) { sdkModelCallRecordOnce(ctx, "abort") }
		run := sdkModelCallRunAgent(t, false, sdkModelCallPrompt())
		// The aborted result is returned as data, so the stream resolves.
		if run.err != nil {
			t.Fatalf("err = %v", run.err)
		}
		if got := sdkModelCallLM.callCount(); got != 1 {
			t.Fatalf("%d model calls", got)
		}
		if got := sdkModelCallRecorded(); !slices.Equal(got, []string{"abort"}) {
			t.Fatalf("callbacks = %v", got)
		}
	})
}

// AI SDK: packages/workflow/src/do-stream-step.test.ts > doStreamStep > returns model stream errors as terminal step data
func TestSDKModelCallStreamErrorIsTerminal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkModelCallSetup(sdkModelCallChunks(
			provider.StreamChunk{Type: provider.ChunkTypeStreamStart},
			provider.StreamChunk{Type: provider.ChunkTypeError, Err: errors.New("terminal model error")},
			sdkModelCallFinish(types.FinishReasonError),
		))
		run := sdkModelCallRunAgent(t, false, sdkModelCallPrompt())
		if run.err != nil {
			t.Fatal(run.err)
		}
		if run.result.Error != "terminal model error" || run.result.FinishReason != "error" {
			t.Fatalf("result = %+v", run.result)
		}
		if !slices.ContainsFunc(run.parts, func(part map[string]any) bool {
			return part["type"] == "error" && part["error"] == "terminal model error"
		}) {
			t.Fatalf("parts = %v", run.parts)
		}
	})
}

// AI SDK: packages/workflow/src/do-stream-step.test.ts > doStreamStep > returns a no-output error when an incomplete stream contains $name
func TestSDKModelCallIncompleteStreamNoOutput(t *testing.T) {
	encrypted := json.RawMessage(`{"openai":{"reasoningEncryptedContent":"encrypted-reasoning"}}`)
	tests := []struct {
		name   string
		chunks []provider.StreamChunk
	}{
		{"stream start only", []provider.StreamChunk{{Type: provider.ChunkTypeStreamStart}}},
		{"response metadata only", []provider.StreamChunk{
			{Type: provider.ChunkTypeStreamStart},
			{Type: provider.ChunkTypeResponseMetadata, ResponseMetadata: &provider.ResponseMetadata{ID: "response-1", ModelID: "mock-model"}},
		}},
		{"bare text start", []provider.StreamChunk{
			{Type: provider.ChunkTypeStreamStart},
			{Type: provider.ChunkTypeTextStart, ID: "text-1"},
		}},
		{"empty encrypted reasoning delta", []provider.StreamChunk{
			{Type: provider.ChunkTypeStreamStart},
			{Type: provider.ChunkTypeReasoningStart, ID: "reasoning-1", ProviderMetadata: encrypted},
			{Type: provider.ChunkTypeReasoning, ID: "reasoning-1", ProviderMetadata: encrypted},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				sdkModelCallSetup(sdkModelCallChunks(tt.chunks...))
				run := sdkModelCallRunAgent(t, false, sdkModelCallPrompt())
				if run.err != nil {
					t.Fatal(run.err)
				}
				if run.result.Error != sdkModelCallNoOutput {
					t.Fatalf("error = %q", run.result.Error)
				}
			})
		})
	}
}

// AI SDK: packages/workflow/src/do-stream-step.test.ts > doStreamStep > retains partial output when the model stream ends without a finish part
func TestSDKModelCallPartialOutputWithoutFinish(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkModelCallSetup(sdkModelCallChunks(
			provider.StreamChunk{Type: provider.ChunkTypeStreamStart},
			provider.StreamChunk{Type: provider.ChunkTypeTextStart, ID: "text-1"},
			provider.StreamChunk{Type: provider.ChunkTypeText, ID: "text-1", Text: "partial output"},
		))
		run := sdkModelCallRunAgent(t, false, sdkModelCallPrompt())
		if run.err != nil {
			t.Fatal(run.err)
		}
		step := run.result.Steps[0]
		if run.result.Error != "" || step.FinishReason != "" {
			t.Fatalf("result = %+v", run.result)
		}
		if len(step.Content) != 1 || step.Content[0].Type != workflowai.PartText || step.Content[0].Text != "partial output" {
			t.Fatalf("content = %+v", step.Content)
		}
	})
}

// AI SDK: packages/workflow/src/do-stream-step.test.ts > doStreamStep > withholds tool calls with tool choice none
func TestSDKModelCallStreamToolChoiceNone(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		search := types.ToolCall{ID: "call-1", ToolName: "search", RawArguments: `{}`}
		sdkModelCallSetup(sdkModelCallChunks(
			provider.StreamChunk{Type: provider.ChunkTypeStreamStart},
			provider.StreamChunk{Type: provider.ChunkTypeToolInputStart, ToolCall: &types.ToolCall{ID: "call-1", ToolName: "search"}},
			provider.StreamChunk{Type: provider.ChunkTypeToolInputDelta, ID: "call-1", Text: `{}`},
			provider.StreamChunk{Type: provider.ChunkTypeToolInputEnd, ToolCall: &types.ToolCall{ID: "call-1"}},
			provider.StreamChunk{Type: provider.ChunkTypeToolCall, ToolCall: &search},
			sdkModelCallFinish(types.FinishReasonToolCalls),
		), sdkModelCallText("unused"))
		testOptions.Tools = workflowai.ToolSet{"search": {InputSchema: sdkModelCallQuerySchema}}
		none := types.NoneToolChoice()
		testOptions.ToolChoice = &none
		repaired := false
		testOptions.RepairToolCall = func(context.Context, ai.ToolCallRepairOptions) (*types.ToolCall, error) {
			repaired = true
			return nil, nil
		}
		run := sdkModelCallRunAgent(t, false, sdkModelCallPrompt())
		if run.err != nil {
			t.Fatal(run.err)
		}
		want := (&ai.ToolChoiceViolationError{ToolChoice: none}).Error()
		if len(run.result.Steps[0].ToolCalls) != 0 || run.result.Error != want || repaired {
			t.Fatalf("result = %+v, repaired = %v", run.result, repaired)
		}
		if slices.ContainsFunc(run.parts, func(part map[string]any) bool { return strings.HasPrefix(part["type"].(string), "tool-") }) {
			t.Fatalf("parts = %v", sdkModelCallTypes(run.parts))
		}
	})
}

// AI SDK: packages/workflow/src/do-generate-step.test.ts > doGenerateStep > uses SDK retries for retryable failures
func TestSDKModelCallGenerateRetries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkModelCallSetup(failWith(sdkModelCallAPIError(503, nil)), sdkModelCallText("Answer"))
		one := 1
		testOptions.MaxRetries = &one
		run := sdkModelCallRunAgent(t, true, sdkModelCallPrompt())
		if run.err != nil {
			t.Fatal(run.err)
		}
		if run.result.FinishReason != "stop" || sdkModelCallLM.callCount() != 2 {
			t.Fatalf("finish reason = %q after %d calls", run.result.FinishReason, sdkModelCallLM.callCount())
		}
	})
}

// AI SDK: packages/workflow/src/do-generate-step.test.ts > doGenerateStep > checks the absolute deadline again before retry dispatch
func TestSDKModelCallGenerateDeadlineBeforeRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkModelCallSetup(func() ([]provider.StreamChunk, error) {
			// The failed attempt uses up the rest of the Timeout.
			time.Sleep(time.Second)
			return nil, sdkModelCallAPIError(503, map[string]string{"retry-after": "0"})
		}, sdkModelCallText("unused"))
		testOptions.Timeout = 500 * time.Millisecond
		one := 1
		testOptions.MaxRetries = &one
		run := sdkModelCallRunAgent(t, true, sdkModelCallPrompt())
		if run.err == nil {
			t.Fatal("expected an error")
		}
		if got := sdkModelCallLM.callCount(); got != 1 {
			t.Fatalf("%d model calls", got)
		}
	})
}

// AI SDK: packages/workflow/src/do-generate-step.test.ts > doGenerateStep > parses repaired inputs inside the step and returns callback replay data
func TestSDKModelCallGenerateRepairsInput(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkModelCallSetup(toolCalls(call("call", "lookup", `{`)))
		testOptions.Tools = workflowai.ToolSet{"lookup": {InputSchema: sdkModelCallCitySchema}}
		var mu sync.Mutex
		repairs := 0
		testOptions.RepairToolCall = func(_ context.Context, options ai.ToolCallRepairOptions) (*types.ToolCall, error) {
			mu.Lock()
			defer mu.Unlock()
			repairs++
			fixed := options.ToolCall
			fixed.RawArguments = `{"city":"London"}`
			return &fixed, nil
		}
		run := sdkModelCallRunAgent(t, true, sdkModelCallPrompt())
		if run.err != nil {
			t.Fatal(run.err)
		}
		mu.Lock()
		defer mu.Unlock()
		calls := run.result.Steps[0].ToolCalls
		if repairs != 1 || len(calls) != 1 || calls[0].Invalid || !sdkModelCallJSONEqual(calls[0].Input, json.RawMessage(`{"city":"London"}`)) {
			t.Fatalf("%d repairs, tool calls = %+v", repairs, calls)
		}
	})
}

// AI SDK: packages/workflow/src/do-generate-step.test.ts > doGenerateStep > keeps URL attachments in repair callback messages after model prompt conversion
func TestSDKModelCallRepairMessagesKeepURLs(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkModelCallSetup(toolCalls(call("call", "lookup", `{`)))
		testOptions.Tools = workflowai.ToolSet{"lookup": {InputSchema: sdkModelCallCitySchema}}
		testOptions.Download = func(_ context.Context, requests []promptutils.DownloadRequest) ([]*promptutils.DownloadResult, error) {
			results := make([]*promptutils.DownloadResult, len(requests))
			for i := range requests {
				results[i] = &promptutils.DownloadResult{Data: []byte{1, 2, 3}, MediaType: "application/pdf"}
			}
			return results, nil
		}
		var mu sync.Mutex
		var messages []types.Message
		testOptions.RepairToolCall = func(_ context.Context, options ai.ToolCallRepairOptions) (*types.ToolCall, error) {
			mu.Lock()
			defer mu.Unlock()
			messages = options.Messages
			fixed := options.ToolCall
			fixed.RawArguments = `{"city":"London"}`
			return &fixed, nil
		}
		run := sdkModelCallRunAgent(t, true, []workflowai.ModelMessage{sdkModelCallFileMessage()})
		if run.err != nil {
			t.Fatal(run.err)
		}
		sent, ok := sdkModelCallLM.prompt(0)[0].Content[0].(types.FileContent)
		if !ok || !slices.Equal(sent.Data, []byte{1, 2, 3}) {
			t.Fatalf("prompt file = %#v", sdkModelCallLM.prompt(0)[0].Content[0])
		}
		mu.Lock()
		defer mu.Unlock()
		if len(messages) != 1 {
			t.Fatalf("repair messages = %+v", messages)
		}
		repairFile, ok := messages[0].Content[0].(types.FileContent)
		if !ok || repairFile.FileData.URL != sdkModelCallFileURL {
			t.Fatalf("repair file = %#v", messages[0].Content[0])
		}
	})
}

// AI SDK: packages/workflow/src/do-generate-step.test.ts > doGenerateStep > enforces tool choice %s
func TestSDKModelCallGenerateEnforcesToolChoice(t *testing.T) {
	for _, choice := range []types.ToolChoice{types.RequiredToolChoice(), types.SpecificToolChoice("lookup")} {
		t.Run(string(choice.Type), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				sdkModelCallSetup(sdkModelCallText("Answer"))
				testOptions.Tools = workflowai.ToolSet{"lookup": {InputSchema: sdkModelCallCitySchema}}
				testOptions.ToolChoice = &choice
				run := sdkModelCallRunAgent(t, true, sdkModelCallPrompt())
				if run.err != nil {
					t.Fatal(run.err)
				}
				if want := (&ai.ToolChoiceViolationError{ToolChoice: choice}).Error(); run.result.Error != want {
					t.Fatalf("error = %q, want %q", run.result.Error, want)
				}
			})
		})
	}
}

// AI SDK: packages/workflow/src/do-generate-step.test.ts > doGenerateStep > rejects tool calls with tool choice none without repairing them
func TestSDKModelCallGenerateToolChoiceNone(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkModelCallSetup(toolCalls(call("call-1", "lookup", `invalid json`)), sdkModelCallText("unused"))
		testOptions.Tools = workflowai.ToolSet{"lookup": {InputSchema: sdkModelCallQuerySchema}}
		none := types.NoneToolChoice()
		testOptions.ToolChoice = &none
		repaired := false
		testOptions.RepairToolCall = func(context.Context, ai.ToolCallRepairOptions) (*types.ToolCall, error) {
			repaired = true
			return nil, nil
		}
		run := sdkModelCallRunAgent(t, true, sdkModelCallPrompt())
		if run.err != nil {
			t.Fatal(run.err)
		}
		want := (&ai.ToolChoiceViolationError{ToolChoice: none}).Error()
		if len(run.result.Steps[0].ToolCalls) != 0 || run.result.Error != want || repaired {
			t.Fatalf("result = %+v, repaired = %v", run.result, repaired)
		}
	})
}

// AI SDK: packages/workflow/src/model-call-iterator.test.ts > modelCallIterator > marks tool input rejected by the original schema as invalid
func TestSDKModelCallSchemaRejectsInput(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkModelCallSetup(toolCalls(call("call-1", "readFile", `{"path":"etc/hosts"}`)), sdkModelCallText("Sorry."))
		testOptions.Tools = sdkModelCallReadFileTools()
		run := sdkModelCallRunAgent(t, false, sdkModelCallPrompt())
		if run.err != nil {
			t.Fatal(run.err)
		}
		calls := run.result.Steps[0].ToolCalls
		if len(calls) != 1 || !calls[0].Invalid || calls[0].ToolName != "readFile" || !strings.Contains(calls[0].Error, "Invalid input for tool readFile") {
			t.Fatalf("tool calls = %+v", calls)
		}
	})
}

// AI SDK: packages/workflow/src/model-call-iterator.test.ts > modelCallIterator > repairs tool input rejected by the original schema
func TestSDKModelCallRepairsSchemaRejection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkModelCallSetup(toolCalls(call("call-1", "readFile", `{"path":"etc/hosts"}`)), sdkModelCallText("Done."))
		testOptions.Tools = sdkModelCallReadFileTools()
		var mu sync.Mutex
		var causes []error
		testOptions.RepairToolCall = func(_ context.Context, options ai.ToolCallRepairOptions) (*types.ToolCall, error) {
			mu.Lock()
			defer mu.Unlock()
			causes = append(causes, options.Error)
			fixed := options.ToolCall
			fixed.RawArguments = `{"path":"/etc/hosts"}`
			return &fixed, nil
		}
		run := sdkModelCallRunAgent(t, false, sdkModelCallPrompt())
		if run.err != nil {
			t.Fatal(run.err)
		}
		mu.Lock()
		defer mu.Unlock()
		var invalid *ai.InvalidToolInputError
		if len(causes) != 1 || !errors.As(causes[0], &invalid) {
			t.Fatalf("repair errors = %v", causes)
		}
		calls := run.result.Steps[0].ToolCalls
		if len(calls) != 1 || calls[0].Invalid || !sdkModelCallJSONEqual(calls[0].Input, json.RawMessage(`{"path":"/etc/hosts"}`)) {
			t.Fatalf("tool calls = %+v", calls)
		}
	})
}

// AI SDK: packages/workflow/src/model-call-iterator.test.ts > modelCallIterator > calls repairToolCall with step-safe tools for original-schema failures
func TestSDKModelCallRepairOptions(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkModelCallSetup(toolCalls(call("call-1", "readFile", `{"path":"etc/hosts"}`)), sdkModelCallText("Done."))
		testOptions.Tools = sdkModelCallReadFileTools()
		var mu sync.Mutex
		var got ai.ToolCallRepairOptions
		testOptions.RepairToolCall = func(_ context.Context, options ai.ToolCallRepairOptions) (*types.ToolCall, error) {
			mu.Lock()
			defer mu.Unlock()
			got = options
			fixed := options.ToolCall
			fixed.RawArguments = `{"path":"/etc/hosts"}`
			return &fixed, nil
		}
		if run := sdkModelCallRunAgent(t, false, sdkModelCallPrompt()); run.err != nil {
			t.Fatal(run.err)
		}
		mu.Lock()
		defer mu.Unlock()
		if !slices.ContainsFunc(got.Tools, func(tool types.Tool) bool { return tool.Name == "readFile" }) {
			t.Fatalf("tools = %+v", got.Tools)
		}
		schema, err := got.InputSchema("readFile")
		if err != nil || schema["type"] != "object" {
			t.Fatalf("input schema = %v, %v", schema, err)
		}
		var invalid *ai.InvalidToolInputError
		if !errors.As(got.Error, &invalid) || invalid.ToolInput != `{"path":"etc/hosts"}` {
			t.Fatalf("repair error = %#v", got.Error)
		}
	})
}

// AI SDK: packages/workflow/src/model-call-iterator.test.ts > modelCallIterator > does not repair a tool call a second time after the model step repaired it
func TestSDKModelCallRepairsOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkModelCallSetup(toolCalls(call("call-1", "readFile", `{"path":"etc/hosts"}`)), sdkModelCallText("Sorry."))
		testOptions.Tools = sdkModelCallReadFileTools()
		var mu sync.Mutex
		repairs := 0
		testOptions.RepairToolCall = func(_ context.Context, options ai.ToolCallRepairOptions) (*types.ToolCall, error) {
			mu.Lock()
			defer mu.Unlock()
			repairs++
			unchanged := options.ToolCall
			return &unchanged, nil
		}
		run := sdkModelCallRunAgent(t, false, sdkModelCallPrompt())
		if run.err != nil {
			t.Fatal(run.err)
		}
		mu.Lock()
		defer mu.Unlock()
		calls := run.result.Steps[0].ToolCalls
		if repairs != 1 || len(calls) != 1 || !calls[0].Invalid {
			t.Fatalf("%d repairs, tool calls = %+v", repairs, calls)
		}
	})
}

// AI SDK: packages/workflow/src/model-call-iterator.test.ts > modelCallIterator > generation settings > merges defined prepareStep overrides
func TestSDKModelCallPrepareStepSettings(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkModelCallSetup(sdkModelCallText("done"))
		temperature, topP := 0.2, 0.5
		testOptions.Temperature, testOptions.TopP = &temperature, &topP
		testOptions.PrepareStep = func(context.Context, workflowai.PrepareStepInfo) (workflowai.PrepareStepResult, error) {
			var result workflowai.PrepareStepResult
			overrideTopP, maxOutputTokens := 0.9, 256
			result.GenerationSettings = workflowai.GenerationSettings{TopP: &overrideTopP, MaxOutputTokens: &maxOutputTokens}
			return result, nil
		}
		if run := sdkModelCallRunAgent(t, false, sdkModelCallPrompt()); run.err != nil {
			t.Fatal(run.err)
		}
		options := sdkModelCallLM.options(0)
		if options.Temperature == nil || *options.Temperature != 0.2 || options.TopP == nil || *options.TopP != 0.9 || options.MaxTokens == nil || *options.MaxTokens != 256 {
			t.Fatalf("temperature = %v, top p = %v, max tokens = %v", options.Temperature, options.TopP, options.MaxTokens)
		}
	})
}

// AI SDK: packages/workflow/src/model-call-iterator.test.ts > modelCallIterator > activeTools > passes no tools to the model when prepareStep returns an empty list
func TestSDKModelCallEmptyActiveTools(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkModelCallSetup(sdkModelCallText("done"))
		testOptions.Tools = workflowai.ToolSet{"hidden": {InputSchema: map[string]any{"type": "object"}}}
		testOptions.PrepareStep = func(context.Context, workflowai.PrepareStepInfo) (workflowai.PrepareStepResult, error) {
			return workflowai.PrepareStepResult{ActiveTools: []string{}}, nil
		}
		if run := sdkModelCallRunAgent(t, false, sdkModelCallPrompt()); run.err != nil {
			t.Fatal(run.err)
		}
		if tools := sdkModelCallLM.options(0).Tools; len(tools) != 0 {
			t.Fatalf("tools = %+v", tools)
		}
	})
}

// AI SDK: packages/workflow/src/model-call-iterator.test.ts > modelCallIterator > telemetry > filters runtime context before passing it to the model step (include=%s)
func TestSDKModelCallTelemetryRuntimeContext(t *testing.T) {
	tests := []struct {
		name    string
		include map[string]bool
		want    map[string]any
	}{
		{"undefined", nil, map[string]any{}},
		{"false", map[string]bool{"requestId": false, "secret": false}, map[string]any{}},
		{"true", map[string]bool{"requestId": true, "secret": false}, map[string]any{"requestId": "request-1"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				sdkModelCallSetup(sdkModelCallText("done"))
				integration := &sdkModelCallIntegration{}
				runtimeContext := map[string]any{"requestId": "request-1", "secret": "private"}
				testOptions.RuntimeContext = runtimeContext
				testOptions.Telemetry = &telemetry.Options{IncludeRuntimeContext: tt.include, Integrations: []telemetry.TelemetryIntegration{integration}}
				if run := sdkModelCallRunAgent(t, false, sdkModelCallPrompt()); run.err != nil {
					t.Fatal(run.err)
				}
				got := integration.stepRuntimeContexts()
				if len(got) == 0 || len(got[0]) != len(tt.want) || len(tt.want) > 0 && !reflect.DeepEqual(got[0], tt.want) {
					t.Fatalf("runtime contexts = %v, want %v", got, tt.want)
				}
				if !reflect.DeepEqual(runtimeContext, map[string]any{"requestId": "request-1", "secret": "private"}) {
					t.Fatalf("runtime context changed to %v", runtimeContext)
				}
			})
		})
	}
}

// AI SDK: packages/workflow/src/model-call-iterator.test.ts > modelCallIterator > telemetry > uses runtime context updated by prepareStep for model-step telemetry
func TestSDKModelCallTelemetryPreparedRuntimeContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkModelCallSetup(sdkModelCallText("done"))
		integration := &sdkModelCallIntegration{}
		testOptions.RuntimeContext = map[string]any{"requestId": "initial"}
		testOptions.PrepareStep = func(context.Context, workflowai.PrepareStepInfo) (workflowai.PrepareStepResult, error) {
			return workflowai.PrepareStepResult{RuntimeContext: map[string]any{"requestId": "updated", "secret": "private"}}, nil
		}
		testOptions.Telemetry = &telemetry.Options{IncludeRuntimeContext: map[string]bool{"requestId": true}, Integrations: []telemetry.TelemetryIntegration{integration}}
		if run := sdkModelCallRunAgent(t, false, sdkModelCallPrompt()); run.err != nil {
			t.Fatal(run.err)
		}
		got := integration.stepRuntimeContexts()
		if len(got) == 0 || !reflect.DeepEqual(got[0], map[string]any{"requestId": "updated"}) {
			t.Fatalf("runtime contexts = %v", got)
		}
	})
}

// AI SDK: packages/workflow/src/model-call-iterator.test.ts > modelCallIterator > telemetry > should expose provider metadata on model-call end
func TestSDKModelCallTelemetryProviderMetadata(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		finish := sdkModelCallFinish(types.FinishReasonStop)
		finish.ProviderMetadata = json.RawMessage(`{"gateway":{"generationId":"generation-id"}}`)
		sdkModelCallSetup(sdkModelCallChunks(finish))
		integration := &sdkModelCallIntegration{}
		testOptions.Telemetry = &telemetry.Options{Integrations: []telemetry.TelemetryIntegration{integration}}
		if run := sdkModelCallRunAgent(t, false, sdkModelCallPrompt()); run.err != nil {
			t.Fatal(run.err)
		}
		integration.mu.Lock()
		defer integration.mu.Unlock()
		want := map[string]any{"gateway": map[string]any{"generationId": "generation-id"}}
		if len(integration.callEnds) != 1 || !reflect.DeepEqual(integration.callEnds[0].ProviderMetadata, want) {
			t.Fatalf("model call ends = %+v", integration.callEnds)
		}
	})
}

// AI SDK: packages/workflow/src/model-call-iterator.test.ts > modelCallIterator > telemetry > records the model-call start when the model step fails
func TestSDKModelCallTelemetryFailedModelCall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkModelCallSetup(failWith(errors.New("rejected")))
		integration := &sdkModelCallIntegration{}
		testOptions.Telemetry = &telemetry.Options{Integrations: []telemetry.TelemetryIntegration{integration}}
		run := sdkModelCallRunAgent(t, false, sdkModelCallPrompt())
		if run.err == nil || !strings.Contains(run.err.Error(), "rejected") {
			t.Fatalf("err = %v", run.err)
		}
		integration.mu.Lock()
		defer integration.mu.Unlock()
		var starts []string
		for _, event := range integration.events {
			if event == "onStepStart" || event == "onLanguageModelCallStart" || event == "onLanguageModelCallEnd" {
				starts = append(starts, event)
			}
		}
		if want := []string{"onStepStart", "onLanguageModelCallStart"}; !slices.Equal(starts, want) {
			t.Fatalf("events = %v, want %v", integration.events, want)
		}
	})
}

// AI SDK: packages/workflow/src/model-call-iterator.test.ts > modelCallIterator > conversation prompt > passes initial instructions and messages to prepareStep
func TestSDKModelCallPrepareStepInitialPrompt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkModelCallSetup(sdkModelCallText("done"))
		testOptions.Instructions = "initial instructions"
		var infos []workflowai.PrepareStepInfo
		testOptions.PrepareStep = func(_ context.Context, info workflowai.PrepareStepInfo) (workflowai.PrepareStepResult, error) {
			infos = append(infos, info)
			return workflowai.PrepareStepResult{}, nil
		}
		messages := []workflowai.ModelMessage{user("initial message")}
		if run := sdkModelCallRunAgent(t, false, messages); run.err != nil {
			t.Fatal(run.err)
		}
		if len(infos) == 0 {
			t.Fatal("PrepareStep was not called")
		}
		if infos[0].InitialInstructions != "initial instructions" || !reflect.DeepEqual(infos[0].InitialMessages, messages) {
			t.Fatalf("initial instructions = %v, messages = %v", infos[0].InitialInstructions, infos[0].InitialMessages)
		}
	})
}

// AI SDK: packages/workflow/src/model-call-iterator.test.ts > modelCallIterator > conversation prompt > preserves assistant text alongside tool calls for the next step
func TestSDKModelCallTextAlongsideToolCalls(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		testCall := call("call-1", "testTool", `{"query":"test"}`)
		sdkModelCallSetup(sdkModelCallChunks(
			provider.StreamChunk{Type: provider.ChunkTypeTextStart, ID: "t"},
			provider.StreamChunk{Type: provider.ChunkTypeText, ID: "t", Text: "I found the answer before calling the tool."},
			provider.StreamChunk{Type: provider.ChunkTypeTextEnd, ID: "t"},
			provider.StreamChunk{Type: provider.ChunkTypeToolCall, ToolCall: &testCall},
			sdkModelCallFinish(types.FinishReasonToolCalls),
		), sdkModelCallText("done"))
		testOptions.Tools = sdkModelCallTestTools("testTool")
		if run := sdkModelCallRunAgent(t, false, sdkModelCallPrompt()); run.err != nil {
			t.Fatal(run.err)
		}
		prompt := sdkModelCallLM.prompt(1)
		if len(prompt) != 3 || prompt[1].Role != types.RoleAssistant || prompt[2].Role != types.RoleTool {
			t.Fatalf("prompt = %+v", prompt)
		}
		if got := sdkModelCallContentTypes(prompt[1]); !slices.Equal(got, []string{"text", "tool-call"}) {
			t.Fatalf("assistant content = %v", got)
		}
		if text := prompt[1].Content[0].(types.TextContent).Text; text != "I found the answer before calling the tool." {
			t.Fatalf("text = %q", text)
		}
		result := prompt[2].Content[0].(types.ToolResultContent)
		if result.ToolCallID != "call-1" || result.ToolName != "testTool" {
			t.Fatalf("tool result = %+v", result)
		}
	})
}

// AI SDK: packages/workflow/src/model-call-iterator.test.ts > modelCallIterator > conversation prompt > interleaves provider-executed results while keeping client results in a tool message
func TestSDKModelCallInterleavesProviderResults(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		providerCall := types.ToolCall{ID: "provider-call", ToolName: "providerTool", RawArguments: `{"query":"docs"}`, ProviderExecuted: true}
		clientCall := call("client-call", "clientTool", `{"id":1}`)
		sdkModelCallSetup(sdkModelCallChunks(
			provider.StreamChunk{Type: provider.ChunkTypeToolCall, ToolCall: &providerCall},
			provider.StreamChunk{Type: provider.ChunkTypeToolResult, ToolResult: &types.ToolResult{
				ToolCallID: "provider-call", ToolName: "providerTool", Result: []any{map[string]any{"toolName": "clientTool"}}, ProviderExecuted: true,
			}},
			provider.StreamChunk{Type: provider.ChunkTypeToolCall, ToolCall: &clientCall},
			sdkModelCallFinish(types.FinishReasonToolCalls),
		), sdkModelCallText("done"))
		testOptions.Tools = sdkModelCallTestTools("clientTool")
		if run := sdkModelCallRunAgent(t, false, sdkModelCallPrompt()); run.err != nil {
			t.Fatal(run.err)
		}
		prompt := sdkModelCallLM.prompt(1)
		if len(prompt) != 3 || prompt[1].Role != types.RoleAssistant || prompt[2].Role != types.RoleTool {
			t.Fatalf("prompt = %+v", prompt)
		}
		if got := sdkModelCallContentTypes(prompt[1]); !slices.Equal(got, []string{"tool-call", "tool-result", "tool-call"}) {
			t.Fatalf("assistant content = %v", got)
		}
		if id := prompt[1].Content[1].(types.ToolResultContent).ToolCallID; id != "provider-call" {
			t.Fatalf("assistant tool result = %s", id)
		}
		if len(prompt[2].Content) != 1 || prompt[2].Content[0].(types.ToolResultContent).ToolCallID != "client-call" {
			t.Fatalf("tool message = %+v", prompt[2])
		}
	})
}

// AI SDK: packages/workflow/src/model-call-iterator.test.ts > modelCallIterator > conversation prompt > preserves generated files in the assistant message history
func TestSDKModelCallGeneratedFiles(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkModelCallSetup(sdkModelCallChunks(
			provider.StreamChunk{Type: provider.ChunkTypeTextStart, ID: "t"},
			provider.StreamChunk{Type: provider.ChunkTypeText, ID: "t", Text: "Download the generated file."},
			provider.StreamChunk{Type: provider.ChunkTypeTextEnd, ID: "t"},
			sdkModelCallFile("file-content"),
			sdkModelCallFinish(types.FinishReasonStop),
		))
		run := sdkModelCallRunAgent(t, false, sdkModelCallPrompt())
		if run.err != nil {
			t.Fatal(run.err)
		}
		sdkModelCallCheckFiles(t, run.result.Steps[0], "ZmlsZS1jb250ZW50")
		last := run.result.Messages[len(run.result.Messages)-1]
		if last.Role != workflowai.RoleAssistant || len(last.Content) != 2 || last.Content[0].Text != "Download the generated file." ||
			last.Content[1].Type != workflowai.PartFile || last.Content[1].Data != "ZmlsZS1jb250ZW50" || last.Content[1].MediaType != "text/plain" {
			t.Fatalf("last message = %+v", last)
		}
	})
}

// AI SDK: packages/workflow/src/model-call-iterator.test.ts > modelCallIterator > conversation prompt > preserves sources in step results without adding them to message history
func TestSDKModelCallSources(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkModelCallSetup(sdkModelCallChunks(
			provider.StreamChunk{Type: provider.ChunkTypeTextStart, ID: "t"},
			provider.StreamChunk{Type: provider.ChunkTypeText, ID: "t", Text: "Answer with a source."},
			provider.StreamChunk{Type: provider.ChunkTypeTextEnd, ID: "t"},
			sdkModelCallSource(),
			sdkModelCallFinish(types.FinishReasonStop),
		))
		run := sdkModelCallRunAgent(t, false, sdkModelCallPrompt())
		if run.err != nil {
			t.Fatal(run.err)
		}
		step := run.result.Steps[0]
		if got := sdkModelCallPartTypes(step.Content); !slices.Equal(got, []string{"text", "source"}) {
			t.Fatalf("content = %v", got)
		}
		sdkModelCallCheckSources(t, step)
		last := run.result.Messages[len(run.result.Messages)-1]
		if got := sdkModelCallPartTypes(last.Content); !slices.Equal(got, []string{"text"}) {
			t.Fatalf("last message = %+v", last)
		}
	})
}

// AI SDK: packages/workflow/src/model-call-iterator.test.ts > modelCallIterator > conversation prompt > preserves file and text order in a subsequent tool-call turn
func TestSDKModelCallFileAndTextOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		testCall := call("call-1", "testTool", `{"query":"test"}`)
		sdkModelCallSetup(sdkModelCallChunks(
			sdkModelCallFile("file-before-text"),
			provider.StreamChunk{Type: provider.ChunkTypeTextStart, ID: "t"},
			provider.StreamChunk{Type: provider.ChunkTypeText, ID: "t", Text: "Use this file."},
			provider.StreamChunk{Type: provider.ChunkTypeTextEnd, ID: "t"},
			provider.StreamChunk{Type: provider.ChunkTypeToolCall, ToolCall: &testCall},
			sdkModelCallFinish(types.FinishReasonToolCalls),
		), sdkModelCallText("done"))
		testOptions.Tools = sdkModelCallTestTools("testTool")
		if run := sdkModelCallRunAgent(t, false, sdkModelCallPrompt()); run.err != nil {
			t.Fatal(run.err)
		}
		assistant := sdkModelCallLM.prompt(1)[1]
		if got := sdkModelCallContentTypes(assistant); !slices.Equal(got, []string{"file", "text", "tool-call"}) {
			t.Fatalf("assistant content = %v", got)
		}
		if file := assistant.Content[0].(types.FileContent); string(file.Data) != "file-before-text" || file.MediaType != "text/plain" {
			t.Fatalf("file = %+v", file)
		}
	})
}

// AI SDK: packages/workflow/src/model-call-iterator.test.ts > modelCallIterator > conversation prompt > omits empty text from file-bearing tool-call message history
func TestSDKModelCallOmitsEmptyText(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		testCall := call("call-1", "testTool", `{"query":"test"}`)
		sdkModelCallSetup(sdkModelCallChunks(
			sdkModelCallFile("file-content"),
			provider.StreamChunk{Type: provider.ChunkTypeTextStart, ID: "t"},
			provider.StreamChunk{Type: provider.ChunkTypeText, ID: "t", Text: ""},
			provider.StreamChunk{Type: provider.ChunkTypeTextEnd, ID: "t"},
			provider.StreamChunk{Type: provider.ChunkTypeToolCall, ToolCall: &testCall},
			sdkModelCallFinish(types.FinishReasonToolCalls),
		), sdkModelCallText("done"))
		testOptions.Tools = sdkModelCallTestTools("testTool")
		run := sdkModelCallRunAgent(t, false, sdkModelCallPrompt())
		if run.err != nil {
			t.Fatal(run.err)
		}
		// The step keeps the empty text, and the tool's result, which the
		// agent adds to the step.
		if got := sdkModelCallPartTypes(run.result.Steps[0].Content); !slices.Equal(got, []string{"file", "text", "tool-call", "tool-result"}) {
			t.Fatalf("step content = %v", got)
		}
		assistant := run.result.Messages[slices.IndexFunc(run.result.Messages, func(message workflowai.ModelMessage) bool { return message.Role == workflowai.RoleAssistant })]
		if got := sdkModelCallPartTypes(assistant.Content); !slices.Equal(got, []string{"file", "tool-call"}) {
			t.Fatalf("assistant message = %v", got)
		}
		if got := sdkModelCallContentTypes(sdkModelCallLM.prompt(1)[1]); !slices.Equal(got, []string{"file", "tool-call"}) {
			t.Fatalf("next prompt assistant content = %v", got)
		}
	})
}

// AI SDK: packages/workflow/src/model-call-iterator.test.ts > modelCallIterator > providerMetadata to providerOptions mapping > preserves text provider metadata in assistant continuation messages
func TestSDKModelCallTextMetadataToOptions(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		metadata := json.RawMessage(`{"openai":{"itemId":"message-1","phase":"final_answer"}}`)
		sdkModelCallSetup(sdkModelCallChunks(
			provider.StreamChunk{Type: provider.ChunkTypeTextStart, ID: "t", ProviderMetadata: metadata},
			provider.StreamChunk{Type: provider.ChunkTypeText, ID: "t", Text: "An answer."},
			provider.StreamChunk{Type: provider.ChunkTypeTextEnd, ID: "t"},
			sdkModelCallFinish(types.FinishReasonStop),
		))
		run := sdkModelCallRunAgent(t, false, sdkModelCallPrompt())
		if run.err != nil {
			t.Fatal(run.err)
		}
		content := run.result.Steps[0].Content
		if len(content) != 1 || content[0].Text != "An answer." || !sdkModelCallJSONEqual(content[0].ProviderMetadata, metadata) {
			t.Fatalf("step content = %+v", content)
		}
		last := run.result.Messages[len(run.result.Messages)-1]
		if len(last.Content) != 1 || !sdkModelCallJSONEqual(sdkModelCallMarshal(t, last.Content[0].ProviderOptions), metadata) {
			t.Fatalf("assistant message = %+v", last)
		}
	})
}

// AI SDK: packages/workflow/src/model-call-iterator.test.ts > modelCallIterator > providerMetadata to providerOptions mapping > replays reasoning and provider metadata with tool calls in emission order
func TestSDKModelCallReasoningAndToolCallMetadata(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		metadata := json.RawMessage(`{"anthropic":{"signature":"reasoning-signature"},"openai":{"reasoningEncryptedContent":"encrypted-reasoning"}}`)
		toolMetadata := map[string]any{"openai": map[string]any{"itemId": "function-call-item", "reasoningEncryptedContent": "encrypted-reasoning"}}
		testCall := call("call-1", "testTool", `{"query":"test"}`)
		testCall.ProviderMetadata = toolMetadata
		sdkModelCallSetup(sdkModelCallChunks(
			provider.StreamChunk{Type: provider.ChunkTypeReasoningStart, ID: "r", ProviderMetadata: metadata},
			provider.StreamChunk{Type: provider.ChunkTypeReasoning, ID: "r", Reasoning: "Inspect the configuration first."},
			provider.StreamChunk{Type: provider.ChunkTypeReasoningEnd, ID: "r"},
			provider.StreamChunk{Type: provider.ChunkTypeTextStart, ID: "t"},
			provider.StreamChunk{Type: provider.ChunkTypeText, ID: "t", Text: "I will inspect the configuration."},
			provider.StreamChunk{Type: provider.ChunkTypeTextEnd, ID: "t"},
			provider.StreamChunk{Type: provider.ChunkTypeToolCall, ToolCall: &testCall},
			sdkModelCallFinish(types.FinishReasonToolCalls),
		), sdkModelCallText("done"))
		testOptions.Tools = sdkModelCallTestTools("testTool")
		run := sdkModelCallRunAgent(t, false, sdkModelCallPrompt())
		if run.err != nil {
			t.Fatal(run.err)
		}
		reasoning := run.result.Steps[0].Content[0]
		if reasoning.Type != workflowai.PartReasoning || reasoning.Text != "Inspect the configuration first." || !sdkModelCallJSONEqual(reasoning.ProviderMetadata, metadata) {
			t.Fatalf("reasoning = %+v", reasoning)
		}
		assistant := sdkModelCallLM.prompt(1)[1]
		if got := sdkModelCallContentTypes(assistant); !slices.Equal(got, []string{"reasoning", "text", "tool-call"}) {
			t.Fatalf("assistant content = %v", got)
		}
		if options := assistant.Content[0].(types.ReasoningContent).ProviderOptions; !sdkModelCallJSONEqual(sdkModelCallMarshal(t, options), metadata) {
			t.Fatalf("reasoning options = %v", options)
		}
		if options := assistant.Content[2].(types.ToolCallContent).ProviderOptions; !reflect.DeepEqual(options, toolMetadata) {
			t.Fatalf("tool call options = %v", options)
		}
	})
}

// AI SDK: packages/workflow/src/model-call-iterator.test.ts > modelCallIterator > providerMetadata to providerOptions mapping > should preserve providerMetadata as providerOptions in tool-call messages
// AI SDK: packages/workflow/src/model-call-iterator.test.ts > modelCallIterator > providerMetadata to providerOptions mapping > should preserve providerMetadata for multiple parallel tool calls
// AI SDK: packages/workflow/src/model-call-iterator.test.ts > modelCallIterator > providerMetadata to providerOptions mapping > should handle mixed tool calls with and without providerMetadata
// AI SDK: packages/workflow/src/model-call-iterator.test.ts > modelCallIterator > providerMetadata to providerOptions mapping > preserves OpenAI itemId in tool-call provider metadata
// AI SDK: packages/workflow/src/model-call-iterator.test.ts > modelCallIterator > providerMetadata to providerOptions mapping > preserves all OpenAI tool-call provider metadata fields
// AI SDK: packages/workflow/src/model-call-iterator.test.ts > modelCallIterator > providerMetadata to providerOptions mapping > preserves mixed provider metadata on tool calls
func TestSDKModelCallToolCallMetadataToOptions(t *testing.T) {
	type metadataCall struct {
		name     string
		metadata map[string]any
	}
	tests := []struct {
		name  string
		calls []metadataCall
	}{
		{"google thought signature", []metadataCall{{"testTool", map[string]any{"google": map[string]any{"thoughtSignature": "sig_abc123_test_signature"}}}}},
		{"parallel calls", []metadataCall{
			{"weatherTool", map[string]any{"google": map[string]any{"thoughtSignature": "sig_weather_123"}}},
			{"newsTool", map[string]any{"google": map[string]any{"thoughtSignature": "sig_news_456"}}},
		}},
		{"mixed calls", []metadataCall{
			{"toolWithMeta", map[string]any{"vertex": map[string]any{"thoughtSignature": "sig_vertex_789"}}},
			{"toolWithoutMeta", nil},
		}},
		{"openai item ID", []metadataCall{{"testTool", map[string]any{"openai": map[string]any{"itemId": "fc_0402bf2d292dd7ed00697a35fb10e0819ab0098545c4d0d7f5"}}}}},
		{"all openai fields", []metadataCall{{"testTool", map[string]any{"openai": map[string]any{
			"itemId": "fc_0402bf2d292dd7ed00697a35fb10e0819ab0098545c4d0d7f5", "someOtherField": "should-be-preserved",
		}}}}},
		{"mixed providers", []metadataCall{{"testTool", map[string]any{
			"google": map[string]any{"thoughtSignature": "sig_gemini_preserved"},
			"openai": map[string]any{"itemId": "fc_should_be_preserved"},
		}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var calls []types.ToolCall
				var names []string
				for i, c := range tt.calls {
					toolCall := call("call-"+strconv.Itoa(i+1), c.name, `{}`)
					toolCall.ProviderMetadata = c.metadata
					calls = append(calls, toolCall)
					names = append(names, c.name)
				}
				sdkModelCallSetup(toolCalls(calls...), sdkModelCallText("done"))
				testOptions.Tools = sdkModelCallTestTools(names...)
				if run := sdkModelCallRunAgent(t, false, sdkModelCallPrompt()); run.err != nil {
					t.Fatal(run.err)
				}
				assistant := sdkModelCallLM.prompt(1)[1]
				for _, c := range tt.calls {
					index := slices.IndexFunc(assistant.Content, func(part types.ContentPart) bool {
						toolCall, ok := part.(types.ToolCallContent)
						return ok && toolCall.ToolName == c.name
					})
					if index < 0 {
						t.Fatalf("assistant content = %+v", assistant.Content)
					}
					if options := assistant.Content[index].(types.ToolCallContent).ProviderOptions; !reflect.DeepEqual(options, c.metadata) && (len(options) != 0 || c.metadata != nil) {
						t.Fatalf("%s options = %v, want %v", c.name, options, c.metadata)
					}
				}
			})
		})
	}
}

// AI SDK: packages/workflow/src/model-call-iterator.test.ts > modelCallIterator > providerMetadata to providerOptions mapping > should not add providerOptions when providerMetadata is undefined
func TestSDKModelCallToolCallWithoutMetadata(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkModelCallSetup(toolCalls(call("call-1", "testTool", `{"query":"test"}`)), sdkModelCallText("done"))
		testOptions.Tools = sdkModelCallTestTools("testTool")
		if run := sdkModelCallRunAgent(t, false, sdkModelCallPrompt()); run.err != nil {
			t.Fatal(run.err)
		}
		assistant := sdkModelCallLM.prompt(1)[1]
		index := slices.IndexFunc(assistant.Content, func(part types.ContentPart) bool { _, ok := part.(types.ToolCallContent); return ok })
		if index < 0 {
			t.Fatalf("assistant content = %+v", assistant.Content)
		}
		if options := assistant.Content[index].(types.ToolCallContent).ProviderOptions; options != nil {
			t.Fatalf("provider options = %v", options)
		}
	})
}

// AI SDK: packages/workflow/src/model-call-iterator.test.ts > modelCallIterator > runtimeContext and toolsContext > applies current contexts to the reconstructed step and yield value
func TestSDKModelCallStepContexts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkModelCallSetup(sdkModelCallText("done"))
		runtimeContext := map[string]any{"tenantId": "tenant_123"}
		toolsContext := map[string]any{"weather": map[string]any{"unit": "celsius"}}
		testOptions.RuntimeContext = runtimeContext
		testOptions.ToolsContext = toolsContext
		run := sdkModelCallRunAgent(t, false, sdkModelCallPrompt())
		if run.err != nil {
			t.Fatal(run.err)
		}
		step := run.result.Steps[0]
		if step.StepNumber != 0 || !sdkModelCallJSONEqual(sdkModelCallMarshal(t, step.RuntimeContext), sdkModelCallMarshal(t, runtimeContext)) ||
			!sdkModelCallJSONEqual(sdkModelCallMarshal(t, step.ToolsContext), sdkModelCallMarshal(t, toolsContext)) {
			t.Fatalf("step contexts = %v, %v", step.RuntimeContext, step.ToolsContext)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-stream-step-model.test.ts > WorkflowAgent stream step model > reports the model that produced each step like ToolLoopAgent and generate
// AI SDK: packages/workflow/src/workflow-agent-stream-step-model.test.ts > WorkflowAgent stream step model > reports the configured model when the response carries a different model ID
func TestSDKModelCallStepModel(t *testing.T) {
	for _, responseModelID := range []string{"mock-model-id", "mock-model-id-2026-01-01"} {
		for _, generate := range []bool{false, true} {
			t.Run(responseModelID+" "+sdkModelCallMode(generate), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					sdkModelCallSetup(sdkModelCallToolResponse(responseModelID, nil), sdkModelCallTextResponse("It is 18°C.", responseModelID, nil))
					testOptions.Tools = sdkModelCallWeatherTools()
					run := sdkModelCallRunAgent(t, generate, []workflowai.ModelMessage{user(sdkModelCallWeatherPrompt)})
					if run.err != nil {
						t.Fatal(run.err)
					}
					if len(run.result.Steps) != 2 {
						t.Fatalf("steps = %+v", run.result.Steps)
					}
					for _, step := range run.result.Steps {
						if !generate && step.Response.ModelID != responseModelID {
							t.Fatalf("response model ID = %q", step.Response.ModelID)
						}
						if got := sdkModelCallMarshal(t, step.Model); !sdkModelCallJSONEqual(got, json.RawMessage(`{"provider":"mock-provider","modelId":"mock-model-id"}`)) {
							t.Fatalf("model = %s", got)
						}
					}
				})
			})
		}
	}
}

// AI SDK: packages/workflow/src/workflow-agent-stream-invalid-tool-calls.test.ts > WorkflowAgent stream step results match ToolLoopAgent > keeps the tool call for ${scenario.name}
func TestSDKModelCallInvalidToolCallStepResults(t *testing.T) {
	for _, scenario := range sdkModelCallInvalidScenarios() {
		for _, generate := range []bool{false, true} {
			t.Run(scenario.name+" "+sdkModelCallMode(generate), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					sdkModelCallSetup(scenario.responses...)
					testOptions.Tools = sdkModelCallWeatherTools()
					run := sdkModelCallRunAgent(t, generate, []workflowai.ModelMessage{user(sdkModelCallWeatherPrompt)})
					if run.err != nil {
						t.Fatal(run.err)
					}
					if got := sdkModelCallLM.callCount(); got != 2 {
						t.Fatalf("%d model calls", got)
					}
					step := run.result.Steps[0]
					if got := sdkModelCallPartTypes(step.Content); !slices.Equal(got, scenario.contentTypes) {
						t.Fatalf("content = %v, want %v", got, scenario.contentTypes)
					}
					if len(step.ToolCalls) != 1 || step.ToolCalls[0].ToolName != scenario.toolName || step.ToolCalls[0].Invalid != scenario.invalid {
						t.Fatalf("tool calls = %+v", step.ToolCalls)
					}
					if step.ToolCalls[0].Dynamic != scenario.invalid {
						t.Fatalf("dynamic = %v", step.ToolCalls[0].Dynamic)
					}
					var toolMessages int
					for _, message := range sdkModelCallLM.prompt(1) {
						if message.Role == types.RoleTool {
							toolMessages++
						}
					}
					if toolMessages != 1 {
						t.Fatalf("%d tool messages", toolMessages)
					}
				})
			})
		}
	}
}

// AI SDK: packages/workflow/src/workflow-agent-stream-invalid-tool-calls.test.ts > WorkflowAgent stream chunks > writes the same chunks for ${scenario.name}
func TestSDKModelCallInvalidToolCallChunks(t *testing.T) {
	for _, scenario := range sdkModelCallInvalidScenarios() {
		t.Run(scenario.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				sdkModelCallSetup(scenario.responses...)
				testOptions.Tools = sdkModelCallWeatherTools()
				run := sdkModelCallRunAgent(t, false, []workflowai.ModelMessage{user(sdkModelCallWeatherPrompt)})
				if run.err != nil {
					t.Fatal(run.err)
				}
				if got := sdkModelCallTypes(run.parts); !slices.Equal(got, scenario.chunkTypes) {
					t.Fatalf("parts = %v, want %v", got, scenario.chunkTypes)
				}
			})
		})
	}
}

// AI SDK: packages/workflow/src/workflow-agent-content-retention.test.ts > WorkflowAgent.stream content retention > retains model files and sources in results, callbacks, and message history
func TestSDKModelCallRetainsFilesAndSources(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkModelCallSetup(sdkModelCallChunks(
			provider.StreamChunk{Type: provider.ChunkTypeStreamStart},
			sdkModelCallSource(),
			sdkModelCallFile("file-content"),
			provider.StreamChunk{Type: provider.ChunkTypeTextStart, ID: "text-1"},
			provider.StreamChunk{Type: provider.ChunkTypeText, ID: "text-1", Text: "answer"},
			provider.StreamChunk{Type: provider.ChunkTypeTextEnd, ID: "text-1"},
			sdkModelCallFinish(types.FinishReasonStop),
		))
		var stepEnd workflowai.StepResult
		var end workflowai.Result
		testOptions.OnStepEnd = func(_ context.Context, step workflowai.StepResult) { stepEnd = step }
		testOptions.OnEnd = func(_ context.Context, result workflowai.EndEvent) { end = result.Result }
		run := sdkModelCallRunAgent(t, false, []workflowai.ModelMessage{user("question")})
		if run.err != nil {
			t.Fatal(run.err)
		}
		step := run.result.Steps[0]
		if got := sdkModelCallPartTypes(step.Content); !slices.Equal(got, []string{"source", "file", "text"}) {
			t.Fatalf("content = %v", got)
		}
		sdkModelCallCheckFiles(t, step, "ZmlsZS1jb250ZW50")
		sdkModelCallCheckSources(t, step)
		// The run's result went through JSON, so compare the encodings.
		if !sdkModelCallJSONEqual(sdkModelCallMarshal(t, stepEnd), sdkModelCallMarshal(t, step)) || len(end.Steps) != 1 || !sdkModelCallJSONEqual(sdkModelCallMarshal(t, end.Steps[0]), sdkModelCallMarshal(t, step)) {
			t.Fatalf("OnStepEnd = %+v, OnEnd steps = %+v", stepEnd, end.Steps)
		}
		assistant := run.result.Messages[slices.IndexFunc(run.result.Messages, func(message workflowai.ModelMessage) bool { return message.Role == workflowai.RoleAssistant })]
		if got := sdkModelCallPartTypes(assistant.Content); !slices.Equal(got, []string{"file", "text"}) || assistant.Content[0].Data != "ZmlsZS1jb250ZW50" {
			t.Fatalf("assistant message = %+v", assistant)
		}
		if !sdkModelCallJSONEqual(sdkModelCallMarshal(t, end.Messages), sdkModelCallMarshal(t, run.result.Messages)) {
			t.Fatalf("OnEnd messages = %+v", end.Messages)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-content-retention.test.ts > WorkflowAgent.stream content retention > retains custom parts in step results and subsequent model requests
func TestSDKModelCallRetainsCustomParts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		metadata := json.RawMessage(`{"openai":{"itemId":"cmp_1","encryptedContent":"encrypted"}}`)
		search := call("call-1", "search", `{}`)
		sdkModelCallSetup(sdkModelCallChunks(
			provider.StreamChunk{Type: provider.ChunkTypeStreamStart},
			provider.StreamChunk{Type: provider.ChunkTypeCustom, CustomContent: &types.CustomContent{Kind: "openai.compaction", ProviderMetadata: metadata}},
			provider.StreamChunk{Type: provider.ChunkTypeToolCall, ToolCall: &search},
			sdkModelCallFinish(types.FinishReasonToolCalls),
		), sdkModelCallText("done"))
		testOptions.Tools = sdkModelCallTestTools("search")
		run := sdkModelCallRunAgent(t, false, []workflowai.ModelMessage{user("question")})
		if run.err != nil {
			t.Fatal(run.err)
		}
		if content := run.result.Steps[0].Content; len(content) == 0 || content[0].Type != "custom" || !sdkModelCallJSONEqual(content[0].ProviderMetadata, metadata) {
			t.Fatalf("step content = %+v", content)
		}
		custom, ok := sdkModelCallLM.prompt(1)[1].Content[0].(types.CustomContent)
		if !ok || custom.Kind != "openai.compaction" || !sdkModelCallJSONEqual(sdkModelCallMarshal(t, custom.ProviderOptions), metadata) {
			t.Fatalf("prompt part = %#v", sdkModelCallLM.prompt(1)[1].Content[0])
		}
		assistant := run.result.Messages[slices.IndexFunc(run.result.Messages, func(message workflowai.ModelMessage) bool { return message.Role == workflowai.RoleAssistant })]
		if !slices.Contains(sdkModelCallPartTypes(assistant.Content), "custom") {
			t.Fatalf("assistant message = %+v", assistant)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-stream-error.test.ts > WorkflowAgent.stream error parts > forwards the error part and resolves with its original value
func TestSDKModelCallStreamErrorPart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkModelCallSetup(sdkModelCallErrorStream(errors.New("terminal model error")))
		var ended workflowai.Result
		testOptions.OnEnd = func(_ context.Context, result workflowai.EndEvent) { ended = result.Result }
		run := sdkModelCallRunAgent(t, false, []workflowai.ModelMessage{user("trigger the terminal error")})
		if run.err != nil {
			t.Fatal(run.err)
		}
		if run.result.FinishReason != "error" || run.result.Error != "terminal model error" {
			t.Fatalf("result = %+v", run.result)
		}
		if !slices.ContainsFunc(run.parts, func(part map[string]any) bool {
			return part["type"] == "error" && part["error"] == "terminal model error"
		}) {
			t.Fatalf("parts = %v", run.parts)
		}
		last := run.result.Messages[len(run.result.Messages)-1]
		sdkModelCallCheckPartialAnswer(t, last)
		if messages := run.result.Steps[0].Response.Messages; len(messages) != 1 || !reflect.DeepEqual(messages[0], last) {
			t.Fatalf("response messages = %+v", messages)
		}
		if !reflect.DeepEqual(ended.Messages, run.result.Messages) || ended.Text != "A partial answer." {
			t.Fatalf("OnEnd result = %+v", ended)
		}
	})
}

// An error part without a message still ends the turn with an error.
//
// AI SDK: packages/workflow/src/workflow-agent-stream-error.test.ts > WorkflowAgent.stream error parts > preserves a falsy error value
// AI SDK: packages/workflow/src/workflow-agent-stream-error.test.ts > WorkflowAgent.stream error parts > preserves the presence of an undefined error value
func TestSDKModelCallStreamErrorPartWithoutMessage(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkModelCallSetup(sdkModelCallErrorStream(nil))
		run := sdkModelCallRunAgent(t, false, []workflowai.ModelMessage{user("trigger the terminal error")})
		if run.err != nil {
			t.Fatal(run.err)
		}
		if run.result.Error == "" || run.result.FinishReason != "error" {
			t.Fatalf("result = %+v", run.result)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-stream-error.test.ts > WorkflowAgent.stream error parts > calls onError once for a model stream error part
func TestSDKModelCallStreamErrorCallsOnErrorOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkModelCallSetup(sdkModelCallErrorStream(errors.New("terminal model error")))
		testOptions.OnError = func(ctx context.Context, err error) { sdkModelCallRecordOnce(ctx, err.Error()) }
		if run := sdkModelCallRunAgent(t, false, []workflowai.ModelMessage{user("trigger the terminal error")}); run.err != nil {
			t.Fatal(run.err)
		}
		if got := sdkModelCallRecorded(); !slices.Equal(got, []string{"terminal model error"}) {
			t.Fatalf("OnError calls = %v", got)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-stream-error.test.ts > WorkflowAgent.stream error parts > reports an incomplete stream without output as a no-output error
func TestSDKModelCallIncompleteStreamError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkModelCallSetup(sdkModelCallChunks(
			provider.StreamChunk{Type: provider.ChunkTypeStreamStart},
			provider.StreamChunk{Type: provider.ChunkTypeResponseMetadata, ResponseMetadata: &provider.ResponseMetadata{ID: "response-1", ModelID: "mock-model"}},
		))
		testOptions.OnError = func(ctx context.Context, err error) { sdkModelCallRecordOnce(ctx, err.Error()) }
		run := sdkModelCallRunAgent(t, false, []workflowai.ModelMessage{user("trigger an incomplete stream")})
		if run.err != nil {
			t.Fatal(run.err)
		}
		if run.result.FinishReason != "other" || run.result.Error != sdkModelCallNoOutput {
			t.Fatalf("result = %+v", run.result)
		}
		if got := sdkModelCallRecorded(); !slices.Equal(got, []string{sdkModelCallNoOutput}) {
			t.Fatalf("OnError calls = %v", got)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-warnings.test.ts > WorkflowAgent provider warnings > logs the warning of a model call once, like ToolLoopAgent
// AI SDK: packages/workflow/src/workflow-agent-warnings.test.ts > WorkflowAgent provider warnings > logs the warning of every model call in a two-step run
// AI SDK: packages/workflow/src/workflow-agent-warnings.test.ts > WorkflowAgent provider warnings > logs nothing when AI_SDK_LOG_WARNINGS is false
// AI SDK: packages/workflow/src/workflow-agent-warnings.test.ts > WorkflowAgent provider warnings > passes the same warnings, provider, and model to a custom logger
// AI SDK: packages/workflow/src/workflow-agent-warnings.test.ts > WorkflowAgent provider warnings > logs nothing when the model returns no warnings
func TestSDKModelCallLogsWarnings(t *testing.T) {
	warning := types.Warning{Type: "unsupported", Feature: "temperature"}
	formatted := ai.FormatWarning(warning, "mock-provider", "mock-model-id")
	tests := []struct {
		name      string
		responses func() []func() ([]provider.StreamChunk, error)
		disable   bool
		want      []ai.LogWarningsOptions
	}{
		{
			name: "one model call",
			responses: func() []func() ([]provider.StreamChunk, error) {
				return []func() ([]provider.StreamChunk, error){sdkModelCallTextResponse("Done.", "mock-model-id", []types.Warning{warning})}
			},
			want: []ai.LogWarningsOptions{{Warnings: []types.Warning{warning}, Provider: "mock-provider", Model: "mock-model-id"}},
		},
		{
			name: "two model calls",
			responses: func() []func() ([]provider.StreamChunk, error) {
				return []func() ([]provider.StreamChunk, error){
					sdkModelCallToolResponse("mock-model-id", []types.Warning{warning}),
					sdkModelCallTextResponse("It is 18°C.", "mock-model-id", []types.Warning{warning}),
				}
			},
			want: []ai.LogWarningsOptions{
				{Warnings: []types.Warning{warning}, Provider: "mock-provider", Model: "mock-model-id"},
				{Warnings: []types.Warning{warning}, Provider: "mock-provider", Model: "mock-model-id"},
			},
		},
		{
			name: "AI_SDK_LOG_WARNINGS false",
			responses: func() []func() ([]provider.StreamChunk, error) {
				return []func() ([]provider.StreamChunk, error){sdkModelCallTextResponse("Done.", "mock-model-id", []types.Warning{warning})}
			},
			disable: true,
		},
		{
			name: "no warnings",
			responses: func() []func() ([]provider.StreamChunk, error) {
				return []func() ([]provider.StreamChunk, error){sdkModelCallTextResponse("Done.", "mock-model-id", nil)}
			},
		},
	}
	for _, tt := range tests {
		for _, generate := range []bool{false, true} {
			t.Run(tt.name+" "+sdkModelCallMode(generate), func(t *testing.T) {
				var mu sync.Mutex
				var logged []ai.LogWarningsOptions
				ai.SetLogWarnings(func(options ai.LogWarningsOptions) {
					mu.Lock()
					defer mu.Unlock()
					logged = append(logged, options)
				})
				t.Cleanup(func() { ai.SetLogWarnings(nil) })
				if tt.disable {
					t.Setenv("AI_SDK_LOG_WARNINGS", "false")
				}
				synctest.Test(t, func(t *testing.T) {
					sdkModelCallSetup(tt.responses()...)
					testOptions.Tools = sdkModelCallWeatherTools()
					if run := sdkModelCallRunAgent(t, generate, []workflowai.ModelMessage{user(sdkModelCallWeatherPrompt)}); run.err != nil {
						t.Fatal(run.err)
					}
				})
				mu.Lock()
				defer mu.Unlock()
				if !reflect.DeepEqual(logged, tt.want) {
					t.Fatalf("logged = %+v, want %+v", logged, tt.want)
				}
				for _, options := range logged {
					if got := ai.FormatWarning(options.Warnings[0], options.Provider, options.Model); got != formatted {
						t.Fatalf("warning = %q, want %q", got, formatted)
					}
				}
			})
		}
	}
}

// sdkModelCallModel is a language model that answers each call, streamed or
// generated, with the next scripted chunks.
type sdkModelCallModel struct {
	mu        sync.Mutex
	urls      map[string][]string
	responses []func() ([]provider.StreamChunk, error)
	calls     []*provider.GenerateOptions
}

func (m *sdkModelCallModel) SpecificationVersion() string { return "v3" }

func (m *sdkModelCallModel) Provider() string { return "mock-provider" }

func (m *sdkModelCallModel) ModelID() string { return "mock-model-id" }

func (m *sdkModelCallModel) SupportsTools() bool { return true }

func (m *sdkModelCallModel) SupportsStructuredOutput() bool { return false }

func (m *sdkModelCallModel) SupportsImageInput() bool { return false }

func (m *sdkModelCallModel) SupportedURLs() map[string][]string { return m.urls }

func (m *sdkModelCallModel) next(ctx context.Context, options *provider.GenerateOptions) ([]provider.StreamChunk, error) {
	m.mu.Lock()
	if len(m.calls) >= len(m.responses) {
		m.mu.Unlock()
		return nil, errors.New("unexpected model call")
	}
	respond := m.responses[len(m.calls)]
	m.calls = append(m.calls, options)
	m.mu.Unlock()
	chunks, err := respond()
	if errors.Is(err, errHang) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return chunks, err
}

func (m *sdkModelCallModel) DoStream(ctx context.Context, options *provider.GenerateOptions) (provider.TextStream, error) {
	chunks, err := m.next(ctx, options)
	if err != nil {
		return nil, err
	}
	return &fakeStream{chunks: chunks}, nil
}

// DoGenerate answers with the content of the scripted chunks.
func (m *sdkModelCallModel) DoGenerate(ctx context.Context, options *provider.GenerateOptions) (*types.GenerateResult, error) {
	chunks, err := m.next(ctx, options)
	if err != nil {
		return nil, err
	}
	result := &types.GenerateResult{}
	for _, chunk := range chunks {
		switch chunk.Type {
		case provider.ChunkTypeStreamStart:
			result.Warnings = append(result.Warnings, chunk.Warnings...)
		case provider.ChunkTypeResponseMetadata:
			result.ResponseMetadata = &types.ResponseMetadata{ID: chunk.ResponseMetadata.ID, ModelID: chunk.ResponseMetadata.ModelID}
		case provider.ChunkTypeText:
			result.Content = append(result.Content, types.TextContent{Text: chunk.Text})
		case provider.ChunkTypeReasoning:
			result.Content = append(result.Content, types.ReasoningContent{Text: chunk.Reasoning})
		case provider.ChunkTypeToolCall:
			result.ToolCalls = append(result.ToolCalls, *chunk.ToolCall)
		case provider.ChunkTypeFinish:
			result.FinishReason = chunk.FinishReason
			if chunk.Usage != nil {
				result.Usage = *chunk.Usage
			}
		}
	}
	return result, nil
}

func (m *sdkModelCallModel) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.calls)
}

// options returns the options of model call i.
func (m *sdkModelCallModel) options(i int) *provider.GenerateOptions {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls[i]
}

// prompt returns the prompt messages of model call i.
func (m *sdkModelCallModel) prompt(i int) []types.Message {
	return m.options(i).Prompt.Messages
}

// sdkModelCallLM is the model sdkModelCallRunner gives its agent.
var sdkModelCallLM *sdkModelCallModel

type sdkModelCallChat struct {
	Messages []workflowai.ModelMessage
	Generate bool
}

func (sdkModelCallChat) Workflow(workflowai.Result) {}

// sdkModelCallRunner runs an agent with testOptions and sdkModelCallLM,
// streaming unless the input asks to generate.
func sdkModelCallRunner(ctx context.Context, input sdkModelCallChat) (workflowai.Result, error) {
	options := testOptions
	options.Model = sdkModelCallLM
	callOptions := testCallOptions
	callOptions.Messages = input.Messages
	agent := workflowai.NewWorkflowAgent(options)
	if input.Generate {
		return agent.Generate(ctx, callOptions)
	}
	return agent.Stream(ctx, callOptions)
}

// sdkModelCallResult is the outcome of a run of sdkModelCallRunner: its
// result, the raw parts of its stream, and the names of its steps.
type sdkModelCallResult struct {
	result workflowai.Result
	err    error
	parts  []map[string]any
	steps  []string
}

// sdkModelCallRunAgent runs sdkModelCallRunner on messages. A streamed run's
// raw stream parts are read until the stream closes.
func sdkModelCallRunAgent(t *testing.T, generate bool, messages []workflowai.ModelMessage) sdkModelCallResult {
	t.Helper()
	world := testworld.New(worlds.Runners(workflow.Use(sdkModelCallRunner)))
	defer world.Close()
	ctx := workflow.NewContext(t.Context(), workflow.NewClient(world))
	run, err := workflow.Start(ctx, sdkModelCallChat{Messages: messages, Generate: generate})
	if err != nil {
		t.Fatal(err)
	}
	var out sdkModelCallResult
	if !generate {
		reader, err := run.Reader(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for {
			data, err := reader.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			var part map[string]any
			if err := json.Unmarshal(data.Data, &part); err != nil {
				t.Fatal(err)
			}
			out.parts = append(out.parts, part)
		}
	}
	out.result, out.err = run.Value(ctx)
	out.steps = stepNames(t, world, run.ID())
	return out
}

// sdkModelCallSetup resets the agent options and scripts the model's
// responses.
func sdkModelCallSetup(responses ...func() ([]provider.StreamChunk, error)) {
	sdkModelCallLM = &sdkModelCallModel{responses: responses}
	testOptions = workflowai.WorkflowAgentOptions{}
	testCallOptions = workflowai.CallOptions{}
	sdkModelCallRecords.mu.Lock()
	sdkModelCallRecords.values = nil
	sdkModelCallRecords.mu.Unlock()
}

// sdkModelCallRecords holds what sdkModelCallRecordOnce recorded.
var sdkModelCallRecords struct {
	mu     sync.Mutex
	values []string
}

// sdkModelCallRecordOnce records value in an inline step, so a replay of the
// callback that calls it does not record it again.
func sdkModelCallRecordOnce(ctx context.Context, value string) {
	_, _ = workflow.InlineStep(ctx, sdkModelCallRecord, value)
}

func sdkModelCallRecord(_ context.Context, value string) (struct{}, error) {
	sdkModelCallRecords.mu.Lock()
	defer sdkModelCallRecords.mu.Unlock()
	sdkModelCallRecords.values = append(sdkModelCallRecords.values, value)
	return struct{}{}, nil
}

func sdkModelCallRecorded() []string {
	sdkModelCallRecords.mu.Lock()
	defer sdkModelCallRecords.mu.Unlock()
	return slices.Clone(sdkModelCallRecords.values)
}

// sdkModelCallIntegration records the telemetry events it receives.
type sdkModelCallIntegration struct {
	telemetry.NoopTelemetryIntegration
	mu         sync.Mutex
	events     []string
	stepStarts []telemetry.TelemetryStepStartEvent
	callEnds   []telemetry.LanguageModelCallEndEvent
}

func (i *sdkModelCallIntegration) record(event string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.events = append(i.events, event)
}

func (i *sdkModelCallIntegration) OnStart(ctx context.Context, _ telemetry.TelemetryStartEvent) context.Context {
	i.record("onStart")
	return ctx
}

func (i *sdkModelCallIntegration) OnStepStart(ctx context.Context, e telemetry.TelemetryStepStartEvent) context.Context {
	i.mu.Lock()
	i.stepStarts = append(i.stepStarts, e)
	i.mu.Unlock()
	i.record("onStepStart")
	return ctx
}

func (i *sdkModelCallIntegration) OnLanguageModelCallStart(ctx context.Context, _ telemetry.LanguageModelCallStartEvent) context.Context {
	i.record("onLanguageModelCallStart")
	return ctx
}

func (i *sdkModelCallIntegration) OnLanguageModelCallEnd(_ context.Context, e telemetry.LanguageModelCallEndEvent) {
	i.mu.Lock()
	i.callEnds = append(i.callEnds, e)
	i.mu.Unlock()
	i.record("onLanguageModelCallEnd")
}

func (i *sdkModelCallIntegration) OnError(_ context.Context, _ telemetry.TelemetryErrorEvent) {
	i.record("onError")
}

// stepRuntimeContexts returns the runtime context of each step start.
func (i *sdkModelCallIntegration) stepRuntimeContexts() []map[string]any {
	i.mu.Lock()
	defer i.mu.Unlock()
	var out []map[string]any
	for _, event := range i.stepStarts {
		out = append(out, event.RuntimeContext)
	}
	return out
}

// sdkModelCallScenario is a scenario of
// workflow-agent-stream-invalid-tool-calls.test.ts.
type sdkModelCallScenario struct {
	name         string
	responses    []func() ([]provider.StreamChunk, error)
	contentTypes []string
	toolName     string
	invalid      bool
	chunkTypes   []string
}

func sdkModelCallInvalidScenarios() []sdkModelCallScenario {
	second := []string{"start-step", "reset-step", "model-call-start", "model-call-response-metadata", "text-start", "text-delta", "text-end", "model-call-end", "finish"}
	first := func(types ...string) []string {
		return slices.Concat([]string{"reset-step", "model-call-start", "model-call-response-metadata", "text-start", "text-delta", "text-end"}, types, []string{"finish-step"}, second)
	}
	toolCall := func(c types.ToolCall) func() ([]provider.StreamChunk, error) {
		return sdkModelCallResponse("mock-model-id", nil, types.FinishReasonToolCalls, "Checking the weather.", &c)
	}
	return []sdkModelCallScenario{
		{
			name:         "an unknown tool",
			responses:    []func() ([]provider.StreamChunk, error){toolCall(call("missing-call", "missing", `{"city":"London"}`)), sdkModelCallTextResponse("It is 18°C.", "mock-model-id", nil)},
			contentTypes: []string{"text", "tool-call", "tool-error"},
			toolName:     "missing",
			invalid:      true,
			chunkTypes:   first("tool-call", "tool-error", "model-call-end"),
		},
		{
			name:         "tool input that fails the schema",
			responses:    []func() ([]provider.StreamChunk, error){toolCall(call("weather-call", "weather", `{"city":42}`)), sdkModelCallTextResponse("It is 18°C.", "mock-model-id", nil)},
			contentTypes: []string{"text", "tool-call", "tool-error"},
			toolName:     "weather",
			invalid:      true,
			// The SDK writes the calls of tools with their own validator after
			// the model call ends. Go tools are validated with JSON schema in
			// the model call step, like the SDK's JSON schema tools.
			chunkTypes: first("tool-call", "tool-error", "model-call-end"),
		},
		{
			name:         "a valid tool call",
			responses:    []func() ([]provider.StreamChunk, error){sdkModelCallToolResponse("mock-model-id", nil), sdkModelCallTextResponse("It is 18°C.", "mock-model-id", nil)},
			contentTypes: []string{"text", "tool-call", "tool-result"},
			toolName:     "weather",
			chunkTypes:   first("tool-call", "model-call-end", "tool-result"),
		},
	}
}

// sdkModelCallResponse returns the chunks the SDK tests' createAgentModel
// streams for a response: a stream start with the warnings, the response
// metadata, a text block, the tool call when there is one, and the finish.
func sdkModelCallResponse(modelID string, warnings []types.Warning, finish types.FinishReason, text string, toolCall *types.ToolCall) func() ([]provider.StreamChunk, error) {
	return func() ([]provider.StreamChunk, error) {
		chunks := []provider.StreamChunk{
			{Type: provider.ChunkTypeStreamStart, Warnings: warnings},
			{Type: provider.ChunkTypeResponseMetadata, ResponseMetadata: &provider.ResponseMetadata{ID: "response-fixture", Timestamp: time.Unix(0, 0), ModelID: modelID}},
			{Type: provider.ChunkTypeTextStart, ID: "text-0"},
			{Type: provider.ChunkTypeText, ID: "text-0", Text: text},
			{Type: provider.ChunkTypeTextEnd, ID: "text-0"},
		}
		if toolCall != nil {
			c := *toolCall
			chunks = append(chunks, provider.StreamChunk{Type: provider.ChunkTypeToolCall, ToolCall: &c})
		}
		return append(chunks, sdkModelCallFinish(finish)), nil
	}
}

func sdkModelCallTextResponse(text, modelID string, warnings []types.Warning) func() ([]provider.StreamChunk, error) {
	return sdkModelCallResponse(modelID, warnings, types.FinishReasonStop, text, nil)
}

func sdkModelCallToolResponse(modelID string, warnings []types.Warning) func() ([]provider.StreamChunk, error) {
	weather := call("weather-call", "weather", `{"city":"London"}`)
	return sdkModelCallResponse(modelID, warnings, types.FinishReasonToolCalls, "Checking the weather.", &weather)
}

// sdkModelCallText answers with a text block and a stop finish.
func sdkModelCallText(value string) func() ([]provider.StreamChunk, error) {
	return sdkModelCallChunks(
		provider.StreamChunk{Type: provider.ChunkTypeTextStart, ID: "text-1"},
		provider.StreamChunk{Type: provider.ChunkTypeText, ID: "text-1", Text: value},
		provider.StreamChunk{Type: provider.ChunkTypeTextEnd, ID: "text-1"},
		sdkModelCallFinish(types.FinishReasonStop),
	)
}

// sdkModelCallErrorStream answers with reasoning, a partial answer, an
// error part carrying err, and an error finish.
func sdkModelCallErrorStream(err error) func() ([]provider.StreamChunk, error) {
	return sdkModelCallChunks(
		provider.StreamChunk{Type: provider.ChunkTypeStreamStart},
		provider.StreamChunk{Type: provider.ChunkTypeReasoningStart, ID: "reasoning-1"},
		provider.StreamChunk{Type: provider.ChunkTypeReasoning, ID: "reasoning-1", Reasoning: "Thinking."},
		provider.StreamChunk{Type: provider.ChunkTypeReasoningEnd, ID: "reasoning-1"},
		provider.StreamChunk{Type: provider.ChunkTypeTextStart, ID: "text-1"},
		provider.StreamChunk{Type: provider.ChunkTypeText, ID: "text-1", Text: "A partial answer."},
		provider.StreamChunk{Type: provider.ChunkTypeTextEnd, ID: "text-1"},
		provider.StreamChunk{Type: provider.ChunkTypeError, Err: err},
		sdkModelCallFinish(types.FinishReasonError),
	)
}

func sdkModelCallChunks(chunks ...provider.StreamChunk) func() ([]provider.StreamChunk, error) {
	return func() ([]provider.StreamChunk, error) { return slices.Clone(chunks), nil }
}

func sdkModelCallFinish(reason types.FinishReason) provider.StreamChunk {
	input, output, total := int64(1), int64(1), int64(2)
	return provider.StreamChunk{Type: provider.ChunkTypeFinish, FinishReason: reason, Usage: &types.Usage{InputTokens: &input, OutputTokens: &output, TotalTokens: &total}}
}

// sdkModelCallFile is a generated text/plain file with content.
func sdkModelCallFile(content string) provider.StreamChunk {
	return provider.StreamChunk{Type: provider.ChunkTypeFile, GeneratedFileContent: &types.GeneratedFileContent{MediaType: "text/plain", Data: []byte(content)}}
}

func sdkModelCallSource() provider.StreamChunk {
	return provider.StreamChunk{Type: provider.ChunkTypeSource, SourceContent: &types.SourceContent{SourceType: "url", ID: "source-1", URL: "https://example.com/source", Title: "Example source"}}
}

func sdkModelCallAPIError(status int, headers map[string]string) error {
	return &providererrors.ProviderError{Provider: "mock-provider", StatusCode: status, Message: "model call failed with status " + strconv.Itoa(status), ResponseHeaders: headers}
}

const (
	sdkModelCallFileURL       = "https://example.com/file.pdf"
	sdkModelCallWeatherPrompt = "What is the weather in London?"
	sdkModelCallNoOutput      = "No output generated. The model stream ended without a finish chunk."
)

func sdkModelCallPrompt() []workflowai.ModelMessage {
	return []workflowai.ModelMessage{user("test")}
}

func sdkModelCallFileMessage() workflowai.ModelMessage {
	return workflowai.ModelMessage{Role: workflowai.RoleUser, Content: []workflowai.ContentPart{{Type: workflowai.PartFile, Data: sdkModelCallFileURL, MediaType: "application/pdf"}}}
}

func sdkModelCallMode(generate bool) string {
	if generate {
		return "generate"
	}
	return "stream"
}

var (
	sdkModelCallCitySchema = map[string]any{
		"type":       "object",
		"properties": map[string]any{"city": map[string]any{"type": "string"}},
		"required":   []any{"city"},
	}
	sdkModelCallQuerySchema = map[string]any{
		"type":       "object",
		"properties": map[string]any{"query": map[string]any{"type": "string"}},
		"required":   []any{"query"},
	}
)

func sdkModelCallWeatherTools() workflowai.ToolSet {
	return workflowai.ToolSet{"weather": {InputSchema: sdkModelCallCitySchema, Execute: workflowai.ExecuteStep(sdkModelCallWeather)}}
}

func sdkModelCallWeather(context.Context, struct {
	City string `json:"city"`
}) (map[string]any, error) {
	return map[string]any{"temperature": 18}, nil
}

// sdkModelCallReadFileTools has a readFile tool whose schema accepts only
// absolute paths.
func sdkModelCallReadFileTools() workflowai.ToolSet {
	schema := map[string]any{
		"type":       "object",
		"properties": map[string]any{"path": map[string]any{"type": "string", "pattern": "^/"}},
		"required":   []any{"path"},
	}
	return workflowai.ToolSet{"readFile": {InputSchema: schema, Execute: workflowai.ExecuteStep(sdkModelCallReadFile)}}
}

func sdkModelCallReadFile(_ context.Context, input struct {
	Path string `json:"path"`
}) (string, error) {
	return "contents of " + input.Path, nil
}

// sdkModelCallTestTools has a tool for each name that accepts any input and
// returns a success result.
func sdkModelCallTestTools(names ...string) workflowai.ToolSet {
	tools := workflowai.ToolSet{}
	for _, name := range names {
		tools[name] = workflowai.Tool{InputSchema: map[string]any{"type": "object"}, Execute: workflowai.ExecuteFunc(func(context.Context, json.RawMessage, workflowai.ToolCallOptions) (any, error) {
			return map[string]any{"result": "success"}, nil
		})}
	}
	return tools
}

func sdkModelCallSleep(_ context.Context, input struct {
	Seconds int `json:"seconds"`
}) (string, error) {
	time.Sleep(time.Duration(input.Seconds) * time.Second)
	return "slept", nil
}

// sdkModelCallCheckPartialAnswer checks the assistant message of
// sdkModelCallErrorStream.
func sdkModelCallCheckPartialAnswer(t *testing.T, message workflowai.ModelMessage) {
	t.Helper()
	if message.Role != workflowai.RoleAssistant || len(message.Content) != 2 ||
		message.Content[0].Type != workflowai.PartReasoning || message.Content[0].Text != "Thinking." ||
		message.Content[1].Type != workflowai.PartText || message.Content[1].Text != "A partial answer." {
		t.Fatalf("assistant message = %+v", message)
	}
}

// sdkModelCallCheckFiles checks that step has one file with data base64.
func sdkModelCallCheckFiles(t *testing.T, step workflowai.StepResult, data string) {
	t.Helper()
	var got []map[string]any
	_ = json.Unmarshal(sdkModelCallMarshal(t, step.Files), &got)
	if len(got) != 1 || got[0]["base64"] != data {
		t.Fatalf("files = %v", got)
	}
	if _, err := base64.StdEncoding.DecodeString(data); err != nil {
		t.Fatal(err)
	}
}

// sdkModelCallCheckSources checks that step has the source of
// sdkModelCallSource.
func sdkModelCallCheckSources(t *testing.T, step workflowai.StepResult) {
	t.Helper()
	want := `[{"type":"source","sourceType":"url","id":"source-1","url":"https://example.com/source","title":"Example source"}]`
	if got := sdkModelCallMarshal(t, step.Sources); !sdkModelCallJSONEqual(got, json.RawMessage(want)) {
		t.Fatalf("sources = %s", got)
	}
}

func sdkModelCallMarshal(t *testing.T, v any) json.RawMessage {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// sdkModelCallJSONEqual reports whether a and b encode the same JSON value.
func sdkModelCallJSONEqual(a, b json.RawMessage) bool {
	var x, y any
	if json.Unmarshal(a, &x) != nil || json.Unmarshal(b, &y) != nil {
		return false
	}
	return reflect.DeepEqual(x, y)
}

func sdkModelCallTypes(parts []map[string]any) []string {
	var out []string
	for _, part := range parts {
		out = append(out, part["type"].(string))
	}
	return out
}

func sdkModelCallPartTypes(parts []workflowai.ContentPart) []string {
	var out []string
	for _, part := range parts {
		out = append(out, part.Type)
	}
	return out
}

// sdkModelCallContentTypes returns the types of a prompt message's content.
func sdkModelCallContentTypes(message types.Message) []string {
	var out []string
	for _, part := range message.Content {
		switch part.(type) {
		case types.TextContent:
			out = append(out, "text")
		case types.ReasoningContent:
			out = append(out, "reasoning")
		case types.FileContent:
			out = append(out, "file")
		case types.ToolCallContent:
			out = append(out, "tool-call")
		case types.ToolResultContent:
			out = append(out, "tool-result")
		case types.CustomContent:
			out = append(out, "custom")
		default:
			out = append(out, reflect.TypeOf(part).Name())
		}
	}
	return out
}
