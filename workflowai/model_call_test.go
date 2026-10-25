package workflowai_test

import (
	"context"
	"encoding/json"
	"errors"
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
	promptutils "github.com/digitallysavvy/go-ai/pkg/providerutils/prompt"
	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/workflowai"
	"github.com/eriicafes/go-workflow/worlds/testworld"
)

// retryableError is a provider error that may succeed when retried.
type retryableError struct{}

func (retryableError) Error() string { return "overloaded" }

func (retryableError) IsRetryable() bool { return true }

func failWith(err error) func() ([]provider.StreamChunk, error) {
	return func() ([]provider.StreamChunk, error) { return nil, err }
}

func TestStreamRetriesOpeningModelCall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		setup(failWith(retryableError{}), failWith(retryableError{}), text("Hello!"))
		result, chunks, err := chat(t, []workflowai.ModelMessage{user("hi")})
		if err != nil {
			t.Fatal(err)
		}
		// Retries happen in one step execution, before any part is written.
		want := []string{"start", "start-step", "reset-step", "text-start", "text-delta", "text-end", "finish-step", "finish"}
		if got := chunkTypes(chunks); !slices.Equal(got, want) {
			t.Fatalf("chunks = %v, want %v", got, want)
		}
		if result.Text != "Hello!" || len(testModel.calls) != 3 {
			t.Fatalf("text = %q after %d calls", result.Text, len(testModel.calls))
		}
	})
}

func TestStreamStopsRetryingOpeningModelCall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		setup(failWith(retryableError{}), failWith(retryableError{}), failWith(retryableError{}), text("unused"))
		_, _, err := chat(t, []workflowai.ModelMessage{user("hi")})
		if err == nil || len(testModel.calls) != 3 {
			t.Fatalf("err = %v after %d calls", err, len(testModel.calls))
		}
	})
}

func TestStreamRetryHonorsRetryHeaders(t *testing.T) {
	rateLimited := func(headers map[string]string) error {
		return &providererrors.ProviderError{StatusCode: 429, Message: "rate limited", ResponseHeaders: headers}
	}
	tests := []struct {
		name    string
		headers map[string]string
		want    time.Duration
	}{
		{"retry-after-ms", map[string]string{"retry-after-ms": "1500", "retry-after": "30"}, 1500 * time.Millisecond},
		{"retry-after seconds", map[string]string{"Retry-After": "10"}, 10 * time.Second},
		{"retry-after date", map[string]string{"Retry-After": "Sat, 01 Jan 2000 00:00:20 GMT"}, 20 * time.Second},
		{"over a minute", map[string]string{"Retry-After": "120"}, 2 * time.Second},
		{"invalid", map[string]string{"Retry-After": "soon"}, 2 * time.Second},
		{"no headers", nil, 2 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var calls []time.Time
				record := func(respond func() ([]provider.StreamChunk, error)) func() ([]provider.StreamChunk, error) {
					return func() ([]provider.StreamChunk, error) {
						calls = append(calls, time.Now())
						return respond()
					}
				}
				setup(record(failWith(rateLimited(tt.headers))), record(text("Hello!")))
				if _, _, err := chat(t, []workflowai.ModelMessage{user("hi")}); err != nil {
					t.Fatal(err)
				}
				if len(calls) != 2 {
					t.Fatalf("%d calls", len(calls))
				}
				if got := calls[1].Sub(calls[0]); got != tt.want {
					t.Fatalf("waited %v, want %v", got, tt.want)
				}
			})
		})
	}
}

func TestStreamRetryBackoffIsCapped(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls []time.Time
		record := func(respond func() ([]provider.StreamChunk, error)) func() ([]provider.StreamChunk, error) {
			return func() ([]provider.StreamChunk, error) {
				calls = append(calls, time.Now())
				return respond()
			}
		}
		var responses []func() ([]provider.StreamChunk, error)
		for range 7 {
			responses = append(responses, record(failWith(retryableError{})))
		}
		setup(append(responses, record(text("Hello!")))...)
		maxRetries := 7
		testOptions.MaxRetries = &maxRetries
		if _, _, err := chat(t, []workflowai.ModelMessage{user("hi")}); err != nil {
			t.Fatal(err)
		}
		var waits []time.Duration
		for i := 1; i < len(calls); i++ {
			waits = append(waits, calls[i].Sub(calls[i-1]))
		}
		want := []time.Duration{2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 32 * time.Second, time.Minute, time.Minute}
		if !slices.Equal(waits, want) {
			t.Fatalf("waits = %v, want %v", waits, want)
		}
	})
}

func TestStreamSendsReasoningSettings(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		setup(text("Hi!"))
		level, send := types.ReasoningHigh, false
		testOptions.Reasoning = &level
		testOptions.SendReasoning = &send
		if _, _, err := chat(t, []workflowai.ModelMessage{user("hi")}); err != nil {
			t.Fatal(err)
		}
		call := testModel.calls[0]
		if call.Reasoning == nil || *call.Reasoning != types.ReasoningHigh || call.SendReasoning == nil || *call.SendReasoning {
			t.Fatalf("reasoning = %v, send reasoning = %v", call.Reasoning, call.SendReasoning)
		}
	})
}

func TestStreamRepairsToolCall(t *testing.T) {
	tests := []struct {
		name      string
		call      types.ToolCall
		wantError any
	}{
		{"invalid input", call("c1", "getWeather", `{"town":1}`), &ai.InvalidToolInputError{}},
		{"unknown tool", call("c1", "get_weather", `{"city":"Lagos"}`), &ai.NoSuchToolError{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				setup(toolCalls(tt.call), text("It is 21 degrees."))
				var mu sync.Mutex
				var got ai.ToolCallRepairOptions
				testOptions.RepairToolCall = func(_ context.Context, options ai.ToolCallRepairOptions) (*types.ToolCall, error) {
					mu.Lock()
					defer mu.Unlock()
					got = options
					return &types.ToolCall{ToolName: "getWeather", RawArguments: `{"city":"Lagos"}`}, nil
				}
				_, chunks, err := chat(t, []workflowai.ModelMessage{user("weather in Lagos?")})
				if err != nil {
					t.Fatal(err)
				}
				mu.Lock()
				defer mu.Unlock()
				if reflect.TypeOf(got.Error) != reflect.TypeOf(tt.wantError) || got.ToolCall.ID != "c1" || len(got.Tools) == 0 {
					t.Fatalf("repair options = %+v", got)
				}
				if schema, err := got.InputSchema("getWeather"); err != nil || schema == nil {
					t.Fatalf("input schema = %v, %v", schema, err)
				}
				// The repaired call keeps its ID and executes.
				if weatherRuns != 1 || slices.Contains(chunkTypes(chunks), "tool-input-error") {
					t.Fatalf("weather ran %d times, chunks = %v", weatherRuns, chunkTypes(chunks))
				}
				for _, chunk := range chunks {
					if chunk["type"] == "tool-input-available" && (chunk["toolCallId"] != "c1" || chunk["toolName"] != "getWeather") {
						t.Fatalf("tool call chunk = %v", chunk)
					}
				}
			})
		})
	}
}

func TestStreamUnrepairedToolCall(t *testing.T) {
	tests := []struct {
		name      string
		repair    ai.ToolCallRepairFunction
		wantError string
	}{
		{"no repair", func(context.Context, ai.ToolCallRepairOptions) (*types.ToolCall, error) { return nil, nil }, "invalid input for tool getWeather"},
		{"repair error", func(context.Context, ai.ToolCallRepairOptions) (*types.ToolCall, error) {
			return nil, errors.New("no luck")
		}, "repairing tool call: no luck"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				setup(toolCalls(call("c1", "getWeather", `{"town":1}`)), text("Sorry."))
				testOptions.RepairToolCall = tt.repair
				_, chunks, err := chat(t, []workflowai.ModelMessage{user("weather?")})
				if err != nil {
					t.Fatal(err)
				}
				var errorText string
				for _, chunk := range chunks {
					if chunk["type"] == "tool-input-error" {
						errorText, _ = chunk["errorText"].(string)
					}
				}
				if weatherRuns != 0 || !strings.HasPrefix(errorText, tt.wantError) {
					t.Fatalf("weather ran %d times, error = %q", weatherRuns, errorText)
				}
			})
		})
	}
}

func providerSearch(id string) types.ToolCall {
	return types.ToolCall{ID: id, ToolName: "web_search", RawArguments: `{"query":"flights"}`, ProviderExecuted: true}
}

func providerResult(id string) provider.StreamChunk {
	return provider.StreamChunk{Type: provider.ChunkTypeToolResult, ToolResult: &types.ToolResult{
		ToolCallID: id, ToolName: "web_search", Result: map[string]any{"url": "https://example.com"}, ProviderExecuted: true,
	}}
}

func TestStreamProviderToolResult(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		search := providerSearch("p1")
		setup(func() ([]provider.StreamChunk, error) {
			return []provider.StreamChunk{
				{Type: provider.ChunkTypeToolCall, ToolCall: &search},
				providerResult("p1"),
				{Type: provider.ChunkTypeText, Text: "Found flights."},
				{Type: provider.ChunkTypeFinish, FinishReason: types.FinishReasonStop},
			}, nil
		}, text("Anything else?"))
		result, chunks, err := chat(t, []workflowai.ModelMessage{user("search flights")})
		if err != nil {
			t.Fatal(err)
		}
		var output ai.UIMessageChunk
		for _, chunk := range chunks {
			if chunk["type"] == "tool-output-available" {
				output = chunk
			}
		}
		if output == nil || output["toolCallId"] != "p1" || output["providerExecuted"] != true {
			t.Fatalf("chunks = %v", chunks)
		}
		// The result stays in the assistant message, so the next turn sends
		// it back to the provider with its call.
		if _, _, err := chat(t, append(result.Messages, user("thanks"))); err != nil {
			t.Fatal(err)
		}
		var sent *types.ToolResultContent
		for _, message := range testModel.prompts()[1] {
			for _, part := range message.Content {
				if part, ok := part.(types.ToolResultContent); ok && message.Role == types.RoleAssistant {
					sent = &part
				}
			}
		}
		if sent == nil || sent.ToolCallID != "p1" || !sent.ProviderExecuted || sent.Output == nil || sent.Output.Type != types.ToolResultOutputJSON {
			t.Fatalf("provider result = %+v", sent)
		}
	})
}

func TestStreamDeferredProviderToolResult(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		search := providerSearch("p1")
		weather := call("c1", "getWeather", `{"city":"Lagos"}`)
		setup(func() ([]provider.StreamChunk, error) {
			return []provider.StreamChunk{
				{Type: provider.ChunkTypeToolCall, ToolCall: &search},
				{Type: provider.ChunkTypeToolCall, ToolCall: &weather},
				{Type: provider.ChunkTypeFinish, FinishReason: types.FinishReasonToolCalls},
			}, nil
		}, func() ([]provider.StreamChunk, error) {
			// The provider delivers the search result in the next step.
			return []provider.StreamChunk{
				providerResult("p1"),
				{Type: provider.ChunkTypeText, Text: "Here you go."},
				{Type: provider.ChunkTypeFinish, FinishReason: types.FinishReasonStop},
			}, nil
		})
		result, _, err := chat(t, []workflowai.ModelMessage{user("search and weather")})
		if err != nil {
			t.Fatal(err)
		}
		last := result.Steps[1].Content
		if len(last) == 0 || last[0].Type != workflowai.PartToolResult || last[0].ToolCallID != "p1" || !last[0].ProviderExecuted {
			t.Fatalf("second step content = %+v", last)
		}
	})
}

func TestStreamDownloadsUnsupportedURLs(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		setup(text("A boarding pass."))
		var mu sync.Mutex
		var requested []string
		testOptions.Download = func(_ context.Context, requests []promptutils.DownloadRequest) ([]*promptutils.DownloadResult, error) {
			mu.Lock()
			defer mu.Unlock()
			results := make([]*promptutils.DownloadResult, len(requests))
			for i, request := range requests {
				requested = append(requested, request.URL)
				results[i] = &promptutils.DownloadResult{Data: []byte("%PDF"), MediaType: "application/pdf"}
			}
			return results, nil
		}
		message := workflowai.ModelMessage{Role: workflowai.RoleUser, Content: []workflowai.ContentPart{
			{Type: workflowai.PartText, Text: "What is this?"},
			{Type: workflowai.PartFile, Data: "https://example.com/pass.pdf", MediaType: "application/pdf"},
		}}
		if _, _, err := chat(t, []workflowai.ModelMessage{message}); err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		defer mu.Unlock()
		if !slices.Equal(requested, []string{"https://example.com/pass.pdf"}) {
			t.Fatalf("requested = %v", requested)
		}
		file, ok := testModel.prompts()[0][0].Content[1].(types.FileContent)
		if !ok || string(file.Data) != "%PDF" {
			t.Fatalf("file = %#v", testModel.prompts()[0][0].Content[1])
		}
	})
}

func TestStreamTransform(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		setup(text("hello"))
		testCallOptions.Transform = []ai.StreamTransformFunc{func(_ context.Context, chunk provider.StreamChunk) []provider.StreamChunk {
			chunk.Text = strings.ToUpper(chunk.Text)
			return []provider.StreamChunk{chunk}
		}}
		result, chunks, err := chat(t, []workflowai.ModelMessage{user("hi")})
		if err != nil {
			t.Fatal(err)
		}
		var delta any
		for _, chunk := range chunks {
			if chunk["type"] == "text-delta" {
				delta = chunk["delta"]
			}
		}
		if result.Text != "HELLO" || delta != "HELLO" {
			t.Fatalf("text = %q, delta = %v", result.Text, delta)
		}
	})
}

func TestStreamIncludeRawChunks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		setup(func() ([]provider.StreamChunk, error) {
			return []provider.StreamChunk{
				{Type: provider.ChunkTypeRaw, Raw: map[string]any{"type": "ping"}},
				{Type: provider.ChunkTypeText, Text: "Hi"},
				{Type: provider.ChunkTypeFinish, FinishReason: types.FinishReasonStop},
			}, nil
		})
		testCallOptions.IncludeRawChunks = true
		_, chunks, err := chat(t, []workflowai.ModelMessage{user("hi")})
		if err != nil {
			t.Fatal(err)
		}
		// Raw parts reach the run's stream but have no UI chunk.
		if !testModel.calls[0].IncludeRawChunks || slices.Contains(chunkTypes(chunks), "raw") {
			t.Fatalf("include raw chunks = %v, chunks = %v", testModel.calls[0].IncludeRawChunks, chunkTypes(chunks))
		}
		if _, ok := workflowai.ToUIMessageChunk(workflowai.ModelCallStreamPart{Type: "raw", RawValue: json.RawMessage(`{"type":"ping"}`)}); ok {
			t.Fatal("raw part has a UI chunk")
		}
	})
}

func TestGenerateInclude(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		setup(text("21 degrees."))
		testCallOptions.Include = &ai.IncludeOptions{RequestBody: true, ResponseBody: true, RequestMessages: true}
		world := testworld.New(workflow.Use(GenerateRunner))
		defer world.Close()
		ctx := workflow.NewContext(t.Context(), workflow.NewClient(world))
		run, err := workflow.Start(ctx, Generate{Prompt: "weather in Lagos?"})
		if err != nil {
			t.Fatal(err)
		}
		result, err := run.Value(ctx)
		if err != nil {
			t.Fatal(err)
		}
		step := result.Steps[0]
		if step.Request.Body == nil || step.Response.Body == nil || len(step.Request.Messages) != 1 {
			t.Fatalf("request = %+v, response body = %v", step.Request, step.Response.Body)
		}
	})
}

func TestStreamErrorPartEndsTurn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		setup(func() ([]provider.StreamChunk, error) {
			return []provider.StreamChunk{
				{Type: provider.ChunkTypeText, Text: "partial"},
				{Type: provider.ChunkTypeError, Text: "overloaded"},
				{Type: provider.ChunkTypeFinish, FinishReason: types.FinishReasonError},
			}, nil
		}, text("unused"))
		var reported []error
		testOptions.OnError = func(_ context.Context, err error) { reported = append(reported, err) }
		result, chunks, err := chat(t, []workflowai.ModelMessage{user("hi")})
		if err != nil {
			t.Fatal(err)
		}
		// The error part is streamed, the partial text is kept, and the
		// model call is not retried.
		want := []string{"start", "start-step", "reset-step", "text-start", "text-delta", "error", "text-end", "finish-step", "finish"}
		if got := chunkTypes(chunks); !slices.Equal(got, want) {
			t.Fatalf("chunks = %v, want %v", got, want)
		}
		// Callbacks run again on replay, so OnError may be called more than
		// once.
		if result.Error != "overloaded" || result.Text != "partial" || len(testModel.calls) != 1 || len(reported) == 0 || reported[0].Error() != "overloaded" {
			t.Fatalf("result = %+v after %d calls, reported %v", result, len(testModel.calls), reported)
		}
	})
}
