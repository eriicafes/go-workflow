package workflowai_test

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/digitallysavvy/go-ai/pkg/ai"
	"github.com/digitallysavvy/go-ai/pkg/provider/types"
	"github.com/eriicafes/go-workflow/workflowai"
)

func TestStreamInvalidToolCall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		setup(toolCalls(call("c1", "getWeather", `{"town":1}`)), text("Sorry."))
		_, chunks, err := chat(t, []workflowai.ModelMessage{user("weather?")})
		if err != nil {
			t.Fatal(err)
		}
		if weatherRuns != 0 {
			t.Fatal("invalid call executed")
		}
		if !slices.Contains(chunkTypes(chunks), "tool-input-error") {
			t.Fatalf("chunks = %v", chunkTypes(chunks))
		}
		result := testModel.prompts()[1][2].Content[0].(types.ToolResultContent)
		if result.Output.Type != types.ToolResultOutputErrorText {
			t.Fatalf("result = %+v", result)
		}
	})
}

func TestStreamClientToolCall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		setup(toolCalls(call("c1", "getLocation", `{}`)), text("You are in Lagos."))
		first, chunks, err := chat(t, []workflowai.ModelMessage{user("where am I?")})
		if err != nil {
			t.Fatal(err)
		}
		// The turn ends with the call for the client to run.
		want := []string{"start", "start-step", "reset-step", "tool-input-available", "finish-step", "finish"}
		if got := chunkTypes(chunks); !slices.Equal(got, want) {
			t.Fatalf("chunks = %v, want %v", got, want)
		}
		if len(first.ToolCalls) != 1 || len(first.ToolResults) != 0 {
			t.Fatalf("result = %+v", first)
		}

		// useChat sends the output the client added with addToolOutput.
		data := `[
			{"id":"u1","role":"user","parts":[{"type":"text","text":"where am I?"}]},
			{"id":"a1","role":"assistant","parts":[{"type":"step-start"},{"type":"tool-getLocation","toolCallId":"c1","state":"output-available","input":{},"output":{"city":"Lagos"}}]}
		]`
		var messages []ai.UIMessage
		if err := json.Unmarshal([]byte(data), &messages); err != nil {
			t.Fatal(err)
		}
		converted, err := workflowai.ConvertToModelMessages(t.Context(), messages)
		if err != nil {
			t.Fatal(err)
		}
		result, _, err := chat(t, converted)
		if err != nil {
			t.Fatal(err)
		}
		if result.Text != "You are in Lagos." {
			t.Fatalf("text = %q", result.Text)
		}
		prompt := testModel.prompts()[1]
		output := prompt[len(prompt)-1].Content[0].(types.ToolResultContent).Output
		if output.Value.(map[string]any)["city"] != "Lagos" {
			t.Fatalf("tool output = %+v", output)
		}
	})
}

func TestStreamInlineStepTool(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls []types.ToolCall
		for i, seconds := range []int{10, 1, 1} {
			calls = append(calls, call(fmt.Sprintf("c%d", i+1), "waitInline", fmt.Sprintf(`{"name":"c%d","seconds":%d}`, i+1, seconds)))
		}
		setup(toolCalls(calls...), text("Done."))
		testOptions.ToolCallConcurrency = 2
		result, _, err := chat(t, []workflowai.ModelMessage{user("wait")})
		if err != nil {
			t.Fatal(err)
		}
		// Inline steps run concurrently up to the limit, the third once the
		// second settles, and each runs once.
		if got, want := waits.startedAfter("c1", "c2", "c3"), []time.Duration{0, 0, time.Second}; !slices.Equal(got, want) {
			t.Fatalf("starts = %v, want %v", got, want)
		}
		if waits.runs != 3 || len(result.Steps[0].ToolResults) != 3 {
			t.Fatalf("runs = %d, results = %v", waits.runs, result.Steps[0].ToolResults)
		}
		var output string
		if err := json.Unmarshal(result.Steps[0].ToolResults[2].Output, &output); err != nil || output != "c3" {
			t.Fatalf("output = %q, %v", output, err)
		}
	})
}

func TestExecuteInlineStepDerivesInputSchema(t *testing.T) {
	executor := workflowai.ExecuteInlineStep(func(context.Context, struct {
		City string `json:"city"`
	}) (string, error) {
		return "", nil
	})
	schema := executor.(interface{ InputSchema() map[string]any }).InputSchema()
	properties, _ := schema["properties"].(map[string]any)
	if _, ok := properties["city"]; !ok {
		t.Fatalf("schema = %v", schema)
	}
}
