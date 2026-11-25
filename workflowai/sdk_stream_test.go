package workflowai_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/digitallysavvy/go-ai/pkg/ai"
	"github.com/digitallysavvy/go-ai/pkg/provider"
	"github.com/digitallysavvy/go-ai/pkg/provider/types"
	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/workflowai"
)

// AI SDK: packages/workflow/src/to-ui-message-chunk.test.ts > createModelCallToUIChunkTransform > preserves the existing output when options are omitted
func TestSDKStreamUIChunks(t *testing.T) {
	chunks := sdkStreamTransform(t, sdkStreamText, 0)
	sdkStreamExpect(t, chunks, `[
		{"type":"start"},
		{"type":"start-step"},
		{"type":"text-start","id":"text-1"},
		{"type":"text-delta","id":"text-1","delta":"Hello"},
		{"type":"text-end","id":"text-1"},
		{"type":"finish-step"},
		{"type":"finish"}
	]`)
}

// AI SDK: packages/workflow/src/to-ui-message-chunk.test.ts > createModelCallToUIChunkTransform > resumes in UI chunk index space
func TestSDKStreamUIChunksResumeIndex(t *testing.T) {
	canonical := sdkStreamTransform(t, sdkStreamToolTurn, 0)
	if len(canonical) != 14 {
		t.Fatalf("%d chunks: %v", len(canonical), chunkTypes(canonical))
	}
	if got := chunkTypes(canonical[4:7]); !reflect.DeepEqual(got, []string{"text-end", "tool-input-start", "tool-input-delta"}) {
		t.Fatalf("chunks 4 to 7 = %v", got)
	}
	resumed := sdkStreamTransform(t, sdkStreamToolTurn, 4)
	if got, want := sdkStreamJSON(t, resumed), sdkStreamJSON(t, canonical[4:]); got != want {
		t.Fatalf("resumed = %s, want %s", got, want)
	}
}

// AI SDK: packages/workflow/src/to-ui-message-chunk.test.ts > createModelCallToUIChunkTransform > reconstructs the canonical $name stream at every cursor
func TestSDKStreamUIChunksEveryCursor(t *testing.T) {
	fixtures := []struct {
		name  string
		parts []workflowai.ModelCallStreamPart
	}{
		{"text", sdkStreamText},
		{"tool call", sdkStreamToolTurn},
		{"tool error", []workflowai.ModelCallStreamPart{
			{Type: "tool-input-start", ID: "call-1", ToolName: "weather"},
			{Type: "tool-input-delta", ID: "call-1", Delta: "{}"},
			{Type: "tool-input-end", ID: "call-1"},
			{Type: "tool-call", ToolCallID: "call-1", ToolName: "weather", Input: json.RawMessage(`{}`)},
			{Type: "tool-error", ToolCallID: "call-1", ToolName: "weather", Input: json.RawMessage(`{}`), Error: "Weather lookup failed"},
		}},
		{"ignored raw parts", []workflowai.ModelCallStreamPart{
			{Type: "text-start", ID: "text-1"},
			{Type: "model-call-start"},
			{Type: "model-call-response-metadata"},
			{Type: "raw", RawValue: json.RawMessage(`{"ignored":true}`)},
			{Type: "text-end", ID: "text-1"},
		}},
		{"step boundaries", []workflowai.ModelCallStreamPart{
			{Type: "text-start", ID: "text-1"},
			{Type: "text-end", ID: "text-1"},
			{Type: "finish-step"},
			{Type: "start-step"},
			{Type: "text-start", ID: "text-2"},
			{Type: "text-end", ID: "text-2"},
		}},
		{"signed tool approval", []workflowai.ModelCallStreamPart{
			{Type: "tool-call", ToolCallID: "call-1", ToolName: "weather", Input: json.RawMessage(`{"city":"London"}`)},
			{Type: "tool-approval-request", ApprovalID: "approval-call-1", ToolCallID: "call-1", Signature: "signed-approval"},
		}},
	}
	for _, fixture := range fixtures {
		t.Run(fixture.name, func(t *testing.T) {
			canonical := sdkStreamTransform(t, fixture.parts, 0)
			for cursor := 0; cursor <= len(canonical); cursor++ {
				resumed := sdkStreamTransform(t, fixture.parts, cursor)
				joined := append(append([]ai.UIMessageChunk{}, canonical[:cursor]...), resumed...)
				if got, want := sdkStreamJSON(t, joined), sdkStreamJSON(t, canonical); got != want {
					t.Fatalf("cursor %d: %s, want %s", cursor, got, want)
				}
			}
		})
	}
}

// AI SDK: packages/workflow/src/to-ui-message-chunk.test.ts > createModelCallToUIChunkTransform > omits the full stream when the UI cursor is past the end
func TestSDKStreamUIChunksCursorPastEnd(t *testing.T) {
	if chunks := sdkStreamTransform(t, sdkStreamToolTurn, 100); len(chunks) != 0 {
		t.Fatalf("chunks = %v", chunkTypes(chunks))
	}
}

// AI SDK: packages/workflow/src/to-ui-message-chunk.test.ts > createModelCallToUIChunkTransform > rejects invalid UI start index %s
func TestSDKStreamUIChunksRejectsNegativeIndex(t *testing.T) {
	// A Go int cannot be fractional, NaN, or infinite, so a negative index is
	// the invalid start index left.
	if chunks := sdkStreamTransform(t, sdkStreamText, -1); len(chunks) != 0 {
		t.Fatalf("chunks = %v", chunkTypes(chunks))
	}
}

// AI SDK: packages/workflow/src/to-ui-message-chunk.test.ts > workflow UI stream reset-step > converts reset-step model-call parts to UI message chunks
func TestSDKStreamResetStepChunk(t *testing.T) {
	chunk, ok := workflowai.ToUIMessageChunk(workflowai.ModelCallStreamPart{Type: "reset-step"})
	if !ok {
		t.Fatal("reset-step has no UI chunk")
	}
	sdkStreamExpect(t, []ai.UIMessageChunk{chunk}, `[{"type":"reset-step"}]`)
}

// AI SDK: packages/workflow/src/to-ui-message-chunk.test.ts > workflow UI stream reset-step > starts a new normalization frame after reset-step
func TestSDKStreamResetStepFrame(t *testing.T) {
	chunks := sdkStreamTransform(t, []workflowai.ModelCallStreamPart{
		{Type: "text-start", ID: "text-1"},
		{Type: "reset-step"},
		{Type: "text-start", ID: "text-1"},
		{Type: "text-delta", ID: "text-1", Text: "retry"},
		{Type: "text-end", ID: "text-1"},
	}, 0)
	sdkStreamExpect(t, chunks[2:len(chunks)-2], `[
		{"type":"text-start","id":"text-1"},
		{"type":"reset-step"},
		{"type":"text-start","id":"text-1"},
		{"type":"text-delta","id":"text-1","delta":"retry"},
		{"type":"text-end","id":"text-1"}
	]`)
}

// AI SDK: packages/workflow/src/to-ui-message-chunk.test.ts > workflow signed tool approvals > preserves the approval signature in the UI message chunk
func TestSDKStreamApprovalSignatureChunk(t *testing.T) {
	chunk, ok := workflowai.ToUIMessageChunk(workflowai.ModelCallStreamPart{
		Type:       "tool-approval-request",
		ApprovalID: "approval-call-1",
		ToolCallID: "call-1",
		Signature:  "signed-approval",
	})
	if !ok {
		t.Fatal("tool-approval-request has no UI chunk")
	}
	sdkStreamExpect(t, []ai.UIMessageChunk{chunk}, `[{"type":"tool-approval-request","approvalId":"approval-call-1","toolCallId":"call-1","signature":"signed-approval"}]`)
}

// AI SDK: packages/workflow/src/workflow-execution-result.test.ts > stream interpretation of execution outcomes > returns data for a %s outcome without an exception
func TestSDKStreamCompletedOutcome(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		setup(text("Hello!"))
		run := sdkApprovalRun(t, "stream", []workflowai.ModelMessage{user("hi")})
		if run.err != nil || run.result.Text != "Hello!" || run.result.Error != "" {
			t.Fatalf("result = %+v, err = %v", run.result, run.err)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-execution-result.test.ts > stream interpretation of execution outcomes > returns data for a %s outcome without an exception
func TestSDKStreamAbortedOutcome(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		setup(toolCalls(call("c1", "getWeather", `{"city":"Lagos"}`)), hang)
		testOptions.Timeout = 30 * time.Second
		run := sdkApprovalRun(t, "stream", []workflowai.ModelMessage{user("weather in Lagos?")})
		if run.err != nil || len(run.result.Steps) != 1 {
			t.Fatalf("result = %+v, err = %v", run.result, run.err)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-execution-result.test.ts > stream interpretation of execution outcomes > preserves a model stream failure as an own error property: %s
func TestSDKStreamModelStreamFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		setup(func() ([]provider.StreamChunk, error) {
			return []provider.StreamChunk{
				{Type: provider.ChunkTypeError, Text: "model failure"},
				{Type: provider.ChunkTypeFinish, FinishReason: types.FinishReasonError},
			}, nil
		})
		run := sdkApprovalRun(t, "stream", []workflowai.ModelMessage{user("hi")})
		if run.err != nil || run.result.Error != "model failure" {
			t.Fatalf("result = %+v, err = %v", run.result, run.err)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-execution-result.test.ts > stream interpretation of execution outcomes > rejects an execution failure even when its value is undefined: %s
func TestSDKStreamExecutionFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		setup(text("unused"))
		testOptions.PrepareStep = func(context.Context, workflowai.PrepareStepInfo) (workflowai.PrepareStepResult, error) {
			return workflowai.PrepareStepResult{}, errors.New("execution failure")
		}
		run := sdkApprovalRun(t, "stream", []workflowai.ModelMessage{user("hi")})
		if run.err == nil || !strings.Contains(run.err.Error(), "execution failure") {
			t.Fatalf("err = %v", run.err)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-execution-result.test.ts > stream interpretation of execution outcomes > preserves a thrown abort separately from an abort returned as data
func TestSDKStreamThrownAbort(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		setup(text("unused"))
		testOptions.PrepareStep = func(context.Context, workflowai.PrepareStepInfo) (workflowai.PrepareStepResult, error) {
			return workflowai.PrepareStepResult{}, context.Canceled
		}
		run := sdkApprovalRun(t, "stream", []workflowai.ModelMessage{user("hi")})
		if run.err == nil || !strings.Contains(run.err.Error(), context.Canceled.Error()) {
			t.Fatalf("err = %v", run.err)
		}
	})
}

var sdkStreamText = []workflowai.ModelCallStreamPart{
	{Type: "text-start", ID: "text-1"},
	{Type: "text-delta", ID: "text-1", Text: "Hello"},
	{Type: "text-end", ID: "text-1"},
}

var sdkStreamToolTurn = []workflowai.ModelCallStreamPart{
	{Type: "text-start", ID: "text-1"},
	{Type: "text-delta", ID: "text-1", Text: "Checking"},
	{Type: "text-end", ID: "text-1"},
	{Type: "tool-input-start", ID: "call-1", ToolName: "weather"},
	{Type: "tool-input-delta", ID: "call-1", Delta: `{"city":"London"}`},
	{Type: "tool-input-end", ID: "call-1"},
	{Type: "tool-call", ToolCallID: "call-1", ToolName: "weather", Input: json.RawMessage(`{"city":"London"}`)},
	{Type: "tool-result", ToolCallID: "call-1", ToolName: "weather", Input: json.RawMessage(`{"city":"London"}`), Output: json.RawMessage(`{"temperature":18}`)},
	{Type: "text-start", ID: "text-2"},
	{Type: "text-delta", ID: "text-2", Text: "It is 18°C."},
	{Type: "text-end", ID: "text-2"},
}

// sdkStreamTransform reads parts as a run stream with UIMessageChunks from
// uiStartIndex.
func sdkStreamTransform(t *testing.T, parts []workflowai.ModelCallStreamPart, uiStartIndex int) []ai.UIMessageChunk {
	t.Helper()
	reader := &sdkStreamReader{}
	for _, part := range parts {
		data, err := json.Marshal(part)
		if err != nil {
			t.Fatal(err)
		}
		reader.chunks = append(reader.chunks, data)
	}
	var chunks []ai.UIMessageChunk
	for chunk := range workflowai.UIMessageChunks(t.Context(), reader, uiStartIndex) {
		chunks = append(chunks, chunk)
	}
	return chunks
}

// sdkStreamReader is a closed run stream holding chunks.
type sdkStreamReader struct {
	chunks [][]byte
	next   int
}

func (r *sdkStreamReader) Next() (workflow.StreamChunk, error) {
	if r.next == len(r.chunks) {
		return workflow.StreamChunk{}, io.EOF
	}
	r.next++
	return workflow.StreamChunk{Index: r.next - 1, Data: r.chunks[r.next-1]}, nil
}

func (r *sdkStreamReader) Read(data []byte) (int, error) {
	chunk, err := r.Next()
	return copy(data, chunk.Data), err
}

func (r *sdkStreamReader) TailIndex() (int, error) { return len(r.chunks) - 1, nil }

// sdkStreamJSON returns chunks as normalized JSON.
func sdkStreamJSON(t *testing.T, chunks []ai.UIMessageChunk) string {
	t.Helper()
	if chunks == nil {
		chunks = []ai.UIMessageChunk{}
	}
	data, err := json.Marshal(chunks)
	if err != nil {
		t.Fatal(err)
	}
	var value any
	if err := json.Unmarshal(data, &value); err != nil {
		t.Fatal(err)
	}
	data, _ = json.Marshal(value)
	return string(data)
}

// sdkStreamExpect checks that chunks encode to the JSON want.
func sdkStreamExpect(t *testing.T, chunks []ai.UIMessageChunk, want string) {
	t.Helper()
	var value any
	if err := json.Unmarshal([]byte(want), &value); err != nil {
		t.Fatal(err)
	}
	normalized, _ := json.Marshal(value)
	if got := sdkStreamJSON(t, chunks); got != string(normalized) {
		t.Fatalf("chunks = %s, want %s", got, normalized)
	}
}
