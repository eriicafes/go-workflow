package workflowai

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/digitallysavvy/go-ai/pkg/ai"
	"github.com/digitallysavvy/go-ai/pkg/provider/types"
	"github.com/eriicafes/go-workflow"
)

// ToolSet maps tool names to tools.
type ToolSet map[string]Tool

// Tool is a tool the model can call.
//
//	"bookFlight": {
//		Description:   "Book a flight for a passenger.",
//		NeedsApproval: workflowai.NeedsApprovalAlways,
//		Execute:       workflowai.ExecuteStep(bookFlight),
//	},
//
// Execute and NeedsApproval run in workflow context, so they replay with the
// run and must be deterministic. Do side effects in Execute through steps, so
// a call's result is recorded and survives restarts. ExecuteStep runs a step
// function as a step, ExecuteInlineStep as an inline step, and ExecuteFunc
// calls a function that may combine several operations or wait durably, such
// as with workflow.Sleep or a hook.
type Tool struct {
	Description string
	// InputSchema is the JSON schema of the tool's input. Inputs that fail it
	// are reported to the model as invalid tool calls. When nil, the schema
	// comes from Execute when it describes its input, as ExecuteStep does, and
	// otherwise accepts any object. Set it for tools that run on the client.
	InputSchema map[string]any
	// NeedsApproval reports whether a call needs the user's approval before
	// it executes. A call that needs approval ends the stream with a
	// tool-approval-request, and the next Stream with the user's response in
	// its messages executes or denies it. Nil never asks for approval.
	NeedsApproval NeedsApprovalFunc
	// Execute runs a call. A tool without Execute runs on the client: a call
	// to it ends the stream, and the client sends the result in the next
	// Stream's messages.
	Execute Executor
	// ToModelOutput converts a call's output to what the model sees of it.
	// Results and the stream keep the raw output. It runs in workflow
	// context.
	ToModelOutput func(ctx context.Context, options ToolModelOutputOptions) (ToolResultOutput, error)
	// ContextSchema is the JSON schema of the tool's entry in ToolsContext,
	// which a call fails without.
	ContextSchema map[string]any
	// Provider is a tool the model's provider executes, such as Anthropic's
	// web search, which the agent sends to the model as it is. Its results
	// arrive in the model's stream. A provider tool with
	// SupportsDeferredResults may deliver a result in a later model call, and
	// the loop continues until it does. A provider tool needs no other field.
	Provider *types.Tool
}

// inputSchema returns the tool's input schema.
func (t Tool) inputSchema() map[string]any {
	if t.InputSchema != nil {
		return t.InputSchema
	}
	if described, ok := t.Execute.(interface{ InputSchema() map[string]any }); ok {
		return described.InputSchema()
	}
	return map[string]any{"type": "object"}
}

// ToolModelOutputOptions describes the output a Tool's ToModelOutput
// converts.
type ToolModelOutputOptions struct {
	ToolCallID string
	Input      json.RawMessage
	Output     any
}

// ToolCallOptions describes the tool call an Executor or NeedsApprovalFunc
// handles.
type ToolCallOptions struct {
	ToolCallID string
	// Messages is the conversation up to the step that made the call.
	Messages []ModelMessage
	// Context is the tool's entry in ToolsContext.
	Context any
}

// ToolCall is a tool call made by the model.
type ToolCall struct {
	ToolCallID       string          `json:"toolCallId"`
	ToolName         string          `json:"toolName"`
	Input            json.RawMessage `json:"input"`
	ProviderExecuted bool            `json:"providerExecuted,omitempty"`
	ThoughtSignature string          `json:"thoughtSignature,omitempty"`
	// ProviderMetadata is the provider's metadata of the call, which the
	// assistant message sends back to it as provider options.
	ProviderMetadata json.RawMessage `json:"providerMetadata,omitempty"`
	// Invalid marks a call to an unknown tool or with input that does not
	// match the tool's schema. Error explains why. Invalid calls are reported
	// to the model as errors and not executed.
	Invalid bool `json:"invalid,omitempty"`
	// Dynamic marks a call whose input is not typed by its tool, as the
	// Workflow SDK marks an invalid call.
	Dynamic bool   `json:"dynamic,omitempty"`
	Error   string `json:"error,omitempty"`
}

func (c ToolCall) contentPart() ContentPart {
	return ContentPart{
		Type:             PartToolCall,
		ToolCallID:       c.ToolCallID,
		ToolName:         c.ToolName,
		Input:            c.Input,
		ProviderExecuted: c.ProviderExecuted,
		ThoughtSignature: c.ThoughtSignature,
		ProviderMetadata: c.ProviderMetadata,
	}
}

// ToolResult is the result of an executed tool call. Output holds the error
// message when IsError is set.
type ToolResult struct {
	ToolCallID string          `json:"toolCallId"`
	ToolName   string          `json:"toolName"`
	Input      json.RawMessage `json:"input"`
	Output     json.RawMessage `json:"output"`
	IsError    bool            `json:"isError,omitempty"`
	// ProviderExecuted marks the result of a tool the provider executed, and
	// ProviderMetadata holds the provider's metadata of the result.
	ProviderExecuted bool           `json:"providerExecuted,omitempty"`
	ProviderMetadata map[string]any `json:"providerMetadata,omitempty"`
}

// toolResult is a tool call's result for the model and its stream part.
type toolResult struct {
	call    ToolCall
	output  *ToolResultOutput
	raw     json.RawMessage
	isError bool
	// invalid marks the result of an invalid call, which the client already
	// received as a tool-input-error.
	invalid bool
	// rejected marks the result of an approved call that failed its check,
	// which the stream does not report.
	rejected bool
}

// Executor executes tool calls. It may also describe its input with an
// InputSchema() map[string]any method, which the agent calls when it sends
// the tool to the model.
type Executor interface {
	// Execute starts a tool call with its JSON input and returns the call's
	// result, whose value must encode to JSON. It runs in workflow context.
	// A pending result, such as one from workflow.Background, lets the calls
	// of a step run concurrently.
	Execute(ctx context.Context, input json.RawMessage, options ToolCallOptions) *workflow.Result[any]
}

// ExecuteFunc is a function that executes tool calls. A call blocks the
// runner until f returns, so ExecuteFunc calls do not overlap each other.
type ExecuteFunc func(ctx context.Context, input json.RawMessage, options ToolCallOptions) (any, error)

// Execute calls f and returns its settled result.
func (f ExecuteFunc) Execute(ctx context.Context, input json.RawMessage, options ToolCallOptions) *workflow.Result[any] {
	return workflow.NewResult(f(ctx, input, options))
}

type stepExecutor[In, Out any] struct {
	fn      func(context.Context, In) (Out, error)
	options []workflow.StepOptions
	schema  func() map[string]any
	// inline runs fn as an inline step.
	inline bool
}

// ExecuteStep returns an Executor that decodes a call's input into In and
// starts fn with it as a background step, so calls run concurrently, each
// call executes at least once, and its result is recorded. Like any step, it
// runs inline in the flow up to workflow.SetMaxInlineSteps, and otherwise as
// a step delivery from the World's queue. The tool's input schema is derived
// from In's JSON fields when the agent first sends the tool to the model.
func ExecuteStep[In, Out any](fn func(context.Context, In) (Out, error), options ...workflow.StepOptions) Executor {
	return newStepExecutor(fn, options, false)
}

// ExecuteInlineStep returns an Executor like ExecuteStep that starts fn as an
// inline step with workflow.InlineBackground. It always runs in the flow,
// never through the World's queue, and its retries run in a later flow. Use it
// for short work, and ExecuteStep for slow work, which a step delivery can run
// on another worker.
func ExecuteInlineStep[In, Out any](fn func(context.Context, In) (Out, error), options ...workflow.StepOptions) Executor {
	return newStepExecutor(fn, options, true)
}

func newStepExecutor[In, Out any](fn func(context.Context, In) (Out, error), options []workflow.StepOptions, inline bool) Executor {
	return stepExecutor[In, Out]{fn: fn, options: options, inline: inline, schema: sync.OnceValue(func() map[string]any {
		return ai.SchemaFor[In]().Validator().JSONSchema()
	})}
}

func (e stepExecutor[In, Out]) Execute(ctx context.Context, raw json.RawMessage, _ ToolCallOptions) *workflow.Result[any] {
	var input In
	if err := json.Unmarshal(raw, &input); err != nil {
		return workflow.NewResult[any](nil, fmt.Errorf("decode tool input: %w", err))
	}
	start := workflow.Background[In, Out]
	if e.inline {
		start = workflow.InlineBackground[In, Out]
	}
	result := start(ctx, e.fn, input, e.options...)
	return workflow.Then(ctx, result, func(output Out, err error) *workflow.Result[any] {
		return workflow.NewResult[any](output, err)
	})
}

func (e stepExecutor[In, Out]) InputSchema() map[string]any { return e.schema() }

// NeedsApprovalFunc reports whether a tool call needs approval. It runs in
// workflow context. It can call
// workflow.Step or workflow.InlineStep to record side effects and external
// reads, so replays don't repeat them.
type NeedsApprovalFunc func(ctx context.Context, input json.RawMessage, options ToolCallOptions) (bool, error)

// NeedsApprovalAlways is a NeedsApprovalFunc that asks for approval for every call.
func NeedsApprovalAlways(context.Context, json.RawMessage, ToolCallOptions) (bool, error) {
	return true, nil
}
