package workflowai

import (
	"cmp"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/digitallysavvy/go-ai/pkg/ai"
	"github.com/digitallysavvy/go-ai/pkg/provider"
	providererrors "github.com/digitallysavvy/go-ai/pkg/provider/errors"
	"github.com/digitallysavvy/go-ai/pkg/provider/types"
	promptutils "github.com/digitallysavvy/go-ai/pkg/providerutils/prompt"
	"github.com/digitallysavvy/go-ai/pkg/schema"
	"github.com/digitallysavvy/go-ai/pkg/telemetry"
	"github.com/eriicafes/go-workflow"
)

// GenerationSettings are the model call settings of an agent.
type GenerationSettings struct {
	MaxOutputTokens  *int              `json:"maxOutputTokens,omitempty"`
	Temperature      *float64          `json:"temperature,omitempty"`
	TopP             *float64          `json:"topP,omitempty"`
	TopK             *int              `json:"topK,omitempty"`
	PresencePenalty  *float64          `json:"presencePenalty,omitempty"`
	FrequencyPenalty *float64          `json:"frequencyPenalty,omitempty"`
	StopSequences    []string          `json:"stopSequences,omitempty"`
	Seed             *int              `json:"seed,omitempty"`
	Headers          map[string]string `json:"headers,omitempty"`
	ProviderOptions  map[string]any    `json:"providerOptions,omitempty"`
	// Reasoning sets how much the model reasons, for models that support
	// it.
	Reasoning *types.ReasoningLevel `json:"reasoning,omitempty"`
	// SendReasoning sets whether the provider sends the model's reasoning.
	SendReasoning *bool `json:"sendReasoning,omitempty"`
	// MaxRetries is the number of times opening a model call is retried
	// after an error the provider reports as retryable. Nil retries twice. A
	// model stream that fails partway is not retried.
	MaxRetries *int `json:"maxRetries,omitempty"`
}

// toolDefinition is the part of a Tool a model call sends to the model.
type toolDefinition struct {
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
	InputSchema map[string]any `json:"inputSchema,omitempty"`
}

// modelCallInput is the recorded input of a model call step.
type modelCallInput struct {
	Instructions string             `json:"instructions,omitempty"`
	Messages     []ModelMessage     `json:"messages"`
	Tools        []toolDefinition   `json:"tools,omitempty"`
	ToolChoice   *types.ToolChoice  `json:"toolChoice,omitempty"`
	Settings     GenerationSettings `json:"settings"`
	// Deadline is when the call's Timeout passes, in milliseconds since the
	// Unix epoch, or 0 without one. The model is not called after it.
	Deadline int64 `json:"deadline,omitempty"`
	// StepNumber and Streaming describe the call.
	StepNumber int  `json:"stepNumber"`
	Streaming  bool `json:"streaming,omitempty"`
	// IncludeRawChunks asks the provider for its raw chunks.
	IncludeRawChunks bool `json:"includeRawChunks,omitempty"`
	// Include keeps the request and response bodies of a generated call.
	Include *ai.IncludeOptions `json:"include,omitempty"`
	// ResponseFormat is the format the Output asks the model to answer in.
	ResponseFormat *provider.ResponseFormat `json:"responseFormat,omitempty"`
	// Writer names the stream a streamed call writes its parts to. Nil
	// writes nothing.
	Writer *workflow.WriterOption `json:"writer,omitempty"`
}

// modelCallResult is the recorded result of a model call step.
type modelCallResult struct {
	// Aborted is set when the call's deadline passed before the model was
	// called, as the Workflow SDK returns an abort as data so the step is
	// not retried.
	Aborted bool `json:"aborted,omitempty"`
	// Content is the assistant content in the order the model produced it.
	Content      []ContentPart `json:"content"`
	ToolCalls    []ToolCall    `json:"toolCalls,omitempty"`
	FinishReason string        `json:"finishReason"`
	Usage        types.Usage   `json:"usage"`
	Response     Response      `json:"response"`
	// Error is the message of the first error part of the model stream. A
	// stream error ends the turn.
	Error string `json:"error,omitempty"`
	// ProviderMetadata is the provider's metadata of the response, and
	// Warnings the warnings it reported.
	ProviderMetadata map[string]any  `json:"providerMetadata,omitempty"`
	Warnings         []types.Warning `json:"warnings,omitempty"`
	// Performance is how fast the model answered.
	Performance *StepPerformance `json:"performance,omitempty"`
	// RequestBody is the request sent to the provider, kept when Include
	// asks for it.
	RequestBody any `json:"requestBody,omitempty"`
}

// modelCall holds what a model call step needs that its input cannot carry:
// the model and the call's callbacks. The agent passes it to doStreamStep and
// doGenerateStep through their context, since a step delivery decodes the
// input from the event log and replay builds the modelCall again.
type modelCall struct {
	model      provider.LanguageModel
	telemetry  *telemetry.Options
	repair     ai.ToolCallRepairFunction
	download   ai.DownloadFunction
	transforms []ai.StreamTransformFunc
	// providerTools are the active provider tools, sent to the model as they
	// are.
	providerTools []types.Tool
}

func (m modelCall) options(ctx context.Context, input modelCallInput) (*provider.GenerateOptions, error) {
	options := &provider.GenerateOptions{
		Prompt:           types.Prompt{System: input.Instructions, Messages: promptMessages(input.Messages)},
		Temperature:      input.Settings.Temperature,
		MaxTokens:        input.Settings.MaxOutputTokens,
		TopP:             input.Settings.TopP,
		TopK:             input.Settings.TopK,
		FrequencyPenalty: input.Settings.FrequencyPenalty,
		PresencePenalty:  input.Settings.PresencePenalty,
		StopSequences:    input.Settings.StopSequences,
		Seed:             input.Settings.Seed,
		Headers:          withUserAgent(input.Settings.Headers),
		ProviderOptions:  input.Settings.ProviderOptions,
		Reasoning:        input.Settings.Reasoning,
		SendReasoning:    input.Settings.SendReasoning,
		ResponseFormat:   input.ResponseFormat,
		IncludeRawChunks: input.IncludeRawChunks,
	}
	for _, tool := range input.Tools {
		options.Tools = append(options.Tools, types.Tool{Name: tool.Name, Description: tool.Description, Parameters: tool.InputSchema})
	}
	options.Tools = append(options.Tools, m.providerTools...)
	if input.ToolChoice != nil {
		options.ToolChoice = *input.ToolChoice
	} else if len(options.Tools) > 0 {
		options.ToolChoice = types.AutoToolChoice()
	}
	// Normalize the prompt like go-ai does before every model call, which
	// also merges consecutive tool messages and downloads the file URLs the
	// model does not support.
	download := m.download
	if download == nil {
		download = ai.DefaultDownload
	}
	prompt, err := promptutils.NormalizePromptWithDownloadSupport(ctx, options.Prompt, true, download, ai.SupportedURLCheckerForModel(m.model))
	if err != nil {
		return nil, err
	}
	options.Prompt = prompt
	return options, nil
}

// doStreamStep streams one model call, writing its parts to the Writer as
// they arrive.
//
// Retries only open the model stream again, and an error part in the stream
// ends the turn instead of retrying it: the part is written
// and recorded in the result, and the rest of the stream is still read for its
// finish reason and usage. The step itself is not retried, but it runs again
// when its delivery is interrupted, so it first writes a reset-step part that
// makes a client discard the parts of an earlier execution.
func doStreamStep(ctx context.Context, input modelCallInput) (result modelCallResult, err error) {
	m, err := modelCallFrom(ctx)
	if err != nil {
		return modelCallResult{}, err
	}
	var writer io.Writer = io.Discard
	if input.Writer != nil {
		w, err := workflow.Writer(ctx, *input.Writer)
		if err != nil {
			return modelCallResult{}, err
		}
		defer w.Close()
		if err := writePart(w, ModelCallStreamPart{Type: "reset-step"}); err != nil {
			return modelCallResult{}, err
		}
		writer = w
	}

	options, err := m.options(ctx, input)
	if err != nil {
		return modelCallResult{}, err
	}
	started := time.Now()
	var firstOutput time.Time
	stream, err := retry(ctx, input.Settings.MaxRetries, input.deadline(), func() (provider.TextStream, error) {
		return m.model.DoStream(ctx, options)
	})
	if errors.Is(err, errDeadlinePassed) {
		return modelCallResult{Aborted: true}, nil
	}
	if err != nil {
		return modelCallResult{}, cancelCause(ctx, err)
	}
	defer stream.Close()

	acc := newAccumulator(func(call types.ToolCall) ToolCall { return m.toolCall(ctx, input, call) })
	acc.toolChoice, acc.provider, acc.modelID = input.ToolChoice, m.model.Provider(), m.model.ModelID()
	acc.performance = func(usage types.Usage) *StepPerformance {
		return measurePerformance(started, firstOutput, time.Now(), usage)
	}
	chunks := readChunks(ctx, stream)
	for {
		chunk, err := nextChunk(ctx, chunks)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return modelCallResult{}, cancelCause(ctx, err)
		}
		if firstOutput.IsZero() && isOutputChunk(chunk.Type) {
			firstOutput = time.Now()
		}
		for _, chunk := range m.transform(ctx, *chunk) {
			parts := acc.add(&chunk)
			if chunk.Type == provider.ChunkTypeError {
				message := chunk.Text
				if chunk.Err != nil {
					message = chunk.Err.Error()
				}
				if acc.result.Error == "" {
					acc.result.Error = cmp.Or(message, "model stream error")
				}
				parts = append(parts, ModelCallStreamPart{Type: "error", Error: message})
			}
			for _, part := range parts {
				if err := writePart(writer, part); err != nil {
					return modelCallResult{}, err
				}
			}
		}
	}
	for _, part := range acc.close() {
		if err := writePart(writer, part); err != nil {
			return modelCallResult{}, err
		}
	}
	switch {
	case acc.result.FinishReason != "":
	case acc.result.Error != "":
		// Like the Workflow SDK, a stream that ends with an error and no
		// finish chunk finishes with an error.
		acc.result.FinishReason = string(types.FinishReasonError)
	case !hasOutput(acc.result.Content):
		// A stream that ends without a finish chunk and without output, such
		// as one with only an empty text block, generated nothing.
		acc.result.Error, acc.result.FinishReason = errNoOutput, string(types.FinishReasonOther)
		if err := writePart(writer, ModelCallStreamPart{Type: "error", Error: acc.result.Error}); err != nil {
			return modelCallResult{}, err
		}
	}
	if acc.result.Performance == nil {
		acc.result.Performance = measurePerformance(started, firstOutput, time.Now(), acc.result.Usage)
	}
	m.logWarnings(acc.result)
	return acc.result, nil
}

// doGenerateStep makes one model call without streaming, retrying it like
// doStreamStep retries opening its stream.
func doGenerateStep(ctx context.Context, input modelCallInput) (result modelCallResult, err error) {
	m, err := modelCallFrom(ctx)
	if err != nil {
		return modelCallResult{}, err
	}
	options, err := m.options(ctx, input)
	if err != nil {
		return modelCallResult{}, err
	}
	started := time.Now()
	generated, err := retry(ctx, input.Settings.MaxRetries, input.deadline(), func() (*types.GenerateResult, error) {
		return m.model.DoGenerate(ctx, options)
	})
	if errors.Is(err, errDeadlinePassed) {
		return modelCallResult{Aborted: true}, nil
	}
	if err != nil {
		return modelCallResult{}, cancelCause(ctx, err)
	}
	ended := time.Now()
	result = modelCallResult{FinishReason: string(generated.FinishReason), Usage: generated.Usage, ProviderMetadata: generated.ProviderMetadata, Warnings: generated.Warnings}
	result.Performance = measurePerformance(started, ended, ended, generated.Usage)
	if generated.ResponseMetadata != nil {
		result.Response = Response{ID: generated.ResponseMetadata.ID, ModelID: generated.ResponseMetadata.ModelID}
	}
	if input.Include != nil && input.Include.RequestBody {
		result.RequestBody = generated.RawRequest
	}
	if input.Include != nil && input.Include.ResponseBody {
		result.Response.Body = generated.RawResponse
	}
	// Like the Workflow SDK, the step keeps the model's content in order,
	// with each tool call where the model made it. Tool results in the
	// content are the results of tools the provider executed.
	calls := map[string]types.ToolCall{}
	for _, call := range generated.ToolCalls {
		calls[call.ID] = call
	}
	withhold := input.ToolChoice != nil && input.ToolChoice.Type == types.ToolChoiceNone
	withheld := 0
	addCall := func(call types.ToolCall) {
		if withhold {
			withheld++
			return
		}
		toolCall := m.toolCall(ctx, input, call)
		result.ToolCalls = append(result.ToolCalls, toolCall)
		result.Content = append(result.Content, toolCall.contentPart())
	}
	hasText := false
	for _, content := range generated.Content {
		if c, ok := content.(types.ToolCallContent); ok {
			call, known := calls[c.ToolCallID]
			if !known {
				call = types.ToolCall{ID: c.ToolCallID, ToolName: c.ToolName, RawArguments: c.Input, Arguments: c.Arguments}
			}
			call.ProviderExecuted = call.ProviderExecuted || c.ProviderExecuted
			delete(calls, c.ToolCallID)
			addCall(call)
			continue
		}
		part, ok := fromContentPart(content)
		if !ok {
			continue
		}
		if part.Type == PartToolResult {
			part.ProviderExecuted = true
		}
		hasText = hasText || part.Type == PartText
		result.Content = append(result.Content, part)
	}
	if !hasText && generated.Text != "" {
		result.Content = append(result.Content, ContentPart{Type: PartText, Text: generated.Text})
	}
	for _, call := range generated.ToolCalls {
		if _, left := calls[call.ID]; left {
			addCall(call)
		}
	}
	if violation := toolChoiceViolation(input.ToolChoice, result.ToolCalls, withheld, result.FinishReason, m.model.Provider(), m.model.ModelID()); violation != "" {
		result.FinishReason, result.Error = string(types.FinishReasonError), violation
	}
	m.logWarnings(result)
	return result, nil
}

// transform applies the call's stream transforms to a provider chunk, in
// order.
func (m modelCall) transform(ctx context.Context, chunk provider.StreamChunk) []provider.StreamChunk {
	chunks := []provider.StreamChunk{chunk}
	for _, transform := range m.transforms {
		var next []provider.StreamChunk
		for _, chunk := range chunks {
			next = append(next, transform(ctx, chunk)...)
		}
		chunks = next
	}
	return chunks
}

// toolCall parses a provider tool call. When the call is invalid and the
// agent has a RepairToolCall, the call is repaired once and parsed again.
func (m modelCall) toolCall(ctx context.Context, input modelCallInput, call types.ToolCall) ToolCall {
	parsed, cause := parseToolCall(input.Tools, call)
	if cause == nil || m.repair == nil {
		return parsed
	}
	tools := make([]types.Tool, len(input.Tools))
	for i, tool := range input.Tools {
		tools[i] = types.Tool{Name: tool.Name, Description: tool.Description, Parameters: tool.InputSchema}
	}
	repaired, err := m.repair(ctx, ai.ToolCallRepairOptions{
		ToolCall: call,
		Tools:    tools,
		InputSchema: func(toolName string) (map[string]any, error) {
			for _, tool := range input.Tools {
				if tool.Name == toolName {
					return tool.InputSchema, nil
				}
			}
			return nil, &ai.NoSuchToolError{ToolName: toolName}
		},
		Instructions: input.Instructions,
		System:       input.Instructions,
		Messages:     promptMessages(input.Messages),
		Error:        cause,
	})
	if err != nil {
		parsed.Error = fmt.Sprintf("repairing tool call: %v", err)
		return parsed
	}
	if repaired == nil {
		return parsed
	}
	fixed := *repaired
	fixed.ID = cmp.Or(fixed.ID, call.ID)
	parsed, _ = parseToolCall(input.Tools, fixed)
	return parsed
}

// accumulator turns provider chunks into ModelCallStreamParts and collects
// the step's content.
type accumulator struct {
	// parse turns a provider tool call into a ToolCall.
	parse  func(types.ToolCall) ToolCall
	result modelCallResult
	// open maps the IDs of open text and reasoning blocks to their index in
	// the result's content.
	open map[string]int
	// textID and reasoningID are the open blocks that chunks without an ID
	// belong to.
	textID, reasoningID string
	blocks              int
	// performance measures the call when it finishes.
	performance func(types.Usage) *StepPerformance
	// toolChoice is the call's tool choice, which the response must
	// satisfy. With tool choice none, tool parts are withheld, and withheld
	// counts the tool calls among them.
	toolChoice        *types.ToolChoice
	withheld          int
	provider, modelID string
}

func newAccumulator(parse func(types.ToolCall) ToolCall) *accumulator {
	return &accumulator{parse: parse, open: map[string]int{}, performance: func(types.Usage) *StepPerformance { return nil }}
}

func (a *accumulator) add(chunk *provider.StreamChunk) []ModelCallStreamPart {
	if a.toolChoice != nil && a.toolChoice.Type == types.ToolChoiceNone {
		// Like the Workflow SDK, tool choice none withholds tool parts, so
		// readers cannot act on calls the response is rejected for.
		switch chunk.Type {
		case provider.ChunkTypeToolCall:
			a.withheld++
			return nil
		case provider.ChunkTypeToolInputStart, provider.ChunkTypeToolInputDelta, provider.ChunkTypeToolInputEnd, provider.ChunkTypeToolResult:
			return nil
		}
	}
	switch chunk.Type {
	case provider.ChunkTypeTextStart:
		return a.start(PartText, a.blockID(chunk.ID, &a.textID), chunk.ProviderMetadata)
	case provider.ChunkTypeText:
		id := a.blockID(chunk.ID, &a.textID)
		parts := a.start(PartText, id, nil)
		a.result.Content[a.open[id]].Text += chunk.Text
		mergeMetadata(&a.result.Content[a.open[id]], chunk.ProviderMetadata)
		return append(parts, ModelCallStreamPart{Type: "text-delta", ID: id, Text: chunk.Text, ProviderMetadata: chunk.ProviderMetadata})
	case provider.ChunkTypeTextEnd:
		return a.end(a.blockID(chunk.ID, &a.textID), &a.textID, chunk.ProviderMetadata)
	case provider.ChunkTypeReasoningStart:
		return a.start(PartReasoning, a.blockID(chunk.ID, &a.reasoningID), chunk.ProviderMetadata)
	case provider.ChunkTypeReasoning:
		id := a.blockID(chunk.ID, &a.reasoningID)
		parts := a.start(PartReasoning, id, nil)
		text := chunk.Reasoning
		if text == "" {
			text = chunk.Text
		}
		a.result.Content[a.open[id]].Text += text
		mergeMetadata(&a.result.Content[a.open[id]], chunk.ProviderMetadata)
		return append(parts, ModelCallStreamPart{Type: "reasoning-delta", ID: id, Text: text, ProviderMetadata: chunk.ProviderMetadata})
	case provider.ChunkTypeReasoningEnd:
		return a.end(a.blockID(chunk.ID, &a.reasoningID), &a.reasoningID, chunk.ProviderMetadata)
	case provider.ChunkTypeToolInputStart:
		if chunk.ToolCall == nil {
			return nil
		}
		return []ModelCallStreamPart{{Type: "tool-input-start", ID: chunk.ToolCall.ID, ToolName: chunk.ToolCall.ToolName, ProviderExecuted: chunk.ToolCall.ProviderExecuted}}
	case provider.ChunkTypeToolInputDelta:
		id := chunk.ID
		if chunk.ToolCall != nil {
			id = chunk.ToolCall.ID
		}
		return []ModelCallStreamPart{{Type: "tool-input-delta", ID: id, Delta: chunk.Text}}
	case provider.ChunkTypeToolCall:
		if chunk.ToolCall == nil {
			return nil
		}
		// A tool call ends the text and reasoning blocks before it.
		parts := a.close()
		call := a.parse(*chunk.ToolCall)
		a.result.ToolCalls = append(a.result.ToolCalls, call)
		a.result.Content = append(a.result.Content, call.contentPart())
		parts = append(parts, ModelCallStreamPart{
			Type:             "tool-call",
			ToolCallID:       call.ToolCallID,
			ToolName:         call.ToolName,
			Input:            call.Input,
			Invalid:          call.Invalid,
			Error:            call.Error,
			ProviderExecuted: call.ProviderExecuted,
			ProviderMetadata: marshalMetadata(chunk.ToolCall.ProviderMetadata),
		})
		// Like the Workflow SDK, an invalid call is followed by its error.
		if call.Invalid && !call.ProviderExecuted {
			parts = append(parts, ModelCallStreamPart{Type: "tool-error", ToolCallID: call.ToolCallID, ToolName: call.ToolName, Input: call.Input, Error: call.Error})
		}
		return parts
	case provider.ChunkTypeToolApprovalRequest:
		if chunk.ToolApprovalRequest == nil {
			return nil
		}
		// The model asks approval only for the tools its provider executes.
		request := chunk.ToolApprovalRequest
		part := ContentPart{Type: PartToolApprovalRequest, ApprovalID: request.ApprovalID, ToolCallID: cmp.Or(request.ToolCallID, request.ToolCall.ID), ProviderExecuted: true}
		a.result.Content = append(a.result.Content, part)
		return []ModelCallStreamPart{{Type: "tool-approval-request", ApprovalID: part.ApprovalID, ToolCallID: part.ToolCallID}}
	case provider.ChunkTypeRaw:
		return []ModelCallStreamPart{{Type: "raw", RawValue: marshalRaw(chunk.Raw)}}
	case provider.ChunkTypeToolResult:
		// Tools the provider executes report their results in the model's
		// stream, sometimes in a later step than their call.
		result := chunk.ToolResult
		if result == nil || !result.ProviderExecuted {
			return nil
		}
		value := result.Result
		if result.Error != nil && value == nil {
			value = result.Error.Error()
		}
		output := jsonOutput(value)
		part := ModelCallStreamPart{Type: "tool-result", ToolCallID: result.ToolCallID, ToolName: result.ToolName, Output: marshalRaw(value), ProviderExecuted: true}
		if result.Error != nil {
			output = &ToolResultOutput{Type: "error-json", Value: marshalRaw(value)}
			part = ModelCallStreamPart{Type: "tool-error", ToolCallID: result.ToolCallID, ToolName: result.ToolName, Error: result.Error.Error(), ProviderExecuted: true}
		}
		a.result.Content = append(a.result.Content, ContentPart{
			Type:             PartToolResult,
			ToolCallID:       result.ToolCallID,
			ToolName:         result.ToolName,
			Output:           output,
			ProviderExecuted: true,
			ProviderOptions:  result.ProviderMetadata,
		})
		return []ModelCallStreamPart{part}
	case provider.ChunkTypeFile:
		if chunk.GeneratedFileContent == nil {
			return nil
		}
		file := chunk.GeneratedFileContent
		data := base64.StdEncoding.EncodeToString(file.Data)
		a.result.Content = append(a.result.Content, ContentPart{Type: PartFile, Data: data, MediaType: file.MediaType})
		return []ModelCallStreamPart{{Type: "file", MediaType: file.MediaType, Data: data}}
	case provider.ChunkTypeSource:
		if chunk.SourceContent == nil {
			return nil
		}
		source := chunk.SourceContent
		a.result.Content = append(a.result.Content, ContentPart{
			Type:             PartSource,
			SourceType:       source.SourceType,
			SourceID:         source.ID,
			URL:              source.URL,
			Title:            source.Title,
			MediaType:        source.MediaType,
			Filename:         source.Filename,
			ProviderMetadata: chunk.ProviderMetadata,
		})
		return []ModelCallStreamPart{{
			Type:             "source",
			ID:               source.ID,
			SourceType:       source.SourceType,
			URL:              source.URL,
			Title:            source.Title,
			MediaType:        source.MediaType,
			Filename:         source.Filename,
			ProviderMetadata: chunk.ProviderMetadata,
		}}
	case provider.ChunkTypeResponseMetadata:
		if chunk.ResponseMetadata != nil {
			metadata := chunk.ResponseMetadata
			a.result.Response = Response{ID: metadata.ID, ModelID: metadata.ModelID}
			part := ModelCallStreamPart{Type: "model-call-response-metadata", ID: metadata.ID, ModelID: metadata.ModelID}
			if !metadata.Timestamp.IsZero() {
				part.Timestamp = metadata.Timestamp.UTC().Format(time.RFC3339Nano)
			}
			return []ModelCallStreamPart{part}
		}
	case provider.ChunkTypeUsage:
		if chunk.Usage != nil {
			a.result.Usage = *chunk.Usage
		}
	case provider.ChunkTypeCustom:
		if chunk.CustomContent != nil {
			custom := chunk.CustomContent
			a.result.Content = append(a.result.Content, ContentPart{Type: PartCustom, Kind: custom.Kind, ProviderOptions: custom.ProviderOptions, ProviderMetadata: custom.ProviderMetadata})
		}
	case provider.ChunkTypeStreamStart:
		a.result.Warnings = append(a.result.Warnings, chunk.Warnings...)
		return []ModelCallStreamPart{{Type: "model-call-start", Warnings: chunk.Warnings}}
	case provider.ChunkTypeFinish, provider.ChunkTypeFinishStep:
		if chunk.FinishReason != "" {
			a.result.FinishReason = string(chunk.FinishReason)
		}
		if metadata := decodeMetadata(chunk.ProviderMetadata); metadata != nil {
			a.result.ProviderMetadata = metadata
		}
		if chunk.Usage != nil {
			a.result.Usage = *chunk.Usage
		}
		if chunk.Type == provider.ChunkTypeFinish {
			usage := a.result.Usage
			a.result.Performance = a.performance(usage)
			violation := toolChoiceViolation(a.toolChoice, a.result.ToolCalls, a.withheld, a.result.FinishReason, a.provider, a.modelID)
			if violation != "" {
				a.result.FinishReason, a.result.Error = string(types.FinishReasonError), violation
			}
			parts := []ModelCallStreamPart{{Type: "model-call-end", FinishReason: a.result.FinishReason, Usage: &usage, ProviderMetadata: chunk.ProviderMetadata, Performance: a.result.Performance}}
			if violation != "" {
				parts = append(parts, ModelCallStreamPart{Type: "error", Error: violation})
			}
			return parts
		}
	}
	return nil
}

// blockID returns id, or the open block that *current names when id is empty,
// opening a new one when none is open.
func (a *accumulator) blockID(id string, current *string) string {
	if id != "" {
		*current = id
		return id
	}
	if *current == "" {
		*current = strconv.Itoa(a.blocks)
		a.blocks++
	}
	return *current
}

// start opens the block id unless it is open and returns its start part.
func (a *accumulator) start(kind, id string, metadata json.RawMessage) []ModelCallStreamPart {
	if _, open := a.open[id]; open {
		return nil
	}
	a.open[id] = len(a.result.Content)
	a.result.Content = append(a.result.Content, ContentPart{Type: kind, ProviderMetadata: metadata})
	return []ModelCallStreamPart{{Type: kind + "-start", ID: id, ProviderMetadata: metadata}}
}

// end closes the block id and returns its end part.
func (a *accumulator) end(id string, current *string, metadata json.RawMessage) []ModelCallStreamPart {
	index, open := a.open[id]
	if !open {
		return nil
	}
	delete(a.open, id)
	if *current == id {
		*current = ""
	}
	part := &a.result.Content[index]
	mergeMetadata(part, metadata)
	return []ModelCallStreamPart{{Type: part.Type + "-end", ID: id, ProviderMetadata: metadata}}
}

// close ends every open block in the order they were opened.
func (a *accumulator) close() []ModelCallStreamPart {
	var parts []ModelCallStreamPart
	for index, part := range a.result.Content {
		for id, open := range a.open {
			if open == index {
				current := &a.textID
				if part.Type == PartReasoning {
					current = &a.reasoningID
				}
				parts = append(parts, a.end(id, current, nil)...)
			}
		}
	}
	return parts
}

// retry calls fn until it succeeds, it fails with an error the provider does
// not report as retryable, or maxRetries retries have failed. Nil maxRetries
// retries twice. Retries wait 2 seconds and then twice as long as the
// previous wait, up to a minute, or the delay of the error's retry headers.
func retry[T any](ctx context.Context, maxRetries *int, deadline time.Time, fn func() (T, error)) (T, error) {
	retries := 2
	if maxRetries != nil {
		retries = *maxRetries
	}
	delay := 2 * time.Second
	for attempt := 0; ; attempt++ {
		// Like the Workflow SDK, the deadline is checked before every
		// dispatch, so a model call does not start after it.
		if !deadline.IsZero() && !time.Now().Before(deadline) {
			var zero T
			return zero, errDeadlinePassed
		}
		value, err := fn()
		var retryable interface{ IsRetryable() bool }
		if err == nil || attempt >= retries || !errors.As(err, &retryable) || !retryable.IsRetryable() {
			return value, err
		}
		select {
		case <-time.After(retryDelay(err, delay)):
		case <-ctx.Done():
			var zero T
			return zero, err
		}
		delay = min(delay*2, time.Minute)
	}
}

// errDeadlinePassed reports that a model call's deadline passed before it
// was dispatched.
var errDeadlinePassed = errors.New("workflowai: model call deadline passed")

// deadline returns the call's deadline, or the zero time without one.
func (input modelCallInput) deadline() time.Time {
	if input.Deadline == 0 {
		return time.Time{}
	}
	return time.UnixMilli(input.Deadline)
}

// cancelCause returns the cause of ctx's cancellation when ctx is done, so
// a model call cut short by cancellation reports it instead of the
// provider's error, and err otherwise.
// streamChunk is a chunk read from a model stream, or the error reading
// it.
type streamChunk struct {
	chunk *provider.StreamChunk
	err   error
}

// readChunks reads stream in its own goroutine until it ends, fails, or ctx
// is done, so a model stream that ignores cancellation cannot keep the call
// running.
func readChunks(ctx context.Context, stream provider.TextStream) <-chan streamChunk {
	chunks := make(chan streamChunk)
	go func() {
		defer close(chunks)
		for {
			chunk, err := stream.Next()
			select {
			case chunks <- streamChunk{chunk, err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	return chunks
}

// nextChunk returns the next chunk of chunks, or ctx's cause once ctx is
// done, as the Workflow SDK stops reading a stream when the call aborts.
func nextChunk(ctx context.Context, chunks <-chan streamChunk) (*provider.StreamChunk, error) {
	select {
	case next, ok := <-chunks:
		if !ok {
			return nil, context.Cause(ctx)
		}
		return next.chunk, next.err
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}
}

func cancelCause(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	return err
}

// retryDelay returns the delay that the retry-after-ms or Retry-After
// header of err's response asks for, when it is under a minute or shorter
// than backoff, and otherwise backoff.
func retryDelay(err error, backoff time.Duration) time.Duration {
	var providerErr *providererrors.ProviderError
	if !errors.As(err, &providerErr) {
		return backoff
	}
	var retryAfterMS, retryAfter string
	for name, value := range providerErr.ResponseHeaders {
		switch strings.ToLower(name) {
		case "retry-after-ms":
			retryAfterMS = value
		case "retry-after":
			retryAfter = value
		}
	}
	delay := time.Duration(-1)
	if ms, err := strconv.ParseFloat(retryAfterMS, 64); err == nil {
		delay = time.Duration(ms * float64(time.Millisecond))
	} else if seconds, err := strconv.ParseFloat(retryAfter, 64); err == nil {
		delay = time.Duration(seconds * float64(time.Second))
	} else if when, err := http.ParseTime(retryAfter); err == nil {
		delay = time.Until(when)
	}
	if delay >= 0 && (delay < time.Minute || delay < backoff) {
		return delay
	}
	return backoff
}

// mergeMetadata keeps the latest provider metadata of a block, and takes a
// reasoning signature from it.
func mergeMetadata(part *ContentPart, metadata json.RawMessage) {
	if len(metadata) == 0 {
		return
	}
	part.ProviderMetadata = metadata
	if part.Type != PartReasoning {
		return
	}
	var providers map[string]struct {
		Signature string `json:"signature"`
	}
	if json.Unmarshal(metadata, &providers) == nil {
		for _, name := range []string{"anthropic", "bedrock"} {
			if signature := providers[name].Signature; signature != "" {
				part.Signature = signature
			}
		}
	}
}

// toolChoiceViolation returns why a model call's response does not satisfy
// its tool choice, as the Workflow SDK enforces it, or "": with tool choice
// none it made a tool call, and with tool choice required or a named tool it
// did not make the call, unless it was interrupted.
func toolChoiceViolation(choice *types.ToolChoice, calls []ToolCall, withheld int, finishReason, providerName, modelID string) string {
	if choice == nil {
		return ""
	}
	violated := false
	switch choice.Type {
	case types.ToolChoiceNone:
		violated = withheld > 0 || len(calls) > 0
	case types.ToolChoiceRequired:
		violated = finishReason != "interrupted" && len(calls) == 0
	case types.ToolChoiceTool:
		violated = finishReason != "interrupted" && !slices.ContainsFunc(calls, func(call ToolCall) bool { return call.ToolName == choice.ToolName })
	}
	if !violated {
		return ""
	}
	return (&ai.ToolChoiceViolationError{ToolChoice: *choice, FinishReason: types.FinishReason(finishReason), Provider: providerName, ModelID: modelID}).Error()
}

// errNoOutput is the error of a model stream that ended without a finish
// chunk and without output, as the Workflow SDK reports it.
const errNoOutput = "No output generated. The model stream ended without a finish chunk."

// hasOutput reports whether content holds model output: text, reasoning, a
// tool call, or any other part that is not an empty text or reasoning block.
func hasOutput(content []ContentPart) bool {
	for _, part := range content {
		switch part.Type {
		case PartText, PartReasoning:
			if part.Text != "" {
				return true
			}
		default:
			return true
		}
	}
	return false
}

// userAgent is the user agent suffix the Workflow SDK adds to the model
// calls of a WorkflowAgent, so providers can attribute its usage.
const userAgent = "ai-sdk-agent/workflow"

// withUserAgent returns headers with the agent's user agent suffix added to
// their user-agent header.
func withUserAgent(headers map[string]string) map[string]string {
	out := maps.Clone(headers)
	if out == nil {
		out = map[string]string{}
	}
	key := "user-agent"
	for name := range out {
		if strings.EqualFold(name, key) {
			key = name
		}
	}
	out[key] = strings.TrimSpace(out[key] + " " + userAgent)
	return out
}

// logWarnings logs the warnings the provider reported for a model call
// through go-ai's warning logger, as the Workflow SDK does. The model call
// step logs them, so replays do not log them again.
func (m modelCall) logWarnings(result modelCallResult) {
	if len(result.Warnings) == 0 {
		return
	}
	ai.LogWarnings(ai.LogWarningsOptions{Warnings: result.Warnings, Provider: m.model.Provider(), Model: cmp.Or(result.Response.ModelID, m.model.ModelID())})
}

// StepPerformance is how fast a step's model call answered, as the Workflow
// SDK measures it: its response time, its time to first output, and its
// token rates.
type StepPerformance struct {
	ResponseTimeMs                 int64    `json:"responseTimeMs"`
	EffectiveOutputTokensPerSecond float64  `json:"effectiveOutputTokensPerSecond"`
	OutputTokensPerSecond          *float64 `json:"outputTokensPerSecond,omitempty"`
	InputTokensPerSecond           *float64 `json:"inputTokensPerSecond,omitempty"`
	EffectiveTotalTokensPerSecond  float64  `json:"effectiveTotalTokensPerSecond"`
	TimeToFirstOutputMs            *int64   `json:"timeToFirstOutputMs,omitempty"`
}

// measurePerformance returns the performance of a model call that started,
// produced its first output and ended at the given times.
func measurePerformance(started, firstOutput, ended time.Time, usage types.Usage) *StepPerformance {
	response := ended.Sub(started)
	performance := &StepPerformance{ResponseTimeMs: response.Milliseconds()}
	perSecond := func(tokens int64, d time.Duration) float64 {
		if d <= 0 {
			return 0
		}
		return float64(tokens) / d.Seconds()
	}
	var input, output int64
	if usage.InputTokens != nil {
		input = *usage.InputTokens
	}
	if usage.OutputTokens != nil {
		output = *usage.OutputTokens
	}
	performance.EffectiveOutputTokensPerSecond = perSecond(output, response)
	performance.EffectiveTotalTokensPerSecond = perSecond(input+output, response)
	if !firstOutput.IsZero() {
		first := firstOutput.Sub(started)
		ms := first.Milliseconds()
		performance.TimeToFirstOutputMs = &ms
		inputRate := perSecond(input, first)
		performance.InputTokensPerSecond = &inputRate
		outputRate := perSecond(output, ended.Sub(firstOutput))
		if ended.Equal(firstOutput) {
			outputRate = performance.EffectiveOutputTokensPerSecond
		}
		performance.OutputTokensPerSecond = &outputRate
	}
	return performance
}

// isOutputChunk reports whether a chunk of a model stream is model output.
func isOutputChunk(chunkType provider.ChunkType) bool {
	switch chunkType {
	case provider.ChunkTypeText, provider.ChunkTypeReasoning, provider.ChunkTypeToolCall, provider.ChunkTypeToolInputStart, provider.ChunkTypeToolInputDelta, provider.ChunkTypeFile, provider.ChunkTypeSource:
		return true
	}
	return false
}

// decodeMetadata decodes provider metadata, or returns nil for none.
func decodeMetadata(raw json.RawMessage) map[string]any {
	var metadata map[string]any
	if len(raw) == 0 || json.Unmarshal(raw, &metadata) != nil || len(metadata) == 0 {
		return nil
	}
	return metadata
}

func marshalMetadata(metadata map[string]any) json.RawMessage {
	if len(metadata) == 0 {
		return nil
	}
	return marshalRaw(metadata)
}

// parseToolCall reads a provider tool call's input and validates it against
// the tool's input schema. An invalid call to an unknown tool or with input
// that fails the schema comes with the error RepairToolCall receives.
func parseToolCall(tools []toolDefinition, call types.ToolCall) (ToolCall, error) {
	parsed := ToolCall{
		ToolCallID:       call.ID,
		ToolName:         call.ToolName,
		ProviderExecuted: call.ProviderExecuted,
		ThoughtSignature: call.ThoughtSignature,
		ProviderMetadata: marshalMetadata(call.ProviderMetadata),
	}
	raw := call.RawArguments
	if raw == "" {
		parsed.Input = marshalRaw(call.Arguments)
		if call.Arguments == nil {
			parsed.Input = json.RawMessage("{}")
		}
	} else if json.Valid([]byte(raw)) {
		parsed.Input = json.RawMessage(raw)
	} else {
		// Input that is not JSON, such as truncated arguments, is kept as a
		// JSON string, as the Workflow SDK keeps the raw text.
		parsed.Input = marshalRaw(raw)
	}
	if call.Invalid {
		parsed.Invalid, parsed.Dynamic = true, true
		parsed.Error = "invalid tool call"
		if call.Error != nil {
			parsed.Error = call.Error.Error()
		}
		return parsed, nil
	}
	if call.ProviderExecuted {
		return parsed, nil
	}
	invalidInput := func(err error) (ToolCall, error) {
		parsed.Invalid, parsed.Dynamic, parsed.Error = true, true, fmt.Sprintf("Invalid input for tool %s: %v", call.ToolName, err)
		return parsed, &ai.InvalidToolInputError{ToolName: call.ToolName, ToolInput: cmp.Or(raw, string(parsed.Input)), Cause: err}
	}
	var input any
	if err := json.Unmarshal([]byte(cmp.Or(raw, string(parsed.Input))), &input); err != nil {
		return invalidInput(err)
	}
	names := make([]string, 0, len(tools))
	for _, tool := range tools {
		names = append(names, tool.Name)
		if tool.Name != call.ToolName {
			continue
		}
		if tool.InputSchema != nil {
			if err := schema.NewJSONSchema(tool.InputSchema).Validate(input); err != nil {
				return invalidInput(err)
			}
		}
		return parsed, nil
	}
	parsed.Invalid, parsed.Dynamic, parsed.Error = true, true, fmt.Sprintf("Model tried to call unavailable tool '%s'. Available tools: %s.", call.ToolName, strings.Join(names, ", "))
	return parsed, &ai.NoSuchToolError{ToolName: call.ToolName, AvailableTools: names}
}

// promptMessages converts model messages to the go-ai messages a provider
// receives. The provider resolves the approvals of the tools it executes, so
// those approval parts are sent. Other approval parts are conversation data
// for the client and are not.
func promptMessages(messages []ModelMessage) []types.Message {
	prompt := make([]ModelMessage, len(messages))
	for i, message := range messages {
		message.Content = slices.DeleteFunc(slices.Clone(message.Content), func(part ContentPart) bool {
			return (part.Type == PartToolApprovalRequest || part.Type == PartToolApprovalResponse) && !part.ProviderExecuted
		})
		prompt[i] = message
	}
	return toProviderMessages(prompt)
}

// telemetryUsage converts token usage to its telemetry form.
func telemetryUsage(usage types.Usage) telemetry.TelemetryUsage {
	out := telemetry.TelemetryUsage{InputTokens: usage.InputTokens, OutputTokens: usage.OutputTokens, TotalTokens: usage.TotalTokens}
	if usage.InputDetails != nil {
		out.NoCacheInputTokens = usage.InputDetails.NoCacheTokens
		out.CacheReadInputTokens = usage.InputDetails.CacheReadTokens
		out.CacheCreationInputTokens = usage.InputDetails.CacheWriteTokens
	}
	if usage.OutputDetails != nil {
		out.OutputTextTokens = usage.OutputDetails.TextTokens
		out.ReasoningTokens = usage.OutputDetails.ReasoningTokens
	}
	return out
}

// modelCallKey is the context key of the modelCall a model call step uses.
type modelCallKey struct{}

// withModelCall returns ctx carrying m for the model call step it starts.
func withModelCall(ctx context.Context, m modelCall) context.Context {
	return context.WithValue(ctx, modelCallKey{}, m)
}

// modelCallFrom returns the modelCall ctx carries.
func modelCallFrom(ctx context.Context) (modelCall, error) {
	m, ok := ctx.Value(modelCallKey{}).(modelCall)
	if !ok || m.model == nil {
		return modelCall{}, errors.New("workflowai: model call step started without its model")
	}
	return m, nil
}
