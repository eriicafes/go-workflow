package workflowai

import (
	"cmp"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	// StepNumber and Streaming describe the call to telemetry.
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
	// Content is the assistant content in the order the model produced it.
	Content      []ContentPart `json:"content"`
	ToolCalls    []ToolCall    `json:"toolCalls,omitempty"`
	FinishReason string        `json:"finishReason"`
	Usage        types.Usage   `json:"usage"`
	Response     Response      `json:"response"`
	// Error is the message of the first error part of the model stream. A
	// stream error ends the turn.
	Error string `json:"error,omitempty"`
	// RequestBody is the request sent to the provider, kept when Include
	// asks for it.
	RequestBody any `json:"requestBody,omitempty"`
}

// modelCall calls a language model in a step. Its methods are the step
// functions, so a model call is identified by the method and its position in
// the run.
type modelCall struct {
	model      provider.LanguageModel
	telemetry  *telemetry.Options
	repair     ai.ToolCallRepairFunction
	download   ai.DownloadFunction
	transforms []ai.StreamTransformFunc
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
		Headers:          input.Settings.Headers,
		ProviderOptions:  input.Settings.ProviderOptions,
		Reasoning:        input.Settings.Reasoning,
		SendReasoning:    input.Settings.SendReasoning,
		ResponseFormat:   input.ResponseFormat,
		IncludeRawChunks: input.IncludeRawChunks,
	}
	for _, tool := range input.Tools {
		options.Tools = append(options.Tools, types.Tool{Name: tool.Name, Description: tool.Description, Parameters: tool.InputSchema})
	}
	if input.ToolChoice != nil {
		options.ToolChoice = *input.ToolChoice
	} else if len(options.Tools) > 0 {
		options.ToolChoice = types.AutoToolChoice()
	}
	// Normalize the prompt like go-ai does before every model call, which
	// also merges consecutive tool messages and, with a Download function,
	// downloads the file URLs the model does not support.
	prompt, err := promptutils.NormalizePromptWithDownloadSupport(ctx, options.Prompt, true, m.download, ai.SupportedURLCheckerForModel(m.model))
	if err != nil {
		return nil, err
	}
	options.Prompt = prompt
	return options, nil
}

// stream streams one model call, writing its parts to the Writer as they
// arrive.
//
// Retries only open the model stream again, and an error part in the stream
// ends the turn instead of retrying it: the part is written
// and recorded in the result, and the rest of the stream is still read for its
// finish reason and usage. The step itself is not retried, but it runs again
// when its delivery is interrupted, so it first writes a reset-step part that
// makes a client discard the parts of an earlier execution.
func (m modelCall) stream(ctx context.Context, input modelCallInput) (result modelCallResult, err error) {
	var writer io.Writer = io.Discard
	if input.Writer != nil {
		w, release, err := workflow.Writer(ctx, *input.Writer)
		if err != nil {
			return modelCallResult{}, err
		}
		defer release()
		if err := writePart(w, ModelCallStreamPart{Type: "reset-step"}); err != nil {
			return modelCallResult{}, err
		}
		writer = w
	}

	options, err := m.options(ctx, input)
	if err != nil {
		return modelCallResult{}, err
	}
	ctx, endSpans := m.startSpans(ctx, input, options)
	defer func() { endSpans(result, err) }()
	stream, err := retry(ctx, input.Settings.MaxRetries, func() (provider.TextStream, error) {
		return m.model.DoStream(ctx, options)
	})
	if err != nil {
		return modelCallResult{}, cancelCause(ctx, err)
	}
	defer stream.Close()

	acc := newAccumulator(func(call types.ToolCall) ToolCall { return m.toolCall(ctx, input, call) })
	for {
		chunk, err := stream.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return modelCallResult{}, cancelCause(ctx, err)
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
	if acc.result.Error == "" && acc.result.FinishReason == "" && len(acc.result.Content) == 0 {
		acc.result.Error = "no output generated"
		if err := writePart(writer, ModelCallStreamPart{Type: "error", Error: acc.result.Error}); err != nil {
			return modelCallResult{}, err
		}
	}
	return acc.result, nil
}

// generate makes one model call without streaming, retrying it like stream
// retries opening its stream.
func (m modelCall) generate(ctx context.Context, input modelCallInput) (result modelCallResult, err error) {
	options, err := m.options(ctx, input)
	if err != nil {
		return modelCallResult{}, err
	}
	ctx, endSpans := m.startSpans(ctx, input, options)
	defer func() { endSpans(result, err) }()
	generated, err := retry(ctx, input.Settings.MaxRetries, func() (*types.GenerateResult, error) {
		return m.model.DoGenerate(ctx, options)
	})
	if err != nil {
		return modelCallResult{}, cancelCause(ctx, err)
	}
	result = modelCallResult{FinishReason: string(generated.FinishReason), Usage: generated.Usage}
	if generated.ResponseMetadata != nil {
		result.Response = Response{ID: generated.ResponseMetadata.ID, ModelID: generated.ResponseMetadata.ModelID}
	}
	if input.Include != nil && input.Include.RequestBody {
		result.RequestBody = generated.RawRequest
	}
	if input.Include != nil && input.Include.ResponseBody {
		result.Response.Body = generated.RawResponse
	}
	hasText := false
	for _, content := range generated.Content {
		if part, ok := fromContentPart(content); ok && part.Type != PartToolCall {
			hasText = hasText || part.Type == PartText
			result.Content = append(result.Content, part)
		}
	}
	if !hasText && generated.Text != "" {
		result.Content = append(result.Content, ContentPart{Type: PartText, Text: generated.Text})
	}
	for _, call := range generated.ToolCalls {
		toolCall := m.toolCall(ctx, input, call)
		result.ToolCalls = append(result.ToolCalls, toolCall)
		result.Content = append(result.Content, toolCall.contentPart())
	}
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

// startSpans starts the spans of a model call under the span in ctx:
// an operation span, a step span, and the model call span, whose context
// the model call uses. The returned function ends them with the call's
// result.
func (m modelCall) startSpans(ctx context.Context, input modelCallInput, options *provider.GenerateOptions) (context.Context, func(modelCallResult, error)) {
	if m.telemetry == nil || !telemetry.Enabled(m.telemetry) {
		return ctx, func(modelCallResult, error) {}
	}
	callID := workflow.StepID(ctx)
	operation := "ai.generateText"
	if input.Streaming {
		operation = "ai.streamText"
	}
	settings, modelProvider, modelID := m.telemetry, m.model.Provider(), m.model.ModelID()
	ctx = telemetry.FireOnStart(ctx, telemetry.TelemetryStartEvent{
		CallID:        callID,
		OperationType: operation,
		ModelProvider: modelProvider,
		ModelID:       modelID,
		Settings:      settings,
		System:        input.Instructions,
		Messages:      options.Prompt.Messages,
	})
	ctx = telemetry.FireOnStepStart(ctx, telemetry.TelemetryStepStartEvent{
		Settings:      settings,
		CallID:        callID,
		OperationType: operation,
		StepNumber:    input.StepNumber,
		ModelProvider: modelProvider,
		ModelID:       modelID,
	})
	callCtx := telemetry.FireOnLanguageModelCallStart(ctx, telemetry.LanguageModelCallStartEvent{
		Settings:         settings,
		CallID:           callID,
		ModelProvider:    modelProvider,
		ModelID:          modelID,
		Prompt:           options.Prompt,
		Tools:            options.Tools,
		System:           input.Instructions,
		Temperature:      options.Temperature,
		MaxOutputTokens:  options.MaxTokens,
		TopP:             options.TopP,
		TopK:             options.TopK,
		PresencePenalty:  options.PresencePenalty,
		FrequencyPenalty: options.FrequencyPenalty,
		StopSequences:    options.StopSequences,
		Seed:             options.Seed,
	})
	start := time.Now()
	return callCtx, func(result modelCallResult, err error) {
		if err != nil {
			telemetry.FireOnStepError(callCtx, telemetry.TelemetryErrorEvent{Settings: settings, CallID: callID, Error: err})
			telemetry.FireOnError(ctx, telemetry.TelemetryErrorEvent{Settings: settings, CallID: callID, Error: err})
			return
		}
		usage := telemetryUsage(result.Usage)
		var text string
		for _, part := range result.Content {
			if part.Type == PartText {
				text += part.Text
			}
		}
		telemetry.FireOnLanguageModelCallEnd(ctx, telemetry.LanguageModelCallEndEvent{
			Settings:      settings,
			CallID:        callID,
			ModelProvider: modelProvider,
			ModelID:       modelID,
			FinishReason:  result.FinishReason,
			Usage:         usage,
			ResponseID:    result.Response.ID,
			Performance:   telemetry.LanguageModelCallPerformance{ResponseTimeMs: time.Since(start).Milliseconds()},
		})
		telemetry.FireOnStepEnd(ctx, telemetry.TelemetryStepEndEvent{
			Settings:      settings,
			CallID:        callID,
			OperationType: operation,
			StepNumber:    input.StepNumber,
			FinishReason:  result.FinishReason,
			Usage:         usage,
			Text:          text,
			ResponseID:    result.Response.ID,
		})
		telemetry.FireOnEnd(ctx, telemetry.TelemetryFinishEvent{
			Settings:      settings,
			CallID:        callID,
			OperationType: operation,
			FinishReason:  result.FinishReason,
			Usage:         usage,
			ModelProvider: modelProvider,
			ModelID:       modelID,
			Text:          text,
		})
	}
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
}

func newAccumulator(parse func(types.ToolCall) ToolCall) *accumulator {
	return &accumulator{parse: parse, open: map[string]int{}}
}

func (a *accumulator) add(chunk *provider.StreamChunk) []ModelCallStreamPart {
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
		return append(parts, ModelCallStreamPart{
			Type:             "tool-call",
			ToolCallID:       call.ToolCallID,
			ToolName:         call.ToolName,
			Input:            call.Input,
			Invalid:          call.Invalid,
			Error:            call.Error,
			ProviderExecuted: call.ProviderExecuted,
			ProviderMetadata: marshalMetadata(chunk.ToolCall.ProviderMetadata),
		})
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
			a.result.Response = Response{ID: chunk.ResponseMetadata.ID, ModelID: chunk.ResponseMetadata.ModelID}
		}
	case provider.ChunkTypeUsage:
		if chunk.Usage != nil {
			a.result.Usage = *chunk.Usage
		}
	case provider.ChunkTypeFinish, provider.ChunkTypeFinishStep:
		if chunk.FinishReason != "" {
			a.result.FinishReason = string(chunk.FinishReason)
		}
		if chunk.Usage != nil {
			a.result.Usage = *chunk.Usage
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
func retry[T any](ctx context.Context, maxRetries *int, fn func() (T, error)) (T, error) {
	retries := 2
	if maxRetries != nil {
		retries = *maxRetries
	}
	delay := 2 * time.Second
	for attempt := 0; ; attempt++ {
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

// cancelCause returns the cause of ctx's cancellation when ctx is done, so
// a model call cut short by cancellation reports it instead of the
// provider's error, and err otherwise.
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
	}
	raw := call.RawArguments
	if raw == "" {
		parsed.Input = marshalRaw(call.Arguments)
		if call.Arguments == nil {
			parsed.Input = json.RawMessage("{}")
		}
	} else {
		parsed.Input = json.RawMessage(raw)
	}
	if call.Invalid {
		parsed.Invalid = true
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
		parsed.Invalid, parsed.Error = true, fmt.Sprintf("invalid input for tool %s: %v", call.ToolName, err)
		return parsed, &ai.InvalidToolInputError{ToolName: call.ToolName, ToolInput: string(parsed.Input), Cause: err}
	}
	var input any
	if err := json.Unmarshal(parsed.Input, &input); err != nil {
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
	parsed.Invalid, parsed.Error = true, fmt.Sprintf("model tried to call unavailable tool %q", call.ToolName)
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
