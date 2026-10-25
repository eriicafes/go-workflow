package workflowai

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/digitallysavvy/go-ai/pkg/ai"
	"github.com/digitallysavvy/go-ai/pkg/provider"
	"github.com/digitallysavvy/go-ai/pkg/provider/types"
	"github.com/digitallysavvy/go-ai/pkg/telemetry"
	"github.com/eriicafes/go-workflow"
	"go.opentelemetry.io/otel/propagation"
)

// WorkflowAgentOptions configures a WorkflowAgent.
type WorkflowAgentOptions struct {
	// ID identifies the agent.
	ID string
	// Model is the language model the agent calls. The agent is built in
	// workflow context, and every replay and step delivery of the run builds
	// it again, so the model never crosses a step boundary.
	Model provider.LanguageModel
	// Instructions is the system prompt.
	Instructions string
	// InstructionMessages are system messages sent before the conversation,
	// after Instructions. Unlike Instructions, they can carry provider
	// options, such as cache control.
	InstructionMessages []ModelMessage
	// AllowSystemInMessages allows system messages in a call's Messages.
	// Without it, a call with system messages fails, so a client cannot
	// add instructions to the conversation it sends.
	AllowSystemInMessages bool
	Tools                 ToolSet
	// ToolChoice defaults to auto when the agent has tools.
	ToolChoice *types.ToolChoice
	// ActiveTools limits the tools the model can call. Nil allows all.
	ActiveTools []string
	// Output parses the final text of a call into Result.Output and gives
	// the model the format to answer in.
	Output Output
	// RepairToolCall repairs a tool call to an unknown tool or with input
	// that fails the tool's schema. It runs in the model call's step, once
	// per invalid call. A call it cannot repair is reported to the model as
	// invalid.
	RepairToolCall ai.ToolCallRepairFunction
	// Download fetches the file URLs in messages that the model does not
	// support, before each model call and inside its step. Nil leaves URLs
	// as they are.
	Download ai.DownloadFunction
	// Telemetry records OpenTelemetry spans through go-ai's telemetry
	// integrations. A call's spans form one trace: its root span starts and
	// ends in an inline step at the start of the call, and every step the
	// call starts gets its trace context through ctx. Nil records nothing.
	Telemetry *telemetry.Options
	// Timeout limits how long the model calls of a Stream or Generate call
	// run, counted durably from the start of the call. When it passes, the
	// running model call is cancelled and the call is aborted. Zero has no
	// limit.
	Timeout time.Duration
	// ToolCallConcurrency limits how many local tool calls of a step,
	// including approved calls, execute at once. 1 executes them one at a
	// time in the model's order. Zero allows any number.
	ToolCallConcurrency int
	// StopWhen stops the loop after a step with tool results when any
	// condition is met. Stream has no step limit by default and Generate
	// stops after 20 steps.
	StopWhen []StopCondition
	GenerationSettings
	// RuntimeContext is shared state passed to PrepareStep. It must encode
	// to JSON when it is returned from a workflow.
	RuntimeContext any
	// ToolsContext holds each tool's context, passed to its Execute and
	// NeedsApproval as ToolCallOptions.Context.
	ToolsContext map[string]any
	// ToolApprovalSecret signs tool approval requests with HMAC, so an
	// approval response only executes a call the agent asked approval for
	// with the same input. Use at least 32 random bytes, the same in every
	// process that runs the workflow. Changing it invalidates pending
	// approvals.
	ToolApprovalSecret []byte
	// PrepareCall changes the settings of each Stream and Generate call
	// before the loop starts.
	PrepareCall PrepareCallFunc
	PrepareStep PrepareStepFunc
	Callbacks
}

// CallOptions configures one Stream or Generate call. Fields that are set
// override the agent's WorkflowAgentOptions.
type CallOptions struct {
	// Messages is the conversation to continue. When its last message is a
	// tool message with approval responses, the approved calls execute and
	// the denied calls are reported to the model before the first model
	// call.
	Messages []ModelMessage
	// Prompt is a user message to start from instead of Messages.
	Prompt string

	Instructions        string
	InstructionMessages []ModelMessage
	ToolChoice          *types.ToolChoice
	ActiveTools         []string
	ToolCallConcurrency int
	Timeout             time.Duration
	Output              Output
	RepairToolCall      ai.ToolCallRepairFunction
	Download            ai.DownloadFunction
	Telemetry           *telemetry.Options
	StopWhen            []StopCondition
	// RuntimeContext and ToolsContext replace the agent's.
	RuntimeContext any
	ToolsContext   map[string]any
	PrepareStep    PrepareStepFunc
	Callbacks

	// Transform rewrites the provider chunks of each streamed model call, in
	// order, inside its step. Generate does not stream, so it ignores it.
	Transform []ai.StreamTransformFunc
	// IncludeRawChunks writes the provider's raw chunks to the stream as raw
	// parts, for models that support it.
	IncludeRawChunks bool
	// Include keeps the request and response bodies of Generate's model
	// calls, and the messages sent, in each StepResult.
	Include *ai.IncludeOptions

	// Writer names the run stream that receives the ModelCallStreamParts of
	// a Stream. Nil writes the run's default stream. Streams can only be
	// opened in step context, so the steps that write open it with
	// workflow.Writer. Generate does not write.
	Writer *workflow.WriterOption
	// SendFinish writes a finish part before closing the Writer's stream.
	// Nil sends it.
	SendFinish *bool
	// PreventClose leaves the Writer's stream open, so the run can write
	// more after the agent.
	PreventClose bool
}

// PrepareCallFunc changes the settings of a Stream or Generate call. It runs
// in workflow context once, before approved calls resume and before the
// first model call.
type PrepareCallFunc func(ctx context.Context, options PrepareCallOptions) (PrepareCallResult, error)

// PrepareCallOptions holds the settings of a call: the agent's
// WorkflowAgentOptions with the call's CallOptions applied.
type PrepareCallOptions struct {
	Model               provider.LanguageModel
	Tools               ToolSet
	Instructions        string
	InstructionMessages []ModelMessage
	Messages            []ModelMessage
	ToolChoice          *types.ToolChoice
	ActiveTools         []string
	ToolCallConcurrency int
	StopWhen            []StopCondition
	RuntimeContext      any
	ToolsContext        map[string]any
	GenerationSettings
}

// PrepareCallResult holds the settings PrepareCall changes for the call.
// Zero fields keep the current settings, and set GenerationSettings fields
// replace the agent's. Tools cannot change. A ToolCallConcurrency set in
// CallOptions takes precedence over this one.
type PrepareCallResult struct {
	Model               provider.LanguageModel
	Instructions        string
	InstructionMessages []ModelMessage
	Messages            []ModelMessage
	ToolChoice          *types.ToolChoice
	ActiveTools         []string
	ToolCallConcurrency int
	StopWhen            []StopCondition
	RuntimeContext      any
	ToolsContext        map[string]any
	GenerationSettings
}

// PrepareStepFunc changes the settings of the next model call. It runs in
// workflow context before every step.
type PrepareStepFunc func(ctx context.Context, info PrepareStepInfo) (PrepareStepResult, error)

// PrepareStepInfo describes the step about to run.
type PrepareStepInfo struct {
	StepNumber     int
	Steps          []StepResult
	Messages       []ModelMessage
	Model          provider.LanguageModel
	RuntimeContext any
}

// PrepareStepResult holds the settings PrepareStep changes for one step.
// Zero fields keep the current settings.
type PrepareStepResult struct {
	Model        provider.LanguageModel
	Instructions string
	// Messages replaces the messages sent to the model in this step.
	Messages    []ModelMessage
	ToolChoice  *types.ToolChoice
	ActiveTools []string
	// RuntimeContext replaces the runtime context for this and later steps.
	RuntimeContext any
}

// Result is the result of Stream or Generate. It encodes to JSON, so a
// workflow can return it.
type Result struct {
	// Messages is the conversation including the agent's response. When the
	// agent stopped for approval, its last assistant message carries the
	// tool-approval-request parts.
	Messages []ModelMessage `json:"messages"`
	Steps    []StepResult   `json:"steps"`
	// ToolCalls are the last step's calls, including calls that wait for
	// approval or for a client tool's result.
	ToolCalls []ToolCall `json:"toolCalls,omitempty"`
	// ToolResults are the last step's executed results.
	ToolResults  []ToolResult `json:"toolResults,omitempty"`
	Text         string       `json:"text"`
	FinishReason string       `json:"finishReason"`
	TotalUsage   types.Usage  `json:"totalUsage"`
	// Error is the message of an error part in the last model call's
	// stream, which ended the turn. Stream writes it to the stream and
	// reports it here rather than returning an error.
	Error string `json:"error,omitempty"`
	// Output is the value the call's Output parsed from the last step's
	// text. It is nil without an Output, or when the last step called tools.
	Output any `json:"output,omitempty"`
}

// StepResult is the result of one model call and the tool calls it made.
type StepResult struct {
	StepNumber int           `json:"stepNumber"`
	Content    []ContentPart `json:"content"`
	Text       string        `json:"text"`
	ToolCalls  []ToolCall    `json:"toolCalls,omitempty"`
	// ToolResults holds the results of the calls that executed in the
	// step.
	ToolResults  []ToolResult `json:"toolResults,omitempty"`
	FinishReason string       `json:"finishReason"`
	Usage        types.Usage  `json:"usage"`
	// Request holds what CallOptions.Include asks to keep of the request.
	Request  StepRequest `json:"request,omitzero"`
	Response Response    `json:"response"`
}

// StepRequest is the request of a step's model call.
type StepRequest struct {
	// Body is the request sent to the provider.
	Body any `json:"body,omitempty"`
	// Messages are the messages sent to the model.
	Messages []ModelMessage `json:"messages,omitempty"`
}

// Response identifies the model response of a step.
type Response struct {
	ID      string `json:"id,omitempty"`
	ModelID string `json:"modelId,omitempty"`
	// Messages are the messages the step added to the conversation.
	Messages []ModelMessage `json:"messages,omitempty"`
	// Body is the response from the provider, kept when CallOptions.Include
	// asks for it.
	Body any `json:"body,omitempty"`
}

// writePartsInput is the input of the writePartsStep step.
type writePartsInput struct {
	Writer workflow.WriterOption
	Parts  []ModelCallStreamPart
	Close  bool
}

// WorkflowAgent is an agent whose loop runs as a durable workflow. Each model
// call is a step, and each tool call
// runs its tool's Execute in workflow context, which records its steps. A
// run that stops, crashes, or restarts continues from the last recorded
// model call or tool step instead of starting over, and the parts it streams
// stay in the run's stream for clients to reconnect to.
//
// Create the agent and call Stream or Generate inside a workflow runner.
type WorkflowAgent struct {
	options WorkflowAgentOptions
}

// NewWorkflowAgent returns a WorkflowAgent.
func NewWorkflowAgent(options WorkflowAgentOptions) *WorkflowAgent {
	return &WorkflowAgent{options: options}
}

// ID returns the agent's ID.
func (a *WorkflowAgent) ID() string { return a.options.ID }

// Tools returns the agent's tools.
func (a *WorkflowAgent) Tools() ToolSet { return a.options.Tools }

// Stream runs the agent loop, writing the stream parts of every model call
// and tool result to the run's default stream, or the stream options.Writer
// names, and closes it when the loop ends. It must be called in workflow
// context. It returns an error when the run
// fails, such as when a model call cannot be opened, and reports an error
// part in a model stream in Result.Error.
//
// The loop ends when a step makes no tool calls, a stop condition is met, or
// a call needs approval or runs on the client. In the last two cases the
// stream ends with the pending calls, and the next Stream continues them from
// the messages the client sends back.
func (a *WorkflowAgent) Stream(ctx context.Context, options CallOptions) (Result, error) {
	if options.Writer == nil {
		options.Writer = &workflow.WriterOption{}
	}
	return a.execute(ctx, options, true)
}

// Generate runs the agent loop like Stream, with model calls that do not
// stream and without writing to a stream. It stops after 20 steps unless
// StopWhen is set.
func (a *WorkflowAgent) Generate(ctx context.Context, options CallOptions) (Result, error) {
	options.Writer = nil
	return a.execute(ctx, options, false)
}

func (a *WorkflowAgent) execute(ctx context.Context, options CallOptions, streaming bool) (Result, error) {
	r := &run{
		agent:               a,
		options:             options,
		callbacks:           a.options.Callbacks.merge(options.Callbacks),
		streaming:           streaming,
		model:               a.options.Model,
		instructions:        cmp.Or(options.Instructions, a.options.Instructions),
		instructionMessages: orNil(options.InstructionMessages, a.options.InstructionMessages),
		timeout:             cmp.Or(options.Timeout, a.options.Timeout),
		output:              options.Output,
		repair:              options.RepairToolCall,
		download:            options.Download,
		telemetry:           cmp.Or(options.Telemetry, a.options.Telemetry),
		tools:               a.options.Tools,
		toolChoice:          cmp.Or(options.ToolChoice, a.options.ToolChoice),
		activeTools:         orNil(options.ActiveTools, a.options.ActiveTools),
		concurrency:         cmp.Or(options.ToolCallConcurrency, a.options.ToolCallConcurrency),
		stopWhen:            orNil(options.StopWhen, a.options.StopWhen),
		settings:            a.options.GenerationSettings,
		runtimeContext:      options.RuntimeContext,
		toolsContext:        options.ToolsContext,
		prepareStep:         options.PrepareStep,
		messages:            slices.Clone(options.Messages),
	}
	if r.runtimeContext == nil {
		r.runtimeContext = a.options.RuntimeContext
	}
	if r.toolsContext == nil {
		r.toolsContext = a.options.ToolsContext
	}
	if r.prepareStep == nil {
		r.prepareStep = a.options.PrepareStep
	}
	if r.output == nil {
		r.output = a.options.Output
	}
	if r.repair == nil {
		r.repair = a.options.RepairToolCall
	}
	if r.download == nil {
		r.download = a.options.Download
	}
	if r.stopWhen == nil && !streaming {
		r.stopWhen = []StopCondition{IsStepCount(20)}
	}
	if options.Prompt != "" {
		r.messages = []ModelMessage{{Role: RoleUser, Content: []ContentPart{{Type: PartText, Text: options.Prompt}}}}
	}
	ctx, err := r.prepare(ctx)
	if err != nil {
		closeErr := r.closeWriter(ctx, ModelCallStreamPart{Type: "error", Error: err.Error()})
		return Result{}, errors.Join(err, closeErr)
	}

	r.callbacks.OnStart(ctx, StartEvent{Messages: r.messages})
	result, err := r.loop(ctx)
	if err == nil && result.Error == "" {
		result.Output, err = r.parseOutput(ctx)
	}
	if r.stopTimeout != nil {
		err = errors.Join(err, r.stopTimeout())
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		// A cancelled context or the Timeout aborted the call. The steps
		// before it stay in the result, and the stream ends normally.
		r.callbacks.OnAbort(ctx, AbortEvent{Steps: r.steps})
		return result, errors.Join(err, r.closeWriter(ctx))
	}
	if err != nil {
		r.callbacks.OnError(ctx, err)
		// Report the failure to stream readers and end the stream, so they
		// do not wait for parts that never come.
		closeErr := r.closeWriter(ctx, ModelCallStreamPart{Type: "error", Error: err.Error()})
		return result, errors.Join(err, closeErr)
	}
	if result.Error != "" {
		r.callbacks.OnError(ctx, errors.New(result.Error))
	}
	r.callbacks.OnEnd(ctx, result)
	return result, r.closeWriter(ctx)
}

// run holds the state of one Stream or Generate call.
type run struct {
	agent        *WorkflowAgent
	options      CallOptions
	callbacks    Callbacks
	streaming    bool
	model        provider.LanguageModel
	instructions string
	// instructionMessages come before the conversation in model calls.
	instructionMessages []ModelMessage
	// timeout limits the call's model calls, through modelCtx.
	timeout  time.Duration
	modelCtx context.Context
	// output and responseFormat parse and shape the final answer.
	output         Output
	responseFormat *provider.ResponseFormat
	repair         ai.ToolCallRepairFunction
	download       ai.DownloadFunction
	telemetry      *telemetry.Options
	// stopTimeout disarms the Timeout once the call ends.
	stopTimeout    func() error
	tools          ToolSet
	toolChoice     *types.ToolChoice
	activeTools    []string
	concurrency    int
	stopWhen       []StopCondition
	settings       GenerationSettings
	runtimeContext any
	toolsContext   map[string]any
	prepareStep    PrepareStepFunc
	messages       []ModelMessage
	steps          []StepResult
}

// prepare applies the agent's PrepareCall to the call's settings and
// validates them.
func (r *run) prepare(ctx context.Context) (context.Context, error) {
	if prepareCall := r.agent.options.PrepareCall; prepareCall != nil {
		prepared, err := prepareCall(ctx, PrepareCallOptions{
			Model:               r.model,
			Tools:               r.tools,
			Instructions:        r.instructions,
			InstructionMessages: r.instructionMessages,
			Messages:            r.messages,
			ToolChoice:          r.toolChoice,
			ActiveTools:         r.activeTools,
			ToolCallConcurrency: r.concurrency,
			StopWhen:            r.stopWhen,
			RuntimeContext:      r.runtimeContext,
			ToolsContext:        r.toolsContext,
			GenerationSettings:  r.settings,
		})
		if err != nil {
			return ctx, err
		}
		r.model = cmp.Or(prepared.Model, r.model)
		r.instructions = cmp.Or(prepared.Instructions, r.instructions)
		r.instructionMessages = orNil(prepared.InstructionMessages, r.instructionMessages)
		r.messages = orNil(slices.Clone(prepared.Messages), r.messages)
		r.toolChoice = cmp.Or(prepared.ToolChoice, r.toolChoice)
		r.activeTools = orNil(prepared.ActiveTools, r.activeTools)
		r.concurrency = cmp.Or(r.options.ToolCallConcurrency, prepared.ToolCallConcurrency, r.concurrency)
		r.stopWhen = orNil(prepared.StopWhen, r.stopWhen)
		r.runtimeContext = cmp.Or(prepared.RuntimeContext, r.runtimeContext)
		if prepared.ToolsContext != nil {
			r.toolsContext = prepared.ToolsContext
		}
		r.settings = mergeSettings(r.settings, prepared.GenerationSettings)
	}
	switch {
	case r.model == nil:
		return ctx, errors.New("workflowai: model is required")
	case r.concurrency < 0:
		return ctx, errors.New("workflowai: ToolCallConcurrency must not be negative")
	case slices.ContainsFunc(r.instructionMessages, func(message ModelMessage) bool { return message.Role != RoleSystem }):
		return ctx, errors.New("workflowai: InstructionMessages must be system messages")
	case !r.agent.options.AllowSystemInMessages && slices.ContainsFunc(r.messages, func(message ModelMessage) bool { return message.Role == RoleSystem }):
		return ctx, errors.New("workflowai: system messages in Messages need AllowSystemInMessages; use Instructions instead")
	}
	if r.output != nil {
		format, err := r.output.ResponseFormat(ctx)
		if err != nil {
			return ctx, err
		}
		r.responseFormat = format
	}
	if r.telemetry != nil && telemetry.Enabled(r.telemetry) {
		operation := "ai.workflowAgent.generate"
		if r.streaming {
			operation = "ai.workflowAgent.stream"
		}
		// The root span is recorded in an inline step, and every step the
		// call starts gets its trace context through ctx.
		traceparent, err := workflow.InlineStep(ctx, traceStart{settings: r.telemetry, model: r.model}.run, traceStartInput{Operation: operation, Instructions: r.instructions})
		if err != nil {
			return ctx, err
		}
		ctx = propagation.TraceContext{}.Extract(ctx, propagation.MapCarrier{"traceparent": traceparent})
	}
	r.modelCtx = ctx
	if r.timeout > 0 {
		modelCtx, stop, err := workflow.WithTimeout(ctx, r.timeout)
		if err != nil {
			return ctx, err
		}
		r.modelCtx, r.stopTimeout = modelCtx, stop
	}
	return ctx, nil
}

// parseOutput parses the last step's text with the call's Output. It parses
// nothing without an Output, or when the last step called tools.
func (r *run) parseOutput(ctx context.Context) (any, error) {
	if r.output == nil || len(r.steps) == 0 {
		return nil, nil
	}
	last := r.steps[len(r.steps)-1]
	if last.Text == "" || last.FinishReason == string(types.FinishReasonToolCalls) {
		return nil, nil
	}
	usage := last.Usage
	return r.output.ParseOutput(ctx, ai.ParseCompleteOutputOptions{
		Text:         last.Text,
		Response:     &types.ResponseMetadata{ID: last.Response.ID, ModelID: last.Response.ModelID},
		Usage:        &usage,
		FinishReason: types.FinishReason(last.FinishReason),
	})
}

func (r *run) loop(ctx context.Context) (Result, error) {
	if err := r.resumeApprovals(ctx); err != nil {
		return r.result(nil, nil), err
	}
	for {
		stepNumber := len(r.steps)
		model, instructions, messages := r.model, r.instructions, r.messages
		toolChoice, activeTools := r.toolChoice, r.activeTools
		if r.prepareStep != nil {
			prepared, err := r.prepareStep(ctx, PrepareStepInfo{
				StepNumber:     stepNumber,
				Steps:          r.steps,
				Messages:       r.messages,
				Model:          r.model,
				RuntimeContext: r.runtimeContext,
			})
			if err != nil {
				return r.result(nil, nil), err
			}
			if prepared.Model != nil {
				model = prepared.Model
			}
			instructions = cmp.Or(prepared.Instructions, instructions)
			toolChoice = cmp.Or(prepared.ToolChoice, toolChoice)
			activeTools = orNil(prepared.ActiveTools, activeTools)
			if prepared.Messages != nil {
				messages = prepared.Messages
			}
			if prepared.RuntimeContext != nil {
				r.runtimeContext = prepared.RuntimeContext
			}
		}
		r.callbacks.OnStepStart(ctx, StepStartEvent{StepNumber: stepNumber, Messages: messages})

		input := modelCallInput{
			Instructions:     instructions,
			Messages:         slices.Concat(r.instructionMessages, messages),
			Tools:            r.toolDefinitions(activeTools),
			ToolChoice:       toolChoice,
			Settings:         r.settings,
			ResponseFormat:   r.responseFormat,
			IncludeRawChunks: r.options.IncludeRawChunks,
			StepNumber:       stepNumber,
			Streaming:        r.streaming,
			Include:          r.options.Include,
			Writer:           r.options.Writer,
		}
		call := modelCall{model: model, telemetry: r.telemetry, repair: r.repair, download: r.download, transforms: r.options.Transform}.generate
		if r.streaming {
			call = modelCall{model: model, telemetry: r.telemetry, repair: r.repair, download: r.download, transforms: r.options.Transform}.stream
		}
		// The model call retries opening the model itself, so the step is not
		// retried: a retry would repeat a model call whose stream failed
		// partway.
		response, err := workflow.Step(r.modelCtx, call, input, workflow.StepOptions{MaxRetries: 0})
		if err != nil {
			return r.result(nil, nil), err
		}

		step := StepResult{
			StepNumber:   stepNumber,
			Content:      response.Content,
			ToolCalls:    response.ToolCalls,
			FinishReason: response.FinishReason,
			Usage:        response.Usage,
			Response:     response.Response,
		}
		if include := r.options.Include; include != nil {
			step.Request.Body = response.RequestBody
			if include.RequestMessages {
				step.Request.Messages = input.Messages
			}
		}
		for _, part := range response.Content {
			if part.Type == PartText {
				step.Text += part.Text
			}
		}
		history := r.messages
		var added []ModelMessage
		if len(response.Content) > 0 {
			added = append(added, ModelMessage{Role: RoleAssistant, Content: response.Content})
		}

		// An error in the model stream ends the turn without executing the
		// step's tool calls.
		if response.Error != "" {
			step.Response.Messages = added
			r.messages = append(r.messages, added...)
			r.steps = append(r.steps, step)
			r.callbacks.OnStepEnd(ctx, step)
			result := r.result(step.ToolCalls, nil)
			result.Error = response.Error
			return result, nil
		}

		toolExecutionAllowed := step.FinishReason == string(types.FinishReasonToolCalls) || step.FinishReason == string(types.FinishReasonStop)
		var localCalls []ToolCall
		for _, call := range step.ToolCalls {
			if !call.ProviderExecuted {
				localCalls = append(localCalls, call)
			}
		}
		if !toolExecutionAllowed || len(localCalls) == 0 {
			step.Response.Messages = added
			r.messages = append(r.messages, added...)
			r.steps = append(r.steps, step)
			r.callbacks.OnStepEnd(ctx, step)
			return r.result(step.ToolCalls, nil), nil
		}

		// Split the calls into those to execute now and those that pause the
		// loop: client tools and calls that need approval.
		var executable, paused []ToolCall
		var approvalRequests []ContentPart
		for _, call := range localCalls {
			tool, known := r.tools[call.ToolName]
			if call.Invalid || !known {
				executable = append(executable, call)
				continue
			}
			needsApproval := false
			if tool.NeedsApproval != nil {
				needsApproval, err = tool.NeedsApproval(ctx, call.Input, r.toolCallOptions(call, history))
				if err != nil {
					return r.result(nil, nil), err
				}
			}
			switch {
			case needsApproval:
				request, err := r.approvalRequest(call)
				if err != nil {
					return r.result(nil, nil), err
				}
				approvalRequests = append(approvalRequests, request)
				paused = append(paused, call)
			case tool.Execute == nil:
				paused = append(paused, call)
			default:
				executable = append(executable, call)
			}
		}

		results, parts, err := r.executeTools(ctx, stepNumber, executable, history)
		if err != nil {
			return r.result(nil, nil), err
		}
		step.ToolResults = executedResults(results)
		if len(results) > 0 {
			added = append(added, toolMessage(results))
		}

		// A provider approval request also pauses the loop, since the
		// provider waits for the user's response.
		providerApproval := slices.ContainsFunc(response.Content, func(part ContentPart) bool {
			return part.Type == PartToolApprovalRequest
		})
		if len(paused) > 0 || providerApproval {
			added = addApprovalRequests(added, approvalRequests)
			step.Response.Messages = added
			r.messages = append(r.messages, added...)
			r.steps = append(r.steps, step)
			r.callbacks.OnStepEnd(ctx, step)
			for _, request := range approvalRequests {
				parts = append(parts, ModelCallStreamPart{Type: "tool-approval-request", ApprovalID: request.ApprovalID, ToolCallID: request.ToolCallID, Signature: request.Signature})
			}
			if err := r.write(ctx, parts...); err != nil {
				return r.result(nil, nil), err
			}
			return r.result(step.ToolCalls, step.ToolResults), nil
		}

		step.Response.Messages = added
		r.messages = append(r.messages, added...)
		r.steps = append(r.steps, step)
		// Separate the tool call step from the next model call in the UI
		// message, so the client converts them to separate messages.
		parts = append(parts, ModelCallStreamPart{Type: "finish-step"}, ModelCallStreamPart{Type: "start-step"})
		if err := r.write(ctx, parts...); err != nil {
			return r.result(nil, nil), err
		}
		r.callbacks.OnStepEnd(ctx, step)
		if slices.ContainsFunc(r.stopWhen, func(stop StopCondition) bool { return stop(r.steps) }) {
			return r.result(step.ToolCalls, step.ToolResults), nil
		}
	}
}

// toolDefinitions returns the definitions of the active tools in name order.
func (r *run) toolDefinitions(activeTools []string) []toolDefinition {
	names := make([]string, 0, len(r.tools))
	for name := range r.tools {
		if activeTools == nil || slices.Contains(activeTools, name) {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	definitions := make([]toolDefinition, len(names))
	for i, name := range names {
		definitions[i] = toolDefinition{Name: name, Description: r.tools[name].Description, InputSchema: r.tools[name].inputSchema()}
	}
	return definitions
}

func (r *run) toolCallOptions(call ToolCall, messages []ModelMessage) ToolCallOptions {
	return ToolCallOptions{ToolCallID: call.ToolCallID, Messages: messages, Context: r.toolsContext[call.ToolName]}
}

// executeTools executes calls, at most r.concurrency at once when it is set,
// and returns their results and the stream parts that report them in call
// order. Calls start in order, and a call starts when an earlier one settles
// in recorded order, so replay starts them the same way.
func (r *run) executeTools(ctx context.Context, stepNumber int, calls []ToolCall, messages []ModelMessage) ([]toolResult, []ModelCallStreamPart, error) {
	results := make([]toolResult, len(calls))
	var running []int
	var pending []*workflow.Result[any]
	for next := 0; next < len(calls) || len(running) > 0; {
		for next < len(calls) && (r.concurrency == 0 || len(running) < r.concurrency) {
			i, call := next, calls[next]
			next++
			tool, known := r.tools[call.ToolName]
			switch {
			case call.Invalid:
				results[i] = errorResult(call, call.Error)
				results[i].invalid = true
			case !known:
				results[i] = errorResult(call, fmt.Sprintf("tool %q not found", call.ToolName))
				results[i].invalid = true
			default:
				r.callbacks.OnToolExecutionStart(ctx, ToolExecutionStartEvent{StepNumber: stepNumber, ToolCall: call})
				result := tool.Execute.Execute(ctx, call.Input, r.toolCallOptions(call, messages))
				if result == nil {
					result = workflow.NewResult[any](nil, workflow.ErrNilResult)
				}
				running = append(running, i)
				pending = append(pending, result)
			}
		}
		if len(running) == 0 {
			break
		}
		winner, err := workflow.Select(ctx, workflow.MapResults(pending)...)
		j := slices.Index(workflow.MapResults(pending), winner)
		if j < 0 {
			return nil, nil, err
		}
		output, err := pending[j].Value(ctx)
		results[running[j]] = r.finishTool(ctx, stepNumber, calls[running[j]], output, err)
		running = slices.Delete(running, j, j+1)
		pending = slices.Delete(pending, j, j+1)
	}
	var parts []ModelCallStreamPart
	for _, result := range results {
		if !result.invalid {
			parts = append(parts, result.streamPart())
		}
	}
	return results, parts, nil
}

// finishTool reports the end of a call's execution and returns its result.
func (r *run) finishTool(ctx context.Context, stepNumber int, call ToolCall, output any, err error) toolResult {
	r.callbacks.OnToolExecutionEnd(ctx, ToolExecutionEndEvent{StepNumber: stepNumber, ToolCall: call, Output: output, Error: err})
	if err != nil {
		// Report tool errors to the model so it can recover, instead of
		// failing the run.
		return errorResult(call, err.Error())
	}
	return toolResult{call: call, output: jsonOutput(output), raw: marshalRaw(output)}
}

// approvalRequest returns the approval request of a call, signed when the
// agent has a ToolApprovalSecret.
func (r *run) approvalRequest(call ToolCall) (ContentPart, error) {
	request := ContentPart{Type: PartToolApprovalRequest, ApprovalID: "approval-" + call.ToolCallID, ToolCallID: call.ToolCallID}
	if r.agent.options.ToolApprovalSecret == nil {
		return request, nil
	}
	input, err := decodeInput(call.Input)
	if err != nil {
		return request, err
	}
	request.Signature, err = ai.SignToolApproval(r.agent.options.ToolApprovalSecret, request.ApprovalID, call.ToolCallID, call.ToolName, input)
	return request, err
}

// resumeApprovals executes the approved calls and reports the denied calls
// of the approval responses in the last message, then removes the approval
// parts from the conversation.
func (r *run) resumeApprovals(ctx context.Context) error {
	collected, err := ai.CollectToolApprovals(toProviderMessages(r.messages))
	if err != nil {
		return err
	}
	approved, denied := collected.ApprovedToolApprovals, collected.DeniedToolApprovals
	if len(approved) == 0 && len(denied) == 0 {
		return nil
	}

	var results []toolResult
	var parts []ModelCallStreamPart
	var executable []ToolCall
	for _, approval := range approved {
		call := approvedCall(approval)
		if call.ProviderExecuted {
			continue
		}
		if reason := r.validateApproval(approval.ApprovalRequest, call); reason != "" {
			result := errorResult(call, reason)
			results = append(results, result)
			parts = append(parts, result.streamPart())
			continue
		}
		executable = append(executable, call)
	}
	executed, executedParts, err := r.executeTools(ctx, 0, executable, r.messages)
	if err != nil {
		return err
	}
	results = append(results, executed...)
	parts = append(parts, executedParts...)
	for _, denial := range denied {
		call := approvedCall(denial)
		if call.ProviderExecuted {
			continue
		}
		parts = append(parts, ModelCallStreamPart{Type: "tool-output-denied", ToolCallID: call.ToolCallID})
		if denial.ExistingToolResult != nil {
			continue
		}
		results = append(results, toolResult{call: call, output: &ToolResultOutput{Type: "execution-denied", Reason: denial.ApprovalResponse.Reason}})
	}

	// Strip the approval parts the agent resolved. The approval data stays
	// with the client's UI messages. Keep the approvals of provider-executed
	// calls, marked so model calls send them, since the provider resolves
	// them.
	providerApprovals := map[string]bool{}
	for _, approval := range slices.Concat(approved, denied) {
		if approval.ToolCall.ProviderExecuted {
			providerApprovals[approval.ApprovalResponse.ApprovalID] = true
		}
	}
	cleaned := make([]ModelMessage, 0, len(r.messages)+1)
	for _, message := range r.messages {
		message.Content = slices.Clone(message.Content)
		for i, part := range message.Content {
			if part.Type == PartToolApprovalRequest || part.Type == PartToolApprovalResponse {
				message.Content[i].ProviderExecuted = providerApprovals[part.ApprovalID]
			}
		}
		message.Content = slices.DeleteFunc(message.Content, func(part ContentPart) bool {
			return (part.Type == PartToolApprovalRequest || part.Type == PartToolApprovalResponse) && !part.ProviderExecuted
		})
		if len(message.Content) > 0 {
			cleaned = append(cleaned, message)
		}
	}
	if len(results) > 0 {
		cleaned = append(cleaned, toolMessage(results))
	}
	r.messages = cleaned

	parts = append(parts, ModelCallStreamPart{Type: "finish-step"}, ModelCallStreamPart{Type: "start-step"})
	return r.write(ctx, parts...)
}

// validateApproval returns why an approved call must not execute, or "".
func (r *run) validateApproval(request types.ToolApprovalRequestContent, call ToolCall) string {
	tool, known := r.tools[call.ToolName]
	if !known || tool.Execute == nil {
		return fmt.Sprintf("Tool %q not found", call.ToolName)
	}
	if tool.NeedsApproval == nil {
		return fmt.Sprintf("Tool %q does not require approval", call.ToolName)
	}
	if r.agent.options.ToolApprovalSecret == nil {
		return ""
	}
	if request.Signature == "" {
		return fmt.Sprintf("Invalid tool approval signature for %q: missing signature", request.ApprovalID)
	}
	input, err := decodeInput(call.Input)
	if err != nil {
		return err.Error()
	}
	valid, err := ai.VerifyToolApprovalSignature(r.agent.options.ToolApprovalSecret, request.Signature, request.ApprovalID, call.ToolCallID, call.ToolName, input)
	if err != nil || !valid {
		return fmt.Sprintf("Invalid tool approval signature for %q: invalid signature", request.ApprovalID)
	}
	return ""
}

// write writes parts to the Writer's stream in a step.
func (r *run) write(ctx context.Context, parts ...ModelCallStreamPart) error {
	if r.options.Writer == nil || len(parts) == 0 {
		return nil
	}
	_, err := workflow.Step(ctx, writePartsStep, writePartsInput{Writer: *r.options.Writer, Parts: parts})
	return err
}

// closeWriter writes parts and a finish part to the Writer's stream, then
// closes it, as SendFinish and PreventClose allow.
func (r *run) closeWriter(ctx context.Context, parts ...ModelCallStreamPart) error {
	if r.options.Writer == nil {
		return nil
	}
	if r.options.SendFinish == nil || *r.options.SendFinish {
		parts = append(parts, ModelCallStreamPart{Type: "finish"})
	}
	if r.options.PreventClose {
		return r.write(ctx, parts...)
	}
	_, err := workflow.Step(ctx, writePartsStep, writePartsInput{Writer: *r.options.Writer, Parts: parts, Close: true})
	return err
}

func (r *run) result(toolCalls []ToolCall, toolResults []ToolResult) Result {
	result := Result{
		Messages:     r.messages,
		Steps:        r.steps,
		ToolCalls:    toolCalls,
		ToolResults:  toolResults,
		FinishReason: "other",
	}
	if len(r.steps) > 0 {
		last := r.steps[len(r.steps)-1]
		result.Text = last.Text
		result.FinishReason = last.FinishReason
	}
	for _, step := range r.steps {
		result.TotalUsage = addUsage(result.TotalUsage, step.Usage)
	}
	return result
}

// writePartsStep writes parts to the stream input.Writer names, and closes
// the stream when input.Close is set. Streams can only be written in step
// context, so the agent runs it as a step.
func writePartsStep(ctx context.Context, input writePartsInput) (struct{}, error) {
	writer, release, err := workflow.Writer(ctx, input.Writer)
	if err != nil {
		return struct{}{}, err
	}
	defer release()
	for _, part := range input.Parts {
		if err := writePart(writer, part); err != nil {
			return struct{}{}, err
		}
	}
	if input.Close {
		return struct{}{}, writer.Close()
	}
	return struct{}{}, nil
}

// addApprovalRequests appends approval requests to the last assistant
// message.
func addApprovalRequests(messages []ModelMessage, requests []ContentPart) []ModelMessage {
	if len(requests) == 0 {
		return messages
	}
	for i := len(messages) - 1; i >= 0; i-- {
		if messages[i].Role == RoleAssistant {
			messages[i].Content = append(slices.Clone(messages[i].Content), requests...)
			return messages
		}
	}
	return append([]ModelMessage{{Role: RoleAssistant, Content: requests}}, messages...)
}

// approvedCall returns the tool call of an approval response.
func approvedCall(approval ai.CollectedToolApproval) ToolCall {
	return ToolCall{
		ToolCallID:       approval.ToolCall.ID,
		ToolName:         approval.ToolCall.ToolName,
		Input:            json.RawMessage(approval.ToolCall.RawArguments),
		ProviderExecuted: approval.ToolCall.ProviderExecuted,
		ThoughtSignature: approval.ToolCall.ThoughtSignature,
	}
}

func decodeInput(raw json.RawMessage) (any, error) {
	var input any
	if len(raw) == 0 {
		return map[string]any{}, nil
	}
	err := json.Unmarshal(raw, &input)
	return input, err
}

func errorResult(call ToolCall, message string) toolResult {
	return toolResult{call: call, output: &ToolResultOutput{Type: "error-text", Value: marshalRaw(message)}, raw: marshalRaw(message), isError: true}
}

func executedResults(results []toolResult) []ToolResult {
	var out []ToolResult
	for _, result := range results {
		if result.invalid {
			continue
		}
		out = append(out, ToolResult{ToolCallID: result.call.ToolCallID, ToolName: result.call.ToolName, Input: result.call.Input, Output: result.raw, IsError: result.isError})
	}
	return out
}

func toolMessage(results []toolResult) ModelMessage {
	message := ModelMessage{Role: RoleTool}
	for _, result := range results {
		message.Content = append(message.Content, ContentPart{Type: PartToolResult, ToolCallID: result.call.ToolCallID, ToolName: result.call.ToolName, Output: result.output})
	}
	return message
}

// orNil returns value, or fallback when value is nil.
func orNil[S ~[]E, E any](value, fallback S) S {
	if value == nil {
		return fallback
	}
	return value
}

// mergeSettings returns base with the fields set in override replaced.
func mergeSettings(base, override GenerationSettings) GenerationSettings {
	base.MaxOutputTokens = cmp.Or(override.MaxOutputTokens, base.MaxOutputTokens)
	base.Temperature = cmp.Or(override.Temperature, base.Temperature)
	base.TopP = cmp.Or(override.TopP, base.TopP)
	base.TopK = cmp.Or(override.TopK, base.TopK)
	base.PresencePenalty = cmp.Or(override.PresencePenalty, base.PresencePenalty)
	base.FrequencyPenalty = cmp.Or(override.FrequencyPenalty, base.FrequencyPenalty)
	base.StopSequences = orNil(override.StopSequences, base.StopSequences)
	base.Seed = cmp.Or(override.Seed, base.Seed)
	if override.Headers != nil {
		base.Headers = override.Headers
	}
	if override.ProviderOptions != nil {
		base.ProviderOptions = override.ProviderOptions
	}
	base.MaxRetries = cmp.Or(override.MaxRetries, base.MaxRetries)
	base.Reasoning = cmp.Or(override.Reasoning, base.Reasoning)
	base.SendReasoning = cmp.Or(override.SendReasoning, base.SendReasoning)
	return base
}

func addUsage(a, b types.Usage) types.Usage {
	add := func(x, y *int64) *int64 {
		if x == nil && y == nil {
			return nil
		}
		var sum int64
		if x != nil {
			sum += *x
		}
		if y != nil {
			sum += *y
		}
		return &sum
	}
	return types.Usage{
		InputTokens:  add(a.InputTokens, b.InputTokens),
		OutputTokens: add(a.OutputTokens, b.OutputTokens),
		TotalTokens:  add(a.TotalTokens, b.TotalTokens),
	}
}
