package workflowai

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/digitallysavvy/go-ai/pkg/ai"
	"github.com/digitallysavvy/go-ai/pkg/provider"
	"github.com/digitallysavvy/go-ai/pkg/provider/types"
	"github.com/digitallysavvy/go-ai/pkg/schema"
	"github.com/digitallysavvy/go-ai/pkg/telemetry"
	"github.com/eriicafes/go-workflow"
	"go.opentelemetry.io/otel/trace"
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
	// support, before each model call and inside its step. Nil uses go-ai's
	// DefaultDownload, as the Workflow SDK downloads them by default.
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
	// ToolApprovalSecret names the environment variable holding the secret
	// that signs tool approval requests with HMAC, so an approval response
	// only executes a call the agent asked approval for with the same input.
	// Use at least 32 random bytes, the same in every process that runs the
	// workflow. Changing it invalidates pending approvals.
	ToolApprovalSecret *ToolApprovalSecret
	// ToolApproval decides the approval of every tool call, in place of
	// ToolApprovals and the tools' NeedsApproval. ToolApprovals decides the
	// approval of the calls of the tools it names, in place of their
	// NeedsApproval. A call they approve runs without asking the user, and a
	// call they deny is reported to the model as denied.
	ToolApproval  ToolApprovalFunc
	ToolApprovals map[string]ToolApprovalFunc
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
	ToolApprovalSecret  *ToolApprovalSecret
	// ToolApproval and ToolApprovals replace the agent's when either is set.
	ToolApproval  ToolApprovalFunc
	ToolApprovals map[string]ToolApprovalFunc
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
	// GenerationSettings fields that are set replace the agent's, and the
	// ones PrepareCall returns.
	GenerationSettings
}

// PrepareCallFunc changes the settings of a Stream or Generate call. It runs
// in workflow context once, before approved calls resume and before the
// first model call. It can call
// workflow.Step or workflow.InlineStep to record side effects and external
// reads, so replays don't repeat them.
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
// workflow context before every step. It can call
// workflow.Step or workflow.InlineStep to record side effects and external
// reads, so replays don't repeat them.
type PrepareStepFunc func(ctx context.Context, info PrepareStepInfo) (PrepareStepResult, error)

// PrepareStepInfo describes the step about to run.
type PrepareStepInfo struct {
	StepNumber     int
	Steps          []StepResult
	Messages       []ModelMessage
	Model          provider.LanguageModel
	RuntimeContext any
	// Instructions and InstructionMessages are the instructions in effect,
	// and InitialInstructions and InitialMessages the call's instructions
	// and messages before any step changed them.
	Instructions        string
	InstructionMessages []ModelMessage
	InitialInstructions string
	InitialMessages     []ModelMessage
}

// PrepareStepResult holds the settings PrepareStep changes for one step.
// Zero fields keep the current settings.
type PrepareStepResult struct {
	Model provider.LanguageModel
	// Instructions or InstructionMessages, when either is set, replace the
	// call's instructions for this and later steps, as in the Workflow SDK.
	Instructions        string
	InstructionMessages []ModelMessage
	// Messages replaces the messages sent to the model in this step.
	Messages    []ModelMessage
	ToolChoice  *types.ToolChoice
	ActiveTools []string
	// RuntimeContext replaces the runtime context for this and later steps.
	RuntimeContext any
	// GenerationSettings fields that are set replace the call's for this
	// step.
	GenerationSettings
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
	// Model is the model that made the step's call.
	Model StepModel `json:"model"`
	// Files and Sources are the files the model generated and the sources
	// it cited, which Content also holds.
	Files   []GeneratedFile `json:"files,omitempty"`
	Sources []ContentPart   `json:"sources,omitempty"`
	// ProviderMetadata is the provider's metadata of the response, and
	// Warnings the warnings it reported.
	ProviderMetadata map[string]any  `json:"providerMetadata,omitempty"`
	Warnings         []types.Warning `json:"warnings,omitempty"`
	// Performance is how fast the model answered.
	Performance *StepPerformance `json:"performance,omitempty"`
	// RuntimeContext and ToolsContext are the contexts the step ran with.
	RuntimeContext any            `json:"runtimeContext,omitempty"`
	ToolsContext   map[string]any `json:"toolsContext,omitempty"`
	// Request holds what CallOptions.Include asks to keep of the request.
	Request  StepRequest `json:"request,omitzero"`
	Response Response    `json:"response"`
}

// StepModel identifies the model of a step.
type StepModel struct {
	Provider string `json:"provider"`
	ModelID  string `json:"modelId"`
}

// GeneratedFile is a file a model generated, with its base64 content.
type GeneratedFile struct {
	Base64    string `json:"base64"`
	MediaType string `json:"mediaType"`
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
		approvalSecret:      cmp.Or(options.ToolApprovalSecret, a.options.ToolApprovalSecret),
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
		pendingResults:      map[string]bool{},
	}
	if r.runtimeContext == nil {
		r.runtimeContext = a.options.RuntimeContext
	}
	r.toolApproval, r.toolApprovals = options.ToolApproval, options.ToolApprovals
	if r.toolApproval == nil && r.toolApprovals == nil {
		r.toolApproval, r.toolApprovals = a.options.ToolApproval, a.options.ToolApprovals
	}
	if r.toolsContext == nil {
		r.toolsContext = a.options.ToolsContext
	}
	if r.toolsContext == nil {
		r.toolsContext = map[string]any{}
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
		r.defaultLimit = true
		r.stopWhen = []StopCondition{IsStepCount(defaultStepLimit)}
	}
	if options.Prompt != "" {
		r.messages = []ModelMessage{{Role: RoleUser, Content: []ContentPart{{Type: PartText, Text: options.Prompt}}}}
	}
	ctx, err := r.prepare(ctx)
	if err != nil {
		// Like the Workflow SDK, a call that cannot start leaves its stream
		// untouched.
		return Result{}, err
	}

	r.initialInstructions, r.initialMessages = r.instructions, slices.Clone(r.messages)
	// Like the Workflow SDK, the call resumes the approvals in its messages,
	// then starts, then records its root span.
	r.startedAt = workflow.Now(ctx)
	result := r.result(nil, nil)
	err = r.resumeApprovals(ctx)
	if err == nil {
		r.callbacks.OnStart(ctx, StartEvent{Messages: r.messages, Model: r.model, RuntimeContext: r.runtimeContext, ToolsContext: r.toolsContext})
		ctx, err = r.startTrace(ctx)
	}
	if err == nil {
		result, err = r.loop(ctx)
	}
	if r.defaultLimitStopped && len(r.steps) > 0 {
		last := r.stepModel
		if _, logErr := workflow.Step(ctx, logDefaultStepLimitWarning, defaultStepLimitWarning{StepCount: defaultStepLimit, Provider: last.Provider(), ModelID: last.ModelID()}); logErr != nil {
			err = errors.Join(err, logErr)
		}
	}
	// Like the Workflow SDK, Stream parses its output before the call ends,
	// and a parse failure still ends the call before it fails.
	var parseErr error
	if r.streaming && err == nil && result.Error == "" {
		result.Output, parseErr = r.parseOutput(ctx)
	}
	if r.stopTimeout != nil {
		err = errors.Join(err, r.stopTimeout())
	}
	aborted := err != nil && (r.aborted || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded))
	switch {
	case aborted:
		// A cancelled context or the Timeout aborted the call. The steps
		// before it stay in the result, and the stream ends normally.
		r.callbacks.OnAbort(ctx, AbortEvent{Steps: r.steps})
		r.record(ctx, telemetryEvent{Type: eventAbort, Error: err.Error()})
	case err != nil:
		r.callbacks.OnError(ctx, err)
		r.record(ctx, telemetryEvent{Type: eventError, Error: err.Error()})
	default:
		if result.Error != "" {
			r.callbacks.OnError(ctx, errors.New(result.Error))
			r.record(ctx, telemetryEvent{Type: eventError, Error: result.Error})
		}
		if !r.ended {
			r.end(ctx, result)
		}
		// Like the Workflow SDK's generate result, Generate reports the tool
		// calls and results of every step.
		if !r.streaming {
			result.ToolCalls, result.ToolResults = nil, nil
			for _, step := range r.steps {
				result.ToolCalls = append(result.ToolCalls, step.ToolCalls...)
				result.ToolResults = append(result.ToolResults, step.ToolResults...)
			}
		}
		// Generate parses its output after the call ends, as the plain text
		// of the last step without an Output.
		if !r.streaming && result.Error == "" {
			result.Output, parseErr = r.generateOutput(ctx)
		}
		err = parseErr
	}
	// End the stream, so readers do not wait for parts that never come, then
	// report the call's telemetry, as the Workflow SDK does.
	closeErr := r.closeWriter(ctx)
	r.reportTelemetry(ctx)
	if r.aborted && r.streaming {
		// Like the Workflow SDK's stream, an aborted Stream resolves with the
		// steps that finished, while Generate fails with the abort's cause.
		return result, closeErr
	}
	return result, errors.Join(err, closeErr)
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
	approvalSecret *ToolApprovalSecret
	toolApproval   ToolApprovalFunc
	toolApprovals  map[string]ToolApprovalFunc
	// deadline is when the Timeout passes, or zero without one. A model call
	// is not dispatched after it.
	deadline time.Time
	// aborted is set when a cancelled context or the Timeout aborted the
	// call.
	aborted bool
	// stopTimeout disarms the Timeout once the call ends.
	stopTimeout func() error
	tools       ToolSet
	toolChoice  *types.ToolChoice
	activeTools []string
	concurrency int
	stopWhen    []StopCondition
	// defaultLimit is set when stopWhen is Generate's default step limit,
	// and defaultLimitStopped when the limit ended the loop.
	defaultLimit        bool
	defaultLimitStopped bool
	// stepModel is the model of the last model call.
	stepModel provider.LanguageModel
	// ended is set when OnEnd was called before the call's last writes.
	ended          bool
	settings       GenerationSettings
	runtimeContext any
	toolsContext   map[string]any
	prepareStep    PrepareStepFunc
	messages       []ModelMessage
	steps          []StepResult
	// initialInstructions and initialMessages are the call's instructions
	// and messages before any step changed them.
	initialInstructions string
	initialMessages     []ModelMessage
	// trace is what prepareWorkflowAgentTelemetry recorded, and events are
	// the telemetry events the call reports when it ends.
	trace     *agentTelemetry
	events    []telemetryEvent
	startedAt time.Time
	// pendingResults are the provider calls whose deferred results have not
	// arrived.
	pendingResults map[string]bool
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
		// Stop conditions PrepareCall replaces the default step limit with
		// are its own, so the default limit's warning is not logged for them.
		if prepared.StopWhen != nil && (len(prepared.StopWhen) == 0 || len(r.stopWhen) == 0 || &prepared.StopWhen[0] != &r.stopWhen[0]) {
			r.defaultLimit = false
		}
		r.stopWhen = orNil(prepared.StopWhen, r.stopWhen)
		r.runtimeContext = cmp.Or(prepared.RuntimeContext, r.runtimeContext)
		if prepared.ToolsContext != nil {
			r.toolsContext = prepared.ToolsContext
		}
		r.settings = mergeSettings(r.settings, prepared.GenerationSettings)
	}
	r.settings = mergeSettings(r.settings, r.options.GenerationSettings)
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
	r.modelCtx = ctx
	if r.timeout != 0 {
		// Like the Workflow SDK, a negative Timeout is a deadline that has
		// already passed.
		r.deadline = workflow.Now(ctx).Add(r.timeout)
	}
	if r.timeout > 0 {
		modelCtx, stop, err := workflow.WithTimeout(ctx, r.timeout)
		if err != nil {
			return ctx, err
		}
		r.modelCtx, r.stopTimeout = modelCtx, stop
	}
	return ctx, nil
}

// abortCause returns why the call is aborted: the cause of its context's
// cancellation, or of its Timeout's, or nil while it is not.
func (r *run) abortCause(ctx context.Context) error {
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	if r.modelCtx != nil && r.modelCtx.Err() != nil {
		return context.Cause(r.modelCtx)
	}
	return nil
}

// telemetryActive reports whether the call reports telemetry: when its
// Telemetry is enabled, or, without Telemetry, when go-ai has globally
// registered integrations, as the Workflow SDK reports to its global ones.
func (r *run) telemetryActive() bool {
	if r.telemetry != nil {
		return telemetry.Enabled(r.telemetry)
	}
	_, none := telemetry.GetTelemetryIntegration().(telemetry.NoopTelemetryIntegration)
	return !none
}

// startTrace runs the prepareWorkflowAgentTelemetry inline step when the call
// reports telemetry. When the step reserved the root span's IDs, it returns
// ctx with the root span's trace context, which every step the call starts
// after it gets.
func (r *run) startTrace(ctx context.Context) (context.Context, error) {
	if !r.telemetryActive() {
		return ctx, nil
	}
	prepared, err := workflow.InlineStep(ctx, prepareWorkflowAgentTelemetry, struct{}{})
	if err != nil {
		return ctx, err
	}
	r.trace = &prepared
	// The call started before it resumed the approvals in its messages, whose
	// tool events were recorded before this step.
	start := telemetryEvent{
		Type:           eventStart,
		Timestamp:      r.startedAt.UnixMilli(),
		ModelProvider:  r.model.Provider(),
		ModelID:        r.model.ModelID(),
		RuntimeContext: r.includedRuntimeContext(),
		ToolsContext:   r.includedToolsContext(),
		Settings:       &r.settings,
	}
	if r.recordsInputs() {
		start.Messages = r.messages
	}
	r.events = append([]telemetryEvent{start}, r.events...)
	if !prepared.Reserved {
		return ctx, nil
	}
	root := prepared.rootContext()
	r.modelCtx = trace.ContextWithRemoteSpanContext(r.modelCtx, root)
	return trace.ContextWithRemoteSpanContext(ctx, root), nil
}

// operation returns the telemetry operation of the call. go-ai names the
// root span of these operations invoke_agent, as the Workflow SDK names the
// root span of a WorkflowAgent call.
func (r *run) operation() string {
	if r.streaming {
		return "ai.streamText"
	}
	return "ai.generateText"
}

// record records a telemetry event of the call, which it reports when it
// ends.
func (r *run) record(ctx context.Context, event telemetryEvent) {
	if !r.telemetryActive() {
		return
	}
	event.Timestamp = workflow.Now(ctx).UnixMilli()
	r.events = append(r.events, event)
}

// recordsInputs reports whether the telemetry settings record inputs, which
// the call then records for its report. Globally registered integrations
// get them.
func (r *run) recordsInputs() bool { return r.telemetry == nil || r.telemetry.RecordInputs }

// recordsOutputs reports whether the telemetry settings record outputs.
func (r *run) recordsOutputs() bool { return r.telemetry == nil || r.telemetry.RecordOutputs }

// includedRuntimeContext returns the runtime context fields the telemetry
// settings include.
func (r *run) includedRuntimeContext() map[string]any {
	if r.telemetry == nil {
		return nil
	}
	return includedContext(r.runtimeContext, r.telemetry.IncludeRuntimeContext)
}

// includedToolsContext returns the tool context fields the telemetry
// settings include, by tool.
func (r *run) includedToolsContext() map[string]any {
	if r.telemetry == nil {
		return nil
	}
	var included map[string]any
	for name, context := range r.toolsContext {
		if fields := includedContext(context, r.telemetry.IncludeToolsContext[name]); fields != nil {
			if included == nil {
				included = map[string]any{}
			}
			included[name] = fields
		}
	}
	return included
}

// includedToolContext returns the context fields of tool name the telemetry
// settings include.
func (r *run) includedToolContext(name string) map[string]any {
	if r.telemetry == nil {
		return nil
	}
	return includedContext(r.toolsContext[name], r.telemetry.IncludeToolsContext[name])
}

// reportTelemetry runs the reportWorkflowAgentTelemetry step with the call's
// recorded events. Like in the Workflow SDK, telemetry never changes the
// outcome of the call, so its error is ignored.
func (r *run) reportTelemetry(ctx context.Context) {
	if r.trace == nil {
		return
	}
	input := reportTelemetryInput{Telemetry: *r.trace, Operation: r.operation(), Instructions: r.instructions, Events: r.events}
	_, _ = workflow.Step(withModelCall(ctx, modelCall{model: r.model, telemetry: r.telemetry}), reportWorkflowAgentTelemetry, input)
}

// endStep ends an agent step: it calls OnStepEnd unless notify is false, and
// records the step's end for telemetry.
func (r *run) endStep(ctx context.Context, step StepResult, notify bool) {
	if notify {
		r.callbacks.OnStepEnd(ctx, step)
	}
	r.record(ctx, telemetryEvent{
		Type:             eventStepEnd,
		StepNumber:       step.StepNumber,
		FinishReason:     step.FinishReason,
		Usage:            step.Usage,
		Text:             step.Text,
		ResponseID:       step.Response.ID,
		ProviderMetadata: step.ProviderMetadata,
		Files:            fileParts(step.Content),
		RuntimeContext:   r.includedRuntimeContext(),
		ToolsContext:     r.includedToolsContext(),
	})
}

// end ends the call.
func (r *run) end(ctx context.Context, result Result) {
	r.callbacks.OnEnd(ctx, EndEvent{Result: result, RuntimeContext: r.runtimeContext, ToolsContext: r.toolsContext})
	event := telemetryEvent{
		Type:           eventEnd,
		FinishReason:   result.FinishReason,
		Usage:          result.TotalUsage,
		Text:           result.Text,
		ModelProvider:  r.model.Provider(),
		ModelID:        r.model.ModelID(),
		RuntimeContext: r.includedRuntimeContext(),
		ToolsContext:   r.includedToolsContext(),
	}
	for _, step := range r.steps {
		event.Files = append(event.Files, fileParts(step.Content)...)
	}
	if len(r.steps) > 0 {
		last := r.steps[len(r.steps)-1]
		event.ProviderMetadata = last.ProviderMetadata
		if r.recordsOutputs() {
			event.ToolCalls = last.ToolCalls
		}
	}
	r.record(ctx, event)
}

// parseOutput parses the last step's text with the call's Output. It parses
// nothing without an Output, or when the last step called tools.
func (r *run) parseOutput(ctx context.Context) (any, error) {
	if r.output == nil || len(r.steps) == 0 {
		return nil, nil
	}
	last := r.steps[len(r.steps)-1]
	if last.Text == "" {
		return nil, nil
	}
	return r.parse(ctx, r.output, last)
}

// generateOutput parses Generate's output from the last step when it
// stopped, or when it has text and did not stop to call tools, as the
// Workflow SDK's generate does. Without an Output, the output is the step's
// text.
func (r *run) generateOutput(ctx context.Context) (any, error) {
	if len(r.steps) == 0 {
		return nil, errors.New("No output generated.") //nolint:staticcheck // the Workflow SDK's error text
	}
	last := r.steps[len(r.steps)-1]
	if last.FinishReason != string(types.FinishReasonStop) && (last.FinishReason == string(types.FinishReasonToolCalls) || last.Text == "") {
		return nil, nil
	}
	if r.output == nil {
		return last.Text, nil
	}
	return r.parse(ctx, r.output, last)
}

// parse parses a step's text with output.
func (r *run) parse(ctx context.Context, output Output, last StepResult) (any, error) {
	usage := last.Usage
	return output.ParseOutput(ctx, ai.ParseCompleteOutputOptions{
		Text:         last.Text,
		Response:     &types.ResponseMetadata{ID: last.Response.ID, ModelID: last.Response.ModelID},
		Usage:        &usage,
		FinishReason: types.FinishReason(last.FinishReason),
	})
}

func (r *run) loop(ctx context.Context) (Result, error) {
	for {
		// Like the Workflow SDK, a call aborted before a step ends without
		// starting it. A passed Timeout does not: the model call step runs
		// and aborts without calling the model.
		if ctx.Err() != nil {
			r.aborted = true
			return r.result(nil, nil), context.Cause(ctx)
		}
		stepNumber := len(r.steps)
		model, messages := r.model, r.messages
		toolChoice, activeTools, settings := r.toolChoice, r.activeTools, r.settings
		if r.prepareStep != nil {
			prepared, err := r.prepareStep(ctx, PrepareStepInfo{
				StepNumber:          stepNumber,
				Steps:               r.steps,
				Messages:            r.messages,
				Model:               r.model,
				RuntimeContext:      r.runtimeContext,
				Instructions:        r.instructions,
				InstructionMessages: r.instructionMessages,
				InitialInstructions: r.initialInstructions,
				InitialMessages:     r.initialMessages,
			})
			if err != nil {
				return r.result(nil, nil), err
			}
			if prepared.Model != nil {
				model = prepared.Model
			}
			if prepared.Instructions != "" || prepared.InstructionMessages != nil {
				r.instructions, r.instructionMessages = prepared.Instructions, prepared.InstructionMessages
			}
			toolChoice = cmp.Or(prepared.ToolChoice, toolChoice)
			activeTools = orNil(prepared.ActiveTools, activeTools)
			if prepared.Messages != nil {
				messages = prepared.Messages
			}
			if prepared.RuntimeContext != nil {
				r.runtimeContext = prepared.RuntimeContext
			}
			settings = mergeSettings(settings, prepared.GenerationSettings)
		}
		r.callbacks.OnStepStart(ctx, StepStartEvent{StepNumber: stepNumber, Messages: messages, Model: model, Steps: slices.Clip(r.steps), RuntimeContext: r.runtimeContext, ToolsContext: r.toolsContext})
		r.record(ctx, telemetryEvent{Type: eventStepStart, StepNumber: stepNumber, ModelProvider: model.Provider(), ModelID: model.ModelID(), RuntimeContext: r.includedRuntimeContext(), ToolsContext: r.includedToolsContext()})

		input := modelCallInput{
			Instructions:     r.instructions,
			Messages:         slices.Concat(r.instructionMessages, messages),
			Tools:            r.toolDefinitions(activeTools),
			ToolChoice:       toolChoice,
			Settings:         settings,
			ResponseFormat:   r.responseFormat,
			IncludeRawChunks: r.options.IncludeRawChunks,
			StepNumber:       stepNumber,
			Streaming:        r.streaming,
			Include:          r.options.Include,
			Writer:           r.options.Writer,
		}
		if !r.deadline.IsZero() {
			input.Deadline = r.deadline.UnixMilli()
		}
		call := doGenerateStep
		if r.streaming {
			call = doStreamStep
		}
		base := r.modelCtx
		if base.Err() != nil {
			base = ctx
		}
		callCtx := withModelCall(base, modelCall{model: model, telemetry: r.telemetry, repair: r.repair, download: r.download, transforms: r.options.Transform, providerTools: r.providerTools(activeTools)})
		// The model call retries opening the model itself, so the step is not
		// retried: a retry would repeat a model call whose stream failed
		// partway.
		callStart := telemetryEvent{Type: eventModelCallStart, StepNumber: stepNumber, ModelProvider: model.Provider(), ModelID: model.ModelID(), Tools: input.Tools, Settings: &input.Settings}
		if r.recordsInputs() {
			callStart.Messages = input.Messages
		}
		r.record(ctx, callStart)
		response, err := workflow.Step(callCtx, call, input, workflow.StepOptions{MaxRetries: 0})
		if err != nil {
			if cause := r.abortCause(ctx); cause != nil {
				r.aborted = true
				return r.result(nil, nil), cause
			}
			// Like the Workflow SDK, the call fails with the model's error,
			// not the step failure that wraps it.
			return r.result(nil, nil), rootCause(err)
		}
		if response.Aborted {
			r.aborted = true
			return r.result(nil, nil), context.DeadlineExceeded
		}
		callEnd := telemetryEvent{Type: eventModelCallEnd, StepNumber: stepNumber, ModelProvider: model.Provider(), ModelID: model.ModelID(), FinishReason: response.FinishReason, Usage: response.Usage, ResponseID: response.Response.ID, ProviderMetadata: response.ProviderMetadata}
		if r.recordsOutputs() {
			callEnd.Content = response.Content
		}
		r.record(ctx, callEnd)
		r.stepModel = model
		if r.streaming && r.options.Writer != nil {
			_, err := workflow.Step(ctx, writeDeferredToolCalls, writeDeferredToolCallsInput{Writer: *r.options.Writer, ToolCalls: []ToolCall{}})
			if err != nil {
				return r.result(nil, nil), err
			}
		}

		step := StepResult{
			StepNumber:       stepNumber,
			Content:          response.Content,
			ToolCalls:        response.ToolCalls,
			FinishReason:     response.FinishReason,
			Usage:            response.Usage,
			Model:            StepModel{Provider: model.Provider(), ModelID: model.ModelID()},
			ProviderMetadata: response.ProviderMetadata,
			Warnings:         response.Warnings,
			Performance:      response.Performance,
			RuntimeContext:   r.runtimeContext,
			ToolsContext:     r.toolsContext,
			Response:         response.Response,
		}
		for _, part := range response.Content {
			switch part.Type {
			case PartFile:
				step.Files = append(step.Files, GeneratedFile{Base64: part.Data, MediaType: part.MediaType})
			case PartSource:
				step.Sources = append(step.Sources, part)
			}
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
		// Like the Workflow SDK, tools and the approval policy get the
		// messages the step sent, without its system messages.
		history := slices.DeleteFunc(slices.Clone(messages), func(message ModelMessage) bool { return message.Role == RoleSystem })
		var added []ModelMessage
		if content := assistantContent(response.Content); len(content) > 0 {
			added = append(added, ModelMessage{Role: RoleAssistant, Content: content})
		}

		// An error in the model stream ends the turn without executing the
		// step's tool calls.
		if response.Error != "" {
			step.Response.Messages = added
			r.messages = append(r.messages, added...)
			r.steps = append(r.steps, step)
			r.endStep(ctx, step, true)
			result := r.result(step.ToolCalls, nil)
			result.Error = response.Error
			return result, nil
		}

		// Tools are processed when the model finished to call them, or when the
		// provider reported results of the tools it executes.
		toolExecutionAllowed := step.FinishReason == string(types.FinishReasonToolCalls) || step.FinishReason == string(types.FinishReasonStop)
		providerResults := providerToolResults(response.Content)
		// Like local results, the model sees a provider's results as the
		// tool's ToModelOutput converts them.
		if err := r.providerModelOutputs(ctx, step.Content, step.ToolCalls); err != nil {
			return r.result(nil, nil), err
		}
		if len(added) > 0 {
			if err := r.providerModelOutputs(ctx, added[0].Content, step.ToolCalls); err != nil {
				return r.result(nil, nil), err
			}
		}
		// A provider tool with deferred results may deliver a call's result
		// in a later model call, which the loop waits for.
		for _, call := range step.ToolCalls {
			if _, ok := providerResults[call.ToolCallID]; !ok && call.ProviderExecuted && r.defersResults(call.ToolName) {
				r.pendingResults[call.ToolCallID] = true
			}
		}
		for id := range providerResults {
			delete(r.pendingResults, id)
		}
		processTools := (toolExecutionAllowed && (len(step.ToolCalls) > 0 || len(providerResults) > 0)) ||
			(step.FinishReason == string(types.FinishReasonLength) && len(providerResults) > 0)
		if !processTools {
			step.Response.Messages = added
			r.messages = append(r.messages, added...)
			r.steps = append(r.steps, step)
			r.endStep(ctx, step, true)
			return r.result(step.ToolCalls, nil), nil
		}
		// When the finish reason does not allow tool execution, only invalid
		// calls get their error results and provider calls their results.
		calls := step.ToolCalls
		if !toolExecutionAllowed {
			calls = slices.DeleteFunc(slices.Clone(calls), func(call ToolCall) bool {
				_, hasResult := providerResults[call.ToolCallID]
				return !(call.Invalid && !call.ProviderExecuted) && !(call.ProviderExecuted && hasResult)
			})
		}

		// A provider approval request also pauses the loop, since the
		// provider waits for the user's response.
		providerApprovals := map[string]bool{}
		for _, part := range response.Content {
			if part.Type == PartToolApprovalRequest && part.ProviderExecuted {
				providerApprovals[part.ToolCallID] = true
			}
		}
		// Like in the Workflow SDK, a provider call without a result, whose
		// tool does not defer results, gets an empty one.
		for _, call := range calls {
			_, reported := providerResults[call.ToolCallID]
			if !call.ProviderExecuted || call.Invalid || reported || providerApprovals[call.ToolCallID] || r.defersResults(call.ToolName) {
				continue
			}
			log.Printf("[WorkflowAgent] Provider-executed tool %q (%s) did not receive a result from the stream. This may indicate a provider issue.", call.ToolName, call.ToolCallID)
			part := ContentPart{Type: PartToolResult, ToolCallID: call.ToolCallID, ToolName: call.ToolName, Output: &ToolResultOutput{Type: "text", Value: marshalRaw("")}, ProviderExecuted: true}
			step.Content = append(slices.Clone(step.Content), part)
			added[0].Content = append(slices.Clone(added[0].Content), part)
			providerResults[call.ToolCallID] = providerResult{ToolResult: ToolResult{ToolCallID: call.ToolCallID, ToolName: call.ToolName, Output: marshalRaw("")}, order: len(response.Content)}
		}

		// Split the local calls into those to execute now and those that pause
		// the loop: client tools and calls that need approval.
		var executable, paused, approvalCalls []ToolCall
		var denied []toolResult
		var automaticRequests, automaticResponses []ContentPart
		hasLocalCalls := false
		for _, call := range calls {
			if call.ProviderExecuted {
				continue
			}
			hasLocalCalls = true
			tool, known := r.tools[call.ToolName]
			if call.Invalid || !known {
				executable = append(executable, call)
				continue
			}
			status, err := r.resolveApproval(ctx, call, history)
			if err != nil {
				return r.result(nil, nil), err
			}
			if status.Type == ToolApprovalApproved || status.Type == ToolApprovalDenied {
				request, response := automaticApproval(call, status)
				automaticRequests = append(automaticRequests, request)
				automaticResponses = append(automaticResponses, response)
			}
			switch {
			case status.Type == ToolApprovalDenied:
				denied = append(denied, toolResult{call: call, output: &ToolResultOutput{Type: "execution-denied", Reason: status.Reason}})
			case status.Type == ToolApprovalUserApproval:
				approvalCalls = append(approvalCalls, call)
				paused = append(paused, call)
			case tool.Execute == nil:
				paused = append(paused, call)
			default:
				executable = append(executable, call)
			}
		}

		results, err := r.executeTools(ctx, stepNumber, executable, history, nil)
		if err != nil {
			return r.result(nil, nil), err
		}

		var deniedResults []deniedResult
		for _, result := range denied {
			deniedResults = append(deniedResults, deniedResult{ToolCallID: result.call.ToolCallID})
		}
		if len(paused) > 0 || len(providerApprovals) > 0 {
			if resolved := slices.Concat(results, denied); len(resolved) > 0 {
				added = append(added, toolMessage(resolved))
			}
			// Calls that wait for the provider's approval have no result yet.
			provider := r.providerResults(calls, providerResults, providerApprovals)
			executed := slices.Concat(executedResults(results), provider)
			step.ToolResults = successful(executed)
			step.Content = stepContent(step.Content, calls, results, providerResults)
			approvalRequests, err := r.approvalRequests(ctx, approvalCalls)
			if err != nil {
				return r.result(nil, nil), err
			}
			// Like the Workflow SDK, the step's content gets the approval
			// requests in the order of their calls, then the responses of the
			// approval policy's decisions, and the messages get the requests.
			ordered := slices.Concat(approvalRequests, automaticRequests)
			slices.SortStableFunc(ordered, func(a, b ContentPart) int {
				return cmp.Compare(callIndex(calls, a.ToolCallID), callIndex(calls, b.ToolCallID))
			})
			step.Content = slices.Concat(step.Content, ordered, automaticResponses)
			added = addApprovalRequests(added, slices.Concat(approvalRequests, automaticRequests))
			step.Response.Messages = added
			r.messages = append(r.messages, added...)
			r.steps = append(r.steps, step)
			r.endStep(ctx, step, !r.streaming)
			// Like the Workflow SDK, a paused turn ends before it writes its
			// results and approval requests.
			result := r.result(step.ToolCalls, executed)
			r.end(ctx, result)
			r.ended = true
			if r.options.Writer != nil {
				if len(executed) > 0 {
					if _, err := workflow.Step(ctx, writeToolResults, writeToolResultsInput{Writer: *r.options.Writer, Results: executed}); err != nil {
						return r.result(nil, nil), err
					}
				}
				if len(deniedResults) > 0 {
					if _, err := workflow.Step(ctx, writeToolOutputDenied, writeToolOutputDeniedInput{Writer: *r.options.Writer, DeniedResults: deniedResults}); err != nil {
						return r.result(nil, nil), err
					}
				}
				if len(approvalRequests) > 0 {
					requests := make([]approvalRequest, len(approvalRequests))
					for i, request := range approvalRequests {
						requests[i] = approvalRequest{ApprovalID: request.ApprovalID, ToolCallID: request.ToolCallID, Signature: request.Signature}
					}
					if _, err := workflow.Step(ctx, writeApprovalRequests, writeApprovalRequestsInput{Writer: *r.options.Writer, ApprovalRequests: requests}); err != nil {
						return r.result(nil, nil), err
					}
				}
			}
			return result, nil
		}

		step.Content = slices.Concat(step.Content, automaticRequests, automaticResponses)
		// The results keep the order of the calls, and provider results for
		// calls of earlier steps come last.
		provider := r.providerResults(calls, providerResults, nil)
		executed := orderedResults(calls, executedResults(results), provider)
		step.ToolResults = successful(executed)
		step.Content = stepContent(step.Content, calls, results, providerResults)
		if resolved := inCallOrder(calls, slices.Concat(results, denied)); len(resolved) > 0 {
			added = append(added, toolMessage(resolved))
		}
		step.Response.Messages = added
		r.messages = append(r.messages, added...)
		r.steps = append(r.steps, step)
		if r.options.Writer != nil {
			if len(deniedResults) > 0 {
				if _, err := workflow.Step(ctx, writeToolOutputDenied, writeToolOutputDeniedInput{Writer: *r.options.Writer, DeniedResults: deniedResults}); err != nil {
					return r.result(nil, nil), err
				}
			}
			_, err := workflow.Step(ctx, writeToolResults, writeToolResultsInput{Writer: *r.options.Writer, Results: executed, WriteStepBoundary: true})
			if err != nil {
				return r.result(nil, nil), err
			}
		}
		r.endStep(ctx, step, true)
		// The loop continues after local calls, or to wait for deferred
		// provider results, and stop conditions are only checked when it
		// could continue.
		if !toolExecutionAllowed || (!hasLocalCalls && len(r.pendingResults) == 0) {
			return r.result(step.ToolCalls, executed), nil
		}
		if slices.ContainsFunc(r.stopWhen, func(stop StopCondition) bool { return stop(r.steps) }) {
			r.defaultLimitStopped = r.defaultLimit
			return r.result(step.ToolCalls, executed), nil
		}
	}
}

// providerResults returns the results of the provider-executed calls among
// calls, followed by the results the provider reported for calls of earlier
// steps, leaving out calls in skip. A call whose result the provider has not
// reported yet, which it may deliver in a later step, has none.
func (r *run) providerResults(calls []ToolCall, reported map[string]providerResult, skip map[string]bool) []ToolResult {
	var out []ToolResult
	seen := map[string]bool{}
	for _, call := range calls {
		if !call.ProviderExecuted || call.Invalid || skip[call.ToolCallID] {
			continue
		}
		seen[call.ToolCallID] = true
		found, ok := reported[call.ToolCallID]
		if !ok {
			continue
		}
		result := found.ToolResult
		result.Input = call.Input
		out = append(out, result)
	}
	ids := make([]string, 0, len(reported))
	for id := range reported {
		if !seen[id] && !skip[id] {
			ids = append(ids, id)
		}
	}
	slices.SortFunc(ids, func(a, b string) int { return cmp.Compare(reported[a].order, reported[b].order) })
	for _, id := range ids {
		out = append(out, reported[id].ToolResult)
	}
	return out
}

// toolDefinitions returns the definitions of the active tools other than
// provider tools, in name order.
func (r *run) toolDefinitions(activeTools []string) []toolDefinition {
	names := make([]string, 0, len(r.tools))
	for name, tool := range r.tools {
		if tool.Provider == nil && (activeTools == nil || slices.Contains(activeTools, name)) {
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

// defersResults reports whether the tool named name is a provider tool that
// may deliver results in a later model call.
func (r *run) defersResults(name string) bool {
	provider := r.tools[name].Provider
	return provider != nil && provider.ProviderExecuted && provider.SupportsDeferredResults
}

// providerTools returns the active provider tools, in name order.
func (r *run) providerTools(activeTools []string) []types.Tool {
	var names []string
	for name, tool := range r.tools {
		if tool.Provider != nil && (activeTools == nil || slices.Contains(activeTools, name)) {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	tools := make([]types.Tool, len(names))
	for i, name := range names {
		tools[i] = *r.tools[name].Provider
	}
	return tools
}

func (r *run) toolCallOptions(call ToolCall, messages []ModelMessage) ToolCallOptions {
	return ToolCallOptions{ToolCallID: call.ToolCallID, Messages: messages, Context: r.toolsContext[call.ToolName]}
}

// executeTools executes calls, at most r.concurrency at once when it is set,
// and returns their results in call order. A call with an entry in checks is
// an approved call from client-supplied messages, which runs only after its
// approval passes checkApproval. Calls start in order, and a call starts when an earlier one settles
// in recorded order, so replay starts them the same way.
func (r *run) executeTools(ctx context.Context, stepNumber int, calls []ToolCall, messages []ModelMessage, checks map[string]ai.CollectedToolApproval) ([]toolResult, error) {
	results := make([]toolResult, len(calls))
	startedAt := map[string]time.Time{}
	var running []int
	var pending []*workflow.Result[any]
	for next := 0; next < len(calls) || len(running) > 0; {
		// Like the Workflow SDK, no queued call starts once the call is
		// aborted, and the abort fails it when the running calls settle.
		aborted := r.abortCause(ctx)
		if aborted != nil && len(running) == 0 {
			return nil, aborted
		}
		for aborted == nil && next < len(calls) && (r.concurrency == 0 || len(running) < r.concurrency) {
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
				if schema := tool.ContextSchema; schema != nil {
					if err := validateContext(schema, r.toolsContext[call.ToolName]); err != nil {
						return nil, fmt.Errorf("workflowai: context of tool %q: %w", call.ToolName, err)
					}
				}
				start := func() *workflow.Result[any] {
					startedAt[call.ToolCallID] = workflow.Now(ctx)
					r.callbacks.OnToolExecutionStart(ctx, ToolExecutionStartEvent{StepNumber: stepNumber, ToolCall: call, Messages: messages, ToolContext: r.toolsContext[call.ToolName]})
					r.record(ctx, telemetryEvent{Type: eventToolStart, StepNumber: stepNumber, ToolCallID: call.ToolCallID, ToolName: call.ToolName, Input: call.Input, ToolContext: r.includedToolContext(call.ToolName)})
					return tool.Execute.Execute(ctx, call.Input, r.toolCallOptions(call, messages))
				}
				var result *workflow.Result[any]
				if approval, ok := checks[call.ToolCallID]; ok {
					result = r.checkApproval(ctx, call, approval, start)
				} else {
					result = start()
				}
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
			return nil, err
		}
		output, err := pending[j].Value(ctx)
		var rejection *approvalRejection
		if errors.As(err, &rejection) {
			results[running[j]] = errorResult(calls[running[j]], rejection.reason)
			results[running[j]].rejected = true
		} else {
			results[running[j]] = r.finishTool(ctx, stepNumber, calls[running[j]], messages, startedAt[calls[running[j]].ToolCallID], output, err)
		}
		running = slices.Delete(running, j, j+1)
		pending = slices.Delete(pending, j, j+1)
	}
	return results, nil
}

// finishTool reports the end of a call's execution and returns its result.
func (r *run) finishTool(ctx context.Context, stepNumber int, call ToolCall, messages []ModelMessage, startedAt time.Time, output any, err error) toolResult {
	r.callbacks.OnToolExecutionEnd(ctx, ToolExecutionEndEvent{
		StepNumber:  stepNumber,
		ToolCall:    call,
		Output:      output,
		Error:       err,
		Success:     err == nil,
		DurationMs:  workflow.Now(ctx).Sub(startedAt).Milliseconds(),
		Messages:    messages,
		ToolContext: r.toolsContext[call.ToolName],
	})
	toolEnd := telemetryEvent{Type: eventToolEnd, StepNumber: stepNumber, ToolCallID: call.ToolCallID, ToolName: call.ToolName, Input: call.Input, Output: marshalRaw(output), ToolContext: r.includedToolContext(call.ToolName)}
	if err != nil {
		toolEnd.Error = err.Error()
	}
	r.record(ctx, toolEnd)
	if err != nil {
		// Report tool errors to the model so it can recover, instead of
		// failing the run.
		return errorResult(call, err.Error())
	}
	result := toolResult{call: call, output: jsonOutput(output), raw: marshalRaw(output)}
	if toModelOutput := r.tools[call.ToolName].ToModelOutput; toModelOutput != nil {
		converted, err := toModelOutput(ctx, ToolModelOutputOptions{ToolCallID: call.ToolCallID, Input: call.Input, Output: output})
		if err != nil {
			return errorResult(call, err.Error())
		}
		result.output = &converted
	}
	return result
}

// approvalRequests returns the approval requests of calls. With a
// ToolApprovalSecret, each is signed in a signWorkflowToolApproval step, and
// the steps run concurrently.
func (r *run) approvalRequests(ctx context.Context, calls []ToolCall) ([]ContentPart, error) {
	requests := make([]ContentPart, len(calls))
	signatures := make([]*workflow.Result[string], len(calls))
	for i, call := range calls {
		requests[i] = ContentPart{Type: PartToolApprovalRequest, ApprovalID: "approval-" + call.ToolCallID, ToolCallID: call.ToolCallID}
		if r.approvalSecret != nil {
			signatures[i] = workflow.Background(ctx, signWorkflowToolApproval, toolApprovalSignatureInput{
				Secret:     *r.approvalSecret,
				ApprovalID: requests[i].ApprovalID,
				ToolCallID: call.ToolCallID,
				ToolName:   call.ToolName,
				Input:      call.Input,
			})
		}
	}
	for i, signature := range signatures {
		if signature == nil {
			continue
		}
		value, err := signature.Value(ctx)
		if err != nil {
			return nil, err
		}
		requests[i].Signature = value
	}
	return requests, nil
}

// resumeApprovals executes the approved calls and reports the denied calls
// of the approval responses in the last message, then removes the approval
// parts from the conversation.
func (r *run) resumeApprovals(ctx context.Context) error {
	// Like the Workflow SDK, only the first response to an approval counts.
	r.messages = firstApprovalResponses(r.messages)
	collected, err := ai.CollectToolApprovals(toProviderMessages(r.messages))
	if err != nil {
		return err
	}
	approved, denied := collected.ApprovedToolApprovals, collected.DeniedToolApprovals
	// Approved calls run in the order the model made them, as in the
	// Workflow SDK, whatever the order of their responses.
	order := map[string]int{}
	for _, message := range r.messages {
		for _, part := range message.Content {
			if part.Type == PartToolCall {
				if _, seen := order[part.ToolCallID]; !seen {
					order[part.ToolCallID] = len(order)
				}
			}
		}
	}
	slices.SortStableFunc(approved, func(a, b ai.CollectedToolApproval) int {
		return cmp.Compare(order[a.ToolCall.ID], order[b.ToolCall.ID])
	})
	if len(approved) == 0 && len(denied) == 0 {
		return nil
	}

	// Approved calls to a tool that is gone or runs on the client get no
	// result, as in the Workflow SDK. The others are checked as they start,
	// and a call that fails its check gets an error result the model sees and
	// the stream does not.
	var executable []ToolCall
	var missing []string
	checks := map[string]ai.CollectedToolApproval{}
	for _, approval := range approved {
		call := approvedCall(approval)
		if call.ProviderExecuted {
			continue
		}
		if tool, known := r.tools[call.ToolName]; !known || tool.Execute == nil {
			missing = append(missing, call.ToolCallID)
			continue
		}
		executable = append(executable, call)
		checks[call.ToolCallID] = approval
	}
	if len(missing) > 0 {
		// The call gets no result, so the conversation cannot continue, as
		// the Workflow SDK reports when it converts it for the model.
		return missingToolResults(missing)
	}
	results, err := r.executeTools(ctx, 0, executable, r.messages, checks)
	if err != nil {
		return err
	}
	var approvedResults []ToolResult
	for _, result := range results {
		if !result.rejected {
			approvedResults = append(approvedResults, ToolResult{ToolCallID: result.call.ToolCallID, ToolName: result.call.ToolName, Input: result.call.Input, Output: result.raw})
		}
	}
	var deniedResults []deniedResult
	for _, denial := range denied {
		call := approvedCall(denial)
		if call.ProviderExecuted {
			continue
		}
		deniedResults = append(deniedResults, deniedResult{ToolCallID: call.ToolCallID})
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

	if r.options.Writer == nil || (len(results) == 0 && len(deniedResults) == 0) {
		return nil
	}
	_, err = workflow.Step(ctx, writeApprovalToolResults, writeApprovalToolResultsInput{Writer: *r.options.Writer, ApprovedResults: approvedResults, DeniedResults: deniedResults})
	return err
}

// checkApproval checks an approved call from client-supplied messages, as
// the Workflow SDK does before it executes one: the tool must still need
// approval, the signature must be valid when the agent has a
// ToolApprovalSecret, which a verifyWorkflowToolApprovalSignature step
// checks, and the input must still match the tool's schema. It returns the
// result of start when the call passes, and an approvalRejection otherwise.
func (r *run) checkApproval(ctx context.Context, call ToolCall, approval ai.CollectedToolApproval, start func() *workflow.Result[any]) *workflow.Result[any] {
	status, err := r.resolveApproval(ctx, call, r.messages)
	if err != nil {
		return rejectApproval(err.Error())
	}
	if r.tools[call.ToolName].NeedsApproval == nil && status.Type == ToolApprovalNotApplicable {
		return rejectApproval(fmt.Sprintf("Tool %q does not require approval", call.ToolName))
	}
	execute := func() *workflow.Result[any] {
		if reason := r.revalidateApproval(ctx, call, approval); reason != "" {
			return rejectApproval(reason)
		}
		return start()
	}
	if r.approvalSecret == nil {
		return execute()
	}
	request := approval.ApprovalRequest
	if request.Signature == "" {
		return rejectApproval((&ai.InvalidToolApprovalSignatureError{ApprovalID: request.ApprovalID, ToolCallID: call.ToolCallID, Reason: "missing signature"}).Error())
	}
	verified := workflow.Background(ctx, verifyWorkflowToolApprovalSignature, toolApprovalSignatureInput{
		Secret:     *r.approvalSecret,
		Signature:  request.Signature,
		ApprovalID: request.ApprovalID,
		ToolCallID: call.ToolCallID,
		ToolName:   call.ToolName,
		Input:      call.Input,
	})
	return workflow.Then(ctx, verified, func(valid bool, err error) *workflow.Result[any] {
		if err != nil {
			return rejectApproval(err.Error())
		}
		if !valid {
			return rejectApproval((&ai.InvalidToolApprovalSignatureError{ApprovalID: request.ApprovalID, ToolCallID: call.ToolCallID, Reason: "invalid signature"}).Error())
		}
		return execute()
	})
}

// revalidateApproval returns why an approved call's input no longer matches
// its tool's schema, or "".
func (r *run) revalidateApproval(ctx context.Context, call ToolCall, approval ai.CollectedToolApproval) string {
	tool := types.Tool{
		Name:       call.ToolName,
		Parameters: r.tools[call.ToolName].inputSchema(),
		Execute:    func(context.Context, map[string]any, types.ToolExecutionOptions) (any, error) { return nil, nil },
	}
	validated, err := ai.ValidateApprovedToolApprovals(ctx, ai.ValidateApprovedToolApprovalsOptions{
		ApprovedToolApprovals: []ai.CollectedToolApproval{approval},
		Tools:                 []types.Tool{tool},
		Messages:              toProviderMessages(r.messages),
		ToolsContext:          r.toolsContext,
		RuntimeContext:        r.runtimeContext,
	})
	switch {
	case err != nil:
		return err.Error()
	case len(validated.InvalidToolApprovals) > 0:
		return validated.InvalidToolApprovals[0].Error.Error()
	case len(validated.DeniedToolApprovals) > 0:
		return cmp.Or(validated.DeniedToolApprovals[0].ApprovalResponse.Reason, "Tool approval denied")
	}
	// The approval policy may deny a call the user approved.
	status, err := r.resolveApproval(ctx, call, r.messages)
	if err != nil {
		return err.Error()
	}
	if status.Type == ToolApprovalDenied {
		return cmp.Or(status.Reason, "Tool approval denied")
	}
	return ""
}

// closeWriter ends the Writer's stream with the closeStream step, as
// SendFinish and PreventClose allow.
func (r *run) closeWriter(ctx context.Context) error {
	if r.options.Writer == nil {
		return nil
	}
	sendFinish := r.options.SendFinish == nil || *r.options.SendFinish
	if !sendFinish && r.options.PreventClose {
		return nil
	}
	_, err := workflow.Step(ctx, closeStream, closeStreamInput{Writer: *r.options.Writer, PreventClose: r.options.PreventClose, SendFinish: sendFinish})
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

// providerResult is a result the provider reported in a model call's
// content, with its position there.
type providerResult struct {
	ToolResult
	order int
}

// providerToolResults returns the results of provider-executed tools in
// content, by call ID.
func providerToolResults(content []ContentPart) map[string]providerResult {
	results := map[string]providerResult{}
	for i, part := range content {
		if part.Type != PartToolResult || !part.ProviderExecuted {
			continue
		}
		result := ToolResult{ToolCallID: part.ToolCallID, ToolName: part.ToolName, Output: json.RawMessage("null"), ProviderExecuted: true, ProviderMetadata: part.ProviderOptions}
		if part.Output != nil {
			result.Output = rawOrNull(part.Output.Value)
			result.IsError = strings.HasPrefix(part.Output.Type, "error")
		}
		results[part.ToolCallID] = providerResult{ToolResult: result, order: i}
	}
	return results
}

// orderedResults returns the local and provider results in the order of
// calls, followed by the provider results for calls of earlier steps.
func orderedResults(calls []ToolCall, local, provider []ToolResult) []ToolResult {
	byID := map[string]ToolResult{}
	for _, result := range slices.Concat(local, provider) {
		byID[result.ToolCallID] = result
	}
	var out []ToolResult
	inCalls := map[string]bool{}
	for _, call := range calls {
		inCalls[call.ToolCallID] = true
		if result, ok := byID[call.ToolCallID]; ok {
			out = append(out, result)
		}
	}
	for _, result := range provider {
		if !inCalls[result.ToolCallID] {
			out = append(out, result)
		}
	}
	return out
}

// callIndex returns the index of the call with id toolCallID in calls.
func callIndex(calls []ToolCall, toolCallID string) int {
	return slices.IndexFunc(calls, func(call ToolCall) bool { return call.ToolCallID == toolCallID })
}

// assistantContent returns the content a step's assistant message carries,
// as the Workflow SDK builds it: without the sources and empty text parts of
// the step's content, and with each part's provider metadata as the
// provider options the next model call sends back.
func assistantContent(content []ContentPart) []ContentPart {
	out := make([]ContentPart, 0, len(content))
	for _, part := range content {
		if part.Type == PartSource || part.Type == PartText && part.Text == "" {
			continue
		}
		if metadata := decodeMetadata(part.ProviderMetadata); metadata != nil && part.ProviderOptions == nil && part.Type != PartToolResult {
			part.ProviderOptions = metadata
			part.ProviderMetadata = nil
		}
		out = append(out, part)
	}
	return out
}

// rootCause returns the innermost error of err's chain.
func rootCause(err error) error {
	for {
		next := errors.Unwrap(err)
		if next == nil {
			return err
		}
		err = next
	}
}

// providerModelOutputs converts the outputs of the provider's results in
// content with their tools' ToModelOutput, in place.
func (r *run) providerModelOutputs(ctx context.Context, content []ContentPart, calls []ToolCall) error {
	for i, part := range content {
		toModelOutput := r.tools[part.ToolName].ToModelOutput
		if part.Type != PartToolResult || !part.ProviderExecuted || toModelOutput == nil || part.Output == nil {
			continue
		}
		var output any
		_ = json.Unmarshal(part.Output.Value, &output)
		var input json.RawMessage
		if index := slices.IndexFunc(calls, func(call ToolCall) bool { return call.ToolCallID == part.ToolCallID }); index >= 0 {
			input = calls[index].Input
		}
		converted, err := toModelOutput(ctx, ToolModelOutputOptions{ToolCallID: part.ToolCallID, Input: input, Output: output})
		if err != nil {
			return err
		}
		content[i].Output = &converted
	}
	return nil
}

// validateContext validates a tool's context against its JSON schema.
func validateContext(contextSchema map[string]any, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	var decoded any
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	return schema.NewJSONSchema(contextSchema).Validate(decoded)
}

// firstApprovalResponses returns messages without the approval responses
// that repeat an earlier response to the same approval.
func firstApprovalResponses(messages []ModelMessage) []ModelMessage {
	seen := map[string]bool{}
	out := make([]ModelMessage, 0, len(messages))
	for _, message := range messages {
		content := slices.DeleteFunc(slices.Clone(message.Content), func(part ContentPart) bool {
			if part.Type != PartToolApprovalResponse {
				return false
			}
			repeated := seen[part.ApprovalID]
			seen[part.ApprovalID] = true
			return repeated
		})
		message.Content = content
		out = append(out, message)
	}
	return out
}

// missingToolResults returns the AI SDK's error for tool calls without a
// result.
func missingToolResults(ids []string) error {
	if len(ids) == 1 {
		return fmt.Errorf("Tool result is missing for tool call %s.", ids[0]) //nolint:staticcheck // the AI SDK's error text
	}
	return fmt.Errorf("Tool results are missing for tool calls %s.", strings.Join(ids, ", ")) //nolint:staticcheck // the AI SDK's error text
}

// fileParts returns the file parts of content.
func fileParts(content []ContentPart) []ContentPart {
	var files []ContentPart
	for _, part := range content {
		if part.Type == PartFile {
			files = append(files, part)
		}
	}
	return files
}

// successful returns the results of the calls that did not fail.
func successful(results []ToolResult) []ToolResult {
	return slices.DeleteFunc(slices.Clone(results), func(result ToolResult) bool { return result.IsError })
}

// stepContent returns a step's content with the results of its tool calls,
// as the Workflow SDK reports them: a tool-result part with its input for
// each executed call, a tool-error part for each invalid or failed call, and
// the provider's results with their calls' input, failed ones as tool-error
// parts.
func stepContent(content []ContentPart, calls []ToolCall, results []toolResult, provider map[string]providerResult) []ContentPart {
	inputs := map[string]json.RawMessage{}
	for _, call := range calls {
		inputs[call.ToolCallID] = call.Input
	}
	out := make([]ContentPart, 0, len(content)+len(results))
	for _, part := range content {
		if part.Type == PartToolResult && part.ProviderExecuted {
			part.Input = inputs[part.ToolCallID]
			if result, ok := provider[part.ToolCallID]; ok && result.IsError {
				part = ContentPart{Type: PartToolError, ToolCallID: part.ToolCallID, ToolName: part.ToolName, Input: part.Input, Error: errorText(result.Output), ProviderExecuted: true, ProviderOptions: part.ProviderOptions}
			}
		}
		out = append(out, part)
	}
	for _, result := range results {
		if result.isError {
			out = append(out, ContentPart{Type: PartToolError, ToolCallID: result.call.ToolCallID, ToolName: result.call.ToolName, Input: result.call.Input, Error: errorText(result.raw)})
			continue
		}
		out = append(out, ContentPart{Type: PartToolResult, ToolCallID: result.call.ToolCallID, ToolName: result.call.ToolName, Input: result.call.Input, Output: result.output})
	}
	return out
}

// errorText returns the message of a failed result's JSON output.
func errorText(output json.RawMessage) string {
	var message string
	if json.Unmarshal(output, &message) != nil {
		return string(output)
	}
	return message
}

// inCallOrder returns results in the order of their calls.
func inCallOrder(calls []ToolCall, results []toolResult) []toolResult {
	out := make([]toolResult, 0, len(results))
	for _, call := range calls {
		for _, result := range results {
			if result.call.ToolCallID == call.ToolCallID {
				out = append(out, result)
			}
		}
	}
	return out
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
	// Like the Workflow SDK, headers and each provider's options merge into
	// the ones they override.
	if override.Headers != nil {
		base.Headers = maps.Clone(base.Headers)
		if base.Headers == nil {
			base.Headers = map[string]string{}
		}
		maps.Copy(base.Headers, override.Headers)
	}
	if override.ProviderOptions != nil {
		merged := maps.Clone(base.ProviderOptions)
		if merged == nil {
			merged = map[string]any{}
		}
		for name, options := range override.ProviderOptions {
			current, isMap := merged[name].(map[string]any)
			next, nextIsMap := options.(map[string]any)
			if isMap && nextIsMap {
				combined := maps.Clone(current)
				maps.Copy(combined, next)
				merged[name] = combined
				continue
			}
			merged[name] = options
		}
		base.ProviderOptions = merged
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
	usage := types.Usage{
		InputTokens:  add(a.InputTokens, b.InputTokens),
		OutputTokens: add(a.OutputTokens, b.OutputTokens),
		TotalTokens:  add(a.TotalTokens, b.TotalTokens),
	}
	// Like the Workflow SDK, token details are kept, and a count no step
	// reported stays unset.
	if a.InputDetails != nil || b.InputDetails != nil {
		x, y := cmp.Or(a.InputDetails, &types.InputTokenDetails{}), cmp.Or(b.InputDetails, &types.InputTokenDetails{})
		usage.InputDetails = &types.InputTokenDetails{
			NoCacheTokens:    add(x.NoCacheTokens, y.NoCacheTokens),
			CacheReadTokens:  add(x.CacheReadTokens, y.CacheReadTokens),
			CacheWriteTokens: add(x.CacheWriteTokens, y.CacheWriteTokens),
		}
	}
	if a.OutputDetails != nil || b.OutputDetails != nil {
		x, y := cmp.Or(a.OutputDetails, &types.OutputTokenDetails{}), cmp.Or(b.OutputDetails, &types.OutputTokenDetails{})
		usage.OutputDetails = &types.OutputTokenDetails{
			TextTokens:      add(x.TextTokens, y.TextTokens),
			ReasoningTokens: add(x.ReasoningTokens, y.ReasoningTokens),
		}
	}
	return usage
}

// approvalRejection is why an approved call from client-supplied messages
// did not pass checkApproval.
type approvalRejection struct{ reason string }

func (e *approvalRejection) Error() string { return e.reason }

// rejectApproval returns a settled result that rejects an approved call.
func rejectApproval(reason string) *workflow.Result[any] {
	return workflow.NewResult[any](nil, &approvalRejection{reason: reason})
}
