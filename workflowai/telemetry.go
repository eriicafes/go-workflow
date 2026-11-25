package workflowai

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/digitallysavvy/go-ai/pkg/provider/types"
	"github.com/digitallysavvy/go-ai/pkg/telemetry"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// A Stream or Generate call spans many deliveries and processes, and
// workflow code runs again on every replay, so no process can tell go-ai's
// telemetry integrations about the call as it happens. Like the Workflow SDK
// with its durable telemetry, the agent records the call's telemetry events
// in workflow code, with the times they happened, and the
// reportWorkflowAgentTelemetry step reports them to the integrations, once,
// when the call ends.
//
// OpenTelemetry cannot be told the times of the spans go-ai draws, so the
// tracer provider that draws them needs NewTelemetrySpanProcessor, which
// gives them the recorded times, and TelemetryIDGenerator, which gives the
// call's root span the ID the prepareWorkflowAgentTelemetry step reserved,
// so the spans of the call's steps nest under it.

// TelemetryIDGenerator is an OpenTelemetry ID generator that gives the root
// span of a workflow agent's call the ID the agent reserved for it, and
// random IDs to every other span. Use it with NewTelemetrySpanProcessor:
//
//	provider := sdktrace.NewTracerProvider(
//		sdktrace.WithIDGenerator(workflowai.TelemetryIDGenerator{}),
//		sdktrace.WithSpanProcessor(workflowai.NewTelemetrySpanProcessor(sdktrace.NewBatchSpanProcessor(exporter))),
//	)
type TelemetryIDGenerator struct{}

// NewIDs returns the reserved trace and span IDs in ctx, or random ones.
func (TelemetryIDGenerator) NewIDs(ctx context.Context) (trace.TraceID, trace.SpanID) {
	if r, ok := ctx.Value(reservedIDsKey{}).(*reservedIDs); ok && r.take() {
		return r.traceID, r.spanID
	}
	var traceID trace.TraceID
	_, _ = rand.Read(traceID[:])
	return traceID, randomSpanID()
}

// NewSpanID returns the reserved span ID in ctx, or a random one.
func (TelemetryIDGenerator) NewSpanID(ctx context.Context, traceID trace.TraceID) trace.SpanID {
	if r, ok := ctx.Value(reservedIDsKey{}).(*reservedIDs); ok && r.traceID == traceID && r.take() {
		return r.spanID
	}
	return randomSpanID()
}

// NewTelemetrySpanProcessor returns an OpenTelemetry span processor that
// passes spans to next, after it gives the spans a workflow agent reports
// when a call ends the times their events were recorded at. Until it shuts
// down, the agent reserves the ID of each call's root span in the process,
// so the steps the call runs nest under it; a process that runs workflow
// agents creates it before it runs them. Use it with TelemetryIDGenerator.
func NewTelemetrySpanProcessor(next sdktrace.SpanProcessor) sdktrace.SpanProcessor {
	reportedTelemetry.Add(1)
	return &telemetrySpanProcessor{next: next}
}

// reportedTelemetry counts the process's running NewTelemetrySpanProcessors.
var reportedTelemetry atomic.Int64

type telemetrySpanProcessor struct {
	next     sdktrace.SpanProcessor
	shutdown sync.Once
}

func (p *telemetrySpanProcessor) OnStart(parent context.Context, span sdktrace.ReadWriteSpan) {
	telemetryClock.start(span.SpanContext())
	p.next.OnStart(parent, span)
}

func (p *telemetrySpanProcessor) OnEnd(span sdktrace.ReadOnlySpan) {
	if start, end, ok := telemetryClock.end(span.SpanContext()); ok {
		span = reportedSpan{ReadOnlySpan: span, start: start, end: end, events: reportedEvents(span, start, end)}
	}
	p.next.OnEnd(span)
}

func (p *telemetrySpanProcessor) Shutdown(ctx context.Context) error {
	p.shutdown.Do(func() { reportedTelemetry.Add(-1) })
	return p.next.Shutdown(ctx)
}

func (p *telemetrySpanProcessor) ForceFlush(ctx context.Context) error { return p.next.ForceFlush(ctx) }

// reportedSpan is a span the agent reported, with its recorded times.
type reportedSpan struct {
	sdktrace.ReadOnlySpan
	start, end time.Time
	events     []sdktrace.Event
}

func (s reportedSpan) StartTime() time.Time     { return s.start }
func (s reportedSpan) EndTime() time.Time       { return s.end }
func (s reportedSpan) Events() []sdktrace.Event { return s.events }

// reportedEvents returns the events of span, which were added while it was
// reported, moved from the span's time in the report to its recorded times.
func reportedEvents(span sdktrace.ReadOnlySpan, start, end time.Time) []sdktrace.Event {
	events := span.Events()
	if len(events) == 0 {
		return events
	}
	reportStart, reportEnd := span.StartTime(), span.EndTime()
	moved := make([]sdktrace.Event, len(events))
	for i, event := range events {
		moved[i] = event
		switch {
		case !event.Time.After(reportStart):
			moved[i].Time = start
		case !event.Time.Before(reportEnd):
			moved[i].Time = end
		default:
			fraction := float64(event.Time.Sub(reportStart)) / float64(reportEnd.Sub(reportStart))
			moved[i].Time = start.Add(time.Duration(fraction * float64(end.Sub(start))))
		}
	}
	return moved
}

// telemetryClock holds the recorded time of the event each reported trace
// is at, which the spans it starts and ends take.
var telemetryClock = &clock{now: map[trace.TraceID]time.Time{}, starts: map[trace.SpanID]time.Time{}}

type clock struct {
	mu     sync.Mutex
	now    map[trace.TraceID]time.Time
	starts map[trace.SpanID]time.Time
}

func (c *clock) set(traceID trace.TraceID, at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now[traceID] = at
}

func (c *clock) clear(traceID trace.TraceID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.now, traceID)
}

func (c *clock) start(span trace.SpanContext) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if at, ok := c.now[span.TraceID()]; ok {
		c.starts[span.SpanID()] = at
	}
}

func (c *clock) end(span trace.SpanContext) (start, end time.Time, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	start, ok = c.starts[span.SpanID()]
	delete(c.starts, span.SpanID())
	return start, c.now[span.TraceID()], ok
}

// agentTelemetry is what prepareWorkflowAgentTelemetry records for a call.
type agentTelemetry struct {
	CallID string `json:"callId"`
	// Reserved is set when the root span's IDs are reserved: TraceID and
	// RootSpanID, in hex. Parent is the span the root span is a child of.
	Reserved   bool        `json:"reserved,omitempty"`
	TraceID    string      `json:"traceId,omitempty"`
	RootSpanID string      `json:"rootSpanId,omitempty"`
	TraceFlags byte        `json:"traceFlags,omitempty"`
	Parent     *spanParent `json:"parent,omitempty"`
}

// spanParent is the span context of a root span's parent.
type spanParent struct {
	TraceID    string `json:"traceId"`
	SpanID     string `json:"spanId"`
	TraceFlags byte   `json:"traceFlags,omitempty"`
}

// traceID returns the call's reserved trace ID.
func (t *agentTelemetry) traceID() trace.TraceID {
	id, _ := trace.TraceIDFromHex(t.TraceID)
	return id
}

// rootSpanID returns the reserved ID of the call's root span.
func (t *agentTelemetry) rootSpanID() trace.SpanID {
	id, _ := trace.SpanIDFromHex(t.RootSpanID)
	return id
}

// rootContext returns the span context of the call's root span.
func (t *agentTelemetry) rootContext() trace.SpanContext {
	return trace.NewSpanContext(trace.SpanContextConfig{TraceID: t.traceID(), SpanID: t.rootSpanID(), TraceFlags: trace.TraceFlags(t.TraceFlags), Remote: true})
}

// parentContext returns ctx with the root span's parent as its span.
func (t *agentTelemetry) parentContext(ctx context.Context) context.Context {
	var parent trace.SpanContext
	if t.Parent != nil {
		traceID, _ := trace.TraceIDFromHex(t.Parent.TraceID)
		spanID, _ := trace.SpanIDFromHex(t.Parent.SpanID)
		parent = trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: spanID, TraceFlags: trace.TraceFlags(t.Parent.TraceFlags), Remote: true})
	}
	return trace.ContextWithSpanContext(ctx, parent)
}

// prepareWorkflowAgentTelemetry draws the call's telemetry call ID and, when
// the process has a NewTelemetrySpanProcessor, reserves the IDs of the call's
// root span in this step's trace, as a child of the span this step's span is
// a child of, as the Workflow SDK's step of the same name reserves them for
// its durable telemetry integrations.
func prepareWorkflowAgentTelemetry(ctx context.Context, input struct{}) (agentTelemetry, error) {
	prepared := agentTelemetry{CallID: callID()}
	if reportedTelemetry.Load() == 0 {
		return prepared, nil
	}
	prepared.Reserved, prepared.RootSpanID, prepared.TraceFlags = true, randomSpanID().String(), byte(trace.FlagsSampled)
	current := trace.SpanContextFromContext(ctx)
	if current.IsValid() {
		prepared.TraceID, prepared.TraceFlags = current.TraceID().String(), byte(current.TraceFlags())
	} else {
		var traceID trace.TraceID
		_, _ = rand.Read(traceID[:])
		prepared.TraceID = traceID.String()
	}
	if span, ok := trace.SpanFromContext(ctx).(sdktrace.ReadOnlySpan); ok && span.Parent().IsValid() {
		parent := span.Parent()
		prepared.Parent = &spanParent{TraceID: parent.TraceID().String(), SpanID: parent.SpanID().String(), TraceFlags: byte(parent.TraceFlags())}
	}
	return prepared, nil
}

// telemetryEvent is a telemetry event the agent records in workflow code and
// reports when the call ends. Contexts hold only the fields the telemetry
// settings include.
type telemetryEvent struct {
	Type string `json:"type"`
	// Timestamp is when the event happened, in milliseconds since the Unix
	// epoch, as the Workflow SDK records it.
	Timestamp        int64           `json:"timestamp"`
	StepNumber       int             `json:"stepNumber,omitempty"`
	ModelProvider    string          `json:"modelProvider,omitempty"`
	ModelID          string          `json:"modelId,omitempty"`
	ToolCallID       string          `json:"toolCallId,omitempty"`
	ToolName         string          `json:"toolName,omitempty"`
	Input            json.RawMessage `json:"input,omitempty"`
	Output           json.RawMessage `json:"output,omitempty"`
	Error            string          `json:"error,omitempty"`
	FinishReason     string          `json:"finishReason,omitempty"`
	Usage            types.Usage     `json:"usage"`
	Text             string          `json:"text,omitempty"`
	ResponseID       string          `json:"responseId,omitempty"`
	ProviderMetadata map[string]any  `json:"providerMetadata,omitempty"`
	Files            []ContentPart   `json:"files,omitempty"`
	RuntimeContext   map[string]any  `json:"runtimeContext,omitempty"`
	ToolsContext     map[string]any  `json:"toolsContext,omitempty"`
	ToolContext      map[string]any  `json:"toolContext,omitempty"`
	// Messages are the messages the call or model call started with, and
	// Content the model call's content, recorded when the telemetry
	// settings record inputs and outputs.
	Messages  []ModelMessage      `json:"messages,omitempty"`
	Content   []ContentPart       `json:"content,omitempty"`
	Tools     []toolDefinition    `json:"tools,omitempty"`
	Settings  *GenerationSettings `json:"settings,omitempty"`
	ToolCalls []ToolCall          `json:"toolCalls,omitempty"`
}

// The types of telemetryEvent.
const (
	eventStart          = "start"
	eventStepStart      = "stepStart"
	eventModelCallStart = "modelCallStart"
	eventModelCallEnd   = "modelCallEnd"
	eventToolStart      = "toolStart"
	eventToolEnd        = "toolEnd"
	eventStepEnd        = "stepEnd"
	eventEnd            = "end"
	eventError          = "error"
	eventAbort          = "abort"
)

// reportTelemetryInput is the input of reportWorkflowAgentTelemetry.
type reportTelemetryInput struct {
	Telemetry    agentTelemetry   `json:"telemetry"`
	Operation    string           `json:"operation"`
	Instructions string           `json:"instructions,omitempty"`
	Events       []telemetryEvent `json:"events"`
}

// reportWorkflowAgentTelemetry reports a call's recorded telemetry events to
// the go-ai telemetry integrations, as the Workflow SDK's step of the same
// name does. Each integration gets the events on its own, so one that panics
// does not keep the others from them. The telemetry settings come from the
// modelCall in ctx.
func reportWorkflowAgentTelemetry(ctx context.Context, input reportTelemetryInput) (struct{}, error) {
	m, err := modelCallFrom(ctx)
	if err != nil {
		return struct{}{}, err
	}
	if input.Telemetry.Reserved {
		defer telemetryClock.clear(input.Telemetry.traceID())
	}
	if m.telemetry == nil || len(m.telemetry.Integrations) == 0 {
		// go-ai does not list its globally registered integrations, so they
		// get the events together.
		reportTo(ctx, m.telemetry, input)
		return struct{}{}, nil
	}
	for _, integration := range m.telemetry.Integrations {
		settings := *m.telemetry
		settings.Integrations = []telemetry.TelemetryIntegration{integration}
		reportTo(ctx, &settings, input)
	}
	return struct{}{}, nil
}

// reportTo reports the recorded events to the integrations of settings. A
// panicking integration stops getting them, as the Workflow SDK isolates an
// integration that throws.
func reportTo(ctx context.Context, settings *telemetry.Settings, input reportTelemetryInput) {
	defer func() { _ = recover() }()
	t := input.Telemetry
	callID, operation := t.CallID, input.Operation
	at := func(event telemetryEvent) {
		if t.Reserved {
			telemetryClock.set(t.traceID(), time.UnixMilli(event.Timestamp))
		}
	}
	toolEnds := map[string]telemetryEvent{}
	for _, event := range input.Events {
		if event.Type == eventToolEnd {
			toolEnds[event.ToolCallID] = event
		}
	}
	root := t.parentContext(ctx)
	steps := map[int]context.Context{}
	calls := map[int]context.Context{}
	stepContext := func(stepNumber int) context.Context {
		if stepCtx := steps[stepNumber]; stepCtx != nil {
			return stepCtx
		}
		return root
	}
	for _, event := range input.Events {
		at(event)
		switch event.Type {
		case eventStart:
			start := root
			if t.Reserved {
				start = withReservedIDs(root, t.traceID(), t.rootSpanID())
			}
			startEvent := telemetry.TelemetryStartEvent{
				CallID:         callID,
				OperationType:  operation,
				ModelProvider:  event.ModelProvider,
				ModelID:        event.ModelID,
				Settings:       settings,
				System:         input.Instructions,
				Messages:       toProviderMessages(event.Messages),
				RuntimeContext: event.RuntimeContext,
				ToolsContext:   event.ToolsContext,
			}
			if s := event.Settings; s != nil {
				startEvent.MaxOutputTokens, startEvent.Temperature, startEvent.TopP, startEvent.TopK = s.MaxOutputTokens, s.Temperature, s.TopP, s.TopK
				startEvent.PresencePenalty, startEvent.FrequencyPenalty, startEvent.StopSequences, startEvent.Seed = s.PresencePenalty, s.FrequencyPenalty, s.StopSequences, s.Seed
				startEvent.Headers, startEvent.MaxRetries = s.Headers, s.MaxRetries
			}
			root = telemetry.FireOnStart(start, startEvent)
		case eventStepStart:
			steps[event.StepNumber] = telemetry.FireOnStepStart(root, telemetry.TelemetryStepStartEvent{
				Settings:       settings,
				CallID:         callID,
				OperationType:  operation,
				StepNumber:     event.StepNumber,
				ModelProvider:  event.ModelProvider,
				ModelID:        event.ModelID,
				RuntimeContext: event.RuntimeContext,
				ToolsContext:   event.ToolsContext,
			})
		case eventModelCallStart:
			startEvent := telemetry.LanguageModelCallStartEvent{
				Settings:      settings,
				CallID:        callID,
				ModelProvider: event.ModelProvider,
				ModelID:       event.ModelID,
				Prompt:        types.Prompt{System: input.Instructions, Messages: promptMessages(event.Messages)},
				Tools:         telemetryTools(event.Tools),
				System:        input.Instructions,
			}
			if s := event.Settings; s != nil {
				startEvent.Temperature, startEvent.MaxOutputTokens, startEvent.TopP, startEvent.TopK = s.Temperature, s.MaxOutputTokens, s.TopP, s.TopK
				startEvent.PresencePenalty, startEvent.FrequencyPenalty, startEvent.StopSequences, startEvent.Seed = s.PresencePenalty, s.FrequencyPenalty, s.StopSequences, s.Seed
			}
			calls[event.StepNumber] = telemetry.FireOnLanguageModelCallStart(stepContext(event.StepNumber), startEvent)
		case eventModelCallEnd:
			callCtx := calls[event.StepNumber]
			if callCtx == nil {
				callCtx = stepContext(event.StepNumber)
			}
			telemetry.FireOnLanguageModelCallEnd(callCtx, telemetry.LanguageModelCallEndEvent{
				Settings:         settings,
				CallID:           callID,
				ModelProvider:    event.ModelProvider,
				ModelID:          event.ModelID,
				FinishReason:     event.FinishReason,
				Usage:            telemetryUsage(event.Usage),
				Content:          contentParts(event.Content),
				ResponseID:       event.ResponseID,
				ProviderMetadata: event.ProviderMetadata,
			})
		case eventToolStart:
			stepCtx := stepContext(event.StepNumber)
			args := arguments(event.Input)
			toolCtx := telemetry.FireOnToolCallStart(stepCtx, telemetry.TelemetryToolCallStartEvent{
				Settings:    settings,
				CallID:      callID,
				ToolCallID:  event.ToolCallID,
				ToolName:    event.ToolName,
				Args:        args,
				ToolContext: event.ToolContext,
			})
			// The tool ran in its own step, so the integrations wrap a
			// stand-in that returns its recorded result.
			end, ended := toolEnds[event.ToolCallID]
			_, _ = telemetry.FireExecuteToolWithSettings(toolCtx, settings, event.ToolName, args, func(context.Context, map[string]any) (any, error) {
				if !ended {
					return nil, nil
				}
				at(end)
				output, toolErr := end.result()
				return output, toolErr
			})
		case eventToolEnd:
			output, toolErr := event.result()
			telemetry.FireOnToolCallFinish(stepContext(event.StepNumber), telemetry.TelemetryToolCallFinishEvent{
				Settings:    settings,
				CallID:      callID,
				ToolCallID:  event.ToolCallID,
				ToolName:    event.ToolName,
				Args:        arguments(event.Input),
				Result:      output,
				Error:       toolErr,
				ToolContext: event.ToolContext,
			})
		case eventStepEnd:
			telemetry.FireOnStepEnd(stepContext(event.StepNumber), telemetry.TelemetryStepEndEvent{
				Settings:         settings,
				CallID:           callID,
				OperationType:    operation,
				StepNumber:       event.StepNumber,
				FinishReason:     event.FinishReason,
				Usage:            telemetryUsage(event.Usage),
				Text:             event.Text,
				Files:            generatedFiles(event.Files),
				ResponseID:       event.ResponseID,
				ProviderMetadata: event.ProviderMetadata,
				RuntimeContext:   event.RuntimeContext,
				ToolsContext:     event.ToolsContext,
			})
		case eventEnd:
			telemetry.FireOnEnd(root, telemetry.TelemetryFinishEvent{
				Settings:         settings,
				CallID:           callID,
				OperationType:    operation,
				FinishReason:     event.FinishReason,
				Usage:            telemetryUsage(event.Usage),
				ModelProvider:    event.ModelProvider,
				ModelID:          event.ModelID,
				Text:             event.Text,
				ToolCalls:        providerToolCalls(event.ToolCalls),
				Files:            generatedFiles(event.Files),
				ProviderMetadata: event.ProviderMetadata,
				RuntimeContext:   event.RuntimeContext,
				ToolsContext:     event.ToolsContext,
			})
		case eventError:
			telemetry.FireOnError(root, telemetry.TelemetryErrorEvent{Settings: settings, CallID: callID, Error: errors.New(event.Error)})
		case eventAbort:
			telemetry.FireOnAbort(root, telemetry.TelemetryAbortEvent{Settings: settings, CallID: callID, Reason: errors.New(event.Error)})
		}
	}
}

// result returns a tool end event's output and error.
func (e telemetryEvent) result() (any, error) {
	var output any
	_ = json.Unmarshal(e.Output, &output)
	if e.Error != "" {
		return output, errors.New(e.Error)
	}
	return output, nil
}

// reservedIDsKey is the context key of the IDs the next span takes.
type reservedIDsKey struct{}

// reservedIDs are IDs the agent reserved for the next span started in a
// context.
type reservedIDs struct {
	traceID trace.TraceID
	spanID  trace.SpanID
	taken   atomic.Bool
}

// take reports whether the IDs are still free, and takes them.
func (r *reservedIDs) take() bool { return r.taken.CompareAndSwap(false, true) }

// withReservedIDs returns ctx in which the next span takes traceID and
// spanID.
func withReservedIDs(ctx context.Context, traceID trace.TraceID, spanID trace.SpanID) context.Context {
	return context.WithValue(ctx, reservedIDsKey{}, &reservedIDs{traceID: traceID, spanID: spanID})
}

func randomSpanID() trace.SpanID {
	var id trace.SpanID
	_, _ = rand.Read(id[:])
	return id
}

// callID returns a telemetry call ID in the Workflow SDK's form,
// "call-" and 24 random characters.
func callID() string {
	const alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	id := make([]byte, 24)
	_, _ = rand.Read(id)
	for i, b := range id {
		id[i] = alphabet[int(b)%len(alphabet)]
	}
	return "call-" + string(id)
}

// telemetryTools converts tool definitions to go-ai tools for telemetry.
func telemetryTools(definitions []toolDefinition) []types.Tool {
	var tools []types.Tool
	for _, tool := range definitions {
		tools = append(tools, types.Tool{Name: tool.Name, Description: tool.Description, Parameters: tool.InputSchema})
	}
	return tools
}

// contentParts converts content to go-ai content parts for telemetry.
func contentParts(content []ContentPart) []types.ContentPart {
	if len(content) == 0 {
		return nil
	}
	return toProviderMessages([]ModelMessage{{Role: RoleAssistant, Content: content}})[0].Content
}

// providerToolCalls converts tool calls to go-ai tool calls for telemetry.
func providerToolCalls(calls []ToolCall) []types.ToolCall {
	var out []types.ToolCall
	for _, call := range calls {
		out = append(out, types.ToolCall{ID: call.ToolCallID, ToolName: call.ToolName, Arguments: arguments(call.Input), RawArguments: string(call.Input), ProviderExecuted: call.ProviderExecuted})
	}
	return out
}

// arguments decodes a tool call's JSON input for telemetry.
func arguments(input json.RawMessage) map[string]any {
	var args map[string]any
	_ = json.Unmarshal(input, &args)
	return args
}

// generatedFiles converts the file parts of a step to telemetry files.
func generatedFiles(parts []ContentPart) []types.GeneratedFileContent {
	var files []types.GeneratedFileContent
	for _, part := range parts {
		data, err := base64.StdEncoding.DecodeString(part.Data)
		if err != nil {
			data = []byte(part.Data)
		}
		files = append(files, types.GeneratedFileContent{MediaType: part.MediaType, Data: data})
	}
	return files
}

// includedContext returns the fields of a context map that include
// selects, or nil when it selects none.
func includedContext(context any, include map[string]bool) map[string]any {
	values, ok := context.(map[string]any)
	if !ok {
		return nil
	}
	var included map[string]any
	for key, value := range values {
		if include[key] {
			if included == nil {
				included = map[string]any{}
			}
			included[key] = value
		}
	}
	return included
}
