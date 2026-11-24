package workflowai

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/digitallysavvy/go-ai/pkg/provider/types"
	"github.com/digitallysavvy/go-ai/pkg/telemetry"
	"github.com/eriicafes/go-workflow"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// A Stream or Generate call spans many deliveries and processes, so no
// process can draw its root span, or a span for each of its agent steps, as
// they happen. Like the Workflow SDK, the agent records the call's telemetry
// events in workflow code and reports them to the go-ai telemetry
// integrations in the reportWorkflowAgentTelemetry step when the call ends.
// The prepareWorkflowAgentTelemetry step reserves the IDs of the call's root
// and step spans first, so the chat spans of its model calls, which are
// drawn as they happen, are children of the step spans drawn at the end.
//
// OpenTelemetry cannot be told the IDs and times of the spans go-ai draws,
// so the tracer provider that draws them needs TelemetryIDGenerator and
// NewTelemetrySpanProcessor, which apply the reserved IDs and the recorded
// times. Without them the agent draws a root span when the call starts and
// the spans of each model call as it happens.

// TelemetryIDGenerator is an OpenTelemetry ID generator that gives the spans
// of a workflow agent's telemetry the IDs the agent reserved for them, and
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
// when a call ends the times their events were recorded at. It leaves out
// the stand-in spans the agent draws so go-ai can draw a model call's chat
// span in a later step's process. Until it shuts down, it turns on the
// agent's reported telemetry in the process, so a process that runs workflow
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
	if parent.Value(standInKey{}) != nil {
		span.SetAttributes(standInAttribute)
		return
	}
	telemetryClock.start(span.SpanContext())
	p.next.OnStart(parent, span)
}

func (p *telemetrySpanProcessor) OnEnd(span sdktrace.ReadOnlySpan) {
	for _, attr := range span.Attributes() {
		if attr == standInAttribute {
			return
		}
	}
	if start, end, ok := telemetryClock.end(span.SpanContext()); ok {
		span = reportedSpan{ReadOnlySpan: span, start: start, end: end}
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
}

func (s reportedSpan) StartTime() time.Time { return s.start }
func (s reportedSpan) EndTime() time.Time   { return s.end }

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
	// Reported is set when the call's telemetry is reported when it ends.
	Reported bool   `json:"reported,omitempty"`
	CallID   string `json:"callId"`
	// TraceID and RootSpanID are the reserved IDs of the root span, in hex,
	// and Parent is the span the root span is a child of.
	TraceID    string      `json:"traceId,omitempty"`
	RootSpanID string      `json:"rootSpanId,omitempty"`
	TraceFlags byte        `json:"traceFlags,omitempty"`
	Parent     *spanParent `json:"parent,omitempty"`
	// Traceparent is the trace context of the root span a call that is not
	// reported draws when it starts.
	Traceparent string `json:"traceparent,omitempty"`
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

// stepSpanID returns the reserved ID of the span of agent step stepNumber.
func (t *agentTelemetry) stepSpanID(stepNumber int) trace.SpanID {
	traceID, rootID := t.traceID(), t.rootSpanID()
	data := make([]byte, 0, len(traceID)+len(rootID)+8)
	data = append(append(data, traceID[:]...), rootID[:]...)
	data = binary.BigEndian.AppendUint64(data, uint64(stepNumber))
	sum := sha256.Sum256(data)
	var id trace.SpanID
	copy(id[:], sum[:])
	return id
}

// prepareTelemetryInput is the input of prepareWorkflowAgentTelemetry.
type prepareTelemetryInput struct {
	Operation    string `json:"operation"`
	Instructions string `json:"instructions,omitempty"`
}

// prepareWorkflowAgentTelemetry reserves the IDs of a call's root span when
// the process reports telemetry, as the Workflow SDK's step of the same name
// does. Otherwise it draws a root span that starts and ends at once, and
// records its trace context. The model and telemetry settings come from the
// modelCall in ctx.
func prepareWorkflowAgentTelemetry(ctx context.Context, input prepareTelemetryInput) (agentTelemetry, error) {
	m, err := modelCallFrom(ctx)
	if err != nil {
		return agentTelemetry{}, err
	}
	callID := workflow.StepID(ctx)
	if reportedTelemetry.Load() == 0 {
		ctx = telemetry.FireOnStart(ctx, telemetry.TelemetryStartEvent{
			CallID:        callID,
			OperationType: input.Operation,
			ModelProvider: m.model.Provider(),
			ModelID:       m.model.ModelID(),
			Settings:      m.telemetry,
			System:        input.Instructions,
		})
		carrier := propagation.MapCarrier{}
		propagation.TraceContext{}.Inject(ctx, carrier)
		telemetry.FireOnEnd(ctx, telemetry.TelemetryFinishEvent{
			CallID:        callID,
			OperationType: input.Operation,
			ModelProvider: m.model.Provider(),
			ModelID:       m.model.ModelID(),
			Settings:      m.telemetry,
		})
		return agentTelemetry{CallID: callID, Traceparent: carrier["traceparent"]}, nil
	}
	// The root span is in this step's trace, as a child of the span this
	// step's span is a child of: the span of the code that started the run,
	// when there is one.
	prepared := agentTelemetry{Reported: true, CallID: callID, RootSpanID: randomSpanID().String(), TraceFlags: byte(trace.FlagsSampled)}
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
// reports when the call ends.
type telemetryEvent struct {
	Type          string          `json:"type"`
	Time          time.Time       `json:"time"`
	StepNumber    int             `json:"stepNumber,omitempty"`
	ModelProvider string          `json:"modelProvider,omitempty"`
	ModelID       string          `json:"modelId,omitempty"`
	ToolCallID    string          `json:"toolCallId,omitempty"`
	ToolName      string          `json:"toolName,omitempty"`
	Input         json.RawMessage `json:"input,omitempty"`
	Output        json.RawMessage `json:"output,omitempty"`
	Error         string          `json:"error,omitempty"`
	FinishReason  string          `json:"finishReason,omitempty"`
	Usage         types.Usage     `json:"usage"`
	Text          string          `json:"text,omitempty"`
	ResponseID    string          `json:"responseId,omitempty"`
}

// The types of telemetryEvent.
const (
	eventStart     = "start"
	eventStepStart = "stepStart"
	eventToolStart = "toolStart"
	eventToolEnd   = "toolEnd"
	eventStepEnd   = "stepEnd"
	eventEnd       = "end"
	eventError     = "error"
	eventAbort     = "abort"
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
// name does. The root and step spans take their reserved IDs, and every
// span takes the times of the events that start and end it. The telemetry
// settings come from the modelCall in ctx.
func reportWorkflowAgentTelemetry(ctx context.Context, input reportTelemetryInput) (struct{}, error) {
	m, err := modelCallFrom(ctx)
	if err != nil {
		return struct{}{}, err
	}
	t := input.Telemetry
	settings, callID, traceID := m.telemetry, t.CallID, t.traceID()
	defer telemetryClock.clear(traceID)

	root := t.parentContext(ctx)
	steps := map[int]context.Context{}
	for _, event := range input.Events {
		telemetryClock.set(traceID, event.Time)
		stepCtx := steps[event.StepNumber]
		if stepCtx == nil {
			stepCtx = root
		}
		switch event.Type {
		case eventStart:
			root = telemetry.FireOnStart(withReservedIDs(root, traceID, t.rootSpanID()), telemetry.TelemetryStartEvent{
				CallID:        callID,
				OperationType: input.Operation,
				ModelProvider: event.ModelProvider,
				ModelID:       event.ModelID,
				Settings:      settings,
				System:        input.Instructions,
			})
		case eventStepStart:
			steps[event.StepNumber] = telemetry.FireOnStepStart(withReservedIDs(root, traceID, t.stepSpanID(event.StepNumber)), telemetry.TelemetryStepStartEvent{
				Settings:      settings,
				CallID:        callID,
				OperationType: input.Operation,
				StepNumber:    event.StepNumber,
				ModelProvider: event.ModelProvider,
				ModelID:       event.ModelID,
			})
		case eventToolStart:
			telemetry.FireOnToolCallStart(stepCtx, telemetry.TelemetryToolCallStartEvent{
				Settings:   settings,
				CallID:     callID,
				ToolCallID: event.ToolCallID,
				ToolName:   event.ToolName,
				Args:       arguments(event.Input),
			})
		case eventToolEnd:
			var output any
			_ = json.Unmarshal(event.Output, &output)
			var toolErr error
			if event.Error != "" {
				toolErr = errors.New(event.Error)
			}
			telemetry.FireOnToolCallFinish(stepCtx, telemetry.TelemetryToolCallFinishEvent{
				Settings:   settings,
				CallID:     callID,
				ToolCallID: event.ToolCallID,
				ToolName:   event.ToolName,
				Args:       arguments(event.Input),
				Result:     output,
				Error:      toolErr,
			})
		case eventStepEnd:
			telemetry.FireOnStepEnd(stepCtx, telemetry.TelemetryStepEndEvent{
				Settings:      settings,
				CallID:        callID,
				OperationType: input.Operation,
				StepNumber:    event.StepNumber,
				FinishReason:  event.FinishReason,
				Usage:         telemetryUsage(event.Usage),
				Text:          event.Text,
				ResponseID:    event.ResponseID,
			})
		case eventEnd:
			telemetry.FireOnEnd(root, telemetry.TelemetryFinishEvent{
				Settings:      settings,
				CallID:        callID,
				OperationType: input.Operation,
				FinishReason:  event.FinishReason,
				Usage:         telemetryUsage(event.Usage),
				ModelProvider: event.ModelProvider,
				ModelID:       event.ModelID,
				Text:          event.Text,
			})
		case eventError:
			telemetry.FireOnError(root, telemetry.TelemetryErrorEvent{Settings: settings, CallID: callID, Error: errors.New(event.Error)})
		case eventAbort:
			telemetry.FireOnAbort(root, telemetry.TelemetryAbortEvent{Settings: settings, CallID: callID, Reason: errors.New(event.Error)})
		}
	}
	return struct{}{}, nil
}

// startStandIns draws stand-ins for the root span and the span of step
// stepNumber, with their reserved IDs, so go-ai can draw a model call's chat
// span as a child of the step span. NewTelemetrySpanProcessor leaves the
// stand-ins out. The returned function ends them.
func (t *agentTelemetry) startStandIns(ctx context.Context, settings *telemetry.Settings, operation string, stepNumber int, model modelCall) func() {
	ctx = t.parentContext(context.WithValue(ctx, standInKey{}, true))
	ctx = telemetry.FireOnStart(withReservedIDs(ctx, t.traceID(), t.rootSpanID()), telemetry.TelemetryStartEvent{
		CallID:        t.CallID,
		OperationType: operation,
		ModelProvider: model.model.Provider(),
		ModelID:       model.model.ModelID(),
		Settings:      settings,
	})
	stepCtx := telemetry.FireOnStepStart(withReservedIDs(ctx, t.traceID(), t.stepSpanID(stepNumber)), telemetry.TelemetryStepStartEvent{
		Settings:      settings,
		CallID:        t.CallID,
		OperationType: operation,
		StepNumber:    stepNumber,
		ModelProvider: model.model.Provider(),
		ModelID:       model.model.ModelID(),
	})
	return func() {
		telemetry.FireOnStepEnd(stepCtx, telemetry.TelemetryStepEndEvent{Settings: settings, CallID: t.CallID, OperationType: operation, StepNumber: stepNumber})
		telemetry.FireOnEnd(ctx, telemetry.TelemetryFinishEvent{Settings: settings, CallID: t.CallID, OperationType: operation})
	}
}

// standInKey marks the context of stand-in spans.
type standInKey struct{}

var standInAttribute = attribute.Bool("workflowai.stand_in", true)

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

// arguments decodes a tool call's JSON input for telemetry.
func arguments(input json.RawMessage) map[string]any {
	var args map[string]any
	_ = json.Unmarshal(input, &args)
	return args
}
