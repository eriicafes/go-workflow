package workflowai_test

import (
	"context"
	"maps"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/digitallysavvy/go-ai/pkg/telemetry"
	"github.com/eriicafes/go-workflow/workflowai"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// Without NewTelemetrySpanProcessor, the call still reports one span tree
// when it ends: the root span, a span for each step, and the tool and model
// call spans under them. Only their times and the root span's ID are left to
// the processor and the ID generator.
func TestStreamTelemetryWithoutProcessor(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		exporter := tracetest.NewInMemoryExporter()
		provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
		// go-workflow spans each step with the global tracer provider.
		previous := otel.GetTracerProvider()
		otel.SetTracerProvider(provider)
		defer otel.SetTracerProvider(previous)
		setup(toolCalls(call("c1", "getWeather", `{"city":"Lagos"}`)), text("It is 21 degrees."))
		testOptions.Telemetry = &telemetry.Options{
			RecordInputs:  true,
			RecordOutputs: true,
			Integrations:  []telemetry.TelemetryIntegration{telemetry.NewOpenTelemetry(telemetry.OpenTelemetryOptions{Tracer: provider.Tracer("test")})},
		}
		_, _, steps, err := chatWithSteps(t, []workflowai.ModelMessage{user("weather in Lagos?")})
		if err != nil {
			t.Fatal(err)
		}
		if steps[0] != "prepareWorkflowAgentTelemetry" || steps[len(steps)-1] != "reportWorkflowAgentTelemetry" {
			t.Fatalf("steps = %v", steps)
		}
		tree := newSpanTree(t, exporter.GetSpans())
		root, step1, step2, tool := tree.one(t, "invoke_agent fake-model"), tree.one(t, "step 1"), tree.one(t, "step 2"), tree.one(t, "execute_tool getWeather")
		if tree.parent(step1) != root.Name || tree.parent(step2) != root.Name || tree.parent(tool) != "step 1" {
			t.Fatalf("step 1 parent %q, step 2 parent %q, tool parent %q", tree.parent(step1), tree.parent(step2), tree.parent(tool))
		}
		if chats := tree.byName["chat fake-model"]; len(chats) != 2 || tree.parent(chats[0]) != "step 1" || tree.parent(chats[1]) != "step 2" {
			t.Fatalf("chat spans = %v", chats)
		}
	})
}

func TestStreamDisabledTelemetryRecordsNoSpans(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		exporter := tracetest.NewInMemoryExporter()
		provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
		setup(text("Hi!"))
		testOptions.Telemetry = &telemetry.Options{
			IsEnabled:    telemetry.Bool(false),
			Integrations: []telemetry.TelemetryIntegration{telemetry.NewOpenTelemetry(telemetry.OpenTelemetryOptions{Tracer: provider.Tracer("test")})},
		}
		if _, _, err := chat(t, []workflowai.ModelMessage{user("hi")}); err != nil {
			t.Fatal(err)
		}
		if spans := exporter.GetSpans(); len(spans) != 0 {
			t.Fatalf("recorded %d spans", len(spans))
		}
	})
}

// reportedProvider returns a tracer provider with TelemetryIDGenerator and
// NewTelemetrySpanProcessor, set as the global one for the test, and its
// exporter.
func reportedProvider(t *testing.T) (*sdktrace.TracerProvider, *tracetest.InMemoryExporter) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithIDGenerator(workflowai.TelemetryIDGenerator{}),
		sdktrace.WithSpanProcessor(workflowai.NewTelemetrySpanProcessor(sdktrace.NewSimpleSpanProcessor(exporter))),
	)
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		otel.SetTracerProvider(previous)
		_ = provider.Shutdown(context.Background())
	})
	return provider, exporter
}

// spanTree indexes exported spans by name and finds their parents' names.
type spanTree struct {
	spans  map[trace.SpanID]tracetest.SpanStub
	byName map[string][]tracetest.SpanStub
}

func newSpanTree(t *testing.T, stubs tracetest.SpanStubs) spanTree {
	t.Helper()
	tree := spanTree{spans: map[trace.SpanID]tracetest.SpanStub{}, byName: map[string][]tracetest.SpanStub{}}
	for _, span := range stubs {
		tree.spans[span.SpanContext.SpanID()] = span
		tree.byName[span.Name] = append(tree.byName[span.Name], span)
		for _, attr := range span.Attributes {
			if attr.Key == "workflowai.stand_in" {
				t.Fatalf("stand-in span %q was exported", span.Name)
			}
		}
	}
	return tree
}

func (tree spanTree) one(t *testing.T, name string) tracetest.SpanStub {
	t.Helper()
	if spans := tree.byName[name]; len(spans) == 1 {
		return spans[0]
	}
	t.Fatalf("%d %q spans in %v", len(tree.byName[name]), name, slices.Sorted(maps.Keys(tree.byName)))
	return tracetest.SpanStub{}
}

func (tree spanTree) parent(span tracetest.SpanStub) string {
	return tree.spans[span.Parent.SpanID()].Name
}

// With TelemetryIDGenerator and NewTelemetrySpanProcessor, the call reports
// its root, step, and tool spans when it ends, at the times of their events,
// and the chat spans of its model calls, drawn as they happen, are children
// of its step spans.
func TestStreamReportedTelemetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		provider, exporter := reportedProvider(t)
		setup(toolCalls(call("c1", "wait", `{"name":"a","seconds":5}`)), text("Waited."))
		testOptions.Telemetry = &telemetry.Options{
			RecordInputs:  true,
			RecordOutputs: true,
			Integrations:  []telemetry.TelemetryIntegration{telemetry.NewOpenTelemetry(telemetry.OpenTelemetryOptions{Tracer: provider.Tracer("test")})},
		}
		_, _, steps, err := chatWithSteps(t, []workflowai.ModelMessage{user("wait 5 seconds")})
		if err != nil {
			t.Fatal(err)
		}
		want := []string{"prepareWorkflowAgentTelemetry", "doStreamStep", "writeDeferredToolCalls", "wait", "writeToolResults", "doStreamStep", "writeDeferredToolCalls", "closeStream", "reportWorkflowAgentTelemetry"}
		if !slices.Equal(steps, want) {
			t.Fatalf("steps = %v, want %v", steps, want)
		}

		tree := newSpanTree(t, exporter.GetSpans())
		root, step1, step2, tool := tree.one(t, "invoke_agent fake-model"), tree.one(t, "step 1"), tree.one(t, "step 2"), tree.one(t, "execute_tool wait")
		for _, span := range exporter.GetSpans() {
			if span.SpanContext.TraceID() != root.SpanContext.TraceID() {
				t.Fatalf("span %q is in another trace", span.Name)
			}
		}
		if tree.parent(step1) != root.Name || tree.parent(step2) != root.Name || tree.parent(tool) != "step 1" {
			t.Fatalf("step 1 parent %q, step 2 parent %q, tool parent %q", tree.parent(step1), tree.parent(step2), tree.parent(tool))
		}
		if chats := tree.byName["chat fake-model"]; len(chats) != 2 || tree.parent(chats[0]) != "step 1" || tree.parent(chats[1]) != "step 2" {
			t.Fatalf("chat spans = %v", chats)
		}
		// The runtime's step spans are children of the root span too.
		for _, span := range tree.byName["step doStreamStep"] {
			if span.Parent.SpanID() != root.SpanContext.SpanID() {
				t.Fatalf("step doStreamStep has parent %q", tree.parent(span))
			}
		}
		// The reported spans take the times of their recorded events: the
		// tool waited 5 seconds, inside step 1, inside the root span.
		if d := tool.EndTime.Sub(tool.StartTime); d != 5*time.Second {
			t.Fatalf("tool span lasted %v", d)
		}
		if step1.StartTime.After(tool.StartTime) || step1.EndTime.Before(tool.EndTime) || !step2.StartTime.Before(step2.EndTime) && !step2.StartTime.Equal(step2.EndTime) {
			t.Fatalf("step 1 %v..%v, tool %v..%v", step1.StartTime, step1.EndTime, tool.StartTime, tool.EndTime)
		}
		if root.StartTime.After(step1.StartTime) || root.EndTime.Before(step2.EndTime) || root.EndTime.Sub(root.StartTime) < 5*time.Second {
			t.Fatalf("root %v..%v, steps %v..%v", root.StartTime, root.EndTime, step1.StartTime, step2.EndTime)
		}
	})
}

// LegacyOpenTelemetry draws its spans of the call when it is reported, with
// the times of their events.
func TestStreamReportedLegacyTelemetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		provider, exporter := reportedProvider(t)
		setup(toolCalls(call("c1", "wait", `{"name":"a","seconds":5}`)), text("Waited."))
		testOptions.Telemetry = &telemetry.Options{
			Integrations: []telemetry.TelemetryIntegration{telemetry.NewLegacyOpenTelemetry(telemetry.LegacyOpenTelemetryOptions{Tracer: provider.Tracer("test")})},
		}
		if _, _, err := chat(t, []workflowai.ModelMessage{user("wait 5 seconds")}); err != nil {
			t.Fatal(err)
		}
		tree := newSpanTree(t, exporter.GetSpans())
		root, tool := tree.one(t, "ai.streamText"), tree.one(t, "ai.toolCall")
		if steps := tree.byName["ai.streamText.doStream"]; len(steps) != 2 || tree.parent(steps[0]) != root.Name || tree.parent(tool) != "ai.streamText.doStream" {
			t.Fatalf("spans = %v", slices.Sorted(maps.Keys(tree.byName)))
		}
		if d := tool.EndTime.Sub(tool.StartTime); d != 5*time.Second || root.EndTime.Sub(root.StartTime) < 5*time.Second {
			t.Fatalf("tool lasted %v, root lasted %v", d, root.EndTime.Sub(root.StartTime))
		}
	})
}

// The tool calls of approvals a call resumes before it starts are reported
// under its root span.
func TestStreamReportedTelemetryResumedApproval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		setup(toolCalls(call("c1", "bookFlight", `{"flightId":"LH123"}`)), text("Booked."))
		approvalTurn(t)
		provider, exporter := reportedProvider(t)
		testOptions.Telemetry = &telemetry.Options{
			Integrations: []telemetry.TelemetryIntegration{telemetry.NewOpenTelemetry(telemetry.OpenTelemetryOptions{Tracer: provider.Tracer("test")})},
		}
		if _, _, err := chat(t, respond(t, "", true, `{"flightId":"LH123"}`)); err != nil {
			t.Fatal(err)
		}
		tree := newSpanTree(t, exporter.GetSpans())
		root, tool := tree.one(t, "invoke_agent fake-model"), tree.one(t, "execute_tool bookFlight")
		if tool.Parent.SpanID() != root.SpanContext.SpanID() {
			t.Fatalf("execute_tool bookFlight has parent %q", tree.parent(tool))
		}
	})
}

// recordingIntegration records the operations of the calls it starts.
type recordingIntegration struct {
	telemetry.NoopTelemetryIntegration
	mu         sync.Mutex
	operations []string
}

func (i *recordingIntegration) OnStart(ctx context.Context, e telemetry.TelemetryStartEvent) context.Context {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.operations = append(i.operations, e.OperationType)
	return ctx
}

// Globally registered integrations get the call reported once, as the
// Workflow SDK reports to its global integrations, even without Telemetry.
func TestStreamReportedTelemetryGlobalIntegrations(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		reportedProvider(t)
		recording := &recordingIntegration{}
		telemetry.RegisterTelemetryIntegration(recording)
		defer telemetry.ClearTelemetryIntegrations()
		setup(toolCalls(call("c1", "getWeather", `{"city":"Lagos"}`)), text("It is 21 degrees."))
		if _, _, err := chat(t, []workflowai.ModelMessage{user("weather in Lagos?")}); err != nil {
			t.Fatal(err)
		}
		if want := []string{"ai.streamText"}; !slices.Equal(recording.operations, want) {
			t.Fatalf("operations = %v, want %v", recording.operations, want)
		}
	})
}
