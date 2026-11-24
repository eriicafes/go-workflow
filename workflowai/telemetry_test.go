package workflowai_test

import (
	"slices"
	"testing"
	"testing/synctest"

	"github.com/digitallysavvy/go-ai/pkg/telemetry"
	"github.com/eriicafes/go-workflow/workflowai"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestStreamTelemetryTrace(t *testing.T) {
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
		if _, _, err := chat(t, []workflowai.ModelMessage{user("weather in Lagos?")}); err != nil {
			t.Fatal(err)
		}

		spans := map[trace.SpanID]tracetest.SpanStub{}
		var root tracetest.SpanStub
		var names []string
		for _, span := range exporter.GetSpans() {
			spans[span.SpanContext.SpanID()] = span
			names = append(names, span.Name)
			if span.Name == "ai.workflowAgent.stream fake-model" {
				root = span
			}
		}
		if root.Name == "" {
			t.Fatalf("no root span in %v", names)
		}
		// Replays do not repeat spans: one root, one step span per model
		// call and tool call, and go-ai's operation, step, and chat spans
		// under each model call.
		count := func(name string) int {
			return len(slices.DeleteFunc(slices.Clone(names), func(n string) bool { return n != name }))
		}
		if count("ai.workflowAgent.stream fake-model") != 1 || count("step doStreamStep") != 2 || count("step getWeather") != 1 ||
			count("invoke_agent fake-model") != 2 || count("chat fake-model") != 2 {
			t.Fatalf("spans = %v", names)
		}
		// Every span is in the call's trace, and the step spans of model
		// calls and tools are children of the root span.
		for _, span := range spans {
			if span.SpanContext.TraceID() != root.SpanContext.TraceID() {
				t.Fatalf("span %q is in another trace", span.Name)
			}
			parent := spans[span.Parent.SpanID()].Name
			switch span.Name {
			case "step doStreamStep", "step getWeather":
				if span.Parent.SpanID() != root.SpanContext.SpanID() {
					t.Fatalf("span %q has parent %q", span.Name, parent)
				}
			case "invoke_agent fake-model":
				if parent != "step doStreamStep" {
					t.Fatalf("span %q has parent %q", span.Name, parent)
				}
			}
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

// With TelemetryIDGenerator and NewTelemetrySpanProcessor, the call reports
// its root, step, and tool spans when it ends, and the chat spans of its
// model calls, drawn as they happen, are children of its step spans.
func TestStreamReportedTelemetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		exporter := tracetest.NewInMemoryExporter()
		provider := sdktrace.NewTracerProvider(
			sdktrace.WithIDGenerator(workflowai.TelemetryIDGenerator{}),
			sdktrace.WithSpanProcessor(workflowai.NewTelemetrySpanProcessor(sdktrace.NewSimpleSpanProcessor(exporter))),
		)
		defer provider.Shutdown(t.Context())
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
		want := []string{"prepareWorkflowAgentTelemetry", "doStreamStep", "writeDeferredToolCalls", "getWeather", "writeToolResults", "doStreamStep", "writeDeferredToolCalls", "closeStream", "reportWorkflowAgentTelemetry"}
		if !slices.Equal(steps, want) {
			t.Fatalf("steps = %v, want %v", steps, want)
		}

		spans := map[trace.SpanID]tracetest.SpanStub{}
		byName := map[string][]tracetest.SpanStub{}
		for _, span := range exporter.GetSpans() {
			spans[span.SpanContext.SpanID()] = span
			byName[span.Name] = append(byName[span.Name], span)
			for _, attr := range span.Attributes {
				if attr.Key == "workflowai.stand_in" {
					t.Fatalf("stand-in span %q was exported", span.Name)
				}
			}
		}
		parent := func(span tracetest.SpanStub) string { return spans[span.Parent.SpanID()].Name }
		roots := byName["ai.workflowAgent.stream fake-model"]
		if len(roots) != 1 || len(byName["step 1"]) != 1 || len(byName["step 2"]) != 1 || len(byName["chat fake-model"]) != 2 || len(byName["execute_tool getWeather"]) != 1 {
			t.Fatalf("spans = %v", byName)
		}
		root := roots[0]
		for _, span := range exporter.GetSpans() {
			if span.SpanContext.TraceID() != root.SpanContext.TraceID() {
				t.Fatalf("span %q is in another trace", span.Name)
			}
		}
		if parent(byName["step 1"][0]) != root.Name || parent(byName["step 2"][0]) != root.Name || parent(byName["execute_tool getWeather"][0]) != "step 1" {
			t.Fatalf("step 1 parent %q, step 2 parent %q, tool parent %q", parent(byName["step 1"][0]), parent(byName["step 2"][0]), parent(byName["execute_tool getWeather"][0]))
		}
		if chats := byName["chat fake-model"]; parent(chats[0]) != "step 1" || parent(chats[1]) != "step 2" {
			t.Fatalf("chat parents %q, %q", parent(chats[0]), parent(chats[1]))
		}
		// The runtime's step spans are children of the root span too.
		for _, span := range byName["step doStreamStep"] {
			if span.Parent.SpanID() != root.SpanContext.SpanID() {
				t.Fatalf("step doStreamStep has parent %q", parent(span))
			}
		}
		// The reported spans take the times of their recorded events, so
		// the root span covers its steps.
		for _, step := range slices.Concat(byName["step 1"], byName["step 2"]) {
			if step.StartTime.Before(root.StartTime) || step.EndTime.After(root.EndTime) {
				t.Fatalf("%s %v..%v is outside the root %v..%v", step.Name, step.StartTime, step.EndTime, root.StartTime, root.EndTime)
			}
		}
	})
}
