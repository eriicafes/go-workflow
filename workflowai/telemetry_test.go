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
		if count("ai.workflowAgent.stream fake-model") != 1 || count("step workflowai.modelCall.stream") != 2 || count("step workflowai_test.getWeather") != 1 ||
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
			case "step workflowai.modelCall.stream", "step workflowai_test.getWeather":
				if span.Parent.SpanID() != root.SpanContext.SpanID() {
					t.Fatalf("span %q has parent %q", span.Name, parent)
				}
			case "invoke_agent fake-model":
				if parent != "step workflowai.modelCall.stream" {
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
