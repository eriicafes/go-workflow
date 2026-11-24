package main

import (
	"context"
	"log"
	"os"

	"github.com/eriicafes/go-workflow/workflowai"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// startTracing exports traces over OTLP when OTEL_EXPORTER_OTLP_ENDPOINT is
// set, with workflowai's ID generator and span processor, which report each
// agent call's root, step, and tool spans when the call ends. It returns a
// function that flushes and stops the exporter.
func startTracing(ctx context.Context) func() {
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		return func() {}
	}
	exporter, err := otlptracehttp.New(ctx)
	if err != nil {
		log.Fatal(err)
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithIDGenerator(workflowai.TelemetryIDGenerator{}),
		sdktrace.WithSpanProcessor(workflowai.NewTelemetrySpanProcessor(sdktrace.NewBatchSpanProcessor(exporter))),
	)
	otel.SetTracerProvider(provider)
	return func() {
		if err := provider.Shutdown(context.Background()); err != nil {
			log.Print(err)
		}
	}
}
