package workflowai

import (
	"context"

	"github.com/digitallysavvy/go-ai/pkg/provider"
	"github.com/digitallysavvy/go-ai/pkg/telemetry"
	"github.com/eriicafes/go-workflow"
	"go.opentelemetry.io/otel/propagation"
)

// traceStart starts the root span of a Stream or Generate call. A call spans
// many processes and replays, so the root span starts and ends in one inline
// step, and the call passes the trace context the step records to every step
// through ctx.
type traceStart struct {
	settings *telemetry.Options
	model    provider.LanguageModel
}

// traceStartInput is the input of the traceStart inline step.
type traceStartInput struct {
	Operation    string `json:"operation"`
	Instructions string `json:"instructions,omitempty"`
}

// run starts and ends the root span and returns its W3C traceparent, or ""
// when no telemetry integration records it.
func (t traceStart) run(ctx context.Context, input traceStartInput) (string, error) {
	callID := workflow.StepID(ctx)
	ctx = telemetry.FireOnStart(ctx, telemetry.TelemetryStartEvent{
		CallID:        callID,
		OperationType: input.Operation,
		ModelProvider: t.model.Provider(),
		ModelID:       t.model.ModelID(),
		Settings:      t.settings,
		System:        input.Instructions,
	})
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	telemetry.FireOnEnd(ctx, telemetry.TelemetryFinishEvent{
		CallID:        callID,
		OperationType: input.Operation,
		ModelProvider: t.model.Provider(),
		ModelID:       t.model.ModelID(),
		Settings:      t.settings,
	})
	return carrier["traceparent"], nil
}
