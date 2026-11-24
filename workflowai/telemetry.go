package workflowai

import (
	"context"

	"github.com/digitallysavvy/go-ai/pkg/telemetry"
	"github.com/eriicafes/go-workflow"
	"go.opentelemetry.io/otel/propagation"
)

// traceStartInput is the input of the traceStartStep inline step.
type traceStartInput struct {
	Operation    string `json:"operation"`
	Instructions string `json:"instructions,omitempty"`
}

// traceStartStep starts and ends the root span of a Stream or Generate call
// and returns its W3C traceparent, or "" when no telemetry integration
// records it. A call spans many processes and replays, so the root span
// starts and ends in one inline step, and the call passes the trace context
// the step records to every step through ctx. The model and telemetry
// settings come from the modelCall in ctx.
func traceStartStep(ctx context.Context, input traceStartInput) (string, error) {
	t, err := modelCallFrom(ctx)
	if err != nil {
		return "", err
	}
	callID := workflow.StepID(ctx)
	ctx = telemetry.FireOnStart(ctx, telemetry.TelemetryStartEvent{
		CallID:        callID,
		OperationType: input.Operation,
		ModelProvider: t.model.Provider(),
		ModelID:       t.model.ModelID(),
		Settings:      t.telemetry,
		System:        input.Instructions,
	})
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	telemetry.FireOnEnd(ctx, telemetry.TelemetryFinishEvent{
		CallID:        callID,
		OperationType: input.Operation,
		ModelProvider: t.model.Provider(),
		ModelID:       t.model.ModelID(),
		Settings:      t.telemetry,
	})
	return carrier["traceparent"], nil
}
