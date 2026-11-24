package flows

import (
	"cmp"
	"errors"
	"os"

	"github.com/digitallysavvy/go-ai/pkg/provider"
	"github.com/digitallysavvy/go-ai/pkg/providers/anthropic"
	"github.com/digitallysavvy/go-ai/pkg/providers/gateway"
	"github.com/digitallysavvy/go-ai/pkg/telemetry"
	"github.com/eriicafes/go-workflow/workflowai"
)

// LanguageModel returns the model named by MODEL through the AI Gateway when
// AI_GATEWAY_API_KEY is set, and otherwise through the Anthropic API. Chat
// builds its model with it on every replay, so the model never needs to be
// stored or serialized.
func LanguageModel() (provider.LanguageModel, error) {
	if os.Getenv("AI_GATEWAY_API_KEY") != "" {
		p, err := gateway.New(gateway.Config{})
		if err != nil {
			return nil, err
		}
		return p.LanguageModel(cmp.Or(os.Getenv("MODEL"), "anthropic/claude-sonnet-5.5"))
	}
	if key := os.Getenv("ANTHROPIC_API_KEY"); key != "" {
		return anthropic.New(anthropic.Config{APIKey: key}).LanguageModel(cmp.Or(os.Getenv("MODEL"), anthropic.ClaudeSonnet5_5))
	}
	return nil, errors.New("set AI_GATEWAY_API_KEY or ANTHROPIC_API_KEY")
}

// toolApprovalSecret names the environment variable whose secret signs tool
// approval requests, or returns nil to leave them unsigned when it is unset.
func toolApprovalSecret() *workflowai.ToolApprovalSecret {
	if os.Getenv("TOOL_APPROVAL_SECRET") == "" {
		return nil
	}
	return &workflowai.ToolApprovalSecret{EnvironmentVariable: "TOOL_APPROVAL_SECRET"}
}

// agentTelemetry records the agent's spans with go-ai's OpenTelemetry
// integration when OTEL_EXPORTER_OTLP_ENDPOINT is set, or returns nil to
// record none.
func agentTelemetry() *telemetry.Options {
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		return nil
	}
	return &telemetry.Options{
		FunctionID:   "flight-agent",
		Integrations: []telemetry.TelemetryIntegration{telemetry.NewOpenTelemetry(telemetry.OpenTelemetryOptions{})},
	}
}
