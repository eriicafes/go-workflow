package worlds

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/eriicafes/go-workflow/internal/envelope"
)

// HealthCheck is a Workflow SDK health check: the probe the SDK's
// healthCheck and `workflow health` queue, which the app answers on the
// stream HealthCheckStreamName of the run HealthCheckRunID.
type HealthCheck struct {
	CorrelationID string `json:"correlationId"`
	// ProbeRunID is the run the caller is about to create, whose public key
	// the answer carries when the World encrypts it.
	ProbeRunID string `json:"probeRunId,omitempty"`
}

// ParseHealthCheck returns the health check a delivery's SDKMessage holds,
// and whether it holds one.
func ParseHealthCheck(input QueueInput) (HealthCheck, bool) {
	if input.RunID != "" || len(input.SDKMessage) == 0 {
		return HealthCheck{}, false
	}
	var message struct {
		Message struct {
			HealthCheck   bool    `json:"__healthCheck"`
			CorrelationID *string `json:"correlationId"`
			ProbeRunID    string  `json:"probeRunId"`
			// RunID is the legacy spelling of ProbeRunID.
			RunID string `json:"runId"`
		} `json:"message"`
	}
	if json.Unmarshal(input.SDKMessage, &message) != nil || !message.Message.HealthCheck || message.Message.CorrelationID == nil {
		return HealthCheck{}, false
	}
	check := HealthCheck{CorrelationID: *message.Message.CorrelationID, ProbeRunID: message.Message.ProbeRunID}
	if check.ProbeRunID == "" {
		check.ProbeRunID = message.Message.RunID
	}
	return check, true
}

// AnswerHealthCheck writes the answer to check to world and closes its
// stream, as the Workflow SDK's handleHealthCheckMessage does: a JSON object
// with healthy, correlationId, specVersion (SpecVersion), timestamp, and,
// for a ProbeRunID of a World that encrypts runs, the run's
// encryptionPublicKey.
func AnswerHealthCheck(ctx context.Context, world World, check HealthCheck) error {
	answer := map[string]any{
		"healthy":       true,
		"correlationId": check.CorrelationID,
		"specVersion":   SpecVersion,
		"timestamp":     time.Now().UnixMilli(),
	}
	if check.ProbeRunID != "" {
		// The key is best-effort: the caller falls back to a key lookup.
		keys, err := RunKeysFor(ctx, world, RunID(check.ProbeRunID), world.DeploymentID())
		switch {
		case err != nil:
			log.Printf("workflow: health check %s could not derive a run public key; the caller will fall back to a key lookup: %v", check.CorrelationID, err)
		case keys != nil:
			answer["encryptionPublicKey"] = envelope.EncodePublicKey(keys.PublicKey)
		}
	}
	data, err := json.Marshal(answer)
	if err != nil {
		return err
	}
	runID, name := HealthCheckRunID(check.CorrelationID), HealthCheckStreamName(check.CorrelationID)
	if err := world.StreamsWrite(ctx, StreamsWriteInput{RunID: runID, Name: name, Chunk: data}); err != nil {
		return err
	}
	return world.StreamsClose(ctx, StreamsCloseInput{RunID: runID, Name: name})
}

// HealthCheckRunID returns the run ID a health check's answer is written
// under: wrun_hc_ and its correlation ID, which names no real run.
func HealthCheckRunID(correlationID string) RunID { return RunID("wrun_hc_" + correlationID) }

// HealthCheckStreamName returns the name of a health check's answer stream.
func HealthCheckStreamName(correlationID string) string {
	return "__health_check__" + correlationID
}

// WithHealthCheck wraps handler, an app's flow route, to answer a request
// with a __health query parameter as the Workflow SDK's withHealthCheck
// does: 200 with the JSON object {healthy, endpoint, specVersion}, endpoint
// the request's path, with CORS headers that let another origin check, and
// 204 to an OPTIONS preflight. A nil handler answers other requests with 404.
func WithHealthCheck(handler http.Handler) http.Handler {
	if handler == nil {
		handler = http.NotFoundHandler()
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !r.URL.Query().Has("__health") {
			handler.ServeHTTP(w, r)
			return
		}
		header := w.Header()
		header.Set("Access-Control-Allow-Origin", "*")
		header.Set("Access-Control-Allow-Methods", "POST, OPTIONS, GET, HEAD")
		header.Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		header.Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"healthy": true, "endpoint": r.URL.Path, "specVersion": SpecVersion})
	})
}
