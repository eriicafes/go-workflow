package worlds

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"
	"time"
)

const (
	defaultHTTPPrefix           = "/v1/workflow"
	webhookRequestMaxBytes      = 128 << 10
	webhookResponsePollInterval = 50 * time.Millisecond
)

// HTTPHandler returns the HTTP handler that exposes world to clients and
// webhook callers. It serves RPCHandler at POST <prefix>/rpc and
// WebhookHandler at <prefix>/webhooks/{token}. An empty prefix uses
// /v1/workflow. It does not execute deliveries.
func HTTPHandler(world World, prefix string) http.Handler {
	mux := http.NewServeMux()
	if prefix == "" {
		prefix = defaultHTTPPrefix
	} else {
		prefix = "/" + strings.Trim(prefix, "/")
	}
	mux.Handle(prefix+"/webhooks/{token}", WebhookHandler(world))
	mux.Handle("POST "+prefix+"/rpc", RPCHandler(world))
	return mux
}

// RPCHandler returns the RPC endpoint for world. It accepts POST requests
// with RPCContentType bodies holding an RPCRequest and does no path routing,
// so mount it at <prefix>/rpc to match the path HTTP clients use.
func RPCHandler(world World) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		handleRPC(w, r, world)
	})
}

// WebhookHandler records a webhook request as a hook_received event and queues
// a delivery for the run that owns the webhook. It responds 404 when no active
// webhook owns the token, 405 for a method the webhook does not accept, 413
// when the body exceeds 128 KiB, and otherwise the webhook's Response, the
// response the runner records for a manual webhook, or 202 Accepted. The token
// is read from a {token} path wildcard when the handler is mounted with one,
// otherwise from the last path segment. Mount it at <prefix>/webhooks/{token}
// to match the URLs webhooks are created with.
func WebhookHandler(world World) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := r.PathValue("token")
		if token == "" {
			token = path.Base(r.URL.Path)
		}
		if token == "" || token == "/" || token == "." {
			http.Error(w, ErrHookNotFound.Error(), http.StatusNotFound)
			return
		}
		hook, err := world.HooksGetByToken(r.Context(), HooksGetByTokenInput{Token: token})
		if errors.Is(err, ErrHookNotFound) {
			http.Error(w, ErrHookNotFound.Error(), http.StatusNotFound)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if hook.Webhook == nil {
			http.Error(w, ErrHookNotFound.Error(), http.StatusNotFound)
			return
		}
		if hook.Webhook.Method != "" && r.Method != hook.Webhook.Method {
			w.Header().Set("Allow", hook.Webhook.Method)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, webhookRequestMaxBytes))
		if err != nil {
			var maxBytesError *http.MaxBytesError
			if errors.As(err, &maxBytesError) {
				http.Error(w, ErrWebhookRequestTooLarge.Error(), http.StatusRequestEntityTooLarge)
				return
			}
			http.Error(w, fmt.Errorf("workflow: read webhook body: %w", err).Error(), http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		payload, err := json.Marshal(WebhookReceivedEventData{
			Request: &WebhookRequest{
				Method: r.Method,
				URL:    r.URL.String(),
				Header: map[string][]string(r.Header.Clone()),
				Body:   body,
			},
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		result, err := world.EventsCreate(r.Context(), EventsCreateInput{
			RunID: hook.RunID,
			Event: EventsCreateEvent{EventType: EventTypeHookReceived, CorrelationID: string(hook.HookID), Data: payload},
		})
		if errors.Is(err, ErrRunTerminal) {
			http.Error(w, ErrHookNotFound.Error(), http.StatusNotFound)
			return
		}
		if errors.Is(err, ErrHookNotActive) {
			http.Error(w, ErrHookNotFound.Error(), http.StatusNotFound)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		if hook.Webhook.Manual {
			if _, err := world.Queue(r.Context(), QueueInput{RunID: hook.RunID, IdempotencyKey: string(result.EventID)}); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			response, err := waitWebhookResponse(r.Context(), world, hook.RunID, hook.HookID, result.EventID)
			if err != nil {
				http.Error(w, err.Error(), http.StatusGatewayTimeout)
				return
			}
			writeWebhookResponse(w, response)
			return
		}
		if hook.Webhook.Response != nil {
			if _, err := world.EventsCreate(r.Context(), EventsCreateInput{
				RunID: hook.RunID,
				Event: EventsCreateEvent{EventType: EventTypeHookDisposed, CorrelationID: string(hook.HookID)},
			}); err != nil {
				http.Error(w, err.Error(), http.StatusConflict)
				return
			}
			if _, err := world.Queue(r.Context(), QueueInput{RunID: hook.RunID, IdempotencyKey: string(result.EventID)}); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			writeWebhookResponse(w, *hook.Webhook.Response)
			return
		}
		if _, err := world.Queue(r.Context(), QueueInput{RunID: hook.RunID, IdempotencyKey: string(result.EventID)}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	})
}

func waitWebhookResponse(ctx context.Context, world World, runID RunID, hookID HookID, receivedEventID EventID) (WebhookResponse, error) {
	cursor := string(receivedEventID)
	for {
		result, err := world.EventsList(ctx, EventsListInput{RunID: runID, Pagination: Pagination{Cursor: cursor}})
		if err != nil {
			return WebhookResponse{}, err
		}
		for _, event := range result.Data {
			if event.EventType != EventTypeHookReceived || event.CorrelationID != string(hookID) {
				continue
			}
			var data WebhookReceivedEventData
			if err := json.Unmarshal(event.Data, &data); err != nil {
				return WebhookResponse{}, fmt.Errorf("workflow: decode manual webhook response: %w", err)
			}
			if data.Response != nil && data.ResponseTo == receivedEventID {
				return *data.Response, nil
			}
		}
		if result.HasMore {
			cursor = result.Cursor
			continue
		}
		timer := time.NewTimer(webhookResponsePollInterval)
		select {
		case <-ctx.Done():
			return WebhookResponse{}, ctx.Err()
		case <-timer.C:
		}
	}
}

func writeWebhookResponse(writer http.ResponseWriter, response WebhookResponse) {
	for key, values := range response.Header {
		for _, value := range values {
			writer.Header().Add(key, value)
		}
	}
	if response.Status == 0 {
		response.Status = http.StatusOK
	}
	writer.WriteHeader(response.Status)
	_, _ = writer.Write(response.Body)
}
