package workflow

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/eriicafes/go-workflow/worlds"
)

const webhookResponseMaxBytes = 128 << 10

// WebhookOption configures a webhook-backed hook. Webhooks always receive an
// opaque token; callers must not use a business identifier as a public token.
type WebhookOption struct {
	Method   string
	Response *http.Response
	Manual   bool
}

// CreateWebhook immediately registers a single-use webhook at the current
// workflow replay position.
func CreateWebhook[T any](ctx context.Context, options ...WebhookOption) (*Webhook[T], error) {
	execution, ok := workflowExecutionFromContext(ctx)
	if !ok {
		return nil, ErrNoWorkflow
	}
	option := WebhookOption{}
	if len(options) != 0 {
		option = options[len(options)-1]
	}
	response, err := webhookResponse(option.Response)
	if err != nil {
		return nil, fmt.Errorf("workflow: encode webhook response: %w", err)
	}
	hook := &Hook[webhookPayload]{
		token: worlds.NewHookToken(),
		webhook: &worlds.Webhook{
			Method:   option.Method,
			Manual:   option.Manual,
			Response: response,
		},
	}
	if err := hook.create(ctx); err != nil {
		return &Webhook[T]{hook: hook, baseURL: execution.World().BaseURL()}, err
	}
	return &Webhook[T]{hook: hook, baseURL: execution.World().BaseURL()}, nil
}

// Webhook receives a JSON request body of type T through its token-authorized
// HTTP route.
type Webhook[T any] struct {
	hook    *Hook[webhookPayload]
	baseURL string
}

func (w *Webhook[T]) Token() string { return w.hook.Token() }

func (w *Webhook[T]) ID() string { return w.hook.ID() }

// URL returns this webhook's absolute public URL.
func (w *Webhook[T]) URL() string {
	return strings.TrimRight(w.baseURL, "/") + "/webhooks/" + url.PathEscape(w.Token())
}

// MarshalJSON preserves the webhook declaration when it crosses a durable
// step boundary.
func (w *Webhook[T]) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Token    string          `json:"token"`
		HookID   worlds.HookID   `json:"hookId"`
		Received worlds.EventID  `json:"receivedEventId"`
		Webhook  *worlds.Webhook `json:"webhook"`
		BaseURL  string          `json:"baseURL"`
	}{
		Token:    w.hook.token,
		HookID:   w.hook.id,
		Received: w.hook.received,
		Webhook:  w.hook.webhook,
		BaseURL:  w.baseURL,
	})
}

// UnmarshalJSON restores a webhook reference passed through a durable step.
func (w *Webhook[T]) UnmarshalJSON(data []byte) error {
	var value struct {
		Token    string          `json:"token"`
		HookID   worlds.HookID   `json:"hookId"`
		Received worlds.EventID  `json:"receivedEventId"`
		Webhook  *worlds.Webhook `json:"webhook"`
		BaseURL  string          `json:"baseURL"`
	}
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	w.hook = &Hook[webhookPayload]{token: value.Token, id: value.HookID, received: value.Received, webhook: value.Webhook}
	w.baseURL = value.BaseURL
	return nil
}

func (w *Webhook[T]) Done(ctx context.Context) (bool, error) { return w.hook.Done(ctx) }

// Await suspends until the webhook receives a request, then decodes its JSON
// body as T.
func (w *Webhook[T]) Await(ctx context.Context) (T, error) {
	var value T
	payload, err := w.hook.Await(ctx)
	if err != nil {
		return value, err
	}
	if err := json.Unmarshal(payload.value, &value); err != nil {
		return value, fmt.Errorf("workflow: decode webhook body: %w", err)
	}
	return value, nil
}

func (w *Webhook[T]) Unwrap(ctx context.Context) T {
	value, err := w.Await(ctx)
	if err != nil {
		panic(err)
	}
	return value
}

// Request suspends until the webhook receives a request and reconstructs it.
func (w *Webhook[T]) Request(ctx context.Context) (*http.Request, error) {
	payload, err := w.hook.Await(ctx)
	if err != nil {
		return nil, err
	}
	if payload.Request == nil {
		return nil, fmt.Errorf("workflow: webhook was not received over HTTP")
	}
	request, err := http.NewRequestWithContext(ctx, payload.Request.Method, payload.Request.URL, bytes.NewReader(payload.Request.Body))
	if err != nil {
		return nil, fmt.Errorf("workflow: reconstruct webhook request: %w", err)
	}
	request.Header = http.Header(payload.Request.Header).Clone()
	return request, nil
}

// GetConflict registers the webhook and reports the run that owns the token.
func (w *Webhook[T]) GetConflict(ctx context.Context) (worlds.RunID, bool, error) {
	return w.hook.GetConflict(ctx)
}

func (w *Webhook[T]) Cancel(ctx context.Context) error { return w.hook.Cancel(ctx) }

// RespondWith records the response to this manual webhook in the event log.
// It is an inline durable step, so a replay returns the same response without
// writing another response event.
func (w *Webhook[T]) RespondWith(ctx context.Context, response *http.Response) error {
	execution, ok := workflowExecutionFromContext(ctx)
	if !ok {
		return ErrNoWorkflow
	}
	if w.hook.webhook == nil || !w.hook.webhook.Manual {
		return fmt.Errorf("workflow: webhook does not use a manual response")
	}
	if w.hook.id == "" || w.hook.received == "" {
		hook, err := execution.World().HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: w.Token()})
		if err != nil {
			return err
		}
		w.hook.id, w.hook.received = hook.HookID, hook.ReceivedEventID
	}
	if w.hook.received == "" {
		return fmt.Errorf("workflow: webhook was not received")
	}
	value, err := webhookResponse(response)
	if err != nil {
		return fmt.Errorf("workflow: encode webhook response: %w", err)
	}
	result := worlds.DurableInlineStep(ctx, worlds.DurableInlineStepOptions{
		Execution: execution,
		Position:  execution.NextPosition(),
		Name:      "webhook.respondWith",
	}, func(ctx context.Context) (struct{}, error) {
		data, err := json.Marshal(worlds.WebhookReceivedEventData{Response: value, ResponseTo: w.hook.received})
		if err != nil {
			return struct{}{}, err
		}
		event := worlds.EventsCreateEvent{
			EventType:     worlds.EventTypeHookReceived,
			CorrelationID: string(w.hook.id),
			Data:          data,
		}
		eventCount := execution.EventCount()
		created, err := execution.World().EventsCreate(ctx, worlds.EventsCreateInput{
			RunID:   execution.Run().RunID,
			Event:   event,
			Options: worlds.EventsCreateOptions{EventCount: &eventCount},
		})
		if err != nil {
			return struct{}{}, err
		}
		execution.Merge(created, worlds.Event{
			RunID:         execution.Run().RunID,
			EventID:       created.EventID,
			EventType:     event.EventType,
			CorrelationID: event.CorrelationID,
			Data:          event.Data,
		})
		return struct{}{}, nil
	})
	return result.Err
}

type webhookPayload struct {
	Request *worlds.WebhookRequest
	value   json.RawMessage
}

func (p *webhookPayload) UnmarshalJSON(data []byte) error {
	var received worlds.WebhookReceivedEventData
	if err := json.Unmarshal(data, &received); err != nil {
		return err
	}
	if received.Request != nil {
		p.Request = received.Request
		p.value = bytes.Clone(received.Request.Body)
		return nil
	}
	p.Request = nil
	p.value = bytes.Clone(data)
	return nil
}

func webhookResponse(response *http.Response) (*worlds.WebhookResponse, error) {
	if response == nil {
		return nil, nil
	}
	value := &worlds.WebhookResponse{
		Status: response.StatusCode,
		Header: map[string][]string(response.Header.Clone()),
	}
	if response.Body == nil {
		return value, nil
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, webhookResponseMaxBytes+1))
	if err != nil {
		return nil, err
	}
	if len(body) > webhookResponseMaxBytes {
		return nil, fmt.Errorf("workflow: webhook response body exceeds %d bytes", webhookResponseMaxBytes)
	}
	value.Body = body
	return value, nil
}
