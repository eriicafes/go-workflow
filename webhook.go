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

// WebhookOption holds the options to CreateWebhook. A webhook always gets a
// random hook token, since its URL is public.
type WebhookOption struct {
	// Method is the HTTP method the webhook accepts. When empty, it accepts
	// any method.
	Method string
	// Response is the response sent to the request that resumes the webhook.
	// Its body is limited to 128 KiB. When nil and Manual is false, the
	// request receives 202 Accepted.
	Response *http.Response
	// Manual holds the request open until the runner responds with
	// RespondWith.
	Manual bool
}

// CreateWebhook creates a webhook in the run ctx belongs to. A webhook is a
// hook resumed once by an HTTP request to its URL. On replay it returns the
// same webhook.
// The last of options applies. It returns ErrNoWorkflow when ctx does not
// belong to a run.
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

// Webhook is a hook resumed by an HTTP request to its URL, whose
// JSON request body is decoded as T.
type Webhook[T any] struct {
	hook    *Hook[webhookPayload]
	baseURL string
}

// Token returns the webhook's hook token, which is part of its URL.
func (w *Webhook[T]) Token() string { return w.hook.Token() }

// ID returns the hook ID.
func (w *Webhook[T]) ID() string { return w.hook.ID() }

// URL returns the webhook's absolute URL, under the World's base URL.
func (w *Webhook[T]) URL() string {
	return strings.TrimRight(w.baseURL, "/") + "/webhooks/" + url.PathEscape(w.Token())
}

// MarshalJSON encodes the webhook so it can be passed to a step as input or
// returned from one.
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

// UnmarshalJSON decodes a webhook encoded by MarshalJSON.
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

// Done reports whether the webhook has settled. It never suspends the run.
func (w *Webhook[T]) Done(ctx context.Context) (bool, error) { return w.hook.Done(ctx) }

// Await returns the request body decoded from JSON as T, suspending the run
// until the webhook receives a request. It returns the same errors as
// Hook.Await.
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

// Unwrap is like Await but panics if Await returns an error.
func (w *Webhook[T]) Unwrap(ctx context.Context) T {
	value, err := w.Await(ctx)
	if err != nil {
		panic(err)
	}
	return value
}

// Request returns the request that resumed the webhook, with its method, URL,
// header, and body, suspending the run until the webhook receives one.
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

// GetConflict is like Hook.GetConflict.
func (w *Webhook[T]) GetConflict(ctx context.Context) (worlds.RunID, bool, error) {
	return w.hook.GetConflict(ctx)
}

// Cancel disposes of the webhook so its URL no longer accepts requests.
func (w *Webhook[T]) Cancel(ctx context.Context) error { return w.hook.Cancel(ctx) }

// RespondWith sends response to the request that resumed a webhook created
// with WebhookOption.Manual. In workflow context it records the response in a
// step, so replay does not send it again. RespondWith fails when the webhook
// is not manual or has not received a request, and returns ErrNoWorkflow
// outside a run.
func (w *Webhook[T]) RespondWith(ctx context.Context, response *http.Response) error {
	if _, ok := worlds.ExecutionFromContext(ctx); !ok {
		return ErrNoWorkflow
	}
	if w.hook.webhook == nil || !w.hook.webhook.Manual {
		return fmt.Errorf("workflow: webhook does not use a manual response")
	}
	value, err := webhookResponse(response)
	if err != nil {
		return fmt.Errorf("workflow: encode webhook response: %w", err)
	}
	input := webhookRespondInput{Token: w.Token(), HookID: w.hook.id, Received: w.hook.received, Response: value}
	if _, ok := workflowExecutionFromContext(ctx); ok {
		_, err := Step(ctx, webhookRespondWith, input)
		return err
	}
	_, err = webhookRespondWith(ctx, input)
	return err
}

type webhookRespondInput struct {
	Token    string
	HookID   worlds.HookID
	Received worlds.EventID
	Response *worlds.WebhookResponse
}

func webhookRespondWith(ctx context.Context, input webhookRespondInput) (struct{}, error) {
	execution, ok := worlds.ExecutionFromContext(ctx)
	if !ok {
		return struct{}{}, ErrNoWorkflow
	}
	world, runID := execution.World(), execution.Run().RunID
	if input.HookID == "" || input.Received == "" {
		hook, err := world.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: input.Token})
		if err != nil {
			return struct{}{}, err
		}
		input.HookID, input.Received = hook.HookID, hook.ReceivedEventID
	}
	if input.Received == "" {
		return struct{}{}, fmt.Errorf("workflow: webhook was not received")
	}
	recorded, err := world.EventsListByCorrelationID(ctx, worlds.EventsListByCorrelationIDInput{RunID: runID, CorrelationID: string(input.HookID)})
	if err != nil {
		return struct{}{}, err
	}
	for _, event := range recorded.Data {
		var received worlds.WebhookReceivedEventData
		if event.EventType == worlds.EventTypeHookReceived && json.Unmarshal(event.Data, &received) == nil && received.Response != nil && received.ResponseTo == input.Received {
			return struct{}{}, nil
		}
	}
	data, err := json.Marshal(worlds.WebhookReceivedEventData{Response: input.Response, ResponseTo: input.Received})
	if err != nil {
		return struct{}{}, err
	}
	_, err = world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{
		EventType:     worlds.EventTypeHookReceived,
		CorrelationID: string(input.HookID),
		Data:          data,
	}})
	return struct{}{}, err
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
