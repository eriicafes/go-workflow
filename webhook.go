package workflow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
// same webhook. The last of options applies. It returns ErrNoWorkflow when ctx
// does not belong to a run.
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
	hook.create(execution, execution.NextPosition(), hook.token)
	return &Webhook[T]{hook: hook, baseURL: execution.World().BaseURL()}, nil
}

// Webhook is a hook resumed by an HTTP request to its URL, whose JSON request
// body is decoded as T.
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

func (w *Webhook[T]) settlementID() (string, bool) { return w.hook.settlementID() }
func (w *Webhook[T]) done(ctx context.Context) (bool, error, error) {
	return w.hook.done(ctx)
}

// Request waits for the HTTP request and preserves its origin for a step response.
func (w *Webhook[T]) Request(ctx context.Context) (*WebhookRequest[T], error) {
	payload, err := w.hook.Next(ctx)
	if err != nil {
		return nil, err
	}
	if payload.Request == nil {
		return nil, fmt.Errorf("workflow: webhook was not received over HTTP")
	}
	request, err := http.NewRequest(payload.Request.Method, payload.Request.URL, bytes.NewReader(payload.Request.Body))
	if err != nil {
		return nil, err
	}
	request.Header = http.Header(payload.Request.Header).Clone()
	return &WebhookRequest[T]{Request: request, data: webhookRequestData{Request: *payload.Request, RunID: w.hook.executionState.Run().RunID, HookID: w.hook.id, WebhookEventID: w.hook.received, Manual: w.hook.webhook.Manual}}, nil
}

// Dispose durably deactivates the webhook endpoint and panics on failure.
func (w *Webhook[T]) Dispose(ctx context.Context) {
	w.hook.Dispose(ctx)
}

// WebhookRequest is a serializable HTTP request. Pass it to a step to inspect
// its body or send a manual response.
type WebhookRequest[T any] struct {
	*http.Request
	data webhookRequestData
}

type webhookRequestData struct {
	Request        worlds.WebhookRequest `json:"request"`
	RunID          worlds.RunID          `json:"runId"`
	HookID         worlds.HookID         `json:"hookId"`
	WebhookEventID worlds.EventID        `json:"webhookEventId"`
	Manual         bool                  `json:"manual"`
}

func (r *WebhookRequest[T]) MarshalJSON() ([]byte, error) { return json.Marshal(r.data) }
func (r *WebhookRequest[T]) UnmarshalJSON(data []byte) error {
	if err := json.Unmarshal(data, &r.data); err != nil {
		return err
	}
	request, err := http.NewRequest(r.data.Request.Method, r.data.Request.URL, bytes.NewReader(r.data.Request.Body))
	if err != nil {
		return err
	}
	request.Header = http.Header(r.data.Request.Header).Clone()
	r.Request = request
	return nil
}

// JSON decodes the stored body without consuming Request.Body.
func (r *WebhookRequest[T]) JSON() (T, error) {
	var value T
	err := json.Unmarshal(r.data.Request.Body, &value)
	return value, err
}

// RespondWith records the response for this request. It is step-only, and
// retrying the same response is idempotent.
func (r *WebhookRequest[T]) RespondWith(ctx context.Context, response *http.Response) error {
	execution, ok := stepExecutionFromContext(ctx)
	if !ok {
		return ErrNotInStep
	}
	if !r.data.Manual {
		return FatalError(fmt.Errorf("workflow: webhook does not use a manual response"))
	}
	if r.data.RunID != execution.Run().RunID || r.data.HookID == "" || r.data.WebhookEventID == "" {
		return FatalError(fmt.Errorf("workflow: invalid webhook request origin"))
	}
	value, err := webhookResponse(response)
	if err != nil {
		return FatalError(fmt.Errorf("workflow: encode webhook response: %w", err))
	}
	if value == nil {
		value = &worlds.WebhookResponse{}
	}
	payload, err := json.Marshal(worlds.WebhookReceivedEventData{Response: value, ResponseTo: r.data.WebhookEventID})
	if err != nil {
		return err
	}
	data, err := worlds.EncodeHookReceived(payload)
	if err != nil {
		return err
	}
	world := execution.World()
	_, err = world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: r.data.RunID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeHookReceived, CorrelationID: string(r.data.HookID), Data: data}})
	if err == nil {
		return nil
	}
	if !errors.Is(err, worlds.ErrHookNotActive) && !errors.Is(err, worlds.ErrRunTerminal) {
		return err
	}
	cursor := ""
	for {
		recorded, listErr := world.EventsListByCorrelationID(ctx, worlds.EventsListByCorrelationIDInput{RunID: r.data.RunID, CorrelationID: string(r.data.HookID), Pagination: worlds.Pagination{Cursor: cursor}})
		if listErr != nil {
			return listErr
		}
		for _, event := range recorded.Data {
			received, ok := worlds.DecodeWebhookReceived(event.Data)
			if event.EventType == worlds.EventTypeHookReceived && ok && received.Response != nil && received.ResponseTo == r.data.WebhookEventID {
				actual, _ := json.Marshal(received.Response)
				expected, _ := json.Marshal(value)
				if bytes.Equal(actual, expected) {
					return nil
				}
				return FatalError(fmt.Errorf("workflow: webhook request already has a different response"))
			}
		}
		if !recorded.HasMore {
			return FatalError(err)
		}
		cursor = recorded.Cursor
	}
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
