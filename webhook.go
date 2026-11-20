package workflow

import (
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
	// Response is sent to every request to the webhook. Its body is limited
	// to 128 KiB. When nil and Manual is false, requests receive 202
	// Accepted.
	Response *http.Response
	// Manual holds each request open until a step responds to it with
	// RespondWith.
	Manual bool
}

// CreateWebhook creates a webhook in the run ctx belongs to, as the Workflow
// SDK's createWebhook does. A webhook is a hook resumed by each HTTP request
// to its URL. On replay it returns the same webhook. The last of options
// applies. It returns ErrNoWorkflow when ctx does not belong to a run.
func CreateWebhook[T any](ctx context.Context, options ...WebhookOption) (*Webhook[T], error) {
	execution, ok := workflowExecutionFromContext(ctx)
	if !ok {
		return nil, ErrNoWorkflow
	}
	option := WebhookOption{}
	if len(options) != 0 {
		option = options[len(options)-1]
	}
	hook := &Hook[worlds.WebhookRequest]{token: worlds.NewHookToken(), isWebhook: true}
	if option.Manual || option.Response != nil {
		metadata := worlds.WebhookMetadata{Manual: option.Manual}
		if !option.Manual {
			response, err := webhookResponse(option.Response)
			if err != nil {
				return nil, fmt.Errorf("workflow: encode webhook response: %w", err)
			}
			metadata.Response = &response
		}
		encoded, err := execution.EncodePayload(metadata)
		if err != nil {
			return nil, fmt.Errorf("workflow: encode webhook metadata: %w", err)
		}
		hook.metadata = encoded
	}
	hook.create(execution, execution.NextPosition(), hook.token)
	return &Webhook[T]{hook: hook, baseURL: execution.World().BaseURL()}, nil
}

// Webhook is a hook resumed by HTTP requests to its URL, whose JSON request
// bodies decode as T.
type Webhook[T any] struct {
	hook    *Hook[worlds.WebhookRequest]
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

// Request waits for the next HTTP request to the webhook.
func (w *Webhook[T]) Request(ctx context.Context) (*WebhookRequest[T], error) {
	request, err := w.hook.Next(ctx)
	if err != nil {
		return nil, err
	}
	return newWebhookRequest[T](request), nil
}

// Dispose durably deactivates the webhook endpoint and panics on failure.
func (w *Webhook[T]) Dispose(ctx context.Context) {
	w.hook.Dispose(ctx)
}

// WebhookRequest is a request to a webhook, the Workflow SDK's Request. Its
// body is a stream of the run, so Body, JSON, and RespondWith are step-only:
// pass the request to a step to read it or respond.
type WebhookRequest[T any] struct {
	Method string
	URL    string
	Header http.Header
	value  worlds.WebhookRequest
}

func newWebhookRequest[T any](value worlds.WebhookRequest) *WebhookRequest[T] {
	header := value.Header
	if header == nil {
		header = http.Header{}
	}
	return &WebhookRequest[T]{Method: value.Method, URL: value.URL, Header: header, value: value}
}

// MarshalPayload encodes the request as the SDK's Request.
func (r WebhookRequest[T]) MarshalPayload() (any, error) { return r.value, nil }

// UnmarshalPayload decodes the SDK's Request.
func (r *WebhookRequest[T]) UnmarshalPayload(decode func(any) error) error {
	var value worlds.WebhookRequest
	if err := decode(&value); err != nil {
		return err
	}
	*r = *newWebhookRequest[T](value)
	return nil
}

// Body returns the request body. It must be called in step context.
func (r *WebhookRequest[T]) Body(ctx context.Context) ([]byte, error) {
	execution, ok := stepExecutionFromContext(ctx)
	if !ok {
		return nil, ErrNotInStep
	}
	if r.value.Body == nil {
		return []byte{}, nil
	}
	return worlds.ReadByteStream(ctx, execution.World(), execution.Run().RunID, *r.value.Body)
}

// JSON decodes the request body as T. It must be called in step context.
func (r *WebhookRequest[T]) JSON(ctx context.Context) (T, error) {
	var value T
	body, err := r.Body(ctx)
	if err != nil {
		return value, err
	}
	err = json.Unmarshal(body, &value)
	return value, err
}

// RespondWith sends response to a manual webhook's request. It is
// step-only, and the request answers with the first response sent, so a
// retried attempt's response is ignored.
func (r *WebhookRequest[T]) RespondWith(ctx context.Context, response *http.Response) error {
	execution, ok := stepExecutionFromContext(ctx)
	if !ok {
		return ErrNotInStep
	}
	if r.value.ResponseWritable == nil {
		return FatalError(fmt.Errorf("workflow: webhook does not use a manual response"))
	}
	value, err := webhookResponse(response)
	if err != nil {
		return FatalError(fmt.Errorf("workflow: encode webhook response: %w", err))
	}
	return execution.WriteWebhookResponse(ctx, r.value, value)
}

// webhookResponse returns response as the SDK's Response, its body held in
// memory. A nil response is 200 OK.
func webhookResponse(response *http.Response) (worlds.WebhookResponse, error) {
	if response == nil {
		return worlds.WebhookResponse{Status: http.StatusOK}, nil
	}
	value := worlds.WebhookResponse{Status: response.StatusCode, Header: response.Header.Clone()}
	if response.Body == nil {
		return value, nil
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, webhookResponseMaxBytes+1))
	if err != nil {
		return worlds.WebhookResponse{}, err
	}
	if len(body) > webhookResponseMaxBytes {
		return worlds.WebhookResponse{}, fmt.Errorf("workflow: webhook response body exceeds %d bytes", webhookResponseMaxBytes)
	}
	value.Body = &worlds.StreamRef{BodyInit: body}
	return value, nil
}
