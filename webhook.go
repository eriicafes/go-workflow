package workflow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/eriicafes/go-workflow/workflowops"
)

// WebhookOption configures a webhook-backed hook.
type WebhookOption struct {
	Token        string
	MinRetention time.Duration
	Force        bool
	Method       string
	Response     *http.Response
	Manual       bool
}

// Webhook receives a JSON request body of type T through its public URL.
type Webhook[T any] struct {
	hook   *Hook[webhookPayload]
	config *workflowops.Webhook
}

// NewWebhook creates a hook that receives a JSON request body of type T.
func NewWebhook[T any](options ...WebhookOption) *Webhook[T] {
	option := WebhookOption{}
	if len(options) != 0 {
		option = options[len(options)-1]
	}
	response, err := encodeWebhookResponse(option.Response)
	if err != nil {
		panic(fmt.Errorf("workflow: encode webhook response: %w", err))
	}
	hook := NewHook[webhookPayload](HookOption{
		Token:        option.Token,
		MinRetention: option.MinRetention,
		Force:        option.Force,
	})
	config := &workflowops.Webhook{
		Method:   option.Method,
		Response: response,
		Manual:   option.Manual,
	}
	return &Webhook[T]{hook: hook, config: config}
}

func (webhook *Webhook[T]) Token() string {
	return webhook.hook.Token()
}

// Task returns the registered webhook operation. It returns
// ErrHookNotRegistered until Value, Wait, Request, or GetConflict has
// registered the webhook in a workflow replay.
func (webhook *Webhook[T]) Task() (*workflowops.Task, error) {
	return webhook.hook.Task()
}

// URL returns the public token-authorized route served by NewHTTPHandler.
func (webhook *Webhook[T]) URL() string {
	return APIPrefix + "/webhook/" + webhook.Token()
}

// MarshalJSON preserves the token when a webhook reference is passed to a
// durable step.
func (webhook *Webhook[T]) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Token string `json:"token"`
	}{Token: webhook.Token()})
}

// UnmarshalJSON restores a webhook reference passed into a durable step.
func (webhook *Webhook[T]) UnmarshalJSON(data []byte) error {
	var value struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	webhook.hook = NewHook[webhookPayload](HookOption{Token: value.Token})
	webhook.config = &workflowops.Webhook{}
	return nil
}

// Wait registers the webhook when first reached, then waits for its payload.
func (webhook *Webhook[T]) Wait(ctx context.Context) error {
	_, err := webhook.Value(ctx)
	return err
}

// Value suspends until the webhook is resumed and decodes its JSON body as T.
func (webhook *Webhook[T]) Value(ctx context.Context) (T, error) {
	var zero T
	payload, err := webhook.hook.value(ctx, webhook.config)
	if err != nil {
		return zero, err
	}
	if err := json.Unmarshal(payload.value, &zero); err != nil {
		return zero, fmt.Errorf("workflow: decode webhook body: %w", err)
	}
	return zero, nil
}

// Request suspends until the webhook is resumed and returns its HTTP request.
func (webhook *Webhook[T]) Request(ctx context.Context) (*http.Request, error) {
	payload, err := webhook.hook.value(ctx, webhook.config)
	if err != nil {
		return nil, err
	}
	if payload.Request == nil {
		return nil, fmt.Errorf("workflow: webhook was not resumed over HTTP")
	}
	request, err := http.NewRequestWithContext(
		ctx,
		payload.Request.Method,
		payload.Request.URL,
		bytes.NewReader(payload.Request.Body),
	)
	if err != nil {
		return nil, fmt.Errorf("workflow: reconstruct webhook request: %w", err)
	}
	request.Header = payload.Request.Header.Clone()
	return request, nil
}

// GetConflict registers the webhook and reports the owning run ID when its
// token is already claimed.
func (webhook *Webhook[T]) GetConflict(ctx context.Context) (string, bool, error) {
	return webhook.hook.getConflict(ctx, webhook.config)
}

type webhookRequest struct {
	Method string      `json:"method"`
	URL    string      `json:"url"`
	Header http.Header `json:"header"`
	Body   []byte      `json:"body"`
}

type webhookPayload struct {
	Request *webhookRequest `json:"request,omitempty"`
	value   json.RawMessage
}

func (payload *webhookPayload) UnmarshalJSON(data []byte) error {
	if raw := bytes.TrimSpace(data); len(raw) != 0 && raw[0] == '{' {
		var envelope struct {
			Request *webhookRequest `json:"request"`
		}
		if err := json.Unmarshal(raw, &envelope); err != nil {
			return err
		}
		if envelope.Request != nil {
			payload.Request = envelope.Request
			payload.value = append(payload.value[:0], envelope.Request.Body...)
			return nil
		}
	}
	payload.Request = nil
	payload.value = append(payload.value[:0], data...)
	return nil
}

type webhookResponse struct {
	Status int                 `json:"status"`
	Header map[string][]string `json:"header,omitempty"`
	Body   []byte              `json:"body,omitempty"`
}

// ReplyWebhook records a response for this manual webhook. It can only be
// called by a durable step.
func (webhook *Webhook[T]) ReplyWebhook(ctx context.Context, response *http.Response) error {
	if inStep, _ := ctx.Value(stepContextKey{}).(bool); !inStep {
		return ErrNoWorkflow
	}
	value, err := encodeWebhookResponse(response)
	if err != nil {
		return fmt.Errorf("workflow: encode webhook response: %w", err)
	}
	client, _, err := clientFromContext(ctx)
	if err != nil {
		return err
	}
	return client.ReplyWebhook(ctx, webhook.Token(), value)
}

// webhookHandler exposes the fixed token route created by URL.
func webhookHandler(world World) http.Handler {
	client := Client(world)
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		token := request.PathValue("token")
		if token == "" {
			writeError(writer, http.StatusNotFound, "workflow: webhook token is required")
			return
		}
		hook, found, err := client.GetHook(request.Context(), token)
		if err != nil {
			writeError(writer, http.StatusInternalServerError, err.Error())
			return
		}
		if !found || hook.Webhook == nil {
			writeError(writer, http.StatusNotFound, ErrHookNotFound.Error())
			return
		}
		if hook.Webhook.Method != "" && request.Method != hook.Webhook.Method {
			writer.Header().Set("Allow", hook.Webhook.Method)
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(request.Body)
		if err != nil {
			writeError(writer, http.StatusBadRequest, "workflow: read webhook body")
			return
		}
		defer request.Body.Close()
		payload, err := json.Marshal(webhookPayload{
			Request: &webhookRequest{
				Method: request.Method,
				URL:    request.URL.String(),
				Header: request.Header.Clone(),
				Body:   body,
			},
		})
		if err != nil {
			writeError(writer, http.StatusInternalServerError, err.Error())
			return
		}
		if err := client.ResumeWebhook(request.Context(), token, payload); err != nil {
			if errors.Is(err, ErrHookNotFound) {
				writeError(writer, http.StatusNotFound, err.Error())
				return
			}
			writeError(writer, http.StatusInternalServerError, err.Error())
			return
		}
		if hook.Webhook.Manual {
			response, err := client.WaitWebhookResponse(request.Context(), token)
			if err != nil {
				writeError(writer, http.StatusGatewayTimeout, err.Error())
				return
			}
			writeWebhookResponse(writer, response)
			return
		}
		if len(hook.Webhook.Response) != 0 {
			writeWebhookResponse(writer, hook.Webhook.Response)
			return
		}
		writer.WriteHeader(http.StatusAccepted)
	})
}

func encodeWebhookResponse(response *http.Response) (json.RawMessage, error) {
	if response == nil {
		return nil, nil
	}
	var body []byte
	var err error
	if response.Body != nil {
		body, err = io.ReadAll(response.Body)
		if err != nil {
			return nil, err
		}
	}
	return json.Marshal(webhookResponse{
		Status: response.StatusCode,
		Header: response.Header.Clone(),
		Body:   body,
	})
}

func writeWebhookResponse(writer http.ResponseWriter, encoded json.RawMessage) {
	var response webhookResponse
	if err := json.Unmarshal(encoded, &response); err != nil {
		writeError(writer, http.StatusInternalServerError, "workflow: decode webhook response")
		return
	}
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
