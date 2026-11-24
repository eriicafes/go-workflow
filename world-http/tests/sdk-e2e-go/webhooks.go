package sdke2e

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	workflow "github.com/eriicafes/go-workflow"
)

// A Go webhook request's body is a stream of the run, which only a step can
// read, so the fixtures read it in a step where TypeScript reads it in the
// workflow.

type webhookPayload struct {
	URL    string `json:"url"`
	Method string `json:"method"`
	Body   string `json:"body"`
}

type webhookWorkflow struct {
	workflow.Struct[[]webhookPayload]
}

func webhookRun(ctx context.Context, _ webhookWorkflow) ([]webhookPayload, error) {
	// Every webhook is created before the test sends requests to them.
	withDefault, err := workflow.CreateWebhook[any](ctx)
	if err != nil {
		return nil, err
	}
	withStatic, err := workflow.CreateWebhook[any](ctx, workflow.WebhookOption{Response: &http.Response{
		StatusCode: http.StatusPaymentRequired,
		Body:       io.NopCloser(strings.NewReader("Hello from static response!")),
	}})
	if err != nil {
		return nil, err
	}
	withManual, err := workflow.CreateWebhook[any](ctx, workflow.WebhookOption{Manual: true})
	if err != nil {
		return nil, err
	}
	var payloads []webhookPayload
	for _, webhook := range []struct {
		hook *workflow.Webhook[any]
		step func(context.Context, *workflow.WebhookRequest[any]) (string, error)
	}{{withDefault, readWebhookBody}, {withStatic, readWebhookBody}, {withManual, sendWebhookResponse}} {
		request, err := webhook.hook.Request(ctx)
		if err != nil {
			return nil, err
		}
		body, err := workflow.Step(ctx, webhook.step, request)
		if err != nil {
			return nil, err
		}
		payloads = append(payloads, webhookPayload{URL: request.URL, Method: request.Method, Body: body})
	}
	return payloads, nil
}

type parallelStepsThenWebhookWorkflow struct {
	workflow.Struct[[]string]
	workflow.Args
	Iterations int
}

func parallelStepsThenWebhookRun(ctx context.Context, input parallelStepsThenWebhookWorkflow) ([]string, error) {
	tokens := []string{}
	for i := range input.Iterations {
		a := workflow.Background(ctx, parallelHookRaceStep, fmt.Sprintf("%d-a", i))
		b := workflow.Background(ctx, parallelHookRaceStep, fmt.Sprintf("%d-b", i))
		if err := workflow.Join(ctx, a, b); err != nil {
			return nil, err
		}
		body, token, err := awaitWebhook(ctx)
		if err != nil {
			return nil, err
		}
		tokens = append(tokens, token)
		if body != "body-"+token {
			return nil, workflow.FatalError(fmt.Errorf("iteration %d: expected body-%s, got %s", i, token, body))
		}
	}
	return tokens, nil
}

// awaitWebhook creates a webhook, and returns the body of its first
// request and its token. It disposes the webhook when it returns.
func awaitWebhook(ctx context.Context) (string, string, error) {
	webhook, err := workflow.CreateWebhook[any](ctx)
	if err != nil {
		return "", "", err
	}
	defer webhook.Dispose(ctx)
	request, err := webhook.Request(ctx)
	if err != nil {
		return "", "", err
	}
	body, err := workflow.Step(ctx, readWebhookBody, request)
	return body, webhook.Token(), err
}

func readWebhookBody(ctx context.Context, request *workflow.WebhookRequest[any]) (string, error) {
	body, err := request.Body(ctx)
	return string(body), err
}

func sendWebhookResponse(ctx context.Context, request *workflow.WebhookRequest[any]) (string, error) {
	body, err := request.Body(ctx)
	if err != nil {
		return "", err
	}
	response := &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("Hello from webhook!"))}
	return string(body), request.RespondWith(ctx, response)
}

func parallelHookRaceStep(ctx context.Context, label string) (string, error) {
	// Matching delays make both steps likely to complete together.
	if err := wait(ctx, 50*time.Millisecond); err != nil {
		return "", err
	}
	return label, nil
}
