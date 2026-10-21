package workflow_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/testworld"
)

type webhookInput struct{}
type webhookOutput struct{ Value int }
type webhookRequestInput struct{}
type webhookRequestOutput struct {
	Method string
	Header string
	Body   string
}
type webhookSerializationInput struct{}
type webhookSerializationOutput struct {
	Token string
	URL   string
}
type webhookResponseInput struct{}
type webhookResponseOutput struct{ Rejected bool }

type webhookWorld struct {
	*testworld.World
	baseURL string
}

func (w *webhookWorld) BaseURL() string { return w.baseURL }

func (webhookInput) Workflow(webhookOutput)                           {}
func (webhookRequestInput) Workflow(webhookRequestOutput)             {}
func (webhookSerializationInput) Workflow(webhookSerializationOutput) {}
func (webhookResponseInput) Workflow(webhookResponseOutput)           {}

func TestCreateWebhookRequiresWorkflowExecution(t *testing.T) {
	_, err := workflow.CreateWebhook[struct{}](context.Background())
	if !errors.Is(err, workflow.ErrNoWorkflow) {
		t.Fatalf("CreateWebhook error = %v, want ErrNoWorkflow", err)
	}
}

func TestCreateWebhookRejectsLargeResponse(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, _ webhookInput) (webhookOutput, error) {
		_, err := workflow.CreateWebhook[webhookOutput](ctx, workflow.WebhookOption{
			Response: &http.Response{Body: io.NopCloser(bytes.NewReader(make([]byte, 128<<10+1)))},
		})
		return webhookOutput{}, err
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, webhookInput{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Value(ctx); err == nil {
		t.Fatal("CreateWebhook accepted an oversized response body")
	}
}

func TestWebhookUsesDurableHookAndPublicHandler(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, _ webhookInput) (webhookOutput, error) {
		webhook, err := workflow.CreateWebhook[webhookOutput](ctx, workflow.WebhookOption{Method: http.MethodPost})
		if err != nil {
			return webhookOutput{}, err
		}
		request, err := webhook.Request(ctx)
		if err != nil {
			return webhookOutput{}, err
		}
		return request.JSON()
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, webhookInput{})
	if err != nil {
		t.Fatal(err)
	}
	hook := waitForWebhook(t, ctx, world)
	response := httptest.NewRecorder()
	worlds.HTTPHandler(world, "").ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/workflow/webhooks/"+hook.Token, bytes.NewBufferString(`{"Value":42}`)))
	if response.Code != http.StatusAccepted {
		t.Fatalf("webhook response = %d", response.Code)
	}
	output, err := run.Value(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if output.Value != 42 {
		t.Fatalf("output = %#v", output)
	}
}

func TestManualWebhookRespondWithWritesTheDurableResponse(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, _ webhookInput) (webhookOutput, error) {
		webhook, err := workflow.CreateWebhook[webhookOutput](ctx, workflow.WebhookOption{Manual: true})
		if err != nil {
			return webhookOutput{}, err
		}
		request, err := webhook.Request(ctx)
		if err != nil {
			return webhookOutput{}, err
		}
		return workflow.Step(ctx, func(ctx context.Context, request *workflow.WebhookRequest[webhookOutput]) (webhookOutput, error) {
			if err := request.RespondWith(ctx, &http.Response{StatusCode: http.StatusCreated, Header: http.Header{"X-Workflow": {"done"}}, Body: io.NopCloser(bytes.NewBufferString("accepted"))}); err != nil {
				return webhookOutput{}, err
			}
			return request.JSON()
		}, request)
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, webhookInput{})
	if err != nil {
		t.Fatal(err)
	}
	hook := waitForWebhook(t, ctx, world)
	response := httptest.NewRecorder()
	worlds.HTTPHandler(world, "").ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/workflow/webhooks/"+hook.Token, bytes.NewBufferString(`{"Value":42}`)))
	if response.Code != http.StatusCreated || response.Header().Get("X-Workflow") != "done" || response.Body.String() != "accepted" {
		t.Fatalf("manual response = %d %q %q", response.Code, response.Header().Get("X-Workflow"), response.Body.String())
	}
	if _, err := run.Value(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestWebhookRequestSurvivesStepSerializationAndResponseRetry(t *testing.T) {
	var attempts atomic.Int32
	world := testworld.New(workflow.Use(func(ctx context.Context, _ webhookRequestInput) (webhookRequestOutput, error) {
		webhook, err := workflow.CreateWebhook[webhookOutput](ctx, workflow.WebhookOption{Manual: true, Method: http.MethodPost})
		if err != nil {
			return webhookRequestOutput{}, err
		}
		defer webhook.Dispose(ctx)
		request, err := webhook.Request(ctx)
		if err != nil {
			return webhookRequestOutput{}, err
		}
		if !errors.Is(request.RespondWith(ctx, nil), workflow.ErrNotInStep) {
			return webhookRequestOutput{}, errors.New("workflow response was accepted")
		}
		return workflow.Step(ctx, func(ctx context.Context, request *workflow.WebhookRequest[webhookOutput]) (webhookRequestOutput, error) {
			attempt := attempts.Add(1)
			body, err := io.ReadAll(request.Body)
			if err != nil {
				return webhookRequestOutput{}, err
			}
			for range 2 {
				value, err := request.JSON()
				if err != nil || value.Value != 42 {
					return webhookRequestOutput{}, workflow.FatalError(errors.New("JSON consumed the request body"))
				}
			}
			if err := request.RespondWith(ctx, &http.Response{StatusCode: http.StatusCreated, Body: io.NopCloser(bytes.NewBufferString("accepted"))}); err != nil {
				return webhookRequestOutput{}, err
			}
			if attempt == 1 {
				return webhookRequestOutput{}, errors.New("retry after committed response")
			}
			var fatal worlds.FatalError
			if err := request.RespondWith(ctx, &http.Response{StatusCode: http.StatusBadRequest}); !errors.As(err, &fatal) {
				return webhookRequestOutput{}, errors.New("different response was accepted")
			}
			return webhookRequestOutput{Method: request.Method, Header: request.Header.Get("X-Request-ID"), Body: string(body)}, nil
		}, request)
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), 3*time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, webhookRequestInput{})
	if err != nil {
		t.Fatal(err)
	}
	hook := waitForWebhook(t, ctx, world)
	request := httptest.NewRequest(http.MethodPost, "/v1/workflow/webhooks/"+hook.Token, bytes.NewBufferString(`{"Value":42}`))
	request.Header.Set("X-Request-ID", "serialized")
	response := httptest.NewRecorder()
	worlds.HTTPHandler(world, "").ServeHTTP(response, request)
	if response.Code != http.StatusCreated || response.Body.String() != "accepted" {
		t.Fatalf("response=%d %s", response.Code, response.Body.String())
	}
	output, err := run.Value(ctx)
	if err != nil || output.Method != http.MethodPost || output.Header != "serialized" || output.Body != `{"Value":42}` || attempts.Load() != 2 {
		t.Fatalf("output=%#v err=%v attempts=%d", output, err, attempts.Load())
	}
	page, err := world.EventsListByCorrelationID(ctx, worlds.EventsListByCorrelationIDInput{RunID: worlds.RunID(run.ID()), CorrelationID: string(hook.HookID)})
	if err != nil {
		t.Fatal(err)
	}
	responses := 0
	for _, event := range page.Data {
		payload, ok := worlds.DecodeWebhookReceived(event.Data)
		if event.EventType == worlds.EventTypeHookReceived && ok && payload.Response != nil {
			responses++
		}
	}
	if responses != 1 {
		t.Fatalf("response events=%d", responses)
	}
}

func TestWebhookRequestReconstructsMethodHeadersAndBody(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, _ webhookRequestInput) (webhookRequestOutput, error) {
		webhook, err := workflow.CreateWebhook[struct{}](ctx, workflow.WebhookOption{Method: http.MethodPatch})
		if err != nil {
			return webhookRequestOutput{}, err
		}
		request, err := webhook.Request(ctx)
		if err != nil {
			return webhookRequestOutput{}, err
		}
		body, err := io.ReadAll(request.Body)
		if err != nil {
			return webhookRequestOutput{}, err
		}
		return webhookRequestOutput{Method: request.Method, Header: request.Header.Get("X-Request-ID"), Body: string(body)}, nil
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, webhookRequestInput{})
	if err != nil {
		t.Fatal(err)
	}
	hook := waitForWebhook(t, ctx, world)
	request := httptest.NewRequest(http.MethodPatch, "/v1/workflow/webhooks/"+hook.Token, bytes.NewBufferString("request body"))
	request.Header.Set("X-Request-ID", "request-1")
	response := httptest.NewRecorder()
	worlds.HTTPHandler(world, "").ServeHTTP(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("webhook response = %d", response.Code)
	}
	output, err := run.Value(ctx)
	if err != nil || output.Method != http.MethodPatch || output.Header != "request-1" || output.Body != "request body" {
		t.Fatalf("request output = %#v, %v", output, err)
	}
}

func TestWebhookDeclarationUsesPublicBaseURL(t *testing.T) {
	runner := workflow.Use(func(ctx context.Context, _ webhookSerializationInput) (webhookSerializationOutput, error) {
		webhook, err := workflow.CreateWebhook[webhookOutput](ctx)
		if err != nil {
			return webhookSerializationOutput{}, err
		}
		return webhookSerializationOutput{Token: webhook.Token(), URL: webhook.URL()}, nil
	})
	world := &webhookWorld{
		World:   testworld.NewManual(runner),
		baseURL: "https://webhooks.example.com/v1/workflow",
	}
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, webhookSerializationInput{})
	if err != nil {
		t.Fatal(err)
	}
	queued := world.QueuedMessages()
	if len(queued) != 1 {
		t.Fatalf("queued messages = %#v", queued)
	}
	if err := worlds.NewRuntime(world, worlds.Runners(runner)).Execute(ctx, queued[0].Input); err != nil {
		t.Fatal(err)
	}
	output, err := run.Value(ctx)
	if err != nil || output.Token == "" || output.URL != "https://webhooks.example.com/v1/workflow/webhooks/"+output.Token {
		t.Fatalf("round trip = %#v, %v", output, err)
	}
}

func TestRespondWithRequiresStepContext(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, _ webhookResponseInput) (webhookResponseOutput, error) {
		var request workflow.WebhookRequest[webhookOutput]
		return webhookResponseOutput{Rejected: errors.Is(request.RespondWith(ctx, nil), workflow.ErrNotInStep)}, nil
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, webhookResponseInput{})
	if err != nil {
		t.Fatal(err)
	}
	output, err := run.Value(ctx)
	if err != nil || !output.Rejected {
		t.Fatalf("automatic webhook response = %#v, %v", output, err)
	}
}

func TestManualWebhookRespondWithRejectsRequestsThatHaveNotArrived(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, _ webhookResponseInput) (webhookResponseOutput, error) {
		return workflow.Step(ctx, func(ctx context.Context, request *workflow.WebhookRequest[webhookOutput]) (webhookResponseOutput, error) {
			return webhookResponseOutput{Rejected: request.RespondWith(ctx, nil) != nil}, nil
		}, &workflow.WebhookRequest[webhookOutput]{})
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, webhookResponseInput{})
	if err != nil {
		t.Fatal(err)
	}
	output, err := run.Value(ctx)
	if err != nil || !output.Rejected {
		t.Fatalf("early manual webhook response = %#v, %v", output, err)
	}
}

func TestWebhookJSONRejectsMalformedPayload(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, _ webhookInput) (webhookOutput, error) {
		webhook, err := workflow.CreateWebhook[webhookOutput](ctx)
		if err != nil {
			return webhookOutput{}, err
		}
		request, err := webhook.Request(ctx)
		if err != nil {
			return webhookOutput{}, err
		}
		return request.JSON()
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, webhookInput{})
	if err != nil {
		t.Fatal(err)
	}
	hook := waitForWebhook(t, ctx, world)
	response := httptest.NewRecorder()
	worlds.HTTPHandler(world, "").ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/workflow/webhooks/"+hook.Token, bytes.NewBufferString("not JSON")))
	if response.Code != http.StatusAccepted {
		t.Fatalf("webhook response = %d", response.Code)
	}
	if _, err := run.Value(ctx); err == nil {
		t.Fatal("WebhookRequest.JSON accepted malformed JSON")
	}
}

func waitForWebhook(t *testing.T, ctx context.Context, world worlds.World) worlds.Hook {
	t.Helper()
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		page, err := world.HooksList(ctx, worlds.HooksListInput{})
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Data) != 0 {
			return page.Data[0]
		}
	}
	t.Fatal("webhook was not registered")
	return worlds.Hook{}
}
