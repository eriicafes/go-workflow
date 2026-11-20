package workflow_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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

// readJSON is a step that decodes a webhook request's body.
func readJSON(ctx context.Context, request *workflow.WebhookRequest[webhookOutput]) (webhookOutput, error) {
	return request.JSON(ctx)
}

func TestWebhookTakesEveryRequest(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, _ webhookInput) (webhookOutput, error) {
		webhook, err := workflow.CreateWebhook[webhookOutput](ctx)
		if err != nil {
			return webhookOutput{}, err
		}
		sum := 0
		for range 2 {
			request, err := webhook.Request(ctx)
			if err != nil {
				return webhookOutput{}, err
			}
			// The body is a stream of the run, read in a step.
			if _, err := request.JSON(ctx); !errors.Is(err, workflow.ErrNotInStep) {
				return webhookOutput{}, fmt.Errorf("JSON in workflow context: %v", err)
			}
			value, err := workflow.Step(ctx, readJSON, request)
			if err != nil {
				return webhookOutput{}, err
			}
			sum += value.Value
		}
		return webhookOutput{Value: sum}, nil
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), 2*time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, webhookInput{})
	if err != nil {
		t.Fatal(err)
	}
	hook := waitForWebhook(t, ctx, world)
	for _, body := range []string{`{"Value":40}`, `{"Value":2}`} {
		response := httptest.NewRecorder()
		worlds.HTTPHandler(world, "").ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/workflow/webhooks/"+hook.Token, bytes.NewBufferString(body)))
		if response.Code != http.StatusAccepted {
			t.Fatalf("webhook response = %d %s", response.Code, response.Body)
		}
	}
	output, err := run.Value(ctx)
	if err != nil || output.Value != 42 {
		t.Fatalf("output = %#v err=%v", output, err)
	}
}

func TestStaticWebhookResponseAnswersEveryRequest(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, _ webhookInput) (webhookOutput, error) {
		webhook, err := workflow.CreateWebhook[webhookOutput](ctx, workflow.WebhookOption{
			Response: &http.Response{StatusCode: http.StatusOK, Header: http.Header{"X-Static": {"yes"}}, Body: io.NopCloser(bytes.NewBufferString("thanks"))},
		})
		if err != nil {
			return webhookOutput{}, err
		}
		for range 2 {
			if _, err := webhook.Request(ctx); err != nil {
				return webhookOutput{}, err
			}
		}
		return webhookOutput{Value: 2}, nil
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), 2*time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, webhookInput{})
	if err != nil {
		t.Fatal(err)
	}
	hook := waitForWebhook(t, ctx, world)
	for range 2 {
		response := httptest.NewRecorder()
		worlds.HTTPHandler(world, "").ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/workflow/webhooks/"+hook.Token, nil))
		if response.Code != http.StatusOK || response.Header().Get("X-Static") != "yes" || response.Body.String() != "thanks" {
			t.Fatalf("static response = %d %v %q", response.Code, response.Header(), response.Body)
		}
	}
	if output, err := run.Value(ctx); err != nil || output.Value != 2 {
		t.Fatalf("output = %#v err=%v", output, err)
	}
}

func TestManualWebhookRespondWithAnswersTheRequest(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(fmt.Sprintf("encrypted=%v", encrypted), func(t *testing.T) { testManualWebhook(t, encrypted) })
	}
}

func testManualWebhook(t *testing.T, encrypted bool) {
	var attempts atomic.Int32
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
			// A retried attempt's response is ignored.
			if attempts.Add(1) == 1 {
				return webhookOutput{}, errors.New("retry")
			}
			return request.JSON(ctx)
		}, request, workflow.StepOptions{MaxRetries: 1})
	}))
	defer world.Close()
	if encrypted {
		if err := world.SetEncryptionKey(bytes.Repeat([]byte{5}, 32)); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), 3*time.Second)
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
	if output, err := run.Value(ctx); err != nil || output.Value != 42 {
		t.Fatalf("output = %#v err=%v", output, err)
	}
}

func TestWebhookRequestKeepsMethodURLAndHeaders(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, _ webhookRequestInput) (webhookRequestOutput, error) {
		webhook, err := workflow.CreateWebhook[webhookOutput](ctx)
		if err != nil {
			return webhookRequestOutput{}, err
		}
		request, err := webhook.Request(ctx)
		if err != nil {
			return webhookRequestOutput{}, err
		}
		return workflow.Step(ctx, func(ctx context.Context, request *workflow.WebhookRequest[webhookOutput]) (webhookRequestOutput, error) {
			body, err := request.Body(ctx)
			return webhookRequestOutput{Method: request.Method, Header: request.Header.Get("X-Test"), Body: string(body)}, err
		}, request)
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), 2*time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, webhookRequestInput{})
	if err != nil {
		t.Fatal(err)
	}
	hook := waitForWebhook(t, ctx, world)
	request := httptest.NewRequest(http.MethodPut, "/v1/workflow/webhooks/"+hook.Token+"?a=b", bytes.NewBufferString("raw body"))
	request.Header.Set("X-Test", "header")
	worlds.HTTPHandler(world, "").ServeHTTP(httptest.NewRecorder(), request)
	output, err := run.Value(ctx)
	if err != nil || output.Method != http.MethodPut || output.Header != "header" || output.Body != "raw body" {
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

func TestRespondWithRejectsAWebhookWithoutManualResponses(t *testing.T) {
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
		t.Fatalf("response to a webhook without manual responses = %#v, %v", output, err)
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
		return workflow.Step(ctx, readJSON, request, workflow.StepOptions{MaxRetries: 0})
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
