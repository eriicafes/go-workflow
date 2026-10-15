package worlds_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/testworld"
)

type clientInput struct{ Value int }
type clientOutput struct{ Value int }

func (clientInput) Workflow(clientOutput) {}

func TestNewClientExecutesWorkflowAndDurableStep(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, input clientInput) (clientOutput, error) {
		value, err := workflow.Step(ctx, func(_ context.Context, value int) (int, error) { return value + 1, nil }, input.Value)
		return clientOutput{Value: value}, err
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, clientInput{Value: 41})
	if err != nil {
		t.Fatal(err)
	}
	output, err := run.Await(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if output.Value != 42 {
		t.Fatalf("output = %#v", output)
	}
}

func TestNewHTTPClientExecutesWorkflow(t *testing.T) {
	world := testworld.New(workflow.Use(func(_ context.Context, input clientInput) (clientOutput, error) {
		return clientOutput{Value: input.Value}, nil
	}))
	defer world.Close()
	client := workflow.NewHTTPClient("http://world/v1/workflow", newInMemoryHTTPClient(worlds.HTTPHandler(world, "")))
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), client), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, clientInput{Value: 7})
	if err != nil {
		t.Fatal(err)
	}
	output, err := run.Await(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if output.Value != 7 {
		t.Fatalf("output = %#v", output)
	}
}

func TestNewHTTPClientRejectsInvalidEndpoints(t *testing.T) {
	for _, endpoint := range []string{"", "/v1/workflow", "localhost:8080"} {
		t.Run(endpoint, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("NewHTTPClient did not panic")
				}
			}()
			_ = workflow.NewHTTPClient(endpoint)
		})
	}
}

func TestHTTPClientReportsRPCFailuresAndCapabilitiesFallback(t *testing.T) {
	transport := roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusServiceUnavailable,
			Status:     "503 Service Unavailable",
			Header:     make(http.Header),
			Body:       io.NopCloser(bytes.NewReader(nil)),
			Request:    request,
		}, nil
	})
	client := workflow.NewHTTPClient("http://world/v1/workflow", &http.Client{Transport: transport})
	ctx := workflow.NewContext(context.Background(), client)
	if _, err := workflow.Start(ctx, clientInput{}); err == nil {
		t.Fatal("Start accepted a failed RPC response")
	}
	if capabilities := client.Capabilities(); capabilities != (worlds.Capabilities{}) {
		t.Fatalf("capabilities after RPC failure = %#v", capabilities)
	}
}

func TestHTTPClientRejectsMalformedRPCResponses(t *testing.T) {
	transport := roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Header:     make(http.Header),
			Body:       io.NopCloser(bytes.NewBufferString("not cbor")),
			Request:    request,
		}, nil
	})
	client := workflow.NewHTTPClient("http://world/v1/workflow", &http.Client{Transport: transport})
	if _, err := workflow.Start(workflow.NewContext(context.Background(), client), clientInput{}); err == nil {
		t.Fatal("Start accepted a malformed RPC response")
	}
}

func newInMemoryHTTPClient(handler http.Handler) *http.Client {
	return &http.Client{Transport: roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		return &http.Response{
			StatusCode: recorder.Code,
			Header:     recorder.Result().Header,
			Body:       io.NopCloser(bytes.NewReader(recorder.Body.Bytes())),
			Request:    request,
		}, nil
	})}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestHTTPClientWritesAndReadsRunStreams(t *testing.T) {
	world := testworld.New(workflow.Use(func(_ context.Context, input clientInput) (clientOutput, error) {
		return clientOutput{Value: input.Value}, nil
	}))
	defer world.Close()
	client := workflow.NewHTTPClient("http://world/v1/workflow", newInMemoryHTTPClient(worlds.HTTPHandler(world, "")))
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), client), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, clientInput{Value: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Await(ctx); err != nil {
		t.Fatal(err)
	}
	writer, release, err := run.Writer(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("remote")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := release(); err != nil {
		t.Fatal(err)
	}
	reader, err := run.Reader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "remote" {
		t.Fatalf("stream = %q", data)
	}
}
