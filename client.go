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

	"github.com/eriicafes/go-workflow/workflowops"
)

// Client schedules and reads workflow runs.
type Client = workflowops.Client

type httpClient struct {
	endpoint string
	http     *http.Client
}

// NewHTTPClient creates an HTTP-backed workflow client for a worker endpoint.
func NewHTTPClient(endpoint string) Client {
	parsed, err := url.ParseRequestURI(endpoint)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		panic("workflow: client endpoint must be an absolute HTTP URL")
	}
	return &httpClient{endpoint: strings.TrimRight(endpoint, "/"), http: http.DefaultClient}
}

func (client *httpClient) CreateRun(
	ctx context.Context,
	lease *workflowops.Lease,
	input workflowops.RunRequest,
) (string, error) {
	if lease != nil {
		return "", errors.New("workflow: HTTP client cannot create child runs")
	}
	if input.RunID != "" ||
		input.Sequence != workflowops.RootSequence ||
		(input.Kind != "" && input.Kind != workflowops.KindRun) {
		return "", errors.New("workflow: HTTP client can create root runs only")
	}
	var response startResponse
	err := client.request(
		ctx,
		http.MethodPost,
		APIPrefix+"/runs",
		startRequest{
			Identity: input.Identity,
			Input:    input.Input,
		},
		&response,
	)
	return response.ID, err
}

func (client *httpClient) WaitRun(ctx context.Context, id string) (json.RawMessage, error) {
	var response runResponse
	err := client.request(
		ctx,
		http.MethodGet,
		APIPrefix+"/runs/"+url.PathEscape(id),
		nil,
		&response,
	)
	return response.Output, err
}

func (client *httpClient) GetRun(ctx context.Context, id string) (workflowops.RunInfo, bool, error) {
	var response runInfoResponse
	err := client.request(ctx, http.MethodGet, APIPrefix+"/runs/"+url.PathEscape(id)+"/info", nil, &response)
	return response.Info, response.Found, err
}

func (client *httpClient) Wake(ctx context.Context, id string) (int, error) {
	var response wakeResponse
	err := client.request(ctx, http.MethodPost, APIPrefix+"/runs/"+url.PathEscape(id)+"/wake", nil, &response)
	return response.StoppedCount, err
}

func (client *httpClient) CancelOperations(ctx context.Context, tasks ...workflowops.Task) error {
	if len(tasks) == 0 {
		return nil
	}
	return client.request(
		ctx,
		http.MethodPost,
		APIPrefix+"/runs/cancel",
		cancelRequest{Tasks: tasks},
		nil,
	)
}

func (client *httpClient) ReadStream(ctx context.Context, stream workflowops.Stream, offset int64) (workflowops.StreamRead, error) {
	var response streamReadResponse
	path := fmt.Sprintf(
		"%s/%s/streams?namespace=%s&offset=%d",
		APIPrefix+"/runs",
		url.PathEscape(stream.RunID),
		url.QueryEscape(stream.Namespace),
		offset,
	)
	err := client.request(ctx, http.MethodGet, path, nil, &response)
	return response.StreamRead, err
}

func (client *httpClient) LockStream(ctx context.Context, stream workflowops.Stream, lock string) (bool, error) {
	var response streamLockResponse
	err := client.request(ctx, http.MethodPost, APIPrefix+"/streams/lock", streamLockRequest{
		Stream: stream,
		Lock:   lock,
	}, &response)
	return response.Acquired, err
}

func (client *httpClient) AppendStream(ctx context.Context, stream workflowops.Stream, lock string, data []byte) error {
	return client.request(ctx, http.MethodPost, APIPrefix+"/streams/append", streamAppendRequest{
		Stream: stream,
		Lock:   lock,
		Data:   data,
	}, nil)
}

func (client *httpClient) ReleaseStream(ctx context.Context, stream workflowops.Stream, lock string, closeStream bool) error {
	return client.request(ctx, http.MethodPost, APIPrefix+"/streams/release", streamReleaseRequest{
		Stream: stream,
		Lock:   lock,
		Close:  closeStream,
	}, nil)
}

func (client *httpClient) GetHook(ctx context.Context, token string) (workflowops.Hook, bool, error) {
	var response hookResponse
	err := client.request(ctx, http.MethodGet, APIPrefix+"/hooks/"+url.PathEscape(token), nil, &response)
	if err != nil {
		return workflowops.Hook{}, false, err
	}
	return response.Hook, response.Found, nil
}

func (client *httpClient) ResumeWebhook(ctx context.Context, token string, payload json.RawMessage) error {
	return client.request(ctx, http.MethodPost, APIPrefix+"/hooks/"+url.PathEscape(token), hookResumeRequest{Payload: payload}, nil)
}

func (client *httpClient) ReplyWebhook(ctx context.Context, token string, response json.RawMessage) error {
	return client.request(ctx, http.MethodPost, APIPrefix+"/webhooks/"+url.PathEscape(token)+"/response", response, nil)
}

func (client *httpClient) WaitWebhookResponse(ctx context.Context, token string) (json.RawMessage, error) {
	var response json.RawMessage
	err := client.request(ctx, http.MethodGet, APIPrefix+"/webhooks/"+url.PathEscape(token)+"/response", nil, &response)
	return response, err
}

func (client *httpClient) request(ctx context.Context, method, path string, input, output any) error {
	var body io.Reader
	if input != nil {
		encoded, err := json.Marshal(input)
		if err != nil {
			return err
		}
		body = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, client.endpoint+path, body)
	if err != nil {
		return err
	}
	if input != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := client.http.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		var failure errorResponse
		if json.NewDecoder(response.Body).Decode(&failure) == nil && failure.Error != "" {
			return workflowops.DecodeFailure(failure.Error)
		}
		return fmt.Errorf("workflow: HTTP request failed with %s", response.Status)
	}
	if output == nil {
		return nil
	}
	return json.NewDecoder(response.Body).Decode(output)
}
