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
	lease *workflowops.LeasedRun,
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
		apiPrefix,
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
		apiPrefix+"/"+url.PathEscape(id),
		nil,
		&response,
	)
	return response.Output, err
}

func (client *httpClient) Events(ctx context.Context, id string, after int64) ([]Event, error) {
	var response eventsResponse
	path := fmt.Sprintf(
		"%s/%s/events?after=%d",
		apiPrefix,
		url.PathEscape(id),
		after,
	)
	err := client.request(ctx, http.MethodGet, path, nil, &response)
	return response.Events, err
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
			return errors.New(failure.Error)
		}
		return fmt.Errorf("workflow: HTTP request failed with %s", response.Status)
	}
	return json.NewDecoder(response.Body).Decode(output)
}
