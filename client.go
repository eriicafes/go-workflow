package workflow

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/eriicafes/go-workflow/worlds"
	"github.com/fxamacker/cbor/v2"
)

// Client is the application-facing workflow control and read API. It does not
// grant arbitrary event, queue, or stream-write access.
type Client interface {
	RunsCreate(context.Context, worlds.ClientRunsCreateInput) (worlds.RunID, error)
	RunsGet(context.Context, worlds.RunsGetInput) (worlds.Run, error)
	RunsList(context.Context, worlds.RunsListInput) (worlds.Page[worlds.Run], error)
	RunsWaitForTerminalStatus(context.Context, worlds.RunsWaitForTerminalStatusInput) (worlds.Run, error)
	RunsCancel(context.Context, worlds.ClientRunsCancelInput) error
	RunsWake(context.Context, worlds.ClientRunsWakeInput) (int, error)
	RunsSetAttributes(context.Context, worlds.RunsSetAttributesInput) error

	StepsGet(context.Context, worlds.StepsGetInput) (worlds.Step, error)
	StepsList(context.Context, worlds.StepsListInput) (worlds.Page[worlds.Step], error)

	StreamsGetChunks(context.Context, worlds.StreamsGetChunksInput) (worlds.StreamChunksResult, error)
	StreamsGetInfo(context.Context, worlds.StreamsGetInfoInput) (worlds.StreamInfo, error)
	StreamsList(context.Context, worlds.StreamsListInput) ([]string, error)
	StreamsWrite(context.Context, worlds.StreamsWriteInput) error
	StreamsWriteMulti(context.Context, worlds.StreamsWriteMultiInput) error
	StreamsClose(context.Context, worlds.StreamsCloseInput) error

	HooksGetByToken(context.Context, worlds.HooksGetByTokenInput) (worlds.Hook, error)
	HooksResume(context.Context, worlds.ClientHooksResumeInput) error

	Analytics() worlds.Analytics
	Capabilities() worlds.Capabilities
}

// NewClient exposes world through the limited application client surface.
func NewClient(world worlds.World) Client { return worldClient{world: world} }

type worldClient struct{ world worlds.World }

func (c worldClient) RunsCreate(ctx context.Context, input worlds.ClientRunsCreateInput) (worlds.RunID, error) {
	runID := input.RunID
	if runID == "" {
		runID = worlds.NewRunID()
	}
	if _, err := c.world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: input.WorkflowName, Input: input.Input, Attributes: input.Attributes}); err != nil {
		return "", err
	}
	if _, err := c.world.Queue(ctx, worlds.QueueInput{RunID: runID, IdempotencyKey: string(runID)}); err != nil {
		return "", err
	}
	return runID, nil
}

func (c worldClient) RunsGet(ctx context.Context, input worlds.RunsGetInput) (worlds.Run, error) {
	return c.world.RunsGet(ctx, input)
}

func (c worldClient) RunsList(ctx context.Context, input worlds.RunsListInput) (worlds.Page[worlds.Run], error) {
	return c.world.RunsList(ctx, input)
}

func (c worldClient) RunsWaitForTerminalStatus(ctx context.Context, input worlds.RunsWaitForTerminalStatusInput) (worlds.Run, error) {
	return c.world.RunsWaitForTerminalStatus(ctx, input)
}

func (c worldClient) RunsCancel(ctx context.Context, input worlds.ClientRunsCancelInput) error {
	_, err := c.world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: input.RunID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeRunCancelled}})
	return err
}

func (c worldClient) RunsWake(ctx context.Context, input worlds.ClientRunsWakeInput) (int, error) {
	return worlds.WakeRun(ctx, c.world, input.RunID, input.CorrelationIDs)
}

func (c worldClient) RunsSetAttributes(ctx context.Context, input worlds.RunsSetAttributesInput) error {
	return c.world.RunsSetAttributes(ctx, input)
}

func (c worldClient) StepsGet(ctx context.Context, input worlds.StepsGetInput) (worlds.Step, error) {
	return c.world.StepsGet(ctx, input)
}

func (c worldClient) StepsList(ctx context.Context, input worlds.StepsListInput) (worlds.Page[worlds.Step], error) {
	return c.world.StepsList(ctx, input)
}

func (c worldClient) StreamsGetChunks(ctx context.Context, input worlds.StreamsGetChunksInput) (worlds.StreamChunksResult, error) {
	return c.world.StreamsGetChunks(ctx, input)
}

func (c worldClient) StreamsGetInfo(ctx context.Context, input worlds.StreamsGetInfoInput) (worlds.StreamInfo, error) {
	return c.world.StreamsGetInfo(ctx, input)
}

func (c worldClient) StreamsList(ctx context.Context, input worlds.StreamsListInput) ([]string, error) {
	return c.world.StreamsList(ctx, input)
}

func (c worldClient) StreamsWrite(ctx context.Context, input worlds.StreamsWriteInput) error {
	return c.world.StreamsWrite(ctx, input)
}

func (c worldClient) StreamsWriteMulti(ctx context.Context, input worlds.StreamsWriteMultiInput) error {
	return c.world.StreamsWriteMulti(ctx, input)
}

func (c worldClient) StreamsClose(ctx context.Context, input worlds.StreamsCloseInput) error {
	return c.world.StreamsClose(ctx, input)
}

func (c worldClient) streamsCreateWriteSession(ctx context.Context, input worlds.StreamsCreateWriteSessionInput) (worlds.StreamWriteSession, error) {
	return c.world.StreamsCreateWriteSession(ctx, input)
}

func (c worldClient) HooksGetByToken(ctx context.Context, input worlds.HooksGetByTokenInput) (worlds.Hook, error) {
	return c.world.HooksGetByToken(ctx, input)
}

func (c worldClient) HooksResume(ctx context.Context, input worlds.ClientHooksResumeInput) error {
	hook, err := c.world.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: input.Token})
	if err != nil {
		return err
	}
	result, err := c.world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: hook.RunID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeHookReceived, CorrelationID: string(hook.HookID), Data: input.Data}})
	if err != nil {
		return err
	}
	_, err = c.world.Queue(ctx, worlds.QueueInput{RunID: hook.RunID, IdempotencyKey: string(result.EventID)})
	return err
}

func (c worldClient) Analytics() worlds.Analytics { return c.world.Analytics() }

func (c worldClient) Capabilities() worlds.Capabilities { return c.world.Capabilities() }

// NewHTTPClient connects to a World's HTTPHandler. The last non-nil optional
// HTTP client replaces http.DefaultClient.
func NewHTTPClient(endpoint string, clients ...*http.Client) Client {
	parsed, err := url.ParseRequestURI(endpoint)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		panic("workflow: client endpoint must be an absolute HTTP URL")
	}
	client := http.DefaultClient
	if len(clients) != 0 {
		client = clients[len(clients)-1]
	}
	return &httpClient{endpoint: strings.TrimRight(endpoint, "/"), client: client}
}

type httpClient struct {
	endpoint string
	client   *http.Client
}

func (c *httpClient) call(ctx context.Context, method string, input, output any) error {
	var raw cbor.RawMessage
	if input != nil {
		encoded, err := cbor.Marshal(input)
		if err != nil {
			return err
		}
		raw = encoded
	}
	body, err := cbor.Marshal(worlds.RPCRequest{Method: method, Input: raw})
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+"/rpc", bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", worlds.RPCContentType)
	response, err := c.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("workflow: RPC request failed with %s", response.Status)
	}
	var envelope worlds.RPCResponse
	if err := cbor.NewDecoder(response.Body).Decode(&envelope); err != nil {
		return fmt.Errorf("workflow: decode RPC response: %w", err)
	}
	if envelope.Error != nil {
		return fmt.Errorf("workflow: RPC %s: %s", envelope.Error.Code, envelope.Error.Message)
	}
	if output == nil {
		return nil
	}
	if err := cbor.Unmarshal(envelope.Output, output); err != nil {
		return fmt.Errorf("workflow: decode RPC output: %w", err)
	}
	return nil
}

func (c *httpClient) RunsCreate(ctx context.Context, input worlds.ClientRunsCreateInput) (worlds.RunID, error) {
	var output worlds.RunID
	err := c.call(ctx, "client.runs.create", input, &output)
	return output, err
}

func (c *httpClient) RunsGet(ctx context.Context, input worlds.RunsGetInput) (worlds.Run, error) {
	var output worlds.Run
	err := c.call(ctx, "runs.get", input, &output)
	return output, err
}

func (c *httpClient) RunsList(ctx context.Context, input worlds.RunsListInput) (worlds.Page[worlds.Run], error) {
	var output worlds.Page[worlds.Run]
	err := c.call(ctx, "runs.list", input, &output)
	return output, err
}

func (c *httpClient) RunsWaitForTerminalStatus(ctx context.Context, input worlds.RunsWaitForTerminalStatusInput) (worlds.Run, error) {
	var output worlds.Run
	err := c.call(ctx, "runs.waitForTerminalStatus", input, &output)
	return output, err
}

func (c *httpClient) RunsCancel(ctx context.Context, input worlds.ClientRunsCancelInput) error {
	return c.call(ctx, "client.runs.cancel", input, nil)
}

func (c *httpClient) RunsWake(ctx context.Context, input worlds.ClientRunsWakeInput) (int, error) {
	var output int
	err := c.call(ctx, "client.runs.wake", input, &output)
	return output, err
}

func (c *httpClient) RunsSetAttributes(ctx context.Context, input worlds.RunsSetAttributesInput) error {
	return c.call(ctx, "runs.setAttributes", input, nil)
}

func (c *httpClient) StepsGet(ctx context.Context, input worlds.StepsGetInput) (worlds.Step, error) {
	var output worlds.Step
	err := c.call(ctx, "steps.get", input, &output)
	return output, err
}

func (c *httpClient) StepsList(ctx context.Context, input worlds.StepsListInput) (worlds.Page[worlds.Step], error) {
	var output worlds.Page[worlds.Step]
	err := c.call(ctx, "steps.list", input, &output)
	return output, err
}

func (c *httpClient) StreamsGetChunks(ctx context.Context, input worlds.StreamsGetChunksInput) (worlds.StreamChunksResult, error) {
	var output worlds.StreamChunksResult
	err := c.call(ctx, "streams.getChunks", input, &output)
	return output, err
}

func (c *httpClient) StreamsGetInfo(ctx context.Context, input worlds.StreamsGetInfoInput) (worlds.StreamInfo, error) {
	var output worlds.StreamInfo
	err := c.call(ctx, "streams.getInfo", input, &output)
	return output, err
}

func (c *httpClient) StreamsList(ctx context.Context, input worlds.StreamsListInput) ([]string, error) {
	var output []string
	err := c.call(ctx, "streams.list", input, &output)
	return output, err
}

func (c *httpClient) StreamsWrite(ctx context.Context, input worlds.StreamsWriteInput) error {
	return c.call(ctx, "client.streams.write", input, nil)
}

func (c *httpClient) StreamsWriteMulti(ctx context.Context, input worlds.StreamsWriteMultiInput) error {
	return c.call(ctx, "client.streams.writeMulti", input, nil)
}

func (c *httpClient) StreamsClose(ctx context.Context, input worlds.StreamsCloseInput) error {
	return c.call(ctx, "client.streams.close", input, nil)
}

func (c *httpClient) streamsCreateWriteSession(ctx context.Context, input worlds.StreamsCreateWriteSessionInput) (worlds.StreamWriteSession, error) {
	var id worlds.StreamID
	if err := c.call(ctx, "streams.createWriteSession", input, &id); err != nil {
		return nil, err
	}
	return httpStreamWriteSession{client: c, runID: input.RunID, name: input.Name, id: id}, nil
}

type httpStreamWriteSession struct {
	client *httpClient
	runID  worlds.RunID
	name   string
	id     worlds.StreamID
}

func (s httpStreamWriteSession) ID() worlds.StreamID { return s.id }
func (s httpStreamWriteSession) Write(ctx context.Context, input worlds.StreamWriteSessionWriteInput) error {
	return s.client.call(ctx, "streams.writeSession", worlds.StreamsWriteSessionInput{RunID: s.runID, Name: s.name, SessionID: s.id, Sequence: input.Sequence, Chunks: input.Chunks}, nil)
}
func (s httpStreamWriteSession) Close(ctx context.Context) error {
	return s.client.call(ctx, "streams.closeWriteSession", worlds.StreamsCloseWriteSessionInput{RunID: s.runID, Name: s.name, SessionID: s.id, Close: true}, nil)
}
func (s httpStreamWriteSession) Release(ctx context.Context) error {
	return s.client.call(ctx, "streams.closeWriteSession", worlds.StreamsCloseWriteSessionInput{RunID: s.runID, Name: s.name, SessionID: s.id}, nil)
}

func (c *httpClient) HooksGetByToken(ctx context.Context, input worlds.HooksGetByTokenInput) (worlds.Hook, error) {
	var output worlds.Hook
	err := c.call(ctx, "hooks.getByToken", input, &output)
	return output, err
}

func (c *httpClient) HooksResume(ctx context.Context, input worlds.ClientHooksResumeInput) error {
	return c.call(ctx, "client.hooks.resume", input, nil)
}

func (c *httpClient) Analytics() worlds.Analytics { return httpAnalytics{client: c} }

func (c *httpClient) Capabilities() worlds.Capabilities {
	var output worlds.Capabilities
	if err := c.call(context.Background(), "world.capabilities", nil, &output); err != nil {
		return worlds.Capabilities{}
	}
	return output
}

type httpAnalytics struct{ client *httpClient }

func (a httpAnalytics) RunsGet(ctx context.Context, input worlds.AnalyticsRunsGetInput) (worlds.AnalyticsRun, error) {
	var output worlds.AnalyticsRun
	err := a.client.call(ctx, "analytics.runs.get", input, &output)
	return output, err
}

func (a httpAnalytics) RunsList(ctx context.Context, input worlds.AnalyticsRunsListInput) (worlds.Page[worlds.AnalyticsRun], error) {
	var output worlds.Page[worlds.AnalyticsRun]
	err := a.client.call(ctx, "analytics.runs.list", input, &output)
	return output, err
}

func (a httpAnalytics) AttributesList(ctx context.Context, input worlds.AnalyticsAttributesListInput) (worlds.Page[worlds.AnalyticsAttribute], error) {
	var output worlds.Page[worlds.AnalyticsAttribute]
	err := a.client.call(ctx, "analytics.attributes.list", input, &output)
	return output, err
}

func (a httpAnalytics) WaitsGet(ctx context.Context, input worlds.AnalyticsWaitsGetInput) (worlds.AnalyticsWait, error) {
	var output worlds.AnalyticsWait
	err := a.client.call(ctx, "analytics.waits.get", input, &output)
	return output, err
}

func (a httpAnalytics) WaitsList(ctx context.Context, input worlds.AnalyticsWaitsListInput) (worlds.Page[worlds.AnalyticsWait], error) {
	var output worlds.Page[worlds.AnalyticsWait]
	err := a.client.call(ctx, "analytics.waits.list", input, &output)
	return output, err
}
