package worlds

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/fxamacker/cbor/v2"
)

type clientContextKey struct{}

// NewClientContext returns a copy of ctx that carries client. Runtime.Execute
// removes it from a delivery's context, so in a run ClientFromContext
// reports only a Client the run's own code put in its context.
func NewClientContext(ctx context.Context, client Client) context.Context {
	return context.WithValue(ctx, clientContextKey{}, client)
}

// ClientFromContext returns the Client NewClientContext put in ctx.
func ClientFromContext(ctx context.Context) (Client, bool) {
	client, ok := ctx.Value(clientContextKey{}).(Client)
	return client, ok
}

// Client is the API for starting, reading, cancelling, and resuming runs, and
// for reading and writing their streams, from outside a run. It does not
// execute runs. Start, Get, GetHook, and ResumeHook use the Client carried in
// a context by workflow.NewContext or workflow.Middleware.
type Client interface {
	// RunsCreate starts a run of the named workflow and queues its first
	// delivery. It generates a run ID when input.RunID is empty and returns
	// the run ID.
	RunsCreate(context.Context, ClientRunsCreateInput) (RunID, error)
	// RunsGet returns a run. It returns ErrRunNotFound when the run does not
	// exist.
	RunsGet(context.Context, RunsGetInput) (Run, error)
	// RunsList returns one page of the runs that match the input filters.
	RunsList(context.Context, RunsListInput) (Page[Run], error)
	// RunsWaitForTerminalStatus waits up to input.Timeout for a run to finish
	// and returns the run as it is when the run finishes or the timeout
	// expires.
	RunsWaitForTerminalStatus(context.Context, RunsWaitForTerminalStatusInput) (Run, error)
	// RunsCancel cancels a run. It returns ErrRunTerminal when the run
	// has already finished.
	RunsCancel(context.Context, ClientRunsCancelInput) error
	// RunsWake settles a run's pending waits early, as Run.WakeUp does, and
	// returns the number of waits it settled.
	RunsWake(context.Context, ClientRunsWakeInput) (int, error)
	// RunsSetAttributes applies attribute changes to a run.
	RunsSetAttributes(context.Context, RunsSetAttributesInput) error

	// StepsGet returns a step of a run. It returns ErrStepNotFound when
	// the step does not exist.
	StepsGet(context.Context, StepsGetInput) (Step, error)
	// StepsList returns one page of the steps in a run.
	StepsList(context.Context, StepsListInput) (Page[Step], error)

	// StreamsGetChunks returns the chunks of a stream after input.Cursor. When
	// none are available and the stream is not closed, it waits up to
	// input.Wait for more.
	StreamsGetChunks(context.Context, StreamsGetChunksInput) (StreamChunksResult, error)
	// StreamsGetInfo returns a stream's tail index and whether it is closed.
	StreamsGetInfo(context.Context, StreamsGetInfoInput) (StreamInfo, error)
	// StreamsList returns the names of a run's streams.
	StreamsList(context.Context, StreamsListInput) ([]string, error)
	// StreamsCreateWriteSession creates an exclusive writer session for a
	// stream. It returns ErrStreamLocked while another session holds the
	// stream.
	StreamsCreateWriteSession(context.Context, StreamsCreateWriteSessionInput) (StreamWriteSession, error)
	// StreamsWrite appends a chunk to a stream. It returns
	// ErrStreamLocked while a writer holds the stream.
	StreamsWrite(context.Context, StreamsWriteInput) error
	// StreamsWriteMulti appends several chunks to a stream. It returns
	// ErrStreamLocked while a writer holds the stream.
	StreamsWriteMulti(context.Context, StreamsWriteMultiInput) error
	// StreamsClose closes a stream. It returns ErrStreamLocked while a
	// writer holds the stream.
	StreamsClose(context.Context, StreamsCloseInput) error

	// HooksGetByToken returns the active hook with a hook token. It returns
	// ErrHookNotFound when no active hook has the token.
	HooksGetByToken(context.Context, HooksGetByTokenInput) (Hook, error)
	// HooksResume resumes the hook with input.Token with the payload
	// input.Data and queues a delivery so its run resumes. It returns
	// ErrHookNotFound when no active hook has the token.
	HooksResume(context.Context, ClientHooksResumeInput) error

	// Analytics returns the World's analytics API. It may be nil when the
	// World has analytics disabled.
	Analytics() Analytics
	// Capabilities reports the optional features the World supports.
	Capabilities() Capabilities
}

// NewClient returns a Client that calls world directly, in the same process.
func NewClient(world World) Client { return worldClient{world: world} }

type worldClient struct{ world World }

func (c worldClient) RunsCreate(ctx context.Context, input ClientRunsCreateInput) (RunID, error) {
	runID := input.RunID
	if runID == "" {
		runID = NewRunID()
	}
	if _, err := c.world.EventsCreateRun(ctx, EventsCreateRunInput{RunID: runID, WorkflowName: input.WorkflowName, Input: input.Input, Attributes: input.Attributes}); err != nil {
		return "", err
	}
	if _, err := c.world.Queue(ctx, QueueInput{RunID: runID, IdempotencyKey: string(runID)}); err != nil {
		return "", err
	}
	return runID, nil
}

func (c worldClient) RunsGet(ctx context.Context, input RunsGetInput) (Run, error) {
	return c.world.RunsGet(ctx, input)
}

func (c worldClient) RunsList(ctx context.Context, input RunsListInput) (Page[Run], error) {
	return c.world.RunsList(ctx, input)
}

func (c worldClient) RunsWaitForTerminalStatus(ctx context.Context, input RunsWaitForTerminalStatusInput) (Run, error) {
	return c.world.RunsWaitForTerminalStatus(ctx, input)
}

func (c worldClient) RunsCancel(ctx context.Context, input ClientRunsCancelInput) error {
	_, err := c.world.EventsCreate(ctx, EventsCreateInput{RunID: input.RunID, Event: EventsCreateEvent{EventType: EventTypeRunCancelled}})
	return err
}

func (c worldClient) RunsWake(ctx context.Context, input ClientRunsWakeInput) (int, error) {
	return WakeRun(ctx, c.world, input.RunID, input.CorrelationIDs)
}

func (c worldClient) RunsSetAttributes(ctx context.Context, input RunsSetAttributesInput) error {
	return c.world.RunsSetAttributes(ctx, input)
}

func (c worldClient) StepsGet(ctx context.Context, input StepsGetInput) (Step, error) {
	return c.world.StepsGet(ctx, input)
}

func (c worldClient) StepsList(ctx context.Context, input StepsListInput) (Page[Step], error) {
	return c.world.StepsList(ctx, input)
}

func (c worldClient) StreamsGetChunks(ctx context.Context, input StreamsGetChunksInput) (StreamChunksResult, error) {
	return c.world.StreamsGetChunks(ctx, input)
}

func (c worldClient) StreamsGetInfo(ctx context.Context, input StreamsGetInfoInput) (StreamInfo, error) {
	return c.world.StreamsGetInfo(ctx, input)
}

func (c worldClient) StreamsList(ctx context.Context, input StreamsListInput) ([]string, error) {
	return c.world.StreamsList(ctx, input)
}

func (c worldClient) StreamsWrite(ctx context.Context, input StreamsWriteInput) error {
	return c.world.StreamsWrite(ctx, input)
}

func (c worldClient) StreamsWriteMulti(ctx context.Context, input StreamsWriteMultiInput) error {
	return c.world.StreamsWriteMulti(ctx, input)
}

func (c worldClient) StreamsClose(ctx context.Context, input StreamsCloseInput) error {
	return c.world.StreamsClose(ctx, input)
}

func (c worldClient) StreamsCreateWriteSession(ctx context.Context, input StreamsCreateWriteSessionInput) (StreamWriteSession, error) {
	return c.world.StreamsCreateWriteSession(ctx, input)
}

func (c worldClient) HooksGetByToken(ctx context.Context, input HooksGetByTokenInput) (Hook, error) {
	return c.world.HooksGetByToken(ctx, input)
}

func (c worldClient) HooksResume(ctx context.Context, input ClientHooksResumeInput) error {
	hook, err := c.world.HooksGetByToken(ctx, HooksGetByTokenInput{Token: input.Token})
	if err != nil {
		return err
	}
	result, err := c.world.EventsCreate(ctx, EventsCreateInput{RunID: hook.RunID, Event: EventsCreateEvent{EventType: EventTypeHookReceived, CorrelationID: string(hook.HookID), Data: input.Data}})
	if err != nil {
		return err
	}
	_, err = c.world.Queue(ctx, QueueInput{RunID: hook.RunID, IdempotencyKey: string(result.EventID)})
	return err
}

func (c worldClient) Analytics() Analytics { return c.world.Analytics() }

func (c worldClient) Capabilities() Capabilities { return c.world.Capabilities() }

// NewHTTPClient returns a Client that calls a World served by HTTPHandler.
// endpoint is the absolute URL of the server followed by the handler's
// prefix, such as http://worker:8081/v1/workflow, and requests go to
// <endpoint>/rpc. It panics when endpoint is not an absolute HTTP URL.
// Requests use the last of clients, or http.DefaultClient when none is given.
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
	body, err := cbor.Marshal(RPCRequest{Method: method, Input: raw})
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+"/rpc", bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", RPCContentType)
	response, err := c.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("workflow: RPC request failed with %s", response.Status)
	}
	var envelope RPCResponse
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

func (c *httpClient) RunsCreate(ctx context.Context, input ClientRunsCreateInput) (RunID, error) {
	var output RunID
	err := c.call(ctx, "client.runs.create", input, &output)
	return output, err
}

func (c *httpClient) RunsGet(ctx context.Context, input RunsGetInput) (Run, error) {
	var output Run
	err := c.call(ctx, "runs.get", input, &output)
	return output, err
}

func (c *httpClient) RunsList(ctx context.Context, input RunsListInput) (Page[Run], error) {
	var output Page[Run]
	err := c.call(ctx, "runs.list", input, &output)
	return output, err
}

func (c *httpClient) RunsWaitForTerminalStatus(ctx context.Context, input RunsWaitForTerminalStatusInput) (Run, error) {
	var output Run
	err := c.call(ctx, "runs.waitForTerminalStatus", input, &output)
	return output, err
}

func (c *httpClient) RunsCancel(ctx context.Context, input ClientRunsCancelInput) error {
	return c.call(ctx, "client.runs.cancel", input, nil)
}

func (c *httpClient) RunsWake(ctx context.Context, input ClientRunsWakeInput) (int, error) {
	var output int
	err := c.call(ctx, "client.runs.wake", input, &output)
	return output, err
}

func (c *httpClient) RunsSetAttributes(ctx context.Context, input RunsSetAttributesInput) error {
	return c.call(ctx, "runs.setAttributes", input, nil)
}

func (c *httpClient) StepsGet(ctx context.Context, input StepsGetInput) (Step, error) {
	var output Step
	err := c.call(ctx, "steps.get", input, &output)
	return output, err
}

func (c *httpClient) StepsList(ctx context.Context, input StepsListInput) (Page[Step], error) {
	var output Page[Step]
	err := c.call(ctx, "steps.list", input, &output)
	return output, err
}

func (c *httpClient) StreamsGetChunks(ctx context.Context, input StreamsGetChunksInput) (StreamChunksResult, error) {
	var output StreamChunksResult
	err := c.call(ctx, "streams.getChunks", input, &output)
	return output, err
}

func (c *httpClient) StreamsGetInfo(ctx context.Context, input StreamsGetInfoInput) (StreamInfo, error) {
	var output StreamInfo
	err := c.call(ctx, "streams.getInfo", input, &output)
	return output, err
}

func (c *httpClient) StreamsList(ctx context.Context, input StreamsListInput) ([]string, error) {
	var output []string
	err := c.call(ctx, "streams.list", input, &output)
	return output, err
}

func (c *httpClient) StreamsWrite(ctx context.Context, input StreamsWriteInput) error {
	return c.call(ctx, "client.streams.write", input, nil)
}

func (c *httpClient) StreamsWriteMulti(ctx context.Context, input StreamsWriteMultiInput) error {
	return c.call(ctx, "client.streams.writeMulti", input, nil)
}

func (c *httpClient) StreamsClose(ctx context.Context, input StreamsCloseInput) error {
	return c.call(ctx, "client.streams.close", input, nil)
}

func (c *httpClient) StreamsCreateWriteSession(ctx context.Context, input StreamsCreateWriteSessionInput) (StreamWriteSession, error) {
	var id StreamID
	if err := c.call(ctx, "streams.createWriteSession", input, &id); err != nil {
		return nil, err
	}
	return httpStreamWriteSession{client: c, runID: input.RunID, name: input.Name, id: id}, nil
}

type httpStreamWriteSession struct {
	client *httpClient
	runID  RunID
	name   string
	id     StreamID
}

func (s httpStreamWriteSession) ID() StreamID { return s.id }
func (s httpStreamWriteSession) Write(ctx context.Context, input StreamWriteSessionWriteInput) error {
	return s.client.call(ctx, "streams.writeSession", StreamsWriteSessionInput{RunID: s.runID, Name: s.name, SessionID: s.id, Sequence: input.Sequence, Chunks: input.Chunks}, nil)
}
func (s httpStreamWriteSession) Close(ctx context.Context) error {
	return s.client.call(ctx, "streams.closeWriteSession", StreamsCloseWriteSessionInput{RunID: s.runID, Name: s.name, SessionID: s.id, Close: true}, nil)
}
func (s httpStreamWriteSession) Release(ctx context.Context) error {
	return s.client.call(ctx, "streams.closeWriteSession", StreamsCloseWriteSessionInput{RunID: s.runID, Name: s.name, SessionID: s.id}, nil)
}

func (c *httpClient) HooksGetByToken(ctx context.Context, input HooksGetByTokenInput) (Hook, error) {
	var output Hook
	err := c.call(ctx, "hooks.getByToken", input, &output)
	return output, err
}

func (c *httpClient) HooksResume(ctx context.Context, input ClientHooksResumeInput) error {
	return c.call(ctx, "client.hooks.resume", input, nil)
}

func (c *httpClient) Analytics() Analytics { return httpAnalytics{client: c} }

func (c *httpClient) Capabilities() Capabilities {
	var output Capabilities
	if err := c.call(context.Background(), "world.capabilities", nil, &output); err != nil {
		return Capabilities{}
	}
	return output
}

type httpAnalytics struct{ client *httpClient }

func (a httpAnalytics) RunsGet(ctx context.Context, input AnalyticsRunsGetInput) (AnalyticsRun, error) {
	var output AnalyticsRun
	err := a.client.call(ctx, "analytics.runs.get", input, &output)
	return output, err
}

func (a httpAnalytics) RunsList(ctx context.Context, input AnalyticsRunsListInput) (Page[AnalyticsRun], error) {
	var output Page[AnalyticsRun]
	err := a.client.call(ctx, "analytics.runs.list", input, &output)
	return output, err
}

func (a httpAnalytics) AttributesList(ctx context.Context, input AnalyticsAttributesListInput) (Page[AnalyticsAttribute], error) {
	var output Page[AnalyticsAttribute]
	err := a.client.call(ctx, "analytics.attributes.list", input, &output)
	return output, err
}

func (a httpAnalytics) WaitsGet(ctx context.Context, input AnalyticsWaitsGetInput) (AnalyticsWait, error) {
	var output AnalyticsWait
	err := a.client.call(ctx, "analytics.waits.get", input, &output)
	return output, err
}

func (a httpAnalytics) WaitsList(ctx context.Context, input AnalyticsWaitsListInput) (Page[AnalyticsWait], error) {
	var output Page[AnalyticsWait]
	err := a.client.call(ctx, "analytics.waits.list", input, &output)
	return output, err
}
