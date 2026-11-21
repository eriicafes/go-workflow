package worlds

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

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
//
// The methods up to Analytics call the World method of the same name. The
// methods after it are the Client's own.
type Client interface {
	// RunsGet returns a run. It returns ErrRunNotFound when the run does not
	// exist.
	RunsGet(context.Context, RunsGetInput) (Run, error)
	// RunsList returns one page of the runs that match the input filters.
	RunsList(context.Context, RunsListInput) (Page[Run], error)
	// RunsWaitForTerminalStatus waits up to input.Timeout for a run to finish
	// and returns the run as it is when the run finishes or the timeout
	// expires.
	RunsWaitForTerminalStatus(context.Context, RunsWaitForTerminalStatusInput) (Run, error)
	// RunsSetAttributes applies attribute changes to a run.
	RunsSetAttributes(context.Context, RunsSetAttributesInput) error
	// RunsDeleteDataMany deletes the data of finished runs. It returns an
	// error wrapping errors.ErrUnsupported when the World does not delete
	// run data.
	RunsDeleteDataMany(context.Context, RunsDeleteDataManyInput) (RunsDeleteDataManyResult, error)

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
	// StreamsOpenWriter returns a writer for a stream, which other writers
	// of the stream do not wait for.
	StreamsOpenWriter(context.Context, StreamsOpenWriterInput) (StreamWriter, error)

	// HooksGet returns hook metadata by ID. It returns ErrHookNotFound when
	// the hook does not exist.
	HooksGet(context.Context, HooksGetInput) (Hook, error)
	// HooksGetByToken returns the active or retained hook with a hook token.
	// It returns ErrHookNotFound when no hook owns the token.
	HooksGetByToken(context.Context, HooksGetByTokenInput) (Hook, error)

	// Analytics returns the World's analytics API. It may be nil when the
	// World has analytics disabled.
	Analytics() Analytics

	// RunsCreate starts a run of the named workflow and queues its first
	// delivery. It generates a run ID when input.RunID is empty and returns
	// the run ID.
	RunsCreate(context.Context, ClientRunsCreateInput) (RunID, error)
	// RunsCancel cancels a run. It returns ErrRunTerminal when the run
	// has already finished.
	RunsCancel(context.Context, ClientRunsCancelInput) error
	// RunsWake settles a run's pending waits early, as Run.WakeUp does, and
	// returns the number of waits it settled.
	RunsWake(context.Context, ClientRunsWakeInput) (int, error)
	// StreamsWrite appends a chunk to a stream. It returns ErrStreamClosed
	// when the stream is closed.
	StreamsWrite(context.Context, StreamsWriteInput) error
	// StreamsWriteMulti appends several chunks to a stream. It returns
	// ErrStreamClosed when the stream is closed.
	StreamsWriteMulti(context.Context, StreamsWriteMultiInput) error
	// StreamsClose closes a stream for every writer.
	StreamsClose(context.Context, StreamsCloseInput) error
	// HooksResume resumes the hook with input.Token with the payload
	// input.Data, as ResumeHook does. It returns ErrHookNotFound when no
	// active hook has the token.
	HooksResume(context.Context, ClientHooksResumeInput) error
	// Spec returns the RPC protocol version and capabilities of the World.
	Spec(context.Context) (ClientSpec, error)
	// RunKeys returns the keys of a run's payloads, or nil when they are not
	// encrypted, as the World's EncryptionKeys resolves them.
	RunKeys(context.Context, RunKeysInput) (*RunKeys, error)
	// RunsNewID returns the ID of a new run in input.Region, as the World
	// mints it: tagged with its region by a World that implements
	// RegionalRunIDs, and a plain run ID otherwise.
	RunsNewID(context.Context, ClientRunsNewIDInput) (RunID, error)
	// RunsStartDeployment returns the deployment a new run starts on, as
	// StartDeploymentID does, so its payloads can be encrypted for it
	// before it is created.
	RunsStartDeployment(context.Context, ClientRunsStartDeploymentInput) (string, error)
}

// ClientSpec describes the World a Client calls.
type ClientSpec struct {
	// RPCVersion is the RPC protocol version of the server.
	RPCVersion int `cbor:"rpcVersion"`
	// Capabilities reports the optional behavior the World implements.
	Capabilities Capabilities `cbor:"capabilities"`
}

// CreateRun creates a run and queues its first delivery, as StartRun does. It
// fills an empty RunID, and an empty DeploymentID as StartDeploymentID does,
// for LatestDeployment when input.Latest.
func CreateRun(ctx context.Context, world World, input ClientRunsCreateInput) (RunID, error) {
	if input.RunID == "" {
		input.RunID = NewRunID()
	}
	if input.DeploymentID == "" {
		deployment := Deployment{}
		if input.Latest {
			deployment = LatestDeployment
		}
		deploymentID, err := StartDeploymentID(ctx, world, deployment)
		if err != nil {
			return "", err
		}
		input.DeploymentID = deploymentID
	}
	if err := StartRun(ctx, world, input.EventsCreateRunInput); err != nil {
		return "", err
	}
	return input.RunID, nil
}

// NewClient returns a Client that calls world directly, in the same process.
func NewClient(world World) Client { return worldClient{world: world} }

type worldClient struct{ world World }

func (c worldClient) RunsGet(ctx context.Context, input RunsGetInput) (Run, error) {
	return c.world.RunsGet(ctx, input)
}

func (c worldClient) RunsList(ctx context.Context, input RunsListInput) (Page[Run], error) {
	return c.world.RunsList(ctx, input)
}

func (c worldClient) RunsWaitForTerminalStatus(ctx context.Context, input RunsWaitForTerminalStatusInput) (Run, error) {
	return c.world.RunsWaitForTerminalStatus(ctx, input)
}

func (c worldClient) RunsSetAttributes(ctx context.Context, input RunsSetAttributesInput) error {
	return c.world.RunsSetAttributes(ctx, input)
}

func (c worldClient) RunsDeleteDataMany(ctx context.Context, input RunsDeleteDataManyInput) (RunsDeleteDataManyResult, error) {
	return c.world.RunsDeleteDataMany(ctx, input)
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

func (c worldClient) StreamsOpenWriter(ctx context.Context, input StreamsOpenWriterInput) (StreamWriter, error) {
	return OpenStreamWriter(ctx, c.world, input)
}

func (c worldClient) HooksGet(ctx context.Context, input HooksGetInput) (Hook, error) {
	return c.world.HooksGet(ctx, input)
}

func (c worldClient) HooksGetByToken(ctx context.Context, input HooksGetByTokenInput) (Hook, error) {
	return c.world.HooksGetByToken(ctx, input)
}

func (c worldClient) Analytics() Analytics { return c.world.Analytics() }

func (c worldClient) RunsCreate(ctx context.Context, input ClientRunsCreateInput) (RunID, error) {
	return CreateRun(ctx, c.world, input)
}

func (c worldClient) RunsCancel(ctx context.Context, input ClientRunsCancelInput) error {
	_, err := c.world.EventsCreate(ctx, EventsCreateInput{RunID: input.RunID, Event: EventsCreateEvent{EventType: EventTypeRunCancelled}})
	return err
}

func (c worldClient) RunsWake(ctx context.Context, input ClientRunsWakeInput) (int, error) {
	return WakeRun(ctx, c.world, input.RunID, input.CorrelationIDs)
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

func (c worldClient) HooksResume(ctx context.Context, input ClientHooksResumeInput) error {
	hook, err := c.world.HooksGetByToken(ctx, HooksGetByTokenInput{Token: input.Token})
	if err != nil {
		return err
	}
	_, err = ResumeHook(ctx, c.world, hook, input.Data)
	return err
}

func (c worldClient) Spec(context.Context) (ClientSpec, error) {
	return ClientSpec{RPCVersion: RPCVersion, Capabilities: c.world.Capabilities()}, nil
}

func (c worldClient) RunKeys(ctx context.Context, input RunKeysInput) (*RunKeys, error) {
	return RunKeysFor(ctx, c.world, input.RunID, input.DeploymentID)
}

func (c worldClient) RunsNewID(_ context.Context, input ClientRunsNewIDInput) (RunID, error) {
	return newRunID(c.world, input), nil
}

func (c worldClient) RunsStartDeployment(ctx context.Context, input ClientRunsStartDeploymentInput) (string, error) {
	return StartDeploymentID(ctx, c.world, input.deployment())
}

// maxHTTPWait caps how long one RPC request may block in
// runs.waitForTerminalStatus or streams.getChunks, so a long wait never holds a
// request open past common proxy and server timeouts. Callers loop until they
// have what they wait for.
const maxHTTPWait = 10 * time.Second

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
	mutex    sync.Mutex
	spec     *ClientSpec
	// socketDeclinedUntil is when the client next tries a stream socket
	// after the server declined one.
	socketDeclinedUntil time.Time
	// runKeys caches the keys of runs' payloads by run ID.
	runKeys sync.Map
}

func (c *httpClient) call(ctx context.Context, method string, input, output any) error {
	var raw cbor.RawMessage
	if input != nil {
		encoded, err := rpcEncoding.Marshal(input)
		if err != nil {
			return err
		}
		raw = encoded
	}
	rpcVersion := RPCVersion
	body, err := rpcEncoding.Marshal(RPCRequest{Method: method, Input: raw, RPCVersion: &rpcVersion})
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
		return newRPCClientError(*envelope.Error)
	}
	if output == nil {
		return nil
	}
	if err := cbor.Unmarshal(envelope.Output, output); err != nil {
		return fmt.Errorf("workflow: decode RPC output: %w", err)
	}
	return nil
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
	input.Timeout = min(input.Timeout, maxHTTPWait)
	var output Run
	err := c.call(ctx, "runs.waitForTerminalStatus", input, &output)
	return output, err
}

func (c *httpClient) RunsSetAttributes(ctx context.Context, input RunsSetAttributesInput) error {
	return c.call(ctx, "runs.setAttributes", input, nil)
}

func (c *httpClient) RunsDeleteDataMany(ctx context.Context, input RunsDeleteDataManyInput) (RunsDeleteDataManyResult, error) {
	var output RunsDeleteDataManyResult
	err := c.call(ctx, "runs.deleteDataMany", input, &output)
	return output, err
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
	input.Wait = min(input.Wait, maxHTTPWait)
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

// streamSocketRetryAfter is how long a client that a server declined a
// stream socket writes through RPC before it tries again.
const streamSocketRetryAfter = 5 * time.Minute

// StreamsOpenWriter returns a writer that writes through the server's stream
// socket once it opens, and through RPC calls until then, or when the server
// declines it.
func (c *httpClient) StreamsOpenWriter(ctx context.Context, input StreamsOpenWriterInput) (StreamWriter, error) {
	direct := newDirectStreamWriter(c, input)
	c.mutex.Lock()
	declined := time.Now().Before(c.socketDeclinedUntil)
	c.mutex.Unlock()
	if declined || input.WriterID == "" {
		return direct, nil
	}
	target, err := StreamSocketURL(c.endpoint, input.RunID, input.Name, input.WriterID)
	if err != nil {
		return direct, nil
	}
	return OpenStreamSocketWriter(ctx, StreamSocketOptions{
		URL:        target,
		HTTPClient: c.client,
		Fallback:   direct,
		OnDecline: func() {
			c.mutex.Lock()
			c.socketDeclinedUntil = time.Now().Add(streamSocketRetryAfter)
			c.mutex.Unlock()
		},
	}), nil
}

func (c *httpClient) HooksGet(ctx context.Context, input HooksGetInput) (Hook, error) {
	var hook Hook
	err := c.call(ctx, "hooks.get", input, &hook)
	return hook, err
}

func (c *httpClient) HooksGetByToken(ctx context.Context, input HooksGetByTokenInput) (Hook, error) {
	var output Hook
	err := c.call(ctx, "hooks.getByToken", input, &output)
	return output, err
}

func (c *httpClient) Analytics() Analytics { return httpAnalytics{client: c} }

func (c *httpClient) RunsCreate(ctx context.Context, input ClientRunsCreateInput) (RunID, error) {
	var output RunID
	err := c.call(ctx, "client.runs.create", input, &output)
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

func (c *httpClient) StreamsWrite(ctx context.Context, input StreamsWriteInput) error {
	return c.call(ctx, "client.streams.write", input, nil)
}

func (c *httpClient) StreamsWriteMulti(ctx context.Context, input StreamsWriteMultiInput) error {
	return c.call(ctx, "client.streams.writeMulti", input, nil)
}

func (c *httpClient) StreamsClose(ctx context.Context, input StreamsCloseInput) error {
	return c.call(ctx, "client.streams.close", input, nil)
}

func (c *httpClient) HooksResume(ctx context.Context, input ClientHooksResumeInput) error {
	return c.call(ctx, "client.hooks.resume", input, nil)
}

// Spec calls client.spec once and returns its result from then on, since a
// World's spec does not change while it serves.
func (c *httpClient) RunKeys(ctx context.Context, input RunKeysInput) (*RunKeys, error) {
	if cached, ok := c.runKeys.Load(input.RunID); ok {
		return cached.(*RunKeys), nil
	}
	var key []byte
	if err := c.call(ctx, "runs.encryptionKey", input, &key); err != nil {
		return nil, err
	}
	var keys *RunKeys
	if key != nil {
		var err error
		if keys, err = NewRunKeys(key); err != nil {
			return nil, err
		}
	}
	c.runKeys.Store(input.RunID, keys)
	return keys, nil
}

func (c *httpClient) RunsNewID(ctx context.Context, input ClientRunsNewIDInput) (RunID, error) {
	var id RunID
	err := c.call(ctx, "client.runs.newId", input, &id)
	return id, err
}

func (c *httpClient) RunsStartDeployment(ctx context.Context, input ClientRunsStartDeploymentInput) (string, error) {
	var deploymentID string
	err := c.call(ctx, "client.runs.startDeployment", input, &deploymentID)
	return deploymentID, err
}

func (c *httpClient) Spec(ctx context.Context) (ClientSpec, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.spec != nil {
		return *c.spec, nil
	}
	var output ClientSpec
	if err := c.call(ctx, "client.spec", nil, &output); err != nil {
		return ClientSpec{}, err
	}
	c.spec = &output
	return output, nil
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

// rpcClientError is a server's RPC failure. It wraps the sentinel error the
// server named, so errors.Is matches it as it would in-process.
type rpcClientError struct {
	RPCError
	sentinel error
}

func newRPCClientError(failure RPCError) error {
	if failure.Sentinel == "hook_conflict" && failure.ConflictingRunID != "" {
		return &HookConflictError{RunID: failure.ConflictingRunID}
	}
	if failure.Sentinel == "throttled" {
		return &ThrottleError{Cause: rpcClientError{RPCError: failure}, After: failure.RetryAfter}
	}
	err := rpcClientError{RPCError: failure}
	for _, sentinel := range rpcSentinels {
		if sentinel.name == failure.Sentinel {
			err.sentinel = sentinel.err
			break
		}
	}
	return err
}

func (e rpcClientError) Error() string {
	return fmt.Sprintf("workflow: RPC %s: %s", e.Code, e.Message)
}

func (e rpcClientError) Unwrap() error { return e.sentinel }
