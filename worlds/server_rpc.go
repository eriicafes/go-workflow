package worlds

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"reflect"
	"time"

	"github.com/fxamacker/cbor/v2"
)

// RPCVersion is the version of the RPC protocol HTTPHandler serves and the
// HTTP client speaks.
const RPCVersion = 1

// RPCContentType is the content type accepted and returned by HTTPHandler's
// RPC endpoint.
const RPCContentType = "application/cbor"

// RPCRequest is the CBOR envelope sent to an HTTPHandler RPC endpoint.
type RPCRequest struct {
	// Method names the operation, such as "runs.get".
	Method string `cbor:"method"`
	// Input is the CBOR-encoded input of the operation.
	Input cbor.RawMessage `cbor:"input"`
	// RPCVersion is the client's RPC protocol version. When set and
	// different from RPCVersion, the request fails with invalid_argument.
	RPCVersion *int `cbor:"rpcVersion,omitempty"`
}

// RPCResponse is the CBOR envelope returned by an HTTPHandler RPC endpoint.
type RPCResponse struct {
	Output cbor.RawMessage `cbor:"output,omitempty"`
	Error  *RPCError       `cbor:"error,omitempty"`
}

// RPCError describes a failed RPC request.
type RPCError struct {
	// Code classifies the error as not_found, invalid_argument, conflict,
	// cancelled, deadline_exceeded, or internal.
	Code    string `cbor:"code"`
	Message string `cbor:"message"`
	// Sentinel names the error the failure matches, such as run_not_found,
	// so a client can return that error.
	Sentinel string `cbor:"sentinel,omitempty"`
	// Details is the CBOR encoding of the error Sentinel names when it is an
	// error type, such as a *HookNotFoundError, so a Go client returns one
	// with the same fields.
	Details cbor.RawMessage `cbor:"details,omitempty"`
	// ConflictingRunID names the run holding a hook token, for a
	// hook_conflict failure of a *HookConflictError.
	ConflictingRunID RunID `cbor:"conflictingRunId,omitempty"`
	// RetryAfter is how long a throttled failure of a *ThrottleError, or a
	// too_early failure of a *TooEarlyError, asks the caller to wait, or
	// zero when the World did not say.
	RetryAfter time.Duration `cbor:"retryAfter,omitempty"`
}

// maxRPCWait caps how long one RPC request may block in a World wait, so a
// caller cannot hold a server connection open indefinitely.
const maxRPCWait = 30 * time.Second

// errRPCInvalidInput reports a request the server cannot decode or route.
var errRPCInvalidInput = errors.New("workflow: invalid RPC request")

// rpcErrors names the errors an RPC failure carries to clients, with the
// code each is reported under. A failure takes the first entry it matches.
var rpcErrors = []rpcErrorKind{
	rpcSentinel("workflow_not_registered", "not_found", ErrWorkflowNotRegistered),
	rpcSentinel("run_not_found", "not_found", ErrRunNotFound),
	rpcSentinel("step_not_found", "not_found", ErrStepNotFound),
	rpcSentinel("event_not_found", "not_found", ErrEventNotFound),
	rpcType[*HookNotFoundError]("hook_not_found", "not_found"),
	rpcSentinel("event_invalid", "invalid_argument", ErrEventInvalid),
	rpcSentinel("entity_conflict", "conflict", ErrEntityConflict),
	rpcType[*RunNotSupportedError]("run_not_supported", "invalid_argument"),
	rpcSentinel("attribute_invalid", "invalid_argument", ErrAttributeInvalid),
	rpcType[*TooEarlyError]("too_early", "too_early"),
	rpcSentinel("run_already_exists", "conflict", ErrRunAlreadyExists),
	rpcType[*RunExpiredError]("run_expired", "conflict"),
	rpcSentinel("step_not_startable", "conflict", ErrStepNotStartable),
	rpcSentinel("stream_closed", "conflict", ErrStreamClosed),
	rpcType[*StreamExpiredError]("stream_expired", "not_found"),
	rpcType[*HookConflictError]("hook_conflict", "conflict"),
	rpcSentinel("start_hook_rejected", "invalid_argument", ErrStartHookRejected),
	rpcSentinel("hook_not_active", "conflict", ErrHookNotActive),
	rpcType[*HookForceClaimedError]("hook_force_claimed", "conflict"),
	rpcType[*InBandSupersededError]("in_band_superseded", "conflict"),
	rpcSentinel("run_cancelled", "conflict", ErrRunCancelled),
	rpcSentinel("resume_not_queued", "internal", ErrResumeNotQueued),
	rpcSentinel("webhook_request_too_large", "invalid_argument", ErrWebhookRequestTooLarge),
	rpcSentinel("deployment_unavailable", "internal", ErrDeploymentUnavailable),
	rpcSentinel("deployment_mismatch", "internal", ErrDeploymentMismatch),
	rpcSentinel("corrupted_event_log", "internal", ErrCorruptedEventLog),
	rpcSentinel("delete_data_invalid", "invalid_argument", ErrDeleteDataInvalid),
	rpcSentinel("unsupported", "unimplemented", errors.ErrUnsupported),
	rpcType[*ThrottleError]("throttled", "resource_exhausted"),
	rpcSentinel("cancelled", "cancelled", context.Canceled),
	rpcSentinel("deadline_exceeded", "deadline_exceeded", context.DeadlineExceeded),
}

// rpcErrorKind is an error an RPC failure carries: a sentinel error, or an
// error type whose fields travel as the failure's Details.
type rpcErrorKind struct {
	name, code string
	// find returns the error of the kind in err's chain, and its Details.
	find func(err error) (cbor.RawMessage, bool)
	// rebuild returns the error of the kind a failure carries.
	rebuild func(details cbor.RawMessage) error
}

func rpcSentinel(name, code string, sentinel error) rpcErrorKind {
	return rpcErrorKind{
		name: name, code: code,
		find:    func(err error) (cbor.RawMessage, bool) { return nil, errors.Is(err, sentinel) },
		rebuild: func(cbor.RawMessage) error { return sentinel },
	}
}

// rpcType is an rpcErrorKind of the error type E, a pointer to a struct.
func rpcType[E error](name, code string) rpcErrorKind {
	return rpcErrorKind{
		name: name, code: code,
		find: func(err error) (cbor.RawMessage, bool) {
			found, ok := errors.AsType[E](err)
			if !ok {
				return nil, false
			}
			details, _ := rpcEncoding.Marshal(found)
			return details, true
		},
		rebuild: func(details cbor.RawMessage) error {
			value := reflect.New(reflect.TypeFor[E]().Elem())
			if len(details) > 0 {
				_ = cbor.Unmarshal(details, value.Interface())
			}
			return value.Interface().(E)
		},
	}
}

// rpcVersionMismatchError reports incompatible RPC protocol versions.
type rpcVersionMismatchError struct {
	client int
	server int
}

func (e rpcVersionMismatchError) Error() string {
	return fmt.Sprintf("workflow: RPC version does not match server: client=%d server=%d", e.client, e.server)
}

func handleRPC(writer http.ResponseWriter, request *http.Request, world World) {
	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || mediaType != RPCContentType {
		http.Error(writer, "workflow: Content-Type must be application/cbor", http.StatusUnsupportedMediaType)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(writer, request.Body, 8<<20))
	if err != nil {
		writeRPC(writer, RPCResponse{Error: newRPCError(fmt.Errorf("%w: %w", errRPCInvalidInput, err))})
		return
	}
	var envelope RPCRequest
	if err := cbor.Unmarshal(body, &envelope); err != nil || envelope.Method == "" {
		if err == nil {
			err = errors.New("missing method")
		}
		writeRPC(writer, RPCResponse{Error: newRPCError(fmt.Errorf("%w: %w", errRPCInvalidInput, err))})
		return
	}
	output, err := dispatchRPC(request.Context(), world, envelope)
	if err != nil {
		writeRPC(writer, RPCResponse{Error: newRPCError(err)})
		return
	}
	writeRPC(writer, RPCResponse{Output: output})
}

func writeRPC(writer http.ResponseWriter, response RPCResponse) {
	body, err := rpcEncoding.Marshal(response)
	if err != nil {
		http.Error(writer, "workflow: could not encode RPC response", http.StatusInternalServerError)
		return
	}
	writer.Header().Set("Content-Type", RPCContentType)
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(body)
}

func dispatchRPC(ctx context.Context, world World, request RPCRequest) (cbor.RawMessage, error) {
	if request.RPCVersion != nil && *request.RPCVersion != RPCVersion {
		return nil, rpcVersionMismatchError{client: *request.RPCVersion, server: RPCVersion}
	}
	switch request.Method {
	case "world.deploymentId":
		return encodeRPC(world.DeploymentID())
	case "runs.get":
		input, err := decodeRPC[RunsGetInput](request.Input)
		if err != nil {
			return nil, err
		}
		value, err := world.RunsGet(ctx, input)
		if err != nil {
			return nil, err
		}
		return encodeRPC(value)
	case "runs.list":
		input, err := decodeRPC[RunsListInput](request.Input)
		if err != nil {
			return nil, err
		}
		value, err := world.RunsList(ctx, input)
		if err != nil {
			return nil, err
		}
		return encodeRPC(value)
	case "runs.waitForTerminalStatus":
		input, err := decodeRPC[RunsWaitForTerminalStatusInput](request.Input)
		if err != nil {
			return nil, err
		}
		input.Timeout = min(input.Timeout, maxRPCWait)
		value, err := world.RunsWaitForTerminalStatus(ctx, input)
		if err != nil {
			return nil, err
		}
		return encodeRPC(value)
	case "runs.setAttributes":
		input, err := decodeRPC[RunsSetAttributesInput](request.Input)
		if err != nil {
			return nil, err
		}
		err = world.RunsSetAttributes(ctx, input)
		if err != nil {
			return nil, err
		}
		return encodeRPC(struct{}{})
	case "runs.deleteDataMany":
		input, err := decodeRPC[RunsDeleteDataManyInput](request.Input)
		if err != nil {
			return nil, err
		}
		value, err := world.RunsDeleteDataMany(ctx, input)
		if err != nil {
			return nil, err
		}
		return encodeRPC(value)
	case "steps.get":
		input, err := decodeRPC[StepsGetInput](request.Input)
		if err != nil {
			return nil, err
		}
		value, err := world.StepsGet(ctx, input)
		if err != nil {
			return nil, err
		}
		return encodeRPC(value)
	case "steps.list":
		input, err := decodeRPC[StepsListInput](request.Input)
		if err != nil {
			return nil, err
		}
		value, err := world.StepsList(ctx, input)
		if err != nil {
			return nil, err
		}
		return encodeRPC(value)
	case "events.create":
		input, err := decodeRPC[EventsCreateInput](request.Input)
		if err != nil {
			return nil, err
		}
		value, err := world.EventsCreate(ctx, input)
		if err != nil {
			return nil, err
		}
		// ResolveData selects the replay events only. The created event is
		// always returned with its data.
		result, err := rpcEventResult(ctx, world, input.RunID, input.Event.CorrelationID, value)
		if err != nil {
			return nil, err
		}
		return encodeRPC(result)
	case "events.createRun":
		input, err := decodeRPC[EventsCreateRunInput](request.Input)
		if err != nil {
			return nil, err
		}
		// A run created without an ID gets one.
		if input.RunID == "" {
			input.RunID = NewRunID()
		}
		value, err := world.EventsCreateRun(ctx, input)
		if err != nil {
			return nil, err
		}
		result, err := rpcEventResult(ctx, world, input.RunID, "", value)
		if err != nil {
			return nil, err
		}
		return encodeRPC(result)
	case "events.createBatch":
		input, err := decodeRPC[EventsCreateBatchInput](request.Input)
		if err != nil {
			return nil, err
		}
		result, err := rpcEventBatch(ctx, world, input)
		if err != nil {
			return nil, err
		}
		return encodeRPC(result)
	case "events.get":
		input, err := decodeRPC[EventsGetInput](request.Input)
		if err != nil {
			return nil, err
		}
		value, err := world.EventsGet(ctx, input)
		if err != nil {
			return nil, err
		}
		return encodeRPC(value)
	case "events.list":
		input, err := decodeRPC[EventsListInput](request.Input)
		if err != nil {
			return nil, err
		}
		value, err := world.EventsList(ctx, input)
		if err != nil {
			return nil, err
		}
		return encodeRPC(value)
	case "events.listByCorrelationID":
		input, err := decodeRPC[EventsListByCorrelationIDInput](request.Input)
		if err != nil {
			return nil, err
		}
		value, err := world.EventsListByCorrelationID(ctx, input)
		if err != nil {
			return nil, err
		}
		return encodeRPC(value)
	case "hooks.get":
		input, err := decodeRPC[HooksGetInput](request.Input)
		if err != nil {
			return nil, err
		}
		value, err := world.HooksGet(ctx, input)
		if err != nil {
			return nil, err
		}
		return encodeRPC(value)
	case "hooks.getByToken":
		input, err := decodeRPC[HooksGetByTokenInput](request.Input)
		if err != nil {
			return nil, err
		}
		value, err := world.HooksGetByToken(ctx, input)
		if err != nil {
			return nil, err
		}
		return encodeRPC(value)
	case "hooks.list":
		input, err := decodeRPC[HooksListInput](request.Input)
		if err != nil {
			return nil, err
		}
		value, err := world.HooksList(ctx, input)
		if err != nil {
			return nil, err
		}
		return encodeRPC(value)
	case "queue.queue":
		input, err := decodeRPC[QueueInput](request.Input)
		if err != nil {
			return nil, err
		}
		value, err := world.Queue(ctx, input)
		if err != nil {
			return nil, err
		}
		return encodeRPC(value)
	case "queue.queueBatch":
		input, err := decodeRPC[QueueBatchInput](request.Input)
		if err != nil {
			return nil, err
		}
		value, err := world.QueueBatch(ctx, input)
		if err != nil {
			return nil, err
		}
		return encodeRPC(value)
	case "streams.write":
		input, err := decodeRPC[StreamsWriteInput](request.Input)
		if err != nil {
			return nil, err
		}
		err = world.StreamsWrite(ctx, input)
		if err != nil {
			return nil, err
		}
		return encodeRPC(struct{}{})
	case "streams.writeMulti":
		input, err := decodeRPC[StreamsWriteMultiInput](request.Input)
		if err != nil {
			return nil, err
		}
		err = world.StreamsWriteMulti(ctx, input)
		if err != nil {
			return nil, err
		}
		return encodeRPC(struct{}{})
	case "streams.close":
		input, err := decodeRPC[StreamsCloseInput](request.Input)
		if err != nil {
			return nil, err
		}
		err = world.StreamsClose(ctx, input)
		if err != nil {
			return nil, err
		}
		return encodeRPC(struct{}{})
	case "streams.getChunks":
		input, err := decodeRPC[StreamsGetChunksInput](request.Input)
		if err != nil {
			return nil, err
		}
		input.Wait = min(input.Wait, maxRPCWait)
		value, err := world.StreamsGetChunks(ctx, input)
		if err != nil {
			return nil, err
		}
		return encodeRPC(value)
	case "streams.getInfo":
		input, err := decodeRPC[StreamsGetInfoInput](request.Input)
		if err != nil {
			return nil, err
		}
		value, err := world.StreamsGetInfo(ctx, input)
		if err != nil {
			return nil, err
		}
		return encodeRPC(value)
	case "streams.list":
		input, err := decodeRPC[StreamsListInput](request.Input)
		if err != nil {
			return nil, err
		}
		value, err := world.StreamsList(ctx, input)
		if err != nil {
			return nil, err
		}
		return encodeRPC(value)
	case "analytics.runs.get", "analytics.runs.list", "analytics.attributes.list",
		"analytics.steps.get", "analytics.steps.list",
		"analytics.events.get", "analytics.events.getMany", "analytics.events.list", "analytics.events.listByCorrelationId",
		"analytics.hooks.get", "analytics.hooks.list",
		"analytics.waits.get", "analytics.waits.list":
		analytics := world.Analytics()
		if analytics == nil {
			return nil, errors.New("workflow: analytics capability is unavailable")
		}
		switch request.Method {
		case "analytics.runs.get":
			input, err := decodeRPC[AnalyticsRunsGetInput](request.Input)
			if err != nil {
				return nil, err
			}
			value, err := analytics.RunsGet(ctx, input)
			if err != nil {
				return nil, err
			}
			return encodeRPC(value)
		case "analytics.runs.list":
			input, err := decodeRPC[AnalyticsRunsListInput](request.Input)
			if err != nil {
				return nil, err
			}
			value, err := analytics.RunsList(ctx, input)
			if err != nil {
				return nil, err
			}
			return encodeRPC(value)
		case "analytics.attributes.list":
			input, err := decodeRPC[AnalyticsAttributesListInput](request.Input)
			if err != nil {
				return nil, err
			}
			value, err := analytics.AttributesList(ctx, input)
			if err != nil {
				return nil, err
			}
			return encodeRPC(value)
		case "analytics.steps.get":
			return analyticsRPC(ctx, request.Input, analytics.StepsGet)
		case "analytics.steps.list":
			return analyticsRPC(ctx, request.Input, analytics.StepsList)
		case "analytics.events.get":
			return analyticsRPC(ctx, request.Input, analytics.EventsGet)
		case "analytics.events.getMany":
			return analyticsRPC(ctx, request.Input, analytics.EventsGetMany)
		case "analytics.events.list":
			return analyticsRPC(ctx, request.Input, analytics.EventsList)
		case "analytics.events.listByCorrelationId":
			return analyticsRPC(ctx, request.Input, analytics.EventsListByCorrelationID)
		case "analytics.hooks.get":
			return analyticsRPC(ctx, request.Input, analytics.HooksGet)
		case "analytics.hooks.list":
			return analyticsRPC(ctx, request.Input, analytics.HooksList)
		case "analytics.waits.get":
			input, err := decodeRPC[AnalyticsWaitsGetInput](request.Input)
			if err != nil {
				return nil, err
			}
			value, err := analytics.WaitsGet(ctx, input)
			if err != nil {
				return nil, err
			}
			return encodeRPC(value)
		case "analytics.waits.list":
			input, err := decodeRPC[AnalyticsWaitsListInput](request.Input)
			if err != nil {
				return nil, err
			}
			value, err := analytics.WaitsList(ctx, input)
			if err != nil {
				return nil, err
			}
			return encodeRPC(value)
		default:
			return nil, fmt.Errorf("%w: unknown analytics method %q", errRPCInvalidInput, request.Method)
		}
	case "runs.encryptionKey":
		input, err := decodeRPC[RunKeysInput](request.Input)
		if err != nil {
			return nil, err
		}
		var key []byte
		if provider, ok := world.(EncryptionKeys); ok {
			if key, err = provider.EncryptionKeyForRun(ctx, input.RunID, input.DeploymentID); err != nil {
				return nil, err
			}
		}
		return encodeRPC(key)
	case "client.runs.newId":
		input, err := decodeRPC[ClientRunsNewIDInput](request.Input)
		if err != nil {
			return nil, err
		}
		return encodeRPC(newRunID(world, input))
	case "client.runs.startDeployment":
		input, err := decodeRPC[ClientRunsStartDeploymentInput](request.Input)
		if err != nil {
			return nil, err
		}
		deploymentID, err := StartDeploymentID(ctx, world, input.deployment())
		if err != nil {
			return nil, err
		}
		return encodeRPC(deploymentID)
	case "client.spec":
		return encodeRPC(ClientSpec{RPCVersion: RPCVersion, Capabilities: world.Capabilities()})
	case "client.runs.create":
		input, err := decodeRPC[ClientRunsCreateInput](request.Input)
		if err != nil {
			return nil, err
		}
		runID, err := CreateRun(ctx, world, input)
		if err != nil {
			return nil, err
		}
		return encodeRPC(runID)
	case "client.runs.cancel":
		input, err := decodeRPC[ClientRunsCancelInput](request.Input)
		if err != nil {
			return nil, err
		}
		if _, err := world.EventsCreate(ctx, EventsCreateInput{RunID: input.RunID, Event: EventsCreateEvent{EventType: EventTypeRunCancelled}}); err != nil {
			return nil, err
		}
		return encodeRPC(struct{}{})
	case "client.runs.wake":
		input, err := decodeRPC[ClientRunsWakeInput](request.Input)
		if err != nil {
			return nil, err
		}
		value, err := WakeRun(ctx, world, input.RunID, input.CorrelationIDs)
		if err != nil {
			return nil, err
		}
		return encodeRPC(value)
	case "client.streams.write":
		input, err := decodeRPC[StreamsWriteInput](request.Input)
		if err != nil {
			return nil, err
		}
		if err := world.StreamsWrite(ctx, input); err != nil {
			return nil, err
		}
		return encodeRPC(struct{}{})
	case "client.streams.writeMulti":
		input, err := decodeRPC[StreamsWriteMultiInput](request.Input)
		if err != nil {
			return nil, err
		}
		if err := world.StreamsWriteMulti(ctx, input); err != nil {
			return nil, err
		}
		return encodeRPC(struct{}{})
	case "client.streams.close":
		input, err := decodeRPC[StreamsCloseInput](request.Input)
		if err != nil {
			return nil, err
		}
		if err := world.StreamsClose(ctx, input); err != nil {
			return nil, err
		}
		return encodeRPC(struct{}{})
	case "client.hooks.resume":
		input, err := decodeRPC[ClientHooksResumeInput](request.Input)
		if err != nil {
			return nil, err
		}
		hook, err := world.HooksGetByToken(ctx, HooksGetByTokenInput{Token: input.Token})
		if err != nil {
			return nil, err
		}
		if _, err := ResumeHook(ctx, world, hook, input.Data, input.ResumeID); err != nil {
			return nil, err
		}
		return encodeRPC(struct{}{})
	default:
		return nil, fmt.Errorf("%w: unknown method %q", errRPCInvalidInput, request.Method)
	}
}

type (
	// ClientRunsCreateInput holds the arguments to the client.runs.create RPC
	// method, which creates a run and queues its first delivery. An empty
	// RunID is replaced with a new one.
	ClientRunsCreateInput struct {
		EventsCreateRunInput
		// Latest pins a run with no DeploymentID to the World's latest
		// deployment, as LatestDeployment does.
		Latest bool
	}
	// RunKeysInput holds the arguments to Client.RunKeys and the
	// runs.encryptionKey RPC method, which answers with the World's key for
	// the run.
	RunKeysInput struct {
		RunID        RunID
		DeploymentID string
	}
	// ClientRunsStartDeploymentInput holds the arguments to
	// Client.RunsStartDeployment and the client.runs.startDeployment RPC
	// method.
	ClientRunsStartDeploymentInput struct {
		// DeploymentID names the deployment, as DeploymentWithID does, and
		// Latest asks for LatestDeployment.
		DeploymentID string
		Latest       bool
	}
	// ClientRunsNewIDInput holds the arguments to Client.RunsNewID and the
	// client.runs.newId RPC method. Region is a Region's code, or "" to
	// leave it to the World.
	ClientRunsNewIDInput struct {
		Region Region
	}
	// ClientRunsCancelInput holds the arguments to the client.runs.cancel RPC
	// method, which cancels the run.
	ClientRunsCancelInput struct{ RunID RunID }
	// ClientRunsWakeInput holds the arguments to the client.runs.wake RPC
	// method, which calls WakeRun.
	ClientRunsWakeInput struct {
		RunID          RunID
		CorrelationIDs []string
	}
	// ClientHooksResumeInput holds the arguments to the client.hooks.resume
	// RPC method, which calls ResumeHook for the hook that owns Token with
	// Data.
	ClientHooksResumeInput struct {
		Token string
		Data  []byte
		// ResumeID names the logical resume, so a retried request records
		// its payload once. Empty starts a new one.
		ResumeID string `cbor:",omitempty"`
	}
)

// RPCEventResult is the output of the events.create and events.createRun RPC
// methods: the committed Event and the run, step, or hook it created or
// changed. Event is nil for an event accepted without appending, such as a
// run_started of a running run.
type RPCEventResult struct {
	Event       *Event `cbor:",omitempty"`
	Run         *Run   `cbor:",omitempty"`
	Step        *Step  `cbor:",omitempty"`
	Hook        *Hook  `cbor:",omitempty"`
	Wait        *Wait  `cbor:",omitempty"`
	StepCreated bool   `cbor:",omitempty"`
	// Allocated is the number of positions the write allocated in the log.
	Allocated   int
	InBandCount int
	// Events holds the events EventsCreateOptions selected. HasMore is set
	// with them, since they are never paged.
	Events  []Event `cbor:",omitempty"`
	HasMore *bool   `cbor:",omitempty"`
}

// RPCEventBatchResult is the output of the events.createBatch RPC method, with
// one outcome per event.
type RPCEventBatchResult struct {
	Results     []RPCBatchEventResult
	InBandCount int
	// Allocated is the number of positions the batch allocated.
	Allocated int
	// Events is the skipped-slot report of an in-band batch, and HasMore
	// reports that it is incomplete.
	Events  []Event `cbor:",omitempty"`
	HasMore *bool   `cbor:",omitempty"`
}

// RPCBatchEventResult is one event's outcome in an RPCEventBatchResult: Status
// 200 with the committed Event and the entity it changed, or the status and
// error code a single events.create would have failed with.
type RPCBatchEventResult struct {
	Status  int
	Error   string `cbor:",omitempty"`
	Message string `cbor:",omitempty"`
	Event   *Event `cbor:",omitempty"`
	Run     *Run   `cbor:",omitempty"`
	Step    *Step  `cbor:",omitempty"`
	Wait    *Wait  `cbor:",omitempty"`
}

// rpcEventResult reads the committed event of result and the entity it
// created or changed.
func rpcEventResult(ctx context.Context, world World, runID RunID, correlationID string, result EventResult) (RPCEventResult, error) {
	if result.EventID == "" {
		// An event accepted without appending, such as a run_started of a
		// running run, is answered with the run alone, as world-local's is.
		value := RPCEventResult{Allocated: result.AllocatedEventCount, InBandCount: result.InBandCount}
		run, err := world.RunsGet(ctx, RunsGetInput{RunID: runID})
		if err != nil {
			return RPCEventResult{}, err
		}
		value.Run = &run
		if result.Events != nil {
			hasMore := result.MoreEvents
			value.Events, value.HasMore = result.Events, &hasMore
		}
		return value, nil
	}
	event, err := world.EventsGet(ctx, EventsGetInput{RunID: runID, EventID: result.EventID})
	if err != nil {
		return RPCEventResult{}, err
	}
	value := RPCEventResult{Event: &event, Hook: result.Hook, Wait: result.Wait, StepCreated: result.StepCreated, Allocated: result.AllocatedEventCount, InBandCount: result.InBandCount}
	value.Run, value.Step, err = eventEntity(ctx, world, event, correlationID, result.Step)
	if err != nil {
		return RPCEventResult{}, err
	}
	if result.Events != nil {
		hasMore := result.MoreEvents
		value.Events, value.HasMore = result.Events, &hasMore
	}
	return value, nil
}

// eventEntity reads the run that a run event changed, or the step that a step
// event changed. step, when set, is the step the World already returned.
func eventEntity(ctx context.Context, world World, event Event, correlationID string, step *Step) (*Run, *Step, error) {
	switch event.EventType {
	case EventTypeRunCreated, EventTypeRunStarted, EventTypeRunCompleted, EventTypeRunFailed, EventTypeRunCancelled, EventTypeAttrSet:
		run, err := world.RunsGet(ctx, RunsGetInput{RunID: event.RunID})
		if err != nil {
			return nil, nil, err
		}
		return &run, nil, nil
	case EventTypeStepCreated, EventTypeStepStarted, EventTypeStepCompleted, EventTypeStepFailed, EventTypeStepRetrying:
		if step != nil {
			return nil, step, nil
		}
		value, err := world.StepsGet(ctx, StepsGetInput{RunID: event.RunID, StepID: StepID(correlationID)})
		if err != nil {
			return nil, nil, err
		}
		return nil, &value, nil
	}
	return nil, nil, nil
}

// rpcEventBatch appends a batch and reports each event's outcome: the
// committed event and the entity it changed, or the status and error code a
// single events.create would have failed with.
func rpcEventBatch(ctx context.Context, world World, input EventsCreateBatchInput) (RPCEventBatchResult, error) {
	batch, err := world.EventsCreateBatch(ctx, input)
	if err != nil {
		return RPCEventBatchResult{}, err
	}
	value := RPCEventBatchResult{Results: make([]RPCBatchEventResult, len(batch.Results)), InBandCount: batch.InBandCount, Allocated: batch.AllocatedEventCount}
	if batch.Events != nil {
		more := batch.MoreEvents
		value.Events, value.HasMore = batch.Events, &more
	}
	for index, item := range batch.Results {
		if item.Err != nil {
			failure := newRPCError(item.Err)
			value.Results[index] = RPCBatchEventResult{Status: rpcErrorStatus(failure.Code), Error: failure.Code, Message: failure.Message}
			continue
		}
		event, err := rpcEventResult(ctx, world, input.RunID, input.Events[index].CorrelationID, item.EventResult)
		if err != nil {
			return RPCEventBatchResult{}, err
		}
		value.Results[index] = RPCBatchEventResult{Status: http.StatusOK, Event: event.Event, Run: event.Run, Step: event.Step, Wait: event.Wait}
	}
	return value, nil
}

// rpcErrorStatus returns the HTTP status reported for an RPC error code.
func rpcErrorStatus(code string) int {
	switch code {
	case "not_found":
		return http.StatusNotFound
	case "invalid_argument":
		return http.StatusBadRequest
	case "conflict":
		return http.StatusConflict
	case "unimplemented":
		return http.StatusNotImplemented
	case "resource_exhausted":
		return http.StatusTooManyRequests
	case "too_early":
		return http.StatusTooEarly
	}
	return http.StatusInternalServerError
}

func newRPCError(err error) *RPCError {
	for _, kind := range rpcErrors {
		details, ok := kind.find(err)
		if !ok {
			continue
		}
		failure := &RPCError{Code: kind.code, Message: err.Error(), Sentinel: kind.name, Details: details}
		if conflict, ok := errors.AsType[*HookConflictError](err); ok {
			failure.ConflictingRunID = conflict.RunID
		}
		if throttle, ok := errors.AsType[*ThrottleError](err); ok {
			failure.RetryAfter = throttle.After
		}
		if early, ok := errors.AsType[*TooEarlyError](err); ok {
			failure.RetryAfter = early.RetryAfter
		}
		return failure
	}
	var rpcVersionMismatch rpcVersionMismatchError
	if errors.Is(err, errRPCInvalidInput) || errors.As(err, &rpcVersionMismatch) {
		return &RPCError{Code: "invalid_argument", Message: err.Error()}
	}
	return &RPCError{Code: "internal", Message: err.Error()}
}

func decodeRPC[T any](raw cbor.RawMessage) (T, error) {
	var value T
	if len(raw) == 0 {
		return value, fmt.Errorf("%w: missing input", errRPCInvalidInput)
	}
	if err := cbor.Unmarshal(raw, &value); err != nil {
		return value, fmt.Errorf("%w: decode input: %w", errRPCInvalidInput, err)
	}
	return value, nil
}

// rpcEncoding writes times as tagged RFC 3339 strings with nanoseconds, so
// JavaScript clients decode them as Dates and Go clients keep full precision.
var rpcEncoding = func() cbor.EncMode {
	mode, err := cbor.EncOptions{Time: cbor.TimeRFC3339Nano, TimeTag: cbor.EncTagRequired}.EncMode()
	if err != nil {
		panic(err)
	}
	return mode
}()

// analyticsRPC decodes an analytics call's input, calls read, and encodes
// its result.
func analyticsRPC[In, Out any](ctx context.Context, raw cbor.RawMessage, read func(context.Context, In) (Out, error)) (cbor.RawMessage, error) {
	input, err := decodeRPC[In](raw)
	if err != nil {
		return nil, err
	}
	value, err := read(ctx, input)
	if err != nil {
		return nil, err
	}
	return encodeRPC(value)
}

func encodeRPC(value any) (cbor.RawMessage, error) {
	encoded, err := rpcEncoding.Marshal(value)
	return cbor.RawMessage(encoded), err
}

// NewClientRunsStartDeploymentInput returns the RPC input naming deployment.
func NewClientRunsStartDeploymentInput(deployment Deployment) ClientRunsStartDeploymentInput {
	return ClientRunsStartDeploymentInput{DeploymentID: deployment.id, Latest: deployment.latest}
}

func (input ClientRunsStartDeploymentInput) deployment() Deployment {
	if input.DeploymentID != "" {
		return DeploymentWithID(input.DeploymentID)
	}
	return Deployment{latest: input.Latest}
}

// newRunID returns the ID of a new run of world in input's region.
func newRunID(world World, input ClientRunsNewIDInput) RunID {
	return RegionRunID(world, NewRunID(), input.Region)
}
