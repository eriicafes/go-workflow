package worlds

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"

	"github.com/fxamacker/cbor/v2"
)

// RPCContentType is the content type accepted and returned by HTTPHandler's
// RPC endpoint.
const RPCContentType = "application/cbor"

// RPCRequest is the CBOR envelope sent to an HTTPHandler RPC endpoint.
type RPCRequest struct {
	// Method names the operation, such as "runs.get".
	Method string `cbor:"method"`
	// Input is the CBOR-encoded input of the operation.
	Input cbor.RawMessage `cbor:"input"`
	// SpecVersion is the client's RPC protocol version. When set and
	// different from the World's, the request fails with invalid_argument.
	SpecVersion *int `cbor:"specVersion,omitempty"`
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
}

// specVersionMismatchError is private because a client can only act on the
// invalid_argument RPC response, and it is not a storage or runtime condition.
type specVersionMismatchError struct {
	client int
	server int
}

func (e *specVersionMismatchError) Error() string {
	return fmt.Sprintf("workflow: RPC spec version does not match world: client=%d server=%d", e.client, e.server)
}

func handleRPC(writer http.ResponseWriter, request *http.Request, world World) {
	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || mediaType != RPCContentType {
		http.Error(writer, "workflow: Content-Type must be application/cbor", http.StatusUnsupportedMediaType)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(writer, request.Body, 8<<20))
	if err != nil {
		writeRPC(writer, RPCResponse{Error: newRPCError(err)})
		return
	}
	var envelope RPCRequest
	if err := cbor.Unmarshal(body, &envelope); err != nil || envelope.Method == "" {
		if err == nil {
			err = fmt.Errorf("workflow: missing RPC method")
		}
		writeRPC(writer, RPCResponse{Error: newRPCError(err)})
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
	body, err := cbor.Marshal(response)
	if err != nil {
		http.Error(writer, "workflow: could not encode RPC response", http.StatusInternalServerError)
		return
	}
	writer.Header().Set("Content-Type", RPCContentType)
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(body)
}

func dispatchRPC(ctx context.Context, world World, request RPCRequest) (cbor.RawMessage, error) {
	if request.SpecVersion != nil && *request.SpecVersion != world.SpecVersion() {
		return nil, &specVersionMismatchError{client: *request.SpecVersion, server: world.SpecVersion()}
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
		return encodeRPC(value)
	case "events.createRun":
		input, err := decodeRPC[EventsCreateRunInput](request.Input)
		if err != nil {
			return nil, err
		}
		value, err := world.EventsCreateRun(ctx, input)
		if err != nil {
			return nil, err
		}
		return encodeRPC(value)
	case "events.createBatch":
		input, err := decodeRPC[EventsCreateBatchInput](request.Input)
		if err != nil {
			return nil, err
		}
		value, err := world.EventsCreateBatch(ctx, input)
		if err != nil {
			return nil, err
		}
		return encodeRPC(value)
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
	case "queue.cancel":
		input, err := decodeRPC[QueueCancelInput](request.Input)
		if err != nil {
			return nil, err
		}
		if err := world.QueueCancel(ctx, input); err != nil {
			return nil, err
		}
		return encodeRPC(struct{}{})
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
	case "streams.createWriteSession":
		input, err := decodeRPC[StreamsCreateWriteSessionInput](request.Input)
		if err != nil {
			return nil, err
		}
		session, err := world.StreamsCreateWriteSession(ctx, input)
		if err != nil {
			return nil, err
		}
		return encodeRPC(session.ID())
	case "streams.writeSession":
		input, err := decodeRPC[StreamsWriteSessionInput](request.Input)
		if err != nil {
			return nil, err
		}
		if err := world.StreamsWriteSession(ctx, input); err != nil {
			return nil, err
		}
		return encodeRPC(struct{}{})
	case "streams.closeWriteSession":
		input, err := decodeRPC[StreamsCloseWriteSessionInput](request.Input)
		if err != nil {
			return nil, err
		}
		if err := world.StreamsCloseWriteSession(ctx, input); err != nil {
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
			input, err := decodeRPC[StepsGetInput](request.Input)
			if err != nil {
				return nil, err
			}
			value, err := world.StepsGet(ctx, input)
			if err != nil {
				return nil, err
			}
			return encodeRPC(value)
		case "analytics.steps.list":
			input, err := decodeRPC[StepsListInput](request.Input)
			if err != nil {
				return nil, err
			}
			value, err := world.StepsList(ctx, input)
			if err != nil {
				return nil, err
			}
			return encodeRPC(value)
		case "analytics.events.get":
			input, err := decodeRPC[EventsGetInput](request.Input)
			if err != nil {
				return nil, err
			}
			value, err := world.EventsGet(ctx, input)
			if err != nil {
				return nil, err
			}
			return encodeRPC(value)
		case "analytics.events.getMany":
			input, err := decodeRPC[rpcEventsGetManyInput](request.Input)
			if err != nil {
				return nil, err
			}
			values := make([]Event, 0, len(input.EventIDs))
			for _, eventID := range input.EventIDs {
				value, err := world.EventsGet(ctx, EventsGetInput{RunID: input.RunID, EventID: eventID})
				if errors.Is(err, ErrEventNotFound) {
					continue
				}
				if err != nil {
					return nil, err
				}
				values = append(values, value)
			}
			return encodeRPC(values)
		case "analytics.events.list":
			input, err := decodeRPC[EventsListInput](request.Input)
			if err != nil {
				return nil, err
			}
			value, err := world.EventsList(ctx, input)
			if err != nil {
				return nil, err
			}
			return encodeRPC(value)
		case "analytics.events.listByCorrelationId":
			input, err := decodeRPC[EventsListByCorrelationIDInput](request.Input)
			if err != nil {
				return nil, err
			}
			value, err := world.EventsListByCorrelationID(ctx, input)
			if err != nil {
				return nil, err
			}
			return encodeRPC(value)
		case "analytics.hooks.get":
			input, err := decodeRPC[HooksGetInput](request.Input)
			if err != nil {
				return nil, err
			}
			value, err := world.HooksGet(ctx, input)
			if err != nil {
				return nil, err
			}
			return encodeRPC(value)
		case "analytics.hooks.list":
			input, err := decodeRPC[HooksListInput](request.Input)
			if err != nil {
				return nil, err
			}
			value, err := world.HooksList(ctx, input)
			if err != nil {
				return nil, err
			}
			return encodeRPC(value)
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
			return nil, fmt.Errorf("workflow: unknown analytics RPC method %q", request.Method)
		}
	case "client.spec":
		return encodeRPC(ClientSpec{SpecVersion: world.SpecVersion(), Capabilities: world.Capabilities()})
	case "client.runs.create":
		input, err := decodeRPC[ClientRunsCreateInput](request.Input)
		if err != nil {
			return nil, err
		}
		if input.RunID == "" {
			input.RunID = NewRunID()
		}
		if input.DeploymentID == "" {
			input.DeploymentID = world.DeploymentID()
		}
		if _, err := world.EventsCreateRun(ctx, input); err != nil {
			return nil, err
		}
		if _, err := world.Queue(ctx, QueueInput{RunID: input.RunID, IdempotencyKey: string(input.RunID), DeploymentID: input.DeploymentID}); err != nil {
			return nil, err
		}
		return encodeRPC(input.RunID)
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
		if _, err := ResumeHook(ctx, world, hook, input.Data); err != nil {
			return nil, err
		}
		return encodeRPC(struct{}{})
	default:
		return nil, fmt.Errorf("workflow: unknown RPC method %q", request.Method)
	}
}

type (
	rpcEventsGetManyInput struct {
		RunID    RunID
		EventIDs []EventID
	}
	// ClientRunsCreateInput holds the arguments to the client.runs.create RPC
	// method, which creates a run and queues its first delivery. An empty
	// RunID is replaced with a new one.
	ClientRunsCreateInput = EventsCreateRunInput
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
	}
)

func newRPCError(err error) *RPCError {
	var specVersionMismatch *specVersionMismatchError
	code := "internal"
	switch {
	case errors.Is(err, ErrWorkflowNotFound), errors.Is(err, ErrRunNotFound), errors.Is(err, ErrStepNotFound), errors.Is(err, ErrEventNotFound), errors.Is(err, ErrHookNotFound), errors.Is(err, ErrQueueMessageNotFound):
		code = "not_found"
	case errors.Is(err, ErrEventInvalid), errors.As(err, &specVersionMismatch):
		code = "invalid_argument"
	case errors.Is(err, ErrRunAlreadyExists), errors.Is(err, ErrRunTerminal), errors.Is(err, ErrStepNotStartable), errors.Is(err, ErrStreamLocked), errors.Is(err, ErrHookConflict), errors.Is(err, ErrInBandSuperseded):
		code = "conflict"
	case errors.Is(err, context.Canceled):
		code = "cancelled"
	case errors.Is(err, context.DeadlineExceeded):
		code = "deadline_exceeded"
	}
	return &RPCError{Code: code, Message: err.Error()}
}

func decodeRPC[T any](raw cbor.RawMessage) (T, error) {
	var value T
	if len(raw) == 0 {
		return value, fmt.Errorf("workflow: missing RPC input")
	}
	if err := cbor.Unmarshal(raw, &value); err != nil {
		return value, fmt.Errorf("workflow: decode RPC input: %w", err)
	}
	return value, nil
}

func encodeRPC(value any) (cbor.RawMessage, error) {
	encoded, err := cbor.Marshal(value)
	return cbor.RawMessage(encoded), err
}
