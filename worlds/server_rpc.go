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
	Method string          `cbor:"method"`
	Input  cbor.RawMessage `cbor:"input"`
}

// RPCResponse is the CBOR envelope returned by an HTTPHandler RPC endpoint.
type RPCResponse struct {
	Output cbor.RawMessage `cbor:"output,omitempty"`
	Error  *RPCError       `cbor:"error,omitempty"`
}

// RPCError describes a failed RPC request.
type RPCError struct {
	Code    string `cbor:"code"`
	Message string `cbor:"message"`
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
	switch request.Method {
	case "world.capabilities":
		return encodeRPC(world.Capabilities())
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
	case "analytics.runs.get", "analytics.runs.list", "analytics.attributes.list":
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
		default:
			input, err := decodeRPC[AnalyticsAttributesListInput](request.Input)
			if err != nil {
				return nil, err
			}
			value, err := analytics.AttributesList(ctx, input)
			if err != nil {
				return nil, err
			}
			return encodeRPC(value)
		}
	case "client.runs.create":
		input, err := decodeRPC[ClientRunsCreateInput](request.Input)
		if err != nil {
			return nil, err
		}
		if input.RunID == "" {
			input.RunID = NewRunID()
		}
		if _, err := world.EventsCreateRun(ctx, input); err != nil {
			return nil, err
		}
		if _, err := world.Queue(ctx, QueueInput{RunID: input.RunID, IdempotencyKey: string(input.RunID)}); err != nil {
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
		result, err := world.EventsCreate(ctx, EventsCreateInput{RunID: hook.RunID, Event: EventsCreateEvent{EventType: EventTypeHookReceived, CorrelationID: string(hook.HookID), Data: input.Data}})
		if err != nil {
			return nil, err
		}
		if _, err := world.Queue(ctx, QueueInput{RunID: hook.RunID, IdempotencyKey: string(result.EventID)}); err != nil {
			return nil, err
		}
		return encodeRPC(struct{}{})
	default:
		return nil, fmt.Errorf("workflow: unknown RPC method %q", request.Method)
	}
}

type (
	ClientRunsCreateInput = EventsCreateRunInput
	ClientRunsCancelInput struct{ RunID RunID }
	ClientRunsWakeInput   struct {
		RunID          RunID
		CorrelationIDs []string
	}
	ClientHooksResumeInput struct {
		Token string
		Data  []byte
	}
)

func newRPCError(err error) *RPCError {
	code := "internal"
	switch {
	case errors.Is(err, ErrWorkflowNotFound), errors.Is(err, ErrRunNotFound), errors.Is(err, ErrStepNotFound), errors.Is(err, ErrHookNotFound), errors.Is(err, ErrQueueMessageNotFound):
		code = "not_found"
	case errors.Is(err, ErrEventInvalid):
		code = "invalid_argument"
	case errors.Is(err, ErrRunTerminal), errors.Is(err, ErrStepNotStartable):
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
