package worlds

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"time"
)

// A webhook's metadata says how it responds: 202 Accepted, a static Response,
// or, with respondWith "manual", the Response a step writes to the request's
// response stream. Each request resumes the webhook's hook with a Request
// whose body is a byte stream of the run, which only steps read.

const (
	// webhookRequestMaxBytes bounds a webhook request body.
	webhookRequestMaxBytes = 128 << 10
	// webhookStreamPollWait is how long one read of a response stream waits.
	webhookStreamPollWait = time.Second
	// byteStreamFraming names the length-prefixed framing of a byte stream.
	byteStreamFraming = "framed-v1"
)

// StreamRef refers to a stream of a run, as a payload's ReadableStream and
// WritableStream values do. A ReadableStream made from a body in memory holds
// the body in BodyInit instead of naming a stream.
type StreamRef struct {
	Name string
	// Type is "bytes" for a byte stream, and empty for a stream of values.
	Type string
	// Framing is "framed-v1" for a byte stream whose chunks are each
	// prefixed with their big-endian u32 length.
	Framing string
	// BodyInit is the body of a ReadableStream made from it.
	BodyInit []byte
}

func (r StreamRef) reduced(typ string) PayloadReduced {
	if typ == "ReadableStream" && r.Name == "" {
		return PayloadReduced{Type: typ, Value: map[string]any{"bodyInit": r.BodyInit}}
	}
	value := map[string]any{"name": r.Name}
	if r.Type != "" {
		value["type"] = r.Type
	}
	if r.Framing != "" {
		value["framing"] = r.Framing
	}
	return PayloadReduced{Type: typ, Value: value}
}

func streamRefOf(value any) (*StreamRef, error) {
	reduced, ok := value.(PayloadReduced)
	if !ok {
		if value == nil {
			return nil, nil
		}
		return nil, fmt.Errorf("%w: stream reference is %T", ErrPayloadInvalid, value)
	}
	fields, _ := reduced.Value.(map[string]any)
	ref := &StreamRef{}
	ref.Name, _ = fields["name"].(string)
	ref.Type, _ = fields["type"].(string)
	ref.Framing, _ = fields["framing"].(string)
	switch body := fields["bodyInit"].(type) {
	case []byte:
		ref.BodyInit = body
	case string:
		ref.BodyInit = []byte(body)
	}
	return ref, nil
}

// WebhookRequest is the Workflow SDK's Request value that resumes a
// webhook's hook.
type WebhookRequest struct {
	Method string
	URL    string
	Header http.Header
	// Body is the request body's byte stream, or nil for a request without
	// one.
	Body *StreamRef
	// ResponseWritable is the stream a manual webhook's response is written
	// to, or nil for a webhook that does not respond manually.
	ResponseWritable *StreamRef
}

// MarshalPayload encodes the request as a Request value.
func (r WebhookRequest) MarshalPayload() (any, error) {
	value := map[string]any{"method": r.Method, "url": r.URL, "headers": r.Header, "body": nil, "duplex": "half"}
	if value["headers"] == nil {
		value["headers"] = http.Header{}
	}
	if r.Body != nil {
		value["body"] = r.Body.reduced("ReadableStream")
	}
	if r.ResponseWritable != nil {
		value["responseWritable"] = r.ResponseWritable.reduced("WritableStream")
	}
	return PayloadReduced{Type: "Request", Value: value}, nil
}

// UnmarshalPayload decodes a Request value.
func (r *WebhookRequest) UnmarshalPayload(decode func(any) error) error {
	var reduced PayloadReduced
	if err := decode(&reduced); err != nil {
		return err
	}
	fields, ok := reduced.Value.(map[string]any)
	if reduced.Type != "Request" || !ok {
		return fmt.Errorf("%w: %s is not a Request", ErrPayloadInvalid, reduced.Type)
	}
	r.Method, _ = fields["method"].(string)
	r.URL, _ = fields["url"].(string)
	r.Header, _ = fields["headers"].(http.Header)
	var err error
	if r.Body, err = streamRefOf(fields["body"]); err != nil {
		return err
	}
	r.ResponseWritable, err = streamRefOf(fields["responseWritable"])
	return err
}

// WebhookResponse is the Workflow SDK's Response value a webhook answers
// with. A zero Status is sent as 200.
type WebhookResponse struct {
	Status     int
	StatusText string
	Header     http.Header
	// Body is the response body, a ReadableStream made from it or naming a
	// byte stream of the run.
	Body *StreamRef
}

// MarshalPayload encodes the response as a Response value.
func (r WebhookResponse) MarshalPayload() (any, error) {
	status := r.Status
	if status == 0 {
		status = http.StatusOK
	}
	statusText := r.StatusText
	if statusText == "" {
		statusText = http.StatusText(status)
	}
	header := r.Header
	if header == nil {
		header = http.Header{}
	}
	value := map[string]any{"type": "default", "url": "", "status": status, "statusText": statusText, "headers": header, "body": nil, "redirected": false}
	if r.Body != nil {
		value["body"] = r.Body.reduced("ReadableStream")
	}
	return PayloadReduced{Type: "Response", Value: value}, nil
}

// UnmarshalPayload decodes a Response value.
func (r *WebhookResponse) UnmarshalPayload(decode func(any) error) error {
	var reduced PayloadReduced
	if err := decode(&reduced); err != nil {
		return err
	}
	fields, ok := reduced.Value.(map[string]any)
	if reduced.Type != "Response" || !ok {
		return fmt.Errorf("%w: %s is not a Response", ErrPayloadInvalid, reduced.Type)
	}
	if status, ok := fields["status"].(float64); ok {
		r.Status = int(status)
	}
	r.StatusText, _ = fields["statusText"].(string)
	r.Header, _ = fields["headers"].(http.Header)
	var err error
	r.Body, err = streamRefOf(fields["body"])
	return err
}

// WebhookMetadata is a webhook's metadata as it is recorded: respondWith is
// "manual" or the static Response.
// A webhook that answers 202 Accepted has no metadata.
type WebhookMetadata struct {
	// Manual holds each request open until a step responds to it.
	Manual bool
	// Response is sent to every request when set.
	Response *WebhookResponse
}

// MarshalPayload encodes the metadata as {respondWith}.
func (m WebhookMetadata) MarshalPayload() (any, error) {
	if m.Manual {
		return map[string]any{"respondWith": "manual"}, nil
	}
	return map[string]any{"respondWith": m.Response}, nil
}

// UnmarshalPayload decodes {respondWith}.
func (m *WebhookMetadata) UnmarshalPayload(decode func(any) error) error {
	var fields struct {
		RespondWith any `json:"respondWith"`
	}
	if err := decode(&fields); err != nil {
		return err
	}
	switch respondWith := fields.RespondWith.(type) {
	case nil:
	case string:
		if respondWith != "manual" {
			return fmt.Errorf("%w: respondWith %q", ErrPayloadInvalid, respondWith)
		}
		m.Manual = true
	case PayloadReduced:
		m.Response = &WebhookResponse{}
		return decode(&struct {
			RespondWith *WebhookResponse `json:"respondWith"`
		}{m.Response})
	default:
		return fmt.Errorf("%w: respondWith is %T", ErrPayloadInvalid, respondWith)
	}
	return nil
}

// WebhookHandler serves a webhook's requests. It writes the request body to a
// byte stream of the run, resumes the webhook's hook with a Request referring
// to it, and responds as the webhook's metadata says: 202 Accepted, its
// static Response, or the Response a step writes for the request. It
// responds 404 when no active webhook owns the token and 413 when the body
// exceeds 128 KiB.
//
// The token is read from a {token} path wildcard when the handler is mounted
// with one, otherwise from the last path segment. Mount it at
// <prefix>/webhooks/{token} to match the URLs webhooks are created with.
func WebhookHandler(world World) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := r.PathValue("token")
		if token == "" {
			token = path.Base(r.URL.Path)
		}
		if token == "" || token == "/" || token == "." {
			http.Error(w, ErrHookNotFound.Error(), http.StatusNotFound)
			return
		}
		ctx := r.Context()
		hook, err := world.HooksGetByToken(ctx, HooksGetByTokenInput{Token: token})
		if errors.Is(err, ErrHookNotFound) || err == nil && !hook.IsWebhook {
			http.Error(w, ErrHookNotFound.Error(), http.StatusNotFound)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		keys, err := RunKeysFor(ctx, world, hook.RunID, hook.ResumeContext.DeploymentID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		var metadata WebhookMetadata
		if err := DecodePayload(hook.Metadata, keys, &metadata); err != nil {
			http.Error(w, fmt.Errorf("workflow: decode webhook metadata: %w", err).Error(), http.StatusInternalServerError)
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, webhookRequestMaxBytes))
		if err != nil {
			var maxBytesError *http.MaxBytesError
			if errors.As(err, &maxBytesError) {
				http.Error(w, ErrWebhookRequestTooLarge.Error(), http.StatusRequestEntityTooLarge)
				return
			}
			http.Error(w, fmt.Errorf("workflow: read webhook body: %w", err).Error(), http.StatusBadRequest)
			return
		}
		request := WebhookRequest{Method: r.Method, URL: r.URL.String(), Header: r.Header.Clone()}
		if len(body) > 0 {
			request.Body = &StreamRef{Name: NewStreamName(), Type: "bytes", Framing: byteStreamFraming}
			if err := writeByteStream(ctx, world, hook.RunID, request.Body.Name, body); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
		if metadata.Manual {
			request.ResponseWritable = &StreamRef{Name: NewStreamName()}
		}
		options, err := HookPayloadOptions(ctx, NewClient(world), hook)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		payload, err := EncodePayload(request, options)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if _, err := ResumeHook(ctx, world, hook, payload, ""); err != nil {
			status := http.StatusInternalServerError
			switch {
			case errors.Is(err, ErrHookForceClaimed):
				status = http.StatusServiceUnavailable
			case errors.Is(err, ErrRunTerminal), errors.Is(err, ErrHookNotActive):
				// Another run that took the token over is served by its own
				// requests, so this one is refused rather than redirected.
				status, err = http.StatusNotFound, ErrHookNotFound
				owner, lookupErr := world.HooksGetByToken(ctx, HooksGetByTokenInput{Token: token})
				switch {
				case lookupErr == nil && owner.HookID != hook.HookID:
					status, err = http.StatusServiceUnavailable, ErrHookForceClaimed
				case lookupErr != nil && !errors.Is(lookupErr, ErrHookNotFound):
					status, err = http.StatusInternalServerError, lookupErr
				}
			}
			http.Error(w, err.Error(), status)
			return
		}
		switch {
		case metadata.Manual:
			response, err := readWebhookResponse(ctx, world, hook.RunID, request.ResponseWritable.Name, keys)
			if err != nil {
				status := http.StatusInternalServerError
				if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
					status = http.StatusGatewayTimeout
				}
				http.Error(w, err.Error(), status)
				return
			}
			writeWebhookResponse(ctx, w, world, hook.RunID, response)
		case metadata.Response != nil:
			writeWebhookResponse(ctx, w, world, hook.RunID, *metadata.Response)
		default:
			w.WriteHeader(http.StatusAccepted)
		}
	})
}

// writeByteStream writes data to a new byte stream of runID in one framed
// chunk, and closes it.
func writeByteStream(ctx context.Context, world World, runID RunID, name string, data []byte) error {
	frame := binary.BigEndian.AppendUint32(make([]byte, 0, 4+len(data)), uint32(len(data)))
	if err := world.StreamsWrite(ctx, StreamsWriteInput{RunID: runID, Name: name, Chunk: append(frame, data...)}); err != nil {
		return err
	}
	return world.StreamsClose(ctx, StreamsCloseInput{RunID: runID, Name: name})
}

// WriteWebhookResponse writes response as the response to a manual
// webhook's request, to its ResponseWritable stream: the response's body,
// when it has one, as a byte stream of runID, then the Response as the
// stream's frame, encrypted with keys. The request answers with the first
// response written.
func WriteWebhookResponse(ctx context.Context, world World, runID RunID, request WebhookRequest, response WebhookResponse, keys *RunKeys) error {
	if request.ResponseWritable == nil {
		return errors.New("workflow: webhook does not use a manual response")
	}
	frame, err := EncodeStreamFrame(response, keys, nil)
	if err != nil {
		return err
	}
	name := request.ResponseWritable.Name
	if err := world.StreamsWrite(ctx, StreamsWriteInput{RunID: runID, Name: name, Chunk: frame}); err != nil {
		if errors.Is(err, ErrStreamClosed) {
			// An earlier attempt responded.
			return nil
		}
		return err
	}
	return world.StreamsClose(ctx, StreamsCloseInput{RunID: runID, Name: name})
}

// readWebhookResponse waits for the first response written to a manual
// webhook request's response stream.
func readWebhookResponse(ctx context.Context, world World, runID RunID, name string, keys *RunKeys) (WebhookResponse, error) {
	cursor := ""
	for {
		page, err := world.StreamsGetChunks(ctx, StreamsGetChunksInput{RunID: runID, Name: name, Cursor: cursor, Wait: webhookStreamPollWait})
		if err != nil {
			return WebhookResponse{}, err
		}
		for _, chunk := range page.Data {
			payloads, err := SplitStreamFrames(chunk.Data)
			if err != nil {
				return WebhookResponse{}, err
			}
			for _, payload := range payloads {
				var response WebhookResponse
				if err := DecodePayload(payload, keys, &response); err != nil {
					return WebhookResponse{}, fmt.Errorf("workflow: decode webhook response: %w", err)
				}
				return response, nil
			}
		}
		if page.Done && !page.HasMore {
			return WebhookResponse{}, errors.New("workflow: the run did not send a webhook response")
		}
		cursor = page.Cursor
		if page.RetryAfter > 0 {
			timer := time.NewTimer(page.RetryAfter)
			select {
			case <-ctx.Done():
				timer.Stop()
				return WebhookResponse{}, ctx.Err()
			case <-timer.C:
			}
		}
		if err := ctx.Err(); err != nil {
			return WebhookResponse{}, err
		}
	}
}

func writeWebhookResponse(ctx context.Context, w http.ResponseWriter, world World, runID RunID, response WebhookResponse) {
	var body []byte
	if response.Body != nil {
		if response.Body.Name == "" {
			body = response.Body.BodyInit
		} else {
			var err error
			if body, err = ReadByteStream(ctx, world, runID, *response.Body); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
	}
	for key, values := range response.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	status := response.Status
	if status == 0 {
		status = http.StatusOK
	}
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// ReadByteStream returns the whole of a byte stream of runID once it is
// closed, unframing a framed one, or the body of a ReadableStream made from
// one in memory.
func ReadByteStream(ctx context.Context, reader interface {
	StreamsGetChunks(context.Context, StreamsGetChunksInput) (StreamChunksResult, error)
}, runID RunID, ref StreamRef) ([]byte, error) {
	if ref.Name == "" {
		return bytes.Clone(ref.BodyInit), nil
	}
	var out []byte
	cursor := ""
	for {
		page, err := reader.StreamsGetChunks(ctx, StreamsGetChunksInput{RunID: runID, Name: ref.Name, Cursor: cursor, Wait: webhookStreamPollWait})
		if err != nil {
			return nil, err
		}
		for _, chunk := range page.Data {
			data := chunk.Data
			if ref.Framing != byteStreamFraming {
				out = append(out, data...)
				continue
			}
			for len(data) > 0 {
				if len(data) < 4 {
					return nil, fmt.Errorf("%w: truncated byte stream frame", ErrPayloadInvalid)
				}
				size := binary.BigEndian.Uint32(data)
				if int(size) > len(data)-4 {
					return nil, fmt.Errorf("%w: byte stream frame of %d bytes", ErrPayloadInvalid, size)
				}
				out = append(out, data[4:4+size]...)
				data = data[4+size:]
			}
		}
		if page.Done && !page.HasMore {
			return out, nil
		}
		cursor = page.Cursor
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
}
