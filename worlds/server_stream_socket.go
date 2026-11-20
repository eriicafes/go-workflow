package worlds

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/fxamacker/cbor/v2"
)

// Stream writes can travel over a WebSocket held for a writer's lifetime, in
// the Workflow SDK's v1 stream socket protocol. Every message is a frame:
//
//	frame := u32_be(meta_len) || cbor(meta) || u32_be(body_len) || body
//
// A write's meta is {type: "write", reqId, chunkSeq, numChunks}, and its body
// the chunks, each a u32_be length and its bytes. A close's meta is {type:
// "close", reqId} with an empty body. The server answers each request with
// {type: "write_ack" or "close_ack", reqId}, or {type: "error", reqId,
// status, message, retryAfter}, and may send {type: "drain", reason, graceMs}
// before it closes the socket. chunkSeq is the writer's own count of the
// chunks it sent before the write, which servers observe but do not enforce.

const (
	// streamSocketRoute is the socket route under an HTTP prefix.
	streamSocketRoute = "/websockets/v1/runs/{runId}/streams/{name...}"
	// streamSocketDialTimeout bounds opening a socket. Writes go through
	// the fallback meanwhile.
	streamSocketDialTimeout = 10 * time.Second
	// streamSocketMessageBytes is the most chunk bytes one write message
	// carries. A larger chunk goes alone.
	streamSocketMessageBytes = 1 << 20
	// streamSocketChunksPerWrite is the most chunks one write message
	// carries.
	streamSocketChunksPerWrite = 1000
	// streamSocketReadLimit bounds one message the server reads.
	streamSocketReadLimit = 16 << 20
)

type streamSocketWrite struct {
	Type      string `cbor:"type"`
	ReqID     int    `cbor:"reqId"`
	ChunkSeq  int    `cbor:"chunkSeq"`
	NumChunks int    `cbor:"numChunks"`
}

type streamSocketClose struct {
	Type  string `cbor:"type"`
	ReqID int    `cbor:"reqId"`
}

// streamSocketReply is any message the server sends.
type streamSocketReply struct {
	Type       string `cbor:"type"`
	ReqID      *int   `cbor:"reqId,omitempty"`
	Status     int    `cbor:"status,omitempty"`
	Message    string `cbor:"message,omitempty"`
	RetryAfter string `cbor:"retryAfter,omitempty"`
	Reason     string `cbor:"reason,omitempty"`
	GraceMs    int    `cbor:"graceMs,omitempty"`
}

func encodeSocketFrame(meta any, body []byte) ([]byte, error) {
	encoded, err := cbor.Marshal(meta)
	if err != nil {
		return nil, err
	}
	frame := binary.BigEndian.AppendUint32(nil, uint32(len(encoded)))
	frame = append(frame, encoded...)
	frame = binary.BigEndian.AppendUint32(frame, uint32(len(body)))
	return append(frame, body...), nil
}

// decodeSocketFrame splits one whole frame into its meta and body.
func decodeSocketFrame(data []byte) (cbor.RawMessage, []byte, error) {
	if len(data) < 4 {
		return nil, nil, errors.New("workflow: stream socket frame is too short")
	}
	metaEnd := 4 + int(binary.BigEndian.Uint32(data))
	if metaEnd+4 > len(data) || metaEnd < 4 {
		return nil, nil, errors.New("workflow: stream socket frame is too short")
	}
	bodyEnd := metaEnd + 4 + int(binary.BigEndian.Uint32(data[metaEnd:]))
	if bodyEnd != len(data) || bodyEnd < metaEnd+4 {
		return nil, nil, errors.New("workflow: stream socket frame length does not match")
	}
	return cbor.RawMessage(data[4:metaEnd]), data[metaEnd+4 : bodyEnd], nil
}

func encodeSocketChunks(chunks [][]byte) []byte {
	var body []byte
	for _, chunk := range chunks {
		body = binary.BigEndian.AppendUint32(body, uint32(len(chunk)))
		body = append(body, chunk...)
	}
	return body
}

func decodeSocketChunks(body []byte, count int) ([][]byte, error) {
	chunks := make([][]byte, 0, count)
	for len(body) > 0 {
		if len(body) < 4 {
			return nil, errors.New("workflow: stream socket chunk is truncated")
		}
		size := int(binary.BigEndian.Uint32(body))
		if size > len(body)-4 {
			return nil, errors.New("workflow: stream socket chunk is truncated")
		}
		chunks = append(chunks, body[4:4+size])
		body = body[4+size:]
	}
	if len(chunks) != count {
		return nil, fmt.Errorf("workflow: stream socket write declares %d chunks but carries %d", count, len(chunks))
	}
	return chunks, nil
}

// StreamSocketURL returns the URL of the stream socket for a writer of the
// stream name of runID, under base, an http or https URL such as a World's
// BaseURL. The default stream, whose name is empty, ends the path with a
// slash.
func StreamSocketURL(base string, runID RunID, name, writerID string) (string, error) {
	parsed, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	scheme := map[string]string{"https": "wss", "http": "ws"}[parsed.Scheme]
	if scheme == "" || parsed.Host == "" {
		return "", fmt.Errorf("workflow: stream socket base URL must be an absolute http or https URL: %q", base)
	}
	return scheme + "://" + parsed.Host + strings.TrimRight(parsed.EscapedPath(), "/") +
		"/websockets/v1/runs/" + url.PathEscape(string(runID)) + "/streams/" + url.PathEscape(name) +
		"?" + url.Values{"writerId": {writerID}}.Encode(), nil
}

// StreamSocketOptions configures OpenStreamSocketWriter.
type StreamSocketOptions struct {
	// URL is the stream socket's ws:// or wss:// URL (see StreamSocketURL).
	URL string
	// Header is sent with the upgrade request.
	Header http.Header
	// HTTPClient sends the upgrade request. Nil uses http.DefaultClient.
	HTTPClient *http.Client
	// Fallback writes while the socket opens, when the server declines it
	// or it fails, and after the server drains it. It is required.
	Fallback StreamWriter
	// OnDecline is called when the server answers the upgrade with an
	// ordinary HTTP response, so a caller can stop trying for a while.
	OnDecline func()
}

// OpenStreamSocketWriter returns a writer that opens a stream socket in the
// background and writes through Fallback until it is open, as the Workflow
// SDK's writers do. A write whose socket fails before the server
// acknowledges it is sent again through Fallback, so a chunk may be stored
// twice but is never lost.
func OpenStreamSocketWriter(ctx context.Context, options StreamSocketOptions) StreamWriter {
	if options.HTTPClient == nil {
		options.HTTPClient = http.DefaultClient
	}
	dialCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), streamSocketDialTimeout)
	writer := &socketStreamWriter{fallback: options.Fallback, opened: make(chan struct{}), cancelDial: cancel}
	go func() {
		defer close(writer.opened)
		defer cancel()
		conn, response, err := websocket.Dial(dialCtx, options.URL, &websocket.DialOptions{HTTPClient: options.HTTPClient, HTTPHeader: options.Header})
		if err != nil {
			if response != nil && response.StatusCode != http.StatusSwitchingProtocols && options.OnDecline != nil {
				options.OnDecline()
			}
			return
		}
		writer.conn = conn
	}()
	return writer
}

type socketStreamWriter struct {
	fallback   StreamWriter
	opened     chan struct{}
	cancelDial context.CancelFunc
	// mu serializes the writer's calls. conn is the open socket, set by the
	// dial before opened closes, and cleared when it fails or drains.
	mu       sync.Mutex
	conn     *websocket.Conn
	reqID    int
	chunkSeq int
	draining bool
	done     bool
}

// socket returns the open socket, or nil while it opens or after it was
// given up.
func (w *socketStreamWriter) socket() *websocket.Conn {
	select {
	case <-w.opened:
		return w.conn
	default:
		return nil
	}
}

func (w *socketStreamWriter) Write(ctx context.Context, chunks [][]byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.done {
		return errStreamWriterDone
	}
	for start := 0; start < len(chunks); {
		end := start + 1
		size := len(chunks[start])
		for end < len(chunks) && end-start < streamSocketChunksPerWrite && size+len(chunks[end]) <= streamSocketMessageBytes {
			size += len(chunks[end])
			end++
		}
		if err := w.writeBatch(ctx, chunks[start:end]); err != nil {
			return err
		}
		start = end
	}
	return nil
}

func (w *socketStreamWriter) writeBatch(ctx context.Context, chunks [][]byte) error {
	defer func() { w.chunkSeq += len(chunks) }()
	conn := w.socket()
	if conn == nil {
		return w.fallback.Write(ctx, chunks)
	}
	w.reqID++
	frame, err := encodeSocketFrame(streamSocketWrite{Type: "write", ReqID: w.reqID, ChunkSeq: w.chunkSeq, NumChunks: len(chunks)}, encodeSocketChunks(chunks))
	if err != nil {
		return err
	}
	for {
		reply, err := w.request(ctx, conn, frame, "write_ack")
		var retry streamSocketRetry
		switch {
		case errors.As(err, &retry):
			if err := sleepContext(ctx, retry.after); err != nil {
				return err
			}
			continue
		case errors.Is(err, errStreamSocketFailed):
			// The write may or may not have landed; storing it again keeps
			// the chunks, at the cost of a possible duplicate.
			w.giveUp(websocket.StatusGoingAway, "socket failed")
			return w.fallback.Write(ctx, chunks)
		case err != nil:
			return err
		}
		_ = reply
		if w.draining {
			w.giveUp(websocket.StatusNormalClosure, "drained")
		}
		return nil
	}
}

var errStreamSocketFailed = errors.New("workflow: stream socket failed")

// streamSocketRetry is a request the server refused with 429.
type streamSocketRetry struct{ after time.Duration }

func (r streamSocketRetry) Error() string { return "workflow: stream socket throttled" }

// request sends frame and reads messages until the reply to it, noting a
// drain notice on the way.
func (w *socketStreamWriter) request(ctx context.Context, conn *websocket.Conn, frame []byte, ack string) (streamSocketReply, error) {
	if err := conn.Write(ctx, websocket.MessageBinary, frame); err != nil {
		if ctx.Err() != nil {
			return streamSocketReply{}, ctx.Err()
		}
		return streamSocketReply{}, fmt.Errorf("%w: %v", errStreamSocketFailed, err)
	}
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return streamSocketReply{}, ctx.Err()
			}
			return streamSocketReply{}, fmt.Errorf("%w: %v", errStreamSocketFailed, err)
		}
		meta, _, err := decodeSocketFrame(data)
		if err != nil {
			return streamSocketReply{}, fmt.Errorf("%w: %v", errStreamSocketFailed, err)
		}
		var reply streamSocketReply
		if err := cbor.Unmarshal(meta, &reply); err != nil {
			return streamSocketReply{}, fmt.Errorf("%w: %v", errStreamSocketFailed, err)
		}
		switch {
		case reply.Type == "drain":
			w.draining = true
			continue
		case reply.Type == "error" && reply.ReqID == nil:
			return streamSocketReply{}, fmt.Errorf("%w: %d %s", errStreamSocketFailed, reply.Status, reply.Message)
		case reply.ReqID == nil || *reply.ReqID != w.reqID:
			continue
		case reply.Type == ack:
			return reply, nil
		case reply.Type == "error" && reply.Status == http.StatusTooManyRequests:
			after, _ := strconv.Atoi(reply.RetryAfter)
			return streamSocketReply{}, streamSocketRetry{after: max(time.Duration(after)*time.Second, 100*time.Millisecond)}
		case reply.Type == "error":
			return streamSocketReply{}, streamSocketError(reply.Status, reply.Message)
		}
		return streamSocketReply{}, fmt.Errorf("%w: unexpected %s reply", errStreamSocketFailed, reply.Type)
	}
}

// streamSocketError maps a refused request to the contract's errors.
func streamSocketError(status int, message string) error {
	switch status {
	case http.StatusConflict, http.StatusGone:
		return fmt.Errorf("%w: %s", ErrStreamClosed, message)
	case http.StatusNotFound:
		return fmt.Errorf("%w: %s", ErrRunNotFound, message)
	}
	return fmt.Errorf("workflow: stream socket request refused with %d: %s", status, message)
}

// giveUp closes the socket and writes through the fallback from then on.
func (w *socketStreamWriter) giveUp(code websocket.StatusCode, reason string) {
	if w.conn != nil {
		_ = w.conn.Close(code, reason)
		w.conn = nil
	}
}

func (w *socketStreamWriter) Release(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.done {
		return nil
	}
	w.done = true
	w.cancelDial()
	<-w.opened
	w.giveUp(websocket.StatusNormalClosure, "released")
	return w.fallback.Release(ctx)
}

func (w *socketStreamWriter) CloseStream(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.done {
		return errStreamWriterDone
	}
	if conn := w.socket(); conn != nil {
		w.reqID++
		frame, err := encodeSocketFrame(streamSocketClose{Type: "close", ReqID: w.reqID}, nil)
		if err != nil {
			return err
		}
		for {
			_, err = w.request(ctx, conn, frame, "close_ack")
			var retry streamSocketRetry
			if errors.As(err, &retry) {
				if err := sleepContext(ctx, retry.after); err != nil {
					return err
				}
				continue
			}
			break
		}
		switch {
		case err == nil:
			w.done = true
			w.giveUp(websocket.StatusNormalClosure, "closed")
			return w.fallback.Release(ctx)
		case !errors.Is(err, errStreamSocketFailed):
			return err
		}
		// Closing again through the fallback is harmless if the close landed.
		w.giveUp(websocket.StatusGoingAway, "socket failed")
	}
	w.cancelDial()
	<-w.opened
	w.giveUp(websocket.StatusNormalClosure, "closed")
	if err := w.fallback.CloseStream(ctx); err != nil {
		return err
	}
	w.done = true
	return nil
}

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// StreamSocketHandler returns the stream socket endpoint for world, which
// HTTPHandler serves at <prefix>/websockets/v1/runs/{runId}/streams/{name}
// behind the RPC endpoint's protection. Each socket is one writer, which
// writes through world's own StreamWriterOpener when it has them.
func StreamSocketHandler(world World) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writerID := r.URL.Query().Get("writerId")
		if writerID == "" {
			http.Error(w, "workflow: writerId is required", http.StatusBadRequest)
			return
		}
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		conn.SetReadLimit(streamSocketReadLimit)
		ctx := r.Context()
		input := StreamsOpenWriterInput{RunID: RunID(r.PathValue("runId")), Name: r.PathValue("name"), WriterID: writerID}
		writer, err := OpenStreamWriter(ctx, world, input)
		if err != nil {
			_ = conn.Close(websocket.StatusInternalError, "open stream writer")
			return
		}
		closed := false
		defer func() {
			if !closed {
				_ = writer.Release(context.WithoutCancel(ctx))
			}
		}()
		for {
			_, data, err := conn.Read(ctx)
			if err != nil {
				return
			}
			reply, end := serveStreamSocketMessage(ctx, writer, data)
			if reply != nil {
				if err := conn.Write(ctx, websocket.MessageBinary, reply); err != nil {
					return
				}
			}
			if end {
				closed = true
				_ = conn.Close(websocket.StatusNormalClosure, "closed")
				return
			}
		}
	})
}

// serveStreamSocketMessage applies one request and returns its reply, and
// whether the stream was closed.
func serveStreamSocketMessage(ctx context.Context, writer StreamWriter, data []byte) ([]byte, bool) {
	meta, body, err := decodeSocketFrame(data)
	var request struct {
		Type      string `cbor:"type"`
		ReqID     *int   `cbor:"reqId"`
		NumChunks int    `cbor:"numChunks"`
	}
	if err == nil {
		err = cbor.Unmarshal(meta, &request)
	}
	if err != nil || request.ReqID == nil {
		reply, _ := encodeSocketFrame(streamSocketReply{Type: "error", Status: http.StatusBadRequest, Message: "invalid request"}, nil)
		return reply, false
	}
	refuse := func(err error) []byte {
		status := http.StatusInternalServerError
		switch {
		case errors.Is(err, ErrStreamClosed):
			status = http.StatusConflict
		case errors.Is(err, ErrRunNotFound):
			status = http.StatusNotFound
		case errors.Is(err, errStreamWriterDone):
			status = http.StatusGone
		}
		if status == http.StatusInternalServerError {
			log.Printf("workflow: stream socket %s: %v", request.Type, err)
		}
		reply, _ := encodeSocketFrame(streamSocketReply{Type: "error", ReqID: request.ReqID, Status: status, Message: err.Error()}, nil)
		return reply
	}
	switch request.Type {
	case "write":
		chunks, err := decodeSocketChunks(body, request.NumChunks)
		if err != nil {
			reply, _ := encodeSocketFrame(streamSocketReply{Type: "error", ReqID: request.ReqID, Status: http.StatusBadRequest, Message: err.Error()}, nil)
			return reply, false
		}
		if err := writer.Write(ctx, chunks); err != nil {
			return refuse(err), false
		}
		reply, _ := encodeSocketFrame(streamSocketReply{Type: "write_ack", ReqID: request.ReqID}, nil)
		return reply, false
	case "close":
		if err := writer.CloseStream(ctx); err != nil {
			return refuse(err), false
		}
		reply, _ := encodeSocketFrame(streamSocketReply{Type: "close_ack", ReqID: request.ReqID}, nil)
		return reply, true
	}
	reply, _ := encodeSocketFrame(streamSocketReply{Type: "error", ReqID: request.ReqID, Status: http.StatusBadRequest, Message: "unknown request type " + request.Type}, nil)
	return reply, false
}
