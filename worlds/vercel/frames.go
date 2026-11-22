package vercelworld

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"
)

// The workflow API's event routes exchange frames:
//
//	frame := u32_be(meta_len) || cbor(meta) || u32_be(body_len) || body
//
// The meta is a CBOR map of the event's metadata, and the body is its opaque
// payload, which the backend stores without decoding. A list answer is a
// sequence of event frames ended by a frame whose meta has _end: 1.

// maxFramePart caps one frame part read from the backend.
const maxFramePart = 256 << 20

// errFrameInvalid is a frame that arrived whole and cannot be decoded.
var errFrameInvalid = errors.New("workflow/vercel: invalid frame")

// truncatedError is an event list answer that ended, or whose connection
// failed, before its end frame. The events read before it are whole.
type truncatedError struct{ err error }

func (e *truncatedError) Error() string { return e.err.Error() }
func (e *truncatedError) Unwrap() error { return e.err }

// frame is one decoded frame.
type frame struct {
	Meta map[string]any
	Body []byte
}

// encodeFrame appends the frame of meta and body to dst.
func encodeFrame(dst []byte, meta map[string]any, body []byte) ([]byte, error) {
	encoded, err := cborEncode.Marshal(meta)
	if err != nil {
		return nil, err
	}
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(encoded)))
	dst = append(dst, encoded...)
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(body)))
	return append(dst, body...), nil
}

// frameReader reads frames from a response body.
type frameReader struct {
	r *bufio.Reader
}

func newFrameReader(r io.Reader) *frameReader {
	return &frameReader{r: bufio.NewReaderSize(r, 64<<10)}
}

// next returns the next frame, or io.EOF at a clean end of the body.
func (f *frameReader) next() (frame, error) {
	meta, err := f.part()
	if err != nil {
		return frame{}, err
	}
	body, err := f.part()
	if err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return frame{}, fmt.Errorf("workflow/vercel: read frame body: %w", err)
	}
	var decoded map[string]any
	if err := cborDecode.Unmarshal(meta, &decoded); err != nil {
		return frame{}, fmt.Errorf("%w: decode frame meta: %v", errFrameInvalid, err)
	}
	return frame{Meta: decoded, Body: body}, nil
}

func (f *frameReader) part() ([]byte, error) {
	var size [4]byte
	if _, err := io.ReadFull(f.r, size[:]); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, fmt.Errorf("workflow/vercel: truncated frame: %w", err)
		}
		return nil, err
	}
	length := binary.BigEndian.Uint32(size[:])
	if length > maxFramePart {
		return nil, fmt.Errorf("%w: frame part of %d bytes is too large", errFrameInvalid, length)
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(f.r, data); err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return nil, fmt.Errorf("workflow/vercel: truncated frame: %w", err)
	}
	return data, nil
}

// eventPage is a list of event frames and the end frame's cursor.
type eventPage struct {
	Events  []wireEvent
	Cursor  string
	HasMore bool
	// Seq and SeqInBand are the run's position counters the backend read
	// before the listing, when it reports them.
	Seq, SeqInBand *int
	// MaxEvents is the most events the run may hold, from the x-wf-max-events
	// header, or zero when the answer names none.
	MaxEvents int
}

// readEventPage reads event frames until the end frame. Each event frame's
// meta is the event, and its body is the payload of the event's payload
// field. A terminal error frame is returned as an error. An answer cut off
// before its end frame is returned as a *truncatedError, with the events read
// before it.
func readEventPage(body io.Reader, header interface{ Get(string) string }) (eventPage, error) {
	reader := newFrameReader(body)
	var page eventPage
	for {
		next, err := reader.next()
		if err != nil {
			if errors.Is(err, errFrameInvalid) {
				return page, err
			}
			if errors.Is(err, io.EOF) {
				err = fmt.Errorf("workflow/vercel: event stream ended without its end frame (%d events read)", len(page.Events))
			}
			return page, &truncatedError{err}
		}
		if isControl(next.Meta, "_end") {
			page.Cursor, _ = next.Meta["next"].(string)
			page.HasMore, _ = next.Meta["hasMore"].(bool)
			page.Seq, page.SeqInBand = snapshotCounters(next.Meta["snapshot"], header)
			if header != nil {
				if limit := headerCounter(header.Get("X-Wf-Max-Events")); limit != nil {
					page.MaxEvents = *limit
				}
			}
			return page, nil
		}
		if isControl(next.Meta, "_error") {
			code, _ := next.Meta["code"].(string)
			message, _ := next.Meta["message"].(string)
			return page, &apiError{Status: 500, Code: code, Message: message}
		}
		event, err := decodeWireEvent(next.Meta, next.Body)
		if err != nil {
			return page, err
		}
		page.Events = append(page.Events, event)
	}
}

// readEventFrame reads the one frame of a single event.
func readEventFrame(body io.Reader) (wireEvent, error) {
	next, err := newFrameReader(body).next()
	if err != nil {
		if errors.Is(err, io.EOF) {
			err = errors.New("workflow/vercel: empty event frame stream")
		}
		return wireEvent{}, err
	}
	if isControl(next.Meta, "_error") {
		code, _ := next.Meta["code"].(string)
		message, _ := next.Meta["message"].(string)
		return wireEvent{}, &apiError{Status: 500, Code: code, Message: message}
	}
	return decodeWireEvent(next.Meta, next.Body)
}

func isControl(meta map[string]any, key string) bool {
	value, ok := asInt(meta[key])
	return ok && value == 1
}

// snapshotCounters reads the position counters, seq and seqInBand, from an
// end frame's snapshot, or from the response headers that carry them
// otherwise. Each is nil when the response reports none.
func snapshotCounters(snapshot any, header interface{ Get(string) string }) (*int, *int) {
	if values, ok := snapshot.(map[string]any); ok {
		seq, seqOK := asInt(values["seq"])
		seqInBand, inBandOK := asInt(values["seqInBand"])
		if seqOK && inBandOK {
			return &seq, &seqInBand
		}
	}
	if header == nil {
		return nil, nil
	}
	return headerCounter(header.Get("X-Wf-Snapshot-Seq")), headerCounter(header.Get("X-Wf-Snapshot-Seq-In-Band"))
}

// headerCounter reads a decimal counter header.
func headerCounter(raw string) *int {
	if raw == "" {
		return nil
	}
	count, err := strconv.Atoi(raw)
	if err != nil || count < 0 {
		return nil
	}
	return &count
}

// asInt reads a decoded CBOR or JSON number.
func asInt(value any) (int, bool) {
	switch value := value.(type) {
	case uint64:
		return int(value), true
	case int64:
		return int(value), true
	case int:
		return value, true
	case float64:
		return int(value), value == float64(int(value))
	}
	return 0, false
}
