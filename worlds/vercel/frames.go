package vercelworld

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
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
		return frame{}, fmt.Errorf("workflow/vercel: decode frame meta: %w", err)
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
		return nil, fmt.Errorf("workflow/vercel: frame part of %d bytes is too large", length)
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
	// SeqInBand is the run's in-band count the backend read before the
	// listing, when it reports one.
	SeqInBand *int
}

// readEventPage reads event frames until the end frame. Each event frame's
// meta is the event, and its body is the payload of the event's payload
// field. A terminal error frame is returned as an error.
func readEventPage(body io.Reader, header interface{ Get(string) string }) (eventPage, error) {
	reader := newFrameReader(body)
	var page eventPage
	for {
		next, err := reader.next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = errors.New("workflow/vercel: event stream ended without its end frame")
			}
			return page, err
		}
		if isControl(next.Meta, "_end") {
			page.Cursor, _ = next.Meta["next"].(string)
			page.HasMore, _ = next.Meta["hasMore"].(bool)
			page.SeqInBand = snapshotSeqInBand(next.Meta["snapshot"], header)
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

// snapshotSeqInBand reads the in-band count from an end frame's snapshot, or
// from the response headers that carry it otherwise.
func snapshotSeqInBand(snapshot any, header interface{ Get(string) string }) *int {
	if values, ok := snapshot.(map[string]any); ok {
		if count, ok := asInt(values["seqInBand"]); ok {
			return &count
		}
	}
	if header == nil {
		return nil
	}
	raw := header.Get("X-Wf-Snapshot-Seq-In-Band")
	if raw == "" {
		return nil
	}
	var count int
	if _, err := fmt.Sscanf(raw, "%d", &count); err != nil || count < 0 {
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
