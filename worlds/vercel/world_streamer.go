package vercelworld

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

const (
	// streamChunksPerWrite is the most chunks one write request carries.
	streamChunksPerWrite = 1000
	// streamPageSize is the default number of chunks one read returns.
	streamPageSize = 100
	streamPollMax  = 500 * time.Millisecond
	// streamCursorCacheSize bounds the backend cursors a World remembers.
	streamCursorCacheSize = 1024
)

// Streams are stored by the backend. Read cursors are chunk indexes, so any
// instance can continue a read.

func (w *World) streamPath(runID worlds.RunID, name string) string {
	return "/v2/runs/" + pathEscape(string(runID)) + "/stream/" + pathEscape(name)
}

func (w *World) streamsPath(runID worlds.RunID, name, suffix string) string {
	return "/v2/runs/" + pathEscape(string(runID)) + "/streams/" + pathEscape(name) + suffix
}

func (w *World) putStream(ctx context.Context, runID worlds.RunID, name string, body []byte, header http.Header) error {
	if header == nil {
		header = http.Header{}
	}
	header.Set("Content-Type", "application/octet-stream")
	if body == nil {
		body = []byte{}
	}
	response, err := w.api.request(ctx, http.MethodPut, w.streamPath(runID, name), nil, body, header)
	if err != nil {
		if status := statusOf(err); status == http.StatusConflict || status == http.StatusGone {
			return errors.Join(worlds.ErrStreamClosed, err)
		}
		return notFound(err, worlds.ErrRunNotFound)
	}
	return response.Body.Close()
}

func (w *World) StreamsWrite(ctx context.Context, input worlds.StreamsWriteInput) error {
	return w.putStream(ctx, input.RunID, input.Name, input.Chunk, nil)
}

func (w *World) StreamsWriteMulti(ctx context.Context, input worlds.StreamsWriteMultiInput) error {
	for start := 0; start < len(input.Chunks); start += streamChunksPerWrite {
		chunks := input.Chunks[start:min(start+streamChunksPerWrite, len(input.Chunks))]
		var body []byte
		for _, chunk := range chunks {
			body = binary.BigEndian.AppendUint32(body, uint32(len(chunk)))
			body = append(body, chunk...)
		}
		header := http.Header{}
		header.Set("X-Stream-Multi", "true")
		if err := w.putStream(ctx, input.RunID, input.Name, body, header); err != nil {
			return err
		}
	}
	return nil
}

func (w *World) StreamsClose(ctx context.Context, input worlds.StreamsCloseInput) error {
	header := http.Header{}
	header.Set("X-Stream-Done", "true")
	return w.putStream(ctx, input.RunID, input.Name, nil, header)
}

// streamSocketRetryAfter is how long a World writes through HTTP after the
// backend declines a stream socket.
const streamSocketRetryAfter = 5 * time.Minute

// directStreamer hides StreamsOpenWriter, so worlds.OpenStreamWriter returns
// a writer of the World's HTTP stream writes.
type directStreamer struct{ worlds.Streamer }

// StreamsOpenWriter returns a writer that sends a stream's writes over the
// backend's stream socket, writing through HTTP while the socket opens and
// when it is declined or fails. The socket is reachable with a deployment's
// OIDC token only, so through the API proxy, or with
// WORKFLOW_STREAMS_TRANSPORT set to http, writes go through HTTP. A writer
// without a writer ID is given one.
func (w *World) StreamsOpenWriter(ctx context.Context, input worlds.StreamsOpenWriterInput) (worlds.StreamWriter, error) {
	if input.WriterID == "" {
		input.WriterID = worlds.NewWriterID()
	}
	direct, err := worlds.OpenStreamWriter(ctx, directStreamer{w}, input)
	if err != nil {
		return nil, err
	}
	w.socketMutex.Lock()
	declined := time.Now().Before(w.socketDeclinedUntil)
	w.socketMutex.Unlock()
	if w.api.proxy || declined || w.streamsOverHTTP {
		return direct, nil
	}
	target, err := worlds.StreamSocketURL(w.api.baseURL, input.RunID, input.Name, input.WriterID)
	if err != nil {
		return direct, nil
	}
	header := http.Header{}
	if err := w.api.authorize(ctx, header); err != nil {
		return direct, nil
	}
	if w.api.userAgent != "" {
		header.Set("User-Agent", w.api.userAgent)
	}
	return worlds.OpenStreamSocketWriter(ctx, worlds.StreamSocketOptions{
		URL:        target,
		Header:     header,
		HTTPClient: w.api.http,
		Fallback:   direct,
		OnDecline: func() {
			w.socketMutex.Lock()
			w.socketDeclinedUntil = time.Now().Add(streamSocketRetryAfter)
			w.socketMutex.Unlock()
		},
	}), nil
}

// chunksResponse is a page of a stream's chunks.
type chunksResponse struct {
	Data []struct {
		Index int    `cbor:"index" json:"index"`
		Data  []byte `cbor:"data" json:"data"`
	} `cbor:"data" json:"data"`
	Cursor  *string `cbor:"cursor" json:"cursor"`
	HasMore bool    `cbor:"hasMore" json:"hasMore"`
	Done    bool    `cbor:"done" json:"done"`
}

func (w *World) StreamsGetChunks(ctx context.Context, input worlds.StreamsGetChunksInput) (worlds.StreamChunksResult, error) {
	start, err := worlds.StreamsStartIndex(ctx, w, input)
	if err != nil {
		return worlds.StreamChunksResult{}, err
	}
	if input.Cursor != "" {
		if start, err = strconv.Atoi(input.Cursor); err != nil || start < 0 {
			return worlds.StreamChunksResult{}, errors.New("workflow/vercel: invalid stream cursor")
		}
	}
	limit := input.Limit
	if limit <= 0 {
		limit = streamPageSize
	}
	deadline := time.Now().Add(input.Wait)
	delay := 50 * time.Millisecond
	for {
		result, err := w.readChunks(ctx, input.RunID, input.Name, start, limit)
		if err != nil || len(result.Data) > 0 || result.Done || time.Until(deadline) <= 0 {
			return result, err
		}
		timer := time.NewTimer(min(delay, time.Until(deadline)))
		select {
		case <-ctx.Done():
			timer.Stop()
			return worlds.StreamChunksResult{}, ctx.Err()
		case <-timer.C:
		}
		delay = min(2*delay, streamPollMax)
	}
}

// readChunks returns up to limit chunks from index start. The backend pages
// by its own cursor, so a read starts from the cursor the World remembers for
// the closest index at or before start, and skips the chunks before start.
func (w *World) readChunks(ctx context.Context, runID worlds.RunID, name string, start, limit int) (worlds.StreamChunksResult, error) {
	result := worlds.StreamChunksResult{Cursor: strconv.Itoa(start), RetryAfter: streamPollMax}
	index, cursor := w.cursors.near(runID, name, start)
	for {
		query := url.Values{"limit": {itoa(max(limit, streamPageSize))}}
		if cursor != "" {
			query.Set("cursor", cursor)
		}
		var page chunksResponse
		err := w.api.do(ctx, http.MethodGet, w.streamsPath(runID, name, "/chunks"), query, nil, &page)
		if statusOf(err) == http.StatusNotFound {
			return result, nil
		}
		if err != nil {
			return worlds.StreamChunksResult{}, err
		}
		for _, chunk := range page.Data {
			index = chunk.Index + 1
			if chunk.Index < start {
				continue
			}
			if len(result.Data) == limit {
				result.HasMore = true
				break
			}
			result.Data = append(result.Data, worlds.StreamChunk{Index: chunk.Index, Data: chunk.Data})
		}
		if page.Cursor != nil && *page.Cursor != "" && len(page.Data) > 0 && !result.HasMore {
			cursor = *page.Cursor
			w.cursors.put(runID, name, index, cursor)
		}
		if result.HasMore || !page.HasMore || page.Cursor == nil || *page.Cursor == "" {
			result.HasMore = result.HasMore || page.HasMore
			result.Done = page.Done
			if len(result.Data) > 0 {
				result.Cursor = strconv.Itoa(result.Data[len(result.Data)-1].Index + 1)
			}
			if result.Done || len(result.Data) > 0 {
				result.RetryAfter = 0
			}
			return result, nil
		}
	}
}

func (w *World) StreamsGetInfo(ctx context.Context, input worlds.StreamsGetInfoInput) (worlds.StreamInfo, error) {
	var info struct {
		TailIndex *int `cbor:"tailIndex" json:"tailIndex"`
		Done      bool `cbor:"done" json:"done"`
	}
	err := w.api.do(ctx, http.MethodGet, w.streamsPath(input.RunID, input.Name, "/info"), nil, nil, &info)
	if statusOf(err) == http.StatusNotFound {
		return worlds.StreamInfo{TailIndex: -1}, nil
	}
	if err != nil {
		return worlds.StreamInfo{}, err
	}
	result := worlds.StreamInfo{TailIndex: -1, Done: info.Done}
	if info.TailIndex != nil {
		result.TailIndex = *info.TailIndex
	}
	return result, nil
}

func (w *World) StreamsList(ctx context.Context, input worlds.StreamsListInput) ([]string, error) {
	header := http.Header{}
	header.Set("Accept", "application/json")
	response, err := w.api.request(ctx, http.MethodGet, "/v2/runs/"+pathEscape(string(input.RunID))+"/streams", nil, nil, header)
	if statusOf(err) == http.StatusNotFound {
		return []string{}, nil
	}
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	data, err := readAll(response.Body)
	if err != nil {
		return nil, err
	}
	var names []string
	if err := json.Unmarshal(data, &names); err != nil {
		var wrapped struct {
			Data []string `json:"data"`
		}
		if json.Unmarshal(data, &wrapped) != nil {
			return nil, errors.New("workflow/vercel: invalid stream list")
		}
		names = wrapped.Data
	}
	if names == nil {
		names = []string{}
	}
	slices.Sort(names)
	return names, nil
}

// streamCursors remembers, per stream, the backend cursor that continues a
// read at a chunk index.
type streamCursors struct {
	mu      sync.Mutex
	entries map[streamKey]map[int]string
	order   []streamKey
}

type streamKey struct {
	runID worlds.RunID
	name  string
}

// near returns the remembered index closest to start without passing it and
// its cursor, or 0 and "" to read from the beginning.
func (c *streamCursors) near(runID worlds.RunID, name string, start int) (int, string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	best, cursor := 0, ""
	for index, value := range c.entries[streamKey{runID, name}] {
		if index <= start && index > best {
			best, cursor = index, value
		}
	}
	return best, cursor
}

func (c *streamCursors) put(runID worlds.RunID, name string, index int, cursor string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.entries = map[streamKey]map[int]string{}
	}
	key := streamKey{runID, name}
	if c.entries[key] == nil {
		if len(c.order) == streamCursorCacheSize {
			delete(c.entries, c.order[0])
			c.order = c.order[1:]
		}
		c.order = append(c.order, key)
		c.entries[key] = map[int]string{}
	}
	if len(c.entries[key]) >= 64 {
		clear(c.entries[key])
	}
	c.entries[key][index] = cursor
}
