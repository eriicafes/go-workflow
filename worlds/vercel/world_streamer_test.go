package vercelworld

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// newBackendWorld returns a World whose backend is handler.
func newBackendWorld(t *testing.T, handler http.HandlerFunc) *World {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	world, err := New(Config{BaseURL: "https://app.example.com/v1/workflow", Topic: "workflow", BackendURL: server.URL, OIDCToken: staticOIDCToken("static-token")})
	if err != nil {
		t.Fatal(err)
	}
	return world
}

func TestStreamsCloseRetriesTransientFailures(t *testing.T) {
	backoff := streamCloseMinBackoff
	streamCloseMinBackoff = time.Millisecond
	t.Cleanup(func() { streamCloseMinBackoff = backoff })
	ctx := context.Background()
	var requests atomic.Int32
	answers := []int{}
	world := newBackendWorld(t, func(writer http.ResponseWriter, request *http.Request) {
		count := int(requests.Add(1))
		status := http.StatusOK
		if count <= len(answers) {
			status = answers[count-1]
		}
		if status == http.StatusTooManyRequests {
			writer.Header().Set("Retry-After", "1")
		}
		writer.WriteHeader(status)
	})
	closeStream := func(statuses ...int) error {
		t.Helper()
		requests.Store(0)
		answers = statuses
		return world.StreamsClose(ctx, worlds.StreamsCloseInput{RunID: "wrun_1", Name: "out"})
	}

	// A close is sent again after a 429, 500, 502, 503, or 504.
	if err := closeStream(http.StatusServiceUnavailable, http.StatusInternalServerError, http.StatusBadGateway, http.StatusGatewayTimeout); err != nil || requests.Load() != 5 {
		t.Fatalf("close = %v after %d requests, want success after 5", err, requests.Load())
	}
	// A 429's Retry-After is waited out.
	started := time.Now()
	if err := closeStream(http.StatusTooManyRequests); err != nil || requests.Load() != 2 || time.Since(started) < time.Second {
		t.Fatalf("throttled close = %v after %d requests in %s", err, requests.Load(), time.Since(started))
	}
	// It gives up after five retries.
	failing := make([]int, 10)
	for index := range failing {
		failing[index] = http.StatusServiceUnavailable
	}
	if err := closeStream(failing...); statusOf(err) != http.StatusServiceUnavailable || requests.Load() != 6 {
		t.Fatalf("failing close = %v after %d requests, want a 503 after 6", err, requests.Load())
	}
	// Any other refusal is not retried.
	if err := closeStream(http.StatusBadRequest); err == nil || requests.Load() != 1 {
		t.Fatalf("refused close = %v after %d requests, want an error after 1", err, requests.Load())
	}
	// A chunk write is not retried.
	requests.Store(0)
	answers = []int{http.StatusServiceUnavailable}
	if err := world.StreamsWrite(ctx, worlds.StreamsWriteInput{RunID: "wrun_1", Name: "out", Chunk: []byte("x")}); err == nil || requests.Load() != 1 {
		t.Fatalf("write = %v after %d requests, want an error after 1", err, requests.Load())
	}
}

func TestStreamReadsReportExpiry(t *testing.T) {
	ctx := context.Background()
	code := "run-expired"
	world := newBackendWorld(t, func(writer http.ResponseWriter, request *http.Request) {
		writeError(writer, http.StatusGone, code, "expired")
	})
	_, chunksErr := world.StreamsGetChunks(ctx, worlds.StreamsGetChunksInput{RunID: "wrun_1", Name: "out", Cursor: "0"})
	_, infoErr := world.StreamsGetInfo(ctx, worlds.StreamsGetInfoInput{RunID: "wrun_1", Name: "out"})
	for _, err := range []error{chunksErr, infoErr} {
		if _, ok := errors.AsType[*worlds.RunExpiredError](err); !ok {
			t.Fatalf("read of an expired run = %v, want &RunExpiredError{}", err)
		}
	}
	// A stream that expired on its own is reported as a *StreamExpiredError.
	code = "stream-expired"
	_, chunksErr = world.StreamsGetChunks(ctx, worlds.StreamsGetChunksInput{RunID: "wrun_1", Name: "out", Cursor: "0"})
	_, infoErr = world.StreamsGetInfo(ctx, worlds.StreamsGetInfoInput{RunID: "wrun_1", Name: "out"})
	for _, err := range []error{chunksErr, infoErr} {
		if is[*worlds.RunExpiredError](err) || !is[*worlds.StreamExpiredError](err) {
			t.Fatalf("read of an expired stream = %v, want &StreamExpiredError{}", err)
		}
	}
}

func TestStreamReadsFromTheBeginningOnAnInvalidCursor(t *testing.T) {
	world := newBackendWorld(t, func(writer http.ResponseWriter, request *http.Request) {
		writeCBOR(writer, map[string]any{
			"data":    []map[string]any{{"index": 0, "data": []byte("a")}, {"index": 1, "data": []byte("b")}},
			"cursor":  nil,
			"hasMore": false,
			"done":    true,
		})
	})
	result, err := world.StreamsGetChunks(context.Background(), worlds.StreamsGetChunksInput{RunID: "wrun_1", Name: "out", Cursor: "not-a-cursor"})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Data) != 2 || result.Data[0].Index != 0 || result.Cursor != "2" || !result.Done {
		t.Fatalf("read = %+v, want both chunks from the beginning", result)
	}
}

// writeThroughWriter writes two chunks to the default stream of a new run,
// closes the stream and returns what the World reads back.
func writeThroughWriter(t *testing.T, world *World) []string {
	t.Helper()
	ctx := context.Background()
	runID, err := worlds.CreateRun(ctx, world, worlds.ClientRunsCreateInput{EventsCreateRunInput: worlds.EventsCreateRunInput{WorkflowName: "wf"}})
	if err != nil {
		t.Fatal(err)
	}
	writer, err := world.StreamsOpenWriter(ctx, worlds.StreamsOpenWriterInput{RunID: runID, Name: worlds.UserStreamName(runID, "")})
	if err != nil {
		t.Fatal(err)
	}
	// Give the socket time to open, so the writes travel over it when the
	// backend accepts it.
	time.Sleep(100 * time.Millisecond)
	if err := writer.Write(ctx, [][]byte{[]byte("a"), []byte("b")}); err != nil {
		t.Fatal(err)
	}
	if err := writer.CloseStream(ctx); err != nil {
		t.Fatal(err)
	}
	result, err := world.StreamsGetChunks(ctx, worlds.StreamsGetChunksInput{RunID: runID, Name: worlds.UserStreamName(runID, "")})
	if err != nil {
		t.Fatal(err)
	}
	var chunks []string
	for _, chunk := range result.Data {
		chunks = append(chunks, string(chunk.Data))
	}
	if !result.Done {
		t.Fatalf("stream not closed: %+v", result)
	}
	return chunks
}

func TestStreamWritersUseTheBackendSocket(t *testing.T) {
	all, _, backend := newWorlds(t, 1)
	backend.sockets = true
	if chunks := writeThroughWriter(t, all[0]); len(chunks) != 2 || chunks[0] != "a" || chunks[1] != "b" {
		t.Fatalf("chunks = %q", chunks)
	}
	if got := backend.count("GET websockets/v1/runs/:id/streams/:id"); got != 1 {
		t.Fatalf("socket requests = %d, want 1", got)
	}
	if got := backend.count("PUT v2/runs/:id/stream/:id"); got != 0 {
		t.Fatalf("HTTP stream writes = %d, want none", got)
	}
}

func TestStreamWritersFallBackToHTTP(t *testing.T) {
	all, _, backend := newWorlds(t, 1)
	if chunks := writeThroughWriter(t, all[0]); len(chunks) != 2 {
		t.Fatalf("chunks = %q", chunks)
	}
	if got := backend.count("PUT v2/runs/:id/stream/:id"); got != 2 {
		t.Fatalf("HTTP stream writes = %d, want 2", got)
	}
	// After a declined socket, writers stop asking for a while.
	writeThroughWriter(t, all[0])
	if got := backend.count("GET websockets/v1/runs/:id/streams/:id"); got != 1 {
		t.Fatalf("socket requests = %d, want 1", got)
	}
}

func TestStreamWritersSkipTheSocketOverHTTPTransport(t *testing.T) {
	t.Setenv("WORKFLOW_STREAMS_TRANSPORT", "http")
	all, _, backend := newWorlds(t, 1)
	backend.sockets = true
	writeThroughWriter(t, all[0])
	if got := backend.count("GET websockets/v1/runs/:id/streams/:id"); got != 0 {
		t.Fatalf("socket requests = %d, want none", got)
	}
}
