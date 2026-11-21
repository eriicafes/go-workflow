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
		if !errors.Is(err, worlds.ErrRunExpired) {
			t.Fatalf("read of an expired run = %v, want ErrRunExpired", err)
		}
	}
	// A stream that expired on its own is reported as ErrStreamExpired.
	code = "stream-expired"
	_, chunksErr = world.StreamsGetChunks(ctx, worlds.StreamsGetChunksInput{RunID: "wrun_1", Name: "out", Cursor: "0"})
	_, infoErr = world.StreamsGetInfo(ctx, worlds.StreamsGetInfoInput{RunID: "wrun_1", Name: "out"})
	for _, err := range []error{chunksErr, infoErr} {
		if errors.Is(err, worlds.ErrRunExpired) || !errors.Is(err, worlds.ErrStreamExpired) {
			t.Fatalf("read of an expired stream = %v, want ErrStreamExpired", err)
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
