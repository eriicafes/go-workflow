package vercelworld

import (
	"context"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

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
