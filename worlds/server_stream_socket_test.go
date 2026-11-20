package worlds_test

import (
	"context"
	"encoding/binary"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/testworld"
	"github.com/fxamacker/cbor/v2"
)

// countingWriter records the writes that reach the fallback.
type countingWriter struct {
	worlds.StreamWriter
	writes, closes atomic.Int32
}

func (w *countingWriter) Write(ctx context.Context, chunks [][]byte) error {
	w.writes.Add(1)
	return w.StreamWriter.Write(ctx, chunks)
}

func (w *countingWriter) CloseStream(ctx context.Context) error {
	w.closes.Add(1)
	return w.StreamWriter.CloseStream(ctx)
}

// socketServer serves world's HTTP handler, counting socket upgrades.
func socketServer(t *testing.T, world worlds.World, options ...worlds.HTTPOption) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	upgrades := &atomic.Int32{}
	handler := worlds.HTTPHandler(world, "", options...)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
			upgrades.Add(1)
		}
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	return server, upgrades
}

// openSocketWriter opens a socket writer for the stream name of runID and
// waits until its writes go through the socket.
func openSocketWriter(t *testing.T, world worlds.World, server *httptest.Server, runID worlds.RunID, name string, header http.Header) (worlds.StreamWriter, *countingWriter) {
	t.Helper()
	ctx := context.Background()
	input := worlds.StreamsOpenWriterInput{RunID: runID, Name: name, WriterID: worlds.NewWriterID()}
	direct, err := worlds.OpenStreamWriter(ctx, world, input)
	if err != nil {
		t.Fatal(err)
	}
	fallback := &countingWriter{StreamWriter: direct}
	target, err := worlds.StreamSocketURL(server.URL+"/v1/workflow", runID, name, input.WriterID)
	if err != nil {
		t.Fatal(err)
	}
	writer := worlds.OpenStreamSocketWriter(ctx, worlds.StreamSocketOptions{URL: target, Header: header, Fallback: fallback})
	return writer, fallback
}

func TestStreamSocketURL(t *testing.T) {
	got, err := worlds.StreamSocketURL("https://example.com/v1/workflow/", "wrun_1", "a b/c", "wrtr_1")
	if err != nil || got != "wss://example.com/v1/workflow/websockets/v1/runs/wrun_1/streams/a%20b%2Fc?writerId=wrtr_1" {
		t.Fatalf("url = %q err=%v", got, err)
	}
	got, err = worlds.StreamSocketURL("http://localhost:8080/v1/workflow", "wrun_1", "", "wrtr_1")
	if err != nil || got != "ws://localhost:8080/v1/workflow/websockets/v1/runs/wrun_1/streams/?writerId=wrtr_1" {
		t.Fatalf("default stream url = %q err=%v", got, err)
	}
	if _, err := worlds.StreamSocketURL("ftp://example.com", "wrun_1", "", "wrtr_1"); err == nil {
		t.Fatal("ftp base accepted")
	}
}

func TestStreamSocketWritesAndCloses(t *testing.T) {
	ctx := context.Background()
	world := testworld.NewManual()
	defer world.Close()
	server, upgrades := socketServer(t, world)
	for _, name := range []string{"out", ""} {
		writer, fallback := openSocketWriter(t, world, server, "wrun_socket", name, nil)
		// Writes go through the fallback until the socket opens.
		var written []string
		for deadline := time.Now().Add(5 * time.Second); ; {
			before := fallback.writes.Load()
			chunk := string(rune('a' + len(written)))
			if err := writer.Write(ctx, [][]byte{[]byte(chunk)}); err != nil {
				t.Fatal(err)
			}
			written = append(written, chunk)
			if fallback.writes.Load() == before {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("socket never opened")
			}
			time.Sleep(10 * time.Millisecond)
		}
		if err := writer.Write(ctx, [][]byte{[]byte("y"), []byte("z")}); err != nil {
			t.Fatal(err)
		}
		written = append(written, "y", "z")
		if err := writer.CloseStream(ctx); err != nil {
			t.Fatal(err)
		}
		if fallback.closes.Load() != 0 {
			t.Fatal("close went through the fallback")
		}
		chunks, err := world.StreamsGetChunks(ctx, worlds.StreamsGetChunksInput{RunID: "wrun_socket", Name: name})
		if err != nil || !chunks.Done || len(chunks.Data) != len(written) {
			t.Fatalf("stream %q = %#v err=%v, want %v", name, chunks, err, written)
		}
		for index, chunk := range chunks.Data {
			if string(chunk.Data) != written[index] {
				t.Fatalf("stream %q chunk %d = %q, want %q", name, index, chunk.Data, written[index])
			}
		}
		if err := writer.Write(ctx, [][]byte{[]byte("late")}); err == nil {
			t.Fatal("closed writer accepted a write")
		}
	}
	if upgrades.Load() != 2 {
		t.Fatalf("upgrades = %d, want 2", upgrades.Load())
	}
}

func TestStreamSocketReportsAClosedStream(t *testing.T) {
	ctx := context.Background()
	world := testworld.NewManual()
	defer world.Close()
	server, _ := socketServer(t, world)
	if err := world.StreamsClose(ctx, worlds.StreamsCloseInput{RunID: "wrun_closed", Name: "out"}); err != nil {
		t.Fatal(err)
	}
	writer, fallback := openSocketWriter(t, world, server, "wrun_closed", "out", nil)
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		before := fallback.writes.Load()
		err := writer.Write(ctx, [][]byte{[]byte("late")})
		if !errors.Is(err, worlds.ErrStreamClosed) {
			t.Fatalf("write to a closed stream = %v", err)
		}
		if fallback.writes.Load() == before {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("socket never opened")
		}
	}
}

func TestStreamSocketFallsBack(t *testing.T) {
	ctx := context.Background()
	world := testworld.NewManual()
	defer world.Close()

	t.Run("Declined", func(t *testing.T) {
		// The RPC protection guards the socket too.
		server, upgrades := socketServer(t, world, worlds.WithClientToken("secret"))
		var declined sync.WaitGroup
		declined.Add(1)
		input := worlds.StreamsOpenWriterInput{RunID: "wrun_declined", Name: "out", WriterID: worlds.NewWriterID()}
		direct, _ := worlds.OpenStreamWriter(ctx, world, input)
		target, _ := worlds.StreamSocketURL(server.URL+"/v1/workflow", input.RunID, input.Name, input.WriterID)
		writer := worlds.OpenStreamSocketWriter(ctx, worlds.StreamSocketOptions{URL: target, Fallback: direct, OnDecline: declined.Done})
		declined.Wait()
		if err := writer.Write(ctx, [][]byte{[]byte("via fallback")}); err != nil {
			t.Fatal(err)
		}
		if err := writer.CloseStream(ctx); err != nil {
			t.Fatal(err)
		}
		chunks, err := world.StreamsGetChunks(ctx, worlds.StreamsGetChunksInput{RunID: input.RunID, Name: input.Name})
		if err != nil || len(chunks.Data) != 1 || !chunks.Done || upgrades.Load() != 1 {
			t.Fatalf("declined stream = %#v err=%v upgrades=%d", chunks, err, upgrades.Load())
		}

		// With the token, the socket opens.
		header := http.Header{"Authorization": {"Bearer secret"}}
		writer, fallback := openSocketWriter(t, world, server, "wrun_authorized", "out", header)
		for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(10 * time.Millisecond) {
			before := fallback.writes.Load()
			if err := writer.Write(ctx, [][]byte{[]byte("x")}); err != nil {
				t.Fatal(err)
			}
			if fallback.writes.Load() == before {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("authorized socket never opened")
			}
		}
		_ = writer.Release(ctx)
	})

	t.Run("Failed", func(t *testing.T) {
		// The server acknowledges the first write, then drops the socket.
		var messages atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			conn, err := websocket.Accept(w, r, nil)
			if err != nil {
				return
			}
			for {
				_, data, err := conn.Read(r.Context())
				if err != nil || messages.Add(1) > 1 {
					_ = conn.CloseNow()
					return
				}
				_ = data
				_ = conn.Write(r.Context(), websocket.MessageBinary, ackFrame(t, 1))
			}
		}))
		t.Cleanup(server.Close)
		input := worlds.StreamsOpenWriterInput{RunID: "wrun_failed", Name: "out", WriterID: worlds.NewWriterID()}
		direct, _ := worlds.OpenStreamWriter(ctx, world, input)
		fallback := &countingWriter{StreamWriter: direct}
		target, _ := worlds.StreamSocketURL(server.URL, input.RunID, input.Name, input.WriterID)
		writer := worlds.OpenStreamSocketWriter(ctx, worlds.StreamSocketOptions{URL: target, Fallback: fallback})
		for deadline := time.Now().Add(5 * time.Second); messages.Load() == 0; time.Sleep(10 * time.Millisecond) {
			if err := writer.Write(ctx, [][]byte{[]byte("x")}); err != nil {
				t.Fatal(err)
			}
			if time.Now().After(deadline) {
				t.Fatal("socket never opened")
			}
		}
		// A write on the failed socket is stored through the fallback.
		before := fallback.writes.Load()
		if err := writer.Write(ctx, [][]byte{[]byte("after")}); err != nil {
			t.Fatal(err)
		}
		if fallback.writes.Load() != before+1 {
			t.Fatal("write on a failed socket did not reach the fallback")
		}
		chunks, err := world.StreamsGetChunks(ctx, worlds.StreamsGetChunksInput{RunID: input.RunID, Name: input.Name})
		if err != nil || len(chunks.Data) == 0 || string(chunks.Data[len(chunks.Data)-1].Data) != "after" {
			t.Fatalf("stream after a failed socket = %#v err=%v", chunks, err)
		}
	})
}

// ackFrame encodes a write_ack for reqID.
func ackFrame(t *testing.T, reqID int) []byte {
	t.Helper()
	meta, err := cbor.Marshal(map[string]any{"type": "write_ack", "reqId": reqID})
	if err != nil {
		t.Fatal(err)
	}
	frame := binary.BigEndian.AppendUint32(nil, uint32(len(meta)))
	frame = append(frame, meta...)
	return binary.BigEndian.AppendUint32(frame, 0)
}

type socketClientInput struct{}
type socketClientOutput struct{}

func (socketClientInput) Workflow(socketClientOutput) {}

// TestHTTPClientWritesStreamsOverTheSocket checks that a remote client's
// writer reaches the server's stream through its socket.
func TestHTTPClientWritesStreamsOverTheSocket(t *testing.T) {
	world := testworld.New(workflow.Use(func(context.Context, socketClientInput) (socketClientOutput, error) {
		return socketClientOutput{}, nil
	}))
	defer world.Close()
	server, upgrades := socketServer(t, world)
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewHTTPClient(server.URL+"/v1/workflow")), 5*time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, socketClientInput{})
	if err != nil {
		t.Fatal(err)
	}
	writer, err := run.Writer(ctx, workflow.WriterOption{Name: "remote"})
	if err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(5 * time.Second); upgrades.Load() == 0; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("client never opened a socket")
		}
	}
	for _, chunk := range []string{"one", "two", "three"} {
		if _, err := writer.Write([]byte(chunk)); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.CloseStream(); err != nil {
		t.Fatal(err)
	}
	chunks, err := world.StreamsGetChunks(ctx, worlds.StreamsGetChunksInput{RunID: worlds.RunID(run.ID()), Name: "remote"})
	if err != nil || !chunks.Done || len(chunks.Data) != 3 || string(chunks.Data[2].Data) != "three" {
		t.Fatalf("remote stream = %#v err=%v", chunks, err)
	}
}
