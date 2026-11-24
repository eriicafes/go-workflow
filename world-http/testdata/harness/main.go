// Command harness serves a Go World over HTTP for the Workflow SDK's test
// suites. The World sends its deliveries to the suite's app with a
// worlds.RemoteExecutor, at the flow route URL in HARNESS_REMOTE_URL, or the
// one the app's World module sets with a POST to /_harness/remote once the
// app listens. HARNESS_ADDR sets the address it listens on, which is
// 127.0.0.1 on a free port by default.
//
// HARNESS_WORLD selects the World: testworld (the default); local, in a
// temporary directory; postgres, in a fresh schema of the database at
// WORKFLOW_POSTGRES_URL, or DATABASE_URL; aws, on DynamoDB Local and
// ElasticMQ (see openAWS); or vercel, on Vercel's workflow backend (see
// openVercel). WORKFLOW_HTTP_TOKEN, when set, protects the client endpoints
// and authenticates the deliveries. HARNESS_DEBUG, when set, logs
// each delivery and, on exit, every run's events; set to a path, it appends
// the log to that file.
//
// It prints one line of JSON with its base URL, then serves. When its standard
// input is a pipe, it exits once the pipe closes, which happens when the
// process that started it exits.
package main

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
	localworld "github.com/eriicafes/go-workflow/worlds/local"
	postgresworld "github.com/eriicafes/go-workflow/worlds/postgres"
	"github.com/eriicafes/go-workflow/worlds/testworld"
	"github.com/fxamacker/cbor/v2"
	"github.com/jackc/pgx/v5"
)

// remote is a RemoteExecutor whose URL is set once the app listens. A
// delivery made before then fails, and its World retries it.
type remote struct {
	token string
	url   atomic.Pointer[string]
}

func (r *remote) Execute(ctx context.Context, runtime worlds.Runtime, input worlds.QueueInput) error {
	url := r.url.Load()
	if url == nil {
		return errors.New("harness: the app's flow route is not known yet")
	}
	err := worlds.RemoteExecutor{URL: *url, Token: r.token}.Execute(ctx, runtime, input)
	if os.Getenv("HARNESS_DEBUG") != "" {
		step := ""
		if input.StepID != nil {
			step = string(*input.StepID)
		}
		log.Printf("harness: delivered run=%s step=%s message=%s attempt=%d count=%d notBefore=%v: %v", input.RunID, step, input.MessageID, input.Attempt, input.DeliveryCount, input.NotBefore, err)
	}
	return err
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	listener, err := net.Listen("tcp", cmp.Or(os.Getenv("HARNESS_ADDR"), "127.0.0.1:0"))
	if err != nil {
		log.Fatal(err)
	}
	baseURL := "http://" + listener.Addr().String() + worlds.HTTPPrefixPath("")
	token := os.Getenv("WORKFLOW_HTTP_TOKEN")
	executor := &remote{token: token}
	if url := os.Getenv("HARNESS_REMOTE_URL"); url != "" {
		executor.url.Store(&url)
	}
	world, closeWorld, err := open(ctx, os.Getenv("HARNESS_WORLD"), baseURL, executor)
	if err != nil {
		log.Fatal(err)
	}
	defer closeWorld()
	if debug := os.Getenv("HARNESS_DEBUG"); debug != "" {
		// A path appends the log there, which outlives the app's output.
		if strings.Contains(debug, "/") {
			file, err := os.OpenFile(debug, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
			if err != nil {
				log.Fatal(err)
			}
			defer file.Close()
			log.SetOutput(file)
		}
		defer dumpEvents(world)
	}

	var options []worlds.HTTPOption
	if token != "" {
		options = append(options, worlds.WithClientToken(token))
	}
	mux := http.NewServeMux()
	var handler http.Handler = worlds.HTTPHandler(world, "", options...)
	if os.Getenv("HARNESS_DEBUG") != "" {
		handler = logRPC(handler)
	}
	mux.Handle("/", handler)
	mux.HandleFunc("POST /_harness/remote", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil || !strings.HasPrefix(string(body), "http") {
			http.Error(w, "harness: the body is the app's flow route URL", http.StatusBadRequest)
			return
		}
		url := string(body)
		executor.url.Store(&url)
		w.WriteHeader(http.StatusNoContent)
	})
	if err := json.NewEncoder(os.Stdout).Encode(map[string]string{"baseURL": baseURL}); err != nil {
		log.Fatal(err)
	}
	// Node.js gives a child a socket for its standard input, and a shell a
	// named pipe.
	if stat, err := os.Stdin.Stat(); err == nil && stat.Mode()&(os.ModeNamedPipe|os.ModeSocket) != 0 {
		go func() {
			// The process that started the harness holds the pipe, which
			// closes when that process exits.
			_, _ = io.Copy(io.Discard, os.Stdin)
			cancel()
		}()
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signals
		cancel()
	}()
	server := &http.Server{Handler: mux}
	go func() {
		<-ctx.Done()
		shutdown, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		_ = server.Shutdown(shutdown)
	}()
	if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

// logRPC logs each event write and the error of each RPC call, for
// HARNESS_DEBUG.
func logRPC(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/rpc") {
			next.ServeHTTP(w, r)
			return
		}
		body, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(body))
		recorder := &bodyRecorder{ResponseWriter: w}
		next.ServeHTTP(recorder, r)
		var request struct {
			Method string         `cbor:"method"`
			Input  map[string]any `cbor:"input"`
		}
		_ = cbor.Unmarshal(body, &request)
		var response struct {
			Output any `cbor:"output"`
			Error  any `cbor:"error"`
		}
		_ = cbor.Unmarshal(recorder.body.Bytes(), &response)
		if strings.HasPrefix(request.Method, "events.create") || request.Method == "runs.encryptionKey" || response.Error != nil {
			log.Printf("harness: rpc %s input=%v error=%v output=%.600v", request.Method, summarize(request.Input), response.Error, response.Output)
		}
	})
}

// summarize returns input without its byte payloads.
func summarize(input map[string]any) map[string]any {
	out := make(map[string]any, len(input))
	for key, value := range input {
		switch value := value.(type) {
		case []byte:
			out[key] = fmt.Sprintf("<%d bytes>", len(value))
		case map[string]any:
			out[key] = summarize(value)
		case []any:
			items := make([]any, len(value))
			for index, item := range value {
				if item, ok := item.(map[string]any); ok {
					items[index] = summarize(item)
				} else {
					items[index] = item
				}
			}
			out[key] = items
		default:
			out[key] = value
		}
	}
	return out
}

type bodyRecorder struct {
	http.ResponseWriter
	body bytes.Buffer
}

func (b *bodyRecorder) Write(data []byte) (int, error) {
	b.body.Write(data)
	return b.ResponseWriter.Write(data)
}

// dumpEvents logs every run's events, for HARNESS_DEBUG.
func dumpEvents(world worlds.World) {
	ctx := context.Background()
	runs, err := world.RunsList(ctx, worlds.RunsListInput{ResolveData: worlds.ResolveDataNone})
	if err != nil {
		log.Printf("harness: list runs: %v", err)
		return
	}
	for _, run := range runs.Data {
		page, err := world.EventsList(ctx, worlds.EventsListInput{RunID: run.RunID})
		if err != nil {
			log.Printf("harness: list events of %s: %v", run.RunID, err)
			continue
		}
		for _, event := range page.Data {
			log.Printf("harness: event run=%s %s %s %s %s", run.RunID, event.CreatedAt.Format(time.RFC3339Nano), event.EventType, event.CorrelationID, event.Data)
		}
	}
}

// dropSchema drops the postgres World's schema.
func dropSchema(schema string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, cmp.Or(os.Getenv("WORKFLOW_POSTGRES_URL"), os.Getenv("DATABASE_URL")))
	if err != nil {
		log.Printf("harness: drop schema %s: %v", schema, err)
		return
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE"); err != nil {
		log.Printf("harness: drop schema %s: %v", schema, err)
	}
}

// open returns the World named kind, which executes deliveries with
// executor, and a function that closes it.
func open(ctx context.Context, kind, baseURL string, executor worlds.Executor) (worlds.World, func(), error) {
	switch kind {
	case "", "testworld":
		world := testworld.New(executor)
		return world, world.Close, nil
	case "local":
		dir, err := os.MkdirTemp("", "harness-local-")
		if err != nil {
			return nil, nil, err
		}
		world, err := localworld.Open(localworld.Config{Dir: dir, BaseURL: baseURL}, executor)
		if err != nil {
			return nil, nil, err
		}
		if err := world.Prepare(ctx); err != nil {
			return nil, nil, err
		}
		return world, func() {
			_ = world.Close()
			_ = os.RemoveAll(dir)
		}, nil
	case "postgres":
		schema := fmt.Sprintf("harness_%d_%d", os.Getpid(), time.Now().UnixNano())
		world, err := postgresworld.Open(ctx, postgresworld.Config{BaseURL: baseURL, Schema: schema})
		if err != nil {
			return nil, nil, err
		}
		go func() {
			if err := postgresworld.Worker(ctx, world, executor); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("harness: postgres worker: %v", err)
			}
		}()
		return world, func() {
			world.Close()
			dropSchema(schema)
		}, nil
	case "aws":
		return openAWS(ctx, baseURL, executor)
	case "vercel":
		return openVercel(ctx, baseURL, executor)
	default:
		return nil, nil, fmt.Errorf("harness: unknown HARNESS_WORLD %q", kind)
	}
}
