// server serves the durable chat API that the web client's
// WorkflowChatTransport talks to. Workflows run in this process on a local
// world, which keeps runs and their streams in .workflows, so a restarted
// server continues interrupted runs and clients reconnect to their streams.
package main

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/digitallysavvy/go-ai/pkg/ai"
	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/examples/durable-agent/workflows"
	"github.com/eriicafes/go-workflow/examples/durable-agent/workflows/flows"
	"github.com/eriicafes/go-workflow/workflowai"
	"github.com/eriicafes/go-workflow/worlds"
	localworld "github.com/eriicafes/go-workflow/worlds/local"
)

func main() {
	// Chat builds its model itself. Check here that it can, so a missing key
	// fails at startup rather than in a run.
	if _, err := flows.LanguageModel(); err != nil {
		log.Fatal(err)
	}
	if os.Getenv("TOOL_APPROVAL_SECRET") == "" {
		log.Print("TOOL_APPROVAL_SECRET is not set, so tool approvals are not signed")
	}

	port := cmp.Or(os.Getenv("PORT"), "8080")
	world, err := localworld.Open(localworld.Config{
		Dir:     ".workflows",
		BaseURL: cmp.Or(os.Getenv("WORKFLOW_BASE_URL"), "http://localhost:"+port+"/v1/workflow"),
	}, worlds.Runners(workflow.Use(flows.Chat)))
	if err != nil {
		log.Fatal(err)
	}
	if err := world.Prepare(context.Background()); err != nil {
		log.Fatal(err)
	}

	mux := routes()
	// The web UI inspects runs through the world's RPC endpoint.
	mux.Handle("/v1/workflow/", localworld.HTTP(world))
	// Serve the client when it is built, so it needs no dev server.
	mux.Handle("/", http.FileServer(http.Dir("web/dist")))
	server := &http.Server{Addr: ":" + port, Handler: workflow.Middleware(mux, workflow.NewClient(world))}

	// On a signal, stop serving and let in-flight deliveries finish before
	// the world closes. A delivery that is cut short runs again on the next
	// start.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		server.Close()
	}()
	log.Printf("chat server listening on %s", server.Addr)
	if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		log.Print(err)
	}
	if err := world.Close(); err != nil {
		log.Print(err)
	}
}

func routes() *http.ServeMux {
	mux := http.NewServeMux()

	// POST /api/chat starts a run for one chat turn and streams it. The run
	// ID header lets the client reconnect to the run's stream.
	mux.HandleFunc("POST /api/chat", func(w http.ResponseWriter, r *http.Request) {
		var input workflows.Chat
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			http.Error(w, "invalid chat request: "+err.Error(), http.StatusBadRequest)
			return
		}
		run, err := workflow.Start(r.Context(), input)
		if err != nil {
			http.Error(w, "start chat: "+err.Error(), http.StatusInternalServerError)
			return
		}
		streamRun(w, r, run.ID(), 0)
	})

	// GET /api/chat/{runId}/stream reconnects to a run's stream, skipping
	// the startIndex UI chunks the client already received.
	mux.HandleFunc("GET /api/chat/{runId}/stream", func(w http.ResponseWriter, r *http.Request) {
		startIndex, err := strconv.Atoi(cmp.Or(r.URL.Query().Get("startIndex"), "0"))
		if err != nil || startIndex < 0 {
			http.Error(w, "startIndex must be a non-negative integer", http.StatusBadRequest)
			return
		}
		streamRun(w, r, r.PathValue("runId"), startIndex)
	})
	return mux
}

// streamRun sends a run's stream as a UI message stream. The stream holds raw
// model call parts, so it is always read from its start and the first
// uiStartIndex UI chunks are skipped.
func streamRun(w http.ResponseWriter, r *http.Request, runID string, uiStartIndex int) {
	run, err := workflow.Get[workflows.ChatResult](r.Context(), runID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	reader, err := run.Reader(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	chunks := workflowai.UIMessageChunks(r.Context(), reader, uiStartIndex)
	err = ai.PipeUIMessageChunksToResponse(chunks, w, &ai.UIMessageStreamResponseInit{
		Headers: map[string]string{"X-Workflow-Run-Id": runID},
	})
	if err != nil && r.Context().Err() == nil {
		log.Printf("stream run %s: %v", runID, err)
	}
}
