// app represents the application process. By default it schedules workflow
// runs through the worker HTTP endpoint. WORKFLOWS_LOCAL=true starts a local
// world in this process instead.
package main

import (
	"cmp"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/examples/basic/flows"
	"github.com/eriicafes/go-workflow/examples/basic/workflows"
	"github.com/eriicafes/go-workflow/worlds"
	localworld "github.com/eriicafes/go-workflow/worlds/local"
)

func main() {
	client, closeWorld, err := workflowClient()
	if err != nil {
		log.Fatal(err)
	}

	// Middleware puts the client in every request context, so handlers can
	// call workflow.Start with r.Context().
	handler := workflow.Middleware(routes(), client)

	address := cmp.Or(
		os.Getenv("ADDR"),
		":"+cmp.Or(os.Getenv("PORT"), "8080"),
	)
	log.Printf("application listening on %s", address)
	if err := http.ListenAndServe(address, handler); err != nil {
		log.Print(err)
	}
	if err := closeWorld(); err != nil {
		log.Print(err)
	}
}

func workflowClient() (workflow.Client, func() error, error) {
	if os.Getenv("WORKFLOWS_LOCAL") != "true" {
		client := workflow.NewHTTPClient(
			cmp.Or(os.Getenv("WORKFLOW_URL"), "http://localhost:8081"),
		)
		return client, func() error { return nil }, nil
	}

	runners := worlds.Runners(
		workflow.Use(flows.GenerateReport),
		workflow.Use(flows.PublishReport),
	)

	world, err := localworld.Open(localworld.Config{Dir: ".workflows"}, runners)
	if err != nil {
		return nil, nil, err
	}
	if err := world.Prepare(context.Background()); err != nil {
		_ = world.Close()
		return nil, nil, err
	}

	log.Print("using local workflow world at .workflows")
	return workflow.NewClient(world), world.Close, nil
}

func routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /reports", createReport)
	mux.HandleFunc("POST /reports/publish", publishReport)
	return mux
}

func createReport(w http.ResponseWriter, r *http.Request) {
	var input workflows.GenerateReport
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		http.Error(w, "invalid report input: "+err.Error(), http.StatusBadRequest)
		return
	}
	if input.Topic == "" {
		http.Error(w, "topic is required", http.StatusBadRequest)
		return
	}

	run, err := workflow.Start(r.Context(), input)
	if err != nil {
		http.Error(w, "schedule workflow: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{"runId": run.ID()})
}

func publishReport(w http.ResponseWriter, r *http.Request) {
	var input workflows.PublishReport
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		http.Error(w, "invalid publish input: "+err.Error(), http.StatusBadRequest)
		return
	}
	if input.Topic == "" {
		http.Error(w, "topic is required", http.StatusBadRequest)
		return
	}

	run, err := workflow.Start(r.Context(), input)
	if err != nil {
		http.Error(w, "schedule publish workflow: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{"runId": run.ID()})
}
