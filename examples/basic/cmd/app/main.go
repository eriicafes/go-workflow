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
	"github.com/eriicafes/go-workflow/examples/basic/workflowdefs"
	localworld "github.com/eriicafes/go-workflow/worlds/local"
)

type app struct {
	workflows workflow.Client
}

func main() {
	workflows, closeWorld, err := workflowClient()
	if err != nil {
		log.Fatal(err)
	}

	server := app{workflows: workflows}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /reports", server.createReport)
	mux.HandleFunc("POST /reports/publish", server.publishReport)

	address := cmp.Or(
		os.Getenv("ADDR"),
		":"+cmp.Or(os.Getenv("PORT"), "8080"),
	)
	log.Printf("application listening on %s", address)
	if err := http.ListenAndServe(address, mux); err != nil {
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

	runners, err := workflow.Runners(
		workflow.Use(flows.GenerateReport),
		workflow.Use(flows.PublishReport),
	)
	if err != nil {
		return nil, nil, err
	}

	world, err := localworld.Open(".workflows", runners)
	if err != nil {
		return nil, nil, err
	}
	if err := world.Start(context.Background()); err != nil {
		_ = world.Close()
		return nil, nil, err
	}

	log.Print("using local workflow world at .workflows")
	return world, world.Close, nil
}

func (a app) createReport(w http.ResponseWriter, r *http.Request) {
	var input workflowdefs.GenerateReport
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		http.Error(w, "invalid report input: "+err.Error(), http.StatusBadRequest)
		return
	}
	if input.Topic == "" {
		http.Error(w, "topic is required", http.StatusBadRequest)
		return
	}

	// The client supplies the scheduling capability to this request context.
	run, err := workflow.Start(workflow.NewContext(r.Context(), a.workflows), input)
	if err != nil {
		http.Error(w, "schedule workflow: "+err.Error(), http.StatusInternalServerError)
		return
	}

	writeRun(w, run.ID())
}

func (a app) publishReport(w http.ResponseWriter, r *http.Request) {
	var input workflowdefs.PublishReport
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		http.Error(w, "invalid publish input: "+err.Error(), http.StatusBadRequest)
		return
	}
	if input.Topic == "" {
		http.Error(w, "topic is required", http.StatusBadRequest)
		return
	}

	run, err := workflow.Start(workflow.NewContext(r.Context(), a.workflows), input)
	if err != nil {
		http.Error(w, "schedule publish workflow: "+err.Error(), http.StatusInternalServerError)
		return
	}

	writeRun(w, run.ID())
}

func writeRun(w http.ResponseWriter, id string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(struct {
		RunID string `json:"runId"`
	}{RunID: id})
}
