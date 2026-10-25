// app represents the application process. In development, the default
// ENVIRONMENT, it starts a local world in this process and serves it at
// /v1/workflow for the web UI. Any other ENVIRONMENT schedules workflow runs
// through the worker HTTP endpoint instead.
package main

import (
	"cmp"
	"context"
	"errors"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/examples/basic/workflows"
	"github.com/eriicafes/go-workflow/examples/basic/workflows/flows"
	"github.com/eriicafes/go-workflow/worlds"
	localworld "github.com/eriicafes/go-workflow/worlds/local"
)

func main() {
	port := cmp.Or(os.Getenv("PORT"), "8080")
	address := cmp.Or(os.Getenv("ADDR"), ":"+port)

	mux := routes()
	client, closeWorld, err := workflowClient(mux, "http://localhost:"+port+"/v1/workflow")
	if err != nil {
		log.Fatal(err)
	}

	// Middleware puts the client in every request context, so handlers can
	// call workflow.Start with r.Context().
	handler := workflow.Middleware(mux, client)

	log.Printf("application listening on %s", address)
	if err := http.ListenAndServe(address, handler); err != nil {
		log.Print(err)
	}
	if err := closeWorld(); err != nil {
		log.Print(err)
	}
}

// workflowClient returns the client the app schedules runs with. A local world
// is also mounted on mux, so the web UI and webhooks can reach it at baseURL.
func workflowClient(mux *http.ServeMux, baseURL string) (workflow.Client, func() error, error) {
	if cmp.Or(os.Getenv("ENVIRONMENT"), "development") != "development" {
		client := workflow.NewHTTPClient(
			cmp.Or(os.Getenv("WORKFLOW_URL"), "http://localhost:8081/v1/workflow"),
		)
		return client, func() error { return nil }, nil
	}

	runners := worlds.Runners(
		workflow.Use(flows.GenerateReport),
		workflow.Use(flows.PublishReport),
	)

	world, err := localworld.Open(localworld.Config{
		Dir:     ".workflows",
		BaseURL: cmp.Or(os.Getenv("WORKFLOWS_LOCAL_BASE_URL"), baseURL),
	}, runners)
	if err != nil {
		return nil, nil, err
	}
	if err := world.Prepare(context.Background()); err != nil {
		_ = world.Close()
		return nil, nil, err
	}
	mux.Handle("/v1/workflow/", localworld.HTTP(world))

	log.Print("using local workflow world at .workflows")
	return workflow.NewClient(world), world.Close, nil
}

func routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		render(w, "index", page{Title: "Reports"})
	})
	mux.HandleFunc("GET /reports/{id}", func(w http.ResponseWriter, r *http.Request) {
		data, ok := runPage[workflows.GenerateReportResult](w, r, "Report")
		if !ok {
			return
		}
		if data.output != nil {
			data.Report = data.output
		}
		render(w, "run", data.page)
	})
	mux.HandleFunc("GET /reports/publish/{id}", func(w http.ResponseWriter, r *http.Request) {
		data, ok := runPage[workflows.PublishReportResult](w, r, "Published report")
		if !ok {
			return
		}
		data.Reviewing = data.Running
		if data.output != nil {
			data.Draft = data.output.Draft
			data.Location = data.output.Location
			data.Reviews = data.output.Reviews
		}
		render(w, "run", data.page)
	})
	mux.HandleFunc("GET /runs/{id}/progress", func(w http.ResponseWriter, r *http.Request) {
		// startIndex resumes from a chunk; a negative value reads the last
		// chunks.
		startIndex := 0
		if value := r.URL.Query().Get("startIndex"); value != "" {
			index, err := strconv.Atoi(value)
			if err != nil {
				http.Error(w, "invalid startIndex", http.StatusBadRequest)
				return
			}
			startIndex = index
		}
		run, err := workflow.Get[any](r.Context(), r.PathValue("id"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		reader, err := run.Reader(r.Context(), workflow.ReaderOption{Name: workflows.ProgressStream, StartIndex: startIndex})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Relay each write as it arrives until the run closes the stream.
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		flusher, _ := w.(http.Flusher)
		for {
			chunk, err := reader.Next()
			if err != nil {
				return
			}
			if _, err := w.Write(chunk.Data); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
	})
	mux.HandleFunc("POST /runs/{id}/review", func(w http.ResponseWriter, r *http.Request) {
		runID := r.PathValue("id")
		review := workflows.Review{
			Reviewer: r.PostFormValue("reviewer"),
			Approved: r.PostFormValue("decision") == "approve",
			Comment:  r.PostFormValue("comment"),
		}
		if !review.Approved && review.Comment == "" {
			http.Error(w, "a comment is required to request changes", http.StatusBadRequest)
			return
		}

		err := workflow.ResumeHook(r.Context(), workflows.ReviewToken(runID), review)
		if errors.Is(err, workflow.ErrHookNotFound) {
			http.Error(w, "this run is not waiting for a review", http.StatusConflict)
			return
		}
		if err != nil {
			http.Error(w, "send review: "+err.Error(), http.StatusInternalServerError)
			return
		}

		http.Redirect(w, r, "/reports/publish/"+url.PathEscape(runID), http.StatusSeeOther)
	})
	mux.HandleFunc("POST /reports", func(w http.ResponseWriter, r *http.Request) {
		input := workflows.GenerateReport{Topic: r.PostFormValue("topic")}
		if input.Topic == "" {
			http.Error(w, "topic is required", http.StatusBadRequest)
			return
		}

		run, err := workflow.Start(r.Context(), input)
		if err != nil {
			http.Error(w, "schedule workflow: "+err.Error(), http.StatusInternalServerError)
			return
		}

		http.Redirect(w, r, "/reports/"+url.PathEscape(run.ID()), http.StatusSeeOther)
	})
	mux.HandleFunc("POST /reports/publish", func(w http.ResponseWriter, r *http.Request) {
		input := workflows.PublishReport{Topic: r.PostFormValue("topic")}
		if input.Topic == "" {
			http.Error(w, "topic is required", http.StatusBadRequest)
			return
		}

		run, err := workflow.Start(r.Context(), input)
		if err != nil {
			http.Error(w, "schedule publish workflow: "+err.Error(), http.StatusInternalServerError)
			return
		}

		http.Redirect(w, r, "/reports/publish/"+url.PathEscape(run.ID()), http.StatusSeeOther)
	})
	return mux
}
