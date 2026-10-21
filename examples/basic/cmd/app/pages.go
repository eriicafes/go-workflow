package main

import (
	"embed"
	"errors"
	"html/template"
	"log"
	"net/http"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/examples/basic/workflows"
	"github.com/eriicafes/go-workflow/worlds"
)

//go:embed templates
var templateFiles embed.FS

var pages = map[string]*template.Template{
	"index": parsePage("templates/index.html"),
	"run":   parsePage("templates/run.html"),
}

func parsePage(name string) *template.Template {
	return template.Must(template.ParseFS(templateFiles, "templates/layout.html", name))
}

// page is the data every template renders. Running is set while the run has
// not finished, so the page follows its progress stream.
type page struct {
	Title     string
	RunID     string
	Status    workflow.Status
	Report    *workflows.GenerateReportResult
	Draft     string
	Location  string
	Reviews   []workflows.Review
	Reviewing bool
	Error     string
	Running   bool
}

func render(w http.ResponseWriter, name string, data page) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := pages[name].ExecuteTemplate(w, "layout", data); err != nil {
		log.Print(err)
	}
}

type runData[Out any] struct {
	page
	output *Out
}

// runPage reads the run named by the id path value. It returns the output only
// once the run has completed, so the page never blocks on a running workflow.
func runPage[Out any](w http.ResponseWriter, r *http.Request, title string) (runData[Out], bool) {
	data := runData[Out]{page: page{Title: title, RunID: r.PathValue("id")}}
	run, err := workflow.Get[Out](r.Context(), data.RunID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return data, false
	}
	data.Status, err = run.Status(r.Context())
	if errors.Is(err, worlds.ErrRunNotFound) {
		http.NotFound(w, r)
		return data, false
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return data, false
	}
	if !data.Status.Terminal() {
		data.Running = true
		return data, true
	}
	output, err := run.Value(r.Context())
	if err != nil {
		data.Error = err.Error()
		return data, true
	}
	data.output = &output
	return data, true
}
