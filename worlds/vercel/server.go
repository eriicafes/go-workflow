// Package vercelworld contains the Vercel deployment integration for workflow
// worlds.
package vercelworld

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/workflowops"
)

// HTTP returns the worker HTTP API consumed by workflow.Client. When QStash is
// configured, it also mounts the signed timer-delivery endpoint at the fixed
// QStash webhook path.
func HTTP(world *World, runners workflowops.RunnerSet) http.Handler {
	api := workflow.NewHTTPHandler(world).(*http.ServeMux)
	if world.config.QStash == nil {
		return api
	}
	api.Handle(workflow.APIPrefix+"/webhook/qstash", qstashHandler(world, runners))
	return api
}

// Queue returns an HTTP consumer for a Vercel Queue message containing a
// workflow.Task JSON body. A non-2xx response lets Vercel retry delivery.
func Queue(world *World, runners workflowops.RunnerSet) http.Handler {
	engine := workflowops.NewEngine(world, runners)
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		defer request.Body.Close()
		var task workflowops.Task
		if err := json.NewDecoder(request.Body).Decode(&task); err != nil || task.RunID == "" {
			http.Error(writer, "invalid workflow queue message", http.StatusBadRequest)
			return
		}
		lease, err := world.ClaimTask(request.Context(), task)
		if err == nil && lease != nil {
			err = engine.Execute(request.Context(), lease)
		}
		if err != nil && !errors.Is(err, workflowops.ErrCancelled) {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	})
}
