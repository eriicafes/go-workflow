// Package vercelworld contains the Vercel deployment integration for workflow
// worlds.
package vercelworld

import (
	"encoding/json"
	"net/http"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/workflowops"
)

// HTTP returns the worker HTTP API consumed by workflow.Client.
func HTTP(world *World) http.Handler {
	return workflow.NewHTTPHandler(world)
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
		if err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	})
}
