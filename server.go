package workflow

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/eriicafes/go-workflow/workflowops"
)

const apiPrefix = "/v1/runs"

type (
	startRequest struct {
		Identity string          `json:"identity"`
		Input    json.RawMessage `json:"input"`
	}
	startResponse struct {
		ID string `json:"id"`
	}
	runResponse struct {
		Output json.RawMessage `json:"output"`
	}
	eventsResponse struct {
		Events []Event `json:"events"`
	}
	errorResponse struct {
		Error string `json:"error"`
	}
)

// NewHTTPHandler exposes a Client through the API consumed by NewHTTPClient.
func NewHTTPHandler(client Client) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+apiPrefix, func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var start startRequest
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&start); err != nil || start.Identity == "" || len(start.Input) == 0 {
			writeError(w, http.StatusBadRequest, "workflow: identity and input are required")
			return
		}
		id, err := client.CreateRun(r.Context(), nil, workflowops.RunRequest{
			Identity:    start.Identity,
			Fingerprint: start.Identity,
			Input:       start.Input,
			Sequence:    workflowops.RootSequence,
			Kind:        workflowops.KindRun,
		})
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusAccepted, startResponse{ID: id})
	})
	mux.HandleFunc("GET "+apiPrefix+"/{id}", func(w http.ResponseWriter, r *http.Request) {
		output, err := client.WaitRun(r.Context(), r.PathValue("id"))
		if err != nil {
			writeRunError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, runResponse{Output: output})
	})
	mux.HandleFunc("GET "+apiPrefix+"/{id}/events", func(w http.ResponseWriter, r *http.Request) {
		var after int64
		if _, err := fmt.Sscan(r.URL.Query().Get("after"), &after); err != nil {
			writeError(w, http.StatusBadRequest, "workflow: invalid event offset")
			return
		}
		events, err := client.Events(r.Context(), r.PathValue("id"), after)
		if err != nil {
			writeRunError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, eventsResponse{Events: events})
	})
	return mux
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, errorResponse{Error: message})
}

func writeRunError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	if err == ErrRunNotFound {
		status = http.StatusNotFound
	}
	writeError(w, status, err.Error())
}
