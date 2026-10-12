package workflow

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/eriicafes/go-workflow/workflowops"
)

// APIPrefix is the versioned base path for workflow HTTP endpoints.
const APIPrefix = "/.well-known/workflow/v1"

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
	runInfoResponse struct {
		Info  workflowops.RunInfo `json:"info"`
		Found bool                `json:"found"`
	}
	wakeResponse struct {
		StoppedCount int `json:"stoppedCount"`
	}
	errorResponse struct {
		Error string `json:"error"`
	}
	cancelRequest struct {
		Tasks []workflowops.Task `json:"tasks"`
	}
	streamReadResponse struct {
		workflowops.StreamRead
	}
	streamAppendRequest struct {
		Stream workflowops.Stream `json:"stream"`
		Lock   string             `json:"lock"`
		Data   []byte             `json:"data"`
	}
	streamReleaseRequest struct {
		Stream workflowops.Stream `json:"stream"`
		Lock   string             `json:"lock"`
		Close  bool               `json:"close"`
	}
	streamLockRequest struct {
		Stream workflowops.Stream `json:"stream"`
		Lock   string             `json:"lock"`
	}
	streamLockResponse struct {
		Acquired bool `json:"acquired"`
	}
	hookResponse struct {
		Hook  workflowops.Hook `json:"hook"`
		Found bool             `json:"found"`
	}
	hookResumeRequest struct {
		Payload json.RawMessage `json:"payload"`
	}
)

// NewHTTPHandler exposes a World through the API consumed by NewHTTPClient.
func NewHTTPHandler(world World) http.Handler {
	client := Client(world)
	mux := http.NewServeMux()
	mux.Handle(APIPrefix+"/webhook/{token}", webhookHandler(world))
	mux.HandleFunc("POST "+APIPrefix+"/runs", func(w http.ResponseWriter, r *http.Request) {
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
	mux.HandleFunc("POST "+APIPrefix+"/runs/cancel", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var cancel cancelRequest
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&cancel); err != nil || len(cancel.Tasks) == 0 {
			writeError(w, http.StatusBadRequest, "workflow: tasks are required")
			return
		}
		if err := client.CancelOperations(r.Context(), cancel.Tasks...); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET "+APIPrefix+"/runs/{id}", func(w http.ResponseWriter, r *http.Request) {
		output, err := client.WaitRun(r.Context(), r.PathValue("id"))
		if err != nil {
			writeRunError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, runResponse{Output: output})
	})
	mux.HandleFunc("GET "+APIPrefix+"/runs/{id}/info", func(w http.ResponseWriter, r *http.Request) {
		info, found, err := client.GetRun(r.Context(), r.PathValue("id"))
		if err != nil {
			writeRunError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, runInfoResponse{Info: info, Found: found})
	})
	mux.HandleFunc("POST "+APIPrefix+"/runs/{id}/wake", func(w http.ResponseWriter, r *http.Request) {
		count, err := client.Wake(r.Context(), r.PathValue("id"))
		if err != nil {
			writeRunError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, wakeResponse{StoppedCount: count})
	})
	mux.HandleFunc("GET "+APIPrefix+"/runs/{id}/streams", func(w http.ResponseWriter, r *http.Request) {
		var offset int64
		if _, err := fmt.Sscan(r.URL.Query().Get("offset"), &offset); err != nil {
			writeError(w, http.StatusBadRequest, "workflow: invalid stream offset")
			return
		}
		value, err := client.ReadStream(r.Context(), workflowops.Stream{
			RunID:     r.PathValue("id"),
			Namespace: r.URL.Query().Get("namespace"),
		}, offset)
		if err != nil {
			writeRunError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, streamReadResponse{StreamRead: value})
	})
	mux.HandleFunc("POST "+APIPrefix+"/streams/append", func(w http.ResponseWriter, r *http.Request) {
		var request streamAppendRequest
		if !decodeRequest(w, r, &request) {
			return
		}
		if request.Stream.RunID == "" || request.Lock == "" {
			writeError(w, http.StatusBadRequest, "workflow: stream and lock are required")
			return
		}
		if err := client.AppendStream(r.Context(), request.Stream, request.Lock, request.Data); err != nil {
			writeStreamError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST "+APIPrefix+"/streams/lock", func(w http.ResponseWriter, r *http.Request) {
		var request streamLockRequest
		if !decodeRequest(w, r, &request) {
			return
		}
		if request.Stream.RunID == "" || request.Lock == "" {
			writeError(w, http.StatusBadRequest, "workflow: stream and lock are required")
			return
		}
		acquired, err := client.LockStream(r.Context(), request.Stream, request.Lock)
		if err != nil {
			writeStreamError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, streamLockResponse{Acquired: acquired})
	})
	mux.HandleFunc("POST "+APIPrefix+"/streams/release", func(w http.ResponseWriter, r *http.Request) {
		var request streamReleaseRequest
		if !decodeRequest(w, r, &request) {
			return
		}
		if request.Stream.RunID == "" || request.Lock == "" {
			writeError(w, http.StatusBadRequest, "workflow: stream and lock are required")
			return
		}
		if err := client.ReleaseStream(r.Context(), request.Stream, request.Lock, request.Close); err != nil {
			writeStreamError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET "+APIPrefix+"/hooks/{token}", func(w http.ResponseWriter, r *http.Request) {
		hook, found, err := client.GetHook(r.Context(), r.PathValue("token"))
		if err != nil {
			writeRunError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, hookResponse{Hook: hook, Found: found})
	})
	mux.HandleFunc("POST "+APIPrefix+"/hooks/{token}", func(w http.ResponseWriter, r *http.Request) {
		var request hookResumeRequest
		if !decodeRequest(w, r, &request) {
			return
		}
		if len(request.Payload) == 0 {
			writeError(w, http.StatusBadRequest, "workflow: hook payload is required")
			return
		}
		if err := client.ResumeWebhook(r.Context(), r.PathValue("token"), request.Payload); err != nil {
			if err == workflowops.ErrHookNotFound {
				writeError(w, http.StatusNotFound, err.Error())
				return
			}
			writeRunError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST "+APIPrefix+"/webhooks/{token}/response", func(w http.ResponseWriter, r *http.Request) {
		var response json.RawMessage
		if !decodeRequest(w, r, &response) {
			return
		}
		if err := client.ReplyWebhook(r.Context(), r.PathValue("token"), response); err != nil {
			writeRunError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET "+APIPrefix+"/webhooks/{token}/response", func(w http.ResponseWriter, r *http.Request) {
		response, err := client.WaitWebhookResponse(r.Context(), r.PathValue("token"))
		if err != nil {
			writeRunError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, response)
	})
	return mux
}

func decodeRequest(w http.ResponseWriter, r *http.Request, value any) bool {
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		writeError(w, http.StatusBadRequest, "workflow: invalid request")
		return false
	}
	return true
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

func writeStreamError(w http.ResponseWriter, err error) {
	writeRunError(w, err)
}
