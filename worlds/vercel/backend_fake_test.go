package vercelworld

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/fxamacker/cbor/v2"

	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/testworld"
)

// fakeBackend imitates Vercel's workflow API over a testworld. It keeps an
// event only as the backend does, as the metadata fields the backend knows
// plus one payload, and answers repeats and refusals with the backend's
// statuses, so the World's tests exercise what survives the real backend.
type fakeBackend struct {
	t      testing.TB
	store  *testworld.World
	server *httptest.Server
	mu     sync.Mutex
	// requests counts requests by method and route.
	requests map[string]int
	// sockets serves stream sockets; the backend declines them otherwise.
	sockets bool
	// vercelIDs holds the vercelId of each event frame written with one.
	vercelIDs []string
	// specs holds the spec version each run was created with.
	specs map[worlds.RunID]int
	// heartbeat, when set, is the interval of the events socket's
	// heartbeats; noEventsSocket declines the events socket.
	heartbeat      time.Duration
	noEventsSocket bool
	// dropPush is a slot whose push the events socket loses.
	dropPush uint64
}

func newFakeBackend(t testing.TB) *fakeBackend {
	fake := &fakeBackend{t: t, store: testworld.NewManual(nil), requests: map[string]int{}, specs: map[worlds.RunID]int{}}
	fake.server = httptest.NewServer(http.HandlerFunc(fake.serve))
	t.Cleanup(func() {
		fake.server.Close()
		fake.store.Close()
	})
	return fake
}

// storedKeys maps each event write meta key the backend keeps to the
// eventData key it returns it under.
var storedKeys = map[string]string{
	"workflowName":            "workflowName",
	"deploymentId":            "deploymentId",
	"attributes":              "attributes",
	"allowReservedAttributes": "allowReservedAttributes",
	"stepName":                "stepName",
	"resumeAt":                "resumeAt",
	"retryAfter":              "retryAfter",
	"hookToken":               "token",
	"hookIsWebhook":           "isWebhook",
	"hookIsSystem":            "isSystem",
	"hookTokenRetentionUntil": "tokenRetentionUntil",
	"hookForce":               "force",
	"encryptionPublicKey":     "encryptionPublicKey",
	"attempt":                 "attempt",
	"inline":                  "inline",
	"creatorMessageId":        "creatorMessageId",
	"changes":                 "changes",
	"writer":                  "writer",
	"errorCode":               "errorCode",
	"cancelReason":            "cancelReason",
	"optimizations":           "optimizations",
	"finalSchedulingReplay":   "finalSchedulingReplay",
	"eventCount":              "eventCount",
	"stepCount":               "stepCount",
	"rsfs":                    "rsfs",
	"stso":                    "stso",
	"ttfs":                    "ttfs",
	"executionContext":        "executionContext",
	"startHook":               "startHook",
}

func (f *fakeBackend) count(route string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.requests[route]
}

func (f *fakeBackend) serve(writer http.ResponseWriter, request *http.Request) {
	ctx := request.Context()
	oidc := request.Header.Get("X-Vercel-Trusted-Oidc-Idp-Token")
	if oidc == "" || request.Header.Get("Authorization") != "Bearer "+oidc {
		writeError(writer, http.StatusUnauthorized, "unauthorized", "missing OIDC token")
		return
	}
	parts := strings.Split(strings.Trim(request.URL.Path, "/"), "/")
	route := request.Method + " " + routeName(parts)
	f.mu.Lock()
	f.requests[route]++
	f.mu.Unlock()
	query := request.URL.Query()
	switch {
	case len(parts) == 4 && parts[0] == "websockets" && parts[1] == "v1" && parts[2] == "runs":
		f.eventsSocket(ctx, writer, request, worlds.RunID(parts[3]))
	case len(parts) == 6 && parts[0] == "websockets" && parts[1] == "v1" && parts[2] == "runs" && parts[4] == "streams":
		if !f.sockets {
			writeError(writer, http.StatusNotFound, "not_found", "no stream sockets")
			return
		}
		mux := http.NewServeMux()
		mux.Handle("GET /websockets/v1/runs/{runId}/streams/{name}", worlds.StreamSocketHandler(f.store))
		mux.ServeHTTP(writer, request)
	case len(parts) == 3 && parts[0] == "v4" && parts[1] == "runs" && parts[2] == "purge" && request.Method == http.MethodPost:
		f.purge(ctx, writer, request)
	case len(parts) == 5 && eventsVersion(parts[0]) && parts[1] == "runs" && parts[3] == "events" && parts[4] == "batch" && request.Method == http.MethodPost:
		f.createBatch(ctx, writer, request, worlds.RunID(parts[2]), parts[0])
	case len(parts) == 5 && eventsVersion(parts[0]) && parts[1] == "runs" && parts[3] == "events" && request.Method == http.MethodPost:
		f.createEvent(ctx, writer, request, worlds.RunID(parts[2]), parts[0])
	case len(parts) == 4 && eventsVersion(parts[0]) && parts[1] == "runs" && parts[3] == "events" && request.Method == http.MethodGet:
		f.listEvents(ctx, writer, worlds.RunID(parts[2]), query, parts[0])
	case len(parts) == 5 && eventsVersion(parts[0]) && parts[1] == "runs" && parts[3] == "events" && request.Method == http.MethodGet:
		event, err := f.store.EventsGet(ctx, worlds.EventsGetInput{RunID: worlds.RunID(parts[2]), EventID: worlds.EventID(parts[4])})
		if err != nil {
			writeGoError(writer, err)
			return
		}
		writer.Header().Set("Content-Type", frameContentType)
		_, _ = writer.Write(f.eventFrame(event))
	case len(parts) == 2 && parts[0] == "v4" && parts[1] == "events":
		// The correlation route pages by cursor, limit and order alone.
		if query.Has("returnAll") {
			writeError(writer, http.StatusBadRequest, "validation-error", "the correlation listing takes no returnAll")
			return
		}
		page, err := f.store.EventsListByCorrelationID(ctx, worlds.EventsListByCorrelationIDInput{RunID: worlds.RunID(query.Get("runId")), CorrelationID: query.Get("correlationId")})
		if err != nil {
			writeGoError(writer, err)
			return
		}
		events, cursor, hasMore := pageEvents(page.Data, query)
		f.writeEvents(writer, events, cursor, hasMore, nil)
	case len(parts) == 3 && parts[0] == "v2" && parts[1] == "runs":
		run, err := f.store.RunsGet(ctx, worlds.RunsGetInput{RunID: worlds.RunID(parts[2])})
		if err != nil {
			writeGoError(writer, err)
			return
		}
		writeCBOR(writer, f.wireRun(run, query))
	case len(parts) == 4 && parts[0] == "v2" && parts[1] == "runs" && parts[3] == "status":
		wait, _ := strconv.Atoi(query.Get("waitMs"))
		run, err := f.store.RunsWaitForTerminalStatus(ctx, worlds.RunsWaitForTerminalStatusInput{RunID: worlds.RunID(parts[2]), Timeout: time.Duration(wait) * time.Millisecond})
		if err != nil {
			writeGoError(writer, err)
			return
		}
		writeCBOR(writer, f.wireRun(run, query))
	case len(parts) == 2 && parts[0] == "v2" && parts[1] == "runs":
		input := worlds.RunsListInput{WorkflowName: query.Get("workflowName"), Pagination: pagination(query)}
		if status := query.Get("status"); status != "" {
			input.Status = []worlds.Status{worlds.Status(status)}
		}
		page, err := f.store.RunsList(ctx, input)
		if err != nil {
			writeGoError(writer, err)
			return
		}
		data := make([]map[string]any, len(page.Data))
		for index, run := range page.Data {
			data[index] = f.wireRun(run, query)
		}
		writeCBOR(writer, map[string]any{"data": data, "cursor": nilIfEmpty(page.Cursor), "hasMore": page.HasMore})
	case len(parts) == 5 && parts[0] == "v2" && parts[1] == "runs" && parts[3] == "steps":
		step, err := f.store.StepsGet(ctx, worlds.StepsGetInput{RunID: worlds.RunID(parts[2]), StepID: worlds.StepID(parts[4])})
		if err != nil {
			writeGoError(writer, err)
			return
		}
		writeCBOR(writer, f.wireStep(ctx, step, query))
	case len(parts) == 4 && parts[0] == "v2" && parts[1] == "runs" && parts[3] == "steps":
		page, err := f.store.StepsList(ctx, worlds.StepsListInput{RunID: worlds.RunID(parts[2]), Pagination: pagination(query)})
		if err != nil {
			writeGoError(writer, err)
			return
		}
		data := make([]map[string]any, len(page.Data))
		for index, step := range page.Data {
			data[index] = f.wireStep(ctx, step, query)
		}
		writeCBOR(writer, map[string]any{"data": data, "cursor": nilIfEmpty(page.Cursor), "hasMore": page.HasMore})
	case len(parts) == 3 && parts[0] == "v2" && parts[1] == "hooks" && parts[2] == "by-token":
		hook, err := f.store.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: query.Get("token")})
		f.writeHook(ctx, writer, hook, err)
	case len(parts) == 3 && parts[0] == "v2" && parts[1] == "hooks":
		hook, err := f.store.HooksGet(ctx, worlds.HooksGetInput{HookID: worlds.HookID(parts[2])})
		f.writeHook(ctx, writer, hook, err)
	case len(parts) == 2 && parts[0] == "v2" && parts[1] == "hooks":
		page, err := f.store.HooksList(ctx, worlds.HooksListInput{RunID: worlds.RunID(query.Get("runId")), Pagination: pagination(query)})
		if err != nil {
			writeGoError(writer, err)
			return
		}
		data := make([]map[string]any, 0, len(page.Data))
		for _, hook := range page.Data {
			data = append(data, f.wireHook(ctx, hook))
		}
		writeCBOR(writer, map[string]any{"data": data, "cursor": nilIfEmpty(page.Cursor), "hasMore": page.HasMore})
	case len(parts) == 5 && parts[0] == "v2" && parts[1] == "runs" && parts[3] == "stream" && request.Method == http.MethodPut:
		f.writeStream(ctx, writer, request, worlds.RunID(parts[2]), parts[4])
	case len(parts) == 6 && parts[0] == "v2" && parts[1] == "runs" && parts[3] == "streams" && parts[5] == "chunks":
		input := worlds.StreamsGetChunksInput{RunID: worlds.RunID(parts[2]), Name: parts[4], Cursor: query.Get("cursor")}
		input.Limit, _ = strconv.Atoi(query.Get("limit"))
		chunks, err := f.store.StreamsGetChunks(ctx, input)
		if err != nil {
			writeGoError(writer, err)
			return
		}
		data := make([]map[string]any, len(chunks.Data))
		for index, chunk := range chunks.Data {
			data[index] = map[string]any{"index": chunk.Index, "data": chunk.Data}
		}
		writeCBOR(writer, map[string]any{"data": data, "cursor": nilIfEmpty(chunks.Cursor), "hasMore": chunks.HasMore, "done": chunks.Done})
	case len(parts) == 6 && parts[0] == "v2" && parts[1] == "runs" && parts[3] == "streams" && parts[5] == "info":
		info, err := f.store.StreamsGetInfo(ctx, worlds.StreamsGetInfoInput{RunID: worlds.RunID(parts[2]), Name: parts[4]})
		if err != nil {
			writeGoError(writer, err)
			return
		}
		writeCBOR(writer, map[string]any{"tailIndex": info.TailIndex, "done": info.Done})
	case len(parts) == 4 && parts[0] == "v2" && parts[1] == "runs" && parts[3] == "streams":
		names, err := f.store.StreamsList(ctx, worlds.StreamsListInput{RunID: worlds.RunID(parts[2])})
		if err != nil {
			writeGoError(writer, err)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(names)
	case len(parts) >= 3 && parts[0] == "v2" && parts[1] == "analytics":
		f.analytics(ctx, writer, request, parts[2:], query)
	default:
		writeError(writer, http.StatusNotFound, "", "no route "+request.URL.Path)
	}
}

// eventsVersion reports whether version serves the run-scoped event routes.
// /v5 serves the runs of every spec version, and is the only route that
// writes to a spec 9 run and enforces the in-band fence. /v4 serves the runs
// below spec 9 and ignores the fence.
func eventsVersion(version string) bool { return version == "v4" || version == "v5" }

func routeName(parts []string) string {
	var names []string
	for index, part := range parts {
		if index > 0 && (parts[index-1] == "runs" || parts[index-1] == "hooks" || parts[index-1] == "stream" || parts[index-1] == "streams") && part != "by-token" {
			names = append(names, ":id")
			continue
		}
		names = append(names, part)
	}
	return strings.Join(names, "/")
}

func pagination(query map[string][]string) worlds.Pagination {
	var result worlds.Pagination
	if values := query["limit"]; len(values) > 0 {
		result.Limit, _ = strconv.Atoi(values[0])
	}
	if values := query["cursor"]; len(values) > 0 {
		result.Cursor = values[0]
	}
	if values := query["sortOrder"]; len(values) > 0 {
		result.SortOrder = worlds.SortOrder(values[0])
	}
	return result
}

func nilIfEmpty(value string) any {
	if value == "" {
		return nil
	}
	return value
}

// outcome is the backend's answer to one event frame.
type outcome struct {
	status  int
	code    string
	message string
	answer  map[string]any
	// seq and seqInBand are the run's position counters, for an in-band
	// refusal.
	seq, seqInBand int
	// retryAfter is the Retry-After of a 425, a step started before its
	// retry is due.
	retryAfter time.Duration
	// details holds the fields a refusal carries at the top level of its
	// body, such as the owner of a start hook's token.
	details map[string]any
}

func (f *fakeBackend) createEvent(ctx context.Context, writer http.ResponseWriter, request *http.Request, runID worlds.RunID, version string) {
	body, _ := io.ReadAll(request.Body)
	in, err := newFrameReader(strings.NewReader(string(body))).next()
	if err != nil {
		writeError(writer, http.StatusBadRequest, "bad-frame", err.Error())
		return
	}
	fence, report, refusal := f.fence(ctx, runID, version, in)
	if refusal != nil {
		writeError(writer, refusal.status, refusal.code, refusal.message)
		return
	}
	result := f.apply(ctx, runID, in, fence, report)
	switch {
	case result.status == http.StatusPreconditionFailed:
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(result.status)
		_ = json.NewEncoder(writer).Encode(map[string]any{"error": result.code, "message": result.message, "seq": result.seq, "seqInBand": result.seqInBand})
	case result.details != nil:
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(result.status)
		body := map[string]any{"error": result.code, "message": result.message}
		maps.Copy(body, result.details)
		_ = json.NewEncoder(writer).Encode(body)
	case result.status != http.StatusOK:
		if result.retryAfter > 0 {
			writer.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(result.retryAfter.Seconds()))))
		}
		writeError(writer, result.status, result.code, result.message)
	default:
		writeCBOR(writer, result.answer)
	}
}

// fence returns the in-band count a write on route version is fenced with,
// and the slot it reports skipped events after. /v5 fences a frame with
// inBand, refuses a write to a spec 9 run without inBand and an in-band
// write without expectedSeqInBand or maxSlot, and reports the events after
// its maxSlot. /v4 refuses a write to a spec 9 run, and fences an in-band
// write to an earlier run without reporting skipped events.
func (f *fakeBackend) fence(ctx context.Context, runID worlds.RunID, version string, in frame) (*int, *int, *outcome) {
	spec, _ := asInt(in.Meta["specVersion"])
	if worlds.EventType(str(in.Meta, "eventType")) != worlds.EventTypeRunCreated {
		spec = f.specVersion(ctx, runID)
	}
	if version == "v4" {
		if spec >= 9 {
			return nil, nil, &outcome{status: http.StatusBadRequest, code: "spec-version", message: "a spec 9 run is written through /v5"}
		}
		if inBand, _ := in.Meta["inBand"].(bool); !inBand {
			return nil, nil, nil
		}
		count, ok := asInt(in.Meta["expectedSeqInBand"])
		if !ok {
			return nil, nil, &outcome{status: http.StatusBadRequest, code: "bad-request", message: "an in-band write needs expectedSeqInBand"}
		}
		return &count, nil, nil
	}
	// inBand is required on every write to a spec 9 run.
	inBand, declared := in.Meta["inBand"].(bool)
	if !declared && spec >= 9 {
		return nil, nil, &outcome{status: http.StatusBadRequest, code: "v5-missing-in-band", message: "Writes to a single-orchestrator (spec 9) run must declare meta.inBand."}
	}
	if !inBand {
		return nil, nil, nil
	}
	count, ok := asInt(in.Meta["expectedSeqInBand"])
	if !ok {
		return nil, nil, &outcome{status: http.StatusBadRequest, code: "bad-request", message: "an in-band write needs expectedSeqInBand"}
	}
	slot, ok := asInt(in.Meta["maxSlot"])
	if !ok {
		return nil, nil, &outcome{status: http.StatusBadRequest, code: "v5-missing-max-slot", message: "An in-band write must declare meta.maxSlot."}
	}
	return &count, &slot, nil
}

// specVersion returns the spec version run_created stamped on runID.
func (f *fakeBackend) specVersion(_ context.Context, runID worlds.RunID) int { return f.spec(runID) }

// spec returns the spec version run_created stamped on runID.
func (f *fakeBackend) spec(runID worlds.RunID) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.specs[runID]
}

// createBatch applies the frames of a batch in order, as one fenced
// allocation, and answers with an outcome per frame. Like the backend, it
// keeps the events it admitted when another is refused, and refuses an event
// behind a refused one of the same correlation ID.
func (f *fakeBackend) createBatch(ctx context.Context, writer http.ResponseWriter, request *http.Request, runID worlds.RunID, version string) {
	body, _ := io.ReadAll(request.Body)
	reader := newFrameReader(strings.NewReader(string(body)))
	var frames []frame
	for {
		next, err := reader.next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			writeError(writer, http.StatusBadRequest, "bad-frame", err.Error())
			return
		}
		frames = append(frames, next)
	}
	if len(frames) == 0 {
		writeError(writer, http.StatusBadRequest, "bad-request", "empty batch")
		return
	}
	if _, err := f.store.RunsGet(ctx, worlds.RunsGetInput{RunID: runID}); err != nil {
		writeGoError(writer, err)
		return
	}
	fence, report, refusal := f.fence(ctx, runID, version, frames[0])
	if refusal != nil {
		writeError(writer, refusal.status, refusal.code, refusal.message)
		return
	}
	var skipped []map[string]any
	if report != nil {
		// The events after the writer's slot that were in the log before
		// the block.
		page, _ := f.store.EventsList(ctx, worlds.EventsListInput{RunID: runID})
		for _, event := range page.Data[min(*report, len(page.Data)):] {
			skipped = append(skipped, f.wireEvent(event, true))
		}
	}
	results := make([]map[string]any, len(frames))
	failed := map[string]bool{}
	allocated := 0
	for index, next := range frames {
		correlation := str(next.Meta, "correlationId")
		if correlation != "" && failed[correlation] {
			results[index] = map[string]any{"status": http.StatusConflict, "error": "batch-earlier-item-failed", "message": "an earlier event of " + correlation + " failed"}
			continue
		}
		result := f.apply(ctx, runID, next, fence, nil)
		if result.status == http.StatusPreconditionFailed {
			// A fenced block is allocated in one step, so a stale count
			// refuses all of it.
			writer.Header().Set("Content-Type", "application/json")
			writer.WriteHeader(result.status)
			_ = json.NewEncoder(writer).Encode(map[string]any{"error": result.code, "message": result.message, "seq": result.seq, "seqInBand": result.seqInBand})
			return
		}
		if result.status != http.StatusOK {
			failed[correlation] = true
			results[index] = map[string]any{"status": result.status, "error": result.code, "message": result.message}
			continue
		}
		result.answer["status"] = http.StatusOK
		results[index] = result.answer
		if count, ok := result.answer["allocated"].(int); ok {
			allocated += count
			if fence != nil {
				next := *fence + count
				fence = &next
			}
		}
	}
	answer := map[string]any{"results": results, "allocated": allocated}
	if report != nil {
		answer["events"], answer["hasMore"] = skipped, false
	}
	writeCBOR(writer, answer)
}

// apply admits one event frame into the store, keeping only what the backend
// keeps of it.
func (f *fakeBackend) apply(ctx context.Context, runID worlds.RunID, in frame, fence, report *int) outcome {
	refuse := func(err error) outcome {
		var conflict *worlds.HookConflictError
		if errors.As(err, &conflict) {
			return outcome{status: http.StatusConflict, code: "start-hook-conflict", message: err.Error(), details: map[string]any{"token": conflict.Token, "conflictingRunId": string(conflict.RunID)}}
		}
		status, code := goErrorStatus(err)
		refused := outcome{status: status, code: code, message: err.Error()}
		// A forced claim's refusal names the hook that took the token.
		if claimed, ok := errors.AsType[*worlds.HookForceClaimedError](err); ok && claimed.RunID != "" {
			refused.details = map[string]any{"claimedBy": map[string]any{"runId": string(claimed.RunID), "hookId": string(claimed.HookID)}}
		}
		var early *worlds.TooEarlyError
		if errors.As(err, &early) {
			refused.retryAfter = early.RetryAfter
		}
		return refused
	}
	eventType := worlds.EventType(str(in.Meta, "eventType"))
	if id := str(in.Meta, "vercelId"); id != "" {
		f.mu.Lock()
		f.vercelIDs = append(f.vercelIDs, id)
		f.mu.Unlock()
	}
	spec, _ := asInt(in.Meta["specVersion"])
	if spec != 8 && spec != 9 {
		return outcome{status: http.StatusBadRequest, code: "spec-version", message: "unexpected spec version"}
	}
	// Keep only what the backend keeps, under the names it returns.
	stored := map[string]any{}
	for key, value := range in.Meta {
		if name, ok := storedKeys[key]; ok {
			stored[name] = value
		}
	}
	if field, ok := payloadFields[eventType]; ok && len(in.Body) > 0 {
		stored[field] = in.Body
	}
	wire := wireEvent{EventType: string(eventType), CorrelationID: str(in.Meta, "correlationId"), Data: stored}
	// An in-band write is stored at the time it occurred.
	occurred, _ := asTime(in.Meta["occurredAt"])
	data, err := eventData(wire)
	if err != nil {
		return outcome{status: http.StatusBadRequest, code: "invalid", message: err.Error()}
	}
	if eventType == worlds.EventTypeRunStarted && str(in.Meta, "workflowName") != "" {
		// A first delivery's run_started creates a run whose run_created never
		// landed.
		creation, _ := eventData(wireEvent{EventType: string(worlds.EventTypeRunCreated), Data: stored})
		if err := worlds.CreateRunForStart(ctx, f.store, runID, creation); err != nil {
			return refuse(err)
		}
		f.mu.Lock()
		if _, ok := f.specs[runID]; !ok {
			f.specs[runID] = spec
		}
		f.mu.Unlock()
		data = nil
	}
	if eventType == worlds.EventTypeRunCreated {
		var created worlds.DataRunCreated
		_ = json.Unmarshal(data, &created)
		if _, err := f.store.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: created.WorkflowName, Input: created.Input, Attributes: created.Attributes, DeploymentID: created.DeploymentID, EncryptionPublicKey: created.EncryptionPublicKey, ExecutionContext: created.ExecutionContext, StartHook: created.StartHook, AllowReservedAttributes: created.AllowReservedAttributes}); err != nil {
			return refuse(err)
		}
		f.mu.Lock()
		f.specs[runID] = spec
		f.mu.Unlock()
		run, _ := f.store.RunsGet(ctx, worlds.RunsGetInput{RunID: runID})
		events, _ := f.store.EventsList(ctx, worlds.EventsListInput{RunID: runID})
		return outcome{status: http.StatusOK, answer: map[string]any{"event": f.wireEvent(events.Data[0], true), "run": f.wireRun(run, nil), "allocated": 1}}
	}
	result, err := f.store.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: eventType, CorrelationID: wire.CorrelationID, Data: data, OccurredAt: occurred, SpecVersion: spec}, Options: worlds.EventsCreateOptions{InBandCount: fence, EventCount: report, ResumeID: str(in.Meta, "resumeId"), ResumePayloadDigest: str(in.Meta, "resumePayloadDigest")}})
	if err != nil {
		if _, ok := errors.AsType[*worlds.InBandSupersededError](err); ok {
			seq, seqInBand := f.counters(ctx, runID)
			return outcome{status: http.StatusPreconditionFailed, code: "in-band-superseded", message: err.Error(), seq: seq, seqInBand: seqInBand}
		}
		return refuse(err)
	}
	// The backend refuses a run_started of a running run as a conflict
	// (run-already-started), where the local World appends nothing.
	if eventType == worlds.EventTypeRunStarted && result.EventID == "" {
		return outcome{status: http.StatusConflict, code: "run-already-started", message: "run already started"}
	}
	event, err := f.store.EventsGet(ctx, worlds.EventsGetInput{RunID: runID, EventID: result.EventID})
	if err != nil {
		return refuse(err)
	}
	answer := map[string]any{"event": f.wireEvent(event, true), "allocated": result.AllocatedEventCount}
	if report != nil {
		// The skipped-slot report of an in-band write.
		skipped := make([]map[string]any, 0, len(result.Events))
		for _, event := range result.Events {
			skipped = append(skipped, f.wireEvent(event, true))
		}
		answer["events"], answer["hasMore"] = skipped, false
		if len(result.Events) > 0 {
			answer["cursor"] = "eid:" + string(result.Events[len(result.Events)-1].EventID)
		}
	}
	if wait := result.Wait; wait != nil {
		wire := map[string]any{"waitId": wait.WaitID, "runId": string(wait.RunID), "status": string(wait.Status), "createdAt": wait.CreatedAt, "updatedAt": wait.UpdatedAt, "specVersion": wait.SpecVersion}
		if wait.ResumeAt != nil {
			wire["resumeAt"] = *wait.ResumeAt
		}
		if wait.CompletedAt != nil {
			wire["completedAt"] = *wait.CompletedAt
		}
		answer["wait"] = wire
	}
	if result.Step != nil {
		answer["step"] = f.wireStep(ctx, *result.Step, nil)
		if result.StepCreated {
			answer["stepCreated"] = true
		}
	}
	if eventType == worlds.EventTypeHookCreated && event.EventType == worlds.EventTypeHookCreated {
		if hook, err := f.store.HooksGet(ctx, worlds.HooksGetInput{HookID: worlds.HookID(event.CorrelationID)}); err == nil {
			answer["hook"] = f.wireHook(ctx, hook)
		}
	}
	return outcome{status: http.StatusOK, answer: answer}
}

func (f *fakeBackend) listEvents(ctx context.Context, writer http.ResponseWriter, runID worlds.RunID, query map[string][]string, version string) {
	if cursor := pagination(query).Cursor; cursor != "" && !strings.HasPrefix(cursor, "eid:") {
		writeError(writer, http.StatusBadRequest, "validation-error", "cursor is not eid:<event ID>")
		return
	}
	// The counters are read before the listing, so the listing covers every
	// position they count.
	seq, seqInBand := f.counters(ctx, runID)
	page, err := f.store.EventsList(ctx, worlds.EventsListInput{RunID: runID})
	if err != nil {
		writeGoError(writer, err)
		return
	}
	events, cursor, hasMore := pageEvents(page.Data, query)
	var snapshot map[string]any
	if version == "v5" {
		snapshot = map[string]any{"seq": seq, "seqInBand": seqInBand}
		writer.Header().Set("X-Wf-Snapshot-Seq", strconv.Itoa(seq))
		writer.Header().Set("X-Wf-Snapshot-Seq-In-Band", strconv.Itoa(seqInBand))
	}
	f.writeEvents(writer, events, cursor, hasMore, snapshot)
}

// counters returns the run's position counter, the number of events in its
// log, and its in-band count.
func (f *fakeBackend) counters(ctx context.Context, runID worlds.RunID) (int, int) {
	run, _ := f.store.RunsGet(ctx, worlds.RunsGetInput{RunID: runID, ResolveData: worlds.ResolveDataNone})
	page, _ := f.store.EventsList(ctx, worlds.EventsListInput{RunID: runID, ResolveData: worlds.EventsResolveDataNone})
	return len(page.Data), run.InBandCount
}

// pageEvents applies a list's order, cursor, and limit to events. Cursors
// name the last event read, as eid:<event ID>.
func pageEvents(events []worlds.Event, query map[string][]string) ([]worlds.Event, string, bool) {
	paging := pagination(query)
	if paging.SortOrder == worlds.SortOrderDescending {
		reversed := make([]worlds.Event, len(events))
		for index, event := range events {
			reversed[len(events)-1-index] = event
		}
		events = reversed
	}
	if after, ok := strings.CutPrefix(paging.Cursor, "eid:"); ok {
		position := len(events)
		for index, event := range events {
			if string(event.EventID) == after {
				position = index + 1
				break
			}
		}
		events = events[position:]
	}
	hasMore := false
	if paging.Limit > 0 && len(events) > paging.Limit {
		events, hasMore = events[:paging.Limit], true
	}
	cursor := ""
	if len(events) > 0 {
		cursor = "eid:" + string(events[len(events)-1].EventID)
	}
	return events, cursor, hasMore
}

func (f *fakeBackend) writeEvents(writer http.ResponseWriter, events []worlds.Event, cursor string, hasMore bool, snapshot map[string]any) {
	writer.Header().Set("Content-Type", frameContentType)
	var out []byte
	for _, event := range events {
		out = append(out, f.eventFrame(event)...)
	}
	end := map[string]any{"_end": 1, "hasMore": hasMore}
	if cursor != "" {
		end["next"] = cursor
	}
	if snapshot != nil {
		end["snapshot"] = snapshot
	}
	out, _ = encodeFrame(out, end, nil)
	_, _ = writer.Write(out)
}

// eventFrame encodes an event as a list frame: the event without its
// payload as meta, and the payload as body.
func (f *fakeBackend) eventFrame(event worlds.Event) []byte {
	meta := f.wireEvent(event, false)
	var body []byte
	if field, ok := payloadFields[event.EventType]; ok {
		if data, ok := meta["eventData"].(map[string]any); ok {
			if tag, ok := data[field].(cbor.Tag); ok {
				body, _ = tag.Content.([]byte)
			}
			delete(data, field)
		}
	}
	out, err := encodeFrame(nil, meta, body)
	if err != nil {
		f.t.Errorf("encode frame: %v", err)
	}
	return out
}

// wireEvent returns an event as the backend stores it.
func (f *fakeBackend) wireEvent(event worlds.Event, _ bool) map[string]any {
	stored := map[string]any{}
	switch event.EventType {
	case worlds.EventTypeRunCreated:
		var created worlds.DataRunCreated
		_ = json.Unmarshal(event.Data, &created)
		stored["workflowName"], stored["deploymentId"], stored["attributes"] = created.WorkflowName, created.DeploymentID, created.Attributes
		if len(created.Input) > 0 {
			stored["input"] = uint8Array(created.Input)
		}
		if len(created.ExecutionContext) > 0 {
			stored["executionContext"] = created.ExecutionContext
		}
		if hook := created.StartHook; hook != nil {
			wire := map[string]any{"token": hook.Token, "hookId": string(hook.HookID)}
			if hook.TokenRetentionUntil != nil {
				wire["tokenRetentionUntil"] = *hook.TokenRetentionUntil
			}
			if len(hook.Metadata) > 0 {
				wire["metadata"] = uint8Array(hook.Metadata)
			}
			stored["startHook"] = wire
		}
	case worlds.EventTypeHookConflict:
		var created worlds.DataHookCreated
		_ = json.Unmarshal(event.Data, &created)
		stored["token"], stored["conflictingRunId"] = created.Token, string(created.ConflictingRunID)
	case worlds.EventTypeHookDisposed:
		var disposed worlds.HookDisposedEventData
		if len(event.Data) > 0 && json.Unmarshal(event.Data, &disposed) == nil {
			if disposed.Token != "" {
				stored["token"] = disposed.Token
			}
			if disposed.ForceClaimedBy != nil {
				stored["forceClaimedBy"] = map[string]any{"runId": string(disposed.ForceClaimedBy.RunID), "hookId": string(disposed.ForceClaimedBy.HookID)}
			}
		}
	default:
		meta, payload, err := encodeEvent(worlds.EventsCreateEvent{EventType: event.EventType, CorrelationID: event.CorrelationID, Data: event.Data})
		if err != nil {
			f.t.Errorf("store %s: %v", event.EventType, err)
		}
		for key, value := range meta {
			if name, ok := storedKeys[key]; ok {
				stored[name] = value
			}
		}
		if field, ok := payloadFields[event.EventType]; ok && len(payload) > 0 {
			stored[field] = uint8Array(payload)
		}
		if event.EventType == worlds.EventTypeHookCreated {
			var created worlds.DataHookCreated
			if json.Unmarshal(event.Data, &created) == nil && created.ForceClaimedFrom != nil {
				stored["forceClaimedFrom"] = wireClaim(created.ForceClaimedFrom)
			}
		}
	}
	wire := map[string]any{"runId": string(event.RunID), "eventId": string(event.EventID), "eventType": string(event.EventType), "createdAt": event.CreatedAt, "specVersion": event.SpecVersion, "eventData": stored}
	if event.ResumeID != "" {
		wire["resumeId"] = event.ResumeID
	}
	if event.CorrelationID != "" {
		wire["correlationId"] = event.CorrelationID
	}
	return wire
}

func lazy(query map[string][]string) bool {
	return query != nil && len(query["remoteRefBehavior"]) > 0 && query["remoteRefBehavior"][0] == "lazy"
}

func (f *fakeBackend) wireRun(run worlds.Run, query map[string][]string) map[string]any {
	wire := map[string]any{"runId": string(run.RunID), "workflowName": run.WorkflowName, "status": string(run.Status), "deploymentId": run.DeploymentID, "attributes": run.Attributes, "createdAt": run.CreatedAt, "updatedAt": run.UpdatedAt, "specVersion": f.spec(run.RunID)}
	if run.EncryptionPublicKey != "" {
		wire["encryptionPublicKey"] = run.EncryptionPublicKey
	}
	if run.StartedAt != nil {
		wire["startedAt"] = *run.StartedAt
	}
	if run.CompletedAt != nil {
		wire["completedAt"] = *run.CompletedAt
	}
	if run.ExpiredAt != nil {
		wire["expiredAt"] = *run.ExpiredAt
	}
	if len(run.ExecutionContext) > 0 {
		wire["executionContext"] = run.ExecutionContext
	}
	if run.StartHook != nil {
		hook := map[string]any{"token": run.StartHook.Token, "hookId": string(run.StartHook.HookID)}
		if run.StartHook.TokenRetentionUntil != nil {
			hook["tokenRetentionUntil"] = *run.StartHook.TokenRetentionUntil
		}
		wire["startHook"] = hook
	}
	if len(run.Error) > 0 {
		wire["error"] = uint8Array(run.Error)
	}
	if !lazy(query) {
		if len(run.Input) > 0 {
			wire["input"] = uint8Array(run.Input)
		}
		if len(run.Output) > 0 {
			wire["output"] = uint8Array(run.Output)
		}
	}
	return wire
}

func (f *fakeBackend) wireStep(ctx context.Context, step worlds.Step, query map[string][]string) map[string]any {
	wire := map[string]any{"runId": string(step.RunID), "stepId": string(step.StepID), "stepName": step.StepName, "status": string(step.Status), "attempt": step.Attempt, "createdAt": step.CreatedAt, "updatedAt": step.UpdatedAt, "specVersion": step.SpecVersion}
	for key, value := range map[string]*time.Time{"startedAt": step.StartedAt, "completedAt": step.CompletedAt, "retryAfter": step.RetryAfter} {
		if value != nil {
			wire[key] = *value
		}
	}
	if len(step.Error) > 0 {
		wire["error"] = uint8Array(step.Error)
	}
	if !lazy(query) {
		if len(step.Input) > 0 {
			wire["input"] = uint8Array(step.Input)
		}
		if len(step.Output) > 0 {
			wire["output"] = uint8Array(step.Output)
		}
	}
	return wire
}

// writeHook answers a hook read.
func (f *fakeBackend) writeHook(ctx context.Context, writer http.ResponseWriter, hook worlds.Hook, err error) {
	if err != nil {
		writeGoError(writer, err)
		return
	}
	writeCBOR(writer, f.wireHook(ctx, hook))
}

func (f *fakeBackend) wireHook(ctx context.Context, hook worlds.Hook) map[string]any {
	wire := map[string]any{"runId": string(hook.RunID), "hookId": string(hook.HookID), "token": hook.Token, "ownerId": "team", "projectId": "project", "environment": "production", "createdAt": hook.CreatedAt, "specVersion": hook.SpecVersion, "resumeCapabilities": map[string]any{"hookResumeDedupVersion": hookResumeDedupVersion}, "resumeContext": map[string]any{"deploymentId": hook.ResumeContext.DeploymentID, "workflowName": hook.ResumeContext.WorkflowName, "encryptionPublicKey": hook.ResumeContext.EncryptionPublicKey, "runSpecVersion": hook.ResumeContext.RunSpecVersion}}
	if hook.TokenRetentionUntil != nil {
		wire["tokenRetentionUntil"] = *hook.TokenRetentionUntil
	}
	if hook.ClaimedFrom != nil {
		wire["claimedFrom"] = wireClaim(hook.ClaimedFrom)
	}
	if len(hook.Metadata) > 0 {
		wire["metadata"] = uint8Array(hook.Metadata)
	}
	return wire
}

func (f *fakeBackend) writeStream(ctx context.Context, writer http.ResponseWriter, request *http.Request, runID worlds.RunID, name string) {
	body, _ := io.ReadAll(request.Body)
	var err error
	switch {
	case request.Header.Get("X-Stream-Done") == "true":
		err = f.store.StreamsClose(ctx, worlds.StreamsCloseInput{RunID: runID, Name: name})
	case request.Header.Get("X-Stream-Multi") == "true":
		var chunks [][]byte
		for len(body) >= 4 {
			size := binary.BigEndian.Uint32(body)
			chunks = append(chunks, body[4:4+size])
			body = body[4+size:]
		}
		err = f.store.StreamsWriteMulti(ctx, worlds.StreamsWriteMultiInput{RunID: runID, Name: name, Chunks: chunks})
	default:
		err = f.store.StreamsWrite(ctx, worlds.StreamsWriteInput{RunID: runID, Name: name, Chunk: body})
	}
	if err != nil {
		writeGoError(writer, err)
		return
	}
	writer.WriteHeader(http.StatusOK)
}

// uint8Array is a payload as the backend's CBOR encoder, cbor-x, writes a
// Uint8Array: the bytes under tag 64.
func uint8Array(data []byte) cbor.Tag { return cbor.Tag{Number: 64, Content: data} }

func writeCBOR(writer http.ResponseWriter, value any) {
	data, err := cborEncode.Marshal(value)
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "encode", err.Error())
		return
	}
	writer.Header().Set("Content-Type", cborContentType)
	_, _ = writer.Write(data)
}

func writeError(writer http.ResponseWriter, status int, code, message string) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(map[string]any{"error": code, "message": message})
}

// writeGoError answers with the status and code the backend uses for err.
func writeGoError(writer http.ResponseWriter, err error) {
	status, code := goErrorStatus(err)
	writeError(writer, status, code, err.Error())
}

func goErrorStatus(err error) (int, string) {
	switch {
	case errors.Is(err, worlds.ErrRunNotFound):
		return http.StatusNotFound, "run-not-found"
	case errors.Is(err, worlds.ErrStepNotFound), errors.Is(err, worlds.ErrEventNotFound):
		return http.StatusNotFound, "not-found"
	case is[*worlds.HookNotFoundError](err), errors.Is(err, worlds.ErrHookNotActive):
		return http.StatusNotFound, "hook-not-found"
	case is[*worlds.TooEarlyError](err):
		return http.StatusTooEarly, "too-early"
	case errors.Is(err, worlds.ErrAttributeInvalid):
		return http.StatusBadRequest, "validation-error"
	case errors.Is(err, worlds.ErrEntityConflict) && is[*worlds.RunExpiredError](err):
		// A transition from, or a creation on, a finished run.
		return http.StatusConflict, "run-terminal"
	case errors.Is(err, worlds.ErrEntityConflict):
		return http.StatusConflict, "entity-conflict"
	case errors.Is(err, worlds.ErrRunAlreadyExists):
		return http.StatusConflict, "run-exists"
	case is[*worlds.RunExpiredError](err):
		// The backend answers a write to a finished run 410, which the
		// World reports as a RunExpiredError.
		return http.StatusGone, "run-expired"
	case errors.Is(err, worlds.ErrStepNotStartable):
		return http.StatusConflict, "step-not-startable"
	case is[*worlds.HookForceClaimedError](err):
		return http.StatusConflict, "hook-force-claimed"
	case is[*worlds.HookConflictError](err):
		return http.StatusConflict, "hook-conflict"
	case errors.Is(err, worlds.ErrStreamClosed):
		return http.StatusConflict, "stream-closed"
	case errors.Is(err, worlds.ErrStartHookRejected):
		return http.StatusBadRequest, "start-hook-admission-rejected"
	case errors.Is(err, worlds.ErrEventInvalid):
		return http.StatusBadRequest, "validation-error"
	}
	return http.StatusInternalServerError, "internal"
}

// purge deletes run data as the backend's /v4/runs/purge does, naming the
// deleted and already_deleted outcomes purged and already_purged, and
// answering in reverse order to exercise reordering.
func (f *fakeBackend) purge(ctx context.Context, writer http.ResponseWriter, request *http.Request) {
	var in struct {
		RunIDs []worlds.RunID `cbor:"runIds"`
	}
	body, err := io.ReadAll(request.Body)
	if err == nil {
		err = cborDecode.Unmarshal(body, &in)
	}
	if err != nil {
		writeError(writer, http.StatusBadRequest, "invalid", err.Error())
		return
	}
	result, err := f.store.RunsDeleteDataMany(ctx, worlds.RunsDeleteDataManyInput{RunIDs: in.RunIDs})
	if err != nil {
		writeGoError(writer, err)
		return
	}
	results := make([]map[string]any, 0, len(result.Results))
	for index := len(result.Results) - 1; index >= 0; index-- {
		value := result.Results[index]
		wire := map[string]any{"runId": string(value.RunID), "outcome": string(value.Outcome)}
		switch value.Outcome {
		case worlds.DeleteDataDeleted:
			wire["outcome"] = "purged"
		case worlds.DeleteDataAlreadyDeleted:
			wire["outcome"] = "already_purged"
		case worlds.DeleteDataSkipped:
			wire["reason"], wire["status"] = value.Reason, string(value.Status)
		}
		results = append(results, wire)
	}
	summary := result.Summary
	writeCBOR(writer, map[string]any{
		"summary": map[string]any{"requested": summary.Requested, "purged": summary.Deleted, "alreadyPurged": summary.AlreadyDeleted, "skipped": summary.Skipped, "notCreated": summary.NotCreated, "notFound": summary.NotFound, "failed": summary.Failed},
		"results": results,
	})
}

// wireClaim returns a hook claim as the backend stores it.
func wireClaim(claim *worlds.HookForceClaim) map[string]any {
	wire := map[string]any{"runId": string(claim.RunID), "hookId": string(claim.HookID)}
	if claim.WorkflowName != "" {
		wire["workflowName"] = claim.WorkflowName
	}
	if claim.DeploymentID != "" {
		wire["deploymentId"] = claim.DeploymentID
	}
	if claim.RunSpecVersion != 0 {
		wire["runSpecVersion"] = claim.RunSpecVersion
	}
	return wire
}

// eventsSocket serves a run's events socket: it acknowledges a subscription,
// then pushes each event of the run above its afterSlot, and heartbeats
// naming the last slot pushed when f.heartbeat is set.
func (f *fakeBackend) eventsSocket(ctx context.Context, writer http.ResponseWriter, request *http.Request, runID worlds.RunID) {
	if f.noEventsSocket {
		writeError(writer, http.StatusNotFound, "not_found", "no events socket")
		return
	}
	conn, err := websocket.Accept(writer, request, nil)
	if err != nil {
		return
	}
	defer conn.CloseNow()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	_, raw, err := conn.Read(ctx)
	if err != nil {
		return
	}
	subscribe, err := newFrameReader(bytes.NewReader(raw)).next()
	if err != nil || str(subscribe.Meta, "type") != "subscribe" {
		return
	}
	reqID, _ := asInt(subscribe.Meta["reqId"])
	after, _ := asInt(subscribe.Meta["afterSlot"])
	ack := map[string]any{"reqId": reqID, "type": "subscribe_ack", "status": http.StatusOK}
	if f.heartbeat > 0 {
		ack["heartbeatMs"] = f.heartbeat.Milliseconds()
	}
	var mu sync.Mutex
	send := func(meta map[string]any, body []byte) {
		frame, _ := encodeFrame(nil, meta, body)
		mu.Lock()
		defer mu.Unlock()
		_ = conn.Write(ctx, websocket.MessageBinary, frame)
	}
	send(ack, nil)
	var last atomic.Int64
	last.Store(int64(after))
	unsubscribe := f.store.EventsSubscribe(ctx, runID, uint64(after), func(event worlds.Event) {
		slot, _ := worlds.EventSlot(event.EventID)
		last.Store(int64(slot))
		if slot == f.dropPush {
			return
		}
		send(map[string]any{"reqId": reqID, "type": "run_event", "slot": slot}, f.eventFrame(event))
	}, func(error) { cancel() })
	defer unsubscribe()
	if f.heartbeat > 0 {
		go func() {
			ticker := time.NewTicker(f.heartbeat)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					send(map[string]any{"reqId": reqID, "type": "run_heartbeat", "slot": last.Load()}, nil)
				}
			}
		}()
	}
	// The client sends nothing more: the read ends when it closes.
	_, _, _ = conn.Read(ctx)
}

// is reports whether err's chain has an error of type E.
func is[E error](err error) bool {
	_, ok := errors.AsType[E](err)
	return ok
}
