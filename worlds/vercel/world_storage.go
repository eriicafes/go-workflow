package vercelworld

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/klauspost/compress/zstd"

	"github.com/eriicafes/go-workflow/internal/envelope"
	"github.com/eriicafes/go-workflow/worlds"
)

// listResponse is a page of a run, step, or hook listing.
type listResponse struct {
	Data    []map[string]any `cbor:"data" json:"data"`
	Cursor  *string          `cbor:"cursor" json:"cursor"`
	HasMore bool             `cbor:"hasMore" json:"hasMore"`
}

// cursor returns the backend's cursor, passed on even on the last page, or
// "" when it sent none.
func (page listResponse) cursor() string {
	if page.Cursor == nil {
		return ""
	}
	return *page.Cursor
}

// entityQuery maps a Pagination and ResolveData to entity list parameters.
// An unset limit or order is left to the backend's defaults.
func entityQuery(pagination worlds.Pagination, resolve worlds.ResolveData) url.Values {
	query := url.Values{}
	if pagination.Limit > 0 {
		query.Set("limit", itoa(pagination.Limit))
	}
	if pagination.Cursor != "" {
		query.Set("cursor", pagination.Cursor)
	}
	if pagination.SortOrder != "" {
		query.Set("sortOrder", string(pagination.SortOrder))
	}
	query.Set("remoteRefBehavior", entityRefs(resolve))
	return query
}

// entityRefs is how the backend returns entity payloads.
func entityRefs(resolve worlds.ResolveData) string {
	if resolve == worlds.ResolveDataNone {
		return "lazy"
	}
	return "resolve"
}

// tagUint8Array is the CBOR tag of a typed array of uint8 (RFC 8746), under
// which the backend's encoder writes a Uint8Array payload.
const tagUint8Array = 64

// payloadBytes returns the bytes of a payload the backend sent, plain or
// tagged as a Uint8Array, or nil when it sent none. Any other value is an
// error, so a payload is never read as absent.
func payloadBytes(value any) ([]byte, error) {
	switch value := value.(type) {
	case nil:
		return nil, nil
	case []byte:
		if len(value) == 0 {
			return nil, nil
		}
		return value, nil
	case cbor.Tag:
		if data, ok := value.Content.([]byte); ok && value.Number == tagUint8Array {
			if len(data) == 0 {
				return nil, nil
			}
			return data, nil
		}
		return nil, fmt.Errorf("workflow/vercel: payload is CBOR tag %d, not bytes", value.Number)
	}
	return nil, fmt.Errorf("workflow/vercel: payload is %T, not bytes", value)
}

// entityPayload is the payload of a run, step, or hook view, without the
// compression it was stored under: a payload prefixed gzip or zstd is
// returned as the payload inside it. An encrypted payload, whose compression
// is inside the encryption, and one that does not decompress, are returned
// as stored.
func entityPayload(values map[string]any, field string) ([]byte, error) {
	data, err := payloadBytes(values[field])
	if err != nil {
		return nil, fmt.Errorf("%w: %s", err, field)
	}
	return decompressPayload(data), nil
}

// decompressPayload returns data without the compression it was stored
// under.
func decompressPayload(data []byte) []byte {
	prefix, body, err := envelope.Decode(data)
	if err != nil {
		return data
	}
	var inner []byte
	switch prefix {
	case envelope.PrefixGzip:
		reader, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			return data
		}
		if inner, err = io.ReadAll(io.LimitReader(reader, maxResponseBytes)); err != nil {
			return data
		}
	case envelope.PrefixZstd:
		decoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(maxResponseBytes))
		if err != nil {
			return data
		}
		defer decoder.Close()
		if inner, err = decoder.DecodeAll(body, nil); err != nil {
			return data
		}
	default:
		return data
	}
	return inner
}

func timeField(values map[string]any, key string) *time.Time {
	value, ok := asTime(values[key])
	if !ok || value.IsZero() {
		return nil
	}
	return &value
}

func requiredTime(values map[string]any, key string) time.Time {
	value, _ := asTime(values[key])
	return value
}

// toRun returns the go-workflow view of a backend run.
func toRun(values map[string]any, resolve worlds.ResolveData) (worlds.Run, error) {
	runError, err := entityPayload(values, "error")
	if err != nil {
		return worlds.Run{}, err
	}
	run := worlds.Run{
		RunID:               worlds.RunID(str(values, "runId")),
		WorkflowName:        str(values, "workflowName"),
		Status:              worlds.Status(str(values, "status")),
		Error:               runError,
		ErrorCode:           str(values, "errorCode"),
		Attributes:          stringMap(values["attributes"]),
		DeploymentID:        str(values, "deploymentId"),
		EncryptionPublicKey: str(values, "encryptionPublicKey"),
		CreatedAt:           requiredTime(values, "createdAt"),
		UpdatedAt:           requiredTime(values, "updatedAt"),
		StartedAt:           timeField(values, "startedAt"),
		CompletedAt:         timeField(values, "completedAt"),
		ExpiredAt:           timeField(values, "expiredAt"),
	}
	run.ExecutionContext, _ = values["executionContext"].(map[string]any)
	run.SpecVersion, _ = asInt(values["specVersion"])
	if hook, ok := values["startHook"].(map[string]any); ok {
		run.StartHook = &worlds.StartHook{Token: str(hook, "token"), HookID: worlds.HookID(str(hook, "hookId")), TokenRetentionUntil: timeField(hook, "tokenRetentionUntil")}
	}
	if resolve != worlds.ResolveDataNone {
		if run.Input, err = entityPayload(values, "input"); err != nil {
			return worlds.Run{}, err
		}
		if run.Output, err = entityPayload(values, "output"); err != nil {
			return worlds.Run{}, err
		}
	}
	return run, nil
}

// toStep returns the go-workflow view of a backend step. Its input is the
// step_created Data the World stores as the step's payload.
func toStep(values map[string]any, resolve worlds.ResolveData) (worlds.Step, error) {
	attempt, _ := asInt(values["attempt"])
	stepError, err := entityPayload(values, "error")
	if err != nil {
		return worlds.Step{}, err
	}
	step := worlds.Step{
		RunID:       worlds.RunID(str(values, "runId")),
		StepID:      worlds.StepID(str(values, "stepId")),
		StepName:    str(values, "stepName"),
		Status:      worlds.Status(str(values, "status")),
		Error:       stepError,
		Attempt:     attempt,
		CreatedAt:   requiredTime(values, "createdAt"),
		UpdatedAt:   requiredTime(values, "updatedAt"),
		StartedAt:   timeField(values, "startedAt"),
		CompletedAt: timeField(values, "completedAt"),
		RetryAfter:  timeField(values, "retryAfter"),
	}
	step.SpecVersion, _ = asInt(values["specVersion"])
	if resolve != worlds.ResolveDataNone {
		if step.Input, err = entityPayload(values, "input"); err != nil {
			return worlds.Step{}, err
		}
		if step.Output, err = entityPayload(values, "output"); err != nil {
			return worlds.Step{}, err
		}
	}
	return step, nil
}

// toWait returns the go-workflow view of a backend wait.
func toWait(values map[string]any) *worlds.Wait {
	wait := &worlds.Wait{
		WaitID:      str(values, "waitId"),
		RunID:       worlds.RunID(str(values, "runId")),
		Status:      worlds.WaitStatus(str(values, "status")),
		ResumeAt:    timeField(values, "resumeAt"),
		CompletedAt: timeField(values, "completedAt"),
		CreatedAt:   requiredTime(values, "createdAt"),
		UpdatedAt:   requiredTime(values, "updatedAt"),
	}
	wait.SpecVersion, _ = asInt(values["specVersion"])
	return wait
}

// hookResumeDedupVersion is the version of the backend's resume
// deduplication this World sends resume IDs for.
const hookResumeDedupVersion = 1

// toHook returns the go-workflow view of a backend hook.
func toHook(values map[string]any) (worlds.Hook, error) {
	hook := worlds.Hook{
		RunID:               worlds.RunID(str(values, "runId")),
		HookID:              worlds.HookID(str(values, "hookId")),
		Token:               str(values, "token"),
		TokenRetentionUntil: timeField(values, "tokenRetentionUntil"),
		ClaimedFrom:         hookClaim(values["claimedFrom"]),
		CreatedAt:           requiredTime(values, "createdAt"),
	}
	if resume, ok := values["resumeContext"].(map[string]any); ok {
		hook.ResumeContext = worlds.HookResumeContext{DeploymentID: str(resume, "deploymentId"), WorkflowName: str(resume, "workflowName"), EncryptionPublicKey: str(resume, "encryptionPublicKey"), WorkflowCoreVersion: str(resume, "workflowCoreVersion"), NodeVersion: str(resume, "nodeVersion")}
		hook.ResumeContext.RunSpecVersion, _ = asInt(resume["runSpecVersion"])
	}
	hook.SpecVersion, _ = asInt(values["specVersion"])
	if capabilities, ok := values["resumeCapabilities"].(map[string]any); ok {
		version, _ := asInt(capabilities["hookResumeDedupVersion"])
		hook.ResumeDedup = version >= hookResumeDedupVersion
	}
	metadata, err := entityPayload(values, "metadata")
	if err != nil {
		return worlds.Hook{}, err
	}
	hook.Metadata = metadata
	hook.IsWebhook, _ = values["isWebhook"].(bool)
	hook.IsSystem, _ = values["isSystem"].(bool)
	return hook, nil
}

func (w *World) RunsGet(ctx context.Context, input worlds.RunsGetInput) (worlds.Run, error) {
	run, err := w.getRun(ctx, input.RunID, input.ResolveData, "", 0)
	if err != nil {
		return worlds.Run{}, err
	}
	// The run view has no in-band count, so read it from the snapshot of a
	// one-event listing.
	loaded, err := w.listEvents(ctx, input.RunID, 0, 1, worlds.EventsResolveDataNone)
	if err != nil {
		return worlds.Run{}, err
	}
	if loaded.SeqInBand != nil {
		run.InBandCount = *loaded.SeqInBand
	}
	return run, nil
}

// getRun reads a run, from the long-poll status route when wait is set.
func (w *World) getRun(ctx context.Context, runID worlds.RunID, resolve worlds.ResolveData, suffix string, wait time.Duration) (worlds.Run, error) {
	query := url.Values{"remoteRefBehavior": {entityRefs(resolve)}}
	if wait > 0 {
		query.Set("waitMs", itoa(int(wait/time.Millisecond)))
	}
	var values map[string]any
	if err := w.api.do(ctx, http.MethodGet, "/v2/runs/"+pathEscape(string(runID))+suffix, query, nil, &values); err != nil {
		return worlds.Run{}, notFound(err, worlds.ErrRunNotFound)
	}
	return toRun(values, resolve)
}

func (w *World) RunsList(ctx context.Context, input worlds.RunsListInput) (worlds.Page[worlds.Run], error) {
	// An empty status filter matches no runs. A nil one matches every
	// status.
	if input.Status != nil && len(input.Status) == 0 {
		return worlds.Page[worlds.Run]{Data: []worlds.Run{}}, nil
	}
	if len(input.Status) > 1 {
		// The backend filters by one status, so more are refused.
		return worlds.Page[worlds.Run]{}, fmt.Errorf("%w: the Vercel backend filters runs by one status", worlds.ErrEventInvalid)
	}
	var status worlds.Status
	if len(input.Status) == 1 {
		status = input.Status[0]
	}
	page, err := w.runsPage(ctx, input, status, input.Pagination)
	if err != nil {
		return worlds.Page[worlds.Run]{}, err
	}
	result := worlds.Page[worlds.Run]{Cursor: page.cursor(), HasMore: page.HasMore}
	for _, values := range page.Data {
		run, err := toRun(values, input.ResolveData)
		if err != nil {
			return worlds.Page[worlds.Run]{}, err
		}
		result.Data = append(result.Data, run)
	}
	return result, nil
}

func (w *World) runsPage(ctx context.Context, input worlds.RunsListInput, status worlds.Status, pagination worlds.Pagination) (listResponse, error) {
	query := entityQuery(pagination, input.ResolveData)
	if input.WorkflowName != "" {
		query.Set("workflowName", input.WorkflowName)
	}
	if status != "" {
		query.Set("status", string(status))
	}
	var page listResponse
	err := w.api.do(ctx, http.MethodGet, "/v2/runs", query, nil, &page)
	return page, err
}

func compareStrings(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// runWaitHeadroom is kept between the wait the backend is asked to hold a
// status request for and the request's own deadline, so the hold ends as an
// answer rather than as a timeout.
var runWaitHeadroom = 10 * time.Second

// longPollUnusable reports whether err says the status route cannot be held
// open, rather than that the backend is unwell: the route is missing, an
// intermediary gave up on the hold, or the request timed out or lost its
// connection.
func longPollUnusable(err error) bool {
	if errors.Is(err, worlds.ErrRunNotFound) {
		return true
	}
	switch statusOf(err) {
	case http.StatusMethodNotAllowed, http.StatusNotImplemented, http.StatusGatewayTimeout:
		return true
	case 0:
		_, failed := errors.AsType[*transientError](err)
		return failed
	}
	return false
}

func (w *World) RunsWaitForTerminalStatus(ctx context.Context, input worlds.RunsWaitForTerminalStatusInput) (worlds.Run, error) {
	deadline := time.Now().Add(input.Timeout)
	delay := 50 * time.Millisecond
	// The backend holds a status request until the run finishes or the wait
	// it was asked for passes. With a request deadline too short to hold one,
	// plain reads are polled instead.
	slice := requestTimeout() - runWaitHeadroom
	longPoll := slice > 0
	for {
		remaining := time.Until(deadline)
		var run worlds.Run
		var err error
		if longPoll && remaining > 0 {
			run, err = w.getRun(ctx, input.RunID, input.ResolveData, "/status", min(remaining, slice))
			if err != nil && ctx.Err() == nil && longPollUnusable(err) {
				// Poll plain reads instead, which also tell a missing run
				// apart.
				longPoll = false
				run, err = w.getRun(ctx, input.RunID, input.ResolveData, "", 0)
			}
		} else {
			run, err = w.getRun(ctx, input.RunID, input.ResolveData, "", 0)
		}
		if err != nil || run.Status.Terminal() || time.Until(deadline) <= 0 {
			return run, err
		}
		if longPoll {
			continue
		}
		timer := time.NewTimer(min(delay, time.Until(deadline)))
		select {
		case <-ctx.Done():
			timer.Stop()
			return worlds.Run{}, ctx.Err()
		case <-timer.C:
		}
		delay = min(2*delay, time.Second)
	}
}

func (w *World) RunsSetAttributes(ctx context.Context, input worlds.RunsSetAttributesInput) error {
	if len(input.Changes) == 0 {
		_, err := w.getRun(ctx, input.RunID, worlds.ResolveDataNone, "", 0)
		return err
	}
	data, err := worlds.EncodeAttrSet(input)
	if err != nil {
		return err
	}
	_, err = w.EventsCreate(ctx, worlds.EventsCreateInput{RunID: input.RunID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeAttrSet, Data: data}})
	return err
}

func (w *World) StepsGet(ctx context.Context, input worlds.StepsGetInput) (worlds.Step, error) {
	query := url.Values{"remoteRefBehavior": {entityRefs(input.ResolveData)}}
	var values map[string]any
	if err := w.api.do(ctx, http.MethodGet, "/v2/runs/"+pathEscape(string(input.RunID))+"/steps/"+pathEscape(string(input.StepID)), query, nil, &values); err != nil {
		return worlds.Step{}, notFound(err, worlds.ErrStepNotFound)
	}
	return toStep(values, input.ResolveData)
}

func (w *World) StepsList(ctx context.Context, input worlds.StepsListInput) (worlds.Page[worlds.Step], error) {
	var page listResponse
	// A 404 is returned as it is.
	if err := w.api.do(ctx, http.MethodGet, "/v2/runs/"+pathEscape(string(input.RunID))+"/steps", entityQuery(input.Pagination, input.ResolveData), nil, &page); err != nil {
		return worlds.Page[worlds.Step]{}, err
	}
	result := worlds.Page[worlds.Step]{Cursor: page.cursor(), HasMore: page.HasMore}
	for _, values := range page.Data {
		step, err := toStep(values, input.ResolveData)
		if err != nil {
			return worlds.Page[worlds.Step]{}, err
		}
		result.Data = append(result.Data, step)
	}
	return result, nil
}

func (w *World) HooksGet(ctx context.Context, input worlds.HooksGetInput) (worlds.Hook, error) {
	var values map[string]any
	if err := w.api.do(ctx, http.MethodGet, "/v2/hooks/"+pathEscape(string(input.HookID)), nil, nil, &values); err != nil {
		return worlds.Hook{}, notFound(err, &worlds.HookNotFoundError{Token: string(input.HookID)})
	}
	return w.hookView(ctx, values, input.ResolveData)
}

func (w *World) HooksGetByToken(ctx context.Context, input worlds.HooksGetByTokenInput) (worlds.Hook, error) {
	var values map[string]any
	if err := w.api.do(ctx, http.MethodGet, "/v2/hooks/by-token", url.Values{"token": {input.Token}}, nil, &values); err != nil {
		return worlds.Hook{}, notFound(err, &worlds.HookNotFoundError{Token: input.Token})
	}
	return w.hookView(ctx, values, input.ResolveData)
}

func (w *World) HooksList(ctx context.Context, input worlds.HooksListInput) (worlds.Page[worlds.Hook], error) {
	query := entityQuery(input.Pagination, input.ResolveData)
	if input.RunID != "" {
		query.Set("runId", string(input.RunID))
	}
	var page listResponse
	if err := w.api.do(ctx, http.MethodGet, "/v2/hooks", query, nil, &page); err != nil {
		return worlds.Page[worlds.Hook]{}, err
	}
	result := worlds.Page[worlds.Hook]{Cursor: page.cursor(), HasMore: page.HasMore}
	for _, values := range page.Data {
		hook, err := w.hookView(ctx, values, input.ResolveData)
		if err != nil {
			return worlds.Page[worlds.Hook]{}, err
		}
		result.Data = append(result.Data, hook)
	}
	return result, nil
}

// hookView completes a backend hook with what its events record: when it
// received a payload and whether it is a webhook. The backend deletes a
// disposed hook.
func (w *World) hookView(ctx context.Context, values map[string]any, resolve worlds.ResolveData) (worlds.Hook, error) {
	hook, err := toHook(values)
	if err != nil {
		return worlds.Hook{}, err
	}
	// The hook's first event is its hook_created.
	events, err := w.EventsListByCorrelationID(ctx, worlds.EventsListByCorrelationIDInput{RunID: hook.RunID, CorrelationID: string(hook.HookID), Pagination: worlds.Pagination{Limit: 1, SortOrder: worlds.SortOrderAscending}})
	if err != nil {
		return worlds.Hook{}, err
	}
	for _, event := range events.Data {
		switch event.EventType {
		case worlds.EventTypeHookCreated:
			var created worlds.DataHookCreated
			if json.Unmarshal(event.Data, &created) == nil {
				hook.IsWebhook = hook.IsWebhook || created.IsWebhook
				hook.IsSystem = hook.IsSystem || created.IsSystem
				if hook.Metadata == nil {
					hook.Metadata = created.Metadata
				}
				if hook.ClaimedFrom == nil {
					hook.ClaimedFrom = created.ForceClaimedFrom
				}
			}
		}
	}
	if resolve == worlds.ResolveDataNone {
		hook.Metadata = nil
	}
	return hook, nil
}

// purgeResult is the backend's run data deletion result, which names the
// deleted and already_deleted outcomes purged and already_purged.
type purgeResult struct {
	Summary struct {
		Requested     int `cbor:"requested" json:"requested"`
		Purged        int `cbor:"purged" json:"purged"`
		AlreadyPurged int `cbor:"alreadyPurged" json:"alreadyPurged"`
		Skipped       int `cbor:"skipped" json:"skipped"`
		NotCreated    int `cbor:"notCreated" json:"notCreated"`
		NotFound      int `cbor:"notFound" json:"notFound"`
		Failed        int `cbor:"failed" json:"failed"`
	} `cbor:"summary" json:"summary"`
	Results []struct {
		RunID     string `cbor:"runId" json:"runId"`
		Outcome   string `cbor:"outcome" json:"outcome"`
		Reason    string `cbor:"reason,omitempty" json:"reason,omitempty"`
		Status    string `cbor:"status,omitempty" json:"status,omitempty"`
		Code      string `cbor:"code,omitempty" json:"code,omitempty"`
		Retryable bool   `cbor:"retryable,omitempty" json:"retryable,omitempty"`
	} `cbor:"results" json:"results"`
}

// RunsDeleteDataMany deletes the data of finished runs through the backend,
// which keeps their metadata.
func (w *World) RunsDeleteDataMany(ctx context.Context, input worlds.RunsDeleteDataManyInput) (worlds.RunsDeleteDataManyResult, error) {
	if err := worlds.CheckDeleteDataInput(input); err != nil {
		return worlds.RunsDeleteDataManyResult{}, err
	}
	ids := make([]string, len(input.RunIDs))
	for index, id := range input.RunIDs {
		ids[index] = string(id)
	}
	var response purgeResult
	if err := w.api.do(ctx, http.MethodPost, "/v4/runs/purge", nil, map[string]any{"runIds": ids}, &response); err != nil {
		return worlds.RunsDeleteDataManyResult{}, err
	}
	byID := make(map[string]worlds.DeleteDataResult, len(response.Results))
	for _, wire := range response.Results {
		outcome := worlds.DeleteDataOutcome(wire.Outcome)
		switch wire.Outcome {
		case "purged":
			outcome = worlds.DeleteDataDeleted
		case "already_purged":
			outcome = worlds.DeleteDataAlreadyDeleted
		}
		byID[wire.RunID] = worlds.DeleteDataResult{RunID: worlds.RunID(wire.RunID), Outcome: outcome, Reason: wire.Reason, Status: worlds.Status(wire.Status), Code: wire.Code, Retryable: wire.Retryable}
	}
	// Exactly one result per requested run, reordered to the request.
	results := make([]worlds.DeleteDataResult, len(input.RunIDs))
	for index, id := range input.RunIDs {
		result, ok := byID[string(id)]
		if !ok || len(byID) != len(input.RunIDs) {
			return worlds.RunsDeleteDataManyResult{}, fmt.Errorf("workflow/vercel: delete data returned %d results for %d runs", len(response.Results), len(input.RunIDs))
		}
		results[index] = result
	}
	return worlds.NewDeleteDataResult(results), nil
}
