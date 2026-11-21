package vercelworld

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// createResponse is the materialized answer to an event write.
type createResponse struct {
	Event       map[string]any `cbor:"event"`
	Run         map[string]any `cbor:"run"`
	Step        map[string]any `cbor:"step"`
	Hook        map[string]any `cbor:"hook"`
	Wait        map[string]any `cbor:"wait"`
	StepCreated bool           `cbor:"stepCreated"`
	Allocated   *int           `cbor:"allocated"`
	// Events and HasMore are the skipped-slot report of an in-band write.
	Events  []map[string]any `cbor:"events"`
	HasMore bool             `cbor:"hasMore"`
}

// repeatable lists the event types whose repeat returns the event that
// admitted the transition instead of failing. The backend refuses a repeat as
// a conflict, so the World finds that event.
var repeatable = map[worlds.EventType]bool{
	worlds.EventTypeRunStarted:    true,
	worlds.EventTypeRunCancelled:  true,
	worlds.EventTypeStepCreated:   true,
	worlds.EventTypeHookCreated:   true,
	worlds.EventTypeHookDisposed:  true,
	worlds.EventTypeWaitCreated:   true,
	worlds.EventTypeWaitCompleted: true,
}

// runCreationMeta returns the frame meta of a run's creation, which
// run_created carries, and a first flow's run_started too.
func (w *World) runCreationMeta(input worlds.EventsCreateRunInput, eventType worlds.EventType) map[string]any {
	meta := map[string]any{
		"eventType":         string(eventType),
		"specVersion":       specVersion,
		"workflowName":      input.WorkflowName,
		"deploymentId":      input.DeploymentID,
		"occurredAt":        time.Now().UTC(),
		"remoteRefBehavior": "lazy",
	}
	if input.EncryptionPublicKey != "" {
		meta["encryptionPublicKey"] = input.EncryptionPublicKey
	}
	if len(input.ExecutionContext) > 0 {
		meta["executionContext"] = input.ExecutionContext
	}
	if start := input.StartHook; start != nil {
		hook := map[string]any{"token": start.Token}
		if start.TokenRetentionUntil != nil {
			hook["tokenRetentionUntil"] = start.TokenRetentionUntil.UTC()
		}
		if len(start.Metadata) > 0 {
			hook["metadata"] = start.Metadata
		}
		meta["startHook"] = hook
	}
	attributes := input.Attributes
	if retention := w.config.retentionFor(input.WorkflowName); retention != worlds.RetainDefault {
		if _, set := attributes[worlds.RetentionAttribute]; !set {
			attributes = make(map[string]string, len(input.Attributes)+1)
			maps.Copy(attributes, input.Attributes)
			attributes[worlds.RetentionAttribute] = retention.String()
		}
	}
	if len(attributes) > 0 {
		meta["attributes"] = attributes
		for key := range attributes {
			if len(key) > 0 && key[0] == '$' {
				meta["allowReservedAttributes"] = true
			}
		}
	}
	return meta
}

func (w *World) EventsCreateRun(ctx context.Context, input worlds.EventsCreateRunInput) (worlds.EventResult, error) {
	meta := w.runCreationMeta(input, worlds.EventTypeRunCreated)
	response, err := w.postEvent(ctx, input.RunID, worlds.EventTypeRunCreated, meta, input.Input)
	if err != nil {
		return worlds.EventResult{}, classify(err, worlds.EventTypeRunCreated)
	}
	event := wireEventFromMap(response.Event)
	// run_created takes the first position, which counts as in-band.
	return worlds.EventResult{EventID: worlds.EventID(event.EventID), EventType: worlds.EventTypeRunCreated, CreatedAt: event.CreatedAt, AllocatedEventCount: 1, InBandCount: 1}, nil
}

func (w *World) EventsCreate(ctx context.Context, input worlds.EventsCreateInput) (worlds.EventResult, error) {
	if err := ctx.Err(); err != nil {
		return worlds.EventResult{}, err
	}
	if !input.Event.EventType.Valid() || input.Event.EventType == worlds.EventTypeRunCreated || input.Event.EventType == worlds.EventTypeHookConflict {
		return worlds.EventResult{}, fmt.Errorf("%w: %s cannot be appended", worlds.ErrEventInvalid, input.Event.EventType)
	}
	meta, payload, err := encodeEvent(input.Event)
	if err != nil {
		return worlds.EventResult{}, err
	}
	if input.Event.EventType == worlds.EventTypeRunStarted {
		// A first flow's run_started carries the run's creation, which the
		// backend creates the run from when run_created never landed.
		var created worlds.DataRunCreated
		if json.Unmarshal(input.Event.Data, &created) == nil && created.WorkflowName != "" {
			meta = w.runCreationMeta(worlds.EventsCreateRunInput{RunID: input.RunID, WorkflowName: created.WorkflowName, Input: created.Input, Attributes: created.Attributes, DeploymentID: created.DeploymentID, EncryptionPublicKey: created.EncryptionPublicKey, ExecutionContext: created.ExecutionContext, StartHook: created.StartHook}, worlds.EventTypeRunStarted)
			meta["specVersion"], meta["executorSpecVersion"] = worlds.EventSpecVersion(input.Event.SpecVersion), specVersion
			payload = created.Input
		}
	}
	setRequestID(ctx, meta)
	if input.Options.ResumeID != "" && input.Event.EventType == worlds.EventTypeHookReceived {
		meta["resumeId"], meta["resumePayloadDigest"] = input.Options.ResumeID, input.Options.ResumePayloadDigest
	}
	meta["occurredAt"] = occurredAt(input.Event)
	meta["remoteRefBehavior"] = "resolve"
	if input.Event.EventType == worlds.EventTypeRunStarted {
		meta["skipPreload"] = true
	}
	held, hasHeld := heldCount(input.Options)
	if hasHeld {
		meta["maxSlot"] = held
	}
	if input.Options.InBandCount != nil {
		meta["inBand"] = true
		meta["expectedSeqInBand"] = *input.Options.InBandCount
	}
	response, err := w.postEvent(ctx, input.RunID, input.Event.EventType, meta, payload)
	if err != nil {
		err = classify(err, input.Event.EventType)
		var conflict *worlds.HookConflictError
		if repeatable[input.Event.EventType] && !errors.Is(err, worlds.ErrInBandSuperseded) && !errors.Is(err, worlds.ErrRunNotFound) && !errors.As(err, &conflict) && !errors.Is(err, worlds.ErrStartHookRejected) {
			if result, found, findErr := w.findAdmitted(ctx, input, held, hasHeld); findErr != nil || found {
				return result, findErr
			}
		}
		return worlds.EventResult{}, err
	}
	wire := wireEventFromMap(response.Event)
	event, err := toEvent(input.RunID, wire, worlds.EventsResolveDataAll)
	if err != nil {
		return worlds.EventResult{}, err
	}
	allocated := 1
	if response.Allocated != nil {
		allocated = *response.Allocated
	}
	result := worlds.EventResult{EventID: event.EventID, EventType: event.EventType, CreatedAt: event.CreatedAt, Data: event.Data, AllocatedEventCount: allocated}
	if input.Options.InBandCount != nil {
		result.InBandCount = *input.Options.InBandCount + allocated
	}
	if response.Step != nil {
		step, err := toStep(response.Step, worlds.ResolveDataAll)
		if err != nil {
			return worlds.EventResult{}, err
		}
		result.Step = &step
		result.StepCreated = input.Event.EventType == worlds.EventTypeStepCreated && allocated > 0
	}
	if response.Wait != nil {
		result.Wait = toWait(response.Wait)
	}
	if response.Hook != nil && event.EventType == worlds.EventTypeHookCreated {
		hook, err := toHook(response.Hook)
		if err == nil {
			result.Hook = &hook
		}
	}
	if input.Options.InBandCount != nil && hasHeld && response.Events != nil {
		// The skipped-slot report of an in-band write.
		result.Events, result.MoreEvents = make([]worlds.Event, 0, len(response.Events)), response.HasMore
		for _, wire := range response.Events {
			event, err := toEvent(input.RunID, wireEventFromMap(wire), input.Options.ResolveData)
			if err != nil {
				return worlds.EventResult{}, err
			}
			result.Events = append(result.Events, event)
		}
		return result, nil
	}
	if err := w.catchUp(ctx, input, &result, held, hasHeld); err != nil {
		return worlds.EventResult{}, err
	}
	return result, nil
}

// occurredAt is when event occurred: its OccurredAt, or now.
func occurredAt(event worlds.EventsCreateEvent) time.Time {
	if !event.OccurredAt.IsZero() {
		return event.OccurredAt.UTC()
	}
	return time.Now().UTC()
}

// heldCount returns the number of events the writer holds, from
// Options.EventCount or the slot of Options.SinceCursor.
func heldCount(options worlds.EventsCreateOptions) (int, bool) {
	if options.EventCount != nil {
		return *options.EventCount, true
	}
	if options.SinceCursor != "" {
		if slot, err := worlds.EventSlot(worlds.EventID(options.SinceCursor)); err == nil {
			return int(slot), true
		}
	}
	return 0, false
}

// catchUp sets result.Events to the events after the ones the writer holds
// that were in the log before the appended event, which the backend reports
// only for an in-band write. For run_started it also reads the run's in-band
// count, which the snapshot of that listing reports.
func (w *World) catchUp(ctx context.Context, input worlds.EventsCreateInput, result *worlds.EventResult, held int, hasHeld bool) error {
	if !hasHeld && input.Event.EventType != worlds.EventTypeRunStarted {
		return nil
	}
	appended, _ := worlds.EventSlot(result.EventID)
	page, err := w.listEvents(ctx, input.RunID, held, 0, input.Options.ResolveData)
	if err != nil {
		return err
	}
	if input.Event.EventType == worlds.EventTypeRunStarted && page.SeqInBand != nil {
		result.InBandCount = *page.SeqInBand
	}
	if !hasHeld {
		return nil
	}
	for _, event := range page.Events {
		// An appended event and the ones after it were not in the log before
		// the call. A repeat appended nothing, and the event it returns was.
		slot, err := worlds.EventSlot(event.EventID)
		if err != nil || (result.AllocatedEventCount > 0 && appended > 0 && slot >= appended) {
			continue
		}
		result.Events = append(result.Events, event)
	}
	return nil
}

// findAdmitted returns the earlier event that admitted the transition input
// repeats, when there is one.
func (w *World) findAdmitted(ctx context.Context, input worlds.EventsCreateInput, held int, hasHeld bool) (worlds.EventResult, bool, error) {
	var events []worlds.Event
	if input.Event.CorrelationID != "" {
		page, err := w.EventsListByCorrelationID(ctx, worlds.EventsListByCorrelationIDInput{RunID: input.RunID, CorrelationID: input.Event.CorrelationID})
		if err != nil {
			return worlds.EventResult{}, false, err
		}
		events = page.Data
	} else {
		page, err := w.listEvents(ctx, input.RunID, 0, 0, worlds.EventsResolveDataAll)
		if err != nil {
			return worlds.EventResult{}, false, err
		}
		events = page.Events
	}
	for _, event := range events {
		matches := event.EventType == input.Event.EventType || (input.Event.EventType == worlds.EventTypeHookCreated && event.EventType == worlds.EventTypeHookConflict)
		if !matches || event.CorrelationID != input.Event.CorrelationID {
			continue
		}
		result := worlds.EventResult{EventID: event.EventID, EventType: event.EventType, CreatedAt: event.CreatedAt, Data: event.Data}
		if input.Options.InBandCount != nil {
			result.InBandCount = *input.Options.InBandCount
		}
		if input.Event.EventType == worlds.EventTypeStepCreated {
			step, err := w.StepsGet(ctx, worlds.StepsGetInput{RunID: input.RunID, StepID: worlds.StepID(input.Event.CorrelationID)})
			if err != nil {
				return worlds.EventResult{}, false, err
			}
			result.Step = &step
		}
		if hasHeld {
			page, err := w.listEvents(ctx, input.RunID, held, 0, input.Options.ResolveData)
			if err != nil {
				return worlds.EventResult{}, false, err
			}
			for _, later := range page.Events {
				if later.EventID != event.EventID {
					result.Events = append(result.Events, later)
				}
			}
		}
		return result, true, nil
	}
	return worlds.EventResult{}, false, nil
}

// batchResponse is the answer to a batch write: one outcome per event.
type batchResponse struct {
	Results []struct {
		Status  int    `cbor:"status"`
		Error   string `cbor:"error"`
		Message string `cbor:"message"`
		createResponse
	} `cbor:"results"`
	Allocated *int `cbor:"allocated"`
	// Events and HasMore are the skipped-slot report of an in-band batch.
	Events  []map[string]any `cbor:"events"`
	HasMore bool             `cbor:"hasMore"`
}

// EventsCreateBatch appends the events in one request, as one fenced
// allocation when InBandCount is set. The backend reports an outcome per
// event and keeps the ones it admitted, refusing an event behind a refused
// one of the same correlation ID.
func (w *World) EventsCreateBatch(ctx context.Context, input worlds.EventsCreateBatchInput) (worlds.EventBatchResult, error) {
	if len(input.Events) == 0 {
		result := worlds.EventBatchResult{Results: []worlds.EventBatchItem{}}
		if input.InBandCount != nil {
			result.InBandCount = *input.InBandCount
		}
		return result, nil
	}
	var body []byte
	for _, event := range input.Events {
		if !event.EventType.Valid() || event.EventType == worlds.EventTypeRunCreated || event.EventType == worlds.EventTypeHookConflict || event.EventType == worlds.EventTypeHookReceived || event.EventType == worlds.EventTypeHookCreated {
			return worlds.EventBatchResult{}, fmt.Errorf("%w: %s cannot be batched", worlds.ErrEventInvalid, event.EventType)
		}
		meta, payload, err := encodeEvent(event)
		if err != nil {
			return worlds.EventBatchResult{}, err
		}
		setRequestID(ctx, meta)
		meta["occurredAt"] = occurredAt(event)
		meta["remoteRefBehavior"] = "resolve"
		if input.InBandCount != nil {
			meta["inBand"] = true
			meta["expectedSeqInBand"] = *input.InBandCount
			if input.EventCount != nil {
				meta["maxSlot"] = *input.EventCount
			}
		}
		if body, err = encodeFrame(body, meta, payload); err != nil {
			return worlds.EventBatchResult{}, err
		}
	}
	header := http.Header{}
	header.Set("Content-Type", "application/octet-stream")
	response, err := w.api.request(ctx, http.MethodPost, eventsRoute+"/runs/"+pathEscape(string(input.RunID))+"/events/batch", nil, body, header)
	if err != nil {
		return worlds.EventBatchResult{}, classify(err, input.Events[0].EventType)
	}
	defer response.Body.Close()
	data, err := readAll(response.Body)
	if err != nil {
		return worlds.EventBatchResult{}, err
	}
	var decoded batchResponse
	if err := cborDecode.Unmarshal(data, &decoded); err != nil {
		return worlds.EventBatchResult{}, fmt.Errorf("workflow/vercel: decode batch answer: %w", err)
	}
	if len(decoded.Results) != len(input.Events) {
		return worlds.EventBatchResult{}, fmt.Errorf("workflow/vercel: batch answer has %d results for %d events", len(decoded.Results), len(input.Events))
	}
	result := worlds.EventBatchResult{Results: make([]worlds.EventBatchItem, len(input.Events))}
	for index, item := range decoded.Results {
		event := input.Events[index]
		if item.Status != 0 && item.Status != http.StatusOK {
			err := classify(&apiError{Status: item.Status, Code: item.Error, Message: item.Message}, event.EventType)
			if errors.Is(err, worlds.ErrInBandSuperseded) {
				return worlds.EventBatchResult{}, err
			}
			if repeatable[event.EventType] && item.Status == http.StatusConflict && item.Error != "batch-earlier-item-failed" {
				// A batch appends new transitions only, so one that repeats
				// an admitted transition is invalid.
				_, repeated, findErr := w.findAdmitted(ctx, worlds.EventsCreateInput{RunID: input.RunID, Event: event}, 0, false)
				if findErr != nil {
					return worlds.EventBatchResult{}, findErr
				}
				if repeated {
					err = fmt.Errorf("%w: batch repeats %s %s", worlds.ErrEventInvalid, event.EventType, event.CorrelationID)
				}
			}
			result.Results[index].Err = err
			continue
		}
		if item.Allocated != nil && *item.Allocated == 0 {
			result.Results[index].Err = fmt.Errorf("%w: batch repeats %s %s", worlds.ErrEventInvalid, event.EventType, event.CorrelationID)
			continue
		}
		appended, err := toEvent(input.RunID, wireEventFromMap(item.Event), worlds.EventsResolveDataAll)
		if err != nil {
			return worlds.EventBatchResult{}, err
		}
		one := worlds.EventResult{EventID: appended.EventID, EventType: appended.EventType, CreatedAt: appended.CreatedAt, Data: appended.Data, AllocatedEventCount: 1}
		result.AllocatedEventCount++
		if item.Step != nil {
			step, err := toStep(item.Step, worlds.ResolveDataAll)
			if err != nil {
				return worlds.EventBatchResult{}, err
			}
			one.Step, one.StepCreated = &step, event.EventType == worlds.EventTypeStepCreated
		}
		if item.Wait != nil {
			one.Wait = toWait(item.Wait)
		}
		result.Results[index].EventResult = one
	}
	if decoded.Allocated != nil {
		result.AllocatedEventCount = *decoded.Allocated
	}
	if input.InBandCount != nil {
		result.InBandCount = *input.InBandCount + result.AllocatedEventCount
		for index := range result.Results {
			result.Results[index].InBandCount = result.InBandCount
		}
		if input.EventCount != nil && decoded.Events != nil {
			result.Events, result.MoreEvents = make([]worlds.Event, 0, len(decoded.Events)), decoded.HasMore
			for _, wire := range decoded.Events {
				event, err := toEvent(input.RunID, wireEventFromMap(wire), worlds.EventsResolveDataAll)
				if err != nil {
					return worlds.EventBatchResult{}, err
				}
				result.Events = append(result.Events, event)
			}
		}
	}
	return result, nil
}

// postEvent writes one event frame and decodes the materialized answer.
func (w *World) postEvent(ctx context.Context, runID worlds.RunID, eventType worlds.EventType, meta map[string]any, payload []byte) (createResponse, error) {
	body, err := encodeFrame(nil, meta, payload)
	if err != nil {
		return createResponse{}, err
	}
	header := http.Header{}
	header.Set("Content-Type", "application/octet-stream")
	response, err := w.api.request(ctx, http.MethodPost, eventsRoute+"/runs/"+pathEscape(string(runID))+"/events/"+pathEscape(string(eventType)), nil, body, header)
	if err != nil {
		return createResponse{}, err
	}
	defer response.Body.Close()
	data, err := readAll(response.Body)
	if err != nil {
		return createResponse{}, err
	}
	var decoded createResponse
	if err := cborDecode.Unmarshal(data, &decoded); err != nil {
		return createResponse{}, fmt.Errorf("workflow/vercel: decode %s answer: %w", eventType, err)
	}
	if decoded.Event == nil {
		return createResponse{}, fmt.Errorf("workflow/vercel: %s answer has no event", eventType)
	}
	return decoded, nil
}

func (w *World) EventsGet(ctx context.Context, input worlds.EventsGetInput) (worlds.Event, error) {
	query := url.Values{"remoteRefBehavior": {"resolve"}}
	response, err := w.api.request(ctx, http.MethodGet, eventsRoute+"/runs/"+pathEscape(string(input.RunID))+"/events/"+pathEscape(string(input.EventID)), query, nil, nil)
	if statusOf(err) == http.StatusNotFound {
		if _, runErr := w.getRun(ctx, input.RunID, worlds.ResolveDataNone, "", 0); runErr != nil {
			return worlds.Event{}, runErr
		}
		return worlds.Event{}, fmt.Errorf("%w: %v", worlds.ErrEventNotFound, err)
	}
	if err != nil {
		return worlds.Event{}, err
	}
	defer response.Body.Close()
	wire, err := readEventFrame(response.Body)
	if err != nil {
		return worlds.Event{}, err
	}
	return toEvent(input.RunID, wire, input.ResolveData)
}

func (w *World) EventsList(ctx context.Context, input worlds.EventsListInput) (worlds.EventsPage, error) {
	query := listQuery(input.Pagination)
	query.Set("remoteRefBehavior", refBehavior(input.ResolveData))
	var snapshot worlds.EventsSnapshot
	page, err := w.eventsPage(ctx, input.RunID, eventsRoute+"/runs/"+pathEscape(string(input.RunID))+"/events", query, input.ResolveData, "", &snapshot)
	return worlds.EventsPage{Page: page, Snapshot: snapshot}, err
}

func (w *World) EventsListByCorrelationID(ctx context.Context, input worlds.EventsListByCorrelationIDInput) (worlds.Page[worlds.Event], error) {
	query := listQuery(input.Pagination)
	query.Set("remoteRefBehavior", refBehavior(input.ResolveData))
	query.Set("correlationId", input.CorrelationID)
	query.Set("runId", string(input.RunID))
	return w.eventsPage(ctx, input.RunID, "/v4/events", query, input.ResolveData, input.CorrelationID, nil)
}

// eventsPage reads one list answer, and its counters into snapshot when it
// is not nil. A correlation listing keeps only the run's events, since
// correlation IDs are unique only within a run.
func (w *World) eventsPage(ctx context.Context, runID worlds.RunID, path string, query url.Values, resolve worlds.EventsResolveData, correlationID string, snapshot *worlds.EventsSnapshot) (worlds.Page[worlds.Event], error) {
	page, err := w.readEvents(ctx, path, query)
	if statusOf(err) == http.StatusNotFound {
		// The run is missing, or it has no such events.
		if _, runErr := w.getRun(ctx, runID, worlds.ResolveDataNone, "", 0); runErr != nil {
			return worlds.Page[worlds.Event]{}, runErr
		}
		return worlds.Page[worlds.Event]{}, nil
	}
	if err != nil {
		return worlds.Page[worlds.Event]{}, err
	}
	if snapshot != nil && page.Seq != nil && page.SeqInBand != nil {
		*snapshot = worlds.EventsSnapshot{EventCount: *page.Seq, InBandCount: *page.SeqInBand}
	}
	result := worlds.Page[worlds.Event]{Cursor: page.Cursor, HasMore: page.HasMore}
	for _, wire := range page.Events {
		if correlationID != "" && wire.RunID != "" && wire.RunID != string(runID) {
			continue
		}
		event, err := toEvent(runID, wire, resolve)
		if err != nil {
			return worlds.Page[worlds.Event]{}, err
		}
		result.Data = append(result.Data, event)
	}
	if !result.HasMore {
		result.Cursor = ""
	}
	return result, nil
}

func (w *World) readEvents(ctx context.Context, path string, query url.Values) (eventPage, error) {
	header := http.Header{}
	header.Set("Accept", frameContentType)
	response, err := w.api.request(ctx, http.MethodGet, path, query, nil, header)
	if err != nil {
		return eventPage{}, err
	}
	defer response.Body.Close()
	return readEventPage(response.Body, response.Header)
}

// loadedEvents is every event of a run after a slot, with the in-band count
// the backend read before listing them.
type loadedEvents struct {
	Events    []worlds.Event
	SeqInBand *int
}

// listEvents reads the run's events after slot after, at most limit when it
// is positive, following cursors to the end.
func (w *World) listEvents(ctx context.Context, runID worlds.RunID, after, limit int, resolve worlds.EventsResolveData) (loadedEvents, error) {
	query := url.Values{"remoteRefBehavior": {refBehavior(resolve)}}
	if limit > 0 {
		query.Set("limit", itoa(limit))
	} else {
		query.Set("returnAll", "true")
	}
	if after > 0 {
		query.Set("cursor", "eid:"+string(worlds.EventIDForSlot(uint64(after))))
	}
	var loaded loadedEvents
	for {
		page, err := w.readEvents(ctx, eventsRoute+"/runs/"+pathEscape(string(runID))+"/events", query)
		if err != nil {
			return loadedEvents{}, notFound(err, worlds.ErrRunNotFound)
		}
		if loaded.SeqInBand == nil {
			loaded.SeqInBand = page.SeqInBand
		}
		for _, wire := range page.Events {
			event, err := toEvent(runID, wire, resolve)
			if err != nil {
				return loadedEvents{}, err
			}
			loaded.Events = append(loaded.Events, event)
		}
		if !page.HasMore || page.Cursor == "" || limit > 0 {
			return loaded, nil
		}
		query.Set("cursor", page.Cursor)
	}
}

// listQuery maps a Pagination to list parameters. No limit reads everything.
func listQuery(pagination worlds.Pagination) url.Values {
	query := url.Values{}
	if pagination.Limit > 0 {
		query.Set("limit", itoa(pagination.Limit))
	} else {
		query.Set("returnAll", "true")
	}
	if pagination.Cursor != "" {
		query.Set("cursor", pagination.Cursor)
	}
	if pagination.SortOrder != "" {
		query.Set("sortOrder", string(pagination.SortOrder))
	}
	return query
}

// refBehavior is how the backend returns event payloads. Events always
// carry them, because the payload of step_created and hook_created also holds
// their metadata; ResolveEventData strips them afterwards.
func refBehavior(worlds.EventsResolveData) string { return "resolve" }
