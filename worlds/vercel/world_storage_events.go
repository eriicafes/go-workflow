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
	// ReportIncomplete says the backend could not complete that report, which
	// the writer reads as HasMore.
	Events           []map[string]any `cbor:"events"`
	HasMore          bool             `cbor:"hasMore"`
	ReportIncomplete bool             `cbor:"reportIncomplete"`
}

// runCreationMeta returns the frame meta of a run's creation, which
// run_created carries, and a first delivery's run_started too.
func (w *World) runCreationMeta(input worlds.EventsCreateRunInput, eventType worlds.EventType) map[string]any {
	meta := map[string]any{
		"eventType":   string(eventType),
		"specVersion": specVersion,
		// inBand is required on every write. A creation is out-of-band;
		// EventsCreate overwrites it for a first delivery's run_started.
		"inBand":            false,
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
	attributes, allowReserved := input.Attributes, input.AllowReservedAttributes
	if retention := w.config.retentionFor(input.WorkflowName); retention != worlds.RetainDefault {
		if _, set := attributes[worlds.RetentionAttribute]; !set {
			// The World sets the run's retention, a reserved attribute.
			attributes = make(map[string]string, len(input.Attributes)+1)
			maps.Copy(attributes, input.Attributes)
			attributes[worlds.RetentionAttribute] = retention.String()
			allowReserved = true
		}
	}
	if len(attributes) > 0 {
		meta["attributes"] = attributes
		if allowReserved {
			meta["allowReservedAttributes"] = true
		}
	}
	return meta
}

func (w *World) EventsCreateRun(ctx context.Context, input worlds.EventsCreateRunInput) (worlds.EventResult, error) {
	// A start hook without a token is refused before it is sent.
	if input.StartHook != nil && input.StartHook.Token == "" {
		return worlds.EventResult{}, fmt.Errorf("%w: start hook without a token", worlds.ErrEventInvalid)
	}
	meta := w.runCreationMeta(input, worlds.EventTypeRunCreated)
	response, err := retryEventWrite(ctx, true, false, func() (createResponse, error) {
		return w.postEvent(ctx, input.RunID, worlds.EventTypeRunCreated, meta, input.Input)
	})
	if err != nil {
		return worlds.EventResult{}, classify(err, worlds.EventTypeRunCreated)
	}
	event, err := toEvent(input.RunID, wireEventFromMap(response.Event), worlds.EventsResolveDataAll)
	if err != nil {
		return worlds.EventResult{}, err
	}
	run, err := responseRun(response)
	if err != nil {
		return worlds.EventResult{}, err
	}
	// run_created takes the first position, which counts as in-band.
	return worlds.EventResult{EventID: event.EventID, EventType: worlds.EventTypeRunCreated, CreatedAt: event.CreatedAt, Data: event.Data, Event: &event, Run: run, AllocatedEventCount: 1, InBandCount: 1}, nil
}

func (w *World) EventsCreate(ctx context.Context, input worlds.EventsCreateInput) (worlds.EventResult, error) {
	if err := ctx.Err(); err != nil {
		return worlds.EventResult{}, err
	}
	if !input.Event.EventType.Creatable() {
		return worlds.EventResult{}, fmt.Errorf("%w: %s cannot be appended", worlds.ErrEventInvalid, input.Event.EventType)
	}
	// An attr_set whose data is over the backend's limit is refused before
	// it is sent.
	if size := worlds.AttrSetDataSize(input.Event.Data); input.Event.EventType == worlds.EventTypeAttrSet && size > worlds.MaxAttrSetDataBytes {
		return worlds.EventResult{}, fmt.Errorf("%w: %w: attr_set data is %d bytes, over %d", worlds.ErrAttributeInvalid, worlds.ErrEventInvalid, size, worlds.MaxAttrSetDataBytes)
	}
	meta, payload, err := encodeEvent(input.Event)
	if err != nil {
		return worlds.EventResult{}, err
	}
	if input.Event.EventType == worlds.EventTypeRunStarted {
		// A first delivery's run_started carries the run's creation, which the
		// backend creates the run from when run_created never landed.
		var created worlds.DataRunCreated
		if json.Unmarshal(input.Event.Data, &created) == nil && created.WorkflowName != "" {
			meta = w.runCreationMeta(worlds.EventsCreateRunInput{RunID: input.RunID, WorkflowName: created.WorkflowName, Input: created.Input, Attributes: created.Attributes, DeploymentID: created.DeploymentID, EncryptionPublicKey: created.EncryptionPublicKey, ExecutionContext: created.ExecutionContext, StartHook: created.StartHook, AllowReservedAttributes: created.AllowReservedAttributes}, worlds.EventTypeRunStarted)
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
	// inBand is required on every write, false included.
	meta["inBand"] = input.Options.InBandCount != nil
	if input.Options.InBandCount != nil {
		meta["expectedSeqInBand"] = *input.Options.InBandCount
		if !hasHeld {
			// The backend refuses an in-band write without maxSlot. A writer
			// that holds no count of the log is given its in-band count,
			// which no current log is shorter than, and is not told what
			// the backend reports.
			meta["maxSlot"] = *input.Options.InBandCount
		}
	}
	if created := input.Options.StepCreatedAt; !created.IsZero() && input.Event.EventType == worlds.EventTypeStepStarted && meta["attempt"] == 1 && payload == nil {
		// The backend reads the step's creation time, in milliseconds, only
		// on the first attempt's start of a step created before it.
		meta["stepCreatedAt"] = created.UnixMilli()
	}
	eligible := retriesTransient(input.Event.EventType, input.Options.InBandCount != nil, input.Options.ResumeID != "")
	response, err := retryEventWrite(ctx, eligible, recordsStepOutcome(input.Event.EventType, input.Options), func() (createResponse, error) {
		return w.postEvent(ctx, input.RunID, input.Event.EventType, meta, payload)
	})
	if err != nil {
		return worlds.EventResult{}, classify(err, input.Event.EventType)
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
	result := worlds.EventResult{EventID: event.EventID, EventType: event.EventType, CreatedAt: event.CreatedAt, Data: event.Data, Event: &event, AllocatedEventCount: allocated}
	if result.Run, err = responseRun(response); err != nil {
		return worlds.EventResult{}, err
	}
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
	if input.Options.InBandCount != nil && hasHeld && (response.Events != nil || response.ReportIncomplete) {
		// The skipped-slot report of an in-band write, which an incomplete
		// one leaves for the writer to read from the log.
		result.Events, result.MoreEvents = make([]worlds.Event, 0, len(response.Events)), response.HasMore || response.ReportIncomplete
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

// batchResponse is the answer to a batch write: one outcome per event.
type batchResponse struct {
	Results []struct {
		Status  int    `cbor:"status"`
		Error   string `cbor:"error"`
		Message string `cbor:"message"`
		createResponse
	} `cbor:"results"`
	Allocated *int `cbor:"allocated"`
	// Events and HasMore are the skipped-slot report of an in-band batch, and
	// ReportIncomplete says the backend could not complete it.
	Events           []map[string]any `cbor:"events"`
	HasMore          bool             `cbor:"hasMore"`
	ReportIncomplete bool             `cbor:"reportIncomplete"`
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
	// A batch is sent again after a transient failure only when every event
	// in it may be.
	eligible := true
	for _, event := range input.Events {
		eligible = eligible && retriesTransient(event.EventType, input.InBandCount != nil, false)
		if !event.EventType.Creatable() || event.EventType == worlds.EventTypeHookReceived || event.EventType == worlds.EventTypeHookCreated {
			return worlds.EventBatchResult{}, fmt.Errorf("%w: %s cannot be batched", worlds.ErrEventInvalid, event.EventType)
		}
		meta, payload, err := encodeEvent(event)
		if err != nil {
			return worlds.EventBatchResult{}, err
		}
		setRequestID(ctx, meta)
		meta["occurredAt"] = occurredAt(event)
		meta["remoteRefBehavior"] = "resolve"
		meta["inBand"] = input.InBandCount != nil
		if input.InBandCount != nil {
			meta["expectedSeqInBand"] = *input.InBandCount
			// The backend refuses an in-band write without maxSlot, which is
			// the writer's in-band count when it holds no count of the log.
			meta["maxSlot"] = *input.InBandCount
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
	decoded, err := retryEventWrite(ctx, eligible, false, func() (batchResponse, error) {
		response, err := w.api.request(ctx, eventsRequest, http.MethodPost, eventsRoute+"/runs/"+pathEscape(string(input.RunID))+"/events/batch", nil, body, header)
		if err != nil {
			return batchResponse{}, err
		}
		defer response.Body.Close()
		data, err := readAll(response.Body)
		if err != nil {
			return batchResponse{}, err
		}
		var decoded batchResponse
		if err := cborDecode.Unmarshal(data, &decoded); err != nil {
			return batchResponse{}, &transientError{err: fmt.Errorf("workflow/vercel: decode batch answer: %w", err)}
		}
		return decoded, nil
	})
	if err != nil {
		return worlds.EventBatchResult{}, classify(err, input.Events[0].EventType)
	}
	if len(decoded.Results) != len(input.Events) {
		return worlds.EventBatchResult{}, fmt.Errorf("workflow/vercel: batch answer has %d results for %d events", len(decoded.Results), len(input.Events))
	}
	result := worlds.EventBatchResult{Results: make([]worlds.EventBatchItem, len(input.Events))}
	for index, item := range decoded.Results {
		event := input.Events[index]
		if item.Status != 0 && item.Status != http.StatusOK {
			err := classify(&apiError{Status: item.Status, Code: item.Error, Message: item.Message}, event.EventType)
			if _, ok := errors.AsType[*worlds.InBandSupersededError](err); ok {
				return worlds.EventBatchResult{}, err
			}
			result.Results[index].Err = err
			continue
		}
		if item.Allocated != nil && *item.Allocated == 0 {
			result.Results[index].Err = fmt.Errorf("%w: batch repeats %s %s", worlds.ErrEntityConflict, event.EventType, event.CorrelationID)
			continue
		}
		appended, err := toEvent(input.RunID, wireEventFromMap(item.Event), worlds.EventsResolveDataAll)
		if err != nil {
			return worlds.EventBatchResult{}, err
		}
		one := worlds.EventResult{EventID: appended.EventID, EventType: appended.EventType, CreatedAt: appended.CreatedAt, Data: appended.Data, Event: &appended, AllocatedEventCount: 1}
		if one.Run, err = responseRun(item.createResponse); err != nil {
			return worlds.EventBatchResult{}, err
		}
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
		if input.EventCount != nil && (decoded.Events != nil || decoded.ReportIncomplete) {
			result.Events, result.MoreEvents = make([]worlds.Event, 0, len(decoded.Events)), decoded.HasMore || decoded.ReportIncomplete
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
	response, err := w.api.request(ctx, eventsRequest, http.MethodPost, eventsRoute+"/runs/"+pathEscape(string(runID))+"/events/"+pathEscape(string(eventType)), nil, body, header)
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
		return createResponse{}, &transientError{err: fmt.Errorf("workflow/vercel: decode %s answer: %w", eventType, err)}
	}
	if decoded.Event == nil {
		return createResponse{}, fmt.Errorf("workflow/vercel: %s answer has no event", eventType)
	}
	return decoded, nil
}

func (w *World) EventsGet(ctx context.Context, input worlds.EventsGetInput) (worlds.Event, error) {
	query := url.Values{"remoteRefBehavior": {"resolve"}}
	response, err := w.api.request(ctx, eventsRequest, http.MethodGet, eventsRoute+"/runs/"+pathEscape(string(input.RunID))+"/events/"+pathEscape(string(input.EventID)), query, nil, nil)
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
	if input.Pagination.Limit <= 0 {
		// No limit reads the whole log.
		query.Set("returnAll", "true")
	}
	query.Set("remoteRefBehavior", refBehavior(input.ResolveData))
	var list worlds.EventsPage
	page, err := w.eventsPage(ctx, input.RunID, eventsRoute+"/runs/"+pathEscape(string(input.RunID))+"/events", query, input.ResolveData, "", &list)
	list.Page = page
	return list, err
}

func (w *World) EventsListByCorrelationID(ctx context.Context, input worlds.EventsListByCorrelationIDInput) (worlds.Page[worlds.Event], error) {
	// The correlation route takes a cursor, a limit and an order, and no
	// returnAll.
	query := listQuery(input.Pagination)
	query.Set("remoteRefBehavior", refBehavior(input.ResolveData))
	query.Set("correlationId", input.CorrelationID)
	query.Set("runId", string(input.RunID))
	return w.eventsPage(ctx, input.RunID, "/v4/events", query, input.ResolveData, input.CorrelationID, nil)
}

// eventsPage reads one list answer, and its counters and the run's event
// limit into list when it is not nil. A correlation listing keeps only the run's events, since
// correlation IDs are unique only within a run.
func (w *World) eventsPage(ctx context.Context, runID worlds.RunID, path string, query url.Values, resolve worlds.EventsResolveData, correlationID string, list *worlds.EventsPage) (worlds.Page[worlds.Event], error) {
	page, err := w.readEventLog(ctx, path, query, correlationID == "")
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
	if list != nil {
		list.MaxEvents = page.MaxEvents
		if page.Seq != nil && page.SeqInBand != nil {
			list.Snapshot = worlds.EventsSnapshot{EventCount: *page.Seq, InBandCount: *page.SeqInBand}
		}
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
	// The backend's cursor is kept on the last page too: it is where an
	// incremental read resumes.
	return result, nil
}

// maxPartialReads is how often a read of a whole log continues an answer
// that was cut off before its end frame.
const maxPartialReads = 2

// readEventLog reads one list answer of path. A read of a run's whole log,
// one whose query has returnAll, keeps what it has read: a rate limit is
// waited out and the same page asked for again, and an answer cut off before
// its end frame is continued after its last whole event, up to
// maxPartialReads times. A read with a limit returns either as its error, as
// does a correlation listing, runScoped false. A run's answer that returns
// events or says there are more without moving the cursor is an error.
func (w *World) readEventLog(ctx context.Context, path string, query url.Values, runScoped bool) (eventPage, error) {
	fullLog := runScoped && query.Get("returnAll") == "true"
	asked := maps.Clone(query)
	var events []wireEvent
	var throttle throttleWaiter
	for partial := 0; ; {
		page, err := w.readEvents(ctx, path, asked)
		if err != nil {
			if throttled, ok := errors.AsType[*worlds.ThrottleError](err); ok && fullLog && throttle.wait(ctx, throttled.After) {
				continue
			}
			if _, truncated := errors.AsType[*truncatedError](err); !truncated || !fullLog || len(page.Events) == 0 || partial == maxPartialReads || ctx.Err() != nil {
				return eventPage{}, err
			}
			partial++
			events = append(events, page.Events...)
			asked.Set("cursor", "eid:"+page.Events[len(page.Events)-1].EventID)
			continue
		}
		if runScoped && (page.Cursor == "" || page.Cursor == asked.Get("cursor")) && (len(page.Events) > 0 || page.HasMore) {
			return eventPage{}, errors.New("workflow/vercel: event list answer did not advance its cursor")
		}
		if page.Cursor == "" && partial > 0 {
			page.Cursor = asked.Get("cursor")
		}
		page.Events = append(events, page.Events...)
		return page, nil
	}
}

func (w *World) readEvents(ctx context.Context, path string, query url.Values) (eventPage, error) {
	header := http.Header{}
	header.Set("Accept", frameContentType)
	response, err := w.api.request(ctx, eventsRequest, http.MethodGet, path, query, nil, header)
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
		page, err := w.readEventLog(ctx, eventsRoute+"/runs/"+pathEscape(string(runID))+"/events", query, true)
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

// listQuery maps a Pagination to list parameters.
func listQuery(pagination worlds.Pagination) url.Values {
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
	return query
}

// refBehavior is how the backend returns event payloads. Events always
// carry them, because the payload of step_created and hook_created also holds
// their metadata; ResolveEventData strips them afterwards.
func refBehavior(worlds.EventsResolveData) string { return "resolve" }

// responseRun returns the run a write's answer holds, or nil when it holds
// none.
func responseRun(response createResponse) (*worlds.Run, error) {
	if response.Run == nil {
		return nil, nil
	}
	run, err := toRun(response.Run, worlds.ResolveDataAll)
	if err != nil {
		return nil, err
	}
	return &run, nil
}
