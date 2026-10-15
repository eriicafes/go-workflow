package vercelworld

import (
	"context"
	"encoding/json"
	"slices"
	"sort"
	"strconv"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// eventDescriptor is the scriptAppend argument for one event. Go parses the
// event data and passes only the fields admission and the views need.
type eventDescriptor struct {
	Type          worlds.EventType `json:"t"`
	CorrelationID string           `json:"c"`
	Record        string           `json:"r"`
	Invalid       bool             `json:"bad,omitempty"`
	Data          string           `json:"d,omitempty"`
	Step          *stepCreatedArg  `json:"step,omitempty"`
	Result        *stepResultArg   `json:"res,omitempty"`
	Attributes    []attributeArg   `json:"attrs,omitempty"`
	Hook          *hookCreatedArg  `json:"hook,omitempty"`
	Received      *hookReceivedArg `json:"recv,omitempty"`
}

type stepCreatedArg struct {
	Name  string `json:"name"`
	Input string `json:"input,omitempty"`
}

type stepResultArg struct {
	Output     string `json:"output,omitempty"`
	Error      string `json:"error,omitempty"`
	RetryAfter string `json:"retryAfter,omitempty"`
}

type attributeArg struct {
	Key   string  `json:"k"`
	Value *string `json:"v,omitempty"`
}

type hookCreatedArg struct {
	Token        string `json:"token"`
	Metadata     string `json:"metadata,omitempty"`
	Webhook      string `json:"webhook,omitempty"`
	Manual       bool   `json:"manual,omitempty"`
	MinRetention string `json:"minRetention,omitempty"`
	RetentionUs  string `json:"retentionUs,omitempty"`
}

type hookReceivedArg struct {
	Request  bool   `json:"req,omitempty"`
	Response bool   `json:"resp,omitempty"`
	To       uint64 `json:"to,omitempty"`
}

type runCreatedData struct {
	WorkflowName string            `json:"workflowName"`
	Input        json.RawMessage   `json:"input"`
	Attributes   map[string]string `json:"attributes"`
	DeploymentID string            `json:"deploymentId,omitempty"`
}

type stepCreatedData struct {
	StepName string          `json:"stepName"`
	Input    json.RawMessage `json:"input"`
}

type stepResultData struct {
	Output     json.RawMessage `json:"output"`
	Error      json.RawMessage `json:"error"`
	RetryAfter *time.Time      `json:"retryAfter"`
}

type hookCreatedData struct {
	Token        string          `json:"token"`
	Metadata     []byte          `json:"metadata"`
	Webhook      *worlds.Webhook `json:"webhook"`
	MinRetention time.Duration   `json:"minRetention"`
}

func (w *World) EventsCreateRun(ctx context.Context, input worlds.EventsCreateRunInput) (worlds.EventResult, error) {
	if err := ctx.Err(); err != nil {
		return worlds.EventResult{}, err
	}
	if input.RunID == "" {
		return worlds.EventResult{}, worlds.ErrEventInvalid
	}
	payload, err := json.Marshal(runCreatedData{WorkflowName: input.WorkflowName, Input: input.Input, Attributes: input.Attributes, DeploymentID: input.DeploymentID})
	if err != nil {
		return worlds.EventResult{}, err
	}
	now := time.Now().UTC()
	record := newEventRecord(worlds.EventsCreateEvent{EventType: worlds.EventTypeRunCreated, Data: payload}, now)
	args := []string{w.prefix, string(input.RunID), micros(now), input.WorkflowName, encodeBytes(input.Input), record, input.DeploymentID}
	keys := make([]string, 0, len(input.Attributes))
	for key := range input.Attributes {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		args = append(args, key, input.Attributes[key])
	}
	if _, err := scriptReply(w.redis.Run(ctx, scriptCreateRun, args...)); err != nil {
		return worlds.EventResult{}, err
	}
	return worlds.EventResult{EventID: worlds.EventIDForSlot(1), EventType: worlds.EventTypeRunCreated, AllocatedEventCount: 1}, nil
}

func (w *World) EventsCreate(ctx context.Context, input worlds.EventsCreateInput) (worlds.EventResult, error) {
	if err := ctx.Err(); err != nil {
		return worlds.EventResult{}, err
	}
	if invalidEventType(input.Event.EventType) {
		return worlds.EventResult{}, worlds.ErrEventInvalid
	}
	now := time.Now().UTC()
	desc, err := describeEvent(input.Event, now)
	if err != nil {
		return worlds.EventResult{}, err
	}
	values, err := scriptReply(w.redis.Run(ctx, scriptAppend, w.prefix, string(input.RunID), micros(now), "append", deltaStart(input.Options), "0", desc))
	if err != nil {
		return worlds.EventResult{}, err
	}
	result, _, err := decodeAppendReply(input.RunID, input.Event.CorrelationID, values, input.Options.ResolveData)
	return result, err
}

func (w *World) EventsCreateBatch(ctx context.Context, input worlds.EventsCreateBatchInput) (worlds.EventBatchResult, error) {
	if err := ctx.Err(); err != nil {
		return worlds.EventBatchResult{}, err
	}
	if len(input.Events) == 0 {
		return worlds.EventBatchResult{}, nil
	}
	now := time.Now().UTC()
	args := []string{w.prefix, string(input.RunID), micros(now), "batch", "-1", "0"}
	for _, event := range input.Events {
		desc, err := describeEvent(event, now)
		if err != nil {
			return worlds.EventBatchResult{}, err
		}
		args = append(args, desc)
	}
	values, err := scriptReply(w.redis.Run(ctx, scriptAppend, args...))
	if err != nil {
		return worlds.EventBatchResult{}, err
	}
	if len(values) != 1+3*len(input.Events) || int(replyInt(values[0])) != len(input.Events) {
		return worlds.EventBatchResult{}, errCorrupt
	}
	result := worlds.EventBatchResult{Events: make([]worlds.EventResult, 0, len(input.Events))}
	for index := 1; index < len(values); index += 3 {
		correlationID := input.Events[len(result.Events)].CorrelationID
		item := worlds.EventResult{EventID: worlds.EventIDForSlot(uint64(replyInt(values[index]))), EventType: worlds.EventType(replyString(values[index+1])), AllocatedEventCount: 1}
		if raw := replyString(values[index+2]); raw != "" {
			step, err := decodeStep(input.RunID, worlds.StepID(correlationID), raw, worlds.ResolveDataAll)
			if err != nil {
				return worlds.EventBatchResult{}, err
			}
			item.Step, item.StepCreated = &step, true
		}
		result.Events = append(result.Events, item)
	}
	return result, nil
}

func (w *World) EventsGet(ctx context.Context, input worlds.EventsGetInput) (worlds.Event, error) {
	if err := ctx.Err(); err != nil {
		return worlds.Event{}, err
	}
	if slot, err := worlds.EventSlot(input.EventID); err == nil {
		raw, ok, err := w.redis.LIndex(ctx, w.key("events", string(input.RunID)), int64(slot)-1)
		if err != nil {
			return worlds.Event{}, err
		}
		if ok {
			return decodeEvent(input.RunID, slot, raw, input.ResolveData)
		}
	}
	if err := w.requireRun(ctx, input.RunID); err != nil {
		return worlds.Event{}, err
	}
	return worlds.Event{}, worlds.ErrEventNotFound
}

func (w *World) requireRun(ctx context.Context, runID worlds.RunID) error {
	exists, err := w.redis.Exists(ctx, w.key("run", string(runID)))
	if err != nil {
		return err
	}
	if !exists {
		return worlds.ErrRunNotFound
	}
	return nil
}

func (w *World) EventsList(ctx context.Context, input worlds.EventsListInput) (worlds.Page[worlds.Event], error) {
	// Pages are by slot, and the cursor is the last returned event ID in
	// either order.
	if err := ctx.Err(); err != nil {
		return worlds.Page[worlds.Event]{}, err
	}
	var cursor int64
	if input.Pagination.Cursor != "" {
		slot, err := worlds.EventSlot(worlds.EventID(input.Pagination.Cursor))
		if err != nil {
			return worlds.Page[worlds.Event]{}, err
		}
		cursor = int64(slot)
	}
	limit := int64(input.Pagination.Limit)
	desc := input.Pagination.SortOrder == worlds.SortOrderDescending
	// [low, high] are 0-based log indexes, one more than the page when limited
	// so HasMore is exact.
	low, high := cursor, int64(-1)
	if limit > 0 {
		high = cursor + limit
	}
	if desc {
		end := cursor - 1
		if cursor == 0 {
			count, err := w.eventCount(ctx, input.RunID)
			if err != nil {
				return worlds.Page[worlds.Event]{}, err
			}
			end = count
		}
		high, low = end-1, 0
		if limit > 0 {
			low = max(high-limit, 0)
		}
	}
	var raws []string
	if high >= low || (!desc && high == -1) {
		var err error
		if raws, err = w.redis.LRange(ctx, w.key("events", string(input.RunID)), low, high); err != nil {
			return worlds.Page[worlds.Event]{}, err
		}
	}
	if len(raws) == 0 {
		if err := w.requireRun(ctx, input.RunID); err != nil {
			return worlds.Page[worlds.Event]{}, err
		}
	}
	events := make([]worlds.Event, len(raws))
	for index, raw := range raws {
		event, err := decodeEvent(input.RunID, uint64(low)+uint64(index)+1, raw, input.ResolveData)
		if err != nil {
			return worlds.Page[worlds.Event]{}, err
		}
		events[index] = event
	}
	if desc {
		slices.Reverse(events)
	}
	page := worlds.Page[worlds.Event]{Data: events}
	if limit > 0 && int64(len(events)) > limit {
		page.Data, page.HasMore = events[:limit], true
		page.Cursor = string(page.Data[limit-1].EventID)
	}
	return page, nil
}

func (w *World) eventCount(ctx context.Context, runID worlds.RunID) (int64, error) {
	value, ok, err := w.redis.HGet(ctx, w.key("run", string(runID)), "count")
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, worlds.ErrRunNotFound
	}
	return strconv.ParseInt(value, 10, 64)
}

func (w *World) EventsListByCorrelationID(ctx context.Context, input worlds.EventsListByCorrelationIDInput) (worlds.Page[worlds.Event], error) {
	// Pages come from a correlation index, and the cursor is the last
	// returned event ID.
	if err := ctx.Err(); err != nil {
		return worlds.Page[worlds.Event]{}, err
	}
	cursor := ""
	if input.Pagination.Cursor != "" {
		slot, err := worlds.EventSlot(worlds.EventID(input.Pagination.Cursor))
		if err != nil {
			return worlds.Page[worlds.Event]{}, err
		}
		cursor = strconv.FormatUint(slot, 10)
	}
	desc := input.Pagination.SortOrder == worlds.SortOrderDescending
	min, max, err := scoreRange(cursor, desc)
	if err != nil {
		return worlds.Page[worlds.Event]{}, err
	}
	members, err := w.redis.ZRangeByScore(ctx, w.corrKey(input.RunID, input.CorrelationID), min, max, desc, fetchCount(input.Pagination.Limit))
	if err != nil {
		return worlds.Page[worlds.Event]{}, err
	}
	if len(members) == 0 {
		if err := w.requireRun(ctx, input.RunID); err != nil {
			return worlds.Page[worlds.Event]{}, err
		}
		return worlds.Page[worlds.Event]{Data: []worlds.Event{}}, nil
	}
	members, _, hasMore := trimPage(members, input.Pagination.Limit)
	args := []string{w.prefix, string(input.RunID)}
	for _, member := range members {
		args = append(args, member.Member)
	}
	result, err := w.redis.Run(ctx, scriptEventsAt, args...)
	if err != nil {
		return worlds.Page[worlds.Event]{}, err
	}
	raws := replyStrings(result)
	if len(raws) != len(members) {
		return worlds.Page[worlds.Event]{}, errCorrupt
	}
	page := worlds.Page[worlds.Event]{Data: make([]worlds.Event, len(raws)), HasMore: hasMore}
	for index, raw := range raws {
		event, err := decodeEvent(input.RunID, uint64(members[index].Score), raw, input.ResolveData)
		if err != nil {
			return worlds.Page[worlds.Event]{}, err
		}
		page.Data[index] = event
	}
	if hasMore {
		page.Cursor = string(page.Data[len(page.Data)-1].EventID)
	}
	return page, nil
}

func invalidEventType(eventType worlds.EventType) bool {
	return eventType == worlds.EventTypeRunCreated || !eventType.Valid()
}

func describeEvent(event worlds.EventsCreateEvent, now time.Time) (string, error) {
	desc := eventDescriptor{Type: event.EventType, CorrelationID: event.CorrelationID, Record: newEventRecord(event, now), Invalid: invalidEventType(event.EventType)}
	switch event.EventType {
	case worlds.EventTypeRunCompleted, worlds.EventTypeRunFailed:
		desc.Data = encodeBytes(event.Data)
	case worlds.EventTypeAttrSet:
		var data attrSetData
		if json.Unmarshal(event.Data, &data) == nil {
			for _, change := range data.Changes {
				desc.Attributes = append(desc.Attributes, attributeArg{Key: change.Key, Value: change.Value})
			}
		}
	case worlds.EventTypeStepCreated:
		var data stepCreatedData
		_ = json.Unmarshal(event.Data, &data)
		desc.Step = &stepCreatedArg{Name: data.StepName, Input: encodeBytes(data.Input)}
	case worlds.EventTypeStepStarted, worlds.EventTypeStepCompleted, worlds.EventTypeStepFailed, worlds.EventTypeStepRetrying, worlds.EventTypeStepCancelled:
		var data stepResultData
		if json.Unmarshal(event.Data, &data) == nil {
			desc.Result = &stepResultArg{Output: encodeBytes(data.Output), Error: encodeBytes(data.Error)}
			if data.RetryAfter != nil {
				desc.Result.RetryAfter = data.RetryAfter.UTC().Format(time.RFC3339Nano)
			}
		}
	case worlds.EventTypeHookCreated:
		var data hookCreatedData
		if json.Unmarshal(event.Data, &data) == nil {
			desc.Hook = &hookCreatedArg{Token: data.Token, Metadata: encodeBytes(data.Metadata)}
			if data.Webhook != nil {
				webhook, err := json.Marshal(data.Webhook)
				if err != nil {
					return "", err
				}
				desc.Hook.Webhook, desc.Hook.Manual = string(webhook), data.Webhook.Manual
			}
			if data.MinRetention > 0 {
				desc.Hook.MinRetention = strconv.FormatInt(int64(data.MinRetention), 10)
				desc.Hook.RetentionUs = strconv.FormatInt((int64(data.MinRetention)+999)/1000, 10)
			}
		}
	case worlds.EventTypeHookReceived:
		var data worlds.WebhookReceivedEventData
		if json.Unmarshal(event.Data, &data) == nil {
			desc.Received = &hookReceivedArg{Request: data.Request != nil, Response: data.Response != nil}
			if slot, err := worlds.EventSlot(data.ResponseTo); err == nil {
				desc.Received.To = slot
			}
		}
	}
	encoded, err := json.Marshal(desc)
	return string(encoded), err
}

// deltaStart returns the 0-based log index the result delta starts at, or -1
// for no delta. Lua clamps it to the log length.
func deltaStart(options worlds.EventsCreateOptions) string {
	if options.EventCount != nil {
		return strconv.Itoa(max(*options.EventCount, 0))
	}
	if options.SinceCursor != "" {
		if slot, err := worlds.EventSlot(worlds.EventID(options.SinceCursor)); err == nil {
			return strconv.FormatUint(slot, 10)
		}
	}
	return "-1"
}

// decodeAppendReply reads {slot, type, allocated, step, forcedRunID,
// deltaStart, deltaRecords...}.
func decodeAppendReply(runID worlds.RunID, correlationID string, values []any, resolve worlds.EventsResolveData) (worlds.EventResult, worlds.RunID, error) {
	if len(values) < 6 {
		return worlds.EventResult{}, "", errCorrupt
	}
	result := worlds.EventResult{EventID: worlds.EventIDForSlot(uint64(replyInt(values[0]))), EventType: worlds.EventType(replyString(values[1])), AllocatedEventCount: int(replyInt(values[2]))}
	if raw := replyString(values[3]); raw != "" {
		step, err := decodeStep(runID, worlds.StepID(correlationID), raw, worlds.ResolveDataAll)
		if err != nil {
			return worlds.EventResult{}, "", err
		}
		result.Step = &step
		result.StepCreated = result.AllocatedEventCount == 1
	}
	start := uint64(replyInt(values[5]))
	records := values[6:]
	if len(records) > 0 {
		result.Events = make([]worlds.Event, len(records))
		for index, record := range records {
			event, err := decodeEvent(runID, start+uint64(index)+1, replyString(record), resolve)
			if err != nil {
				return worlds.EventResult{}, "", err
			}
			result.Events[index] = event
		}
	}
	return result, worlds.RunID(replyString(values[4])), nil
}
