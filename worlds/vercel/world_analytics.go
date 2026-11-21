package vercelworld

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// The backend's analytics page limits (@workflow/world analytics.js): a
// larger Pagination.Limit is sent as the limit, and the page reports
// HasMore.
const (
	analyticsPageLimit          = 100
	analyticsRunScopedPageLimit = 1000
	analyticsGetManyLimit       = 100
)

// analytics reads the backend's analytics endpoints, /v2/analytics, as
// world-vercel's createAnalytics does.
type analytics struct{ world *World }

func (a analytics) get(ctx context.Context, path string, query url.Values, out any) error {
	return a.world.api.do(ctx, http.MethodGet, "/v2/analytics"+path, query, nil, out)
}

func runPath(runID worlds.RunID) string { return "/runs/" + pathEscape(string(runID)) }

// analyticsQuery returns the pagination parameters of a listing whose page
// is at most limit items.
func analyticsQuery(pagination worlds.Pagination, limit int) url.Values {
	query := url.Values{}
	if pagination.Limit > 0 {
		query.Set("limit", strconv.Itoa(min(pagination.Limit, limit)))
	}
	if pagination.Cursor != "" {
		query.Set("cursor", pagination.Cursor)
	}
	if pagination.SortOrder != "" {
		query.Set("sortOrder", string(pagination.SortOrder))
	}
	return query
}

// setWindow sets the creation time window. Like world-vercel's
// assertDateWindow, it refuses one bound without the other, which the
// backend does not take, and a start after the end.
func setWindow(query url.Values, start, end *time.Time) error {
	switch {
	case start == nil && end == nil:
		return nil
	case start == nil || end == nil:
		return fmt.Errorf("%w: the Vercel backend needs both StartTime and EndTime, or neither", worlds.ErrEventInvalid)
	case start.After(*end):
		return fmt.Errorf("%w: StartTime is after EndTime", worlds.ErrEventInvalid)
	}
	query.Set("startTime", start.UTC().Format(time.RFC3339Nano))
	query.Set("endTime", end.UTC().Format(time.RFC3339Nano))
	return nil
}

// analyticsPage converts a page of the backend's analytics with convert.
func analyticsPage[T any](page listResponse, convert func(map[string]any) T) worlds.Page[T] {
	result := worlds.Page[T]{Data: make([]T, len(page.Data)), Cursor: page.cursor(), HasMore: page.HasMore}
	for i, values := range page.Data {
		result.Data[i] = convert(values)
	}
	return result
}

func (a analytics) RunsGet(ctx context.Context, input worlds.AnalyticsRunsGetInput) (worlds.AnalyticsRun, error) {
	var values map[string]any
	if err := a.get(ctx, runPath(input.RunID), nil, &values); err != nil {
		return worlds.AnalyticsRun{}, notFound(err, worlds.ErrRunNotFound)
	}
	return toAnalyticsRun(values), nil
}

func (a analytics) RunsList(ctx context.Context, input worlds.AnalyticsRunsListInput) (worlds.Page[worlds.AnalyticsRun], error) {
	query := analyticsQuery(input.Pagination, analyticsPageLimit)
	if input.WorkflowName != "" {
		query.Set("workflowName", input.WorkflowName)
	}
	switch len(input.Status) {
	case 0:
	case 1:
		query.Set("status", string(input.Status[0]))
	default:
		return worlds.Page[worlds.AnalyticsRun]{}, fmt.Errorf("%w: the Vercel backend filters analytics runs by one status", worlds.ErrEventInvalid)
	}
	if err := setWindow(query, input.StartTime, input.EndTime); err != nil {
		return worlds.Page[worlds.AnalyticsRun]{}, err
	}
	if len(input.Attributes) > 0 {
		attributes, err := json.Marshal(input.Attributes)
		if err != nil {
			return worlds.Page[worlds.AnalyticsRun]{}, err
		}
		query.Set("attributes", string(attributes))
	}
	var page listResponse
	if err := a.get(ctx, "/runs", query, &page); err != nil {
		return worlds.Page[worlds.AnalyticsRun]{}, err
	}
	return analyticsPage(page, toAnalyticsRun), nil
}

func (a analytics) AttributesList(ctx context.Context, input worlds.AnalyticsAttributesListInput) (worlds.Page[worlds.AnalyticsAttribute], error) {
	query := analyticsQuery(input.Pagination, analyticsPageLimit)
	if input.WorkflowName != "" {
		query.Set("workflowName", input.WorkflowName)
	}
	if err := setWindow(query, input.StartTime, input.EndTime); err != nil {
		return worlds.Page[worlds.AnalyticsAttribute]{}, err
	}
	var page listResponse
	if err := a.get(ctx, "/attributes", query, &page); err != nil {
		return worlds.Page[worlds.AnalyticsAttribute]{}, err
	}
	return analyticsPage(page, func(values map[string]any) worlds.AnalyticsAttribute {
		count, _ := asInt(values["runCount"])
		return worlds.AnalyticsAttribute{Key: str(values, "key"), RunCount: count, FirstSeenAt: requiredTime(values, "firstSeenAt"), LastSeenAt: requiredTime(values, "lastSeenAt")}
	}), nil
}

func (a analytics) StepsGet(ctx context.Context, input worlds.AnalyticsStepsGetInput) (worlds.AnalyticsStep, error) {
	var values map[string]any
	if err := a.get(ctx, runPath(input.RunID)+"/steps/"+pathEscape(string(input.StepID)), nil, &values); err != nil {
		return worlds.AnalyticsStep{}, notFound(err, worlds.ErrStepNotFound)
	}
	return toAnalyticsStep(values), nil
}

func (a analytics) StepsList(ctx context.Context, input worlds.AnalyticsStepsListInput) (worlds.Page[worlds.AnalyticsStep], error) {
	var page listResponse
	if err := a.get(ctx, runPath(input.RunID)+"/steps", analyticsQuery(input.Pagination, analyticsRunScopedPageLimit), &page); err != nil {
		return worlds.Page[worlds.AnalyticsStep]{}, notFound(err, worlds.ErrRunNotFound)
	}
	return analyticsPage(page, toAnalyticsStep), nil
}

func (a analytics) EventsGet(ctx context.Context, input worlds.AnalyticsEventsGetInput) (worlds.AnalyticsEvent, error) {
	var values map[string]any
	if err := a.get(ctx, runPath(input.RunID)+"/events/"+pathEscape(string(input.EventID)), nil, &values); err != nil {
		return worlds.AnalyticsEvent{}, notFound(err, worlds.ErrEventNotFound)
	}
	return toAnalyticsEvent(values), nil
}

// EventsGetMany asks for at most analyticsGetManyLimit distinct IDs at a
// time, the backend's limit.
func (a analytics) EventsGetMany(ctx context.Context, input worlds.AnalyticsEventsGetManyInput) ([]worlds.AnalyticsEvent, error) {
	var ids []worlds.EventID
	for _, id := range input.EventIDs {
		if !slices.Contains(ids, id) {
			ids = append(ids, id)
		}
	}
	events := []worlds.AnalyticsEvent{}
	for batch := range slices.Chunk(ids, analyticsGetManyLimit) {
		var values []map[string]any
		if err := a.world.api.do(ctx, http.MethodPost, "/v2/analytics"+runPath(input.RunID)+"/events/get-many", nil, map[string]any{"eventIds": batch}, &values); err != nil {
			return nil, notFound(err, worlds.ErrRunNotFound)
		}
		for _, event := range values {
			events = append(events, toAnalyticsEvent(event))
		}
	}
	return events, nil
}

func (a analytics) EventsList(ctx context.Context, input worlds.AnalyticsEventsListInput) (worlds.Page[worlds.AnalyticsEvent], error) {
	query := analyticsQuery(input.Pagination, analyticsRunScopedPageLimit)
	if input.EventType != "" {
		query.Set("eventType", string(input.EventType))
	}
	if input.CorrelationID != "" {
		query.Set("correlationId", input.CorrelationID)
	}
	var page listResponse
	if err := a.get(ctx, runPath(input.RunID)+"/events", query, &page); err != nil {
		return worlds.Page[worlds.AnalyticsEvent]{}, notFound(err, worlds.ErrRunNotFound)
	}
	return analyticsPage(page, toAnalyticsEvent), nil
}

func (a analytics) EventsListByCorrelationID(ctx context.Context, input worlds.AnalyticsEventsListByCorrelationIDInput) (worlds.Page[worlds.AnalyticsEvent], error) {
	return a.EventsList(ctx, worlds.AnalyticsEventsListInput{RunID: input.RunID, CorrelationID: input.CorrelationID, Pagination: input.Pagination})
}

func (a analytics) HooksGet(ctx context.Context, input worlds.AnalyticsHooksGetInput) (worlds.AnalyticsHook, error) {
	query := url.Values{}
	if input.RunID != "" {
		query.Set("runId", string(input.RunID))
	}
	var values map[string]any
	if err := a.get(ctx, "/hooks/"+pathEscape(string(input.HookID)), query, &values); err != nil {
		return worlds.AnalyticsHook{}, notFound(err, worlds.ErrHookNotFound)
	}
	return toAnalyticsHook(values), nil
}

func (a analytics) HooksList(ctx context.Context, input worlds.AnalyticsHooksListInput) (worlds.Page[worlds.AnalyticsHook], error) {
	query := analyticsQuery(input.Pagination, analyticsPageLimit)
	query.Set("runId", string(input.RunID))
	var page listResponse
	if err := a.get(ctx, "/hooks", query, &page); err != nil {
		return worlds.Page[worlds.AnalyticsHook]{}, notFound(err, worlds.ErrRunNotFound)
	}
	return analyticsPage(page, toAnalyticsHook), nil
}

func (a analytics) WaitsGet(ctx context.Context, input worlds.AnalyticsWaitsGetInput) (worlds.AnalyticsWait, error) {
	var values map[string]any
	if err := a.get(ctx, runPath(input.RunID)+"/waits/"+pathEscape(string(input.WaitID)), nil, &values); err != nil {
		return worlds.AnalyticsWait{}, notFound(err, worlds.ErrEventNotFound)
	}
	return toAnalyticsWait(values), nil
}

func (a analytics) WaitsList(ctx context.Context, input worlds.AnalyticsWaitsListInput) (worlds.Page[worlds.AnalyticsWait], error) {
	query := analyticsQuery(input.Pagination, analyticsRunScopedPageLimit)
	if input.Status != "" {
		query.Set("status", string(input.Status))
	}
	var page listResponse
	if err := a.get(ctx, runPath(input.RunID)+"/waits", query, &page); err != nil {
		return worlds.Page[worlds.AnalyticsWait]{}, notFound(err, worlds.ErrRunNotFound)
	}
	return analyticsPage(page, toAnalyticsWait), nil
}

func toAnalyticsRun(values map[string]any) worlds.AnalyticsRun {
	run := worlds.AnalyticsRun{
		RunID:        worlds.RunID(str(values, "runId")),
		Status:       worlds.Status(str(values, "status")),
		DeploymentID: str(values, "deploymentId"),
		WorkflowName: str(values, "workflowName"),
		ErrorCode:    str(values, "errorCode"),
		Attributes:   map[string]string{},
		CreatedAt:    requiredTime(values, "createdAt"),
		UpdatedAt:    requiredTime(values, "updatedAt"),
		StartedAt:    timeField(values, "startedAt"),
		CompletedAt:  timeField(values, "completedAt"),
	}
	run.SpecVersion, _ = asInt(values["specVersion"])
	if attributes, ok := values["attributes"].(map[string]any); ok {
		for key, value := range attributes {
			if text, ok := value.(string); ok {
				run.Attributes[key] = text
			}
		}
	}
	return run
}

func toAnalyticsStep(values map[string]any) worlds.AnalyticsStep {
	attempt, _ := asInt(values["attempt"])
	return worlds.AnalyticsStep{
		RunID:       worlds.RunID(str(values, "runId")),
		StepID:      worlds.StepID(str(values, "stepId")),
		StepName:    str(values, "stepName"),
		Status:      worlds.Status(str(values, "status")),
		Attempt:     attempt,
		CreatedAt:   requiredTime(values, "createdAt"),
		UpdatedAt:   requiredTime(values, "updatedAt"),
		StartedAt:   timeField(values, "startedAt"),
		CompletedAt: timeField(values, "completedAt"),
		RetryAfter:  timeField(values, "retryAfter"),
	}
}

func toAnalyticsEvent(values map[string]any) worlds.AnalyticsEvent {
	event := worlds.AnalyticsEvent{
		RunID:         worlds.RunID(str(values, "runId")),
		EventID:       worlds.EventID(str(values, "eventId")),
		EventType:     worlds.EventType(str(values, "eventType")),
		CorrelationID: str(values, "correlationId"),
		StepName:      str(values, "stepName"),
		WorkflowName:  str(values, "workflowName"),
		DeploymentID:  str(values, "deploymentId"),
		RunCreatedAt:  requiredTime(values, "runCreatedAt"),
		CreatedAt:     requiredTime(values, "createdAt"),
		ResumeAt:      timeField(values, "resumeAt"),
		RetryAfter:    timeField(values, "retryAfter"),
		ErrorCode:     str(values, "errorCode"),
		IsWebhook:     boolField(values, "isWebhook"),
		IsSystem:      boolField(values, "isSystem"),
	}
	event.SpecVersion, _ = asInt(values["specVersion"])
	return event
}

func toAnalyticsHook(values map[string]any) worlds.AnalyticsHook {
	hook := worlds.AnalyticsHook{
		RunID:      worlds.RunID(str(values, "runId")),
		HookID:     worlds.HookID(str(values, "hookId")),
		Status:     worlds.AnalyticsHookStatus(str(values, "status")),
		CreatedAt:  requiredTime(values, "createdAt"),
		UpdatedAt:  requiredTime(values, "updatedAt"),
		ReceivedAt: timeField(values, "receivedAt"),
		DisposedAt: timeField(values, "disposedAt"),
	}
	hook.IsWebhook, _ = values["isWebhook"].(bool)
	hook.IsSystem, _ = values["isSystem"].(bool)
	return hook
}

func toAnalyticsWait(values map[string]any) worlds.AnalyticsWait {
	return worlds.AnalyticsWait{
		RunID:       worlds.RunID(str(values, "runId")),
		WaitID:      worlds.WaitID(str(values, "waitId")),
		Status:      worlds.WaitStatus(str(values, "status")),
		ResumeAt:    timeField(values, "resumeAt"),
		CreatedAt:   requiredTime(values, "createdAt"),
		UpdatedAt:   requiredTime(values, "updatedAt"),
		CompletedAt: timeField(values, "completedAt"),
	}
}

// boolField returns a nullable boolean field, nil when it is absent or null.
func boolField(values map[string]any, key string) *bool {
	value, ok := values[key].(bool)
	if !ok {
		return nil
	}
	return &value
}
