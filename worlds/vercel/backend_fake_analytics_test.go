package vercelworld

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// analytics serves /v2/analytics from the store's analytics, in the shapes
// @workflow/world's analytics schemas read. Times are timezone-naive UTC
// strings, as the backend's ClickHouse-backed answers are.
func (f *fakeBackend) analytics(ctx context.Context, writer http.ResponseWriter, request *http.Request, parts []string, query map[string][]string) {
	store := f.store.Analytics()
	first := func(key string) string {
		if values := query[key]; len(values) > 0 {
			return values[0]
		}
		return ""
	}
	window := func() (*time.Time, *time.Time) {
		start, startErr := time.Parse(time.RFC3339Nano, first("startTime"))
		end, endErr := time.Parse(time.RFC3339Nano, first("endTime"))
		if startErr != nil || endErr != nil {
			return nil, nil
		}
		return &start, &end
	}
	var value any
	var err error
	switch {
	case len(parts) == 2 && parts[0] == "runs":
		var run worlds.AnalyticsRun
		run, err = store.RunsGet(ctx, worlds.AnalyticsRunsGetInput{RunID: worlds.RunID(parts[1])})
		value = wireAnalyticsRun(run)
	case len(parts) == 1 && parts[0] == "runs":
		input := worlds.AnalyticsRunsListInput{WorkflowName: first("workflowName"), Pagination: pagination(query)}
		input.StartTime, input.EndTime = window()
		if status := first("status"); status != "" {
			input.Status = []worlds.Status{worlds.Status(status)}
		}
		if attributes := first("attributes"); attributes != "" {
			_ = json.Unmarshal([]byte(attributes), &input.Attributes)
		}
		var page worlds.Page[worlds.AnalyticsRun]
		page, err = store.RunsList(ctx, input)
		value = wirePage(page, wireAnalyticsRun)
	case len(parts) == 1 && parts[0] == "attributes":
		input := worlds.AnalyticsAttributesListInput{WorkflowName: first("workflowName"), Pagination: pagination(query)}
		input.StartTime, input.EndTime = window()
		var page worlds.Page[worlds.AnalyticsAttribute]
		page, err = store.AttributesList(ctx, input)
		value = wirePage(page, func(attribute worlds.AnalyticsAttribute) map[string]any {
			return map[string]any{"key": attribute.Key, "runCount": attribute.RunCount, "firstSeenAt": naive(attribute.FirstSeenAt), "lastSeenAt": naive(attribute.LastSeenAt)}
		})
	case len(parts) == 4 && parts[0] == "runs" && parts[2] == "steps":
		var step worlds.AnalyticsStep
		step, err = store.StepsGet(ctx, worlds.AnalyticsStepsGetInput{RunID: worlds.RunID(parts[1]), StepID: worlds.StepID(parts[3])})
		value = wireAnalyticsStep(step)
	case len(parts) == 3 && parts[0] == "runs" && parts[2] == "steps":
		var page worlds.Page[worlds.AnalyticsStep]
		page, err = store.StepsList(ctx, worlds.AnalyticsStepsListInput{RunID: worlds.RunID(parts[1]), Pagination: pagination(query)})
		value = wirePage(page, wireAnalyticsStep)
	case len(parts) == 4 && parts[0] == "runs" && parts[2] == "events" && parts[3] == "get-many" && request.Method == http.MethodPost:
		var body struct {
			EventIDs []worlds.EventID `cbor:"eventIds"`
		}
		data, _ := io.ReadAll(request.Body)
		if err = cborDecode.Unmarshal(data, &body); err == nil {
			var events []worlds.AnalyticsEvent
			events, err = store.EventsGetMany(ctx, worlds.AnalyticsEventsGetManyInput{RunID: worlds.RunID(parts[1]), EventIDs: body.EventIDs})
			wire := make([]map[string]any, len(events))
			for i, event := range events {
				wire[i] = wireAnalyticsEvent(event)
			}
			value = wire
		}
	case len(parts) == 4 && parts[0] == "runs" && parts[2] == "events":
		var event worlds.AnalyticsEvent
		event, err = store.EventsGet(ctx, worlds.AnalyticsEventsGetInput{RunID: worlds.RunID(parts[1]), EventID: worlds.EventID(parts[3])})
		value = wireAnalyticsEvent(event)
	case len(parts) == 3 && parts[0] == "runs" && parts[2] == "events":
		var page worlds.Page[worlds.AnalyticsEvent]
		page, err = store.EventsList(ctx, worlds.AnalyticsEventsListInput{RunID: worlds.RunID(parts[1]), EventType: worlds.EventType(first("eventType")), CorrelationID: first("correlationId"), Pagination: pagination(query)})
		value = wirePage(page, wireAnalyticsEvent)
	case len(parts) == 2 && parts[0] == "hooks":
		var hook worlds.AnalyticsHook
		hook, err = store.HooksGet(ctx, worlds.AnalyticsHooksGetInput{HookID: worlds.HookID(parts[1]), RunID: worlds.RunID(first("runId"))})
		value = wireAnalyticsHook(hook)
	case len(parts) == 1 && parts[0] == "hooks":
		var page worlds.Page[worlds.AnalyticsHook]
		page, err = store.HooksList(ctx, worlds.AnalyticsHooksListInput{RunID: worlds.RunID(first("runId")), Pagination: pagination(query)})
		value = wirePage(page, wireAnalyticsHook)
	case len(parts) == 4 && parts[0] == "runs" && parts[2] == "waits":
		var wait worlds.AnalyticsWait
		wait, err = store.WaitsGet(ctx, worlds.AnalyticsWaitsGetInput{RunID: worlds.RunID(parts[1]), WaitID: worlds.WaitID(parts[3])})
		value = wireAnalyticsWait(wait)
	case len(parts) == 3 && parts[0] == "runs" && parts[2] == "waits":
		var page worlds.Page[worlds.AnalyticsWait]
		page, err = store.WaitsList(ctx, worlds.AnalyticsWaitsListInput{RunID: worlds.RunID(parts[1]), Status: worlds.WaitStatus(first("status")), Pagination: pagination(query)})
		value = wirePage(page, wireAnalyticsWait)
	default:
		writeError(writer, http.StatusNotFound, "", "no analytics route "+request.URL.Path)
		return
	}
	if err != nil {
		writeGoError(writer, err)
		return
	}
	writeCBOR(writer, value)
}

func wirePage[T any](page worlds.Page[T], wire func(T) map[string]any) map[string]any {
	data := make([]map[string]any, len(page.Data))
	for i, item := range page.Data {
		data[i] = wire(item)
	}
	return map[string]any{"data": data, "cursor": nilIfEmpty(page.Cursor), "hasMore": page.HasMore}
}

// naive formats a time as the backend's analytics do, without a zone.
func naive(at time.Time) string { return at.UTC().Format("2006-01-02 15:04:05.000") }

func naivePtr(at *time.Time) any {
	if at == nil {
		return nil
	}
	return naive(*at)
}

func wireAnalyticsRun(run worlds.AnalyticsRun) map[string]any {
	return map[string]any{"runId": string(run.RunID), "status": string(run.Status), "workflowName": run.WorkflowName, "deploymentId": run.DeploymentID, "specVersion": run.SpecVersion, "errorCode": nilIfEmpty(run.ErrorCode), "attributes": run.Attributes, "createdAt": naive(run.CreatedAt), "updatedAt": naive(run.UpdatedAt), "startedAt": naivePtr(run.StartedAt), "completedAt": naivePtr(run.CompletedAt)}
}

func wireAnalyticsStep(step worlds.AnalyticsStep) map[string]any {
	return map[string]any{"runId": string(step.RunID), "stepId": string(step.StepID), "stepName": step.StepName, "status": string(step.Status), "attempt": step.Attempt, "createdAt": naive(step.CreatedAt), "updatedAt": naive(step.UpdatedAt), "startedAt": naivePtr(step.StartedAt), "completedAt": naivePtr(step.CompletedAt), "retryAfter": naivePtr(step.RetryAfter)}
}

func wireAnalyticsEvent(event worlds.AnalyticsEvent) map[string]any {
	wire := map[string]any{"runId": string(event.RunID), "eventId": string(event.EventID), "eventType": string(event.EventType), "correlationId": nilIfEmpty(event.CorrelationID), "stepName": nilIfEmpty(event.StepName), "workflowName": event.WorkflowName, "deploymentId": event.DeploymentID, "specVersion": event.SpecVersion, "runCreatedAt": naive(event.RunCreatedAt), "createdAt": naive(event.CreatedAt), "resumeAt": naivePtr(event.ResumeAt), "retryAfter": naivePtr(event.RetryAfter), "errorCode": nilIfEmpty(event.ErrorCode)}
	if event.IsWebhook != nil {
		wire["isWebhook"] = *event.IsWebhook
	}
	if event.IsSystem != nil {
		wire["isSystem"] = *event.IsSystem
	}
	return wire
}

func wireAnalyticsHook(hook worlds.AnalyticsHook) map[string]any {
	return map[string]any{"runId": string(hook.RunID), "hookId": string(hook.HookID), "status": string(hook.Status), "createdAt": naive(hook.CreatedAt), "updatedAt": naive(hook.UpdatedAt), "receivedAt": naivePtr(hook.ReceivedAt), "disposedAt": naivePtr(hook.DisposedAt), "isWebhook": hook.IsWebhook, "isSystem": hook.IsSystem}
}

func wireAnalyticsWait(wait worlds.AnalyticsWait) map[string]any {
	return map[string]any{"runId": string(wait.RunID), "waitId": string(wait.WaitID), "status": string(wait.Status), "resumeAt": naivePtr(wait.ResumeAt), "createdAt": naive(wait.CreatedAt), "updatedAt": naive(wait.UpdatedAt), "completedAt": naivePtr(wait.CompletedAt)}
}
