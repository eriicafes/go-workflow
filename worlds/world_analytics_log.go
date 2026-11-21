package worlds

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

// AnalyticsSteps reads the steps of a run without their payloads.
type AnalyticsSteps interface {
	// StepsGet returns the step identified by RunID and StepID. It returns
	// ErrStepNotFound when the step does not exist.
	StepsGet(context.Context, AnalyticsStepsGetInput) (AnalyticsStep, error)
	// StepsList returns the steps of a run. It applies Pagination to the
	// result.
	StepsList(context.Context, AnalyticsStepsListInput) (Page[AnalyticsStep], error)
}

// AnalyticsEvents reads the events of a run without their payloads.
type AnalyticsEvents interface {
	// EventsGet returns the event identified by RunID and EventID. It returns
	// ErrEventNotFound when the event does not exist.
	EventsGet(context.Context, AnalyticsEventsGetInput) (AnalyticsEvent, error)
	// EventsGetMany returns the events of a run named by EventIDs, leaving
	// out the IDs no event has.
	EventsGetMany(context.Context, AnalyticsEventsGetManyInput) ([]AnalyticsEvent, error)
	// EventsList returns the events of a run that match the input filters.
	// It applies Pagination to the result.
	EventsList(context.Context, AnalyticsEventsListInput) (Page[AnalyticsEvent], error)
	// EventsListByCorrelationID returns the events of a run's step, hook, or
	// wait. It applies Pagination to the result.
	EventsListByCorrelationID(context.Context, AnalyticsEventsListByCorrelationIDInput) (Page[AnalyticsEvent], error)
}

// AnalyticsHooks reads the hooks of a run, disposed ones included.
type AnalyticsHooks interface {
	// HooksGet returns the hook identified by HookID. It returns
	// ErrHookNotFound when the run has no such hook.
	HooksGet(context.Context, AnalyticsHooksGetInput) (AnalyticsHook, error)
	// HooksList returns the hooks of a run. It applies Pagination to the
	// result.
	HooksList(context.Context, AnalyticsHooksListInput) (Page[AnalyticsHook], error)
}

type (
	// AnalyticsStepsGetInput holds the arguments to AnalyticsSteps.StepsGet.
	AnalyticsStepsGetInput struct {
		RunID  RunID
		StepID StepID
	}
	// AnalyticsStepsListInput holds the arguments to AnalyticsSteps.StepsList.
	AnalyticsStepsListInput struct {
		RunID      RunID
		Pagination Pagination
	}
	// AnalyticsEventsGetInput holds the arguments to AnalyticsEvents.EventsGet.
	AnalyticsEventsGetInput struct {
		RunID   RunID
		EventID EventID
	}
	// AnalyticsEventsGetManyInput holds the arguments to EventsGetMany.
	AnalyticsEventsGetManyInput struct {
		RunID    RunID
		EventIDs []EventID
	}
	// AnalyticsEventsListInput holds the arguments to
	// AnalyticsEvents.EventsList. Unset filters match every event.
	AnalyticsEventsListInput struct {
		RunID         RunID
		EventType     EventType
		CorrelationID string
		Pagination    Pagination
	}
	// AnalyticsEventsListByCorrelationIDInput holds the arguments to
	// AnalyticsEvents.EventsListByCorrelationID.
	AnalyticsEventsListByCorrelationIDInput struct {
		RunID         RunID
		CorrelationID string
		Pagination    Pagination
	}
	// AnalyticsHooksGetInput holds the arguments to AnalyticsHooks.HooksGet.
	// RunID is needed to find a disposed hook, whose view is gone.
	AnalyticsHooksGetInput struct {
		HookID HookID
		RunID  RunID
	}
	// AnalyticsHooksListInput holds the arguments to AnalyticsHooks.HooksList.
	AnalyticsHooksListInput struct {
		RunID      RunID
		Pagination Pagination
	}
)

// AnalyticsStep is the summary of one step, without its payloads.
type AnalyticsStep struct {
	RunID       RunID
	StepID      StepID
	StepName    string
	Status      Status
	Attempt     int
	CreatedAt   time.Time
	UpdatedAt   time.Time
	StartedAt   *time.Time
	CompletedAt *time.Time
	RetryAfter  *time.Time
}

// AnalyticsEvent is the summary of one event, without its payloads, with
// the run it belongs to.
type AnalyticsEvent struct {
	RunID         RunID
	EventID       EventID
	EventType     EventType
	CorrelationID string
	// StepName is set on step events.
	StepName     string
	WorkflowName string
	DeploymentID string
	SpecVersion  int
	RunCreatedAt time.Time
	CreatedAt    time.Time
	// ResumeAt is set on wait_created and wait_completed.
	ResumeAt *time.Time
	// RetryAfter is set on step_retrying.
	RetryAfter *time.Time
	// ErrorCode is set on run_failed.
	ErrorCode string
	// IsWebhook and IsSystem are set on hook_created.
	IsWebhook *bool
	IsSystem  *bool
}

// AnalyticsHookStatus is where a hook is in its life.
type AnalyticsHookStatus string

const (
	AnalyticsHookCreated  AnalyticsHookStatus = "created"
	AnalyticsHookReceived AnalyticsHookStatus = "received"
	AnalyticsHookDisposed AnalyticsHookStatus = "disposed"
	AnalyticsHookConflict AnalyticsHookStatus = "conflict"
)

// AnalyticsHook is the summary of one hook, read from its events, so a
// disposed hook is still reported.
type AnalyticsHook struct {
	RunID  RunID
	HookID HookID
	Status AnalyticsHookStatus
	// CreatedAt is the time of its hook_created or hook_conflict, and
	// UpdatedAt the time of its latest event.
	CreatedAt time.Time
	UpdatedAt time.Time
	// ReceivedAt is the time of its latest hook_received.
	ReceivedAt *time.Time
	DisposedAt *time.Time
	IsWebhook  bool
	IsSystem   bool
}

// LogAnalytics implements AnalyticsSteps, AnalyticsEvents, AnalyticsHooks,
// and AnalyticsWaits from a World's storage reads, as the Workflow SDK's
// analytics report them. A World embeds it in its Analytics.
type LogAnalytics struct{ World World }

func (a LogAnalytics) StepsGet(ctx context.Context, input AnalyticsStepsGetInput) (AnalyticsStep, error) {
	step, err := a.World.StepsGet(ctx, StepsGetInput{RunID: input.RunID, StepID: input.StepID, ResolveData: ResolveDataNone})
	return analyticsStep(step), err
}

func (a LogAnalytics) StepsList(ctx context.Context, input AnalyticsStepsListInput) (Page[AnalyticsStep], error) {
	steps, err := a.World.StepsList(ctx, StepsListInput{RunID: input.RunID, Pagination: input.Pagination, ResolveData: ResolveDataNone})
	if err != nil {
		return Page[AnalyticsStep]{}, err
	}
	page := Page[AnalyticsStep]{Data: make([]AnalyticsStep, len(steps.Data)), Cursor: steps.Cursor, HasMore: steps.HasMore}
	for i, step := range steps.Data {
		page.Data[i] = analyticsStep(step)
	}
	return page, nil
}

func analyticsStep(step Step) AnalyticsStep {
	return AnalyticsStep{RunID: step.RunID, StepID: step.StepID, StepName: step.StepName, Status: step.Status, Attempt: step.Attempt, CreatedAt: step.CreatedAt, UpdatedAt: step.UpdatedAt, StartedAt: step.StartedAt, CompletedAt: step.CompletedAt, RetryAfter: step.RetryAfter}
}

func (a LogAnalytics) EventsGet(ctx context.Context, input AnalyticsEventsGetInput) (AnalyticsEvent, error) {
	event, err := a.World.EventsGet(ctx, EventsGetInput{RunID: input.RunID, EventID: input.EventID, ResolveData: EventsResolveDataNone})
	if err != nil {
		return AnalyticsEvent{}, err
	}
	run, err := a.run(ctx, input.RunID)
	if err != nil {
		return AnalyticsEvent{}, err
	}
	return analyticsEvent(run, event), nil
}

func (a LogAnalytics) EventsGetMany(ctx context.Context, input AnalyticsEventsGetManyInput) ([]AnalyticsEvent, error) {
	run, err := a.run(ctx, input.RunID)
	if err != nil {
		return nil, err
	}
	events := make([]AnalyticsEvent, 0, len(input.EventIDs))
	for _, id := range input.EventIDs {
		event, err := a.World.EventsGet(ctx, EventsGetInput{RunID: input.RunID, EventID: id, ResolveData: EventsResolveDataNone})
		if errors.Is(err, ErrEventNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		events = append(events, analyticsEvent(run, event))
	}
	return events, nil
}

func (a LogAnalytics) EventsList(ctx context.Context, input AnalyticsEventsListInput) (Page[AnalyticsEvent], error) {
	if input.CorrelationID != "" {
		events, err := a.World.EventsListByCorrelationID(ctx, EventsListByCorrelationIDInput{RunID: input.RunID, CorrelationID: input.CorrelationID, ResolveData: EventsResolveDataNone})
		if err != nil {
			return Page[AnalyticsEvent]{}, err
		}
		return a.eventPage(ctx, input.RunID, allEvents(ctx, a.World, input.RunID, input.CorrelationID, events), input.EventType, input.Pagination)
	}
	if input.EventType == "" {
		events, err := a.World.EventsList(ctx, EventsListInput{RunID: input.RunID, Pagination: input.Pagination, ResolveData: EventsResolveDataNone})
		if err != nil {
			return Page[AnalyticsEvent]{}, err
		}
		return a.mapPage(ctx, input.RunID, events.Page)
	}
	events, err := a.log(ctx, input.RunID)
	if err != nil {
		return Page[AnalyticsEvent]{}, err
	}
	return a.eventPage(ctx, input.RunID, func() ([]Event, error) { return events, nil }, input.EventType, input.Pagination)
}

func (a LogAnalytics) EventsListByCorrelationID(ctx context.Context, input AnalyticsEventsListByCorrelationIDInput) (Page[AnalyticsEvent], error) {
	events, err := a.World.EventsListByCorrelationID(ctx, EventsListByCorrelationIDInput{RunID: input.RunID, CorrelationID: input.CorrelationID, Pagination: input.Pagination, ResolveData: EventsResolveDataNone})
	if err != nil {
		return Page[AnalyticsEvent]{}, err
	}
	return a.mapPage(ctx, input.RunID, events)
}

// allEvents returns a reader of every event of a correlation, starting from
// first, its first page.
func allEvents(ctx context.Context, world World, runID RunID, correlationID string, first Page[Event]) func() ([]Event, error) {
	return func() ([]Event, error) {
		events := first.Data
		for page := first; page.HasMore; {
			var err error
			page, err = world.EventsListByCorrelationID(ctx, EventsListByCorrelationIDInput{RunID: runID, CorrelationID: correlationID, Pagination: Pagination{Cursor: page.Cursor}, ResolveData: EventsResolveDataNone})
			if err != nil {
				return nil, err
			}
			events = append(events, page.Data...)
		}
		return events, nil
	}
}

// eventPage pages the events read returns that have eventType, or every
// one when eventType is empty, in log order.
func (a LogAnalytics) eventPage(ctx context.Context, runID RunID, read func() ([]Event, error), eventType EventType, pagination Pagination) (Page[AnalyticsEvent], error) {
	events, err := read()
	if err != nil {
		return Page[AnalyticsEvent]{}, err
	}
	run, err := a.run(ctx, runID)
	if err != nil {
		return Page[AnalyticsEvent]{}, err
	}
	matched := make([]AnalyticsEvent, 0, len(events))
	for _, event := range events {
		if eventType == "" || event.EventType == eventType {
			matched = append(matched, analyticsEvent(run, event))
		}
	}
	return keyPage(matched, pagination, func(event AnalyticsEvent) string { return string(event.EventID) })
}

func (a LogAnalytics) mapPage(ctx context.Context, runID RunID, events Page[Event]) (Page[AnalyticsEvent], error) {
	run, err := a.run(ctx, runID)
	if err != nil {
		return Page[AnalyticsEvent]{}, err
	}
	page := Page[AnalyticsEvent]{Data: make([]AnalyticsEvent, len(events.Data)), Cursor: events.Cursor, HasMore: events.HasMore}
	for i, event := range events.Data {
		page.Data[i] = analyticsEvent(run, event)
	}
	return page, nil
}

func (a LogAnalytics) run(ctx context.Context, runID RunID) (Run, error) {
	return a.World.RunsGet(ctx, RunsGetInput{RunID: runID, ResolveData: ResolveDataNone})
}

// log returns every event of a run, in order, without payloads.
func (a LogAnalytics) log(ctx context.Context, runID RunID) ([]Event, error) {
	// A missing run's events list is empty: the run is read for its error.
	if _, err := a.run(ctx, runID); err != nil {
		return nil, err
	}
	var events []Event
	pagination := Pagination{}
	for {
		page, err := a.World.EventsList(ctx, EventsListInput{RunID: runID, Pagination: pagination, ResolveData: EventsResolveDataNone})
		if err != nil {
			return nil, err
		}
		events = append(events, page.Data...)
		if !page.HasMore {
			return events, nil
		}
		pagination.Cursor = page.Cursor
	}
}

func analyticsEvent(run Run, event Event) AnalyticsEvent {
	summary := AnalyticsEvent{RunID: event.RunID, EventID: event.EventID, EventType: event.EventType, CorrelationID: event.CorrelationID, WorkflowName: run.WorkflowName, DeploymentID: run.DeploymentID, SpecVersion: event.SpecVersion, RunCreatedAt: run.CreatedAt, CreatedAt: event.CreatedAt}
	if len(event.Data) == 0 {
		return summary
	}
	switch event.EventType {
	case EventTypeStepCreated:
		var created DataStepCreated
		_ = json.Unmarshal(event.Data, &created)
		summary.StepName = created.StepName
	case EventTypeStepStarted:
		var started DataStepStarted
		_ = json.Unmarshal(event.Data, &started)
		summary.StepName = started.StepName
	case EventTypeStepCompleted, EventTypeStepFailed, EventTypeStepRetrying:
		var result DataStepResult
		_ = json.Unmarshal(event.Data, &result)
		summary.StepName, summary.RetryAfter = result.StepName, result.RetryAfter
	case EventTypeWaitCreated:
		var created DataWaitCreated
		if json.Unmarshal(event.Data, &created) == nil && !created.ResumeAt.IsZero() {
			summary.ResumeAt = &created.ResumeAt
		}
	case EventTypeWaitCompleted:
		var completed DataWaitCompleted
		_ = json.Unmarshal(event.Data, &completed)
		summary.ResumeAt = completed.ResumeAt
	case EventTypeHookCreated:
		var created DataHookCreated
		if json.Unmarshal(event.Data, &created) == nil {
			summary.IsWebhook, summary.IsSystem = &created.IsWebhook, &created.IsSystem
		}
	case EventTypeRunFailed:
		var failed DataRunFailed
		_ = json.Unmarshal(event.Data, &failed)
		summary.ErrorCode = failed.ErrorCode
	}
	return summary
}

func (a LogAnalytics) HooksGet(ctx context.Context, input AnalyticsHooksGetInput) (AnalyticsHook, error) {
	runID := input.RunID
	if runID == "" {
		hook, err := a.World.HooksGet(ctx, HooksGetInput{HookID: input.HookID, ResolveData: ResolveDataNone})
		if err != nil {
			return AnalyticsHook{}, err
		}
		runID = hook.RunID
	}
	first, err := a.World.EventsListByCorrelationID(ctx, EventsListByCorrelationIDInput{RunID: runID, CorrelationID: string(input.HookID), ResolveData: EventsResolveDataNone})
	if err != nil {
		return AnalyticsHook{}, err
	}
	events, err := allEvents(ctx, a.World, runID, string(input.HookID), first)()
	if err != nil {
		return AnalyticsHook{}, err
	}
	hooks := analyticsHooks(events)
	if len(hooks) == 0 {
		return AnalyticsHook{}, ErrHookNotFound
	}
	return hooks[0], nil
}

func (a LogAnalytics) HooksList(ctx context.Context, input AnalyticsHooksListInput) (Page[AnalyticsHook], error) {
	events, err := a.log(ctx, input.RunID)
	if err != nil {
		return Page[AnalyticsHook]{}, err
	}
	hooks := analyticsHooks(events)
	return keyPage(hooks, input.Pagination, func(hook AnalyticsHook) string {
		return hook.CreatedAt.UTC().Format(time.RFC3339Nano) + "/" + string(hook.HookID)
	})
}

// analyticsHooks folds hook events, in log order, into their hooks, in the
// order they were created.
func analyticsHooks(events []Event) []AnalyticsHook {
	var hooks []AnalyticsHook
	index := map[string]int{}
	for _, event := range events {
		switch event.EventType {
		case EventTypeHookCreated, EventTypeHookConflict:
			hook := AnalyticsHook{RunID: event.RunID, HookID: HookID(event.CorrelationID), Status: AnalyticsHookCreated, CreatedAt: event.CreatedAt, UpdatedAt: event.CreatedAt}
			if event.EventType == EventTypeHookConflict {
				hook.Status = AnalyticsHookConflict
			}
			var created DataHookCreated
			if json.Unmarshal(event.Data, &created) == nil {
				hook.IsWebhook, hook.IsSystem = created.IsWebhook, created.IsSystem
			}
			index[event.CorrelationID] = len(hooks)
			hooks = append(hooks, hook)
		case EventTypeHookReceived, EventTypeHookDisposed:
			i, found := index[event.CorrelationID]
			if !found {
				continue
			}
			hook := &hooks[i]
			at := event.CreatedAt
			hook.UpdatedAt = at
			if event.EventType == EventTypeHookReceived {
				hook.ReceivedAt = &at
				if hook.Status == AnalyticsHookCreated {
					hook.Status = AnalyticsHookReceived
				}
			} else {
				hook.DisposedAt, hook.Status = &at, AnalyticsHookDisposed
			}
		}
	}
	return hooks
}

// keyPage pages values ordered by key, with a cursor that encodes the key
// of the page's last value.
func keyPage[T any](values []T, pagination Pagination, key func(T) string) (Page[T], error) {
	descending := pagination.SortOrder == SortOrderDescending
	slices.SortStableFunc(values, func(a, b T) int {
		if descending {
			return strings.Compare(key(b), key(a))
		}
		return strings.Compare(key(a), key(b))
	})
	start := 0
	if pagination.Cursor != "" {
		raw, err := base64.RawURLEncoding.DecodeString(pagination.Cursor)
		if err != nil {
			return Page[T]{}, fmt.Errorf("%w: invalid cursor %q", ErrEventInvalid, pagination.Cursor)
		}
		after := string(raw)
		start = len(values)
		for i, value := range values {
			if compare := strings.Compare(key(value), after); descending && compare < 0 || !descending && compare > 0 {
				start = i
				break
			}
		}
	}
	end := len(values)
	if pagination.Limit > 0 {
		end = min(start+pagination.Limit, len(values))
	}
	page := Page[T]{Data: values[start:end], HasMore: end < len(values)}
	if page.Data == nil {
		page.Data = []T{}
	}
	// The last page's cursor resumes after it, as the Workflow SDK's Worlds'
	// lists do.
	if end > start {
		page.Cursor = base64.RawURLEncoding.EncodeToString([]byte(key(values[end-1])))
	}
	return page, nil
}

func (a LogAnalytics) WaitsGet(ctx context.Context, input AnalyticsWaitsGetInput) (AnalyticsWait, error) {
	waits, err := a.waits(ctx, input.RunID)
	if err != nil {
		return AnalyticsWait{}, err
	}
	for _, wait := range waits {
		if wait.WaitID == input.WaitID {
			return wait, nil
		}
	}
	return AnalyticsWait{}, ErrEventNotFound
}

func (a LogAnalytics) WaitsList(ctx context.Context, input AnalyticsWaitsListInput) (Page[AnalyticsWait], error) {
	waits, err := a.waits(ctx, input.RunID)
	if err != nil {
		return Page[AnalyticsWait]{}, err
	}
	if input.Status != "" {
		waits = slices.DeleteFunc(waits, func(wait AnalyticsWait) bool { return wait.Status != input.Status })
	}
	return keyPage(waits, input.Pagination, func(wait AnalyticsWait) string {
		return wait.CreatedAt.UTC().Format(time.RFC3339Nano) + "/" + string(wait.WaitID)
	})
}

// waits folds a run's wait events, in log order, into its waits.
func (a LogAnalytics) waits(ctx context.Context, runID RunID) ([]AnalyticsWait, error) {
	events, err := a.log(ctx, runID)
	if err != nil {
		return nil, err
	}
	var waits []AnalyticsWait
	index := map[string]int{}
	for _, event := range events {
		switch event.EventType {
		case EventTypeWaitCreated:
			wait := AnalyticsWait{RunID: runID, WaitID: WaitID(event.CorrelationID), Status: WaitStatusWaiting, CreatedAt: event.CreatedAt, UpdatedAt: event.CreatedAt}
			var created DataWaitCreated
			if json.Unmarshal(event.Data, &created) == nil && !created.ResumeAt.IsZero() {
				wait.ResumeAt = &created.ResumeAt
			}
			index[event.CorrelationID] = len(waits)
			waits = append(waits, wait)
		case EventTypeWaitCompleted:
			if i, found := index[event.CorrelationID]; found && waits[i].CompletedAt == nil {
				at := event.CreatedAt
				waits[i].Status, waits[i].UpdatedAt, waits[i].CompletedAt = WaitStatusCompleted, at, &at
			}
		}
	}
	return waits, nil
}
