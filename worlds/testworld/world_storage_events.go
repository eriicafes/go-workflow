package testworld

import (
	"bytes"
	"context"
	"encoding/json"
	"maps"
	"sort"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

func (m *World) EventsCreate(ctx context.Context, input worlds.EventsCreateInput) (worlds.EventResult, error) {
	if err := ctx.Err(); err != nil {
		return worlds.EventResult{}, err
	}
	if input.Event.EventType == worlds.EventTypeRunCreated || !input.Event.EventType.Valid() {
		return worlds.EventResult{}, worlds.ErrEventInvalid
	}
	defer m.notify()
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.runs[input.RunID]; !ok {
		return worlds.EventResult{}, worlds.ErrRunNotFound
	}
	if err := m.fenceLocked(input.RunID, input.Options.InBandCount); err != nil {
		return worlds.EventResult{}, err
	}
	if result, duplicate, err := m.admitLocked(input.RunID, input.Event); err != nil || duplicate {
		if duplicate {
			result.Events = resolveEventData(m.deltaLocked(input.RunID, input.Options.EventCount, input.Options.SinceCursor), input.Options.ResolveData)
			result.InBandCount = m.runs[input.RunID].InBandCount
		}
		return result, err
	}
	event := worlds.Event{RunID: input.RunID, EventID: worlds.EventIDForSlot(uint64(len(m.events[input.RunID]) + 1)), EventType: input.Event.EventType, CorrelationID: input.Event.CorrelationID, Data: bytes.Clone(input.Event.Data), CreatedAt: time.Now().UTC()}
	before := resolveEventData(m.deltaLocked(input.RunID, input.Options.EventCount, input.Options.SinceCursor), input.Options.ResolveData)
	m.events[input.RunID] = append(m.events[input.RunID], event)
	m.materializeLocked(event)
	result := m.eventResultLocked(event, before, true)
	result.InBandCount = m.advanceLocked(input.RunID, input.Options.InBandCount, 1)
	return result, nil
}

// fenceLocked returns ErrInBandSuperseded when an in-band write's count is
// not the run's current InBandCount.
func (m *World) fenceLocked(runID worlds.RunID, count *int) error {
	if count != nil && *count != m.runs[runID].InBandCount {
		return worlds.ErrInBandSuperseded
	}
	return nil
}

// advanceLocked adds appended to the run's InBandCount for an in-band write
// and returns the count.
func (m *World) advanceLocked(runID worlds.RunID, count *int, appended int) int {
	run := m.runs[runID]
	if count != nil {
		run.InBandCount += appended
		m.runs[runID] = run
	}
	return run.InBandCount
}

func (m *World) EventsCreateRun(ctx context.Context, input worlds.EventsCreateRunInput) (worlds.EventResult, error) {
	if err := ctx.Err(); err != nil {
		return worlds.EventResult{}, err
	}
	defer m.notify()
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.createRunLocked(input)
}

func (m *World) createRunLocked(input worlds.EventsCreateRunInput) (worlds.EventResult, error) {
	if input.RunID == "" {
		return worlds.EventResult{}, worlds.ErrEventInvalid
	}
	if _, exists := m.runs[input.RunID]; exists {
		return worlds.EventResult{}, worlds.ErrRunAlreadyExists
	}
	now := time.Now().UTC()
	payload, err := json.Marshal(runCreatedData{WorkflowName: input.WorkflowName, Input: input.Input, Attributes: input.Attributes, DeploymentID: input.DeploymentID})
	if err != nil {
		return worlds.EventResult{}, err
	}
	run := worlds.Run{RunID: input.RunID, WorkflowName: input.WorkflowName, Status: worlds.StatusPending, Input: bytes.Clone(input.Input), Attributes: maps.Clone(input.Attributes), DeploymentID: input.DeploymentID, CreatedAt: now, UpdatedAt: now}
	if run.Attributes == nil {
		run.Attributes = make(map[string]string)
	}
	m.runs[input.RunID] = run
	m.events[input.RunID] = []worlds.Event{{RunID: input.RunID, EventID: worlds.EventIDForSlot(1), EventType: worlds.EventTypeRunCreated, Data: payload, CreatedAt: now}}
	return worlds.EventResult{EventID: worlds.EventIDForSlot(1), EventType: worlds.EventTypeRunCreated, AllocatedEventCount: 1}, nil
}

type runCreatedData struct {
	WorkflowName string            `json:"workflowName"`
	Input        json.RawMessage   `json:"input"`
	Attributes   map[string]string `json:"attributes"`
	DeploymentID string            `json:"deploymentId,omitempty"`
}

func (m *World) EventsCreateBatch(ctx context.Context, input worlds.EventsCreateBatchInput) (worlds.EventBatchResult, error) {
	if len(input.Events) == 0 {
		return worlds.EventBatchResult{}, nil
	}
	defer m.notify()
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.runs[input.RunID]; !ok {
		return worlds.EventBatchResult{}, worlds.ErrRunNotFound
	}
	if err := m.fenceLocked(input.RunID, input.InBandCount); err != nil {
		return worlds.EventBatchResult{}, err
	}
	checkpoint := m.checkpointLocked()
	result := worlds.EventBatchResult{Events: make([]worlds.EventResult, 0, len(input.Events))}
	for _, value := range input.Events {
		if value.EventType == worlds.EventTypeRunCreated || !value.EventType.Valid() {
			m.restoreLocked(checkpoint)
			return worlds.EventBatchResult{}, worlds.ErrEventInvalid
		}
		if _, duplicate, err := m.admitLocked(input.RunID, value); err != nil || duplicate {
			m.restoreLocked(checkpoint)
			if err != nil {
				return worlds.EventBatchResult{}, err
			}
			return worlds.EventBatchResult{}, worlds.ErrEventInvalid
		}
		event := worlds.Event{RunID: input.RunID, EventID: worlds.EventIDForSlot(uint64(len(m.events[input.RunID]) + 1)), EventType: value.EventType, CorrelationID: value.CorrelationID, Data: bytes.Clone(value.Data), CreatedAt: time.Now().UTC()}
		m.events[input.RunID] = append(m.events[input.RunID], event)
		m.materializeLocked(event)
		result.Events = append(result.Events, m.eventResultLocked(event, nil, true))
	}
	result.InBandCount = m.advanceLocked(input.RunID, input.InBandCount, len(input.Events))
	return result, nil
}

func (m *World) EventsGet(ctx context.Context, input worlds.EventsGetInput) (worlds.Event, error) {
	if err := ctx.Err(); err != nil {
		return worlds.Event{}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, event := range m.events[input.RunID] {
		if event.EventID == input.EventID {
			result := cloneEvent(event)
			if input.ResolveData == worlds.EventsResolveDataNone {
				result.Data = nil
			}
			return result, nil
		}
	}
	if _, ok := m.runs[input.RunID]; !ok {
		return worlds.Event{}, worlds.ErrRunNotFound
	}
	return worlds.Event{}, worlds.ErrEventNotFound
}

func (m *World) HooksClaim(ctx context.Context, input worlds.HooksClaimInput) (worlds.HooksClaimResult, error) {
	// Serialize token ownership and record a forced takeover in the previous
	// owner's run before making the token visible to the claimant.
	if err := ctx.Err(); err != nil {
		return worlds.HooksClaimResult{}, err
	}
	if input.Event.EventType != worlds.EventTypeHookCreated {
		return worlds.HooksClaimResult{}, worlds.ErrEventInvalid
	}
	var created hookCreatedData
	if err := json.Unmarshal(input.Event.Data, &created); err != nil || created.Token == "" {
		return worlds.HooksClaimResult{}, worlds.ErrEventInvalid
	}
	defer m.notify()
	m.mu.Lock()
	defer m.mu.Unlock()
	claimant, ok := m.runs[input.RunID]
	if !ok {
		return worlds.HooksClaimResult{}, worlds.ErrRunNotFound
	}
	if claimant.Status.Terminal() {
		return worlds.HooksClaimResult{}, worlds.ErrRunTerminal
	}
	if err := m.fenceLocked(input.RunID, input.Options.InBandCount); err != nil {
		return worlds.HooksClaimResult{}, err
	}
	m.expireTokenLocked(created.Token, time.Now().UTC())
	result := worlds.HooksClaimResult{EventResult: worlds.EventResult{Events: m.deltaLocked(input.RunID, input.Options.EventCount, input.Options.SinceCursor)}}
	if ownerID := m.tokens[created.Token]; ownerID != "" {
		owner := m.hooks[ownerID]
		if !input.Force {
			return worlds.HooksClaimResult{}, worlds.ErrHookConflict
		}
		if m.runs[owner.RunID].Status.Terminal() {
			delete(m.tokens, created.Token)
		} else {
			data, err := json.Marshal(worlds.HookDisposedEventData{ForceClaimedBy: &worlds.HookForceClaim{RunID: input.RunID, HookID: worlds.HookID(input.Event.CorrelationID)}})
			if err != nil {
				return worlds.HooksClaimResult{}, err
			}
			forced := worlds.Event{RunID: owner.RunID, EventID: worlds.EventIDForSlot(uint64(len(m.events[owner.RunID]) + 1)), EventType: worlds.EventTypeHookDisposed, CorrelationID: string(ownerID), Data: data, CreatedAt: time.Now().UTC()}
			m.events[owner.RunID] = append(m.events[owner.RunID], forced)
			m.materializeLocked(forced)
			result.ForcedRunID = owner.RunID
		}
	}
	event := worlds.Event{RunID: input.RunID, EventID: worlds.EventIDForSlot(uint64(len(m.events[input.RunID]) + 1)), EventType: input.Event.EventType, CorrelationID: input.Event.CorrelationID, Data: bytes.Clone(input.Event.Data), CreatedAt: time.Now().UTC()}
	m.events[input.RunID] = append(m.events[input.RunID], event)
	m.materializeLocked(event)
	result.EventResult = m.eventResultLocked(event, result.Events, true)
	result.InBandCount = m.advanceLocked(input.RunID, input.Options.InBandCount, 1)
	return result, nil
}

type memoryCheckpoint struct {
	runs   map[worlds.RunID]worlds.Run
	events map[worlds.RunID][]worlds.Event
	steps  map[worlds.RunID]map[worlds.StepID]worlds.Step
	hooks  map[worlds.HookID]worlds.Hook
	tokens map[string]worlds.HookID
}

func (m *World) checkpointLocked() memoryCheckpoint {
	checkpoint := memoryCheckpoint{runs: make(map[worlds.RunID]worlds.Run, len(m.runs)), events: make(map[worlds.RunID][]worlds.Event, len(m.events)), steps: make(map[worlds.RunID]map[worlds.StepID]worlds.Step, len(m.steps)), hooks: make(map[worlds.HookID]worlds.Hook, len(m.hooks)), tokens: make(map[string]worlds.HookID, len(m.tokens))}
	for id, run := range m.runs {
		checkpoint.runs[id] = cloneRun(run)
	}
	for id, events := range m.events {
		checkpoint.events[id] = append([]worlds.Event(nil), events...)
	}
	for runID, steps := range m.steps {
		checkpoint.steps[runID] = make(map[worlds.StepID]worlds.Step, len(steps))
		for id, step := range steps {
			checkpoint.steps[runID][id] = step
		}
	}
	for id, hook := range m.hooks {
		checkpoint.hooks[id] = hook
	}
	for token, id := range m.tokens {
		checkpoint.tokens[token] = id
	}
	return checkpoint
}

func (m *World) restoreLocked(checkpoint memoryCheckpoint) {
	m.runs, m.events, m.steps, m.hooks, m.tokens = checkpoint.runs, checkpoint.events, checkpoint.steps, checkpoint.hooks, checkpoint.tokens
}

// admitLocked implements the state-based queue fence. A duplicate start is
// inert and returns the previously committed lifecycle event instead of
// granting another execution.
func (m *World) admitLocked(runID worlds.RunID, requested worlds.EventsCreateEvent) (worlds.EventResult, bool, error) {
	run := m.runs[runID]
	if run.Status.Terminal() {
		if requested.EventType == worlds.EventTypeRunCancelled && run.Status == worlds.StatusCancelled {
			return m.lastResultLocked(runID, requested.EventType, requested.CorrelationID), true, nil
		}
		return worlds.EventResult{}, false, worlds.ErrRunTerminal
	}
	if requested.EventType == worlds.EventTypeRunStarted && run.Status == worlds.StatusRunning {
		return m.lastResultLocked(runID, requested.EventType, ""), true, nil
	}
	if requested.EventType == worlds.EventTypeStepStarted {
		step := m.steps[runID][worlds.StepID(requested.CorrelationID)]
		if step.StepID == "" {
			return worlds.EventResult{}, false, worlds.ErrStepNotStartable
		}
		// A running step may be started again, because its previous attempt's
		// delivery may have died, and each start records a new attempt.
		if step.Status != worlds.StatusPending && step.Status != worlds.StatusRunning {
			return worlds.EventResult{}, false, worlds.ErrStepNotStartable
		}
	}
	if requested.EventType == worlds.EventTypeStepCancelled {
		step := m.steps[runID][worlds.StepID(requested.CorrelationID)]
		if step.StepID == "" {
			return worlds.EventResult{}, false, worlds.ErrStepNotStartable
		}
		if step.Status == worlds.StatusCancelled {
			return m.lastResultLocked(runID, requested.EventType, requested.CorrelationID), true, nil
		}
		if step.Status.Terminal() {
			return worlds.EventResult{}, false, worlds.ErrStepNotStartable
		}
	}
	if requested.EventType == worlds.EventTypeWaitCompleted {
		for _, event := range m.events[runID] {
			if event.EventType == worlds.EventTypeWaitCompleted && event.CorrelationID == requested.CorrelationID {
				return worlds.EventResult{EventID: event.EventID, EventType: event.EventType}, true, nil
			}
		}
	}
	if requested.EventType == worlds.EventTypeHookReceived {
		var received worlds.WebhookReceivedEventData
		if json.Unmarshal(requested.Data, &received) == nil && received.Request != nil {
			hook := m.hooks[worlds.HookID(requested.CorrelationID)]
			if hook.Webhook == nil || hook.ReceivedEventID != "" || m.tokens[hook.Token] != hook.HookID {
				return worlds.EventResult{}, false, worlds.ErrHookNotActive
			}
		}
		if json.Unmarshal(requested.Data, &received) == nil && received.Response != nil {
			hook := m.hooks[worlds.HookID(requested.CorrelationID)]
			if hook.Webhook == nil || hook.ReceivedEventID == "" || received.ResponseTo != hook.ReceivedEventID || hook.RetainUntil != nil || m.tokens[hook.Token] != hook.HookID {
				return worlds.EventResult{}, false, worlds.ErrHookNotActive
			}
		}
	}
	if requested.EventType == worlds.EventTypeHookCreated {
		var created hookCreatedData
		if json.Unmarshal(requested.Data, &created) == nil {
			m.expireTokenLocked(created.Token, time.Now().UTC())
			if _, exists := m.tokens[created.Token]; exists {
				return worlds.EventResult{}, false, worlds.ErrHookConflict
			}
		}
	}
	return worlds.EventResult{}, false, nil
}

func (m *World) lastResultLocked(runID worlds.RunID, eventType worlds.EventType, correlation string) worlds.EventResult {
	for index := len(m.events[runID]) - 1; index >= 0; index-- {
		event := m.events[runID][index]
		if event.EventType == eventType && (correlation == "" || event.CorrelationID == correlation) {
			return m.eventResultLocked(event, nil, false)
		}
	}
	return worlds.EventResult{}
}

func (m *World) eventResultLocked(event worlds.Event, events []worlds.Event, created bool) worlds.EventResult {
	result := worlds.EventResult{EventID: event.EventID, EventType: event.EventType, Events: events}
	if created {
		result.AllocatedEventCount = 1
	}
	if event.EventType != worlds.EventTypeStepCreated {
		return result
	}
	step := m.steps[event.RunID][worlds.StepID(event.CorrelationID)]
	if step.StepID == "" {
		return result
	}
	step.Input, step.Output, step.Error = bytes.Clone(step.Input), bytes.Clone(step.Output), bytes.Clone(step.Error)
	result.Step = &step
	result.StepCreated = created
	return result
}

func (m *World) deltaLocked(runID worlds.RunID, count *int, cursor string) []worlds.Event {
	log := m.events[runID]
	start := len(log)
	if count != nil {
		start = *count
		if start < 0 {
			start = 0
		}
		if start > len(log) {
			start = len(log)
		}
	} else if cursor != "" {
		if slot, err := worlds.EventSlot(worlds.EventID(cursor)); err == nil {
			start = int(slot)
		}
	}
	result := make([]worlds.Event, 0, len(log)-start)
	for _, event := range log[start:] {
		result = append(result, cloneEvent(event))
	}
	return result
}

type attrSetData struct {
	Changes []worlds.RunsAttributeChange `json:"changes"`
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

func (m *World) materializeLocked(event worlds.Event) {
	run := m.runs[event.RunID]
	now := event.CreatedAt
	run.UpdatedAt = now
	switch event.EventType {
	case worlds.EventTypeRunStarted:
		run.Status, run.StartedAt = worlds.StatusRunning, &now
	case worlds.EventTypeRunCompleted:
		run.Status, run.CompletedAt, run.Output = worlds.StatusCompleted, &now, bytes.Clone(event.Data)
	case worlds.EventTypeRunFailed:
		run.Status, run.CompletedAt, run.Error = worlds.StatusFailed, &now, bytes.Clone(event.Data)
	case worlds.EventTypeRunCancelled:
		run.Status, run.CompletedAt = worlds.StatusCancelled, &now
	case worlds.EventTypeAttrSet:
		var value attrSetData
		if json.Unmarshal(event.Data, &value) == nil {
			for _, change := range value.Changes {
				if change.Value == nil {
					delete(run.Attributes, change.Key)
				} else {
					run.Attributes[change.Key] = *change.Value
				}
			}
		}
	case worlds.EventTypeStepCreated:
		var value stepCreatedData
		_ = json.Unmarshal(event.Data, &value)
		if m.steps[event.RunID] == nil {
			m.steps[event.RunID] = make(map[worlds.StepID]worlds.Step)
		}
		id := worlds.StepID(event.CorrelationID)
		m.steps[event.RunID][id] = worlds.Step{RunID: event.RunID, StepID: id, StepName: value.StepName, Status: worlds.StatusPending, Input: bytes.Clone(value.Input), CreatedAt: now, UpdatedAt: now}
	case worlds.EventTypeStepStarted:
		m.updateStepLocked(event, worlds.StatusRunning)
	case worlds.EventTypeStepCompleted:
		m.updateStepLocked(event, worlds.StatusCompleted)
	case worlds.EventTypeStepFailed:
		m.updateStepLocked(event, worlds.StatusFailed)
	case worlds.EventTypeStepRetrying:
		m.updateStepLocked(event, worlds.StatusPending)
	case worlds.EventTypeStepCancelled:
		m.updateStepLocked(event, worlds.StatusCancelled)
	case worlds.EventTypeHookCreated:
		var value hookCreatedData
		_ = json.Unmarshal(event.Data, &value)
		id := worlds.HookID(event.CorrelationID)
		hook := worlds.Hook{RunID: event.RunID, HookID: id, Token: value.Token, Metadata: bytes.Clone(value.Metadata), Webhook: cloneWebhook(value.Webhook), MinRetention: value.MinRetention, CreatedAt: now, UpdatedAt: now}
		m.hooks[id], m.tokens[value.Token] = hook, id
	case worlds.EventTypeHookReceived:
		id := worlds.HookID(event.CorrelationID)
		hook := m.hooks[id]
		hook.UpdatedAt, hook.ReceivedAt = now, &now
		var value worlds.WebhookReceivedEventData
		if json.Unmarshal(event.Data, &value) == nil {
			if value.Request != nil {
				hook.ReceivedEventID = event.EventID
			}
			if value.Response != nil && value.ResponseTo == hook.ReceivedEventID {
				m.completeHookLocked(&hook, now)
			}
		}
		if hook.Webhook == nil || (hook.Webhook != nil && !hook.Webhook.Manual && hook.ReceivedEventID != "") {
			m.completeHookLocked(&hook, now)
		}
		m.hooks[id] = hook
	case worlds.EventTypeHookDisposed:
		id := worlds.HookID(event.CorrelationID)
		hook := m.hooks[id]
		if m.tokens[hook.Token] == id {
			delete(m.tokens, hook.Token)
		}
	}
	m.runs[event.RunID] = run
}

func (m *World) completeHookLocked(hook *worlds.Hook, now time.Time) {
	if hook.MinRetention <= 0 {
		if m.tokens[hook.Token] == hook.HookID {
			delete(m.tokens, hook.Token)
		}
		hook.RetainUntil = nil
		return
	}
	until := now.Add(hook.MinRetention)
	hook.RetainUntil = &until
}

func (m *World) expireTokenLocked(token string, now time.Time) {
	id := m.tokens[token]
	if id == "" {
		return
	}
	hook := m.hooks[id]
	if hook.RetainUntil != nil && !hook.RetainUntil.After(now) {
		delete(m.tokens, token)
		hook.RetainUntil = nil
		m.hooks[id] = hook
	}
}

func (m *World) updateStepLocked(event worlds.Event, status worlds.Status) {
	step := m.steps[event.RunID][worlds.StepID(event.CorrelationID)]
	if step.StepID == "" {
		return
	}
	step.Status, step.UpdatedAt = status, event.CreatedAt
	if status == worlds.StatusRunning {
		step.Attempt++
		step.StartedAt = &event.CreatedAt
	}
	if status == worlds.StatusCompleted || status == worlds.StatusFailed {
		step.CompletedAt = &event.CreatedAt
	}
	var result stepResultData
	if json.Unmarshal(event.Data, &result) == nil {
		step.Output, step.Error, step.RetryAfter = bytes.Clone(result.Output), bytes.Clone(result.Error), result.RetryAfter
	}
	m.steps[event.RunID][step.StepID] = step
}

func (m *World) EventsList(ctx context.Context, input worlds.EventsListInput) (worlds.Page[worlds.Event], error) {
	if err := ctx.Err(); err != nil {
		return worlds.Page[worlds.Event]{}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	log, ok := m.events[input.RunID]
	if !ok {
		return worlds.Page[worlds.Event]{}, worlds.ErrRunNotFound
	}
	values := make([]worlds.Event, len(log))
	for i := range log {
		values[i] = cloneEvent(log[i])
	}
	if input.Pagination.SortOrder == worlds.SortOrderDescending {
		for i, j := 0, len(values)-1; i < j; i, j = i+1, j-1 {
			values[i], values[j] = values[j], values[i]
		}
	}
	if input.ResolveData == worlds.EventsResolveDataNone {
		for index := range values {
			values[index].Data = nil
		}
	}
	if input.Pagination.SortOrder == worlds.SortOrderDescending {
		return page(values, input.Pagination), nil
	}
	return eventPage(values, input.Pagination), nil
}

func (m *World) EventsListByCorrelationID(ctx context.Context, input worlds.EventsListByCorrelationIDInput) (worlds.Page[worlds.Event], error) {
	all, err := m.EventsList(ctx, worlds.EventsListInput{RunID: input.RunID, ResolveData: input.ResolveData})
	if err != nil {
		return worlds.Page[worlds.Event]{}, err
	}
	filtered := make([]worlds.Event, 0)
	for _, event := range all.Data {
		if event.CorrelationID == input.CorrelationID {
			filtered = append(filtered, event)
		}
	}
	return page(filtered, input.Pagination), nil
}

func (m *World) analyticsRunsGet(ctx context.Context, input worlds.AnalyticsRunsGetInput) (worlds.AnalyticsRun, error) {
	run, err := m.RunsGet(ctx, worlds.RunsGetInput{RunID: input.RunID})
	if err != nil {
		return worlds.AnalyticsRun{}, err
	}
	return analyticsRun(run), nil
}

func (m *World) analyticsRunsList(ctx context.Context, input worlds.AnalyticsRunsListInput) (worlds.Page[worlds.AnalyticsRun], error) {
	runs, err := m.RunsList(ctx, worlds.RunsListInput{WorkflowName: input.WorkflowName, Status: input.Status})
	if err != nil {
		return worlds.Page[worlds.AnalyticsRun]{}, err
	}
	values := make([]worlds.AnalyticsRun, 0, len(runs.Data))
	for _, run := range runs.Data {
		if input.StartTime != nil && run.CreatedAt.Before(*input.StartTime) {
			continue
		}
		if input.EndTime != nil && run.CreatedAt.After(*input.EndTime) {
			continue
		}
		matched := true
		for key, value := range input.Attributes {
			if run.Attributes[key] != value {
				matched = false
				break
			}
		}
		if matched {
			values = append(values, analyticsRun(run))
		}
	}
	return page(values, input.Pagination), nil
}
func (m *World) analyticsAttributesList(ctx context.Context, input worlds.AnalyticsAttributesListInput) (worlds.Page[worlds.AnalyticsAttribute], error) {
	runs, err := m.analyticsRunsList(ctx, worlds.AnalyticsRunsListInput{WorkflowName: input.WorkflowName, StartTime: input.StartTime, EndTime: input.EndTime})
	if err != nil {
		return worlds.Page[worlds.AnalyticsAttribute]{}, err
	}
	items := map[string]worlds.AnalyticsAttribute{}
	for _, run := range runs.Data {
		for key := range run.Attributes {
			item := items[key]
			if item.Key == "" {
				item = worlds.AnalyticsAttribute{Key: key, FirstSeenAt: run.CreatedAt}
			}
			item.RunCount++
			item.LastSeenAt = run.UpdatedAt
			items[key] = item
		}
	}
	values := make([]worlds.AnalyticsAttribute, 0, len(items))
	for _, item := range items {
		values = append(values, item)
	}
	sort.Slice(values, func(i, j int) bool { return values[i].Key < values[j].Key })
	return page(values, input.Pagination), nil
}

func resolveEventData(events []worlds.Event, resolveData worlds.EventsResolveData) []worlds.Event {
	if resolveData != worlds.EventsResolveDataNone {
		return events
	}
	for index := range events {
		events[index].Data = nil
	}
	return events
}

func eventPage(values []worlds.Event, pagination worlds.Pagination) worlds.Page[worlds.Event] {
	start := 0
	if pagination.Cursor != "" {
		if slot, err := worlds.EventSlot(worlds.EventID(pagination.Cursor)); err == nil {
			start = int(slot)
		}
	}
	if start > len(values) {
		start = len(values)
	}
	limit := pagination.Limit
	if limit <= 0 {
		limit = len(values) - start
	}
	end := start + limit
	if end > len(values) {
		end = len(values)
	}
	result := worlds.Page[worlds.Event]{Data: values[start:end], HasMore: end < len(values)}
	if result.HasMore && end != 0 {
		result.Cursor = string(values[end-1].EventID)
	}
	return result
}
