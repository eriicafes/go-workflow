package testworld

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

func (m *World) EventsCreate(ctx context.Context, input worlds.EventsCreateInput) (worlds.EventResult, error) {
	if input.Event.EventType == worlds.EventTypeHookCreated {
		return m.createHook(ctx, input)
	}
	if err := ctx.Err(); err != nil {
		return worlds.EventResult{}, err
	}
	if input.Event.EventType == worlds.EventTypeRunCreated || !input.Event.EventType.Valid() {
		return worlds.EventResult{}, worlds.ErrEventInvalid
	}
	// A resume's digest must be its payload's, as world-postgres checks it.
	if err := worlds.CheckResumeDigest(input.Event, input.Options.ResumePayloadDigest); err != nil {
		return worlds.EventResult{}, err
	}
	if input.Event.EventType == worlds.EventTypeRunStarted {
		// A first flow's run_started creates a run whose run_created never
		// landed, and is stored without the creation it carries.
		if err := worlds.CreateRunForStart(ctx, m, input.RunID, input.Event.Data); err != nil {
			return worlds.EventResult{}, err
		}
		input.Event.Data = nil
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
	resumeKey := ""
	if input.Event.EventType == worlds.EventTypeHookReceived && input.Options.ResumeID != "" {
		// A repeat of a resume returns the event it committed.
		resumeKey = string(input.RunID) + "/" + input.Options.ResumeID
		if claim, ok := m.resumes[resumeKey]; ok {
			if claim.digest != input.Options.ResumePayloadDigest {
				return worlds.EventResult{}, fmt.Errorf("%w: resume %s was sent with another payload", worlds.ErrEventInvalid, input.Options.ResumeID)
			}
			for _, event := range m.events[input.RunID] {
				if event.EventID == claim.eventID {
					result := m.eventResultLocked(event, nil, false)
					result.Events = worlds.ResolveEvents(m.deltaLocked(input.RunID, input.Options.EventCount, input.Options.SinceCursor), input.Options.ResolveData)
					result.InBandCount = m.runs[input.RunID].InBandCount
					return result, nil
				}
			}
		}
	}
	if result, duplicate, err := m.admitLocked(input.RunID, input.Event); err != nil || duplicate {
		if duplicate {
			result.Events = worlds.ResolveEvents(m.deltaLocked(input.RunID, input.Options.EventCount, input.Options.SinceCursor), input.Options.ResolveData)
			result.InBandCount = m.runs[input.RunID].InBandCount
		}
		return result, err
	}
	event := worlds.Event{RunID: input.RunID, EventID: worlds.EventIDForSlot(uint64(len(m.events[input.RunID]) + 1)), EventType: input.Event.EventType, CorrelationID: input.Event.CorrelationID, Data: bytes.Clone(input.Event.Data), CreatedAt: createdAt(input.Event, input.Options.InBandCount), SpecVersion: worlds.EventSpecVersion(input.Event.SpecVersion)}
	if resumeKey != "" {
		event.ResumeID = input.Options.ResumeID
		m.resumes[resumeKey] = resumeClaim{digest: input.Options.ResumePayloadDigest, eventID: event.EventID}
	}
	before := worlds.ResolveEvents(m.deltaLocked(input.RunID, input.Options.EventCount, input.Options.SinceCursor), input.Options.ResolveData)
	m.events[input.RunID] = append(m.events[input.RunID], event)
	m.materializeLocked(event)
	result := m.eventResultLocked(event, before, true)
	result.InBandCount = m.advanceLocked(input.RunID, input.Options.InBandCount, 1)
	return result, nil
}

// createdAt returns the creation time of event: its OccurredAt for an
// in-band write that sets one, and now otherwise.
func createdAt(event worlds.EventsCreateEvent, inBand *int) time.Time {
	if inBand != nil && !event.OccurredAt.IsZero() {
		return event.OccurredAt.UTC()
	}
	return time.Now().UTC()
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
	// The run ID and attributes are checked as the Workflow SDK's Worlds
	// check them.
	if err := worlds.CheckRunCreation(input, time.Now()); err != nil {
		return worlds.EventResult{}, err
	}
	if _, exists := m.runs[input.RunID]; exists {
		if created := m.events[input.RunID][0]; worlds.RepeatsAcceptedStart(created.Data, input) {
			return worlds.EventResult{EventID: created.EventID, EventType: created.EventType, CreatedAt: created.CreatedAt, InBandCount: m.runs[input.RunID].InBandCount}, nil
		}
		return worlds.EventResult{}, worlds.ErrRunAlreadyExists
	}
	now := time.Now().UTC()
	if input.StartHook != nil {
		if input.StartHook.Token == "" {
			return worlds.EventResult{}, worlds.ErrEventInvalid
		}
		if decision, ok := m.startDecisions[input.RunID]; ok {
			return worlds.EventResult{}, decision.Err()
		}
		if worlds.StartHookCandidateExpired(input.RunID, now) {
			return worlds.EventResult{}, worlds.ErrStartHookRejected
		}
		// The start hook claims its token with the run, or no run is
		// created, and the candidate's refusal is permanent.
		m.expireTokenLocked(input.StartHook.Token, now)
		if owner := m.tokens[input.StartHook.Token]; owner != "" {
			decision := worlds.StartHookDecision{Token: input.StartHook.Token, ConflictingRunID: m.hooks[owner].RunID}
			m.startDecisions[input.RunID] = decision
			return worlds.EventResult{}, decision.Err()
		}
	}
	payload, err := worlds.RunCreatedData(input)
	if err != nil {
		return worlds.EventResult{}, err
	}
	// run_created takes the first position, which counts as in-band.
	run := worlds.Run{RunID: input.RunID, WorkflowName: input.WorkflowName, Status: worlds.StatusPending, Attributes: maps.Clone(input.Attributes), DeploymentID: input.DeploymentID, EncryptionPublicKey: input.EncryptionPublicKey, ExecutionContext: input.ExecutionContext, InBandCount: 1, SpecVersion: worlds.SpecVersion, CreatedAt: now, UpdatedAt: now, StartHook: worlds.RunEchoStartHook(payload)}
	if run.Attributes == nil {
		run.Attributes = make(map[string]string)
	}
	m.runs[input.RunID] = run
	m.events[input.RunID] = []worlds.Event{{RunID: input.RunID, EventID: worlds.EventIDForSlot(1), EventType: worlds.EventTypeRunCreated, Data: payload, CreatedAt: now, SpecVersion: worlds.SpecVersion}}
	if hook := input.StartHook; hook != nil {
		id := worlds.StartHookID(input.RunID)
		m.hooks[id] = worlds.Hook{RunID: input.RunID, HookID: id, Token: hook.Token, TokenRetentionUntil: hook.TokenRetentionUntil, ResumeContext: worlds.HookResumeContext{DeploymentID: run.DeploymentID, WorkflowName: run.WorkflowName, EncryptionPublicKey: run.EncryptionPublicKey, RunSpecVersion: run.SpecVersion}, SpecVersion: worlds.SpecVersion, CreatedAt: now}
		m.tokens[hook.Token] = id
	}
	return worlds.EventResult{EventID: worlds.EventIDForSlot(1), EventType: worlds.EventTypeRunCreated, CreatedAt: now, AllocatedEventCount: 1, InBandCount: 1}, nil
}

func (m *World) EventsCreateBatch(ctx context.Context, input worlds.EventsCreateBatchInput) (worlds.EventBatchResult, error) {
	if err := ctx.Err(); err != nil {
		return worlds.EventBatchResult{}, err
	}
	for _, value := range input.Events {
		if value.EventType == worlds.EventTypeRunCreated || value.EventType == worlds.EventTypeHookCreated || !value.EventType.Valid() {
			return worlds.EventBatchResult{}, worlds.ErrEventInvalid
		}
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
	result := worlds.EventBatchResult{Results: make([]worlds.EventBatchItem, len(input.Events))}
	if input.EventCount != nil {
		result.Events = worlds.ResolveEvents(m.deltaLocked(input.RunID, input.EventCount, ""), "")
	}
	refused := map[string]bool{}
	for index, value := range input.Events {
		if value.CorrelationID != "" && refused[value.CorrelationID] {
			result.Results[index].Err = fmt.Errorf("%w: an earlier event of %s was refused", worlds.ErrStepNotStartable, value.CorrelationID)
			continue
		}
		if _, duplicate, err := m.admitLocked(input.RunID, value); err != nil || duplicate {
			if err == nil {
				err = fmt.Errorf("%w: batch repeats %s %s", worlds.ErrEventConflict, value.EventType, value.CorrelationID)
			}
			result.Results[index].Err = err
			refused[value.CorrelationID] = value.CorrelationID != ""
			continue
		}
		event := worlds.Event{RunID: input.RunID, EventID: worlds.EventIDForSlot(uint64(len(m.events[input.RunID]) + 1)), EventType: value.EventType, CorrelationID: value.CorrelationID, Data: bytes.Clone(value.Data), CreatedAt: createdAt(value, input.InBandCount), SpecVersion: worlds.EventSpecVersion(value.SpecVersion)}
		m.events[input.RunID] = append(m.events[input.RunID], event)
		m.materializeLocked(event)
		result.Results[index].EventResult = m.eventResultLocked(event, nil, true)
		result.AllocatedEventCount++
	}
	result.InBandCount = m.advanceLocked(input.RunID, input.InBandCount, result.AllocatedEventCount)
	for index := range result.Results {
		result.Results[index].InBandCount = result.InBandCount
	}
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
			result.Data = worlds.ResolveEventData(result.EventType, result.Data, input.ResolveData)
			return result, nil
		}
	}
	if _, ok := m.runs[input.RunID]; !ok {
		return worlds.Event{}, worlds.ErrRunNotFound
	}
	return worlds.Event{}, worlds.ErrEventNotFound
}

func (m *World) createHook(ctx context.Context, input worlds.EventsCreateInput) (worlds.EventResult, error) {
	// Serialize token ownership and record a forced takeover in the previous
	// owner's run before making the token visible to the claimant.
	if err := ctx.Err(); err != nil {
		return worlds.EventResult{}, err
	}
	if input.Event.EventType != worlds.EventTypeHookCreated {
		return worlds.EventResult{}, worlds.ErrEventInvalid
	}
	var created worlds.DataHookCreated
	if err := json.Unmarshal(input.Event.Data, &created); err != nil || created.Token == "" || input.Event.CorrelationID == string(worlds.StartHookID(input.RunID)) {
		// The start hook's ID is the start hook's alone.
		return worlds.EventResult{}, worlds.ErrEventInvalid
	}
	defer m.notify()
	m.mu.Lock()
	defer m.mu.Unlock()
	claimant, ok := m.runs[input.RunID]
	if !ok {
		return worlds.EventResult{}, worlds.ErrRunNotFound
	}
	if claimant.Status.Terminal() {
		return worlds.EventResult{}, worlds.FinishedRunRefusal(claimant.Status, input.Event, "")
	}
	if err := m.fenceLocked(input.RunID, input.Options.InBandCount); err != nil {
		return worlds.EventResult{}, err
	}
	// A hook is created once, as in the Workflow SDK's Worlds.
	if _, disposed := m.disposals[worlds.HookID(input.Event.CorrelationID)]; disposed || m.hasEventLocked(input.RunID, worlds.EventTypeHookCreated, input.Event.CorrelationID) {
		return worlds.EventResult{}, fmt.Errorf("%w: hook %s already created", worlds.ErrEventConflict, input.Event.CorrelationID)
	}
	m.expireTokenLocked(created.Token, time.Now().UTC())
	before := len(m.events[input.RunID])
	result := worlds.EventResult{Events: m.deltaLocked(input.RunID, input.Options.EventCount, input.Options.SinceCursor)}

	if ownerID := m.tokens[created.Token]; ownerID != "" {
		owner := m.hooks[ownerID]
		if !created.Force {
			created.ConflictingRunID = owner.RunID
			input.Event.EventType = worlds.EventTypeHookConflict
		} else {
			created.ForceClaimedFrom = &worlds.HookForceClaim{RunID: owner.RunID, HookID: ownerID, WorkflowName: owner.ResumeContext.WorkflowName, DeploymentID: owner.ResumeContext.DeploymentID, RunSpecVersion: owner.ResumeContext.RunSpecVersion}
			if m.runs[owner.RunID].Status.Terminal() {
				delete(m.tokens, created.Token)
				delete(m.hooks, ownerID)
			} else {
				data, _ := json.Marshal(worlds.HookDisposedEventData{ForceClaimedBy: &worlds.HookForceClaim{RunID: input.RunID, HookID: worlds.HookID(input.Event.CorrelationID)}})
				forced := worlds.Event{RunID: owner.RunID, EventID: worlds.EventIDForSlot(uint64(len(m.events[owner.RunID]) + 1)), EventType: worlds.EventTypeHookDisposed, CorrelationID: string(ownerID), Data: data, CreatedAt: time.Now().UTC(), SpecVersion: worlds.SpecVersion}
				m.events[owner.RunID] = append(m.events[owner.RunID], forced)
				m.materializeLocked(forced)
			}
		}
	}
	input.Event.Data, _ = json.Marshal(created)

	event := worlds.Event{RunID: input.RunID, EventID: worlds.EventIDForSlot(uint64(len(m.events[input.RunID]) + 1)), EventType: input.Event.EventType, CorrelationID: input.Event.CorrelationID, Data: bytes.Clone(input.Event.Data), CreatedAt: time.Now().UTC(), SpecVersion: worlds.EventSpecVersion(input.Event.SpecVersion)}
	m.events[input.RunID] = append(m.events[input.RunID], event)
	m.materializeLocked(event)
	result.Events = append(result.Events, m.events[input.RunID][before:len(m.events[input.RunID])-1]...)
	result = m.eventResultLocked(event, result.Events, true)
	result.AllocatedEventCount = len(m.events[input.RunID]) - before
	result.InBandCount = m.advanceLocked(input.RunID, input.Options.InBandCount, result.AllocatedEventCount)
	return result, nil
}

// admitLocked implements the state-based queue fence. A duplicate start is
// inert and returns the previously committed lifecycle event instead of
// granting another execution.
func (m *World) admitLocked(runID worlds.RunID, requested worlds.EventsCreateEvent) (worlds.EventResult, bool, error) {
	run := m.runs[runID]
	// A run of a newer spec version is a newer release's, as the Workflow
	// SDK's Worlds refuse it.
	if err := worlds.CheckRunSpecVersion(runID, run.SpecVersion); err != nil {
		return worlds.EventResult{}, false, err
	}
	step, stepExists := m.steps[runID][worlds.StepID(requested.CorrelationID)]
	if run.Status.Terminal() {
		if err := worlds.FinishedRunRefusal(run.Status, requested, step.Status); err != nil {
			return worlds.EventResult{}, false, err
		}
	}
	if requested.EventType == worlds.EventTypeRunStarted && run.Status == worlds.StatusRunning {
		// The run started before: no second run_started is appended, and
		// the answer holds no event.
		return worlds.EventResult{}, true, nil
	}
	// A step is created once, and its other events need it unfinished, as
	// in the Workflow SDK's Worlds. A started step may be started again: each
	// start records a new attempt.
	if err := worlds.CheckStepEvent(requested, stepExists, step.Status, step.RetryAfter, time.Now()); err != nil {
		return worlds.EventResult{}, false, err
	}
	if requested.EventType == worlds.EventTypeAttrSet {
		if err := worlds.CheckAttrSet(run.Attributes, requested.Data); err != nil {
			return worlds.EventResult{}, false, err
		}
	}

	switch requested.EventType {
	case worlds.EventTypeWaitCreated, worlds.EventTypeWaitCompleted, worlds.EventTypeAttrSet:
		// A wait completes once it was created.
		if requested.EventType == worlds.EventTypeWaitCompleted && !m.hasEventLocked(runID, worlds.EventTypeWaitCreated, requested.CorrelationID) {
			return worlds.EventResult{}, false, fmt.Errorf("%w: wait %s not found", worlds.ErrEventNotFound, requested.CorrelationID)
		}
		// A wait is created and completed once, and an attribute write of
		// the workflow is recorded once.
		once := requested.EventType != worlds.EventTypeAttrSet || worlds.WorkflowAttrSet(requested.Data)
		if once && requested.CorrelationID != "" && m.hasEventLocked(runID, requested.EventType, requested.CorrelationID) {
			return worlds.EventResult{}, false, fmt.Errorf("%w: %s %s already recorded", worlds.ErrEventConflict, requested.EventType, requested.CorrelationID)
		}
	case worlds.EventTypeHookDisposed:
		// A disposed hook is gone, so disposing it again finds no hook, as
		// in the Workflow SDK's Worlds.
		hook, exists := m.hooks[worlds.HookID(requested.CorrelationID)]
		if !exists || hook.RunID != runID {
			return worlds.EventResult{}, false, worlds.ErrHookNotFound
		}
	}

	if requested.EventType == worlds.EventTypeHookReceived {
		if disposal, disposed := m.disposals[worlds.HookID(requested.CorrelationID)]; disposed && disposal.runID == runID {
			if disposal.forceClaimedBy != nil {
				return worlds.EventResult{}, false, worlds.ErrHookForceClaimed
			}
			return worlds.EventResult{}, false, worlds.ErrHookNotFound
		}
		hook := m.hooks[worlds.HookID(requested.CorrelationID)]
		if hook.HookID == "" || hook.RunID != runID || m.tokens[hook.Token] != hook.HookID {
			return worlds.EventResult{}, false, worlds.ErrHookNotFound
		}
	}

	if requested.EventType == worlds.EventTypeHookCreated {
		var created worlds.DataHookCreated
		if json.Unmarshal(requested.Data, &created) == nil {
			m.expireTokenLocked(created.Token, time.Now().UTC())
			if _, exists := m.tokens[created.Token]; exists {
				return worlds.EventResult{}, false, worlds.ErrHookConflict
			}
		}
	}
	return worlds.EventResult{}, false, nil
}

// hasEventLocked reports whether the run's log holds an eventType event of
// correlation.
func (m *World) hasEventLocked(runID worlds.RunID, eventType worlds.EventType, correlation string) bool {
	for _, event := range m.events[runID] {
		if event.EventType == eventType && event.CorrelationID == correlation {
			return true
		}
	}
	return false
}

func (m *World) firstResultLocked(runID worlds.RunID, eventType worlds.EventType, correlation string) worlds.EventResult {
	for _, event := range m.events[runID] {
		if event.EventType == eventType && event.CorrelationID == correlation {
			return m.eventResultLocked(event, nil, false)
		}
	}
	return worlds.EventResult{}
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
	result := worlds.EventResult{EventID: event.EventID, EventType: event.EventType, CreatedAt: event.CreatedAt, Events: events, Data: bytes.Clone(event.Data)}
	if created {
		result.AllocatedEventCount = 1
	}
	if event.EventType == worlds.EventTypeWaitCreated || event.EventType == worlds.EventTypeWaitCompleted {
		result.Wait = worlds.WaitOfEvents(event.RunID, event.CorrelationID, m.events[event.RunID])
	}
	if event.EventType == worlds.EventTypeHookCreated {
		hook := m.hookPayloadLocked(cloneHook(m.hooks[worlds.HookID(event.CorrelationID)]), worlds.ResolveDataAll)
		result.Hook = &hook
	}
	if event.EventType != worlds.EventTypeStepCreated {
		return result
	}
	step := m.steps[event.RunID][worlds.StepID(event.CorrelationID)]
	if step.StepID == "" {
		return result
	}
	step = m.stepPayloadsLocked(step, worlds.ResolveDataAll)
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
			start = min(int(slot), len(log))
		}
	}
	result := make([]worlds.Event, 0, len(log)-start)
	for _, event := range log[start:] {
		result = append(result, cloneEvent(event))
	}
	return result
}

func (m *World) materializeLocked(event worlds.Event) {
	run := m.runs[event.RunID]
	now := event.CreatedAt
	run.UpdatedAt = now
	switch event.EventType {
	case worlds.EventTypeRunStarted:
		run.Status, run.StartedAt = worlds.StatusRunning, &now
	case worlds.EventTypeRunCompleted:
		run.Status, run.CompletedAt = worlds.StatusCompleted, &now
	case worlds.EventTypeRunFailed:
		run.Status, run.CompletedAt = worlds.StatusFailed, &now
		run.ErrorCode = worlds.RunFailedErrorCode(event.Data)
	case worlds.EventTypeRunCancelled:
		if run.Status != worlds.StatusCancelled {
			run.Status, run.CompletedAt = worlds.StatusCancelled, &now
		}
	case worlds.EventTypeAttrSet:
		var value worlds.DataAttrSet
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
		var value worlds.DataStepCreated
		_ = json.Unmarshal(event.Data, &value)
		if m.steps[event.RunID] == nil {
			m.steps[event.RunID] = make(map[worlds.StepID]worlds.Step)
		}
		id := worlds.StepID(event.CorrelationID)
		m.steps[event.RunID][id] = worlds.Step{RunID: event.RunID, StepID: id, StepName: value.StepName, Status: worlds.StatusPending, SpecVersion: event.SpecVersion, CreatedAt: now, UpdatedAt: now}
	case worlds.EventTypeStepStarted:
		if worlds.StartCreatesStep(event.Data) {
			var value worlds.DataStepStarted
			_ = json.Unmarshal(event.Data, &value)
			if m.steps[event.RunID] == nil {
				m.steps[event.RunID] = make(map[worlds.StepID]worlds.Step)
			}
			id := worlds.StepID(event.CorrelationID)
			m.steps[event.RunID][id] = worlds.Step{RunID: event.RunID, StepID: id, StepName: value.StepName, Status: worlds.StatusPending, SpecVersion: event.SpecVersion, CreatedAt: now, UpdatedAt: now}
		}
		m.updateStepLocked(event, worlds.StatusRunning)
	case worlds.EventTypeStepCompleted:
		m.updateStepLocked(event, worlds.StatusCompleted)
	case worlds.EventTypeStepFailed:
		m.updateStepLocked(event, worlds.StatusFailed)
	case worlds.EventTypeStepRetrying:
		m.updateStepLocked(event, worlds.StatusPending)
	case worlds.EventTypeHookCreated:
		var value worlds.DataHookCreated
		_ = json.Unmarshal(event.Data, &value)
		id := worlds.HookID(event.CorrelationID)
		hook := worlds.Hook{RunID: event.RunID, ResumeContext: worlds.HookResumeContext{DeploymentID: run.DeploymentID, WorkflowName: run.WorkflowName, EncryptionPublicKey: run.EncryptionPublicKey, RunSpecVersion: run.SpecVersion}, HookID: id, Token: value.Token, IsWebhook: value.IsWebhook, TokenRetentionUntil: value.TokenRetentionUntil, ClaimedFrom: value.ForceClaimedFrom, SpecVersion: event.SpecVersion, CreatedAt: now}
		m.hooks[id], m.tokens[value.Token] = hook, id
	case worlds.EventTypeHookDisposed:
		id := worlds.HookID(event.CorrelationID)
		var disposed worlds.HookDisposedEventData
		_ = json.Unmarshal(event.Data, &disposed)
		m.disposals[id] = hookDisposal{runID: event.RunID, forceClaimedBy: disposed.ForceClaimedBy}
		if hook, ok := m.hooks[id]; ok && m.tokens[hook.Token] == id {
			delete(m.tokens, hook.Token)
		}
		delete(m.hooks, id)

	}
	m.runs[event.RunID] = run
	if event.EventType == worlds.EventTypeRunCompleted || event.EventType == worlds.EventTypeRunFailed || event.EventType == worlds.EventTypeRunCancelled {
		// A hook whose token is retained stays until its retention ends;
		// every other hook of the run is deleted.
		for id, hook := range m.hooks {
			if hook.RunID == event.RunID && (hook.TokenRetentionUntil == nil || !hook.TokenRetentionUntil.After(now)) {
				if m.tokens[hook.Token] == id {
					delete(m.tokens, hook.Token)
				}
				delete(m.hooks, id)
			}
		}
		if worlds.ReadRunRetention(run.Attributes) == worlds.RetainNone {
			m.purgeLocked(event.RunID, now)
		}
	} else if worlds.RunDataExpired(run, now) {
		// A step still running when its run's data was deleted settles
		// after it. What it brought is deleted as it lands.
		m.scrubLastEventLocked(event.RunID)
	}
}

func (m *World) expireTokenLocked(token string, now time.Time) {
	id := m.tokens[token]
	if id == "" {
		return
	}
	hook := m.hooks[id]
	if m.runs[hook.RunID].Status.Terminal() && hook.TokenRetentionUntil != nil && !hook.TokenRetentionUntil.After(now) {
		delete(m.tokens, token)
		delete(m.hooks, id)
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
		if step.StartedAt == nil {
			step.StartedAt = &event.CreatedAt
		}
	}
	if status == worlds.StatusCompleted || status == worlds.StatusFailed {
		step.CompletedAt = &event.CreatedAt
	}
	var result worlds.DataStepResult
	if json.Unmarshal(event.Data, &result) == nil {
		step.RetryAfter = result.RetryAfter
	}
	m.steps[event.RunID][step.StepID] = step
}

func (m *World) EventsList(ctx context.Context, input worlds.EventsListInput) (worlds.EventsPage, error) {
	// The list's defaults are world-local's.
	input.Pagination = input.Pagination.WithDefaults(worlds.SortOrderAscending, worlds.MaxEventsPerRun)
	if err := ctx.Err(); err != nil {
		return worlds.EventsPage{}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	log, ok := m.events[input.RunID]
	if !ok {
		return worlds.EventsPage{}, worlds.ErrRunNotFound
	}
	snapshot := worlds.EventsSnapshot{EventCount: len(log), InBandCount: m.runs[input.RunID].InBandCount}
	values := make([]worlds.Event, len(log))
	for i := range log {
		values[i] = cloneEvent(log[i])
	}
	if input.Pagination.SortOrder == worlds.SortOrderDescending {
		for i, j := 0, len(values)-1; i < j; i, j = i+1, j-1 {
			values[i], values[j] = values[j], values[i]
		}
	}
	values = worlds.ResolveEvents(values, input.ResolveData)
	return worlds.EventsPage{Page: eventPage(values, input.Pagination), Snapshot: snapshot}, nil
}

func (m *World) EventsListByCorrelationID(ctx context.Context, input worlds.EventsListByCorrelationIDInput) (worlds.Page[worlds.Event], error) {
	// The list's defaults are world-local's.
	input.Pagination = input.Pagination.WithDefaults(worlds.SortOrderAscending, 20)
	all, err := m.EventsList(ctx, worlds.EventsListInput{RunID: input.RunID, ResolveData: input.ResolveData, Pagination: worlds.Pagination{SortOrder: input.Pagination.SortOrder, Limit: -1}})
	if err != nil {
		return worlds.Page[worlds.Event]{}, err
	}
	filtered := make([]worlds.Event, 0)
	for _, event := range all.Data {
		if event.CorrelationID == input.CorrelationID {
			filtered = append(filtered, event)
		}
	}
	return eventPage(filtered, input.Pagination), nil
}

func (m *World) analyticsRunsGet(ctx context.Context, input worlds.AnalyticsRunsGetInput) (worlds.AnalyticsRun, error) {
	run, err := m.RunsGet(ctx, worlds.RunsGetInput{RunID: input.RunID})
	if err != nil {
		return worlds.AnalyticsRun{}, err
	}
	return worlds.NewAnalyticsRun(run), nil
}

func (m *World) analyticsRunsList(ctx context.Context, input worlds.AnalyticsRunsListInput) (worlds.Page[worlds.AnalyticsRun], error) {
	runs, err := m.RunsList(ctx, worlds.RunsListInput{WorkflowName: input.WorkflowName, Status: input.Status, Pagination: worlds.Pagination{SortOrder: worlds.SortOrderAscending, Limit: -1}})
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
			values = append(values, worlds.NewAnalyticsRun(run))
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

// eventPage pages events listed in pagination's order. The cursor is the
// last event ID of the previous page.
func eventPage(values []worlds.Event, pagination worlds.Pagination) worlds.Page[worlds.Event] {
	start := 0
	if pagination.Cursor != "" {
		if cursor, err := worlds.EventSlot(worlds.EventID(pagination.Cursor)); err == nil {
			descending := pagination.SortOrder == worlds.SortOrderDescending
			for start < len(values) {
				slot, _ := worlds.EventSlot(values[start].EventID)
				if descending && slot < cursor || !descending && slot > cursor {
					break
				}
				start++
			}
		}
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

// EventsSubscribe pushes the run's events after afterSlot as they commit.
func (m *World) EventsSubscribe(ctx context.Context, runID worlds.RunID, afterSlot uint64, onEvent func(worlds.Event), onError func(error)) func() {
	return worlds.FeedEvents(ctx, m, runID, afterSlot, func() (<-chan struct{}, func()) { return m.changed(), func() {} }, onEvent, onError)
}
