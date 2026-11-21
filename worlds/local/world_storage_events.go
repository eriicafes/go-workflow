package localworld

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// txn stages appends to one run. Records are loaded lazily and only dirty
// records are written. Nothing reaches disk before commit, so a rejected
// append or batch is discarded by dropping the txn.
type txn struct {
	w        *World
	run      runRecord
	appended []worlds.Event
	// replay builds views from committed events without reading current views.
	replay bool
	steps  map[worlds.StepID]*stepRecord
	hooks  map[worlds.HookID]*hookRecord
	tokens map[string]*tokenRecord
	dirty  map[any]bool
}

func newTxn(w *World, run runRecord, replay bool) *txn {
	run.Attributes = maps.Clone(run.Attributes)
	if run.Attributes == nil {
		run.Attributes = map[string]string{}
	}
	return &txn{w: w, run: run, replay: replay, steps: map[worlds.StepID]*stepRecord{}, hooks: map[worlds.HookID]*hookRecord{}, tokens: map[string]*tokenRecord{}, dirty: map[any]bool{}}
}

type (
	stepKey  worlds.StepID
	hookKey  worlds.HookID
	tokenKey string
)

func (w *World) begin(runID worlds.RunID) (*txn, error) {
	run, err := w.readRun(runID)
	if err != nil {
		return nil, err
	}
	return newTxn(w, run, false), nil
}

func (t *txn) step(id worlds.StepID) (*stepRecord, error) {
	if value, ok := t.steps[id]; ok || t.replay {
		return value, nil
	}
	value, err := t.w.readStep(t.run.RunID, id)
	if err == nil {
		t.steps[id] = value
	}
	return value, err
}

func (t *txn) hook(id worlds.HookID) (*hookRecord, error) {
	if value, ok := t.hooks[id]; ok || t.replay {
		return value, nil
	}
	value, err := t.w.readHook(id)
	if err == nil {
		t.hooks[id] = value
	}
	return value, err
}

func (t *txn) token(token string) (*tokenRecord, error) {
	if value, ok := t.tokens[token]; ok || t.replay {
		return value, nil
	}
	value, err := t.w.readToken(token)
	if err == nil {
		t.tokens[token] = value
	}
	return value, err
}

func (t *txn) setStep(value *stepRecord) {
	t.steps[value.StepID], t.dirty[stepKey(value.StepID)] = value, true
}

func (t *txn) setHook(value *hookRecord) {
	t.hooks[value.HookID], t.dirty[hookKey(value.HookID)] = value, true
}

func (t *txn) setToken(token string, value *tokenRecord) {
	t.tokens[token], t.dirty[tokenKey(token)] = value, true
}

// releaseToken frees token when hookID still owns it.
func (t *txn) releaseToken(token string, hookID worlds.HookID) error {
	owner, err := t.token(token)
	if err != nil || owner == nil || owner.HookID != hookID {
		return err
	}
	t.setToken(token, nil)
	return nil
}

// admit applies the transition rules to the staged state. A non-empty event
// ID is an idempotent replay of that earlier event.
func (t *txn) admit(event worlds.EventsCreateEvent, now time.Time) (worlds.EventID, error) {
	if t.run.Status.Terminal() {
		// A finished run still records what was running when it finished: a
		// step's final result, and a wait's completion.
		finishing := event.EventType == worlds.EventTypeWaitCompleted
		if event.EventType == worlds.EventTypeStepCompleted || event.EventType == worlds.EventTypeStepFailed {
			step, err := t.step(worlds.StepID(event.CorrelationID))
			if err != nil {
				return "", err
			}
			finishing = step != nil && !step.Status.Terminal()
		}
		if !finishing {
			if event.EventType == worlds.EventTypeRunCancelled && t.run.Status == worlds.StatusCancelled {
				return t.run.CancelledEventID, nil
			}
			return "", worlds.ErrRunTerminal
		}
	}
	switch event.EventType {
	case worlds.EventTypeRunStarted:
		if t.run.Status == worlds.StatusRunning {
			return t.run.StartedEventID, nil
		}
	case worlds.EventTypeStepStarted:
		step, err := t.step(worlds.StepID(event.CorrelationID))
		if err != nil {
			return "", err
		}
		creates := worlds.StartCreatesStep(event.Data)
		if creates && step != nil {
			// An inline step is created once.
			earlier, err := t.findEvent(worlds.EventTypeStepStarted, event.CorrelationID)
			if err != nil || earlier != "" {
				return earlier, err
			}
		}
		if step == nil {
			if !creates {
				return "", worlds.ErrStepNotStartable
			}
			return "", nil
		}
		// A started step can be started again, as a new attempt, after its
		// worker dies.
		if step.Status.Terminal() {
			return "", worlds.ErrStepNotStartable
		}
	case worlds.EventTypeStepCreated:
		step, err := t.step(worlds.StepID(event.CorrelationID))
		if err != nil || step == nil {
			return "", err
		}
		return t.findEvent(worlds.EventTypeStepCreated, event.CorrelationID)
	case worlds.EventTypeStepCompleted, worlds.EventTypeStepFailed, worlds.EventTypeStepRetrying:
		step, err := t.step(worlds.StepID(event.CorrelationID))
		if err != nil {
			return "", err
		}
		// A step's result is recorded while the step is not finished.
		if step == nil || step.Status.Terminal() {
			return "", worlds.ErrStepNotStartable
		}
	case worlds.EventTypeWaitCompleted:
		return t.findEvent(worlds.EventTypeWaitCompleted, event.CorrelationID)

	case worlds.EventTypeHookDisposed:
		hook, err := t.hook(worlds.HookID(event.CorrelationID))
		if err != nil {
			return "", err
		}
		if hook == nil || hook.RunID != t.run.RunID {
			return "", worlds.ErrHookNotActive
		}
		if hook.DisposedAt != nil {
			return t.findEvent(worlds.EventTypeHookDisposed, event.CorrelationID)
		}

	case worlds.EventTypeHookReceived:
		hook, err := t.hook(worlds.HookID(event.CorrelationID))
		if err != nil {
			return "", err
		}
		if hook == nil || hook.RunID != t.run.RunID {
			return "", worlds.ErrHookNotActive
		}
		owner, err := t.token(hook.Token)
		if err != nil {
			return "", err
		}
		if owner == nil || owner.HookID != hook.HookID || hook.DisposedAt != nil {
			if hook.ForceClaimedBy != nil {
				return "", worlds.ErrHookForceClaimed
			}
			return "", worlds.ErrHookNotActive
		}

	case worlds.EventTypeHookCreated:
		var created worlds.DataHookCreated
		if json.Unmarshal(event.Data, &created) != nil || created.Token == "" {
			return "", nil
		}
		owner, err := t.token(created.Token)
		if err != nil {
			return "", err
		}
		if owner.held(now) {
			return "", worlds.ErrHookConflict
		}
	}
	return "", nil
}

// findEvent returns the ID of the run's first eventType event for
// correlationID, committed or staged, or "" when it has none.
func (t *txn) findEvent(eventType worlds.EventType, correlationID string) (worlds.EventID, error) {
	committed, err := t.w.readEvents(t.run.RunID, 1, t.run.EventCount)
	if err != nil {
		return "", err
	}
	for _, event := range append(committed, t.appended...) {
		if event.EventType == eventType && event.CorrelationID == correlationID {
			return event.EventID, nil
		}
	}
	return "", nil
}

// eventTime returns the creation time of event: its OccurredAt for an
// in-band write that sets one, and now otherwise.
func eventTime(event worlds.EventsCreateEvent, inBand *int, now time.Time) time.Time {
	if inBand != nil && !event.OccurredAt.IsZero() {
		return event.OccurredAt.UTC()
	}
	return now
}

// append stages the next slot and materializes it.
func (t *txn) append(value worlds.EventsCreateEvent, now time.Time) (worlds.Event, error) {
	event := worlds.Event{
		RunID:         t.run.RunID,
		EventID:       worlds.EventIDForSlot(uint64(t.run.EventCount + len(t.appended) + 1)),
		EventType:     value.EventType,
		CorrelationID: value.CorrelationID,
		Data:          bytes.Clone(value.Data),
		CreatedAt:     now,
	}
	t.appended = append(t.appended, event)
	return event, t.materialize(event)
}

func (t *txn) materialize(event worlds.Event) error {
	run := &t.run
	now := event.CreatedAt
	run.UpdatedAt = now
	switch event.EventType {
	case worlds.EventTypeRunStarted:
		run.Status, run.StartedAt, run.StartedEventID = worlds.StatusRunning, &now, event.EventID
	case worlds.EventTypeRunCompleted:
		run.Status, run.CompletedAt, run.OutputSlot = worlds.StatusCompleted, &now, eventSlot(event)
	case worlds.EventTypeRunFailed:
		run.Status, run.CompletedAt, run.ErrorSlot = worlds.StatusFailed, &now, eventSlot(event)
	case worlds.EventTypeRunCancelled:
		run.Status, run.CompletedAt, run.CancelledEventID = worlds.StatusCancelled, &now, event.EventID
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
		id := worlds.StepID(event.CorrelationID)
		t.setStep(&stepRecord{Step: worlds.Step{RunID: run.RunID, StepID: id, StepName: value.StepName, Status: worlds.StatusPending, CreatedAt: now, UpdatedAt: now}, InputSlot: eventSlot(event)})
	case worlds.EventTypeStepStarted:
		if worlds.StartCreatesStep(event.Data) {
			// An inline step is created by its first step_started, which
			// holds its input.
			id := worlds.StepID(event.CorrelationID)
			current, err := t.step(id)
			if err != nil {
				return err
			}
			if current == nil {
				var value worlds.DataStepStarted
				_ = json.Unmarshal(event.Data, &value)
				t.setStep(&stepRecord{Step: worlds.Step{RunID: run.RunID, StepID: id, StepName: value.StepName, Status: worlds.StatusPending, CreatedAt: now, UpdatedAt: now}, InputSlot: eventSlot(event)})
			}
		}
		return t.updateStep(event, worlds.StatusRunning)
	case worlds.EventTypeStepCompleted:
		return t.updateStep(event, worlds.StatusCompleted)
	case worlds.EventTypeStepFailed:
		return t.updateStep(event, worlds.StatusFailed)
	case worlds.EventTypeStepRetrying:
		return t.updateStep(event, worlds.StatusPending)
	case worlds.EventTypeHookCreated:
		var value worlds.DataHookCreated
		_ = json.Unmarshal(event.Data, &value)
		id := worlds.HookID(event.CorrelationID)
		hook := &hookRecord{Hook: worlds.Hook{RunID: run.RunID, ResumeContext: worlds.HookResumeContext{DeploymentID: run.DeploymentID, WorkflowName: run.WorkflowName, EncryptionPublicKey: run.EncryptionPublicKey}, HookID: id, Token: value.Token, IsWebhook: value.IsWebhook, TokenRetentionUntil: value.TokenRetentionUntil, ClaimedFrom: value.ForceClaimedFrom, CreatedAt: now, UpdatedAt: now}, MetadataSlot: eventSlot(event)}
		t.setHook(hook)
		if value.Token != "" {
			t.setToken(value.Token, &tokenRecord{HookID: id})
		}
	case worlds.EventTypeHookReceived:
		current, err := t.hook(worlds.HookID(event.CorrelationID))
		if err != nil || current == nil {
			return err
		}
		hook := *current
		hook.UpdatedAt, hook.ReceivedAt = now, &now
		t.setHook(&hook)
	case worlds.EventTypeHookDisposed:
		hook, err := t.hook(worlds.HookID(event.CorrelationID))
		if err != nil || hook == nil {
			return err
		}
		value := *hook
		value.DisposedAt, value.UpdatedAt = &now, now
		var disposed worlds.HookDisposedEventData
		_ = json.Unmarshal(event.Data, &disposed)
		value.ForceClaimedBy = disposed.ForceClaimedBy
		t.setHook(&value)
		return t.releaseToken(hook.Token, hook.HookID)
	}

	if event.EventType == worlds.EventTypeRunCompleted || event.EventType == worlds.EventTypeRunFailed || event.EventType == worlds.EventTypeRunCancelled {
		hooks, err := t.w.readHooks()
		if err != nil {
			return err
		}
		for id, staged := range t.hooks {
			replaced := false
			for i := range hooks {
				if hooks[i].HookID == id {
					hooks[i] = *staged
					replaced = true
					break
				}
			}
			if !replaced {
				hooks = append(hooks, *staged)
			}
		}
		for _, hook := range hooks {
			if hook.RunID != run.RunID {
				continue
			}
			owner, err := t.token(hook.Token)
			if err != nil {
				return err
			}
			if owner == nil || owner.HookID != hook.HookID {
				continue
			}
			if hook.TokenRetentionUntil != nil && hook.TokenRetentionUntil.After(now) {
				t.setToken(hook.Token, &tokenRecord{HookID: hook.HookID, TokenRetentionUntil: hook.TokenRetentionUntil})
			} else {
				t.setToken(hook.Token, nil)
			}
		}
	}
	return nil
}

// completeHook deactivates a completed webhook and releases its token.
func (t *txn) completeHook(hook *hookRecord, now time.Time) error {
	hook.DisposedAt = &now
	return t.releaseToken(hook.Token, hook.HookID)
}

func (t *txn) updateStep(event worlds.Event, status worlds.Status) error {
	current, err := t.step(worlds.StepID(event.CorrelationID))
	if err != nil || current == nil {
		return err
	}
	step := *current
	at := event.CreatedAt
	step.Status, step.UpdatedAt = status, at
	switch status {
	case worlds.StatusRunning:
		step.Attempt++
		if step.StartedAt == nil {
			step.StartedAt = &at
		}
	case worlds.StatusCompleted, worlds.StatusFailed:
		step.CompletedAt = &at
	}
	var result worlds.DataStepResult
	if json.Unmarshal(event.Data, &result) == nil {
		step.RetryAfter = result.RetryAfter
	}
	switch event.EventType {
	case worlds.EventTypeStepCompleted:
		step.OutputSlot, step.ErrorSlot = eventSlot(event), 0
	case worlds.EventTypeStepFailed, worlds.EventTypeStepRetrying:
		step.OutputSlot, step.ErrorSlot = 0, eventSlot(event)
	}
	t.setStep(&step)
	return nil
}

// commit writes event files, then dirty views, then the run record. The run
// record is the commit point. Recovery discards events beyond its EventCount
// and rebuilds the run's views from the committed prefix.
func (t *txn) commit() error {
	if len(t.appended) == 0 {
		return nil
	}
	purge := t.applyRetention(time.Now().UTC())
	err := t.write()
	if err != nil {
		if repairErr := t.w.rebuildRun(t.run.RunID); repairErr != nil {
			t.w.broken = errors.Join(errors.New("workflow/local: storage needs recovery, restart the world"), err, repairErr)
			return t.w.broken
		}
	}
	if err == nil && purge {
		t.w.purge(t.run)
	}
	return err
}

// applyRetention deletes the payloads of the appended events of a run whose
// data is gone, as a step still running when its run's data was deleted
// settles after it. When the appends finish a run with $retention "0", it
// sets the run's ExpiredAt, committed with them, and reports that the rest of
// the run's data is to be purged after the commit.
func (t *txn) applyRetention(now time.Time) bool {
	finished := false
	for _, event := range t.appended {
		switch event.EventType {
		case worlds.EventTypeRunCompleted, worlds.EventTypeRunFailed, worlds.EventTypeRunCancelled:
			finished = true
		}
	}
	purge := finished && !t.replay && worlds.ReadRunRetention(t.run.Attributes) == worlds.RetainNone && t.run.ExpiredAt == nil
	if purge {
		t.run.ExpiredAt, t.run.DataPurged = &now, false
	}
	if purge || worlds.RunDataExpired(t.run.Run, now) {
		for index, event := range t.appended {
			t.appended[index].Data = worlds.ScrubEventData(event.EventType, event.Data)
		}
	}
	return purge
}

func (t *txn) write() error {
	root := t.w.root
	for _, event := range t.appended {
		slot, _ := worlds.EventSlot(event.EventID)
		if err := writeJSON(root, eventPath(t.run.RunID, int(slot)), event); err != nil {
			return err
		}
	}
	if err := t.writeViews(); err != nil {
		return err
	}
	t.run.EventCount += len(t.appended)
	return writeJSON(root, runPath(t.run.RunID), t.run)
}

func (t *txn) writeViews() error {
	root := t.w.root
	for key := range t.dirty {
		var err error
		switch key := key.(type) {
		case stepKey:
			err = writeJSON(root, stepPath(t.run.RunID, worlds.StepID(key)), t.steps[worlds.StepID(key)])
		case hookKey:
			err = writeJSON(root, hookPath(worlds.HookID(key)), t.hooks[worlds.HookID(key)])
		case tokenKey:
			if value := t.tokens[string(key)]; value != nil {
				err = writeJSON(root, tokenPath(string(key)), value)
			} else {
				err = remove(root, tokenPath(string(key)))
			}
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// fence returns ErrInBandSuperseded when an in-band write's count is not the
// run's current InBandCount.
func (t *txn) fence(count *int) error {
	if count != nil && *count != t.run.InBandCount {
		return worlds.ErrInBandSuperseded
	}
	return nil
}

// advance stages appended more in-band events on the run record, which commits
// them with the events.
func (t *txn) advance(count *int, appended int) {
	if count != nil {
		t.run.InBandCount += appended
	}
}

// eventSlot returns the slot of an event this World allocated.
func eventSlot(event worlds.Event) int {
	slot, _ := worlds.EventSlot(event.EventID)
	return int(slot)
}

// load returns the event in slot, from the staged appends or the committed log.
func (t *txn) load(slot int) (worlds.Event, bool, error) {
	for _, event := range t.appended {
		if eventSlot(event) == slot {
			return event, true, nil
		}
	}
	return t.w.readEvent(t.run.RunID, slot)
}

// result returns the result of appending event. A step_created or
// hook_created result carries the view as event left it, with the payload
// already in hand.
func (t *txn) result(event worlds.Event, delta []worlds.Event) worlds.EventResult {
	result := worlds.EventResult{EventID: event.EventID, EventType: event.EventType, CreatedAt: event.CreatedAt, Data: bytes.Clone(event.Data), Events: delta, AllocatedEventCount: 1, InBandCount: t.run.InBandCount}
	if event.EventType == worlds.EventTypeHookCreated {
		if hook := t.hooks[worlds.HookID(event.CorrelationID)]; hook != nil {
			value := hook.Hook
			value.Metadata = worlds.EventPayload(event)
			result.Hook = &value
		}
	}
	if event.EventType == worlds.EventTypeStepCreated {
		if step := t.steps[worlds.StepID(event.CorrelationID)]; step != nil {
			value := step.Step
			value.Input = worlds.EventPayload(event)
			result.Step, result.StepCreated = &value, true
		}
	}
	return result
}

// delta returns the committed events selected by options.
func (w *World) delta(run runRecord, options worlds.EventsCreateOptions) ([]worlds.Event, error) {
	start := run.EventCount
	if options.EventCount != nil {
		start = min(max(*options.EventCount, 0), run.EventCount)
	} else if options.SinceCursor != "" {
		if slot, err := eventCursor(options.SinceCursor); err == nil {
			start = min(slot, run.EventCount)
		}
	}
	if start >= run.EventCount {
		return nil, nil
	}
	events, err := w.readEvents(run.RunID, start+1, run.EventCount)
	return worlds.ResolveEvents(events, options.ResolveData), err
}

func (w *World) EventsCreateRun(ctx context.Context, input worlds.EventsCreateRunInput) (worlds.EventResult, error) {
	if err := ctx.Err(); err != nil {
		return worlds.EventResult{}, err
	}
	if input.RunID == "" {
		return worlds.EventResult{}, worlds.ErrEventInvalid
	}
	unlock, err := w.writeLock()
	if err != nil {
		return worlds.EventResult{}, err
	}
	defer unlock()
	if ok, err := exists(w.root, runPath(input.RunID)); err != nil || ok {
		if ok {
			err = worlds.ErrRunAlreadyExists
		}
		return worlds.EventResult{}, err
	}
	payload, err := worlds.RunCreatedData(input)
	if err != nil {
		return worlds.EventResult{}, err
	}
	now := time.Now().UTC()
	attributes := maps.Clone(input.Attributes)
	if attributes == nil {
		attributes = map[string]string{}
	}
	run := runRecord{Run: worlds.Run{RunID: input.RunID, WorkflowName: input.WorkflowName, Status: worlds.StatusPending, Attributes: attributes, DeploymentID: input.DeploymentID, EncryptionPublicKey: input.EncryptionPublicKey, ExecutionContext: input.ExecutionContext, InBandCount: 1, SpecVersion: worlds.SpecVersion, CreatedAt: now, UpdatedAt: now}, EventCount: 1}
	event := worlds.Event{RunID: input.RunID, EventID: worlds.EventIDForSlot(1), EventType: worlds.EventTypeRunCreated, Data: payload, CreatedAt: now}
	var hook *hookRecord
	if start := input.StartHook; start != nil {
		if start.Token == "" {
			return worlds.EventResult{}, worlds.ErrEventInvalid
		}
		// The start hook claims its token with the run, or no run is
		// created.
		owner, err := w.readToken(start.Token)
		if err != nil {
			return worlds.EventResult{}, err
		}
		if owner.held(now) {
			previous, err := w.readHook(owner.HookID)
			if err != nil {
				return worlds.EventResult{}, err
			}
			return worlds.EventResult{}, &worlds.HookConflictError{RunID: previous.RunID}
		}
		hook = &hookRecord{Hook: worlds.Hook{RunID: input.RunID, HookID: worlds.StartHookID(input.RunID), Token: start.Token, TokenRetentionUntil: start.TokenRetentionUntil, ResumeContext: worlds.HookResumeContext{DeploymentID: input.DeploymentID, WorkflowName: input.WorkflowName, EncryptionPublicKey: input.EncryptionPublicKey}, CreatedAt: now, UpdatedAt: now}}
	}
	if err := w.createRun(run, event, hook); err != nil {
		return worlds.EventResult{}, err
	}
	w.signal()
	// run_created takes the first position, which counts as in-band.
	return worlds.EventResult{EventID: event.EventID, EventType: event.EventType, CreatedAt: now, AllocatedEventCount: 1, InBandCount: 1}, nil
}

// createRun clears leftovers of an uncommitted create, then writes slot one and
// commits the run record.
func (w *World) createRun(run runRecord, event worlds.Event, hook *hookRecord) error {
	for _, dir := range []string{eventsDir(run.RunID), stepsDir(run.RunID)} {
		if err := w.root.RemoveAll(dir); err != nil {
			return err
		}
	}
	if err := writeJSON(w.root, eventPath(run.RunID, 1), event); err != nil {
		return err
	}
	// A start hook and its token are written before the run file commits
	// them. Recovery removes them when the run file was never written.
	if hook != nil {
		if err := writeJSON(w.root, hookPath(hook.HookID), hook); err != nil {
			return err
		}
		if err := writeJSON(w.root, tokenPath(hook.Token), tokenRecord{HookID: hook.HookID}); err != nil {
			return err
		}
	}
	return writeJSON(w.root, runPath(run.RunID), run)
}

func (w *World) EventsCreate(ctx context.Context, input worlds.EventsCreateInput) (worlds.EventResult, error) {
	if input.Event.EventType == worlds.EventTypeHookCreated {
		return w.createHook(ctx, input)
	}
	if err := ctx.Err(); err != nil {
		return worlds.EventResult{}, err
	}
	if !validAppend(input.Event.EventType) {
		return worlds.EventResult{}, worlds.ErrEventInvalid
	}
	unlock, err := w.writeLock()
	if err != nil {
		return worlds.EventResult{}, err
	}
	defer unlock()
	t, err := w.begin(input.RunID)
	if err != nil {
		return worlds.EventResult{}, err
	}
	if err := t.fence(input.Options.InBandCount); err != nil {
		return worlds.EventResult{}, err
	}
	now := time.Now().UTC()
	replay, err := t.admit(input.Event, now)
	if err != nil {
		return worlds.EventResult{}, err
	}
	delta, err := w.delta(t.run, input.Options)
	if err != nil {
		return worlds.EventResult{}, err
	}
	if replay != "" {
		result := worlds.EventResult{EventID: replay, EventType: input.Event.EventType, Events: delta, InBandCount: t.run.InBandCount}
		if earlier, ok, err := t.load(eventSlot(worlds.Event{EventID: replay})); err != nil {
			return worlds.EventResult{}, err
		} else if ok {
			result.CreatedAt, result.Data = earlier.CreatedAt, earlier.Data
		}
		if input.Event.EventType == worlds.EventTypeStepCreated {
			step, err := t.step(worlds.StepID(input.Event.CorrelationID))
			if err != nil {
				return worlds.EventResult{}, err
			}
			if step != nil {
				value, err := stepView(*step, t.load, worlds.ResolveDataAll)
				if err != nil {
					return worlds.EventResult{}, err
				}
				result.Step = &value
			}
		}
		return result, nil
	}
	event, err := t.append(input.Event, eventTime(input.Event, input.Options.InBandCount, now))
	if err != nil {
		return worlds.EventResult{}, err
	}
	t.advance(input.Options.InBandCount, 1)
	if err := t.commit(); err != nil {
		return worlds.EventResult{}, err
	}
	w.signal()
	return t.result(event, delta), nil
}

// EventsCreateBatch stages each admitted event in one txn and commits them
// together. A refused event is left out without touching the staged state,
// so the others keep their own outcomes.
func (w *World) EventsCreateBatch(ctx context.Context, input worlds.EventsCreateBatchInput) (worlds.EventBatchResult, error) {
	if err := ctx.Err(); err != nil {
		return worlds.EventBatchResult{}, err
	}
	for _, value := range input.Events {
		if !validAppend(value.EventType) || value.EventType == worlds.EventTypeHookCreated {
			return worlds.EventBatchResult{}, worlds.ErrEventInvalid
		}
	}
	unlock, err := w.writeLock()
	if err != nil {
		return worlds.EventBatchResult{}, err
	}
	defer unlock()
	t, err := w.begin(input.RunID)
	if err != nil {
		return worlds.EventBatchResult{}, err
	}
	if err := t.fence(input.InBandCount); err != nil {
		return worlds.EventBatchResult{}, err
	}
	result := worlds.EventBatchResult{Results: make([]worlds.EventBatchItem, len(input.Events))}
	if input.EventCount != nil {
		if result.Events, err = w.delta(t.run, worlds.EventsCreateOptions{EventCount: input.EventCount}); err != nil {
			return worlds.EventBatchResult{}, err
		}
	}
	// A refused event refuses the later events of its correlation ID.
	refused := map[string]bool{}
	for index, value := range input.Events {
		if value.CorrelationID != "" && refused[value.CorrelationID] {
			result.Results[index].Err = fmt.Errorf("%w: an earlier event of %s was refused", worlds.ErrStepNotStartable, value.CorrelationID)
			continue
		}
		now := time.Now().UTC()
		replay, err := t.admit(value, now)
		if err == nil && replay != "" {
			err = fmt.Errorf("%w: batch repeats %s %s", worlds.ErrEventInvalid, value.EventType, value.CorrelationID)
		}
		if err != nil && !refusal(err) {
			return worlds.EventBatchResult{}, err
		}
		if err != nil {
			result.Results[index].Err = err
			refused[value.CorrelationID] = value.CorrelationID != ""
			continue
		}
		event, err := t.append(value, eventTime(value, input.InBandCount, now))
		if err != nil {
			return worlds.EventBatchResult{}, err
		}
		result.Results[index].EventResult = t.result(event, nil)
		result.AllocatedEventCount++
	}
	t.advance(input.InBandCount, result.AllocatedEventCount)
	if err := t.commit(); err != nil {
		return worlds.EventBatchResult{}, err
	}
	result.InBandCount = t.run.InBandCount
	for index := range result.Results {
		result.Results[index].InBandCount = result.InBandCount
	}
	if result.AllocatedEventCount > 0 {
		w.signal()
	}
	return result, nil
}

func (w *World) EventsGet(ctx context.Context, input worlds.EventsGetInput) (worlds.Event, error) {
	if err := ctx.Err(); err != nil {
		return worlds.Event{}, err
	}
	unlock, err := w.readLock()
	if err != nil {
		return worlds.Event{}, err
	}
	defer unlock()
	run, err := w.readRun(input.RunID)
	if err != nil {
		return worlds.Event{}, err
	}
	slot, err := worlds.EventSlot(input.EventID)
	if err != nil || int(slot) > run.EventCount {
		return worlds.Event{}, worlds.ErrEventNotFound
	}
	events, err := w.readEvents(input.RunID, int(slot), int(slot))
	if err != nil {
		return worlds.Event{}, err
	}
	return worlds.ResolveEvents(events, input.ResolveData)[0], nil
}

func (w *World) EventsList(ctx context.Context, input worlds.EventsListInput) (worlds.EventsPage, error) {
	// Pages are by slot. The cursor is the last returned event ID, in either
	// order.
	if err := ctx.Err(); err != nil {
		return worlds.EventsPage{}, err
	}
	unlock, err := w.readLock()
	if err != nil {
		return worlds.EventsPage{}, err
	}
	defer unlock()
	run, err := w.readRun(input.RunID)
	if err != nil {
		return worlds.EventsPage{}, err
	}
	// The run record holds both counters, read with the page under the lock.
	snapshot := worlds.EventsSnapshot{EventCount: run.EventCount, InBandCount: run.InBandCount}
	cursor, err := eventCursor(input.Pagination.Cursor)
	if err != nil {
		return worlds.EventsPage{}, err
	}
	limit := input.Pagination.Limit
	if input.Pagination.SortOrder == worlds.SortOrderDescending {
		high := run.EventCount
		if cursor > 0 {
			high = min(cursor-1, run.EventCount)
		}
		low := 1
		if limit > 0 {
			low = max(high-limit+1, 1)
		}
		events, err := w.readEvents(input.RunID, low, high)
		if err != nil {
			return worlds.EventsPage{}, err
		}
		for i, j := 0, len(events)-1; i < j; i, j = i+1, j-1 {
			events[i], events[j] = events[j], events[i]
		}
		return worlds.EventsPage{Page: eventsPage(worlds.ResolveEvents(events, input.ResolveData), low > 1), Snapshot: snapshot}, nil
	}
	low := cursor + 1
	high := run.EventCount
	if limit > 0 {
		high = min(cursor+limit, run.EventCount)
	}
	events, err := w.readEvents(input.RunID, low, high)
	if err != nil {
		return worlds.EventsPage{}, err
	}
	return worlds.EventsPage{Page: eventsPage(worlds.ResolveEvents(events, input.ResolveData), high < run.EventCount), Snapshot: snapshot}, nil
}

func (w *World) EventsListByCorrelationID(ctx context.Context, input worlds.EventsListByCorrelationIDInput) (worlds.Page[worlds.Event], error) {
	if err := ctx.Err(); err != nil {
		return worlds.Page[worlds.Event]{}, err
	}
	unlock, err := w.readLock()
	if err != nil {
		return worlds.Page[worlds.Event]{}, err
	}
	defer unlock()
	run, err := w.readRun(input.RunID)
	if err != nil {
		return worlds.Page[worlds.Event]{}, err
	}
	cursor, err := eventCursor(input.Pagination.Cursor)
	if err != nil {
		return worlds.Page[worlds.Event]{}, err
	}
	all, err := w.readEvents(input.RunID, 1, run.EventCount)
	if err != nil {
		return worlds.Page[worlds.Event]{}, err
	}
	descending := input.Pagination.SortOrder == worlds.SortOrderDescending
	matched := make([]worlds.Event, 0)
	for index := range all {
		if descending {
			index = len(all) - 1 - index
		}
		event := all[index]
		slot := index + 1
		if event.CorrelationID != input.CorrelationID || cursor > 0 && (descending && slot >= cursor || !descending && slot <= cursor) {
			continue
		}
		matched = append(matched, event)
	}
	hasMore := false
	if input.Pagination.Limit > 0 && len(matched) > input.Pagination.Limit {
		matched, hasMore = matched[:input.Pagination.Limit], true
	}
	return eventsPage(worlds.ResolveEvents(matched, input.ResolveData), hasMore), nil
}

// refusal reports whether err is an event's refusal rather than a storage
// failure.
func refusal(err error) bool {
	for _, target := range []error{worlds.ErrEventInvalid, worlds.ErrRunTerminal, worlds.ErrStepNotStartable, worlds.ErrHookNotActive, worlds.ErrHookForceClaimed, worlds.ErrHookConflict} {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}

func validAppend(eventType worlds.EventType) bool {
	return eventType != worlds.EventTypeRunCreated && eventType.Valid()
}

func eventsPage(events []worlds.Event, hasMore bool) worlds.Page[worlds.Event] {
	page := worlds.Page[worlds.Event]{Data: events, HasMore: hasMore && len(events) > 0}
	if page.HasMore {
		page.Cursor = string(events[len(events)-1].EventID)
	}
	return page
}

// EventsSubscribe pushes the run's events after afterSlot as this process
// commits them. Another process's writes reach it when this one next
// commits, or through the reader's own reads.
func (w *World) EventsSubscribe(ctx context.Context, runID worlds.RunID, afterSlot uint64, onEvent func(worlds.Event), onError func(error)) func() {
	return worlds.FeedEvents(ctx, w, runID, afterSlot, func() (<-chan struct{}, func()) { return w.changed(), func() {} }, onEvent, onError)
}
