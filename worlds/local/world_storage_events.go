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

// txn stages appends to one run inside a write transaction. Records are
// loaded lazily, and commit writes the appended events and the records they
// changed. A refused append or batch is discarded by dropping the txn, and
// the write transaction commits everything together.
type txn struct {
	s        *store
	run      runRecord
	appended []worlds.Event
	// digests holds the payload digest of each appended deduplicated resume
	// by its slot.
	digests map[int]string
	steps   map[worlds.StepID]*stepRecord
	// hooks holds the staged hooks, nil for one the txn deleted.
	hooks map[worlds.HookID]*hookRecord
	// disposals holds the hooks the txn disposed.
	disposals map[worlds.HookID]*disposalRecord
	tokens    map[string]*tokenRecord
	waits     map[string]*worlds.Wait
	dirty     map[any]bool
	// purged reports that the commit deleted the run's data.
	purged bool
}

type (
	stepKey  worlds.StepID
	hookKey  worlds.HookID
	tokenKey string
	waitKey  string
)

func newTxn(s *store, run runRecord) *txn {
	run.Attributes = maps.Clone(run.Attributes)
	if run.Attributes == nil {
		run.Attributes = map[string]string{}
	}
	return &txn{s: s, run: run, digests: map[int]string{}, steps: map[worlds.StepID]*stepRecord{}, hooks: map[worlds.HookID]*hookRecord{}, disposals: map[worlds.HookID]*disposalRecord{}, tokens: map[string]*tokenRecord{}, waits: map[string]*worlds.Wait{}, dirty: map[any]bool{}}
}

// begin stages appends to a run the store's tag sees.
func begin(s *store, runID worlds.RunID) (*txn, error) {
	run, err := s.getRun(runID)
	if err != nil {
		return nil, err
	}
	return newTxn(s, run), nil
}

func (t *txn) step(id worlds.StepID) (*stepRecord, error) {
	if value, ok := t.steps[id]; ok {
		return value, nil
	}
	value, err := t.s.getStep(t.run.RunID, id)
	if err == nil {
		t.steps[id] = value
	}
	return value, err
}

func (t *txn) hook(id worlds.HookID) (*hookRecord, error) {
	if value, ok := t.hooks[id]; ok {
		return value, nil
	}
	value, err := t.s.getHookAnyTag(id)
	if err == nil {
		t.hooks[id] = value
	}
	return value, err
}

// disposal returns the hook's disposal, or nil when it was not disposed.
func (t *txn) disposal(id worlds.HookID) (*disposalRecord, error) {
	if value, ok := t.disposals[id]; ok {
		return value, nil
	}
	return t.s.getDisposal(id)
}

func (t *txn) token(token string) (*tokenRecord, error) {
	if value, ok := t.tokens[token]; ok {
		return value, nil
	}
	value, err := t.s.getToken(token)
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

// deleteHook deletes the hook, as world-local deletes a hook once disposed or
// done with its token.
func (t *txn) deleteHook(id worlds.HookID) {
	t.hooks[id], t.dirty[hookKey(id)] = nil, true
}

func (t *txn) setToken(token string, value *tokenRecord) {
	t.tokens[token], t.dirty[tokenKey(token)] = value, true
}

func (t *txn) setWait(value *worlds.Wait, correlationID string) {
	t.waits[correlationID], t.dirty[waitKey(correlationID)] = value, true
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
		// A disposed hook is gone, so disposing it again finds no hook, as
		// in world-local.
		hook, err := t.hook(worlds.HookID(event.CorrelationID))
		if err != nil {
			return "", err
		}
		if hook == nil || hook.RunID != t.run.RunID {
			return "", worlds.ErrHookNotFound
		}

	case worlds.EventTypeHookReceived:
		// A disposed hook refuses deliveries, redirecting them when another
		// run took its token, as world-local's refuseDisposedHookDelivery.
		disposal, err := t.disposal(worlds.HookID(event.CorrelationID))
		if err != nil {
			return "", err
		}
		if disposal != nil && disposal.RunID == t.run.RunID {
			if disposal.ForceClaimedBy != nil {
				return "", worlds.ErrHookForceClaimed
			}
			return "", worlds.ErrHookNotFound
		}
		hook, err := t.hook(worlds.HookID(event.CorrelationID))
		if err != nil {
			return "", err
		}
		if hook == nil || hook.RunID != t.run.RunID {
			return "", worlds.ErrHookNotFound
		}
		owner, err := t.token(hook.Token)
		if err != nil {
			return "", err
		}
		if owner == nil || owner.HookID != hook.HookID {
			return "", worlds.ErrHookNotFound
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

// correlated returns the run's eventType events for correlationID,
// committed then staged.
func (t *txn) correlated(correlationID string) ([]worlds.Event, error) {
	committed, err := t.s.correlated(t.run.RunID, correlationID, 0, false, 0)
	if err != nil {
		return nil, err
	}
	for _, event := range t.appended {
		if event.CorrelationID == correlationID {
			committed = append(committed, event)
		}
	}
	return committed, nil
}

// findEvent returns the ID of the run's first eventType event for
// correlationID, committed or staged, or "" when it has none.
func (t *txn) findEvent(eventType worlds.EventType, correlationID string) (worlds.EventID, error) {
	events, err := t.correlated(correlationID)
	if err != nil {
		return "", err
	}
	for _, event := range events {
		if event.EventType == eventType {
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
		SpecVersion:   worlds.EventSpecVersion(value.SpecVersion),
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
		run.ErrorCode = worlds.RunFailedErrorCode(event.Data)
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
		t.setStep(&stepRecord{Step: worlds.Step{RunID: run.RunID, StepID: id, StepName: value.StepName, Status: worlds.StatusPending, SpecVersion: event.SpecVersion, CreatedAt: now, UpdatedAt: now}, InputSlot: eventSlot(event)})
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
				t.setStep(&stepRecord{Step: worlds.Step{RunID: run.RunID, StepID: id, StepName: value.StepName, Status: worlds.StatusPending, SpecVersion: event.SpecVersion, CreatedAt: now, UpdatedAt: now}, InputSlot: eventSlot(event)})
			}
		}
		return t.updateStep(event, worlds.StatusRunning)
	case worlds.EventTypeStepCompleted:
		return t.updateStep(event, worlds.StatusCompleted)
	case worlds.EventTypeStepFailed:
		return t.updateStep(event, worlds.StatusFailed)
	case worlds.EventTypeStepRetrying:
		return t.updateStep(event, worlds.StatusPending)
	case worlds.EventTypeWaitCreated, worlds.EventTypeWaitCompleted:
		if !run.Status.Terminal() {
			events, err := t.correlated(event.CorrelationID)
			if err != nil {
				return err
			}
			if wait := worlds.WaitOfEvents(run.RunID, event.CorrelationID, events); wait != nil {
				t.setWait(wait, event.CorrelationID)
			}
		}
	case worlds.EventTypeHookCreated:
		var value worlds.DataHookCreated
		_ = json.Unmarshal(event.Data, &value)
		id := worlds.HookID(event.CorrelationID)
		hook := &hookRecord{Hook: worlds.Hook{RunID: run.RunID, ResumeContext: worlds.HookResumeContext{DeploymentID: run.DeploymentID, WorkflowName: run.WorkflowName, EncryptionPublicKey: run.EncryptionPublicKey, RunSpecVersion: run.SpecVersion}, HookID: id, Token: value.Token, IsWebhook: value.IsWebhook, TokenRetentionUntil: value.TokenRetentionUntil, ClaimedFrom: value.ForceClaimedFrom, SpecVersion: event.SpecVersion, CreatedAt: now}, MetadataSlot: eventSlot(event)}
		t.setHook(hook)
		if value.Token != "" {
			// A previous owner whose claim lapsed is deleted with its token.
			previous, err := t.token(value.Token)
			if err != nil {
				return err
			}
			if previous != nil && previous.HookID != id {
				t.deleteHook(previous.HookID)
			}
			t.setToken(value.Token, &tokenRecord{HookID: id, RunID: run.RunID})
		}
	case worlds.EventTypeHookDisposed:
		id := worlds.HookID(event.CorrelationID)
		var disposed worlds.HookDisposedEventData
		_ = json.Unmarshal(event.Data, &disposed)
		t.disposals[id] = &disposalRecord{RunID: run.RunID, ForceClaimedBy: disposed.ForceClaimedBy}
		hook, err := t.hook(id)
		if err != nil || hook == nil {
			return err
		}
		t.deleteHook(id)
		return t.releaseToken(hook.Token, hook.HookID)
	}

	if event.EventType == worlds.EventTypeRunCompleted || event.EventType == worlds.EventTypeRunFailed || event.EventType == worlds.EventTypeRunCancelled {
		// A hook whose token is retained stays until its retention ends,
		// holding the token; every other hook of the run is deleted and its
		// token released, as world-local's releaseRunResources.
		hooks, err := t.s.runHooks(run.RunID)
		if err != nil {
			return err
		}
		for _, hook := range hooks {
			if staged, ok := t.hooks[hook.HookID]; ok {
				if staged == nil {
					continue
				}
				hook = *staged
			}
			owner, err := t.token(hook.Token)
			if err != nil {
				return err
			}
			owns := owner != nil && owner.HookID == hook.HookID
			if hook.TokenRetentionUntil != nil && hook.TokenRetentionUntil.After(now) {
				if owns {
					t.setToken(hook.Token, &tokenRecord{HookID: hook.HookID, RunID: hook.RunID, TokenRetentionUntil: hook.TokenRetentionUntil})
				}
				continue
			}
			if owns {
				t.setToken(hook.Token, nil)
			}
			t.deleteHook(hook.HookID)
		}
	}
	return nil
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

// commit writes the appended events, the records they changed, and the run
// in the write transaction. When they finish a run with $retention "0", the
// run's data is deleted with them.
func (t *txn) commit() error {
	if len(t.appended) == 0 {
		return nil
	}
	purge := t.applyRetention(time.Now().UTC())
	for _, event := range t.appended {
		if err := t.s.insertEvent(event); err != nil {
			return err
		}
		if event.ResumeID != "" {
			if err := t.s.putResume(event, t.digests[eventSlot(event)]); err != nil {
				return err
			}
		}
	}
	for key := range t.dirty {
		var err error
		switch key := key.(type) {
		case stepKey:
			err = t.s.putStep(t.steps[worlds.StepID(key)])
		case hookKey:
			if hook := t.hooks[worlds.HookID(key)]; hook != nil {
				err = t.s.putHook(hook)
			} else {
				err = t.s.deleteHook(worlds.HookID(key))
			}
		case tokenKey:
			err = t.s.putToken(string(key), t.tokens[string(key)])
		case waitKey:
			if !t.run.Status.Terminal() {
				err = t.s.putWait(*t.waits[string(key)])
			}
		}
		if err != nil {
			return err
		}
	}
	for id, disposal := range t.disposals {
		if err := t.s.putDisposal(id, disposal); err != nil {
			return err
		}
	}
	if t.run.Status.Terminal() {
		// A run's waits go when it ends.
		if err := t.s.deleteWaits(t.run.RunID); err != nil {
			return err
		}
	}
	t.run.EventCount += len(t.appended)
	if err := t.s.putRun(t.run); err != nil {
		return err
	}
	if purge {
		t.purged = true
		return t.s.purgeRun(t.run)
	}
	return nil
}

// applyRetention deletes the payloads of the appended events of a run whose
// data is gone, as a step still running when its run's data was deleted
// settles after it. When the appends finish a run with $retention "0", it
// sets the run's ExpiredAt and reports that the rest of the run's data is to
// be deleted with them.
func (t *txn) applyRetention(now time.Time) bool {
	finished := false
	for _, event := range t.appended {
		switch event.EventType {
		case worlds.EventTypeRunCompleted, worlds.EventTypeRunFailed, worlds.EventTypeRunCancelled:
			finished = true
		}
	}
	purge := finished && worlds.ReadRunRetention(t.run.Attributes) == worlds.RetainNone && t.run.ExpiredAt == nil
	if purge {
		t.run.ExpiredAt = &now
	}
	if purge || worlds.RunDataExpired(t.run.Run, now) {
		for index, event := range t.appended {
			t.appended[index].Data = worlds.ScrubEventData(event.EventType, event.Data)
		}
	}
	return purge
}

// fence returns ErrInBandSuperseded when an in-band write's count is not the
// run's current InBandCount.
func (t *txn) fence(count *int) error {
	if count != nil && *count != t.run.InBandCount {
		return worlds.ErrInBandSuperseded
	}
	return nil
}

// advance stages appended more in-band events on the run, which commits them
// with the events.
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
	return t.s.readEvent(t.run.RunID, slot)
}

// result returns the result of appending event. A step_created or
// hook_created result carries the view as event left it, with the payload
// already in hand.
func (t *txn) result(event worlds.Event, delta []worlds.Event) (worlds.EventResult, error) {
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
	if event.EventType == worlds.EventTypeWaitCreated || event.EventType == worlds.EventTypeWaitCompleted {
		events, err := t.correlated(event.CorrelationID)
		if err != nil {
			return worlds.EventResult{}, err
		}
		result.Wait = worlds.WaitOfEvents(t.run.RunID, event.CorrelationID, events)
	}
	return result, nil
}

// delta returns the committed events selected by options.
func (s *store) delta(run runRecord, options worlds.EventsCreateOptions) ([]worlds.Event, error) {
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
	events, err := s.readEvents(run.RunID, start+1, run.EventCount)
	return worlds.ResolveEvents(events, options.ResolveData), err
}

// refused is a refusal a write commits: the transaction keeps what it
// recorded, such as a start hook candidate's permanent rejection, and the
// call returns err.
type refused struct{ err error }

func (r *refused) Error() string { return r.err.Error() }

// commitRefusal runs fn in a write transaction, committing it when fn returns
// nil or a *refused, whose error it then returns.
func (w *World) commitRefusal(ctx context.Context, fn func(*store) error) error {
	var refusal *refused
	err := w.write(ctx, func(s *store) error {
		refusal = nil
		err := fn(s)
		if errors.As(err, &refusal) {
			return nil
		}
		return err
	})
	if err == nil && refusal != nil {
		return refusal.err
	}
	return err
}

func (w *World) EventsCreateRun(ctx context.Context, input worlds.EventsCreateRunInput) (worlds.EventResult, error) {
	if input.RunID == "" {
		return worlds.EventResult{}, worlds.ErrEventInvalid
	}
	var result worlds.EventResult
	err := w.commitRefusal(ctx, func(s *store) error {
		var err error
		result, err = w.createRun(s, input, time.Now().UTC())
		return err
	})
	return result, err
}

// createRun creates a run and its run_created event, with its start hook. A
// start hook candidate the World refuses returns a *refused, which commits
// the refusal.
func (w *World) createRun(s *store, input worlds.EventsCreateRunInput, now time.Time) (worlds.EventResult, error) {
	if existing, err := s.getRunAnyTag(input.RunID); err == nil {
		// A repeat of an accepted start returns its run_created.
		created, found, err := s.readEvent(existing.RunID, 1)
		if err != nil {
			return worlds.EventResult{}, err
		}
		if found && worlds.RepeatsAcceptedStart(created.Data, input) {
			return worlds.EventResult{EventID: created.EventID, EventType: created.EventType, CreatedAt: created.CreatedAt, InBandCount: 1}, nil
		}
		return worlds.EventResult{}, worlds.ErrRunAlreadyExists
	} else if !errors.Is(err, worlds.ErrRunNotFound) {
		return worlds.EventResult{}, err
	}
	payload, err := worlds.RunCreatedData(input)
	if err != nil {
		return worlds.EventResult{}, err
	}
	attributes := maps.Clone(input.Attributes)
	if attributes == nil {
		attributes = map[string]string{}
	}
	run := runRecord{Run: worlds.Run{RunID: input.RunID, WorkflowName: input.WorkflowName, Status: worlds.StatusPending, Attributes: attributes, DeploymentID: input.DeploymentID, EncryptionPublicKey: input.EncryptionPublicKey, ExecutionContext: input.ExecutionContext, InBandCount: 1, SpecVersion: worlds.SpecVersion, CreatedAt: now, UpdatedAt: now, StartHook: worlds.RunEchoStartHook(payload)}, EventCount: 1}
	event := worlds.Event{RunID: input.RunID, EventID: worlds.EventIDForSlot(1), EventType: worlds.EventTypeRunCreated, Data: payload, CreatedAt: now, SpecVersion: worlds.SpecVersion}
	if start := input.StartHook; start != nil {
		if start.Token == "" {
			return worlds.EventResult{}, worlds.ErrEventInvalid
		}
		if decision, err := s.getRejection(input.RunID); err != nil || decision != nil {
			if err == nil {
				err = decision.Err()
			}
			return worlds.EventResult{}, err
		}
		if worlds.StartHookCandidateExpired(input.RunID, now) {
			return worlds.EventResult{}, worlds.ErrStartHookRejected
		}
		if start.TokenRetentionUntil != nil && start.TokenRetentionUntil.After(now.Add(w.hookRetentionLimit)) {
			return worlds.EventResult{}, fmt.Errorf("%w: hook retention cannot exceed %s in the local World", worlds.ErrStartHookRejected, w.hookRetentionLimit)
		}
		// The start hook claims its token with the run, or no run is
		// created, and the candidate's refusal is permanent.
		owner, err := s.getToken(start.Token)
		if err != nil {
			return worlds.EventResult{}, err
		}
		if owner.held(now) {
			decision := worlds.StartHookDecision{Token: start.Token, ConflictingRunID: owner.RunID}
			if err := s.putRejection(input.RunID, decision); err != nil {
				return worlds.EventResult{}, err
			}
			return worlds.EventResult{}, &refused{decision.Err()}
		}
		hook := &hookRecord{Hook: worlds.Hook{RunID: input.RunID, HookID: worlds.StartHookID(input.RunID), Token: start.Token, TokenRetentionUntil: start.TokenRetentionUntil, ResumeContext: worlds.HookResumeContext{DeploymentID: input.DeploymentID, WorkflowName: input.WorkflowName, EncryptionPublicKey: input.EncryptionPublicKey, RunSpecVersion: worlds.SpecVersion}, SpecVersion: worlds.SpecVersion, CreatedAt: now}}
		if owner != nil {
			// The previous owner's claim lapsed: it goes with its token.
			if err := s.deleteHook(owner.HookID); err != nil {
				return worlds.EventResult{}, err
			}
		}
		if err := s.putHook(hook); err != nil {
			return worlds.EventResult{}, err
		}
		if err := s.putToken(start.Token, &tokenRecord{HookID: hook.HookID, RunID: input.RunID}); err != nil {
			return worlds.EventResult{}, err
		}
	}
	if err := s.insertEvent(event); err != nil {
		return worlds.EventResult{}, err
	}
	if err := s.putRun(run); err != nil {
		return worlds.EventResult{}, err
	}
	// run_created takes the first position, which counts as in-band.
	return worlds.EventResult{EventID: event.EventID, EventType: event.EventType, CreatedAt: now, AllocatedEventCount: 1, InBandCount: 1}, nil
}

func (w *World) EventsCreate(ctx context.Context, input worlds.EventsCreateInput) (worlds.EventResult, error) {
	if input.Event.EventType == worlds.EventTypeHookCreated {
		return w.createHook(ctx, input)
	}
	if !validAppend(input.Event.EventType) {
		return worlds.EventResult{}, worlds.ErrEventInvalid
	}
	var result worlds.EventResult
	purged := false
	err := w.commitRefusal(ctx, func(s *store) error {
		var err error
		result, purged, err = w.appendEvent(s, input)
		return err
	})
	if purged {
		w.maintain(ctx)
	}
	return result, err
}

// appendEvent appends one event in the write transaction s.
func (w *World) appendEvent(s *store, input worlds.EventsCreateInput) (worlds.EventResult, bool, error) {
	now := time.Now().UTC()
	if input.Event.EventType == worlds.EventTypeRunStarted {
		// A first flow's run_started creates a run whose run_created never
		// landed, and is stored without the creation it carries.
		var created worlds.DataRunCreated
		if json.Unmarshal(input.Event.Data, &created) == nil && created.WorkflowName != "" {
			if _, err := s.getRunAnyTag(input.RunID); errors.Is(err, worlds.ErrRunNotFound) {
				creation := worlds.EventsCreateRunInput{RunID: input.RunID, WorkflowName: created.WorkflowName, Input: created.Input, Attributes: created.Attributes, DeploymentID: created.DeploymentID, EncryptionPublicKey: created.EncryptionPublicKey, ExecutionContext: created.ExecutionContext, StartHook: created.StartHook}
				if _, err := w.createRun(s, creation, now); err != nil && !errors.Is(err, worlds.ErrRunAlreadyExists) {
					return worlds.EventResult{}, false, err
				}
			} else if err != nil {
				return worlds.EventResult{}, false, err
			}
		}
		input.Event.Data = nil
	}
	t, err := begin(s, input.RunID)
	if err != nil {
		return worlds.EventResult{}, false, err
	}
	if err := t.fence(input.Options.InBandCount); err != nil {
		return worlds.EventResult{}, false, err
	}
	resumeID := ""
	if input.Event.EventType == worlds.EventTypeHookReceived {
		resumeID = input.Options.ResumeID
	}
	var replay worlds.EventID
	if digest, slot, ok, err := s.getResume(t.run.RunID, resumeID); err != nil {
		return worlds.EventResult{}, false, err
	} else if ok && resumeID != "" {
		// A repeat of a resume returns the event it committed.
		if digest != input.Options.ResumePayloadDigest {
			return worlds.EventResult{}, false, fmt.Errorf("%w: resume %s was sent with another payload", worlds.ErrEventInvalid, resumeID)
		}
		replay = worlds.EventIDForSlot(uint64(slot))
	} else if replay, err = t.admit(input.Event, now); err != nil {
		return worlds.EventResult{}, false, err
	}
	delta, err := s.delta(t.run, input.Options)
	if err != nil {
		return worlds.EventResult{}, false, err
	}
	if replay != "" {
		result := worlds.EventResult{EventID: replay, EventType: input.Event.EventType, Events: delta, InBandCount: t.run.InBandCount}
		if earlier, ok, err := t.load(eventSlot(worlds.Event{EventID: replay})); err != nil {
			return worlds.EventResult{}, false, err
		} else if ok {
			result.CreatedAt, result.Data = earlier.CreatedAt, earlier.Data
		}
		if input.Event.EventType == worlds.EventTypeStepCreated {
			step, err := t.step(worlds.StepID(input.Event.CorrelationID))
			if err != nil {
				return worlds.EventResult{}, false, err
			}
			if step != nil {
				value, err := stepView(*step, t.load, worlds.ResolveDataAll)
				if err != nil {
					return worlds.EventResult{}, false, err
				}
				result.Step = &value
			}
		}
		return result, false, nil
	}
	event, err := t.append(input.Event, eventTime(input.Event, input.Options.InBandCount, now))
	if err != nil {
		return worlds.EventResult{}, false, err
	}
	if resumeID != "" {
		t.appended[len(t.appended)-1].ResumeID, event.ResumeID = resumeID, resumeID
		t.digests[eventSlot(event)] = input.Options.ResumePayloadDigest
	}
	t.advance(input.Options.InBandCount, 1)
	if err := t.commit(); err != nil {
		return worlds.EventResult{}, false, err
	}
	result, err := t.result(event, delta)
	return result, t.purged, err
}

// EventsCreateBatch stages each admitted event in one txn and commits them
// together. A refused event is left out without touching the staged state,
// so the others keep their own outcomes.
func (w *World) EventsCreateBatch(ctx context.Context, input worlds.EventsCreateBatchInput) (worlds.EventBatchResult, error) {
	for _, value := range input.Events {
		if !validAppend(value.EventType) || value.EventType == worlds.EventTypeHookCreated {
			return worlds.EventBatchResult{}, worlds.ErrEventInvalid
		}
	}
	var result worlds.EventBatchResult
	purged := false
	err := w.write(ctx, func(s *store) error {
		var err error
		result, purged, err = appendBatch(s, input)
		return err
	})
	if purged {
		w.maintain(ctx)
	}
	return result, err
}

func appendBatch(s *store, input worlds.EventsCreateBatchInput) (worlds.EventBatchResult, bool, error) {
	t, err := begin(s, input.RunID)
	if err != nil {
		return worlds.EventBatchResult{}, false, err
	}
	if err := t.fence(input.InBandCount); err != nil {
		return worlds.EventBatchResult{}, false, err
	}
	result := worlds.EventBatchResult{Results: make([]worlds.EventBatchItem, len(input.Events))}
	if input.EventCount != nil {
		if result.Events, err = s.delta(t.run, worlds.EventsCreateOptions{EventCount: input.EventCount}); err != nil {
			return worlds.EventBatchResult{}, false, err
		}
	}
	// A refused event refuses the later events of its correlation ID.
	refusedIDs := map[string]bool{}
	for index, value := range input.Events {
		if value.CorrelationID != "" && refusedIDs[value.CorrelationID] {
			result.Results[index].Err = fmt.Errorf("%w: an earlier event of %s was refused", worlds.ErrStepNotStartable, value.CorrelationID)
			continue
		}
		now := time.Now().UTC()
		replay, err := t.admit(value, now)
		if err == nil && replay != "" {
			err = fmt.Errorf("%w: batch repeats %s %s", worlds.ErrEventInvalid, value.EventType, value.CorrelationID)
		}
		if err != nil && !refusal(err) {
			return worlds.EventBatchResult{}, false, err
		}
		if err != nil {
			result.Results[index].Err = err
			refusedIDs[value.CorrelationID] = value.CorrelationID != ""
			continue
		}
		event, err := t.append(value, eventTime(value, input.InBandCount, now))
		if err != nil {
			return worlds.EventBatchResult{}, false, err
		}
		if result.Results[index].EventResult, err = t.result(event, nil); err != nil {
			return worlds.EventBatchResult{}, false, err
		}
		result.AllocatedEventCount++
	}
	t.advance(input.InBandCount, result.AllocatedEventCount)
	if err := t.commit(); err != nil {
		return worlds.EventBatchResult{}, false, err
	}
	result.InBandCount = t.run.InBandCount
	for index := range result.Results {
		result.Results[index].InBandCount = result.InBandCount
	}
	return result, t.purged, nil
}

func (w *World) EventsGet(ctx context.Context, input worlds.EventsGetInput) (worlds.Event, error) {
	var event worlds.Event
	err := w.read(ctx, func(s *store) error {
		run, err := s.getRun(input.RunID)
		if err != nil {
			return err
		}
		slot, err := worlds.EventSlot(input.EventID)
		if err != nil || int(slot) > run.EventCount {
			return worlds.ErrEventNotFound
		}
		events, err := s.readEvents(input.RunID, int(slot), int(slot))
		if err != nil {
			return err
		}
		if len(events) == 0 {
			return worlds.ErrEventNotFound
		}
		event = worlds.ResolveEvents(events, input.ResolveData)[0]
		return nil
	})
	return event, err
}

func (w *World) EventsList(ctx context.Context, input worlds.EventsListInput) (worlds.EventsPage, error) {
	// Pages are by slot. The cursor is the last returned event ID, in either
	// order.
	var page worlds.EventsPage
	err := w.read(ctx, func(s *store) error {
		run, err := s.getRun(input.RunID)
		if err != nil {
			return err
		}
		// The run's row holds both counters, read in the page's snapshot.
		snapshot := worlds.EventsSnapshot{EventCount: run.EventCount, InBandCount: run.InBandCount}
		cursor, err := eventCursor(input.Pagination.Cursor)
		if err != nil {
			return err
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
			events, err := s.readEventsDescending(input.RunID, low, high)
			if err != nil {
				return err
			}
			page = worlds.EventsPage{Page: eventsPage(worlds.ResolveEvents(events, input.ResolveData), low > 1), Snapshot: snapshot}
			return nil
		}
		low := cursor + 1
		high := run.EventCount
		if limit > 0 {
			high = min(cursor+limit, run.EventCount)
		}
		events, err := s.readEvents(input.RunID, low, high)
		if err != nil {
			return err
		}
		page = worlds.EventsPage{Page: eventsPage(worlds.ResolveEvents(events, input.ResolveData), high < run.EventCount), Snapshot: snapshot}
		return nil
	})
	return page, err
}

func (w *World) EventsListByCorrelationID(ctx context.Context, input worlds.EventsListByCorrelationIDInput) (worlds.Page[worlds.Event], error) {
	var page worlds.Page[worlds.Event]
	err := w.read(ctx, func(s *store) error {
		if _, err := s.getRun(input.RunID); err != nil {
			return err
		}
		cursor, err := eventCursor(input.Pagination.Cursor)
		if err != nil {
			return err
		}
		limit := input.Pagination.Limit
		if limit > 0 {
			limit++
		}
		matched, err := s.correlated(input.RunID, input.CorrelationID, cursor, input.Pagination.SortOrder == worlds.SortOrderDescending, limit)
		if err != nil {
			return err
		}
		hasMore := false
		if input.Pagination.Limit > 0 && len(matched) > input.Pagination.Limit {
			matched, hasMore = matched[:input.Pagination.Limit], true
		}
		page = eventsPage(worlds.ResolveEvents(matched, input.ResolveData), hasMore)
		return nil
	})
	return page, err
}

// refusal reports whether err is an event's refusal rather than a storage
// failure.
func refusal(err error) bool {
	for _, target := range []error{worlds.ErrEventInvalid, worlds.ErrRunTerminal, worlds.ErrStepNotStartable, worlds.ErrHookNotFound, worlds.ErrHookNotActive, worlds.ErrHookForceClaimed, worlds.ErrHookConflict} {
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

// maintain returns the space a purge freed, after its commit.
func (w *World) maintain(ctx context.Context) {
	if db, err := w.usable(); err == nil {
		db.maintain(ctx)
	}
}
