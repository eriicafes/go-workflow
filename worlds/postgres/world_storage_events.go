package postgresworld

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
	"github.com/fxamacker/cbor/v2"
	"github.com/jackc/pgx/v5"
)

// startHookDecisionAge is how long a start hook candidate's decision is kept:
// past it, every attempt for the candidate is refused anyway.
const startHookDecisionAge = worlds.StartHookCandidateMaxAge

func (w *World) EventsCreateRun(ctx context.Context, input worlds.EventsCreateRunInput) (worlds.EventResult, error) {
	if err := ctx.Err(); err != nil {
		return worlds.EventResult{}, err
	}
	if input.RunID == "" {
		return worlds.EventResult{}, worlds.ErrEventInvalid
	}
	payload, err := worlds.RunCreatedData(input)
	if err != nil {
		return worlds.EventResult{}, err
	}
	data, err := worlds.EncodeEventData(worlds.EventTypeRunCreated, payload)
	if err != nil {
		return worlds.EventResult{}, err
	}
	attributes := input.Attributes
	if attributes == nil {
		attributes = map[string]string{}
	}
	encodedAttributes, err := json.Marshal(attributes)
	if err != nil {
		return worlds.EventResult{}, err
	}
	var executionContext []byte
	if len(input.ExecutionContext) > 0 {
		if executionContext, err = encodeJSONValue(input.ExecutionContext); err != nil {
			return worlds.EventResult{}, err
		}
	}
	at := now()
	var startHook []byte
	if echo := worlds.RunEchoStartHook(payload); echo != nil {
		if startHook, err = cbor.Marshal(echo); err != nil {
			return worlds.EventResult{}, err
		}
	}
	if start := input.StartHook; start != nil {
		// A candidate's decision is permanent.
		var decision, conflicting string
		err := w.pool.QueryRow(ctx, w.q(`SELECT decision, coalesce(conflicting_run_id, '') FROM workflow.workflow_start_hook_admissions WHERE run_id = $1`), input.RunID).Scan(&decision, &conflicting)
		if err == nil && decision == "rejected" {
			return worlds.EventResult{}, &worlds.HookConflictError{RunID: worlds.RunID(conflicting)}
		}
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return worlds.EventResult{}, err
		}
		if errors.Is(err, pgx.ErrNoRows) && worlds.StartHookCandidateExpired(input.RunID, at) {
			return worlds.EventResult{}, worlds.ErrStartHookRejected
		}
		if start.TokenRetentionUntil != nil && start.TokenRetentionUntil.After(at.Add(w.hookRetentionLimit)) {
			return worlds.EventResult{}, fmt.Errorf("%w: hook retention cannot exceed %s in the Postgres World", worlds.ErrStartHookRejected, w.hookRetentionLimit)
		}
	}
	var repeat *worlds.EventResult
	err = w.inTx(ctx, func(tx pgx.Tx) error {
		repeat = nil
		tag, err := tx.Exec(ctx, w.q(`INSERT INTO workflow.workflow_runs (id, name, status, attributes, deployment_id, encryption_public_key, execution_context_cbor, spec_version, created_at, updated_at, start_hook_cbor)
			VALUES ($1, $2, 'pending', $3, $4, $5, $6, $7, $8, $8, $9) ON CONFLICT (id) DO NOTHING`),
			input.RunID, input.WorkflowName, encodedAttributes, input.DeploymentID, nullable(input.EncryptionPublicKey), executionContext, worlds.SpecVersion, at, startHook)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			// A repeat of an accepted start returns its run_created.
			created, err := scanEvent(tx.QueryRow(ctx, w.q(`SELECT `+eventColumns+` FROM workflow.workflow_events WHERE run_id = $1 AND id = $2`), input.RunID, worlds.EventIDForSlot(1)))
			if err == nil && worlds.RepeatsAcceptedStart(created.Data, input) {
				repeat = &worlds.EventResult{EventID: created.EventID, EventType: created.EventType, CreatedAt: created.CreatedAt, InBandCount: 1}
				return nil
			}
			return worlds.ErrRunAlreadyExists
		}
		// run_created takes the first position, which counts as in-band.
		if _, err := tx.Exec(ctx, w.q(`INSERT INTO workflow.workflow_events (run_id, id, type, payload_cbor, created_at, spec_version) VALUES ($1, $2, $3, $4, $5, $6)`),
			input.RunID, worlds.EventIDForSlot(1), worlds.EventTypeRunCreated, data, at, worlds.SpecVersion); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, w.q(`INSERT INTO workflow.workflow_event_slots (run_id, seq_in_band) VALUES ($1, 1)`), input.RunID); err != nil {
			return err
		}
		if input.StartHook == nil {
			return nil
		}
		if err := w.createStartHook(ctx, tx, input, at); err != nil {
			return err
		}
		// Decisions past a candidate's window decide nothing.
		if _, err := tx.Exec(ctx, w.q(`DELETE FROM workflow.workflow_start_hook_admissions WHERE created_at < $1`), at.Add(-startHookDecisionAge)); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, w.q(`INSERT INTO workflow.workflow_start_hook_admissions (run_id, token, decision, created_at) VALUES ($1, $2, 'accepted', $3) ON CONFLICT (run_id) DO NOTHING`), input.RunID, input.StartHook.Token, at)
		return err
	})
	var conflict *worlds.HookConflictError
	if errors.As(err, &conflict) && input.StartHook != nil {
		// The refusal is permanent, recorded apart from the rolled-back
		// creation.
		if _, recordErr := w.pool.Exec(ctx, w.q(`INSERT INTO workflow.workflow_start_hook_admissions (run_id, token, decision, conflicting_run_id, created_at) VALUES ($1, $2, 'rejected', $3, $4) ON CONFLICT (run_id) DO NOTHING`),
			input.RunID, input.StartHook.Token, conflict.RunID, at); recordErr != nil {
			return worlds.EventResult{}, errors.Join(err, recordErr)
		}
	}
	if err != nil {
		return worlds.EventResult{}, err
	}
	if repeat != nil {
		return *repeat, nil
	}
	return worlds.EventResult{EventID: worlds.EventIDForSlot(1), EventType: worlds.EventTypeRunCreated, CreatedAt: at, AllocatedEventCount: 1, InBandCount: 1}, nil
}

func (w *World) EventsCreate(ctx context.Context, input worlds.EventsCreateInput) (worlds.EventResult, error) {
	if input.Event.EventType == worlds.EventTypeHookCreated {
		return w.createHook(ctx, input)
	}
	if err := ctx.Err(); err != nil {
		return worlds.EventResult{}, err
	}
	if !validEvent(input.Event) {
		return worlds.EventResult{}, worlds.ErrEventInvalid
	}
	if input.Event.EventType == worlds.EventTypeRunStarted {
		// A first flow's run_started creates a run whose run_created never
		// landed, and is stored without the creation it carries.
		if err := worlds.CreateRunForStart(ctx, w, input.RunID, input.Event.Data); err != nil {
			return worlds.EventResult{}, err
		}
		input.Event.Data = nil
	}
	var result worlds.EventResult
	err := w.inTx(ctx, func(tx pgx.Tx) error {
		a, err := w.lockRun(ctx, tx, input.RunID)
		if err != nil {
			return err
		}
		if err := a.fence(input.Options.InBandCount); err != nil {
			return err
		}
		resumeID := ""
		if input.Event.EventType == worlds.EventTypeHookReceived {
			resumeID = input.Options.ResumeID
		}
		replay, err := a.resumed(ctx, resumeID, input.Options.ResumePayloadDigest)
		if err != nil {
			return err
		}
		if replay == nil {
			if replay, err = a.admit(ctx, input.Event); err != nil {
				return err
			}
		}
		delta, err := a.delta(ctx, input.Options)
		if err != nil {
			return err
		}
		if replay != nil {
			result = *replay
			result.Events = delta
			result.InBandCount = a.inBandCount
			return nil
		}
		event, err := a.append(ctx, input.Event)
		if err != nil {
			return err
		}
		if resumeID != "" {
			if _, err := tx.Exec(ctx, w.q(`UPDATE workflow.workflow_events SET resume_id = $3, resume_payload_digest = $4 WHERE run_id = $1 AND id = $2`),
				event.RunID, event.EventID, resumeID, input.Options.ResumePayloadDigest); err != nil {
				return err
			}
			event.ResumeID = resumeID
		}
		a.advance(input.Options.InBandCount, 1)
		if result, err = a.result(ctx, event, true); err != nil {
			return err
		}
		result.Events = delta
		return a.flush(ctx)
	})
	if err != nil {
		return worlds.EventResult{}, err
	}
	return result, nil
}

// EventsCreateBatch appends a batch's events in one transaction. Each event
// is admitted against the log as the earlier events of the batch left it,
// before it is inserted, so a refused event appends nothing and the others
// are still appended.
func (w *World) EventsCreateBatch(ctx context.Context, input worlds.EventsCreateBatchInput) (worlds.EventBatchResult, error) {
	if err := ctx.Err(); err != nil {
		return worlds.EventBatchResult{}, err
	}
	for _, value := range input.Events {
		if !validEvent(value) || value.EventType == worlds.EventTypeHookCreated {
			return worlds.EventBatchResult{}, worlds.ErrEventInvalid
		}
	}
	var result worlds.EventBatchResult
	err := w.inTx(ctx, func(tx pgx.Tx) error {
		result = worlds.EventBatchResult{Results: make([]worlds.EventBatchItem, len(input.Events))}
		a, err := w.lockRun(ctx, tx, input.RunID)
		if err != nil {
			return err
		}
		if err := a.fence(input.InBandCount); err != nil {
			return err
		}
		if input.EventCount != nil {
			if result.Events, err = a.delta(ctx, worlds.EventsCreateOptions{EventCount: input.EventCount}); err != nil {
				return err
			}
		}
		refused := map[string]bool{}
		for index, value := range input.Events {
			item := &result.Results[index]
			if value.CorrelationID != "" && refused[value.CorrelationID] {
				item.Err = fmt.Errorf("%w: an earlier event of %s was refused", worlds.ErrStepNotStartable, value.CorrelationID)
				continue
			}
			replay, err := a.admit(ctx, value)
			if err != nil && !refusal(err) {
				return err
			}
			if err == nil && replay != nil {
				err = fmt.Errorf("%w: batch repeats %s %s", worlds.ErrEventInvalid, value.EventType, value.CorrelationID)
			}
			if err != nil {
				item.Err = err
				refused[value.CorrelationID] = value.CorrelationID != ""
				continue
			}
			event, err := a.append(ctx, value)
			if err != nil {
				return err
			}
			if item.EventResult, err = a.result(ctx, event, true); err != nil {
				return err
			}
			result.AllocatedEventCount++
		}
		a.advance(input.InBandCount, result.AllocatedEventCount)
		result.InBandCount = a.inBandCount
		for index := range result.Results {
			result.Results[index].InBandCount = result.InBandCount
		}
		return a.flush(ctx)
	})
	if err != nil {
		return worlds.EventBatchResult{}, err
	}
	return result, nil
}

// refusal reports whether err is an admission refusal, as opposed to a
// storage failure.
func refusal(err error) bool {
	for _, target := range []error{worlds.ErrEventInvalid, worlds.ErrRunTerminal, worlds.ErrStepNotStartable, worlds.ErrHookNotFound, worlds.ErrHookNotActive, worlds.ErrHookForceClaimed, worlds.ErrHookConflict} {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}

func (w *World) EventsGet(ctx context.Context, input worlds.EventsGetInput) (worlds.Event, error) {
	if _, err := worlds.EventSlot(input.EventID); err == nil {
		event, err := scanEvent(w.pool.QueryRow(ctx, w.q(`SELECT `+eventColumns+` FROM workflow.workflow_events WHERE run_id = $1 AND id = $2`), input.RunID, input.EventID))
		if err == nil {
			resolve := input.ResolveData
			if expired, err := w.runDataExpired(ctx, input.RunID); err != nil {
				return worlds.Event{}, err
			} else if expired {
				resolve = worlds.EventsResolveDataNone
			}
			event.Data = worlds.ResolveEventData(event.EventType, event.Data, resolve)
			return event, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return worlds.Event{}, err
		}
	}
	if err := w.runExists(ctx, input.RunID); err != nil {
		return worlds.Event{}, err
	}
	return worlds.Event{}, worlds.ErrEventNotFound
}

func (w *World) runExists(ctx context.Context, runID worlds.RunID) error {
	var exists bool
	if err := w.pool.QueryRow(ctx, w.q(`SELECT EXISTS (SELECT 1 FROM workflow.workflow_runs WHERE id = $1)`), runID).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return worlds.ErrRunNotFound
	}
	return nil
}

// EventsList reads the run's counters before its page, so the page covers
// every position the snapshot counts.
func (w *World) EventsList(ctx context.Context, input worlds.EventsListInput) (worlds.EventsPage, error) {
	var inBandCount int64
	var last *string
	err := w.pool.QueryRow(ctx, w.q(`SELECT s.seq_in_band, (SELECT max(e.id) FROM workflow.workflow_events e WHERE e.run_id = s.run_id)
		FROM workflow.workflow_event_slots s WHERE s.run_id = $1`), input.RunID).Scan(&inBandCount, &last)
	if errors.Is(err, pgx.ErrNoRows) {
		return worlds.EventsPage{}, worlds.ErrRunNotFound
	}
	if err != nil {
		return worlds.EventsPage{}, err
	}
	page, err := w.listEvents(ctx, input.RunID, nil, input.Pagination, input.ResolveData)
	if err != nil {
		return worlds.EventsPage{}, err
	}
	return worlds.EventsPage{Page: page, Snapshot: worlds.EventsSnapshot{EventCount: int(slotOf(last)), InBandCount: int(inBandCount)}}, nil
}

func (w *World) EventsListByCorrelationID(ctx context.Context, input worlds.EventsListByCorrelationIDInput) (worlds.Page[worlds.Event], error) {
	return w.listEvents(ctx, input.RunID, &input.CorrelationID, input.Pagination, input.ResolveData)
}

// listEvents pages a run's events by slot. The cursor is the last returned
// event ID in either direction.
func (w *World) listEvents(ctx context.Context, runID worlds.RunID, correlationID *string, pagination worlds.Pagination, resolve worlds.EventsResolveData) (worlds.Page[worlds.Event], error) {
	var q query
	q.where("run_id = " + q.arg(runID))
	if correlationID != nil {
		q.where("coalesce(correlation_id, '') = " + q.arg(*correlationID))
	}
	descending := pagination.SortOrder == worlds.SortOrderDescending
	if pagination.Cursor != "" {
		slot, err := worlds.EventSlot(worlds.EventID(pagination.Cursor))
		if err != nil {
			return worlds.Page[worlds.Event]{}, errInvalidCursor
		}
		if descending {
			q.where("id < " + q.arg(worlds.EventIDForSlot(slot)))
		} else {
			q.where("id > " + q.arg(worlds.EventIDForSlot(slot)))
		}
	}
	sql := `SELECT ` + eventColumns + ` FROM workflow.workflow_events` + q.clause() + " ORDER BY id"
	if descending {
		sql += " DESC"
	}
	rows, err := w.pool.Query(ctx, w.q(sql+q.limit(pagination.Limit)), q.args...)
	if err != nil {
		return worlds.Page[worlds.Event]{}, err
	}
	events, err := collect(rows, scanEvent)
	if err != nil {
		return worlds.Page[worlds.Event]{}, err
	}
	if expired, err := w.runDataExpired(ctx, runID); err != nil {
		return worlds.Page[worlds.Event]{}, err
	} else if expired {
		resolve = worlds.EventsResolveDataNone
	}
	events = worlds.ResolveEvents(events, resolve)
	if len(events) == 0 {
		if err := w.runExists(ctx, runID); err != nil {
			return worlds.Page[worlds.Event]{}, err
		}
	}
	if pagination.Limit <= 0 || len(events) <= pagination.Limit {
		return worlds.Page[worlds.Event]{Data: events}, nil
	}
	events = events[:pagination.Limit]
	return worlds.Page[worlds.Event]{Data: events, HasMore: true, Cursor: string(events[len(events)-1].EventID)}, nil
}

// appender appends events to one run inside a transaction holding that run's
// row and event slot locks. Admission reads see the transaction's own
// earlier appends, so a batch is admitted against its in-progress state.
type appender struct {
	w           *World
	tx          pgx.Tx
	run         worlds.Run
	count       uint64
	inBandCount int
	wasTerminal bool
	// inBand reports that the write passed the in-band fence, so its events
	// take their OccurredAt as their creation time.
	inBand bool
	// resultSlot is the run's run_completed or run_failed slot, and
	// errorCode its run_failed's code, which flush writes.
	resultSlot *int64
	errorCode  *string
}

// admit applies the transition rules. It returns a non-nil result when the
// event is an idempotent replay of an earlier one.
func (a *appender) admit(ctx context.Context, event worlds.EventsCreateEvent) (*worlds.EventResult, error) {
	var step *worlds.Step
	switch event.EventType {
	case worlds.EventTypeStepCreated, worlds.EventTypeStepStarted, worlds.EventTypeStepCompleted, worlds.EventTypeStepFailed, worlds.EventTypeStepRetrying:
		found, err := a.w.getStep(ctx, a.tx, a.run.RunID, worlds.StepID(event.CorrelationID))
		if err != nil && !errors.Is(err, worlds.ErrStepNotFound) {
			return nil, err
		}
		if err == nil {
			step = &found
		}
	}
	// A finished run still records what was running when it finished: a
	// step's final result, and a wait's completion.
	finishing := (event.EventType == worlds.EventTypeStepCompleted || event.EventType == worlds.EventTypeStepFailed) && step != nil && !step.Status.Terminal() ||
		event.EventType == worlds.EventTypeWaitCompleted
	if a.run.Status.Terminal() && !finishing {
		if event.EventType == worlds.EventTypeRunCancelled && a.run.Status == worlds.StatusCancelled {
			return a.replay(ctx, event.EventType, event.CorrelationID, true)
		}
		return nil, worlds.ErrRunTerminal
	}
	switch event.EventType {
	case worlds.EventTypeRunStarted:
		if a.run.Status == worlds.StatusRunning {
			return a.replay(ctx, event.EventType, "", true)
		}
	case worlds.EventTypeStepCreated:
		if step == nil {
			return nil, nil
		}
		result, err := a.replay(ctx, event.EventType, event.CorrelationID, false)
		if err == nil {
			result.Step = step
		}
		return result, err
	case worlds.EventTypeStepStarted:
		creates := worlds.StartCreatesStep(event.Data)
		switch {
		case creates && step != nil:
			// An inline step is created once.
			return a.replay(ctx, event.EventType, event.CorrelationID, false)
		case step == nil && !creates:
			return nil, worlds.ErrStepNotStartable
		case step != nil && step.Status.Terminal():
			// A started step may be started again, and each start records a
			// new attempt.
			return nil, worlds.ErrStepNotStartable
		}
	case worlds.EventTypeStepCompleted, worlds.EventTypeStepFailed, worlds.EventTypeStepRetrying:
		// A step's result is recorded while the step is not finished.
		if step == nil || step.Status.Terminal() {
			return nil, worlds.ErrStepNotStartable
		}
	case worlds.EventTypeWaitCompleted:
		result, err := a.replay(ctx, event.EventType, event.CorrelationID, false)
		if err != nil || result.EventID != "" {
			return result, err
		}

	case worlds.EventTypeHookReceived:
		hook, found, err := a.w.loadHook(ctx, a.tx, worlds.HookID(event.CorrelationID), worlds.ResolveDataNone)
		if err != nil {
			return nil, err
		}
		if !found {
			// A disposed hook is gone. One another run took over redirects
			// the delivery, as world-postgres's forceClaimRefusal.
			forced, err := a.forceClaimed(ctx, event.CorrelationID)
			if err != nil {
				return nil, err
			}
			if forced {
				return nil, worlds.ErrHookForceClaimed
			}
			return nil, worlds.ErrHookNotFound
		}
		if hook.RunID != a.run.RunID {
			return nil, worlds.ErrHookNotFound
		}
		owner, err := a.w.tokenOwner(ctx, a.tx, hook.Token)
		if err != nil {
			return nil, err
		}
		if owner != hook.HookID {
			return nil, worlds.ErrHookNotFound
		}

	case worlds.EventTypeHookDisposed:
		hook, found, err := a.w.loadHook(ctx, a.tx, worlds.HookID(event.CorrelationID), worlds.ResolveDataNone)
		if err != nil {
			return nil, err
		}
		// A disposed hook is gone, so disposing it again finds no hook, as
		// in world-postgres.
		if !found || hook.RunID != a.run.RunID {
			return nil, worlds.ErrHookNotFound
		}

	case worlds.EventTypeHookCreated:
		var created worlds.DataHookCreated
		if json.Unmarshal(event.Data, &created) != nil {
			return nil, nil
		}
		holder, err := a.w.lockToken(ctx, a.tx, created.Token, now())
		if err != nil {
			return nil, err
		}
		if holder.hookID != "" {
			return nil, worlds.ErrHookConflict
		}
	}
	return nil, nil
}

// replay finds the latest (or, when latest is false, the earliest) event of
// eventType for correlationID. A missing event yields an empty result.
// forceClaimed reports that the run's hook was disposed by another run's
// forced claim of its token: its hook_disposed names the claimant.
func (a *appender) forceClaimed(ctx context.Context, hookID string) (bool, error) {
	var data []byte
	err := a.tx.QueryRow(ctx, a.w.q(`SELECT payload_cbor FROM workflow.workflow_events WHERE run_id = $1 AND correlation_id = $2 AND type = $3 LIMIT 1`),
		a.run.RunID, hookID, worlds.EventTypeHookDisposed).Scan(&data)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	decoded, err := eventData(worlds.EventTypeHookDisposed, data)
	if err != nil {
		return false, err
	}
	var disposed worlds.HookDisposedEventData
	return len(decoded) > 0 && json.Unmarshal(decoded, &disposed) == nil && disposed.ForceClaimedBy != nil, nil
}

func (a *appender) replay(ctx context.Context, eventType worlds.EventType, correlationID string, latest bool) (*worlds.EventResult, error) {
	direction := "ASC"
	if latest {
		direction = "DESC"
	}
	var id worlds.EventID
	err := a.tx.QueryRow(ctx, a.w.q(`SELECT id FROM workflow.workflow_events WHERE run_id = $1 AND type = $2 AND coalesce(correlation_id, '') = $3
		ORDER BY id `+direction+` LIMIT 1`), a.run.RunID, eventType, correlationID).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return &worlds.EventResult{}, nil
	}
	if err != nil {
		return nil, err
	}
	return &worlds.EventResult{EventID: id, EventType: eventType}, nil
}

// delta returns the events selected by options from the log as it is before
// this call's append.
func (a *appender) delta(ctx context.Context, options worlds.EventsCreateOptions) ([]worlds.Event, error) {
	start := a.count
	if options.EventCount != nil {
		start = uint64(min(max(*options.EventCount, 0), int(a.count)))
	} else if options.SinceCursor != "" {
		if slot, err := worlds.EventSlot(worlds.EventID(options.SinceCursor)); err == nil {
			start = min(slot, a.count)
		}
	}
	if start >= a.count {
		return nil, nil
	}
	rows, err := a.tx.Query(ctx, a.w.q(`SELECT `+eventColumns+` FROM workflow.workflow_events WHERE run_id = $1 AND id > $2 ORDER BY id`), a.run.RunID, worlds.EventIDForSlot(start))
	if err != nil {
		return nil, err
	}
	events, err := collect(rows, scanEvent)
	return worlds.ResolveEvents(events, options.ResolveData), err
}

// createdAt returns the creation time of value: its OccurredAt for an
// in-band write that sets one, and now otherwise.
func (a *appender) createdAt(value worlds.EventsCreateEvent) time.Time {
	if a.inBand && !value.OccurredAt.IsZero() {
		return value.OccurredAt.UTC().Truncate(time.Microsecond)
	}
	return now()
}

func (a *appender) append(ctx context.Context, value worlds.EventsCreateEvent) (worlds.Event, error) {
	a.count++
	event := worlds.Event{RunID: a.run.RunID, EventID: worlds.EventIDForSlot(a.count), EventType: value.EventType, CorrelationID: value.CorrelationID, Data: value.Data, CreatedAt: a.createdAt(value), SpecVersion: worlds.EventSpecVersion(value.SpecVersion)}
	if worlds.RunDataExpired(a.run, event.CreatedAt) {
		// A step still running when its run's data was deleted settles after
		// it. What it brought is deleted as it lands.
		event.Data = worlds.ScrubEventData(event.EventType, event.Data)
	}
	data, err := worlds.EncodeEventData(event.EventType, event.Data)
	if err != nil {
		return worlds.Event{}, err
	}
	if _, err := a.tx.Exec(ctx, a.w.q(`INSERT INTO workflow.workflow_events (run_id, id, type, correlation_id, payload_cbor, created_at, spec_version) VALUES ($1, $2, $3, $4, $5, $6, $7)`),
		event.RunID, event.EventID, event.EventType, nullable(event.CorrelationID), data, event.CreatedAt, event.SpecVersion); err != nil {
		return worlds.Event{}, err
	}
	return event, a.materialize(ctx, event, int64(a.count))
}

// materialize applies event, appended at slot, to the views. Payloads stay in
// the event log, and the views record the slots holding them.
func (a *appender) materialize(ctx context.Context, event worlds.Event, slot int64) error {
	at := event.CreatedAt
	a.run.UpdatedAt = at
	switch event.EventType {
	case worlds.EventTypeRunStarted:
		a.run.Status, a.run.StartedAt = worlds.StatusRunning, &at
	case worlds.EventTypeRunCompleted:
		a.run.Status, a.run.CompletedAt, a.resultSlot = worlds.StatusCompleted, &at, &slot
	case worlds.EventTypeRunFailed:
		a.run.Status, a.run.CompletedAt, a.resultSlot = worlds.StatusFailed, &at, &slot
		if code := worlds.RunFailedErrorCode(event.Data); code != "" {
			a.run.ErrorCode, a.errorCode = code, &code
		}
	case worlds.EventTypeRunCancelled:
		a.run.Status, a.run.CompletedAt = worlds.StatusCancelled, &at
	case worlds.EventTypeAttrSet:
		var value worlds.DataAttrSet
		if json.Unmarshal(event.Data, &value) == nil {
			for _, change := range value.Changes {
				if change.Value == nil {
					delete(a.run.Attributes, change.Key)
				} else {
					a.run.Attributes[change.Key] = *change.Value
				}
			}
		}
	case worlds.EventTypeStepCreated:
		var value worlds.DataStepCreated
		_ = json.Unmarshal(event.Data, &value)
		_, err := a.tx.Exec(ctx, a.w.q(`INSERT INTO workflow.workflow_steps (run_id, step_id, step_name, status, input_slot, created_at, updated_at, spec_version)
			VALUES ($1, $2, $3, 'pending', $4, $5, $5, $6)
			ON CONFLICT (run_id, step_id) DO UPDATE SET step_name = excluded.step_name, status = excluded.status, input_slot = excluded.input_slot,
				result_slot = NULL, attempt = 0, created_at = excluded.created_at, updated_at = excluded.updated_at,
				started_at = NULL, completed_at = NULL, retry_after = NULL, spec_version = excluded.spec_version`),
			event.RunID, event.CorrelationID, value.StepName, slot, at, event.SpecVersion)
		return err
	case worlds.EventTypeStepStarted:
		if worlds.StartCreatesStep(event.Data) {
			// An inline step's first step_started creates it and holds its
			// input, as a step_created would.
			var value worlds.DataStepStarted
			_ = json.Unmarshal(event.Data, &value)
			if _, err := a.tx.Exec(ctx, a.w.q(`INSERT INTO workflow.workflow_steps (run_id, step_id, step_name, status, input_slot, created_at, updated_at, spec_version)
				VALUES ($1, $2, $3, 'pending', $4, $5, $5, $6)`),
				event.RunID, event.CorrelationID, value.StepName, slot, at, event.SpecVersion); err != nil {
				return err
			}
		}
		return a.updateStep(ctx, event, worlds.StatusRunning, slot)
	case worlds.EventTypeStepCompleted:
		return a.updateStep(ctx, event, worlds.StatusCompleted, slot)
	case worlds.EventTypeStepFailed:
		return a.updateStep(ctx, event, worlds.StatusFailed, slot)
	case worlds.EventTypeStepRetrying:
		return a.updateStep(ctx, event, worlds.StatusPending, slot)
	case worlds.EventTypeWaitCreated, worlds.EventTypeWaitCompleted:
		return a.updateWait(ctx, event)
	case worlds.EventTypeHookCreated:
		return a.createHook(ctx, event, slot)
	case worlds.EventTypeHookDisposed:
		// Disposal deletes the hook, which releases its token. The event
		// records the disposal and any forced claim.
		_, err := a.tx.Exec(ctx, a.w.q(`DELETE FROM workflow.workflow_hooks WHERE hook_id = $1`), event.CorrelationID)
		return err
	}
	return nil
}

// updateStep applies a step event at slot. A step_completed, step_failed, or
// step_retrying event becomes the step's result slot, holding its output or
// error.
func (a *appender) updateStep(ctx context.Context, event worlds.Event, status worlds.Status, slot int64) error {
	var result worlds.DataStepResult
	parsed := json.Unmarshal(event.Data, &result) == nil
	resulted := event.EventType != worlds.EventTypeStepStarted
	_, err := a.tx.Exec(ctx, a.w.q(`UPDATE workflow.workflow_steps SET status = $3::text::workflow.step_status, updated_at = $4,
			attempt = attempt + CASE WHEN $3::text = 'running' THEN 1 ELSE 0 END,
			started_at = CASE WHEN $3::text = 'running' THEN coalesce(started_at, $4) ELSE started_at END,
			completed_at = CASE WHEN $3::text IN ('completed', 'failed') THEN $4 ELSE completed_at END,
			result_slot = CASE WHEN $6::boolean THEN $7::bigint ELSE result_slot END,
			retry_after = CASE WHEN $5::boolean THEN $8::timestamptz ELSE retry_after END
		WHERE run_id = $1 AND step_id = $2`),
		event.RunID, event.CorrelationID, string(status), event.CreatedAt, parsed, resulted, slot, result.RetryAfter)
	return err
}

// updateWait writes the wait of a wait_created or wait_completed, as the
// run's events now record it. A finished run's waits are gone.
func (a *appender) updateWait(ctx context.Context, event worlds.Event) error {
	if a.run.Status.Terminal() {
		return nil
	}
	wait, err := a.wait(ctx, event)
	if err != nil || wait == nil {
		return err
	}
	_, err = a.tx.Exec(ctx, a.w.q(`INSERT INTO workflow.workflow_waits (wait_id, run_id, status, resume_at, completed_at, created_at, updated_at, spec_version)
		VALUES ($1, $2, $3::text::workflow.wait_status, $4, $5, $6, $7, $8)
		ON CONFLICT (wait_id) DO UPDATE SET status = excluded.status, resume_at = excluded.resume_at, completed_at = excluded.completed_at, updated_at = excluded.updated_at`),
		wait.WaitID, wait.RunID, string(wait.Status), wait.ResumeAt, wait.CompletedAt, wait.CreatedAt, wait.UpdatedAt, wait.SpecVersion)
	return err
}

// wait returns the wait of a wait event as the transaction's events record
// it, this one included.
func (a *appender) wait(ctx context.Context, event worlds.Event) (*worlds.Wait, error) {
	rows, err := a.tx.Query(ctx, a.w.q(`SELECT `+eventColumns+` FROM workflow.workflow_events WHERE run_id = $1 AND correlation_id = $2 AND type IN ($3, $4) ORDER BY id`),
		event.RunID, event.CorrelationID, worlds.EventTypeWaitCreated, worlds.EventTypeWaitCompleted)
	if err != nil {
		return nil, err
	}
	events, err := collect(rows, scanEvent)
	if err != nil {
		return nil, err
	}
	return worlds.WaitOfEvents(event.RunID, event.CorrelationID, events), nil
}

func (a *appender) createHook(ctx context.Context, event worlds.Event, slot int64) error {
	var value worlds.DataHookCreated
	_ = json.Unmarshal(event.Data, &value)
	claimed, _ := json.Marshal(value.ForceClaimedFrom)
	// The token had no holder when admission checked it, so a concurrent
	// claimant may have claimed it since. Losing that race is a conflict.
	resume := encodeResumeContext(worlds.HookResumeContext{DeploymentID: a.run.DeploymentID, WorkflowName: a.run.WorkflowName, EncryptionPublicKey: a.run.EncryptionPublicKey, RunSpecVersion: a.run.SpecVersion})
	tag, err := a.tx.Exec(ctx, a.w.q(`INSERT INTO workflow.workflow_hooks (hook_id, run_id, resume_context, token, claims_token, metadata_slot, is_webhook, is_system, created_at, token_retention_until, claimed_from, spec_version)
		VALUES ($1, $2, $3, $4, $12, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (token) WHERE claims_token DO NOTHING`),
		event.CorrelationID, event.RunID, resume, value.Token, slot, value.IsWebhook, value.IsSystem, event.CreatedAt, value.TokenRetentionUntil, claimed, event.SpecVersion, value.Token != "")
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return worlds.ErrHookConflict
	}
	return nil
}

// fence returns ErrInBandSuperseded when an in-band write's count is not the
// locked run's in-band count.
func (a *appender) fence(count *int) error {
	if count != nil && *count != a.inBandCount {
		return worlds.ErrInBandSuperseded
	}
	a.inBand = count != nil
	return nil
}

// advance adds appended in-band events to the fence, which flush writes.
func (a *appender) advance(count *int, appended int) {
	if count != nil {
		a.inBandCount += appended
	}
}

func (a *appender) result(ctx context.Context, event worlds.Event, created bool) (worlds.EventResult, error) {
	result := worlds.EventResult{EventID: event.EventID, EventType: event.EventType, CreatedAt: event.CreatedAt, Data: event.Data, InBandCount: a.inBandCount}
	if created {
		result.AllocatedEventCount = 1
	}
	if event.EventType == worlds.EventTypeHookCreated {
		hook, _, err := a.w.loadHook(ctx, a.tx, worlds.HookID(event.CorrelationID), worlds.ResolveDataNone)
		if err != nil {
			return worlds.EventResult{}, err
		}
		result.Hook = &hook
	}
	if event.EventType == worlds.EventTypeWaitCreated || event.EventType == worlds.EventTypeWaitCompleted {
		wait, err := a.wait(ctx, event)
		if err != nil {
			return worlds.EventResult{}, err
		}
		result.Wait = wait
	}
	if event.EventType != worlds.EventTypeStepCreated {
		return result, nil
	}
	step, err := a.w.getStep(ctx, a.tx, event.RunID, worlds.StepID(event.CorrelationID))
	if errors.Is(err, worlds.ErrStepNotFound) {
		return result, nil
	}
	if err != nil {
		return worlds.EventResult{}, err
	}
	result.Step, result.StepCreated = &step, created
	return result, nil
}

// flush writes the run view and its fence, notifying the run's event
// subscribers, and its waiters when the run became terminal. Notifications
// are delivered on commit.
func (a *appender) flush(ctx context.Context) error {
	attributes, err := json.Marshal(a.run.Attributes)
	if err != nil {
		return err
	}
	if _, err := a.tx.Exec(ctx, a.w.q(`UPDATE workflow.workflow_runs SET status = $2::text::workflow.status, result_slot = $3, attributes = $4, updated_at = $5,
			started_at = $6, completed_at = $7, error_code = coalesce($8, error_code) WHERE id = $1`),
		a.run.RunID, string(a.run.Status), a.resultSlot, attributes, a.run.UpdatedAt, a.run.StartedAt, a.run.CompletedAt, a.errorCode); err != nil {
		return err
	}
	if _, err := a.tx.Exec(ctx, a.w.q(`UPDATE workflow.workflow_event_slots SET seq_in_band = $2 WHERE run_id = $1`), a.run.RunID, a.inBandCount); err != nil {
		return err
	}
	notify(a.tx, channelEvents, string(a.run.RunID))
	if !a.wasTerminal && a.run.Status.Terminal() {
		// A run's waits go when it ends, and so do its hooks but those
		// whose token is retained, which stay until their retention ends.
		if _, err := a.tx.Exec(ctx, a.w.q(`DELETE FROM workflow.workflow_waits WHERE run_id = $1`), a.run.RunID); err != nil {
			return err
		}
		if _, err := a.tx.Exec(ctx, a.w.q(`DELETE FROM workflow.workflow_hooks WHERE run_id = $1 AND (token_retention_until IS NULL OR token_retention_until <= $2)`), a.run.RunID, now()); err != nil {
			return err
		}
		if err := a.expire(ctx); err != nil {
			return err
		}
		notify(a.tx, channelRuns, string(a.run.RunID))
	}
	return nil
}

// expire applies the retention of a run that has just finished. A run with
// $retention "0" loses its data in this transaction, before waiters are
// notified. Otherwise a Config.Retention for its workflow sets the run's
// expired_at, and Worker deletes its data once it has passed.
func (a *appender) expire(ctx context.Context) error {
	if worlds.ReadRunRetention(a.run.Attributes) == worlds.RetainNone {
		return a.w.purgeRunData(ctx, a.tx, a.run.RunID)
	}
	retention := a.w.retentionFor(a.run.WorkflowName)
	if retention <= 0 || a.run.CompletedAt == nil {
		return nil
	}
	_, err := a.tx.Exec(ctx, a.w.q(`UPDATE workflow.workflow_runs SET expired_at = $2 WHERE id = $1 AND expired_at IS NULL`), a.run.RunID, a.run.CompletedAt.Add(retention))
	return err
}

// eventColumns are the columns scanEvent reads. Data is always read, since
// worlds.ResolveEventData keeps its metadata when payloads are omitted.
const eventColumns = "run_id, id, type, coalesce(correlation_id, ''), payload_cbor, created_at, coalesce(spec_version, 0), coalesce(resume_id, '')"

func scanEvent(row pgx.Row) (worlds.Event, error) {
	var event worlds.Event
	var data []byte
	if err := row.Scan(&event.RunID, &event.EventID, &event.EventType, &event.CorrelationID, &data, &event.CreatedAt, &event.SpecVersion, &event.ResumeID); err != nil {
		return worlds.Event{}, err
	}
	var err error
	if event.Data, err = worlds.DecodeEventData(event.EventType, data); err != nil {
		return worlds.Event{}, fmt.Errorf("workflow/postgres: event %s of run %s: %w", event.EventID, event.RunID, err)
	}
	event.CreatedAt = event.CreatedAt.UTC()
	return event, nil
}

// eventData returns the JSON Data of an event's payload_cbor, or nil.
func eventData(eventType worlds.EventType, data []byte) ([]byte, error) {
	if data == nil {
		return nil, nil
	}
	return worlds.DecodeEventData(eventType, data)
}

// slotOf returns the slot of an event ID, or 0 for none.
func slotOf(id *string) uint64 {
	if id == nil {
		return 0
	}
	slot, _ := worlds.EventSlot(worlds.EventID(*id))
	return slot
}

func validEvent(event worlds.EventsCreateEvent) bool {
	return event.EventType != worlds.EventTypeRunCreated && event.EventType.Valid()
}

func nullable(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

// lockRun locks the run's row and its event slot row, and reads its next
// position from the log under them.
func (w *World) lockRun(ctx context.Context, tx pgx.Tx, runID worlds.RunID) (*appender, error) {
	a := &appender{w: w, tx: tx}
	var attributes []byte
	err := tx.QueryRow(ctx, w.q(`SELECT id, name, deployment_id, coalesce(encryption_public_key, ''), status::text, result_slot, attributes, updated_at, started_at, completed_at, expired_at, spec_version
		FROM workflow.workflow_runs WHERE id = $1 FOR UPDATE`), runID).
		Scan(&a.run.RunID, &a.run.WorkflowName, &a.run.DeploymentID, &a.run.EncryptionPublicKey, &a.run.Status, &a.resultSlot, &attributes, &a.run.UpdatedAt, &a.run.StartedAt, &a.run.CompletedAt, &a.run.ExpiredAt, &a.run.SpecVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, worlds.ErrRunNotFound
	}
	if err != nil {
		return nil, err
	}
	var last *string
	var inBandCount int64
	if err := tx.QueryRow(ctx, w.q(`SELECT seq_in_band, (SELECT max(e.id) FROM workflow.workflow_events e WHERE e.run_id = s.run_id)
		FROM workflow.workflow_event_slots s WHERE s.run_id = $1 FOR UPDATE OF s`), runID).Scan(&inBandCount, &last); err != nil {
		return nil, err
	}
	a.run.Attributes = map[string]string{}
	if err := json.Unmarshal(attributes, &a.run.Attributes); err != nil {
		return nil, err
	}
	a.count = slotOf(last)
	a.inBandCount = int(inBandCount)
	a.run.InBandCount = a.inBandCount
	a.wasTerminal = a.run.Status.Terminal()
	return a, nil
}

// createStartHook creates a run's start hook in the transaction creating the
// run, claiming its token, or returns a *HookConflictError naming the run
// holding it, which rolls the run back.
func (w *World) createStartHook(ctx context.Context, tx pgx.Tx, input worlds.EventsCreateRunInput, at time.Time) error {
	start := input.StartHook
	if start.Token == "" {
		return worlds.ErrEventInvalid
	}
	holder, err := w.lockToken(ctx, tx, start.Token, at)
	if err != nil {
		return err
	}
	if holder.hookID != "" {
		return &worlds.HookConflictError{RunID: holder.runID}
	}
	// metadata_slot 1 joins run_created, which holds the hook's metadata.
	resume := encodeResumeContext(worlds.HookResumeContext{DeploymentID: input.DeploymentID, WorkflowName: input.WorkflowName, EncryptionPublicKey: input.EncryptionPublicKey, RunSpecVersion: worlds.SpecVersion})
	tag, err := tx.Exec(ctx, w.q(`INSERT INTO workflow.workflow_hooks (hook_id, run_id, resume_context, token, claims_token, metadata_slot, is_webhook, created_at, token_retention_until, spec_version)
		VALUES ($1, $2, $3, $4, true, 1, false, $5, $6, $7)
		ON CONFLICT (token) WHERE claims_token DO NOTHING`), worlds.StartHookID(input.RunID), input.RunID, resume, start.Token, at, start.TokenRetentionUntil, worlds.SpecVersion)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		// A concurrent claimant claimed the token since it was checked.
		var owner worlds.RunID
		if err := tx.QueryRow(ctx, w.q(`SELECT run_id FROM workflow.workflow_hooks WHERE token = $1 AND claims_token`), start.Token).Scan(&owner); err != nil {
			return err
		}
		return &worlds.HookConflictError{RunID: owner}
	}
	return nil
}

// EventsSubscribe pushes the run's events after afterSlot as they commit,
// woken by the workflow_run_events notification each write sends.
func (w *World) EventsSubscribe(ctx context.Context, runID worlds.RunID, afterSlot uint64, onEvent func(worlds.Event), onError func(error)) func() {
	return worlds.FeedEvents(ctx, w, runID, afterSlot, func() (<-chan struct{}, func()) {
		return w.notifier.wait(channelEvents, string(runID))
	}, onEvent, onError)
}

// resumed returns the result of the hook_received the resume resumeID
// committed, or nil when it committed none. A repeat of the resume with
// another payload digest is refused.
func (a *appender) resumed(ctx context.Context, resumeID, digest string) (*worlds.EventResult, error) {
	if resumeID == "" {
		return nil, nil
	}
	var id worlds.EventID
	var committed string
	err := a.tx.QueryRow(ctx, a.w.q(`SELECT id, coalesce(resume_payload_digest, '') FROM workflow.workflow_events WHERE run_id = $1 AND resume_id = $2 AND type = $3`),
		a.run.RunID, resumeID, worlds.EventTypeHookReceived).Scan(&id, &committed)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if committed != digest {
		return nil, fmt.Errorf("%w: resume %s was sent with another payload", worlds.ErrEventInvalid, resumeID)
	}
	event, err := scanEvent(a.tx.QueryRow(ctx, a.w.q(`SELECT `+eventColumns+` FROM workflow.workflow_events WHERE run_id = $1 AND id = $2`), a.run.RunID, id))
	if err != nil {
		return nil, err
	}
	result, err := a.result(ctx, event, false)
	return &result, err
}
