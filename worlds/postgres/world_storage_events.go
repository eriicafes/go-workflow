package postgresworld

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
	"github.com/jackc/pgx/v5"
)

type runCreatedData struct {
	WorkflowName string            `json:"workflowName"`
	Input        json.RawMessage   `json:"input"`
	Attributes   map[string]string `json:"attributes"`
	DeploymentID string            `json:"deploymentId,omitempty"`
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
	attributes := input.Attributes
	if attributes == nil {
		attributes = map[string]string{}
	}
	encodedAttributes, err := json.Marshal(attributes)
	if err != nil {
		return worlds.EventResult{}, err
	}
	at := now()
	err = w.inTx(ctx, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `INSERT INTO go_workflow_runs (run_id, workflow_name, status, input, attributes, deployment_id, event_count, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, 1, $7, $7) ON CONFLICT (run_id) DO NOTHING`,
			input.RunID, input.WorkflowName, worlds.StatusPending, input.Input, encodedAttributes, input.DeploymentID, at)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return worlds.ErrRunAlreadyExists
		}
		_, err = tx.Exec(ctx, `INSERT INTO go_workflow_events (run_id, slot, type, correlation_id, data, created_at) VALUES ($1, 1, $2, '', $3, $4)`,
			input.RunID, worlds.EventTypeRunCreated, payload, at)
		return err
	})
	if err != nil {
		return worlds.EventResult{}, err
	}
	return worlds.EventResult{EventID: worlds.EventIDForSlot(1), EventType: worlds.EventTypeRunCreated, AllocatedEventCount: 1}, nil
}

func (w *World) EventsCreate(ctx context.Context, input worlds.EventsCreateInput) (worlds.EventResult, error) {
	if err := ctx.Err(); err != nil {
		return worlds.EventResult{}, err
	}
	if !validEvent(input.Event) {
		return worlds.EventResult{}, worlds.ErrEventInvalid
	}
	var result worlds.EventResult
	err := w.inTx(ctx, func(tx pgx.Tx) error {
		a, err := lockRun(ctx, tx, input.RunID)
		if err != nil {
			return err
		}
		replay, err := a.admit(ctx, input.Event)
		if err != nil {
			return err
		}
		delta, err := a.delta(ctx, input.Options)
		if err != nil {
			return err
		}
		if replay != nil {
			result = *replay
			result.Events = delta
			return nil
		}
		event, err := a.append(ctx, input.Event)
		if err != nil {
			return err
		}
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

func (w *World) EventsCreateBatch(ctx context.Context, input worlds.EventsCreateBatchInput) (worlds.EventBatchResult, error) {
	if err := ctx.Err(); err != nil {
		return worlds.EventBatchResult{}, err
	}
	if len(input.Events) == 0 {
		return worlds.EventBatchResult{}, nil
	}
	var results []worlds.EventResult
	err := w.inTx(ctx, func(tx pgx.Tx) error {
		results = make([]worlds.EventResult, 0, len(input.Events))
		a, err := lockRun(ctx, tx, input.RunID)
		if err != nil {
			return err
		}
		for _, value := range input.Events {
			if !validEvent(value) {
				return worlds.ErrEventInvalid
			}
			replay, err := a.admit(ctx, value)
			if err != nil {
				return err
			}
			if replay != nil {
				return worlds.ErrEventInvalid
			}
			event, err := a.append(ctx, value)
			if err != nil {
				return err
			}
			result, err := a.result(ctx, event, true)
			if err != nil {
				return err
			}
			results = append(results, result)
		}
		return a.flush(ctx)
	})
	if err != nil {
		return worlds.EventBatchResult{}, err
	}
	return worlds.EventBatchResult{Events: results}, nil
}

func (w *World) EventsGet(ctx context.Context, input worlds.EventsGetInput) (worlds.Event, error) {
	slot, err := worlds.EventSlot(input.EventID)
	if err == nil {
		var event worlds.Event
		event, err = scanEvent(w.pool.QueryRow(ctx, `SELECT `+eventColumns(input.ResolveData)+` FROM go_workflow_events WHERE run_id = $1 AND slot = $2`, input.RunID, int64(slot)))
		if err == nil {
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
	if err := w.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM go_workflow_runs WHERE run_id = $1)`, runID).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return worlds.ErrRunNotFound
	}
	return nil
}

func (w *World) EventsList(ctx context.Context, input worlds.EventsListInput) (worlds.Page[worlds.Event], error) {
	return w.listEvents(ctx, input.RunID, nil, input.Pagination, input.ResolveData)
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
		q.where("correlation_id = " + q.arg(*correlationID))
	}
	descending := pagination.SortOrder == worlds.SortOrderDescending
	if pagination.Cursor != "" {
		slot, err := worlds.EventSlot(worlds.EventID(pagination.Cursor))
		if err != nil {
			return worlds.Page[worlds.Event]{}, errInvalidCursor
		}
		if descending {
			q.where("slot < " + q.arg(int64(slot)))
		} else {
			q.where("slot > " + q.arg(int64(slot)))
		}
	}
	sql := `SELECT ` + eventColumns(resolve) + ` FROM go_workflow_events` + q.clause() + " ORDER BY slot"
	if descending {
		sql += " DESC"
	}
	rows, err := w.pool.Query(ctx, sql+q.limit(pagination.Limit), q.args...)
	if err != nil {
		return worlds.Page[worlds.Event]{}, err
	}
	events, err := collect(rows, scanEvent)
	if err != nil {
		return worlds.Page[worlds.Event]{}, err
	}
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
// row lock. Admission reads see the transaction's own earlier appends, so a
// batch is admitted against its in-progress state.
type appender struct {
	tx          pgx.Tx
	run         worlds.Run
	count       uint64
	wasTerminal bool
}

// admit applies the transition rules. It returns a non-nil result when the
// event is an idempotent replay of an earlier one.
func (a *appender) admit(ctx context.Context, event worlds.EventsCreateEvent) (*worlds.EventResult, error) {
	if a.run.Status.Terminal() {
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
	case worlds.EventTypeStepStarted, worlds.EventTypeStepCancelled:
		var status worlds.Status
		err := a.tx.QueryRow(ctx, `SELECT status FROM go_workflow_steps WHERE run_id = $1 AND step_id = $2`, a.run.RunID, event.CorrelationID).Scan(&status)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, worlds.ErrStepNotStartable
		}
		if err != nil {
			return nil, err
		}
		if event.EventType == worlds.EventTypeStepStarted {
			// A running step may be started again, because its previous attempt's
			// delivery may have died, and each start records a new attempt.
			if status != worlds.StatusPending && status != worlds.StatusRunning {
				return nil, worlds.ErrStepNotStartable
			}
		} else {
			if status == worlds.StatusCancelled {
				return a.replay(ctx, event.EventType, event.CorrelationID, true)
			}
			if status.Terminal() {
				return nil, worlds.ErrStepNotStartable
			}
		}
	case worlds.EventTypeWaitCompleted:
		result, err := a.replay(ctx, event.EventType, event.CorrelationID, false)
		if err != nil || result.EventID != "" {
			return result, err
		}
	case worlds.EventTypeHookReceived:
		var received worlds.WebhookReceivedEventData
		if json.Unmarshal(event.Data, &received) != nil || (received.Request == nil && received.Response == nil) {
			return nil, nil
		}
		hook, found, err := loadHook(ctx, a.tx, worlds.HookID(event.CorrelationID))
		if err != nil {
			return nil, err
		}
		if !found || hook.Webhook == nil {
			return nil, worlds.ErrHookNotActive
		}
		owner, err := tokenOwner(ctx, a.tx, hook.Token)
		if err != nil {
			return nil, err
		}
		if owner != hook.HookID {
			return nil, worlds.ErrHookNotActive
		}
		if received.Request != nil && hook.ReceivedEventID != "" {
			return nil, worlds.ErrHookNotActive
		}
		if received.Response != nil && (hook.ReceivedEventID == "" || received.ResponseTo != hook.ReceivedEventID) {
			return nil, worlds.ErrHookNotActive
		}
	case worlds.EventTypeHookCreated:
		var created hookCreatedData
		if json.Unmarshal(event.Data, &created) != nil {
			return nil, nil
		}
		holder, err := lockToken(ctx, a.tx, created.Token, now())
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
// eventType, matching correlationID when it is set. A missing event yields
// an empty result.
func (a *appender) replay(ctx context.Context, eventType worlds.EventType, correlationID string, latest bool) (*worlds.EventResult, error) {
	direction := "ASC"
	if latest {
		direction = "DESC"
	}
	var slot int64
	err := a.tx.QueryRow(ctx, `SELECT slot FROM go_workflow_events WHERE run_id = $1 AND type = $2 AND ($3 = '' OR correlation_id = $3)
		ORDER BY slot `+direction+` LIMIT 1`, a.run.RunID, eventType, correlationID).Scan(&slot)
	if errors.Is(err, pgx.ErrNoRows) {
		return &worlds.EventResult{}, nil
	}
	if err != nil {
		return nil, err
	}
	return &worlds.EventResult{EventID: worlds.EventIDForSlot(uint64(slot)), EventType: eventType}, nil
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
	rows, err := a.tx.Query(ctx, `SELECT `+eventColumns(options.ResolveData)+` FROM go_workflow_events WHERE run_id = $1 AND slot > $2 ORDER BY slot`, a.run.RunID, int64(start))
	if err != nil {
		return nil, err
	}
	return collect(rows, scanEvent)
}

func (a *appender) append(ctx context.Context, value worlds.EventsCreateEvent) (worlds.Event, error) {
	a.count++
	event := worlds.Event{RunID: a.run.RunID, EventID: worlds.EventIDForSlot(a.count), EventType: value.EventType, CorrelationID: value.CorrelationID, Data: value.Data, CreatedAt: now()}
	if _, err := a.tx.Exec(ctx, `INSERT INTO go_workflow_events (run_id, slot, type, correlation_id, data, created_at) VALUES ($1, $2, $3, $4, $5, $6)`,
		event.RunID, int64(a.count), event.EventType, event.CorrelationID, event.Data, event.CreatedAt); err != nil {
		return worlds.Event{}, err
	}
	return event, a.materialize(ctx, event)
}

func (a *appender) materialize(ctx context.Context, event worlds.Event) error {
	at := event.CreatedAt
	a.run.UpdatedAt = at
	switch event.EventType {
	case worlds.EventTypeRunStarted:
		a.run.Status, a.run.StartedAt = worlds.StatusRunning, &at
	case worlds.EventTypeRunCompleted:
		a.run.Status, a.run.CompletedAt, a.run.Output = worlds.StatusCompleted, &at, event.Data
	case worlds.EventTypeRunFailed:
		a.run.Status, a.run.CompletedAt, a.run.Error = worlds.StatusFailed, &at, event.Data
	case worlds.EventTypeRunCancelled:
		a.run.Status, a.run.CompletedAt = worlds.StatusCancelled, &at
	case worlds.EventTypeAttrSet:
		var value attrSetData
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
		var value stepCreatedData
		_ = json.Unmarshal(event.Data, &value)
		_, err := a.tx.Exec(ctx, `INSERT INTO go_workflow_steps (run_id, step_id, step_name, status, input, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $6)
			ON CONFLICT (run_id, step_id) DO UPDATE SET step_name = excluded.step_name, status = excluded.status, input = excluded.input,
				output = NULL, error = NULL, attempt = 0, created_at = excluded.created_at, updated_at = excluded.updated_at,
				started_at = NULL, completed_at = NULL, retry_after = NULL`,
			event.RunID, event.CorrelationID, value.StepName, worlds.StatusPending, []byte(value.Input), at)
		return err
	case worlds.EventTypeStepStarted:
		return a.updateStep(ctx, event, worlds.StatusRunning)
	case worlds.EventTypeStepCompleted:
		return a.updateStep(ctx, event, worlds.StatusCompleted)
	case worlds.EventTypeStepFailed:
		return a.updateStep(ctx, event, worlds.StatusFailed)
	case worlds.EventTypeStepRetrying:
		return a.updateStep(ctx, event, worlds.StatusPending)
	case worlds.EventTypeStepCancelled:
		return a.updateStep(ctx, event, worlds.StatusCancelled)
	case worlds.EventTypeHookCreated:
		return a.createHook(ctx, event)
	case worlds.EventTypeHookReceived:
		return a.receiveHook(ctx, event)
	case worlds.EventTypeHookDisposed:
		hook, found, err := loadHook(ctx, a.tx, worlds.HookID(event.CorrelationID))
		if err != nil || !found {
			return err
		}
		return releaseToken(ctx, a.tx, hook)
	}
	return nil
}

func (a *appender) updateStep(ctx context.Context, event worlds.Event, status worlds.Status) error {
	var result stepResultData
	parsed := json.Unmarshal(event.Data, &result) == nil
	_, err := a.tx.Exec(ctx, `UPDATE go_workflow_steps SET status = $3::text, updated_at = $4,
			attempt = attempt + CASE WHEN $3::text = 'running' THEN 1 ELSE 0 END,
			started_at = CASE WHEN $3::text = 'running' THEN $4 ELSE started_at END,
			completed_at = CASE WHEN $3::text IN ('completed', 'failed') THEN $4 ELSE completed_at END,
			output = CASE WHEN $5::boolean THEN $6::bytea ELSE output END,
			error = CASE WHEN $5::boolean THEN $7::bytea ELSE error END,
			retry_after = CASE WHEN $5::boolean THEN $8::timestamptz ELSE retry_after END
		WHERE run_id = $1 AND step_id = $2`,
		event.RunID, event.CorrelationID, status, event.CreatedAt, parsed, []byte(result.Output), []byte(result.Error), result.RetryAfter)
	return err
}

func (a *appender) createHook(ctx context.Context, event worlds.Event) error {
	var value hookCreatedData
	_ = json.Unmarshal(event.Data, &value)
	var webhook []byte
	if value.Webhook != nil {
		encoded, err := json.Marshal(value.Webhook)
		if err != nil {
			return err
		}
		webhook = encoded
	}
	if _, err := a.tx.Exec(ctx, `INSERT INTO go_workflow_hooks (hook_id, run_id, token, metadata, webhook, created_at, updated_at, min_retention)
		VALUES ($1, $2, $3, $4, $5, $6, $6, $7)
		ON CONFLICT (hook_id) DO UPDATE SET run_id = excluded.run_id, token = excluded.token, metadata = excluded.metadata,
			webhook = excluded.webhook, received_event_id = '', created_at = excluded.created_at, updated_at = excluded.updated_at,
			received_at = NULL, min_retention = excluded.min_retention, retain_until = NULL`,
		event.CorrelationID, event.RunID, value.Token, value.Metadata, webhook, event.CreatedAt, int64(value.MinRetention)); err != nil {
		return err
	}
	if value.Token == "" {
		return nil
	}
	// The token row did not exist when admission checked it, so a concurrent
	// claimant may have inserted it since. Losing that race is a conflict.
	tag, err := a.tx.Exec(ctx, `INSERT INTO go_workflow_hook_tokens (token, hook_id) VALUES ($1, $2)
		ON CONFLICT (token) DO UPDATE SET hook_id = excluded.hook_id WHERE go_workflow_hook_tokens.hook_id = excluded.hook_id`,
		value.Token, event.CorrelationID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return worlds.ErrHookConflict
	}
	return nil
}

func (a *appender) receiveHook(ctx context.Context, event worlds.Event) error {
	hook, found, err := loadHook(ctx, a.tx, worlds.HookID(event.CorrelationID))
	if err != nil || !found {
		return err
	}
	at := event.CreatedAt
	hook.UpdatedAt, hook.ReceivedAt = at, &at
	complete := false
	var value worlds.WebhookReceivedEventData
	if json.Unmarshal(event.Data, &value) == nil {
		if value.Request != nil {
			hook.ReceivedEventID = event.EventID
		}
		if value.Response != nil && value.ResponseTo == hook.ReceivedEventID {
			complete = true
		}
	}
	if hook.Webhook == nil || (!hook.Webhook.Manual && hook.ReceivedEventID != "") {
		complete = true
	}
	if complete {
		if hook.MinRetention <= 0 {
			hook.RetainUntil = nil
			if err := releaseToken(ctx, a.tx, hook); err != nil {
				return err
			}
		} else {
			until := at.Add(hook.MinRetention)
			hook.RetainUntil = &until
		}
	}
	_, err = a.tx.Exec(ctx, `UPDATE go_workflow_hooks SET updated_at = $2, received_at = $3, received_event_id = $4, retain_until = $5 WHERE hook_id = $1`,
		hook.HookID, hook.UpdatedAt, hook.ReceivedAt, hook.ReceivedEventID, hook.RetainUntil)
	return err
}

func (a *appender) result(ctx context.Context, event worlds.Event, created bool) (worlds.EventResult, error) {
	result := worlds.EventResult{EventID: event.EventID, EventType: event.EventType}
	if created {
		result.AllocatedEventCount = 1
	}
	if event.EventType != worlds.EventTypeStepCreated {
		return result, nil
	}
	step, err := getStep(ctx, a.tx, event.RunID, worlds.StepID(event.CorrelationID))
	if errors.Is(err, worlds.ErrStepNotFound) {
		return result, nil
	}
	if err != nil {
		return worlds.EventResult{}, err
	}
	result.Step, result.StepCreated = &step, created
	return result, nil
}

// flush writes the run view and slot counter, notifying waiters when the run
// became terminal. The notification is delivered on commit.
func (a *appender) flush(ctx context.Context) error {
	attributes, err := json.Marshal(a.run.Attributes)
	if err != nil {
		return err
	}
	if _, err := a.tx.Exec(ctx, `UPDATE go_workflow_runs SET status = $2, output = $3, error = $4, attributes = $5, updated_at = $6,
			started_at = $7, completed_at = $8, event_count = $9 WHERE run_id = $1`,
		a.run.RunID, a.run.Status, a.run.Output, a.run.Error, attributes, a.run.UpdatedAt, a.run.StartedAt, a.run.CompletedAt, int64(a.count)); err != nil {
		return err
	}
	if !a.wasTerminal && a.run.Status.Terminal() {
		if _, err := a.tx.Exec(ctx, `SELECT pg_notify($1, $2)`, channelRuns, string(a.run.RunID)); err != nil {
			return err
		}
	}
	return nil
}

func eventColumns(resolve worlds.EventsResolveData) string {
	if resolve == worlds.EventsResolveDataNone {
		return "run_id, slot, type, correlation_id, NULL::bytea, created_at"
	}
	return "run_id, slot, type, correlation_id, data, created_at"
}

func scanEvent(row pgx.Row) (worlds.Event, error) {
	var event worlds.Event
	var slot int64
	if err := row.Scan(&event.RunID, &slot, &event.EventType, &event.CorrelationID, &event.Data, &event.CreatedAt); err != nil {
		return worlds.Event{}, err
	}
	event.EventID = worlds.EventIDForSlot(uint64(slot))
	event.CreatedAt = event.CreatedAt.UTC()
	return event, nil
}

func validEvent(event worlds.EventsCreateEvent) bool {
	return event.EventType != worlds.EventTypeRunCreated && event.EventType.Valid()
}

func lockRun(ctx context.Context, tx pgx.Tx, runID worlds.RunID) (*appender, error) {
	a := &appender{tx: tx}
	var attributes []byte
	var count int64
	err := tx.QueryRow(ctx, `SELECT run_id, status, output, error, attributes, updated_at, started_at, completed_at, event_count
		FROM go_workflow_runs WHERE run_id = $1 FOR UPDATE`, runID).
		Scan(&a.run.RunID, &a.run.Status, &a.run.Output, &a.run.Error, &attributes, &a.run.UpdatedAt, &a.run.StartedAt, &a.run.CompletedAt, &count)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, worlds.ErrRunNotFound
	}
	if err != nil {
		return nil, err
	}
	a.run.Attributes = map[string]string{}
	if err := json.Unmarshal(attributes, &a.run.Attributes); err != nil {
		return nil, err
	}
	a.count = uint64(count)
	a.wasTerminal = a.run.Status.Terminal()
	return a, nil
}
