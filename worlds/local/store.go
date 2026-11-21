package localworld

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/fxamacker/cbor/v2"

	"github.com/eriicafes/go-workflow/worlds"
)

// querier is a transaction of the database.
type querier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// store reads and writes the World's rows in one transaction. A tagged World
// sees the rows of its tag and the untagged ones; an untagged World sees only
// untagged rows. A run's steps, waits, and events belong to its tag.
type store struct {
	ctx context.Context
	q   querier
	tag string
}

// tagged returns the condition on column that limits rows to the store's tag,
// with its arguments.
func (s *store) tagged(column string) (string, []any) {
	if s.tag == "" {
		return column + " IS NULL", nil
	}
	return "(" + column + " IS NULL OR " + column + " = ?)", []any{s.tag}
}

// tagValue is the tag the store writes on new rows.
func (s *store) tagValue() any {
	if s.tag == "" {
		return nil
	}
	return s.tag
}

func (s *store) exec(query string, args ...any) error {
	_, err := s.q.ExecContext(s.ctx, query, args...)
	return err
}

// millis returns at as epoch milliseconds, as the columns hold times.
func millis(at time.Time) int64 { return at.UnixMilli() }

func nullable(value string) any {
	if value == "" {
		return nil
	}
	return value
}

// Runs.

// getRun returns the run, with its counters from their columns, or
// ErrRunNotFound when the store's tag cannot see it.
func (s *store) getRun(runID worlds.RunID) (runRecord, error) {
	condition, args := s.tagged("tag")
	return s.scanRun(s.q.QueryRowContext(s.ctx, "SELECT seq, seq_in_band, doc FROM runs WHERE run_id = ? AND "+condition, append([]any{string(runID)}, args...)...))
}

// getRunAnyTag returns the run whatever its tag, as a forced claim reads the
// run whose hook it takes the token from.
func (s *store) getRunAnyTag(runID worlds.RunID) (runRecord, error) {
	return s.scanRun(s.q.QueryRowContext(s.ctx, "SELECT seq, seq_in_band, doc FROM runs WHERE run_id = ?", string(runID)))
}

func (s *store) scanRun(row interface{ Scan(...any) error }) (runRecord, error) {
	var record runRecord
	var doc string
	if err := row.Scan(&record.EventCount, &record.InBandCount, &doc); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return runRecord{}, worlds.ErrRunNotFound
		}
		return runRecord{}, err
	}
	seq, inBand := record.EventCount, record.InBandCount
	if err := json.Unmarshal([]byte(doc), &record); err != nil {
		return runRecord{}, fmt.Errorf("workflow/local: decode run: %w", err)
	}
	record.EventCount, record.InBandCount = seq, inBand
	return record, nil
}

// putRun inserts or updates the run. An update keeps the run's tag.
func (s *store) putRun(record runRecord) error {
	doc, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return s.exec(`INSERT INTO runs (run_id, tag, workflow_name, status, created_at, seq, seq_in_band, doc) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (run_id) DO UPDATE SET status = excluded.status, seq = excluded.seq, seq_in_band = excluded.seq_in_band, doc = excluded.doc`,
		string(record.RunID), s.tagValue(), record.WorkflowName, string(record.Status), millis(record.CreatedAt), record.EventCount, record.InBandCount, string(doc))
}

// runFilter selects runs.
type runFilter struct {
	workflowName string
	statuses     []worlds.Status
	// active selects runs that have not finished.
	active bool
	// ownTag selects only the store's tag's runs, without the untagged ones
	// a tagged store also sees.
	ownTag bool
}

// listRuns returns the runs the store's tag sees, oldest first.
func (s *store) listRuns(filter runFilter) ([]runRecord, error) {
	condition, args := s.tagged("tag")
	if filter.ownTag && s.tag != "" {
		condition, args = "tag = ?", []any{s.tag}
	}
	query := "SELECT seq, seq_in_band, doc FROM runs WHERE " + condition
	if filter.workflowName != "" {
		query += " AND workflow_name = ?"
		args = append(args, filter.workflowName)
	}
	statuses := filter.statuses
	if filter.active {
		statuses = []worlds.Status{worlds.StatusPending, worlds.StatusRunning}
	}
	if len(statuses) > 0 {
		query += " AND status IN (" + strings.TrimSuffix(strings.Repeat("?, ", len(statuses)), ", ") + ")"
		for _, status := range statuses {
			args = append(args, string(status))
		}
	}
	rows, err := s.q.QueryContext(s.ctx, query+" ORDER BY created_at, run_id", args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var records []runRecord
	for rows.Next() {
		record, err := s.scanRun(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

// Events.

// eventDoc is an event's doc: its Data as worlds.EncodeEventData stores it,
// with what the event's columns do not hold.
type eventDoc struct {
	Data        cbor.RawMessage `cbor:"data,omitempty"`
	CreatedAt   time.Time       `cbor:"createdAt"`
	SpecVersion int             `cbor:"specVersion,omitempty"`
	ResumeID    string          `cbor:"resumeId,omitempty"`
}

var docEncoding = func() cbor.EncMode {
	mode, err := cbor.EncOptions{Time: cbor.TimeRFC3339Nano}.EncMode()
	if err != nil {
		panic(err)
	}
	return mode
}()

func encodeEvent(event worlds.Event) ([]byte, error) {
	data, err := worlds.EncodeEventData(event.EventType, event.Data)
	if err != nil {
		return nil, err
	}
	return docEncoding.Marshal(eventDoc{Data: data, CreatedAt: event.CreatedAt, SpecVersion: event.SpecVersion, ResumeID: event.ResumeID})
}

func decodeEvent(runID worlds.RunID, slot int, eventType, correlationID string, raw []byte) (worlds.Event, error) {
	var doc eventDoc
	if err := cbor.Unmarshal(raw, &doc); err != nil {
		return worlds.Event{}, fmt.Errorf("workflow/local: decode event %d of run %s: %w", slot, runID, err)
	}
	data, err := worlds.DecodeEventData(worlds.EventType(eventType), doc.Data)
	if err != nil {
		return worlds.Event{}, fmt.Errorf("workflow/local: decode event %d of run %s: %w", slot, runID, err)
	}
	return worlds.Event{RunID: runID, EventID: worlds.EventIDForSlot(uint64(slot)), EventType: worlds.EventType(eventType), CorrelationID: correlationID, Data: data, CreatedAt: doc.CreatedAt.UTC(), SpecVersion: doc.SpecVersion, ResumeID: doc.ResumeID}, nil
}

func (s *store) insertEvent(event worlds.Event) error {
	doc, err := encodeEvent(event)
	if err != nil {
		return err
	}
	return s.exec("INSERT INTO events (run_id, seq, event_type, correlation_id, doc) VALUES (?, ?, ?, ?, ?)", string(event.RunID), eventSlot(event), string(event.EventType), nullable(event.CorrelationID), doc)
}

// updateEventData replaces the Data of the event in slot.
func (s *store) updateEventData(event worlds.Event) error {
	doc, err := encodeEvent(event)
	if err != nil {
		return err
	}
	return s.exec("UPDATE events SET doc = ? WHERE run_id = ? AND seq = ?", doc, string(event.RunID), eventSlot(event))
}

// readEvents returns the run's events in slots from..to inclusive, in order.
func (s *store) readEvents(runID worlds.RunID, from, to int) ([]worlds.Event, error) {
	from = max(from, 1)
	if to < from {
		return []worlds.Event{}, nil
	}
	return s.queryEvents("SELECT seq, event_type, coalesce(correlation_id, ''), doc FROM events WHERE run_id = ? AND seq BETWEEN ? AND ? ORDER BY seq", runID, string(runID), from, to)
}

// readEventsDescending returns the run's events in slots from..to inclusive,
// latest first.
func (s *store) readEventsDescending(runID worlds.RunID, from, to int) ([]worlds.Event, error) {
	from = max(from, 1)
	if to < from {
		return []worlds.Event{}, nil
	}
	return s.queryEvents("SELECT seq, event_type, coalesce(correlation_id, ''), doc FROM events WHERE run_id = ? AND seq BETWEEN ? AND ? ORDER BY seq DESC", runID, string(runID), from, to)
}

// readEvent returns the event in slot, or false for slot 0 or a missing one.
func (s *store) readEvent(runID worlds.RunID, slot int) (worlds.Event, bool, error) {
	if slot < 1 {
		return worlds.Event{}, false, nil
	}
	events, err := s.readEvents(runID, slot, slot)
	if err != nil || len(events) == 0 {
		return worlds.Event{}, false, err
	}
	return events[0], true, nil
}

// correlated returns the run's events of correlationID after slot after, or
// before it when descending (0 is no bound), at most limit of them (0 is no
// limit).
func (s *store) correlated(runID worlds.RunID, correlationID string, after int, descending bool, limit int) ([]worlds.Event, error) {
	query := "SELECT seq, event_type, coalesce(correlation_id, ''), doc FROM events INDEXED BY events_by_correlation WHERE run_id = ? AND correlation_id = ?"
	args := []any{string(runID), correlationID}
	order := " ORDER BY seq"
	switch {
	case descending && after > 0:
		query += " AND seq < ?"
		args = append(args, after)
	case !descending && after > 0:
		query += " AND seq > ?"
		args = append(args, after)
	}
	if descending {
		order += " DESC"
	}
	query += order
	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}
	return s.queryEvents(query, runID, args...)
}

func (s *store) queryEvents(query string, runID worlds.RunID, args ...any) ([]worlds.Event, error) {
	rows, err := s.q.QueryContext(s.ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	events := []worlds.Event{}
	for rows.Next() {
		var slot int
		var eventType, correlationID string
		var doc []byte
		if err := rows.Scan(&slot, &eventType, &correlationID, &doc); err != nil {
			return nil, err
		}
		event, err := decodeEvent(runID, slot, eventType, correlationID, doc)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

// Steps.

func (s *store) getStep(runID worlds.RunID, stepID worlds.StepID) (*stepRecord, error) {
	var doc string
	err := s.q.QueryRowContext(s.ctx, "SELECT doc FROM steps WHERE run_id = ? AND step_id = ?", string(runID), string(stepID)).Scan(&doc)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var record stepRecord
	if err := json.Unmarshal([]byte(doc), &record); err != nil {
		return nil, fmt.Errorf("workflow/local: decode step: %w", err)
	}
	return &record, nil
}

func (s *store) putStep(record *stepRecord) error {
	doc, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return s.exec("INSERT INTO steps (run_id, step_id, created_at, doc) VALUES (?, ?, ?, ?) ON CONFLICT (run_id, step_id) DO UPDATE SET doc = excluded.doc",
		string(record.RunID), string(record.StepID), millis(record.CreatedAt), string(doc))
}

func (s *store) listSteps(runID worlds.RunID) ([]stepRecord, error) {
	rows, err := s.q.QueryContext(s.ctx, "SELECT doc FROM steps WHERE run_id = ? ORDER BY created_at, step_id", string(runID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var records []stepRecord
	for rows.Next() {
		var doc string
		if err := rows.Scan(&doc); err != nil {
			return nil, err
		}
		var record stepRecord
		if err := json.Unmarshal([]byte(doc), &record); err != nil {
			return nil, fmt.Errorf("workflow/local: decode step: %w", err)
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

// Waits.

func (s *store) putWait(wait worlds.Wait) error {
	doc, err := json.Marshal(wait)
	if err != nil {
		return err
	}
	return s.exec("INSERT INTO waits (run_id, wait_id, doc) VALUES (?, ?, ?) ON CONFLICT (run_id, wait_id) DO UPDATE SET doc = excluded.doc", string(wait.RunID), string(wait.WaitID), string(doc))
}

// deleteWaits deletes a run's waits, as it ends.
func (s *store) deleteWaits(runID worlds.RunID) error {
	return s.exec("DELETE FROM waits WHERE run_id = ?", string(runID))
}

// openWaits returns the run's waits that have not completed.
func (s *store) openWaits(runID worlds.RunID) ([]worlds.Wait, error) {
	rows, err := s.q.QueryContext(s.ctx, "SELECT doc FROM waits WHERE run_id = ? ORDER BY wait_id", string(runID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var waits []worlds.Wait
	for rows.Next() {
		var doc string
		if err := rows.Scan(&doc); err != nil {
			return nil, err
		}
		var wait worlds.Wait
		if err := json.Unmarshal([]byte(doc), &wait); err != nil {
			return nil, fmt.Errorf("workflow/local: decode wait: %w", err)
		}
		if wait.Status != worlds.WaitStatusCompleted {
			waits = append(waits, wait)
		}
	}
	return waits, rows.Err()
}

// Hooks.

// getHook returns the hook the store's tag sees, or nil.
func (s *store) getHook(hookID worlds.HookID) (*hookRecord, error) {
	condition, args := s.tagged("tag")
	return s.scanHook(s.q.QueryRowContext(s.ctx, "SELECT doc FROM hooks WHERE hook_id = ? AND "+condition, append([]any{string(hookID)}, args...)...))
}

// getHookAnyTag returns the hook whatever its tag, as a token's owner, or
// nil.
func (s *store) getHookAnyTag(hookID worlds.HookID) (*hookRecord, error) {
	return s.scanHook(s.q.QueryRowContext(s.ctx, "SELECT doc FROM hooks WHERE hook_id = ?", string(hookID)))
}

func (s *store) scanHook(row interface{ Scan(...any) error }) (*hookRecord, error) {
	var doc string
	err := row.Scan(&doc)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var record hookRecord
	if err := json.Unmarshal([]byte(doc), &record); err != nil {
		return nil, fmt.Errorf("workflow/local: decode hook: %w", err)
	}
	return &record, nil
}

// putHook inserts or updates the hook. An update keeps the hook's tag.
func (s *store) putHook(record *hookRecord) error {
	doc, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return s.exec("INSERT INTO hooks (hook_id, run_id, token, tag, created_at, doc) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (hook_id) DO UPDATE SET token = excluded.token, doc = excluded.doc",
		string(record.HookID), string(record.RunID), record.Token, s.tagValue(), millis(record.CreatedAt), string(doc))
}

// listHooks returns the hooks the store's tag sees, of runID when it is set,
// oldest first.
func (s *store) listHooks(runID worlds.RunID) ([]hookRecord, error) {
	condition, args := s.tagged("tag")
	query := "SELECT doc FROM hooks WHERE " + condition
	if runID != "" {
		query += " AND run_id = ?"
		args = append(args, string(runID))
	}
	rows, err := s.q.QueryContext(s.ctx, query+" ORDER BY created_at, hook_id", args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var records []hookRecord
	for rows.Next() {
		record, err := s.scanHook(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, *record)
	}
	return records, rows.Err()
}

// deleteHook deletes the hook.
func (s *store) deleteHook(hookID worlds.HookID) error {
	return s.exec("DELETE FROM hooks WHERE hook_id = ?", string(hookID))
}

// getDisposal returns the hook's disposal, or nil when it was not disposed.
func (s *store) getDisposal(hookID worlds.HookID) (*disposalRecord, error) {
	var record disposalRecord
	var runID string
	var claimedBy sql.NullString
	err := s.q.QueryRowContext(s.ctx, "SELECT run_id, force_claimed_by FROM hook_disposals WHERE hook_id = ?", string(hookID)).Scan(&runID, &claimedBy)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	record.RunID = worlds.RunID(runID)
	if claimedBy.Valid {
		if err := json.Unmarshal([]byte(claimedBy.String), &record.ForceClaimedBy); err != nil {
			return nil, fmt.Errorf("workflow/local: decode hook disposal: %w", err)
		}
	}
	return &record, nil
}

// putDisposal records that the hook's disposal committed, and the hook that
// took its token when a forced claim disposed it.
func (s *store) putDisposal(hookID worlds.HookID, record *disposalRecord) error {
	var claimedBy any
	if record.ForceClaimedBy != nil {
		data, err := json.Marshal(record.ForceClaimedBy)
		if err != nil {
			return err
		}
		claimedBy = string(data)
	}
	return s.exec("INSERT INTO hook_disposals (hook_id, run_id, force_claimed_by) VALUES (?, ?, ?) ON CONFLICT (hook_id) DO NOTHING", string(hookID), string(record.RunID), claimedBy)
}

// Tokens.

func (s *store) getToken(token string) (*tokenRecord, error) {
	var record tokenRecord
	var retention sql.NullInt64
	err := s.q.QueryRowContext(s.ctx, "SELECT hook_id, run_id, retention_until FROM hook_tokens WHERE token = ?", token).Scan(&record.HookID, &record.RunID, &retention)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if retention.Valid {
		until := time.UnixMilli(retention.Int64).UTC()
		record.TokenRetentionUntil = &until
	}
	return &record, nil
}

// putToken records token's owner, or frees it for a nil record.
func (s *store) putToken(token string, record *tokenRecord) error {
	if record == nil {
		return s.exec("DELETE FROM hook_tokens WHERE token = ?", token)
	}
	var retention any
	if record.TokenRetentionUntil != nil {
		retention = millis(*record.TokenRetentionUntil)
	}
	return s.exec("INSERT INTO hook_tokens (token, hook_id, run_id, retention_until) VALUES (?, ?, ?, ?) ON CONFLICT (token) DO UPDATE SET hook_id = excluded.hook_id, run_id = excluded.run_id, retention_until = excluded.retention_until",
		token, string(record.HookID), string(record.RunID), retention)
}

// Resumes.

// getResume returns the digest and slot of a deduplicated resume of the run.
func (s *store) getResume(runID worlds.RunID, resumeID string) (string, int, bool, error) {
	var digest sql.NullString
	var slot int
	err := s.q.QueryRowContext(s.ctx, "SELECT payload_digest, seq FROM hook_resumes WHERE run_id = ? AND resume_id = ?", string(runID), resumeID).Scan(&digest, &slot)
	if errors.Is(err, sql.ErrNoRows) {
		return "", 0, false, nil
	}
	return digest.String, slot, err == nil, err
}

func (s *store) putResume(event worlds.Event, digest string) error {
	return s.exec("INSERT INTO hook_resumes (run_id, resume_id, hook_id, seq, payload_digest) VALUES (?, ?, ?, ?, ?)", string(event.RunID), event.ResumeID, event.CorrelationID, eventSlot(event), nullable(digest))
}

// Start hook rejections.

// getRejection returns the refusal of the run's start hook candidate, or nil.
func (s *store) getRejection(runID worlds.RunID) (*worlds.StartHookDecision, error) {
	var decision worlds.StartHookDecision
	var conflicting string
	err := s.q.QueryRowContext(s.ctx, "SELECT token, conflicting_run_id FROM start_hook_rejections WHERE run_id = ?", string(runID)).Scan(&decision.Token, &conflicting)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	decision.ConflictingRunID = worlds.RunID(conflicting)
	return &decision, nil
}

func (s *store) putRejection(runID worlds.RunID, decision worlds.StartHookDecision) error {
	return s.exec("INSERT INTO start_hook_rejections (run_id, tag, token, conflicting_run_id) VALUES (?, ?, ?, ?) ON CONFLICT (run_id) DO NOTHING", string(runID), s.tagValue(), decision.Token, string(decision.ConflictingRunID))
}

// runHooks returns the run's hooks, whatever their tag.
func (s *store) runHooks(runID worlds.RunID) ([]hookRecord, error) {
	rows, err := s.q.QueryContext(s.ctx, "SELECT doc FROM hooks WHERE run_id = ? ORDER BY created_at, hook_id", string(runID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var records []hookRecord
	for rows.Next() {
		record, err := s.scanHook(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, *record)
	}
	return records, rows.Err()
}

// purgeRun deletes the data of a run whose ExpiredAt is set: the payloads in
// its events, which are also its run, step, and hook payloads, and its
// streams' chunks, leaving each stream empty and closed.
func (s *store) purgeRun(run runRecord) error {
	events, err := s.readEvents(run.RunID, 1, run.EventCount)
	if err != nil {
		return err
	}
	for _, event := range events {
		scrubbed := worlds.ScrubEventData(event.EventType, event.Data)
		if bytes.Equal(scrubbed, event.Data) {
			continue
		}
		event.Data = scrubbed
		if err := s.updateEventData(event); err != nil {
			return err
		}
	}
	rows, err := s.q.QueryContext(s.ctx, "SELECT DISTINCT name, tag FROM stream_chunks WHERE run_id = ?", string(run.RunID))
	if err != nil {
		return err
	}
	type stream struct {
		name string
		tag  sql.NullString
	}
	var streams []stream
	for rows.Next() {
		var value stream
		if err := rows.Scan(&value.name, &value.tag); err != nil {
			rows.Close()
			return err
		}
		streams = append(streams, value)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if err := s.exec("DELETE FROM stream_chunks WHERE run_id = ?", string(run.RunID)); err != nil {
		return err
	}
	for _, value := range streams {
		var tag any
		if value.tag.Valid {
			tag = value.tag.String
		}
		if err := s.exec("INSERT INTO stream_chunks (run_id, name, idx, tag, data, eof) VALUES (?, ?, 1, ?, NULL, 1)", string(run.RunID), value.name, tag); err != nil {
			return err
		}
	}
	return nil
}
