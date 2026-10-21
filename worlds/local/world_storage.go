package localworld

import (
	"encoding/base64"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// The run, step, and hook views hold no payloads. The event log is their only
// copy: a view records the slot of each event holding one, and reads load it
// with worlds.EventPayload. A slot of 0 means no such event.

// runRecord is the materialized run plus the committed event count. Writing it
// commits every event file at or below EventCount. The run's input is always
// in slot 1, its run_created event.
type runRecord struct {
	worlds.Run
	EventCount       int            `json:"eventCount"`
	StartedEventID   worlds.EventID `json:"startedEventId,omitempty"`
	CancelledEventID worlds.EventID `json:"cancelledEventId,omitempty"`
	// OutputSlot is the run_completed event and ErrorSlot the run_failed event.
	OutputSlot int `json:"outputSlot,omitempty"`
	ErrorSlot  int `json:"errorSlot,omitempty"`
}

// stepRecord is the materialized step. InputSlot is its step_created event.
// OutputSlot or ErrorSlot is its latest result: step_completed, or
// step_failed or step_retrying. A later result replaces the earlier one.
type stepRecord struct {
	worlds.Step
	InputSlot  int `json:"inputSlot,omitempty"`
	OutputSlot int `json:"outputSlot,omitempty"`
	ErrorSlot  int `json:"errorSlot,omitempty"`
}

// hookRecord is the materialized hook. MetadataSlot is its hook_created event
// in the hook's own run.
type hookRecord struct {
	worlds.Hook
	MetadataSlot int `json:"metadataSlot,omitempty"`
}

// tokenRecord is the token uniqueness index. A token is held while its record
// exists and TokenRetentionUntil has not passed.
type tokenRecord struct {
	HookID              worlds.HookID `json:"hookId"`
	TokenRetentionUntil *time.Time    `json:"tokenRetentionUntil,omitempty"`
}

func (t *tokenRecord) held(now time.Time) bool {
	return t != nil && (t.TokenRetentionUntil == nil || t.TokenRetentionUntil.After(now))
}

// readOptional reads name into target and reports whether it existed.
func (w *World) readOptional(name string, target any) (bool, error) {
	err := readJSON(w.root, name, target)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	return err == nil, err
}

func (w *World) readRun(runID worlds.RunID) (runRecord, error) {
	var record runRecord
	ok, err := w.readOptional(runPath(runID), &record)
	if err == nil && !ok {
		err = worlds.ErrRunNotFound
	}
	return record, err
}

func (w *World) readStep(runID worlds.RunID, stepID worlds.StepID) (*stepRecord, error) {
	var record stepRecord
	if ok, err := w.readOptional(stepPath(runID, stepID), &record); !ok || err != nil {
		return nil, err
	}
	return &record, nil
}

func (w *World) readHook(hookID worlds.HookID) (*hookRecord, error) {
	var record hookRecord
	if ok, err := w.readOptional(hookPath(hookID), &record); !ok || err != nil {
		return nil, err
	}
	return &record, nil
}

func (w *World) readToken(token string) (*tokenRecord, error) {
	var record tokenRecord
	if ok, err := w.readOptional(tokenPath(token), &record); !ok || err != nil {
		return nil, err
	}
	return &record, nil
}

// readEvents reads committed slots from..to inclusive in ascending order.
func (w *World) readEvents(runID worlds.RunID, from, to int) ([]worlds.Event, error) {
	if from < 1 {
		from = 1
	}
	if to < from {
		return []worlds.Event{}, nil
	}
	events := make([]worlds.Event, 0, to-from+1)
	for slot := from; slot <= to; slot++ {
		var event worlds.Event
		if err := readJSON(w.root, eventPath(runID, slot), &event); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, nil
}

// readEvent returns a committed slot's event, or false for slot 0.
func (w *World) readEvent(runID worlds.RunID, slot int) (worlds.Event, bool, error) {
	if slot < 1 {
		return worlds.Event{}, false, nil
	}
	var event worlds.Event
	err := readJSON(w.root, eventPath(runID, slot), &event)
	return event, err == nil, err
}

func (w *World) readHooks() ([]hookRecord, error) {
	names, err := listJSON(w.root, "hooks")
	if err != nil {
		return nil, err
	}
	hooks := make([]hookRecord, 0, len(names))
	for _, name := range names {
		var hook hookRecord
		if err := readJSON(w.root, path.Join("hooks", name), &hook); err != nil {
			return nil, err
		}
		hooks = append(hooks, hook)
	}
	return hooks, nil
}

func (w *World) readRuns() ([]runRecord, error) {
	names, err := listJSON(w.root, "runs")
	if err != nil {
		return nil, err
	}
	runs := make([]runRecord, 0, len(names))
	for _, name := range names {
		var record runRecord
		if err := readJSON(w.root, path.Join("runs", name), &record); err != nil {
			return nil, err
		}
		runs = append(runs, record)
	}
	return runs, nil
}

func runPath(runID worlds.RunID) string { return path.Join("runs", fileKey(string(runID))+".json") }

func eventsDir(runID worlds.RunID) string { return path.Join("events", fileKey(string(runID))) }

func stepsDir(runID worlds.RunID) string { return path.Join("steps", fileKey(string(runID))) }

func hookPath(hookID worlds.HookID) string {
	return path.Join("hooks", fileKey(string(hookID))+".json")
}

func tokenPath(token string) string { return path.Join("tokens", fileKey(token)+".json") }

func eventPath(runID worlds.RunID, slot int) string {
	return path.Join(eventsDir(runID), string(worlds.EventIDForSlot(uint64(slot)))+".json")
}

func stepPath(runID worlds.RunID, stepID worlds.StepID) string {
	return path.Join(stepsDir(runID), fileKey(string(stepID))+".json")
}

// eventLoader returns the event in a run's slot, or false for slot 0.
type eventLoader func(slot int) (worlds.Event, bool, error)

// committed loads a run's committed events.
func (w *World) committed(runID worlds.RunID) eventLoader {
	return func(slot int) (worlds.Event, bool, error) { return w.readEvent(runID, slot) }
}

// payload returns the payload of the event in slot, or nil for slot 0.
func (load eventLoader) payload(slot int) ([]byte, error) {
	event, ok, err := load(slot)
	if !ok || err != nil {
		return nil, err
	}
	return worlds.EventPayload(event), nil
}

// runView returns the run with its payloads loaded from its events.
// ResolveDataNone leaves out the input and output.
func runView(record runRecord, load eventLoader, resolve worlds.ResolveData) (worlds.Run, error) {
	value := record.Run
	value.Input, value.Output, value.Error = nil, nil, nil
	value.Attributes = maps.Clone(value.Attributes)
	if value.Attributes == nil {
		value.Attributes = map[string]string{}
	}
	var err error
	if resolve != worlds.ResolveDataNone {
		if value.Input, err = load.payload(1); err != nil {
			return worlds.Run{}, err
		}
		if value.Output, err = load.payload(record.OutputSlot); err != nil {
			return worlds.Run{}, err
		}
	}
	if value.Error, err = load.payload(record.ErrorSlot); err != nil {
		return worlds.Run{}, err
	}
	return value, nil
}

// stepView returns the step with its payloads loaded from its events.
// ResolveDataNone leaves out the input and output.
func stepView(record stepRecord, load eventLoader, resolve worlds.ResolveData) (worlds.Step, error) {
	value := record.Step
	value.Input, value.Output, value.Error = nil, nil, nil
	var err error
	if resolve != worlds.ResolveDataNone {
		if value.Input, err = load.payload(record.InputSlot); err != nil {
			return worlds.Step{}, err
		}
		if value.Output, err = load.payload(record.OutputSlot); err != nil {
			return worlds.Step{}, err
		}
	}
	if value.Error, err = load.payload(record.ErrorSlot); err != nil {
		return worlds.Step{}, err
	}
	return value, nil
}

// hookView returns the hook with its metadata loaded from hook_created.
// ResolveDataNone leaves out the metadata.
func hookView(record hookRecord, load eventLoader, resolve worlds.ResolveData) (worlds.Hook, error) {
	value := record.Hook
	value.Metadata = nil
	if resolve == worlds.ResolveDataNone {
		return value, nil
	}
	var err error
	value.Metadata, err = load.payload(record.MetadataSlot)
	return value, err
}

// sortKey orders records by creation time, then ID. The fixed-width UTC time
// sorts lexically.
func sortKey(at time.Time, id string) string {
	return at.UTC().Format("2006-01-02T15:04:05.000000000Z") + "\x00" + id
}

// keyPage sorts values by key in the requested order and returns the page
// after the opaque cursor, which encodes the last returned key.
func keyPage[T any](values []T, pagination worlds.Pagination, key func(T) string) (worlds.Page[T], error) {
	descending := pagination.SortOrder == worlds.SortOrderDescending
	slices.SortFunc(values, func(a, b T) int {
		if descending {
			return strings.Compare(key(b), key(a))
		}
		return strings.Compare(key(a), key(b))
	})
	start := 0
	if pagination.Cursor != "" {
		raw, err := base64.RawURLEncoding.DecodeString(pagination.Cursor)
		if err != nil {
			return worlds.Page[T]{}, fmt.Errorf("workflow/local: invalid cursor %q", pagination.Cursor)
		}
		after := string(raw)
		start = len(values)
		for index, value := range values {
			if compare := strings.Compare(key(value), after); descending && compare < 0 || !descending && compare > 0 {
				start = index
				break
			}
		}
	}
	end := len(values)
	if pagination.Limit > 0 {
		end = min(start+pagination.Limit, len(values))
	}
	page := worlds.Page[T]{Data: values[start:end], HasMore: end < len(values)}
	if page.Data == nil {
		page.Data = []T{}
	}
	if page.HasMore && end > start {
		page.Cursor = base64.RawURLEncoding.EncodeToString([]byte(key(values[end-1])))
	}
	return page, nil
}

// eventCursor returns the slot named by an event cursor, or 0 when empty.
func eventCursor(cursor string) (int, error) {
	if cursor == "" {
		return 0, nil
	}
	slot, err := worlds.EventSlot(worlds.EventID(cursor))
	if err != nil {
		return 0, err
	}
	return int(slot), nil
}

func chunkCursor(cursor string) int {
	if value, err := strconv.Atoi(cursor); err == nil && value > 0 {
		return value
	}
	return 0
}
