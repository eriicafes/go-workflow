package localworld

import (
	"bytes"
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

// runRecord is the materialized run plus the committed event count. Writing it
// commits every event file at or below EventCount.
type runRecord struct {
	worlds.Run
	EventCount       int            `json:"eventCount"`
	StartedEventID   worlds.EventID `json:"startedEventId,omitempty"`
	CancelledEventID worlds.EventID `json:"cancelledEventId,omitempty"`
}

type stepRecord struct {
	worlds.Step
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

func (w *World) readHook(hookID worlds.HookID) (*worlds.Hook, error) {
	var record worlds.Hook
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

func cloneRun(value worlds.Run, resolve worlds.ResolveData) worlds.Run {
	value.Input, value.Output, value.Error = bytes.Clone(value.Input), bytes.Clone(value.Output), bytes.Clone(value.Error)
	value.Attributes = maps.Clone(value.Attributes)
	if value.Attributes == nil {
		value.Attributes = map[string]string{}
	}
	if resolve == worlds.ResolveDataNone {
		value.Input, value.Output = nil, nil
	}
	return value
}

func cloneStep(value worlds.Step, resolve worlds.ResolveData) worlds.Step {
	value.Input, value.Output, value.Error = bytes.Clone(value.Input), bytes.Clone(value.Output), bytes.Clone(value.Error)
	if resolve == worlds.ResolveDataNone {
		value.Input, value.Output = nil, nil
	}
	return value
}

func cloneHook(value worlds.Hook, resolve worlds.ResolveData) worlds.Hook {
	value.Metadata = bytes.Clone(value.Metadata)
	if resolve == worlds.ResolveDataNone {
		value.Metadata = nil
	}
	return value
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
