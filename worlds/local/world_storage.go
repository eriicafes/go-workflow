package localworld

import (
	"encoding/base64"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// The run, step, and hook views hold no payloads. The event log is their only
// copy: a view records the slot of each event holding one, and reads load it
// with worlds.EventPayload. A slot of 0 means no such event.

// runRecord is the materialized run, stored as its row's doc. EventCount and
// InBandCount are the row's seq and seq_in_band columns. The run's input is
// always in slot 1, its run_created event.
type runRecord struct {
	worlds.Run
	EventCount       int            `json:"-"`
	StartedEventID   worlds.EventID `json:"startedEventId,omitempty"`
	CancelledEventID worlds.EventID `json:"cancelledEventId,omitempty"`
	// OutputSlot is the run_completed event and ErrorSlot the run_failed event.
	OutputSlot int `json:"outputSlot,omitempty"`
	ErrorSlot  int `json:"errorSlot,omitempty"`
}

// stepRecord is the materialized step. InputSlot is its step_created event,
// or the step_started that created an inline step.
// OutputSlot is its step_completed, and ErrorSlot its latest step_failed or
// step_retrying, which a step_completed keeps.
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

// disposalRecord is a disposed hook's run, and the hook that took its token
// when a forced claim disposed it, as world-local's hook_disposals row.
type disposalRecord struct {
	RunID          worlds.RunID
	ForceClaimedBy *worlds.HookForceClaim
}

// tokenRecord is a token's owner. A token is held while its row exists and
// TokenRetentionUntil has not passed.
type tokenRecord struct {
	HookID              worlds.HookID
	RunID               worlds.RunID
	TokenRetentionUntil *time.Time
}

func (t *tokenRecord) held(now time.Time) bool {
	return t != nil && (t.TokenRetentionUntil == nil || t.TokenRetentionUntil.After(now))
}

// eventLoader returns the event in a run's slot, or false for slot 0.
type eventLoader func(slot int) (worlds.Event, bool, error)

// committed loads a run's committed events.
func (s *store) committed(runID worlds.RunID) eventLoader {
	return func(slot int) (worlds.Event, bool, error) { return s.readEvent(runID, slot) }
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
	if value.HookID == worlds.StartHookID(value.RunID) {
		// A start hook's metadata is in its run's run_created.
		event, ok, err := load(1)
		if ok && err == nil {
			if start := worlds.RunStartHook(event.Data); start != nil {
				value.Metadata = start.Metadata
			}
		}
		return value, err
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
		// A cursor that does not decode starts from the first page, as
		// world-local's parseEntityCursor reads it.
		if raw, err := base64.RawURLEncoding.DecodeString(pagination.Cursor); err == nil {
			after := string(raw)
			start = len(values)
			for index, value := range values {
				if compare := strings.Compare(key(value), after); descending && compare < 0 || !descending && compare > 0 {
					start = index
					break
				}
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
	// The last page's cursor resumes after it, as world-local's does.
	if end > start {
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
