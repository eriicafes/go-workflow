package localworld

import (
	"cmp"
	"encoding/base64"
	"encoding/json"
	"fmt"
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
// when a forced claim disposed it.
type disposalRecord struct {
	RunID          worlds.RunID
	ForceClaimedBy *worlds.HookForceClaim
}

// tokenRecord is a token's owner, with the retention its hook asked for. See
// tokenHeld for when the owner still holds the token.
type tokenRecord struct {
	HookID              worlds.HookID
	RunID               worlds.RunID
	TokenRetentionUntil *time.Time
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

// cursorTime is the time of an entity cursor: UTC with milliseconds.
const cursorTime = "2006-01-02T15:04:05.000Z"

// entityCursor is the cursor of a run, step, or hook page: the creation time
// and ID of its last record, as "<time>|<id>".
func entityCursor(at time.Time, id string) string {
	return at.UTC().Format(cursorTime) + "|" + id
}

// parseEntityCursor returns the creation time, in milliseconds, and the ID an
// entity cursor names. A cursor without an ID names every record of its
// time, and one whose time does not parse is no cursor.
func parseEntityCursor(cursor string) (int64, string, bool) {
	if cursor == "" {
		return 0, "", false
	}
	parts := strings.Split(cursor, "|")
	at, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return 0, "", false
	}
	id := ""
	if len(parts) > 1 {
		id = parts[1]
	}
	return at.UnixMilli(), id, true
}

// entityPage sorts values by creation time in milliseconds, then ID, in the
// requested order, and returns the page after the cursor. A cursor that does
// not parse starts from the first page.
func entityPage[T any](values []T, pagination worlds.Pagination, key func(T) (time.Time, string)) worlds.Page[T] {
	descending := pagination.SortOrder == worlds.SortOrderDescending
	compare := func(at int64, id string, value T) int {
		valueAt, valueID := key(value)
		order := cmp.Compare(valueAt.UnixMilli(), at)
		if order == 0 && id != "" {
			order = strings.Compare(valueID, id)
		}
		if descending {
			return -order
		}
		return order
	}
	slices.SortFunc(values, func(a, b T) int {
		at, id := key(a)
		// compare orders its value against a's key, so b after a is positive.
		return -compare(at.UnixMilli(), id, b)
	})
	start := 0
	if at, id, ok := parseEntityCursor(pagination.Cursor); ok {
		start = len(values)
		for index, value := range values {
			if compare(at, id, value) > 0 {
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
	// The last page's cursor resumes after it.
	if end > start {
		page.Cursor = entityCursor(key(values[end-1]))
	}
	return page
}

// eventCursorPrefix starts the cursor of an events page, which names the last
// event of the page.
const eventCursorPrefix = "key:"

// eventCursor returns the slot named by an event cursor: "key:<event ID>", a
// bare event ID, or "<time>|<event ID>". An empty cursor, and one that names
// no slot, is 0, the start of the log.
func eventCursor(cursor string) int {
	id := cursor
	if rest, ok := strings.CutPrefix(id, eventCursorPrefix); ok {
		id = rest
	} else if _, rest, ok := strings.Cut(id, "|"); ok {
		id = rest
	}
	slot, err := worlds.EventSlot(worlds.EventID(id))
	if err != nil {
		return 0
	}
	return int(slot)
}

// chunkCursor is the cursor of a chunks page: how many chunks precede the
// next one, as base64 of the JSON {"i": count}.
func chunkCursor(count int) string {
	return base64.StdEncoding.EncodeToString([]byte(`{"i":` + strconv.Itoa(count) + `}`))
}

// parseChunkCursor returns the count a chunk cursor holds. A plain decimal
// count is read too, and anything else is 0, the start of the stream.
func parseChunkCursor(cursor string) int {
	if value, err := strconv.Atoi(cursor); err == nil {
		return max(value, 0)
	}
	raw, err := base64.StdEncoding.DecodeString(cursor)
	if err != nil {
		return 0
	}
	var position struct {
		I int `json:"i"`
	}
	if json.Unmarshal(raw, &position) != nil {
		return 0
	}
	return max(position.I, 0)
}

// checkID refuses an ID a caller supplied that is empty or holds ".", "/",
// "\", or a NUL byte, with ErrEventInvalid. No record is stored under one.
func checkID(kind, id string) error {
	if id != "" && !strings.ContainsAny(id, "./\\\x00") {
		return nil
	}
	shown := id
	if runes := []rune(shown); len(runes) > 48 {
		shown = string(runes[:48]) + "…"
	}
	return fmt.Errorf("%w: unsafe %s %q: it must not be empty or contain a dot, a slash, a backslash, or a NUL byte", worlds.ErrEventInvalid, kind, shown)
}
