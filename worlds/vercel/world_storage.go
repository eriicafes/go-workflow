package vercelworld

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

var errCorrupt = errors.New("workflow/vercel: corrupt Redis data")

// Redis keys (p is Config.Prefix). Kinds are distinct namespaces, and keys
// with two user-supplied parts hex-encode the last one so they cannot collide.
//
//	p:run:<run>                 hash: run view, count (event count), score
//	p:attrs:<run>               hash: attributes
//	p:events:<run>              list: event records, index = slot-1
//	p:last:<run>                hash: "<type>[\n<correlation>]" -> last slot
//	p:corr:<run>:<hex(cid)>     zset: slots of a correlation ID
//	p:steps:<run>               hash: step ID -> step JSON
//	p:steporder:<run>           zset: step ID scored by creation slot
//	p:runhooks:<run>            zset: hook IDs of a run
//	p:hook:<hook>               string: hook JSON
//	p:token:<token>             string: owning hook ID
//	p:hooks                     zset: all hook IDs
//	p:runs, p:runs:wf:<name>, p:runs:status:<s>, p:runs:wfstatus:<s>:<name>
//	                            zsets: run IDs by creation score
//	p:order                     string: last creation score
//	p:streams:<run>             set: stream names
//	p:stream:<run>:<hex(name)>  hash: closed, session, seq
//	p:chunks:<run>:<hex(name)>  list: base64 chunks
//	p:queue:<id>                string: queue record JSON
//
// Creation scores are microsecond timestamps made strictly increasing by
// p:order, so they are unique and give stable cursors.
func (w *World) key(kind, id string) string { return w.prefix + ":" + kind + ":" + id }

func (w *World) corrKey(runID worlds.RunID, correlationID string) string {
	return w.key("corr", string(runID)+":"+hex.EncodeToString([]byte(correlationID)))
}

// eventRecord is the JSON stored per event, and its slot is its list position.
type eventRecord struct {
	Type          worlds.EventType `json:"t"`
	CorrelationID string           `json:"c,omitempty"`
	Data          []byte           `json:"d,omitempty"`
	At            string           `json:"at"`
}

// stepView is the step JSON written by scriptAppend.
type stepView struct {
	Name        string `json:"name"`
	Status      string `json:"status"`
	Input       string `json:"input,omitempty"`
	Output      string `json:"output,omitempty"`
	Error       string `json:"error,omitempty"`
	Attempt     int    `json:"attempt"`
	Slot        uint64 `json:"slot"`
	CreatedAt   string `json:"createdAt"`
	UpdatedAt   string `json:"updatedAt"`
	StartedAt   string `json:"startedAt,omitempty"`
	CompletedAt string `json:"completedAt,omitempty"`
	RetryAfter  string `json:"retryAfter,omitempty"`
}

// hookView is the hook JSON written by scriptAppend.
type hookView struct {
	RunID        worlds.RunID `json:"runId"`
	Token        string       `json:"token"`
	Metadata     string       `json:"metadata,omitempty"`
	Webhook      string       `json:"webhook,omitempty"`
	MinRetention string       `json:"minRetention,omitempty"`
	ReceivedSlot uint64       `json:"rid,omitempty"`
	CreatedAt    string       `json:"createdAt"`
	UpdatedAt    string       `json:"updatedAt"`
	ReceivedAt   string       `json:"receivedAt,omitempty"`
	RetainUntil  string       `json:"retainUntil,omitempty"`
}

func micros(t time.Time) string { return strconv.FormatInt(t.UnixMicro(), 10) }

// parseCount parses a counter stored in a run hash, where a missing field is 0.
func parseCount(value string) int {
	count, _ := strconv.Atoi(value)
	return count
}

func parseMicros(value string) time.Time {
	n, _ := strconv.ParseInt(value, 10, 64)
	return time.UnixMicro(n).UTC()
}

func optionalMicros(value string) *time.Time {
	if value == "" {
		return nil
	}
	t := parseMicros(value)
	return &t
}

func encodeBytes(data []byte) string { return base64.StdEncoding.EncodeToString(data) }

func decodeBytes(value string) ([]byte, error) {
	if value == "" {
		return nil, nil
	}
	data, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		return nil, fmt.Errorf("workflow/vercel: decode stored bytes: %w", err)
	}
	return data, nil
}

func newEventRecord(event worlds.EventsCreateEvent, now time.Time) string {
	data, _ := json.Marshal(eventRecord{Type: event.EventType, CorrelationID: event.CorrelationID, Data: event.Data, At: micros(now)})
	return string(data)
}

func decodeEvent(runID worlds.RunID, slot uint64, raw string, resolve worlds.EventsResolveData) (worlds.Event, error) {
	var record eventRecord
	if err := json.Unmarshal([]byte(raw), &record); err != nil {
		return worlds.Event{}, fmt.Errorf("workflow/vercel: decode event %d of %s: %w", slot, runID, err)
	}
	event := worlds.Event{RunID: runID, EventID: worlds.EventIDForSlot(slot), EventType: record.Type, CorrelationID: record.CorrelationID, Data: record.Data, CreatedAt: parseMicros(record.At)}
	if resolve == worlds.EventsResolveDataNone {
		event.Data = nil
	}
	return event, nil
}

func decodeStep(runID worlds.RunID, stepID worlds.StepID, raw string, resolve worlds.ResolveData) (worlds.Step, error) {
	var view stepView
	if err := json.Unmarshal([]byte(raw), &view); err != nil {
		return worlds.Step{}, fmt.Errorf("workflow/vercel: decode step %s: %w", stepID, err)
	}
	step := worlds.Step{RunID: runID, StepID: stepID, StepName: view.Name, Status: worlds.Status(view.Status), Attempt: view.Attempt, CreatedAt: parseMicros(view.CreatedAt), UpdatedAt: parseMicros(view.UpdatedAt), StartedAt: optionalMicros(view.StartedAt), CompletedAt: optionalMicros(view.CompletedAt)}
	var err error
	if step.Error, err = decodeBytes(view.Error); err != nil {
		return worlds.Step{}, err
	}
	if resolve != worlds.ResolveDataNone {
		if step.Input, err = decodeBytes(view.Input); err != nil {
			return worlds.Step{}, err
		}
		if step.Output, err = decodeBytes(view.Output); err != nil {
			return worlds.Step{}, err
		}
	}
	if view.RetryAfter != "" {
		retryAfter, err := time.Parse(time.RFC3339Nano, view.RetryAfter)
		if err != nil {
			return worlds.Step{}, fmt.Errorf("workflow/vercel: decode step retry time: %w", err)
		}
		step.RetryAfter = &retryAfter
	}
	return step, nil
}

func decodeHook(hookID worlds.HookID, raw string, resolve worlds.ResolveData) (worlds.Hook, error) {
	var view hookView
	if err := json.Unmarshal([]byte(raw), &view); err != nil {
		return worlds.Hook{}, fmt.Errorf("workflow/vercel: decode hook %s: %w", hookID, err)
	}
	hook := worlds.Hook{RunID: view.RunID, HookID: hookID, Token: view.Token, CreatedAt: parseMicros(view.CreatedAt), UpdatedAt: parseMicros(view.UpdatedAt), ReceivedAt: optionalMicros(view.ReceivedAt), RetainUntil: optionalMicros(view.RetainUntil)}
	if view.ReceivedSlot != 0 {
		hook.ReceivedEventID = worlds.EventIDForSlot(view.ReceivedSlot)
	}
	if view.MinRetention != "" {
		retention, err := strconv.ParseInt(view.MinRetention, 10, 64)
		if err != nil {
			return worlds.Hook{}, fmt.Errorf("workflow/vercel: decode hook retention: %w", err)
		}
		hook.MinRetention = time.Duration(retention)
	}
	if view.Webhook != "" {
		hook.Webhook = new(worlds.Webhook)
		if err := json.Unmarshal([]byte(view.Webhook), hook.Webhook); err != nil {
			return worlds.Hook{}, fmt.Errorf("workflow/vercel: decode hook webhook: %w", err)
		}
	}
	if resolve != worlds.ResolveDataNone {
		var err error
		if hook.Metadata, err = decodeBytes(view.Metadata); err != nil {
			return worlds.Hook{}, err
		}
	}
	return hook, nil
}

// scriptReply splits a script result into its values, mapping an {"err", code}
// reply to the World error for code.
func scriptReply(result any, err error) ([]any, error) {
	if err != nil {
		return nil, err
	}
	values, ok := result.([]any)
	if !ok || len(values) == 0 {
		return nil, fmt.Errorf("workflow/vercel: unexpected script reply %#v", result)
	}
	if values[0] == "ok" {
		return values[1:], nil
	}
	if values[0] != "err" || len(values) < 2 {
		return nil, fmt.Errorf("workflow/vercel: unexpected script reply %#v", result)
	}
	code, _ := values[1].(string)
	switch code {
	case "run_not_found":
		return nil, worlds.ErrRunNotFound
	case "run_exists":
		return nil, worlds.ErrRunAlreadyExists
	case "run_terminal":
		return nil, worlds.ErrRunTerminal
	case "step_not_startable":
		return nil, worlds.ErrStepNotStartable
	case "hook_not_active":
		return nil, worlds.ErrHookNotActive
	case "hook_conflict":
		return nil, worlds.ErrHookConflict
	case "in_band_superseded":
		return nil, worlds.ErrInBandSuperseded
	case "invalid":
		return nil, worlds.ErrEventInvalid
	case "stream_locked":
		return nil, worlds.ErrStreamLocked
	default:
		return nil, fmt.Errorf("workflow/vercel: script error %q", code)
	}
}

func replyString(value any) string {
	switch value := value.(type) {
	case string:
		return value
	case int64:
		return strconv.FormatInt(value, 10)
	default:
		return ""
	}
}

func replyInt(value any) int64 {
	switch value := value.(type) {
	case int64:
		return value
	case string:
		n, _ := strconv.ParseInt(value, 10, 64)
		return n
	default:
		return 0
	}
}

func replyStrings(value any) []string {
	values, _ := value.([]any)
	result := make([]string, len(values))
	for index, item := range values {
		result[index] = replyString(item)
	}
	return result
}

func flatHash(value any) map[string]string {
	values := replyStrings(value)
	result := make(map[string]string, len(values)/2)
	for index := 0; index+1 < len(values); index += 2 {
		result[values[index]] = values[index+1]
	}
	return result
}

func formatScore(score float64) string { return strconv.FormatFloat(score, 'f', -1, 64) }

// scoreRange returns the ZRANGEBYSCORE bounds after cursor in the requested
// order. Cursors are the score of the last returned member.
func scoreRange(cursor string, desc bool) (min, max string, err error) {
	if cursor != "" {
		if _, err := strconv.ParseFloat(cursor, 64); err != nil {
			return "", "", fmt.Errorf("%w: invalid cursor %q", worlds.ErrEventInvalid, cursor)
		}
	}
	switch {
	case cursor == "":
		return "-inf", "+inf", nil
	case desc:
		return "-inf", "(" + cursor, nil
	default:
		return "(" + cursor, "+inf", nil
	}
}

func fetchCount(limit int) int64 {
	if limit <= 0 {
		return -1
	}
	return int64(limit) + 1
}

// trimPage cuts members fetched with fetchCount to a page and its cursor.
func trimPage(members []redisZ, limit int) ([]redisZ, string, bool) {
	if limit <= 0 || len(members) <= limit {
		return members, "", false
	}
	members = members[:limit]
	return members, formatScore(members[limit-1].Score), true
}
