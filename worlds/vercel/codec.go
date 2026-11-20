package vercelworld

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// The backend keeps an event as metadata it reads, in a fixed set of fields
// per event type, plus one opaque payload in the event type's payload field.
// go-workflow's events have the Workflow SDK's shape, so the codec maps each
// field to the backend's: metadata fields to frame meta, and the payload,
// already in the SDK's envelope, to the frame body.

// payloadFields names the payload field of each event type that has one.
var payloadFields = map[worlds.EventType]string{
	worlds.EventTypeRunCreated:    "input",
	worlds.EventTypeRunStarted:    "input",
	worlds.EventTypeRunCompleted:  "output",
	worlds.EventTypeRunFailed:     "error",
	worlds.EventTypeStepCreated:   "input",
	worlds.EventTypeStepStarted:   "input",
	worlds.EventTypeStepCompleted: "result",
	worlds.EventTypeStepFailed:    "error",
	worlds.EventTypeStepRetrying:  "error",
	worlds.EventTypeHookCreated:   "metadata",
	worlds.EventTypeHookReceived:  "payload",
}

// wireEvent is an event as the backend returns it. Data is its eventData,
// with the payload field holding the payload bytes when it has one.
type wireEvent struct {
	RunID         string
	EventID       string
	EventType     string
	CorrelationID string
	Data          map[string]any
	CreatedAt     time.Time
}

// decodeWireEvent reads an event frame: meta is the event and body is the
// payload of its payload field.
func decodeWireEvent(meta map[string]any, body []byte) (wireEvent, error) {
	event := wireEventFromMap(meta)
	if event.EventType == "" || event.EventID == "" {
		return wireEvent{}, fmt.Errorf("workflow/vercel: event frame without type or ID: %v", meta)
	}
	if len(body) > 0 {
		field, ok := payloadFields[worlds.EventType(event.EventType)]
		if !ok {
			return wireEvent{}, fmt.Errorf("workflow/vercel: %s event cannot carry a payload", event.EventType)
		}
		if event.Data == nil {
			event.Data = map[string]any{}
		}
		event.Data[field] = body
	}
	return event, nil
}

// wireEventFromMap reads an event from a decoded CBOR map.
func wireEventFromMap(value map[string]any) wireEvent {
	event := wireEvent{
		RunID:         str(value, "runId"),
		EventID:       str(value, "eventId"),
		EventType:     str(value, "eventType"),
		CorrelationID: str(value, "correlationId"),
	}
	event.Data, _ = value["eventData"].(map[string]any)
	event.CreatedAt, _ = asTime(value["createdAt"])
	return event
}

// encodeEvent returns the frame meta and payload of a go-workflow event.
func encodeEvent(event worlds.EventsCreateEvent) (map[string]any, []byte, error) {
	meta := map[string]any{"eventType": string(event.EventType), "specVersion": specVersion}
	if event.CorrelationID != "" {
		meta["correlationId"] = event.CorrelationID
	}
	invalid := func(err error) error {
		return fmt.Errorf("%w: %s data: %v", worlds.ErrEventInvalid, event.EventType, err)
	}
	// decode reads the event's Data into value, leaving it zero when the
	// event has none.
	decode := func(value any) error {
		if len(event.Data) == 0 {
			return nil
		}
		if err := json.Unmarshal(event.Data, value); err != nil {
			return invalid(err)
		}
		return nil
	}
	switch event.EventType {
	case worlds.EventTypeRunStarted:
		meta["executorSpecVersion"] = specVersion
		return meta, nil, nil
	case worlds.EventTypeRunCompleted:
		return meta, worlds.DecodeRunCompleted(event.Data), nil
	case worlds.EventTypeRunFailed:
		var data worlds.DataRunFailed
		if err := decode(&data); err != nil {
			return nil, nil, err
		}
		if data.ErrorCode != "" {
			meta["errorCode"] = data.ErrorCode
		}
		return meta, data.Error, nil
	case worlds.EventTypeRunCancelled, worlds.EventTypeWaitCompleted, worlds.EventTypeHookDisposed:
		return meta, nil, nil
	case worlds.EventTypeAttrSet:
		var data worlds.DataAttrSet
		if err := decode(&data); err != nil {
			return nil, nil, err
		}
		changes := make([]map[string]any, len(data.Changes))
		for index, change := range data.Changes {
			var value any
			if change.Value != nil {
				value = *change.Value
			}
			changes[index] = map[string]any{"key": change.Key, "value": value}
		}
		writer := map[string]any{"type": data.Writer.Type}
		if data.Writer.Type == worlds.AttributeWriterStep {
			writer["stepId"], writer["attempt"] = string(data.Writer.StepID), data.Writer.Attempt
		}
		meta["changes"], meta["writer"] = changes, writer
		if hasReservedKey(data.Changes) {
			meta["allowReservedAttributes"] = true
		}
		return meta, nil, nil
	case worlds.EventTypeStepCreated:
		var data worlds.DataStepCreated
		if err := decode(&data); err != nil {
			return nil, nil, err
		}
		meta["stepName"] = data.StepName
		return meta, data.Input, nil
	case worlds.EventTypeStepStarted:
		var data worlds.DataStepStarted
		if err := decode(&data); err != nil {
			return nil, nil, err
		}
		if data.OwnerMessageID != "" {
			meta["ownerMessageId"] = data.OwnerMessageID
		}
		return meta, nil, nil
	case worlds.EventTypeStepCompleted, worlds.EventTypeStepFailed, worlds.EventTypeStepRetrying:
		var data worlds.DataStepResult
		if err := decode(&data); err != nil {
			return nil, nil, err
		}
		if data.RetryAfter != nil {
			meta["retryAfter"] = data.RetryAfter.UTC()
		}
		if event.EventType == worlds.EventTypeStepCompleted {
			return meta, data.Result, nil
		}
		return meta, data.Error, nil
	case worlds.EventTypeHookCreated:
		var data worlds.DataHookCreated
		if err := decode(&data); err != nil {
			return nil, nil, err
		}
		meta["hookToken"] = data.Token
		if data.IsWebhook {
			meta["hookIsWebhook"] = true
		}
		if data.IsSystem {
			meta["hookIsSystem"] = true
		}
		if data.TokenRetentionUntil != nil {
			meta["hookTokenRetentionUntil"] = data.TokenRetentionUntil.UTC()
		}
		if data.Force {
			meta["hookForce"] = true
		}
		return meta, data.Metadata, nil
	case worlds.EventTypeHookReceived:
		return meta, worlds.DecodeHookReceived(event.Data), nil
	case worlds.EventTypeWaitCreated:
		var data worlds.DataWaitCreated
		if err := decode(&data); err != nil {
			return nil, nil, err
		}
		if !data.ResumeAt.IsZero() {
			meta["resumeAt"] = data.ResumeAt.UTC()
		}
		return meta, nil, nil
	}
	return nil, nil, fmt.Errorf("%w: %s cannot be written", worlds.ErrEventInvalid, event.EventType)
}

func hasReservedKey(changes []worlds.RunsAttributeChange) bool {
	for _, change := range changes {
		if len(change.Key) > 0 && change.Key[0] == '$' {
			return true
		}
	}
	return false
}

// toEvent returns the go-workflow event of a backend event.
func toEvent(runID worlds.RunID, wire wireEvent, resolve worlds.EventsResolveData) (worlds.Event, error) {
	data, err := eventData(wire)
	if err != nil {
		return worlds.Event{}, fmt.Errorf("workflow/vercel: decode %s event %s: %w", wire.EventType, wire.EventID, err)
	}
	if wire.RunID != "" {
		runID = worlds.RunID(wire.RunID)
	}
	eventType := worlds.EventType(wire.EventType)
	return worlds.Event{
		RunID:         runID,
		EventID:       worlds.EventID(wire.EventID),
		EventType:     eventType,
		CorrelationID: wire.CorrelationID,
		Data:          worlds.ResolveEventData(eventType, data, resolve),
		CreatedAt:     wire.CreatedAt,
	}, nil
}

// eventData rebuilds the Data of a backend event.
func eventData(wire wireEvent) ([]byte, error) {
	d := wire.Data
	payload := func(field string) json.RawMessage {
		value, _ := d[field].([]byte)
		if len(value) == 0 {
			return nil
		}
		return value
	}
	switch worlds.EventType(wire.EventType) {
	case worlds.EventTypeRunCreated:
		executionContext, _ := d["executionContext"].(map[string]any)
		return json.Marshal(worlds.DataRunCreated{WorkflowName: str(d, "workflowName"), Input: payload("input"), Attributes: stringMap(d["attributes"]), DeploymentID: str(d, "deploymentId"), EncryptionPublicKey: str(d, "encryptionPublicKey"), ExecutionContext: executionContext})
	case worlds.EventTypeRunCompleted:
		return json.Marshal(worlds.DataRunCompleted{Output: payload("output")})
	case worlds.EventTypeRunFailed:
		return json.Marshal(worlds.DataRunFailed{Error: payload("error"), ErrorCode: str(d, "errorCode")})
	case worlds.EventTypeAttrSet:
		return json.Marshal(worlds.DataAttrSet{Changes: attributeChanges(d["changes"]), Writer: attributeWriter(d["writer"])})
	case worlds.EventTypeStepCreated:
		return json.Marshal(worlds.DataStepCreated{StepName: str(d, "stepName"), Input: payload("input")})
	case worlds.EventTypeStepStarted:
		return json.Marshal(worlds.DataStepStarted{OwnerMessageID: str(d, "ownerMessageId")})
	case worlds.EventTypeStepCompleted:
		return json.Marshal(worlds.DataStepResult{Result: payload("result")})
	case worlds.EventTypeStepFailed:
		return json.Marshal(worlds.DataStepResult{Error: payload("error")})
	case worlds.EventTypeStepRetrying:
		var retryAfter *time.Time
		if value, ok := asTime(d["retryAfter"]); ok {
			retryAfter = &value
		}
		return json.Marshal(worlds.DataStepResult{Error: payload("error"), RetryAfter: retryAfter})
	case worlds.EventTypeHookCreated, worlds.EventTypeHookConflict:
		data := worlds.DataHookCreated{Token: str(d, "token"), Metadata: payload("metadata")}
		data.IsWebhook, _ = d["isWebhook"].(bool)
		data.IsSystem, _ = d["isSystem"].(bool)
		data.Force, _ = d["force"].(bool)
		if until, ok := asTime(d["tokenRetentionUntil"]); ok {
			data.TokenRetentionUntil = &until
		}
		if claim := hookClaim(d["forceClaimedFrom"]); claim != nil {
			data.ForceClaimedFrom = claim
		}
		if wire.EventType == string(worlds.EventTypeHookConflict) {
			data.ConflictingRunID = worlds.RunID(str(d, "conflictingRunId"))
			if token := str(d, "token"); token != "" {
				data.Token = token
			}
		}
		return json.Marshal(data)
	case worlds.EventTypeHookReceived:
		return json.Marshal(worlds.DataHookReceived{Payload: payload("payload")})
	case worlds.EventTypeHookDisposed:
		claim := hookClaim(d["forceClaimedBy"])
		if claim == nil {
			return nil, nil
		}
		return json.Marshal(worlds.HookDisposedEventData{ForceClaimedBy: claim})
	case worlds.EventTypeWaitCreated:
		resumeAt, ok := asTime(d["resumeAt"])
		if !ok {
			// Written without Data.
			return nil, nil
		}
		return json.Marshal(worlds.DataWaitCreated{ResumeAt: resumeAt})
	}
	// run_started, run_cancelled, wait_completed, and the backend's own
	// events, such as noop slot fillers, carry no Data.
	return nil, nil
}

func str(values map[string]any, key string) string {
	value, _ := values[key].(string)
	return value
}

func stringMap(value any) map[string]string {
	values, ok := value.(map[string]any)
	if !ok {
		return map[string]string{}
	}
	result := make(map[string]string, len(values))
	for key, value := range values {
		if text, ok := value.(string); ok {
			result[key] = text
		}
	}
	return result
}

func attributeChanges(value any) []worlds.RunsAttributeChange {
	items, _ := value.([]any)
	changes := make([]worlds.RunsAttributeChange, 0, len(items))
	for _, item := range items {
		values, ok := item.(map[string]any)
		if !ok {
			continue
		}
		change := worlds.RunsAttributeChange{Key: str(values, "key")}
		if text, ok := values["value"].(string); ok {
			change.Value = &text
		}
		changes = append(changes, change)
	}
	return changes
}

func attributeWriter(value any) worlds.AttributeWriter {
	values, _ := value.(map[string]any)
	writer := worlds.AttributeWriter{Type: str(values, "type")}
	if writer.Type == "" {
		writer.Type = worlds.AttributeWriterWorkflow
	}
	writer.StepID = worlds.StepID(str(values, "stepId"))
	writer.Attempt, _ = asInt(values["attempt"])
	return writer
}

func hookClaim(value any) *worlds.HookForceClaim {
	values, ok := value.(map[string]any)
	if !ok || str(values, "runId") == "" {
		return nil
	}
	return &worlds.HookForceClaim{RunID: worlds.RunID(str(values, "runId")), HookID: worlds.HookID(str(values, "hookId"))}
}
