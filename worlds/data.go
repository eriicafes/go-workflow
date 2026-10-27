package worlds

import (
	"encoding/json"
	"fmt"
	"time"
)

// eventPayloadFields names the payload field of each event type's Data.
// Other fields are metadata.
var eventPayloadFields = map[EventType]string{
	EventTypeRunCreated:    "input",
	EventTypeRunStarted:    "input",
	EventTypeRunCompleted:  "output",
	EventTypeRunFailed:     "error",
	EventTypeStepCreated:   "input",
	EventTypeStepStarted:   "input",
	EventTypeStepCompleted: "result",
	EventTypeStepFailed:    "error",
	EventTypeStepRetrying:  "error",
	EventTypeHookCreated:   "metadata",
	EventTypeHookConflict:  "metadata",
	EventTypeHookReceived:  "payload",
}

// ResolveEventData returns the Data an event of eventType carries under
// resolve. EventsResolveDataNone removes the type's payload field and keeps
// its metadata, such as a step's name and position.
// Data that is not a JSON object, or has no field left, is removed. Other
// resolve values return data unchanged.
func ResolveEventData(eventType EventType, data []byte, resolve EventsResolveData) []byte {
	field, ok := eventPayloadFields[eventType]
	if resolve != EventsResolveDataNone || len(data) == 0 || !ok {
		return data
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(data, &object); err != nil {
		return nil
	}
	delete(object, field)
	if len(object) == 0 {
		return nil
	}
	stripped, err := json.Marshal(object)
	if err != nil {
		return nil
	}
	return stripped
}

// EventPayload returns the payload a run, step, or hook view exposes from the
// event that recorded it: a run's Input from run_created, Output from
// run_completed, and Error from run_failed; a step's Input from step_created,
// Output from step_completed, and Error from step_failed or step_retrying; and
// a hook's Metadata from hook_created. It returns nil for other events.
//
// The event log is the only copy of these payloads. A World's views record
// which event holds each one and read it back with EventPayload.
func EventPayload(event Event) []byte {
	switch event.EventType {
	case EventTypeRunCreated:
		var data DataRunCreated
		_ = json.Unmarshal(event.Data, &data)
		return data.Input
	case EventTypeRunCompleted:
		return DecodeRunCompleted(event.Data)
	case EventTypeRunFailed:
		return DecodeRunFailed(event.Data)
	case EventTypeStepCreated:
		var data DataStepCreated
		_ = json.Unmarshal(event.Data, &data)
		return data.Input
	case EventTypeStepCompleted:
		var data DataStepResult
		_ = json.Unmarshal(event.Data, &data)
		return data.Result
	case EventTypeStepFailed, EventTypeStepRetrying:
		var data DataStepResult
		_ = json.Unmarshal(event.Data, &data)
		return data.Error
	case EventTypeHookCreated:
		var data DataHookCreated
		_ = json.Unmarshal(event.Data, &data)
		return data.Metadata
	}
	return nil
}

// ResolveEvents applies ResolveEventData to each event's Data in place and
// returns events.
func ResolveEvents(events []Event, resolve EventsResolveData) []Event {
	for index := range events {
		events[index].Data = ResolveEventData(events[index].EventType, events[index].Data, resolve)
	}
	return events
}

// DataRunCreated is the Data of a run_created event.
type DataRunCreated struct {
	WorkflowName string            `json:"workflowName"`
	Input        json.RawMessage   `json:"input"`
	Attributes   map[string]string `json:"attributes"`
	DeploymentID string            `json:"deploymentId,omitempty"`
}

// DataRunCompleted is the Data of a run_completed event.
type DataRunCompleted struct {
	// Output is the JSON output of the run.
	Output json.RawMessage `json:"output,omitempty"`
}

// DecodeRunCompleted returns the Output of run_completed event Data, or nil
// when data is not a DataRunCompleted.
func DecodeRunCompleted(data []byte) json.RawMessage {
	var completed DataRunCompleted
	if json.Unmarshal(data, &completed) != nil {
		return nil
	}
	return completed.Output
}

// DataRunFailed is the Data of a run_failed event.
type DataRunFailed struct {
	// Error is the run's error, encoded as an Error.
	Error json.RawMessage `json:"error,omitempty"`
}

// DecodeRunFailed returns the Error of run_failed event Data, or nil when data
// is not a DataRunFailed.
func DecodeRunFailed(data []byte) json.RawMessage {
	var failed DataRunFailed
	if json.Unmarshal(data, &failed) != nil {
		return nil
	}
	return failed.Error
}

// EncodeRunFailed returns the Data of a run_failed event for err.
func EncodeRunFailed(err error) []byte {
	// Encoding cannot fail, because encodeError returns valid JSON.
	data, _ := json.Marshal(DataRunFailed{Error: encodeError(err)})
	return data
}

// DataStepCreated is the JSON payload of a step_created event.
type DataStepCreated struct {
	Position     int                     `json:"position"`
	StepName     string                  `json:"stepName"`
	Input        json.RawMessage         `json:"input"`
	MaxRetries   int                     `json:"maxRetries"`
	Rollback     bool                    `json:"rollback,omitempty"`
	Cancellation []cancellationReference `json:"cancellation,omitempty"`
}

// DataStepStarted is the JSON payload of a step_started event.
type DataStepStarted struct {
	// OwnerMessageID is the queued message of the flow that started the
	// attempt inline, which owns it until inlineStepLease passes. It is
	// empty for an attempt a step delivery started.
	OwnerMessageID string `json:"ownerMessageId,omitempty"`
}

// DataStepResult is the JSON payload of a step_completed, step_retrying, or
// step_failed event.
type DataStepResult struct {
	Result     json.RawMessage `json:"result,omitempty"`
	Error      json.RawMessage `json:"error,omitempty"`
	RetryAfter *time.Time      `json:"retryAfter,omitempty"`
}

// DataWaitCreated is the JSON payload of a wait_created event.
type DataWaitCreated struct {
	Position       int                     `json:"position"`
	ResumeAt       time.Time               `json:"resumeAt"`
	QueueMessageID string                  `json:"queueMessageId"`
	Cancellation   []cancellationReference `json:"cancellation,omitempty"`
}

// DataHookCreated is the JSON payload of a hook_created or hook_conflict
// event. Webhook carries the settings that IsWebhook summarizes.
type DataHookCreated struct {
	Position  int      `json:"position"`
	Token     string   `json:"token"`
	Metadata  []byte   `json:"metadata,omitempty"`
	IsWebhook bool     `json:"isWebhook,omitempty"`
	IsSystem  bool     `json:"isSystem,omitempty"`
	Webhook   *Webhook `json:"webhook,omitempty"`
	// TokenRetentionUntil reserves the token until this time, including
	// after the run ends.
	TokenRetentionUntil *time.Time      `json:"tokenRetentionUntil,omitempty"`
	Force               bool            `json:"force,omitempty"`
	ForceClaimedFrom    *HookForceClaim `json:"forceClaimedFrom,omitempty"`
	ConflictingRunID    RunID           `json:"conflictingRunId,omitempty"`
}

// DataHookReceived is the Data of a hook_received event.
type DataHookReceived struct {
	// Payload is the JSON value sent to the hook. A webhook receives a
	// WebhookReceivedEventData.
	Payload json.RawMessage `json:"payload,omitempty"`
}

// EncodeHookReceived returns the Data of a hook_received event carrying the
// JSON payload.
func EncodeHookReceived(payload []byte) ([]byte, error) {
	data, err := json.Marshal(DataHookReceived{Payload: payload})
	if err != nil {
		return nil, fmt.Errorf("%w: hook payload is not JSON: %v", ErrEventInvalid, err)
	}
	return data, nil
}

// DecodeHookReceived returns the Payload of hook_received event Data, or nil
// when data is not a DataHookReceived.
func DecodeHookReceived(data []byte) json.RawMessage {
	var received DataHookReceived
	if json.Unmarshal(data, &received) != nil {
		return nil
	}
	return received.Payload
}

// WebhookReceivedEventData is the Data of a hook_received event for a
// webhook. It holds either the received Request or, for a manual
// webhook, the workflow's Response to the request event named by
// ResponseTo.
type WebhookReceivedEventData struct {
	Request    *WebhookRequest  `json:"request,omitempty"`
	Response   *WebhookResponse `json:"response,omitempty"`
	ResponseTo EventID          `json:"responseTo,omitempty"`
}

// DecodeWebhookReceived decodes the WebhookReceivedEventData that
// hook_received event Data carries for a webhook. It reports false for other
// Data.
func DecodeWebhookReceived(data []byte) (WebhookReceivedEventData, bool) {
	var received WebhookReceivedEventData
	payload := DecodeHookReceived(data)
	if payload == nil || json.Unmarshal(payload, &received) != nil {
		return WebhookReceivedEventData{}, false
	}
	return received, true
}

// HookDisposedEventData is the Data of a hook_disposed event. It is empty
// when the hook disposed of itself. ForceClaimedBy is set when another
// hook took the token with a forced claim.
type HookDisposedEventData struct {
	ForceClaimedBy *HookForceClaim `json:"forceClaimedBy,omitempty"`
}

// DataAttrSet is the Data of an attr_set event.
type DataAttrSet struct {
	Changes []RunsAttributeChange `json:"changes"`
	Writer  AttributeWriter       `json:"writer"`
}

// EncodeAttrSet returns the Data of the attr_set event that applies input.
func EncodeAttrSet(input RunsSetAttributesInput) ([]byte, error) {
	data := DataAttrSet{Changes: input.Changes, Writer: AttributeWriter{Type: AttributeWriterWorkflow}}
	if input.Writer != nil {
		data.Writer = *input.Writer
	}
	return json.Marshal(data)
}

// AttributeWriter names what set run attributes: the workflow, or one
// attempt of a step.
type AttributeWriter struct {
	// Type is AttributeWriterWorkflow or AttributeWriterStep.
	Type    string `json:"type"`
	StepID  StepID `json:"stepId,omitempty"`
	Attempt int    `json:"attempt,omitempty"`
}

// AttributeWriter types.
const (
	AttributeWriterWorkflow = "workflow"
	AttributeWriterStep     = "step"
)
