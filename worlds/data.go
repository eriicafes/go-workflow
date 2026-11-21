package worlds

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strings"
	"time"
	"unicode/utf16"
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
// its metadata, such as a step's name.
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
	if raw, ok := object["startHook"]; ok && eventType == EventTypeRunCreated {
		// A start hook's metadata is user data; its token and retention are
		// not.
		var hook map[string]json.RawMessage
		if json.Unmarshal(raw, &hook) == nil {
			delete(hook, "metadata")
			object["startHook"], _ = json.Marshal(hook)
		}
	}
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
// or from the step_started that created an inline step, Output from step_completed, and Error from step_failed or step_retrying; and
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
	case EventTypeStepStarted:
		var data DataStepStarted
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

// EventPayloadFields returns, by event type, the Data field holding user
// data. ScrubEventData removes it. A World that deletes data in its own
// storage, such as in SQL, uses it to find the payloads.
func EventPayloadFields() map[EventType]string {
	return maps.Clone(eventPayloadFields)
}

// ScrubEventData returns event Data without its user data, as deleting a
// run's data leaves it: the payload field of eventType is removed and its
// metadata, such as a step's name or a hook's token, is kept.
func ScrubEventData(eventType EventType, data []byte) []byte {
	return ResolveEventData(eventType, data, EventsResolveDataNone)
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
	WorkflowName string `json:"workflowName" cbor:"workflowName"`
	// Input is the run's argument list, [input], as a payload.
	Input        []byte            `json:"input" cbor:"input"`
	Attributes   map[string]string `json:"attributes" cbor:"attributes"`
	DeploymentID string            `json:"deploymentId,omitempty" cbor:"deploymentId,omitempty"`
	// EncryptionPublicKey is the run's X25519 public key in base64, which
	// writers outside the run seal payloads to. It is set when the run's
	// payloads are encrypted.
	EncryptionPublicKey string `json:"encryptionPublicKey,omitempty" cbor:"encryptionPublicKey,omitempty"`
	// ExecutionContext is the run's executionContext.
	ExecutionContext map[string]any `json:"executionContext,omitempty" cbor:"executionContext,omitempty"`
	// StartHook is the hook created with the run. See StartHook.
	StartHook *StartHook `json:"startHook,omitempty" cbor:"startHook,omitempty"`
	// AllowReservedAttributes permits Attributes keys that start with
	// ReservedAttributePrefix.
	AllowReservedAttributes bool `json:"allowReservedAttributes,omitempty" cbor:"allowReservedAttributes,omitempty"`
}

// StartHook is a hook a World creates in the same write as its run, so a
// second start with the same token is refused before it becomes a run. Its ID
// is StartHookID of the run, and no hook_created is recorded for it:
// run_created stands for it. From then on it is an ordinary hook of the run,
// which the workflow opens with a hook of the same token.
type StartHook struct {
	Token string `json:"token" cbor:"token"`
	// HookID is the hook's ID, which the World sets on what it records.
	HookID HookID `json:"hookId,omitempty" cbor:"hookId,omitempty"`
	// TokenRetentionUntil keeps the token after the run ends until then.
	TokenRetentionUntil *time.Time `json:"tokenRetentionUntil,omitempty" cbor:"tokenRetentionUntil,omitempty"`
	// Metadata is the hook's metadata payload, as a hook_created records
	// it.
	Metadata []byte `json:"metadata,omitempty" cbor:"metadata,omitempty"`
}

// StartHookID returns the ID of the start hook of runID: hook_ and the run
// ID's ULID.
func StartHookID(runID RunID) HookID {
	return HookID("hook_" + strings.TrimPrefix(string(runID), "wrun_"))
}

// StartHookCandidateMaxAge is the age, by its run ID, past which a start hook
// candidate is refused with ErrStartHookRejected: it can never be created,
// and its token is free (@workflow/world START_HOOK_CANDIDATE_MAX_AGE_MS).
const StartHookCandidateMaxAge = 24 * time.Hour

// StartHookDecision is a World's permanent decision on a start hook
// candidate, by its run ID: accepted, or rejected because ConflictingRunID
// held the token. Every later attempt for the candidate, by run_created or by
// a first flow's run_started, gets the same decision.
type StartHookDecision struct {
	Token            string `json:"token" cbor:"token"`
	Accepted         bool   `json:"accepted" cbor:"accepted"`
	ConflictingRunID RunID  `json:"conflictingRunId,omitempty" cbor:"conflictingRunId,omitempty"`
}

// Err returns the error a later attempt for a rejected candidate returns.
func (d StartHookDecision) Err() error {
	if d.Accepted {
		return nil
	}
	return &HookConflictError{RunID: d.ConflictingRunID}
}

// StartHookCandidateExpired reports whether a start hook candidate's run ID
// is older than StartHookCandidateMaxAge. A run ID that is not a ULID is
// never expired.
func StartHookCandidateExpired(runID RunID, now time.Time) bool {
	created, ok := RunIDTime(runID)
	return ok && now.Sub(created) > StartHookCandidateMaxAge
}

// RepeatsAcceptedStart reports whether input repeats the start of an accepted
// candidate whose run_created recorded runCreated: both carry a start hook
// with the same token. A World returns that run_created for it, never
// ErrRunAlreadyExists.
func RepeatsAcceptedStart(runCreated []byte, input EventsCreateRunInput) bool {
	start := RunStartHook(runCreated)
	return input.StartHook != nil && start != nil && start.Token == input.StartHook.Token
}

// RunEchoStartHook returns the start hook a run echoes: the one its
// run_created records, without its metadata.
func RunEchoStartHook(runCreated []byte) *StartHook {
	start := RunStartHook(runCreated)
	if start == nil {
		return nil
	}
	echo := *start
	echo.Metadata = nil
	return &echo
}

// RunStartHook returns the start hook a run_created event records, or nil.
func RunStartHook(data []byte) *StartHook {
	var created DataRunCreated
	if json.Unmarshal(data, &created) != nil {
		return nil
	}
	return created.StartHook
}

// ExecutionContextReplayedFromRunID is the executionContext key naming the
// run a run was replayed from, so tools link the replay to its source.
const ExecutionContextReplayedFromRunID = "replayedFromRunId"

// RunCreatedData returns the Data of the run_created event of input.
func RunCreatedData(input EventsCreateRunInput) ([]byte, error) {
	return json.Marshal(DataRunCreated{
		WorkflowName:            input.WorkflowName,
		Input:                   input.Input,
		Attributes:              input.Attributes,
		DeploymentID:            input.DeploymentID,
		EncryptionPublicKey:     input.EncryptionPublicKey,
		ExecutionContext:        input.ExecutionContext,
		StartHook:               startHookOf(input),
		AllowReservedAttributes: input.AllowReservedAttributes,
	})
}

// startHookOf returns the start hook of input as run_created records it,
// with its ID.
func startHookOf(input EventsCreateRunInput) *StartHook {
	if input.StartHook == nil {
		return nil
	}
	hook := *input.StartHook
	hook.HookID = StartHookID(input.RunID)
	return &hook
}

// DataRunCompleted is the Data of a run_completed event.
type DataRunCompleted struct {
	// Output is the run's output as a payload.
	Output []byte `json:"output,omitempty" cbor:"output,omitempty"`
}

// DecodeRunCompleted returns the Output of run_completed event Data, or nil
// when data is not a DataRunCompleted.
func DecodeRunCompleted(data []byte) []byte {
	var completed DataRunCompleted
	if json.Unmarshal(data, &completed) != nil {
		return nil
	}
	return completed.Output
}

// DataRunFailed is the Data of a run_failed event.
type DataRunFailed struct {
	// Error is the run's error as a payload.
	Error []byte `json:"error,omitempty" cbor:"error,omitempty"`
	// ErrorCode is the category of the error, such as USER_ERROR or
	// MAX_DELIVERIES_EXCEEDED, kept in plain text so tools can sort failures
	// without opening the payload.
	ErrorCode string `json:"errorCode,omitempty" cbor:"errorCode,omitempty"`
}

// DecodeRunFailed returns the Error of run_failed event Data, or nil when data
// is not a DataRunFailed.
func DecodeRunFailed(data []byte) []byte {
	var failed DataRunFailed
	if json.Unmarshal(data, &failed) != nil {
		return nil
	}
	return failed.Error
}

// EncodeRunFailed returns the Data of a run_failed event for err, its
// payload written with options.
func EncodeRunFailed(err error, options PayloadOptions) ([]byte, error) {
	payload, encodeErr := EncodeErrorPayload(err, options)
	if encodeErr != nil {
		return nil, encodeErr
	}
	return json.Marshal(DataRunFailed{Error: payload, ErrorCode: runErrorCode(err)})
}

// RunFailedErrorCode returns the ErrorCode of a run_failed event's Data,
// "" when it has none.
func RunFailedErrorCode(data []byte) string {
	var failed DataRunFailed
	if json.Unmarshal(data, &failed) != nil {
		return ""
	}
	return failed.ErrorCode
}

// RunErrorCoder is implemented by an error that names the ErrorCode of the
// run_failed event a run that failed with it records.
type RunErrorCoder interface{ RunErrorCode() string }

// runErrorCode returns the ErrorCode of the error a run failed with.
func runErrorCode(err error) string {
	var coder RunErrorCoder
	coded := errors.As(err, &coder)
	switch {
	case coded && coder.RunErrorCode() == "REPLAY_DIVERGENCE":
		return "REPLAY_DIVERGENCE"
	case errors.Is(err, ErrCorruptedEventLog):
		return "CORRUPTED_EVENT_LOG"
	case errors.Is(err, ErrMaxDeliveriesExceeded):
		return "MAX_DELIVERIES_EXCEEDED"
	case errors.Is(err, ErrDeploymentMismatch):
		return "DEPLOYMENT_MISMATCH"
	case errors.Is(err, ErrWorkflowNotFound):
		return "WORKFLOW_NOT_REGISTERED"
	case coded:
		return coder.RunErrorCode()
	}
	return "USER_ERROR"
}

// DataStepCreated is the Data of a step_created event. The step's options
// and cancellation contexts come from the replay that declares it.
type DataStepCreated struct {
	StepName     string `json:"stepName" cbor:"stepName"`
	WorkflowName string `json:"workflowName,omitempty" cbor:"workflowName,omitempty"`
	// Input is the step's arguments, {args: [input]}, as a payload.
	Input []byte `json:"input" cbor:"input"`
	// Inline reports that the flow runs the step itself. A step_created
	// records a step queued as a step delivery, or one that failed before
	// it started, and an inline step is created by its first step_started.
	Inline bool `json:"inline,omitempty" cbor:"inline,omitempty"`
	// CreatorMessageID is the queued message of the flow that created the
	// step.
	CreatorMessageID string `json:"creatorMessageId,omitempty" cbor:"creatorMessageId,omitempty"`
}

// DataStepStarted is the Data of a step_started event.
type DataStepStarted struct {
	StepName string `json:"stepName,omitempty" cbor:"stepName,omitempty"`
	// Attempt is the attempt the event starts, counting from 1.
	Attempt int `json:"attempt,omitempty" cbor:"attempt,omitempty"`
	// WorkflowName, Input, and Inline are set on the step_started that
	// creates an inline step, which no step_created precedes. Input is the
	// step's arguments, {args: [input]}, as a payload.
	WorkflowName string `json:"workflowName,omitempty" cbor:"workflowName,omitempty"`
	Input        []byte `json:"input,omitempty" cbor:"input,omitempty"`
	Inline       bool   `json:"inline,omitempty" cbor:"inline,omitempty"`
}

// StartCreatesStep reports whether a step_started event's data creates the
// step it starts: an inline step's first step_started, which carries the
// step's name and input with Inline set, and which no step_created precedes.
func StartCreatesStep(data []byte) bool {
	var started DataStepStarted
	return len(data) != 0 && json.Unmarshal(data, &started) == nil && started.Inline && started.StepName != ""
}

// DataStepResult is the Data of a step_completed, step_retrying, or
// step_failed event.
type DataStepResult struct {
	StepName string `json:"stepName,omitempty" cbor:"stepName,omitempty"`
	// WorkflowName is set on step_completed.
	WorkflowName string `json:"workflowName,omitempty" cbor:"workflowName,omitempty"`
	// Attempt is the attempt that failed, on step_failed and step_retrying.
	Attempt    int        `json:"attempt,omitempty" cbor:"attempt,omitempty"`
	Result     []byte     `json:"result,omitempty" cbor:"result,omitempty"`
	Error      []byte     `json:"error,omitempty" cbor:"error,omitempty"`
	RetryAfter *time.Time `json:"retryAfter,omitempty" cbor:"retryAfter,omitempty"`
	// CreatorMessageID is set on the step_retrying of an inline step whose
	// next attempt runs as a step delivery: the queued message of the flow
	// that hands it over.
	CreatorMessageID string `json:"creatorMessageId,omitempty" cbor:"creatorMessageId,omitempty"`
}

// DataWaitCreated is the Data of a wait_created event. The wait's
// cancellation contexts come from the replay.
type DataWaitCreated struct {
	ResumeAt time.Time `json:"resumeAt" cbor:"resumeAt"`
	// CreatorMessageID is the queued message of the flow that created the
	// wait, whose chain of timer messages arms it.
	CreatorMessageID string `json:"creatorMessageId,omitempty" cbor:"creatorMessageId,omitempty"`
}

// DataWaitCompleted is the Data of a wait_completed event.
type DataWaitCompleted struct {
	// ResumeAt is when the wait was due, as its wait_created records it.
	ResumeAt *time.Time `json:"resumeAt,omitempty" cbor:"resumeAt,omitempty"`
}

// DataHookCreated is the Data of a hook_created or hook_conflict event.
type DataHookCreated struct {
	Token     string `json:"token" cbor:"token"`
	Metadata  []byte `json:"metadata,omitempty" cbor:"metadata,omitempty"`
	IsWebhook bool   `json:"isWebhook,omitempty" cbor:"isWebhook,omitempty"`
	IsSystem  bool   `json:"isSystem,omitempty" cbor:"isSystem,omitempty"`
	// TokenRetentionUntil reserves the token until this time, including
	// after the run ends.
	TokenRetentionUntil *time.Time      `json:"tokenRetentionUntil,omitempty" cbor:"tokenRetentionUntil,omitempty"`
	Force               bool            `json:"force,omitempty" cbor:"force,omitempty"`
	ForceClaimedFrom    *HookForceClaim `json:"forceClaimedFrom,omitempty" cbor:"forceClaimedFrom,omitempty"`
	ConflictingRunID    RunID           `json:"conflictingRunId,omitempty" cbor:"conflictingRunId,omitempty"`
}

// DataHookReceived is the Data of a hook_received event.
type DataHookReceived struct {
	// Payload is the value sent to the hook as a payload. A webhook
	// receives a WebhookRequest.
	Payload []byte `json:"payload,omitempty" cbor:"payload,omitempty"`
	// WorkflowName is the workflow of the hook's run.
	WorkflowName string `json:"workflowName,omitempty" cbor:"workflowName,omitempty"`
}

// EncodeHookReceived returns the Data of a hook_received event carrying
// payload to a hook of a run of workflowName.
func EncodeHookReceived(payload []byte, workflowName string) ([]byte, error) {
	return json.Marshal(DataHookReceived{Payload: payload, WorkflowName: workflowName})
}

// DecodeHookReceived returns the Payload of hook_received event Data, or nil
// when data is not a DataHookReceived.
func DecodeHookReceived(data []byte) []byte {
	var received DataHookReceived
	if json.Unmarshal(data, &received) != nil {
		return nil
	}
	return received.Payload
}

// HookDisposedEventData is the Data of a hook_disposed event. It is empty
// when the hook disposed of itself. ForceClaimedBy is set when another
// hook took the token with a forced claim.
type HookDisposedEventData struct {
	ForceClaimedBy *HookForceClaim `json:"forceClaimedBy,omitempty" cbor:"forceClaimedBy,omitempty"`
}

// DataAttrSet is the Data of an attr_set event. AllowReservedAttributes
// records that the writer permitted keys with the reserved prefix.
type DataAttrSet struct {
	Changes                 []RunsAttributeChange `json:"changes" cbor:"changes"`
	Writer                  AttributeWriter       `json:"writer" cbor:"writer"`
	AllowReservedAttributes bool                  `json:"allowReservedAttributes,omitempty" cbor:"allowReservedAttributes,omitempty"`
}

// WorkflowAttrSet reports whether an attr_set's Data was written by the
// workflow, whose attribute writes are recorded once per correlation ID.
func WorkflowAttrSet(data []byte) bool {
	var set DataAttrSet
	return json.Unmarshal(data, &set) == nil && set.Writer.Type == AttributeWriterWorkflow
}

// EncodeAttrSet returns the Data of the attr_set event that applies input.
func EncodeAttrSet(input RunsSetAttributesInput) ([]byte, error) {
	data := DataAttrSet{Changes: input.Changes, Writer: AttributeWriter{Type: AttributeWriterWorkflow}, AllowReservedAttributes: input.AllowReservedAttributes}
	if input.Writer != nil {
		data.Writer = *input.Writer
	}
	return json.Marshal(data)
}

// Run attribute limits. See CheckAttributes.
const (
	// MaxAttributeKeyLength is the longest attribute key, in UTF-16 code
	// units as JavaScript counts a string's length.
	MaxAttributeKeyLength = 256
	// MaxAttributeValueBytes is the longest attribute value in UTF-8 bytes.
	MaxAttributeValueBytes = 256
	// MaxAttributesPerRun is the most attributes one run holds.
	MaxAttributesPerRun = 64
	// ReservedAttributePrefix starts the keys reserved for framework and
	// library code, such as RetentionAttribute.
	ReservedAttributePrefix = "$"
)

// CheckAttributes returns ErrAttributeInvalid unless attributes are within
// the limits: at most MaxAttributesPerRun, with non-empty keys of at most
// MaxAttributeKeyLength and values of at most MaxAttributeValueBytes. Keys
// starting with ReservedAttributePrefix are refused unless allowReserved.
func CheckAttributes(attributes map[string]string, allowReserved bool) error {
	if len(attributes) > MaxAttributesPerRun {
		return fmt.Errorf("%w: %d attributes, over the limit of %d", ErrAttributeInvalid, len(attributes), MaxAttributesPerRun)
	}
	for key, value := range attributes {
		switch {
		case key == "":
			return fmt.Errorf("%w: empty key", ErrAttributeInvalid)
		case len(utf16.Encode([]rune(key))) > MaxAttributeKeyLength:
			return fmt.Errorf("%w: key %.32q is longer than %d", ErrAttributeInvalid, key, MaxAttributeKeyLength)
		case !allowReserved && strings.HasPrefix(key, ReservedAttributePrefix):
			return fmt.Errorf("%w: key %q starts with the reserved prefix %q", ErrAttributeInvalid, key, ReservedAttributePrefix)
		case len(value) > MaxAttributeValueBytes:
			return fmt.Errorf("%w: value of %q is %d bytes, over the limit of %d", ErrAttributeInvalid, key, len(value), MaxAttributeValueBytes)
		}
	}
	return nil
}

// AttributeWriter names what set run attributes: the workflow, or one
// attempt of a step.
type AttributeWriter struct {
	// Type is AttributeWriterWorkflow or AttributeWriterStep.
	Type    string `json:"type" cbor:"type"`
	StepID  StepID `json:"stepId,omitempty" cbor:"stepId,omitempty"`
	Attempt int    `json:"attempt,omitempty" cbor:"attempt,omitempty"`
}

// AttributeWriter types.
const (
	AttributeWriterWorkflow = "workflow"
	AttributeWriterStep     = "step"
)

// FinishedRunRefusal returns the error a World refuses event with on a
// finished run, the run's status, as the Workflow SDK's Worlds refuse it, or
// nil when the event is checked as on a running run. stepStatus is the
// status of the event's step, or empty.
//
//   - A run_cancelled of a cancelled run is recorded: nil.
//   - A terminal transition, a step, wait, or hook creation, an inline
//     step's creating step_started, and an attr_set are refused with
//     ErrEventConflict, the SDK's EntityConflictError. The error also
//     matches ErrRunTerminal, which says why.
//   - A run_started, a step_started, a hook_received, and a step's result
//     while the step is not running are refused with ErrRunTerminal, the
//     SDK's RunExpiredError.
//   - A wait_completed is refused with ErrEventNotFound, since a run's waits
//     end with it.
//   - A step's result while it runs, a result of a step a World refuses
//     anyway, and a hook_disposed, which needs a hook its token retention
//     kept, are checked further: nil.
func FinishedRunRefusal(status Status, event EventsCreateEvent, stepStatus Status) error {
	switch event.EventType {
	case EventTypeRunCancelled:
		if status == StatusCancelled {
			return nil
		}
		return fmt.Errorf("%w: %w: cannot transition a run from terminal status %s", ErrEventConflict, ErrRunTerminal, status)
	case EventTypeRunCompleted, EventTypeRunFailed:
		return fmt.Errorf("%w: %w: cannot transition a run from terminal status %s", ErrEventConflict, ErrRunTerminal, status)
	case EventTypeStepCreated, EventTypeWaitCreated, EventTypeHookCreated, EventTypeAttrSet:
		return fmt.Errorf("%w: %w: cannot record %s on a run in terminal status %s", ErrEventConflict, ErrRunTerminal, event.EventType, status)
	case EventTypeStepStarted:
		if StartCreatesStep(event.Data) {
			return fmt.Errorf("%w: %w: cannot create a step on a run in terminal status %s", ErrEventConflict, ErrRunTerminal, status)
		}
	case EventTypeStepCompleted, EventTypeStepFailed, EventTypeStepRetrying:
		if stepStatus == StatusRunning || stepStatus == "" || stepStatus.Terminal() {
			return nil
		}
	case EventTypeWaitCompleted:
		return fmt.Errorf("%w: wait %s ended with its run", ErrEventNotFound, event.CorrelationID)
	case EventTypeHookDisposed:
		return nil
	}
	return fmt.Errorf("%w: run is in terminal status %s", ErrRunTerminal, status)
}

// Run ID time limits of a run's creation, as the Workflow SDK's
// validateUlidTimestamp checks them.
const (
	// RunIDMaxAge is how far in the past a new run's ID may have been minted.
	RunIDMaxAge = 24 * time.Hour
	// RunIDMaxLead is how far in the future a new run's ID may be minted.
	RunIDMaxLead = 5 * time.Minute
)

// CheckRunCreation returns the error a World refuses the creation input
// describes with, as the Workflow SDK's Worlds check it, or nil. The run ID
// must be wrun_ and a ULID minted within RunIDMaxAge before now and
// RunIDMaxLead after it, unless the run is created FromRunStarted: otherwise
// ErrEventInvalid, or ErrStartHookRejected for a run with a start hook. Its
// attributes must be within the limits of CheckAttributes
// (ErrAttributeInvalid).
func CheckRunCreation(input EventsCreateRunInput, now time.Time) error {
	if !input.FromRunStarted {
		refused := ErrEventInvalid
		if input.StartHook != nil {
			refused = ErrStartHookRejected
		}
		minted, ok := RunIDTime(input.RunID)
		switch {
		case !ok:
			return fmt.Errorf("%w: run ID %q is not wrun_ and a ULID", refused, input.RunID)
		case now.Sub(minted) > RunIDMaxAge:
			return fmt.Errorf("%w: run ID %s was minted %s ago, over %s", refused, input.RunID, now.Sub(minted).Round(time.Second), RunIDMaxAge)
		case minted.Sub(now) > RunIDMaxLead:
			return fmt.Errorf("%w: run ID %s is minted %s ahead, over %s", refused, input.RunID, minted.Sub(now).Round(time.Second), RunIDMaxLead)
		}
	}
	return CheckAttributes(input.Attributes, input.AllowReservedAttributes)
}

// CheckAttributeChanges returns ErrAttributeInvalid unless an attr_set's
// changes may apply to a run's attributes, as the Workflow SDK's
// validateAttributeChanges checks them: each key within the limits of
// CheckAttributes and set once, each value within MaxAttributeValueBytes,
// and at most MaxAttributesPerRun attributes after the changes.
func CheckAttributeChanges(attributes map[string]string, changes []RunsAttributeChange, allowReserved bool) error {
	seen := make(map[string]bool, len(changes))
	count := len(attributes)
	for _, change := range changes {
		value := ""
		if change.Value != nil {
			value = *change.Value
		}
		if err := CheckAttributes(map[string]string{change.Key: value}, allowReserved); err != nil {
			return err
		}
		if seen[change.Key] {
			return fmt.Errorf("%w: key %q is changed more than once", ErrAttributeInvalid, change.Key)
		}
		seen[change.Key] = true
		_, exists := attributes[change.Key]
		switch {
		case change.Value == nil && exists:
			count--
		case change.Value != nil && !exists:
			count++
		}
	}
	if count > MaxAttributesPerRun {
		return fmt.Errorf("%w: %d attributes after the changes, over the limit of %d", ErrAttributeInvalid, count, MaxAttributesPerRun)
	}
	return nil
}

// MaxAttrSetDataBytes is the most JSON bytes an attr_set's Data may take, as
// the Workflow SDK's ATTRIBUTE_EVENT_DATA_MAX_BYTES.
const MaxAttrSetDataBytes = 8192

// CheckAttrSet returns ErrAttributeInvalid unless the attr_set Data may apply
// to a run's attributes (CheckAttributeChanges).
func CheckAttrSet(attributes map[string]string, data []byte) error {
	var set DataAttrSet
	if err := json.Unmarshal(data, &set); err != nil {
		return fmt.Errorf("%w: attr_set data: %v", ErrEventInvalid, err)
	}
	return CheckAttributeChanges(attributes, set.Changes, set.AllowReservedAttributes)
}

// CheckStepStart returns a *TooEarlyError for a step_started of a step whose
// retry is due at retryAfter, while now is before it, as the Workflow SDK's
// Worlds refuse it, or nil.
func CheckStepStart(retryAfter *time.Time, now time.Time) error {
	if retryAfter == nil || !now.Before(*retryAfter) {
		return nil
	}
	return &TooEarlyError{RetryAfter: retryAfter.Sub(now)}
}

// CheckResumeDigest returns ErrEventInvalid unless a hook_received's
// ResumePayloadDigest is the digest of its payload, as world-postgres
// checks it. An empty digest is not checked.
func CheckResumeDigest(event EventsCreateEvent, digest string) error {
	if digest == "" || event.EventType != EventTypeHookReceived {
		return nil
	}
	if ResumeDigest(DecodeHookReceived(event.Data)) != digest {
		return fmt.Errorf("%w: resume digest does not match its payload", ErrEventInvalid)
	}
	return nil
}

// CheckStepEvent returns the error a World refuses a step event of a running
// run with, as the Workflow SDK's Worlds refuse it, or nil. exists, status,
// and retryAfter describe the event's step.
//
//   - A step_created of a step that exists, and an inline step's creating
//     step_started (StartCreatesStep) of one, are refused with
//     ErrEventConflict.
//   - Any other step event needs its step (ErrStepNotFound), which must not
//     have finished (ErrEventConflict).
//   - A step_started before the step's retry is due is refused with a
//     *TooEarlyError (CheckStepStart).
func CheckStepEvent(event EventsCreateEvent, exists bool, status Status, retryAfter *time.Time, now time.Time) error {
	switch event.EventType {
	case EventTypeStepCreated:
		if exists {
			return fmt.Errorf("%w: step %s already created", ErrEventConflict, event.CorrelationID)
		}
		return nil
	case EventTypeStepStarted, EventTypeStepCompleted, EventTypeStepFailed, EventTypeStepRetrying:
	default:
		return nil
	}
	creates := event.EventType == EventTypeStepStarted && StartCreatesStep(event.Data)
	switch {
	case creates && exists:
		return fmt.Errorf("%w: step %s already created", ErrEventConflict, event.CorrelationID)
	case creates:
		return nil
	case !exists:
		return fmt.Errorf("%w: step %s", ErrStepNotFound, event.CorrelationID)
	case status.Terminal():
		return fmt.Errorf("%w: step %s is %s", ErrEventConflict, event.CorrelationID, status)
	case event.EventType == EventTypeStepStarted:
		return CheckStepStart(retryAfter, now)
	}
	return nil
}
