package worlds

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
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
	EventTypeHookReceived:  "payload",
}

// ResolveEventData returns the Data an event of eventType carries under
// resolve. EventsResolveDataNone removes the type's payload field and keeps
// its metadata, such as a step's name. Data that is not a JSON object, or
// has no field left, is removed. Other resolve values return data unchanged.
func ResolveEventData(eventType EventType, data []byte, resolve EventsResolveData) []byte {
	return stripEventData(eventType, data, resolve, false)
}

func stripEventData(eventType EventType, data []byte, resolve EventsResolveData, startHookMetadata bool) []byte {
	field, ok := eventPayloadFields[eventType]
	if resolve != EventsResolveDataNone || len(data) == 0 || !ok {
		return data
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(data, &object); err != nil {
		return nil
	}
	delete(object, field)
	if raw, ok := object["startHook"]; ok && startHookMetadata && eventType == EventTypeRunCreated {
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
// metadata, such as a step's name or a hook's token, is kept. A run_created
// also loses its start hook's metadata.
func ScrubEventData(eventType EventType, data []byte) []byte {
	return stripEventData(eventType, data, EventsResolveDataNone, true)
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
	Input []byte `json:"input" cbor:"input"`
	// Attributes is left out when the run has none, and DeploymentID is
	// always recorded.
	Attributes   map[string]string `json:"attributes,omitempty" cbor:"attributes,omitempty"`
	DeploymentID string            `json:"deploymentId" cbor:"deploymentId"`
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
// and its token is free.
const StartHookCandidateMaxAge = 24 * time.Hour

// StartHookDecision is a World's permanent decision on a start hook
// candidate, by its run ID: accepted, or rejected because ConflictingRunID
// held the token. Every later attempt for the candidate, by run_created or by
// a first delivery's run_started, gets the same decision.
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
	return &HookConflictError{Token: d.Token, RunID: d.ConflictingRunID, Start: true}
}

// StartHookRejectionRefusal returns the error EventsCreateRun refuses input,
// a start hook candidate, with once the World recorded rejection for its run
// ID: the rejection's *HookConflictError, or ErrStartHookRejected when input
// carries another token.
func StartHookRejectionRefusal(input EventsCreateRunInput, rejection StartHookDecision) error {
	if input.StartHook != nil && rejection.Token != input.StartHook.Token {
		return fmt.Errorf("%w: run %s was already decided for a different start hook token", ErrStartHookRejected, input.RunID)
	}
	return rejection.Err()
}

// ExistingRunRefusal returns the error EventsCreateRun refuses input with
// when its run exists, with the start hook its run_created recorded,
// runCreated, or nil when input repeats its accepted start
// (RepeatsAcceptedStart). A start hook candidate is refused with
// ErrStartHookRejected, and another creation with ErrRunAlreadyExists.
func ExistingRunRefusal(runCreated []byte, input EventsCreateRunInput) error {
	switch {
	case RepeatsAcceptedStart(runCreated, input):
		return nil
	case input.StartHook != nil:
		return fmt.Errorf("%w: run %s already exists without this start hook", ErrStartHookRejected, input.RunID)
	}
	return ErrRunAlreadyExists
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

// ExecutionContextWorkflowCoreVersion and ExecutionContextFeatures are the
// executionContext keys every started run records: the version of the
// go-workflow release that started it, and its features,
// {"encryption": bool}, which say whether its payloads are encrypted.
const (
	// ExecutionContextTraceCarrier is the executionContext key holding the
	// trace context a run was started in, as a propagation carrier.
	ExecutionContextTraceCarrier        = "traceCarrier"
	ExecutionContextWorkflowCoreVersion = "workflowCoreVersion"
	ExecutionContextFeatures            = "features"
	// ExecutionContextNodeVersion is the executionContext key a Workflow
	// SDK run records the Node.js version of its creating deployment under.
	ExecutionContextNodeVersion = "nodeVersion"
)

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
	case errors.Is(err, ErrMaxEventsExceeded):
		return "MAX_EVENTS_EXCEEDED"
	case errors.Is(err, ErrReplayTimeout):
		return "REPLAY_TIMEOUT"
	case errors.Is(err, ErrDeploymentMismatch):
		return "DEPLOYMENT_MISMATCH"
	case errors.Is(err, ErrWorkflowNotRegistered):
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
	// It is always written, false included.
	Inline bool `json:"inline" cbor:"inline"`
	// CreatorMessageID is the queued message of the flow that created the
	// step. A step the runner declared and never waited for, recorded as
	// its run ends, has none: no message runs it.
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
	// The fields below are latency telemetry, in milliseconds, which the
	// flow records on the step_completed or step_failed of the first
	// attempt of the first step of a batch it runs inline.
	//
	// TTFS is the time from the run's creation to the start of the first
	// step's function, when nothing but the runtime stood between them.
	TTFS *float64 `json:"ttfs,omitempty" cbor:"ttfs,omitempty"`
	// STSO is the time from the previous step's result to the start of
	// this step's function, when the two ran back to back. StepCount is
	// the number of finished steps and EventCount the number of events in
	// the log when the step was scheduled, and are set with STSO.
	STSO       *float64 `json:"stso,omitempty" cbor:"stso,omitempty"`
	StepCount  int      `json:"stepCount,omitempty" cbor:"stepCount,omitempty"`
	EventCount int      `json:"eventCount,omitempty" cbor:"eventCount,omitempty"`
	// RSFS is the time from the delivery's run_started to the write of
	// the first step's start, set with TTFS.
	RSFS *float64 `json:"rsfs,omitempty" cbor:"rsfs,omitempty"`
	// FinalSchedulingReplay is how long the runner ran before it
	// scheduled the step.
	FinalSchedulingReplay *float64 `json:"finalSchedulingReplay,omitempty" cbor:"finalSchedulingReplay,omitempty"`
	// Optimizations names the ways the runtime shortened the step's
	// start, such as "turbo".
	Optimizations []string `json:"optimizations,omitempty" cbor:"optimizations,omitempty"`
}

// DataRunCancelled is the Data of a run_cancelled event. A cancellation
// without a reason has no Data.
type DataRunCancelled struct {
	// CancelReason is why the run was cancelled, in plain text of at most
	// MaxCancelReasonLength characters, so tools can show it without
	// opening a payload.
	CancelReason string `json:"cancelReason,omitempty" cbor:"cancelReason,omitempty"`
}

// MaxCancelReasonLength is the most characters a run_cancelled's
// CancelReason may hold.
const MaxCancelReasonLength = 512

// EncodeRunCancelled returns the Data of a run_cancelled event with reason:
// nil without one. It returns ErrEventInvalid for a reason longer than
// MaxCancelReasonLength.
func EncodeRunCancelled(reason string) ([]byte, error) {
	if reason == "" {
		return nil, nil
	}
	if units := len(utf16.Encode([]rune(reason))); units > MaxCancelReasonLength {
		return nil, fmt.Errorf("%w: cancel reason is %d characters, over the limit of %d", ErrEventInvalid, units, MaxCancelReasonLength)
	}
	return json.Marshal(DataRunCancelled{CancelReason: reason})
}

// RunCancelReason returns the CancelReason of a run_cancelled event's Data,
// "" when it has none.
func RunCancelReason(data []byte) string {
	var cancelled DataRunCancelled
	if len(data) == 0 || json.Unmarshal(data, &cancelled) != nil {
		return ""
	}
	return cancelled.CancelReason
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
// IsWebhook is always recorded on a hook_created, and Data without it reads
// as a webhook. A hook_conflict, which has a ConflictingRunID, records only
// that and the token.
type DataHookCreated struct {
	Token     string `json:"token" cbor:"token"`
	Metadata  []byte `json:"metadata,omitempty" cbor:"metadata,omitempty"`
	IsWebhook bool   `json:"isWebhook" cbor:"isWebhook"`
	IsSystem  bool   `json:"isSystem,omitempty" cbor:"isSystem,omitempty"`
	// TokenRetentionUntil reserves the token until this time, including
	// after the run ends.
	TokenRetentionUntil *time.Time      `json:"tokenRetentionUntil,omitempty" cbor:"tokenRetentionUntil,omitempty"`
	Force               bool            `json:"force,omitempty" cbor:"force,omitempty"`
	ForceClaimedFrom    *HookForceClaim `json:"forceClaimedFrom,omitempty" cbor:"forceClaimedFrom,omitempty"`
	ConflictingRunID    RunID           `json:"conflictingRunId,omitempty" cbor:"conflictingRunId,omitempty"`
}

// MarshalJSON records a conflict as its token and the run holding it.
func (d DataHookCreated) MarshalJSON() ([]byte, error) {
	if d.ConflictingRunID != "" {
		return json.Marshal(struct {
			Token            string `json:"token"`
			ConflictingRunID RunID  `json:"conflictingRunId"`
		}{d.Token, d.ConflictingRunID})
	}
	type plain DataHookCreated
	return json.Marshal(plain(d))
}

// UnmarshalJSON reads Data without isWebhook as a webhook's.
func (d *DataHookCreated) UnmarshalJSON(data []byte) error {
	type plain DataHookCreated
	value := plain{IsWebhook: true}
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	*d = DataHookCreated(value)
	return nil
}

// DataHookReceived is the Data of a hook_received event.
type DataHookReceived struct {
	// Payload is the value sent to the hook as a payload. A webhook
	// receives a WebhookRequest.
	Payload []byte `json:"payload,omitempty" cbor:"payload,omitempty"`
	// WorkflowName is the workflow of the hook's run.
	WorkflowName string `json:"workflowName,omitempty" cbor:"workflowName,omitempty"`
	// Token is the token the payload was sent to, which a resume records.
	Token string `json:"token,omitempty" cbor:"token,omitempty"`
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

// HookDisposedEventData is the Data of a hook_disposed event. Token is the
// token the hook released. ForceClaimedBy is set when another hook took the
// token with a forced claim.
type HookDisposedEventData struct {
	Token          string          `json:"token,omitempty" cbor:"token,omitempty"`
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

// CheckAttributes returns an error matching ErrAttributeInvalid unless
// attributes are within the limits: at most MaxAttributesPerRun, with
// non-empty keys of at most MaxAttributeKeyLength and values of at most
// MaxAttributeValueBytes. Keys starting with ReservedAttributePrefix are
// refused unless allowReserved. Errors carry the SDK's messages, checked in
// key order.
func CheckAttributes(attributes map[string]string, allowReserved bool) error {
	keys := slices.Sorted(maps.Keys(attributes))
	changes := make([]RunsAttributeChange, len(keys))
	for index, key := range keys {
		value := attributes[key]
		changes[index] = RunsAttributeChange{Key: key, Value: &value}
	}
	return CheckAttributeChanges(nil, changes, allowReserved)
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
// finished run, the run's status, before checking its step or hook, or nil
// when the event is checked further.
//
//   - A run_cancelled of a cancelled run is recorded: nil.
//   - A run_started is refused with a *RunExpiredError.
//   - A terminal transition, a step, wait, or hook creation, an inline
//     step's creating step_started, and an attr_set are refused with
//     ErrEntityConflict. The error also matches *RunExpiredError, which
//     says why.
//   - A wait_completed is refused with ErrEventNotFound, since a run's waits
//     end with it.
//   - Another step event is checked as CheckStepEvent says, and a
//     hook_received and a hook_disposed against their hook first: nil.
func FinishedRunRefusal(status Status, event EventsCreateEvent) error {
	switch event.EventType {
	case EventTypeRunCancelled:
		if status == StatusCancelled {
			return nil
		}
		return fmt.Errorf("%w: cannot transition a run: %w", ErrEntityConflict, &RunExpiredError{RunStatus: status})
	case EventTypeRunCompleted, EventTypeRunFailed:
		return fmt.Errorf("%w: cannot transition a run: %w", ErrEntityConflict, &RunExpiredError{RunStatus: status})
	case EventTypeStepCreated, EventTypeWaitCreated, EventTypeHookCreated, EventTypeAttrSet:
		return fmt.Errorf("%w: cannot record %s: %w", ErrEntityConflict, event.EventType, &RunExpiredError{RunStatus: status})
	case EventTypeStepStarted:
		if StartCreatesStep(event.Data) {
			return fmt.Errorf("%w: cannot create a step: %w", ErrEntityConflict, &RunExpiredError{RunStatus: status})
		}
		return nil
	case EventTypeStepCompleted, EventTypeStepFailed, EventTypeStepRetrying, EventTypeHookReceived, EventTypeHookDisposed:
		return nil
	case EventTypeWaitCompleted:
		return fmt.Errorf("%w: wait %s ended with its run", ErrEventNotFound, event.CorrelationID)
	}
	return &RunExpiredError{RunStatus: status}
}

// FinishedRunHookRefusal returns a *RunExpiredError for a hook_received on
// a run in status that has finished, once its hook was found, or nil.
func FinishedRunHookRefusal(status Status, event EventsCreateEvent) error {
	if event.EventType == EventTypeHookReceived && status.Terminal() {
		return &RunExpiredError{RunStatus: status}
	}
	return nil
}

// CheckResumeReplay returns the error a World refuses a hook_received
// repeating the resume ID of an earlier one with, or nil when the World
// answers with the earlier event. hookID and digest are the earlier resume's
// hook and payload digest. A resume ID names one resume of one hook, and its
// payload, when both resumes carry a digest: another is refused with
// ErrEntityConflict.
func CheckResumeReplay(event EventsCreateEvent, resumeDigest string, hookID HookID, digest string) error {
	if string(hookID) != event.CorrelationID {
		return fmt.Errorf("%w: resume ID already recorded for hook %s", ErrEntityConflict, hookID)
	}
	if resumeDigest != "" && digest != "" && resumeDigest != digest {
		return fmt.Errorf("%w: resume ID already recorded with another payload", ErrEntityConflict)
	}
	return nil
}

// Run ID time limits of a run's creation, which CheckRunCreation checks.
const (
	// RunIDMaxAge is how far in the past a new run's ID may have been minted.
	RunIDMaxAge = 24 * time.Hour
	// RunIDMaxLead is how far in the future a new run's ID may be minted.
	RunIDMaxLead = 5 * time.Minute
)

// CheckRunCreation returns the error a World refuses the creation input
// describes with, or nil. The run ID must be wrun_ and a ULID minted within
// RunIDMaxAge before now and RunIDMaxLead after it, unless the run is
// created FromRunStarted: otherwise ErrEventInvalid, or ErrStartHookRejected
// for a run with a start hook. Its attributes must be within the limits of
// CheckAttributes (ErrAttributeInvalid).
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

// CheckAttributeChanges returns an error matching ErrAttributeInvalid
// unless an attr_set's changes may apply to a run's attributes: each key and
// value within the limits of CheckAttributes, each key changed once, and at
// most MaxAttributesPerRun attributes after the changes.
func CheckAttributeChanges(attributes map[string]string, changes []RunsAttributeChange, allowReserved bool) error {
	for _, change := range changes {
		if err := checkAttributeKey(change.Key, allowReserved); err != nil {
			return err
		}
		if change.Value != nil && len(*change.Value) > MaxAttributeValueBytes {
			return attributeError(fmt.Sprintf("Attribute value byte length %d exceeds limit %d", len(*change.Value), MaxAttributeValueBytes))
		}
	}
	seen := make(map[string]bool, len(changes))
	count := len(attributes)
	for _, change := range changes {
		if seen[change.Key] {
			return attributeError(fmt.Sprintf("Attribute key %s appears more than once in the same batch", jsonString(change.Key)))
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
		return attributeError(fmt.Sprintf("Run attribute count would exceed limit %d (post-merge %d)", MaxAttributesPerRun, count))
	}
	return nil
}

// MaxAttrSetDataBytes is the most JSON bytes an attr_set's Data may take.
const MaxAttrSetDataBytes = 8192

// AttrSetDataSize returns the size MaxAttrSetDataBytes limits: the bytes of
// an attr_set's Data as JSON that writes <, > and & as themselves, however
// data escapes them.
func AttrSetDataSize(data []byte) int {
	var value any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if decoder.Decode(&value) != nil {
		return len(data)
	}
	var plain bytes.Buffer
	encoder := json.NewEncoder(&plain)
	encoder.SetEscapeHTML(false)
	if encoder.Encode(value) != nil {
		return len(data)
	}
	return plain.Len() - 1 // Encode ends with a newline.
}

// CheckAttrSet returns ErrAttributeInvalid unless the attr_set Data is within
// MaxAttrSetDataBytes and may apply to a run's attributes
// (CheckAttributeChanges).
func CheckAttrSet(attributes map[string]string, data []byte) error {
	if size := AttrSetDataSize(data); size > MaxAttrSetDataBytes {
		return attributeError(fmt.Sprintf("Attribute eventData exceeds limit %d UTF-8 JSON bytes (received %d bytes). Split the changes into smaller setAttributes() calls.", MaxAttrSetDataBytes, size))
	}
	var set DataAttrSet
	if err := json.Unmarshal(data, &set); err != nil {
		return fmt.Errorf("%w: attr_set data: %v", ErrEventInvalid, err)
	}
	return CheckAttributeChanges(attributes, set.Changes, set.AllowReservedAttributes)
}

// CheckStepStart returns a *TooEarlyError for a step_started of a step whose
// retry is due at retryAfter, while now is before it, or nil.
func CheckStepStart(retryAfter *time.Time, now time.Time) error {
	if retryAfter == nil || !now.Before(*retryAfter) {
		return nil
	}
	return &TooEarlyError{RetryAfter: retryAfter.Sub(now)}
}

// CheckResumeDigest returns ErrEventInvalid unless a hook_received's
// ResumePayloadDigest is the digest of its payload. An empty digest is not
// checked.
func CheckResumeDigest(event EventsCreateEvent, digest string) error {
	if digest == "" || event.EventType != EventTypeHookReceived {
		return nil
	}
	if ResumeDigest(DecodeHookReceived(event.Data)) != digest {
		return fmt.Errorf("%w: resume digest does not match its payload", ErrEventInvalid)
	}
	return nil
}

// CheckStepEvent returns the error a World refuses a step event with, or
// nil. exists, status, and retryAfter describe the event's step, and
// runStatus its run.
//
//   - A step_created of a step that exists, and an inline step's creating
//     step_started (StartCreatesStep) of one, are refused with
//     ErrEntityConflict.
//   - Any other step event needs its step (ErrStepNotFound), which must not
//     have finished (ErrEntityConflict).
//   - On a finished run, a step_started, and a step_failed of a step that is
//     not running, are refused with a *RunExpiredError. A step_completed
//     and a step_retrying are not checked against the run.
//   - A step_started before the step's retry is due is refused with a
//     *TooEarlyError (CheckStepStart).
func CheckStepEvent(event EventsCreateEvent, exists bool, status Status, retryAfter *time.Time, runStatus Status, now time.Time) error {
	switch event.EventType {
	case EventTypeStepCreated:
		if exists {
			return fmt.Errorf("%w: step %s already created", ErrEntityConflict, event.CorrelationID)
		}
		return nil
	case EventTypeStepStarted, EventTypeStepCompleted, EventTypeStepFailed, EventTypeStepRetrying:
	default:
		return nil
	}
	creates := event.EventType == EventTypeStepStarted && StartCreatesStep(event.Data)
	switch {
	case creates && exists:
		return fmt.Errorf("%w: step %s already created", ErrEntityConflict, event.CorrelationID)
	case creates:
		return nil
	case !exists:
		return fmt.Errorf("%w: step %s", ErrStepNotFound, event.CorrelationID)
	case status.Terminal():
		return fmt.Errorf("%w: step %s is %s", ErrEntityConflict, event.CorrelationID, status)
	case runStatus.Terminal() && (event.EventType == EventTypeStepStarted || event.EventType == EventTypeStepFailed && status != StatusRunning):
		return &RunExpiredError{RunStatus: runStatus}
	case event.EventType == EventTypeStepStarted:
		return CheckStepStart(retryAfter, now)
	}
	return nil
}

// CheckRunSpecVersion returns a *RunNotSupportedError for an event on a
// run whose spec version, version, is newer than SpecVersion, or nil.
func CheckRunSpecVersion(runID RunID, version int) error {
	if version > SpecVersion {
		return &RunNotSupportedError{RunSpecVersion: version, WorldSpecVersion: SpecVersion}
	}
	return nil
}

// attributeError is a run attribute outside the limits, with the SDK's
// message. It matches ErrAttributeInvalid.
type attributeError string

func (e attributeError) Error() string { return string(e) }

func (e attributeError) Is(target error) bool { return target == ErrAttributeInvalid }

// checkAttributeKey returns an attributeError unless key is a valid
// attribute key.
func checkAttributeKey(key string, allowReserved bool) error {
	units := utf16.Encode([]rune(key))
	switch {
	case key == "":
		return attributeError("Attribute key must not be empty")
	case len(units) > MaxAttributeKeyLength:
		prefix := string(utf16.Decode(units[:32]))
		return attributeError(fmt.Sprintf("Attribute key length %d exceeds limit %d: %s\u2026", len(units), MaxAttributeKeyLength, jsonString(prefix)))
	case !allowReserved && strings.HasPrefix(key, ReservedAttributePrefix):
		return attributeError(fmt.Sprintf("Attribute key %s starts with reserved prefix %q \u2014 that namespace is reserved for framework/library code. Set { allowReservedAttributes: true } only if your caller is framework-level.", jsonString(key), ReservedAttributePrefix))
	}
	return nil
}

// jsonString returns s as a JSON string that writes <, > and & as
// themselves, as JavaScript's JSON.stringify does.
func jsonString(s string) string {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(s)
	return strings.TrimSuffix(buffer.String(), "\n")
}
