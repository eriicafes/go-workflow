package worlds

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Storage records each run as an append-only event log and serves the runs,
// steps, and hooks derived from it.
type Storage interface {
	StorageRuns
	StorageSteps
	StorageEvents
	StorageHooks
}

// StorageRuns reads runs and changes their attributes.
type StorageRuns interface {
	// RunsGet returns the run identified by RunID. It returns ErrRunNotFound when
	// the run does not exist. It applies ResolveData to the returned run.
	RunsGet(context.Context, RunsGetInput) (Run, error)
	// RunsList returns runs that match the input filters. It applies Pagination
	// and ResolveData to the result.
	RunsList(context.Context, RunsListInput) (Page[Run], error)
	// RunsWaitForTerminalStatus blocks until the run reaches a terminal status,
	// Timeout elapses, or ctx is done. It returns the current run when Timeout
	// elapses first, and ctx.Err() when ctx is done first.
	RunsWaitForTerminalStatus(context.Context, RunsWaitForTerminalStatusInput) (Run, error)
	// RunsSetAttributes applies Changes to the run as one attr_set event. A World
	// whose Capabilities report SetAttributes as false ignores the call and
	// returns nil.
	RunsSetAttributes(context.Context, RunsSetAttributesInput) error
	// A World deletes a run's data as the run finishes when the run's
	// RetentionAttribute is RetainNone's, as RunsDeleteDataMany does.
	//
	// RunsDeleteDataMany deletes the data of finished runs: their payloads,
	// the payloads in their events, their hooks' metadata, and their streams'
	// chunks, leaving each stream empty and closed. The run, step, event, and
	// hook records stay, and the run's ExpiredAt is set. It returns one
	// result per RunID, in order. A World whose Capabilities report
	// DeleteData as false returns an error wrapping errors.ErrUnsupported.
	RunsDeleteDataMany(context.Context, RunsDeleteDataManyInput) (RunsDeleteDataManyResult, error)
}

// StorageSteps reads the steps of a run.
type StorageSteps interface {
	// StepsGet returns the step identified by RunID and StepID. It returns
	// ErrStepNotFound when the step does not exist. It applies ResolveData to the
	// returned step.
	StepsGet(context.Context, StepsGetInput) (Step, error)
	// StepsList returns the steps in a run. It applies Pagination and ResolveData
	// to the result.
	StepsList(context.Context, StepsListInput) (Page[Step], error)
}

// StorageEvents appends to and reads a run's event log. Appending an event
// updates the run, step, and hook views derived from it in the same operation.
type StorageEvents interface {
	// EventsCreateRun creates the identified run and its initial run_created
	// event, recording SpecVersion on the run. It does not deduplicate calls.
	// It returns ErrRunAlreadyExists when the run already exists. Its result
	// has AllocatedEventCount and InBandCount set to one: run_created takes
	// the first position and counts as in-band, without being fenced.
	EventsCreateRun(context.Context, EventsCreateRunInput) (EventResult, error)
	// EventsCreate appends a lifecycle event to a run. Events in the result are
	// selected by EventsCreateOptions. For a step_created event, Step holds the
	// step and StepCreated reports whether this call created it.
	//
	// It returns ErrEventInvalid for a malformed event, ErrRunTerminal when
	// the run is terminal and the event needs a running run, ErrHookNotActive
	// when the target hook does not allow the event, and ErrHookConflict when
	// a hook_created token is taken.
	//
	// A finished run refuses step_created, step_started, step_retrying, and
	// wait_created, and still accepts step_completed, step_failed, and
	// wait_completed, which record what was running when it finished. Other
	// checks of a step or wait against the events before it, which a World
	// returns as ErrStepNotStartable, are optional: the runtime decides from
	// the log alone, and a World that appends every such event is correct. A
	// World that checks accepts a step's result while the step is not
	// finished, and a step_started that creates the step it starts: an
	// inline step's first step_started, whose DataStepStarted has Inline,
	// StepName, and Input, and which no step_created precedes.
	//
	// An event that repeats an admitted transition, such as a second
	// run_started, step_created, or hook_disposed, returns the earlier event
	// with AllocatedEventCount zero instead of appending. A step_started for a
	// step already started is not a repeat. It appends a new attempt.
	EventsCreate(context.Context, EventsCreateInput) (EventResult, error)
	// EventsCreateBatch appends Events to a run in order, as one in-band
	// write when InBandCount is set, fenced as one block. It appends none of
	// them, and returns an error, when any event is invalid, the run does not
	// exist, or the fence refuses the block. Otherwise each event has a
	// result in EventBatchResult.Results, refused or appended as EventsCreate
	// would refuse or append it alone, and a repeated transition is refused
	// with ErrEventInvalid. A batch is not all-or-nothing, but it is ordered
	// per correlation ID: once an event is refused, every later event of the
	// batch with the same correlation ID is refused too, so a step's outcome
	// never lands behind its refused creation.
	EventsCreateBatch(context.Context, EventsCreateBatchInput) (EventBatchResult, error)
	// EventsGet returns the event identified by RunID and EventID. It returns
	// ErrEventNotFound when the event does not exist. It applies ResolveData to
	// the returned event.
	EventsGet(context.Context, EventsGetInput) (Event, error)
	// EventsList returns the events in a run. It applies Pagination and
	// ResolveData to the result. Every page carries the run's Snapshot, read
	// before the page, so a listing from the first page to the last covers
	// every position the first page's Snapshot counts.
	EventsList(context.Context, EventsListInput) (EventsPage, error)
	// EventsListByCorrelationID returns events in a run that match CorrelationID.
	// It applies Pagination and ResolveData to the result.
	EventsListByCorrelationID(context.Context, EventsListByCorrelationIDInput) (Page[Event], error)
}

// StorageHooks reads hooks and claims hook tokens.
type StorageHooks interface {
	// HooksGet returns the hook identified by HookID. It returns ErrHookNotFound
	// when the hook does not exist. It applies ResolveData to the returned hook.
	HooksGet(context.Context, HooksGetInput) (Hook, error)
	// HooksGetByToken returns the hook that owns Token. It returns
	// ErrHookNotFound when no hook owns the token, including after its
	// retention has ended. It applies ResolveData to the returned hook.
	HooksGetByToken(context.Context, HooksGetByTokenInput) (Hook, error)
	// HooksList returns hooks that match the input filters. It applies Pagination
	// and ResolveData to the result.
	HooksList(context.Context, HooksListInput) (Page[Hook], error)
}

type (
	// RunsGetInput holds the arguments to RunsGet.
	RunsGetInput struct {
		RunID       RunID
		ResolveData ResolveData
	}
	// RunsListInput holds the arguments to RunsList.
	RunsListInput struct {
		// WorkflowName selects runs of one workflow. Empty selects every
		// workflow.
		WorkflowName string
		// Status selects runs in any of the listed statuses. Empty selects
		// every status.
		Status      []Status
		Pagination  Pagination
		ResolveData ResolveData
	}
	// RunsWaitForTerminalStatusInput holds the arguments to
	// RunsWaitForTerminalStatus.
	RunsWaitForTerminalStatusInput struct {
		RunID RunID
		// Timeout is the longest the call waits. Zero returns the current run
		// without waiting.
		Timeout     time.Duration
		ResolveData ResolveData
	}
	// RunsAttributeChange sets or deletes one run attribute.
	RunsAttributeChange struct {
		Key string `json:"key"`
		// Value is the new value. Nil deletes the attribute.
		Value *string `json:"value"`
	}
	// RunsDeleteDataManyInput holds the arguments to RunsDeleteDataMany:
	// one to MaxDeleteDataRunIDs unique run IDs.
	RunsDeleteDataManyInput struct {
		RunIDs []RunID
	}
	// RunsDeleteDataManyResult is the result of RunsDeleteDataMany: one
	// result per requested run, in order, and a count of each outcome.
	RunsDeleteDataManyResult struct {
		Summary DeleteDataSummary
		Results []DeleteDataResult
	}
	// DeleteDataSummary counts the outcomes of RunsDeleteDataMany.
	DeleteDataSummary struct {
		Requested      int
		Deleted        int
		AlreadyDeleted int
		Skipped        int
		NotCreated     int
		NotFound       int
		Failed         int
	}
	// DeleteDataResult is the outcome of deleting one run's data. Reason and
	// Status are set for DeleteDataSkipped, and Code and Retryable for
	// DeleteDataFailed.
	DeleteDataResult struct {
		RunID     RunID
		Outcome   DeleteDataOutcome
		Reason    string `cbor:",omitempty"`
		Status    Status `cbor:",omitempty"`
		Code      string `cbor:",omitempty"`
		Retryable bool   `cbor:",omitempty"`
	}
	// RunsSetAttributesInput holds the arguments to RunsSetAttributes.
	RunsSetAttributesInput struct {
		RunID   RunID
		Changes []RunsAttributeChange
		// Writer names what set the attributes. Nil records a workflow
		// writer.
		Writer *AttributeWriter
		// AllowReservedAttributes records that the writer permitted keys
		// with the reserved prefix.
		AllowReservedAttributes bool
	}
)

// Attributes the runtime sets on a child run.
const (
	// ParentRunIDAttribute holds the ID of the run that started a child run.
	ParentRunIDAttribute = "$parentRunId"
	// RootRunIDAttribute holds the ID of the run at the root of a child run's
	// lineage.
	RootRunIDAttribute = "$rootRunId"
	// RetentionAttribute holds a run's data retention. See Retention.
	RetentionAttribute = "$retention"
)

// Run is the view of one run derived from its event log.
type Run struct {
	RunID        RunID
	WorkflowName string
	Status       Status
	// Input is the run's argument list, [input], as a payload.
	Input []byte
	// Output is the output payload of a completed run.
	Output []byte
	// Error is the error payload of a failed run. DecodeErrorPayload
	// decodes it.
	Error      []byte
	Attributes map[string]string
	// DeploymentID is the deployment the run is pinned to, recorded when the
	// run is created. Empty means the run is not pinned.
	DeploymentID string
	// EncryptionPublicKey is the run's public key in base64, which writers
	// outside the run seal payloads to. It is empty when the run's payloads
	// are not encrypted.
	EncryptionPublicKey string
	// InBandCount counts the positions allocated to in-band writes, those
	// the run's orchestrator fences with EventsCreateOptions.InBandCount,
	// run_created's included. A flow fences its writes with the count an
	// EventsList snapshot reports.
	InBandCount int
	// SpecVersion is the spec version the run was created under,
	// SpecVersion.
	SpecVersion int
	CreatedAt   time.Time
	UpdatedAt   time.Time
	StartedAt   *time.Time
	CompletedAt *time.Time
	// ExecutionContext is plain-text metadata about how the run was started.
	// See EventsCreateRunInput.
	ExecutionContext map[string]any
	// ExpiredAt is when the run's data was deleted, or is deleted for a
	// World's retention. Once it has passed, the run's payloads are gone and
	// Run.Value returns ErrRunExpired. See RunDataExpired.
	ExpiredAt *time.Time
}

type (
	// StepsGetInput holds the arguments to StepsGet.
	StepsGetInput struct {
		RunID       RunID
		StepID      StepID
		ResolveData ResolveData
	}

	// StepsListInput holds the arguments to StepsList.
	StepsListInput struct {
		RunID       RunID
		Pagination  Pagination
		ResolveData ResolveData
	}
)

// Step is the view of one step derived from its run's event log.
type Step struct {
	RunID    RunID
	StepID   StepID
	StepName string
	// Status is pending before the first attempt starts and again while a
	// retry is scheduled.
	Status Status
	// Input is the step's arguments, {args: [input]}, as a payload.
	Input []byte
	// Output is the output payload of a completed step.
	Output []byte
	// Error is the JSON-encoded Error of the last failed attempt.
	// DecodeErrorPayload decodes it.
	Error []byte
	// Attempt is the number of attempts started so far.
	Attempt     int
	CreatedAt   time.Time
	UpdatedAt   time.Time
	StartedAt   *time.Time
	CompletedAt *time.Time
	// RetryAfter is the earliest time of the next attempt while a retry is
	// scheduled.
	RetryAfter *time.Time
}

type (
	// HooksGetInput holds the arguments to HooksGet.
	HooksGetInput struct {
		HookID      HookID
		ResolveData ResolveData
	}
	// HooksGetByTokenInput holds the arguments to HooksGetByToken.
	HooksGetByTokenInput struct {
		Token       string
		ResolveData ResolveData
	}
	// HooksListInput holds the arguments to HooksList.
	HooksListInput struct {
		Pagination Pagination
		// RunID selects the hooks of one run. Empty selects every run.
		RunID       RunID
		ResolveData ResolveData
	}
	// HookForceClaim names the hook that took a token with a forced claim.
	HookForceClaim struct {
		RunID  RunID  `json:"runId"`
		HookID HookID `json:"hookId"`
	}
)

// Hook is the view of one hook derived from its run's event log.
// HookResumeContext is the part of a hook's run that addresses the
// deliveries its payloads queue: the run's deployment and workflow.
type HookResumeContext struct {
	DeploymentID string
	WorkflowName string
	// EncryptionPublicKey is the run's public key in base64, which a
	// payload sent to the hook is sealed to. It is empty when the run's
	// payloads are not encrypted.
	EncryptionPublicKey string
}

type Hook struct {
	RunID RunID
	// ResumeContext is copied from the hook's run when the hook is created, so
	// a delivery queued for its payloads is addressed without reading the run.
	ResumeContext HookResumeContext
	HookID        HookID
	Token         string
	// Metadata is the hook's metadata payload, recorded when it was created.
	// A webhook's holds how it responds, as WebhookMetadata.
	Metadata []byte
	// IsWebhook reports that the hook is a webhook, which HTTP requests to
	// its URL resume.
	IsWebhook bool
	// TokenRetentionUntil is the earliest time terminal-run cleanup may release
	// the token, or nil when there is no retention reservation. Active runs
	// keep their tokens beyond it; explicit disposal always releases them
	// immediately.
	TokenRetentionUntil *time.Time
	ClaimedFrom         *HookForceClaim
	ForceClaimedBy      *HookForceClaim
	CreatedAt           time.Time
	UpdatedAt           time.Time
	ReceivedAt          *time.Time
	DisposedAt          *time.Time
}

// EventType names the kind of an event and determines the shape of its Data.
type EventType string

const (
	// EventTypeRunCreated records a new run with its workflow name, input,
	// and attributes. It is always the first event of a run.
	EventTypeRunCreated EventType = "run_created"
	// EventTypeRunStarted records that a flow started replaying the run's
	// runner.
	EventTypeRunStarted EventType = "run_started"
	// EventTypeRunCompleted records the run's output.
	EventTypeRunCompleted EventType = "run_completed"
	// EventTypeRunFailed records the run's error.
	EventTypeRunFailed EventType = "run_failed"
	// EventTypeRunCancelled records that the run was cancelled.
	EventTypeRunCancelled EventType = "run_cancelled"
	// EventTypeAttrSet records changes to the run's attributes.
	EventTypeAttrSet EventType = "attr_set"
	// EventTypeStepCreated records a step with its name and input.
	EventTypeStepCreated EventType = "step_created"
	// EventTypeStepStarted records that a delivery started an attempt of the
	// step. A pending or running step can be started, and each start is a new
	// attempt.
	EventTypeStepStarted EventType = "step_started"
	// EventTypeStepCompleted records the step's output.
	EventTypeStepCompleted EventType = "step_completed"
	// EventTypeStepFailed records the step's final error.
	EventTypeStepFailed EventType = "step_failed"
	// EventTypeStepRetrying records a failed attempt and when the step is
	// retried.
	EventTypeStepRetrying EventType = "step_retrying"
	// EventTypeHookCreated records a hook and the token it claims.
	EventTypeHookCreated EventType = "hook_created"
	// EventTypeHookReceived records a hook's payload, or a webhook's request
	// or response.
	EventTypeHookReceived EventType = "hook_received"
	// EventTypeHookDisposed records that a hook released its token, or lost
	// it to a forced claim when its HookDisposedEventData has ForceClaimedBy.
	EventTypeHookDisposed EventType = "hook_disposed"
	// EventTypeHookConflict records that a hook could not be created because
	// another run owns its token.
	EventTypeHookConflict EventType = "hook_conflict"
	// EventTypeWaitCreated records a timer.
	EventTypeWaitCreated EventType = "wait_created"
	// EventTypeWaitCompleted records that a wait completed.
	EventTypeWaitCompleted EventType = "wait_completed"
)

// Valid reports whether t is one of the defined event types.
func (t EventType) Valid() bool {
	switch t {
	case EventTypeRunCreated, EventTypeRunStarted, EventTypeRunCompleted, EventTypeRunFailed,
		EventTypeRunCancelled, EventTypeAttrSet, EventTypeStepCreated, EventTypeStepStarted,
		EventTypeStepCompleted, EventTypeStepFailed, EventTypeStepRetrying,
		EventTypeHookCreated, EventTypeHookReceived, EventTypeHookDisposed, EventTypeHookConflict,
		EventTypeWaitCreated, EventTypeWaitCompleted:
		return true
	default:
		return false
	}
}

type (
	// EventsCreateRunInput holds the arguments to EventsCreateRun.
	EventsCreateRunInput struct {
		RunID        RunID
		WorkflowName string
		// Input is the run's argument list, [input], as a payload.
		Input      []byte
		Attributes map[string]string
		// DeploymentID pins the run to a deployment. Empty leaves the run
		// unpinned. The World records it in run_created and on the Run.
		DeploymentID string
		// EncryptionPublicKey is the run's public key in base64, set when its
		// payloads are encrypted. The World records it in run_created, on the
		// Run, and in the ResumeContext of the run's hooks.
		EncryptionPublicKey string
		// ExecutionContext is plain-text metadata about how the run was
		// started, such as ExecutionContextReplayedFromRunID. The World
		// records it in run_created and on the Run.
		ExecutionContext map[string]any
		// StartHook, on a World with Capabilities().StartHook, creates the
		// run's start hook in the same write as the run, claiming its token.
		// When another run holds the token, no run is created and the World
		// returns a *HookConflictError naming that run.
		StartHook *StartHook
	}
	// EventsCreateInput holds the arguments to EventsCreate.
	EventsCreateInput struct {
		RunID   RunID
		Event   EventsCreateEvent
		Options EventsCreateOptions
	}
	// EventsCreateOptions selects the events returned in EventResult.Events,
	// letting a caller catch up on events appended since it last read the
	// log, and fences an in-band write.
	EventsCreateOptions struct {
		// EventCount is the number of events the caller already holds, the
		// position it decided the write from. When set, Events holds every
		// event after that count that was in the log before this call, plus
		// an induced same-run disposal on force claim. The primary returned
		// event is not included. It takes precedence over SinceCursor.
		//
		// For an in-band write, Events is the skipped-slot report: every
		// event between EventCount and the write's own position, in order.
		// When the World cannot return all of them, it returns what it can
		// and sets EventResult.MoreEvents.
		EventCount *int
		// SinceCursor is the ID of the last event the caller holds. When set
		// and EventCount is nil, Events holds the earlier events after it, on
		// the same terms. When both are unset, Events is empty.
		SinceCursor string
		ResolveData EventsResolveData
		// InBandCount makes the append an in-band write: one made by the run's
		// orchestrator, which fences it with the run's InBandCount as the
		// orchestrator last saw it. The World appends only when the run's
		// InBandCount still equals it, and advances the count by the positions
		// it allocates. Otherwise it returns ErrInBandSuperseded and appends
		// nothing, because another orchestrator has written since. Nil makes
		// the write out-of-band, which is never refused and leaves the count
		// unchanged.
		InBandCount *int
	}
	// EventsCreateBatchInput holds the arguments to EventsCreateBatch.
	EventsCreateBatchInput struct {
		RunID RunID
		// InBandCount fences the whole batch as one in-band write, as
		// EventsCreateOptions.InBandCount does.
		InBandCount *int
		// EventCount is the position the batch was decided from. For an
		// in-band batch, EventBatchResult.Events is the skipped-slot report
		// of the block, as for EventsCreateOptions.EventCount.
		EventCount *int
		Events     []EventsCreateEvent
	}
	// EventsGetInput holds the arguments to EventsGet.
	EventsGetInput struct {
		RunID       RunID
		EventID     EventID
		ResolveData EventsResolveData
	}
	// EventsCreateEvent is an event to append.
	EventsCreateEvent struct {
		EventType EventType
		// CorrelationID is the ID of the step, hook, or wait the event belongs
		// to. It is empty for events about the run itself.
		CorrelationID string
		// Data is the event payload, whose shape depends on EventType.
		Data []byte
		// OccurredAt is when the writer decided the event. A World stores an
		// in-band write's OccurredAt as the event's CreatedAt, so a replay
		// reads the time the writer used. A zero OccurredAt, or an
		// out-of-band write, takes the World's clock.
		OccurredAt time.Time
	}
	// EventsListInput holds the arguments to EventsList.
	EventsListInput struct {
		RunID       RunID
		Pagination  Pagination
		ResolveData EventsResolveData
	}
	// EventsListByCorrelationIDInput holds the arguments to
	// EventsListByCorrelationID.
	EventsListByCorrelationIDInput struct {
		RunID         RunID
		CorrelationID string
		Pagination    Pagination
		ResolveData   EventsResolveData
	}
	// EventResult is the result of appending one event.
	EventResult struct {
		// EventID and EventType identify the appended or previously admitted
		// event, and CreatedAt is when it was appended.
		EventID   EventID
		EventType EventType
		CreatedAt time.Time
		// Events holds the events selected by EventsCreateOptions.
		Events []Event
		// MoreEvents reports that an in-band write's skipped-slot report in
		// Events is incomplete. The write was appended, and the writer reads
		// the log before it uses anything the write resolves.
		MoreEvents bool
		// Step is the step created by a step_created event.
		Step *Step
		Hook *Hook
		Data []byte
		// StepCreated reports whether this call appended the step_created event.
		StepCreated bool
		// AllocatedEventCount is the number of positions this call
		// allocated: one for an appended event, zero for a repeat.
		AllocatedEventCount int
		// InBandCount is the run's InBandCount after the call.
		InBandCount int
	}
	// EventBatchResult is the result of EventsCreateBatch.
	EventBatchResult struct {
		// Results holds one result per event, in order.
		Results []EventBatchItem
		// Events is the skipped-slot report of an in-band batch with
		// EventCount, and MoreEvents reports that it is incomplete, as in
		// EventResult.
		Events     []Event
		MoreEvents bool
		// AllocatedEventCount is the number of events the batch appended.
		AllocatedEventCount int
		// InBandCount is the run's InBandCount after the batch.
		InBandCount int
	}
	// EventBatchItem is one event's outcome in an EventBatchResult: Err is
	// its refusal, or nil when it was appended.
	EventBatchItem struct {
		EventResult
		Err error
	}
	// EventsPage is one page of a run's events.
	EventsPage struct {
		Page[Event]
		// Snapshot is the run's position counters, read before the page.
		Snapshot EventsSnapshot
	}
	// EventsSnapshot is a run's position counters: EventCount, the number of
	// positions in its log, and InBandCount, the positions in-band writes
	// allocated, run_created's included.
	EventsSnapshot struct {
		EventCount  int
		InBandCount int
	}
)

// SpecVersion is the version of the Workflow SDK's specification every run
// follows. A World records it on each run it creates.
const SpecVersion = 9

// Event is one entry in a run's event log.
type Event struct {
	RunID         RunID
	EventID       EventID
	EventType     EventType
	CorrelationID string
	Data          []byte
	CreatedAt     time.Time
}

// EventIDForSlot returns the ID of the event at the given 1-based position in
// its run's event log. Event IDs sort in log order.
func EventIDForSlot(slot uint64) EventID {
	return EventID(fmt.Sprintf("evnt_%026d", slot))
}

// EventSlot returns the 1-based log position encoded in id. It returns an
// error wrapping ErrEventInvalid when id was not made by EventIDForSlot.
func EventSlot(id EventID) (uint64, error) {
	value := string(id)
	if !strings.HasPrefix(value, "evnt_") || len(value) != len("evnt_")+26 {
		return 0, fmt.Errorf("%w: %q", ErrEventInvalid, id)
	}
	slot, err := strconv.ParseUint(value[len("evnt_"):], 10, 64)
	if err != nil || slot == 0 {
		return 0, fmt.Errorf("%w: %q", ErrEventInvalid, id)
	}
	return slot, nil
}

// MaxDeleteDataRunIDs is the most run IDs one RunsDeleteDataMany call takes.
const MaxDeleteDataRunIDs = 100

// DeleteDataOutcome is the outcome of deleting one run's data.
type DeleteDataOutcome string

const (
	// DeleteDataDeleted reports that the call deleted the run's data.
	DeleteDataDeleted DeleteDataOutcome = "deleted"
	// DeleteDataAlreadyDeleted reports that the run's data was already
	// gone, and whatever an earlier deletion left was deleted.
	DeleteDataAlreadyDeleted DeleteDataOutcome = "already_deleted"
	// DeleteDataSkipped reports that the run has not finished, so nothing
	// was deleted. Its Reason is DeleteDataRunNotTerminal.
	DeleteDataSkipped DeleteDataOutcome = "skipped"
	// DeleteDataNotCreated reports a run cancelled before it was created.
	// go-workflow creates a run before queueing it, so its Worlds never
	// report it.
	DeleteDataNotCreated DeleteDataOutcome = "not_created"
	// DeleteDataNotFound reports that no run has the ID.
	DeleteDataNotFound DeleteDataOutcome = "not_found"
	// DeleteDataFailed reports that the deletion did not complete. Deleting
	// is idempotent, so retrying finishes it.
	DeleteDataFailed DeleteDataOutcome = "failed"
)

// DeleteDataRunNotTerminal is the Reason of a DeleteDataSkipped result.
const DeleteDataRunNotTerminal = "run_not_terminal"

// CheckDeleteDataInput returns ErrDeleteDataInvalid unless input names one
// to MaxDeleteDataRunIDs unique run IDs.
func CheckDeleteDataInput(input RunsDeleteDataManyInput) error {
	if len(input.RunIDs) == 0 || len(input.RunIDs) > MaxDeleteDataRunIDs {
		return fmt.Errorf("%w: %d run IDs, want 1 to %d", ErrDeleteDataInvalid, len(input.RunIDs), MaxDeleteDataRunIDs)
	}
	seen := make(map[RunID]bool, len(input.RunIDs))
	for _, id := range input.RunIDs {
		if seen[id] {
			return fmt.Errorf("%w: run ID %s repeated", ErrDeleteDataInvalid, id)
		}
		seen[id] = true
	}
	return nil
}

// NewDeleteDataResult returns the result of RunsDeleteDataMany for results,
// counting their outcomes.
func NewDeleteDataResult(results []DeleteDataResult) RunsDeleteDataManyResult {
	summary := DeleteDataSummary{Requested: len(results)}
	for _, result := range results {
		switch result.Outcome {
		case DeleteDataDeleted:
			summary.Deleted++
		case DeleteDataAlreadyDeleted:
			summary.AlreadyDeleted++
		case DeleteDataSkipped:
			summary.Skipped++
		case DeleteDataNotCreated:
			summary.NotCreated++
		case DeleteDataNotFound:
			summary.NotFound++
		case DeleteDataFailed:
			summary.Failed++
		}
	}
	return RunsDeleteDataManyResult{Summary: summary, Results: results}
}
