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
	// event. It does not deduplicate calls. It returns ErrRunAlreadyExists when
	// the run already exists. Its result has AllocatedEventCount set to one.
	EventsCreateRun(context.Context, EventsCreateRunInput) (EventResult, error)
	// EventsCreate appends a lifecycle event to a run. It enforces the
	// transition rules, so it returns ErrEventInvalid for a malformed event,
	// ErrRunTerminal when the run is terminal, ErrStepNotStartable or
	// ErrHookNotActive when the target step or hook does not allow the event,
	// and ErrHookConflict when a hook_created token is taken. An event that
	// repeats an admitted transition, such as a second run_started, returns the
	// earlier event with AllocatedEventCount zero instead of appending. A
	// step_started for a running step is not a repeat. It appends a new attempt,
	// so a delivery can take over a step whose previous delivery died. Events
	// in the result are selected by EventsCreateOptions. For a step_created
	// event, Step holds the step and StepCreated reports whether this call
	// created it.
	EventsCreate(context.Context, EventsCreateInput) (EventResult, error)
	// EventsCreateBatch appends Events to a run in order, atomically. It appends
	// none of them when any event is invalid or cannot be admitted.
	EventsCreateBatch(context.Context, EventsCreateBatchInput) (EventBatchResult, error)
	// EventsGet returns the event identified by RunID and EventID. It returns
	// ErrEventNotFound when the event does not exist. It applies ResolveData to
	// the returned event.
	EventsGet(context.Context, EventsGetInput) (Event, error)
	// EventsList returns the events in a run. It applies Pagination and
	// ResolveData to the result.
	EventsList(context.Context, EventsListInput) (Page[Event], error)
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
	// retention has ended. It applies
	// ResolveData to the returned hook.
	HooksGetByToken(context.Context, HooksGetByTokenInput) (Hook, error)
	// HooksList returns hooks that match the input filters. It applies Pagination
	// and ResolveData to the result.
	HooksList(context.Context, HooksListInput) (Page[Hook], error)
	// HooksClaim atomically claims the token of the hook_created Event and
	// appends the event. It returns ErrHookConflict when another hook owns the
	// token and Force is false. With Force, it takes the token from its owner,
	// appends hook_disposed with HookDisposedEventData.ForceClaimedBy to the
	// owner's run, and reports that run in ForcedRunID.
	HooksClaim(context.Context, HooksClaimInput) (HooksClaimResult, error)
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
		Key string
		// Value is the new value. Nil deletes the attribute.
		Value *string
	}
	// RunsSetAttributesInput holds the arguments to RunsSetAttributes.
	RunsSetAttributesInput struct {
		RunID   RunID
		Changes []RunsAttributeChange
	}
)

// Attributes the runtime sets on a child run, named as in the Workflow SDK.
const (
	// ParentRunIDAttribute holds the ID of the run that started a child run.
	ParentRunIDAttribute = "$parentRunId"
	// RootRunIDAttribute holds the ID of the run at the root of a child run's
	// lineage.
	RootRunIDAttribute = "$rootRunId"
)

// Run is the view of one run derived from its event log.
type Run struct {
	RunID        RunID
	WorkflowName string
	Status       Status
	// Input is the JSON-encoded workflow input.
	Input []byte
	// Output is the JSON-encoded workflow output of a completed run.
	Output []byte
	// Error is the error message of a failed run.
	Error       []byte
	Attributes  map[string]string
	CreatedAt   time.Time
	UpdatedAt   time.Time
	StartedAt   *time.Time
	CompletedAt *time.Time
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
	// Input is the JSON-encoded step input.
	Input []byte
	// Output is the JSON-encoded output of a completed step.
	Output []byte
	// Error is the error message of the last failed attempt.
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
	// HooksClaimInput holds the arguments to HooksClaim.
	HooksClaimInput struct {
		RunID RunID
		// Event is the hook_created event whose token is claimed.
		Event   EventsCreateEvent
		Options EventsCreateOptions
		// Force takes the token from the hook that owns it instead of
		// returning ErrHookConflict.
		Force bool
	}
	// HooksClaimResult is the result of HooksClaim.
	HooksClaimResult struct {
		EventResult
		// ForcedRunID is the run whose hook lost the token to a forced claim,
		// or empty. The caller queues a delivery for it so its replay observes
		// ErrHookForceClaimed.
		ForcedRunID RunID
	}
	// WebhookRequest is an HTTP request received by a webhook.
	WebhookRequest struct {
		Method string              `json:"method"`
		URL    string              `json:"url"`
		Header map[string][]string `json:"header"`
		Body   []byte              `json:"body"`
	}
	// WebhookResponse is an HTTP response returned to a webhook caller. A zero
	// Status is sent as 200.
	WebhookResponse struct {
		Status int                 `json:"status"`
		Header map[string][]string `json:"header,omitempty"`
		Body   []byte              `json:"body,omitempty"`
	}
	// WebhookReceivedEventData is the Data of a hook_received event for a
	// webhook. It holds either the received Request or, for a manual
	// webhook, the workflow's Response to the request event named by
	// ResponseTo.
	WebhookReceivedEventData struct {
		Request    *WebhookRequest  `json:"request,omitempty"`
		Response   *WebhookResponse `json:"response,omitempty"`
		ResponseTo EventID          `json:"responseTo,omitempty"`
	}
	// HookDisposedEventData is the Data of a hook_disposed event. It is empty
	// when the hook disposed of itself. ForceClaimedBy is set when another
	// hook took the token with a forced claim, as in the Workflow SDK.
	HookDisposedEventData struct {
		ForceClaimedBy *HookForceClaim `json:"forceClaimedBy,omitempty"`
	}
	// HookForceClaim names the hook that took a token with a forced claim.
	HookForceClaim struct {
		RunID  RunID  `json:"runId"`
		HookID HookID `json:"hookId"`
	}
)

// Webhook configures how a webhook answers HTTP requests to its URL.
type Webhook struct {
	// Method is the HTTP method the webhook accepts. Empty accepts any
	// method.
	Method string `json:"method,omitempty"`
	// Manual makes the webhook wait for the workflow to record a response
	// and send that to the caller.
	Manual bool `json:"manual,omitempty"`
	// Response is sent to every caller when set, and the hook is disposed
	// after the first request. When neither Manual nor Response is set, callers
	// receive 202 Accepted.
	Response *WebhookResponse `json:"response,omitempty"`
}

// Hook is the view of one hook derived from its run's event log.
type Hook struct {
	RunID  RunID
	HookID HookID
	Token  string
	// Metadata is the JSON-encoded metadata recorded when the hook was
	// created.
	Metadata []byte
	// Webhook is set when the hook is a webhook.
	Webhook *Webhook
	// ReceivedEventID is the hook_received event that recorded the webhook
	// request.
	ReceivedEventID EventID
	CreatedAt       time.Time
	UpdatedAt       time.Time
	ReceivedAt      *time.Time
	// MinRetention is how long the token stays claimed after the hook is
	// resolved, so the token cannot be reused immediately. Zero releases the
	// token when the hook is resolved.
	MinRetention time.Duration
	// RetainUntil is when the token is released, once the hook has been
	// resolved with a non-zero MinRetention.
	RetainUntil *time.Time
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
	// EventTypeStepCancelled records that the step was cancelled.
	EventTypeStepCancelled EventType = "step_cancelled"
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
		EventTypeStepCompleted, EventTypeStepFailed, EventTypeStepRetrying, EventTypeStepCancelled,
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
		// Input is the JSON-encoded workflow input.
		Input      []byte
		Attributes map[string]string
	}
	// EventsCreateInput holds the arguments to EventsCreate.
	EventsCreateInput struct {
		RunID   RunID
		Event   EventsCreateEvent
		Options EventsCreateOptions
	}
	// EventsCreateOptions selects the events returned in EventResult.Events,
	// letting a caller catch up on events appended since it last read the
	// log.
	EventsCreateOptions struct {
		// EventCount is the number of events the caller already holds. When
		// set, Events holds every event after that count that was in the log
		// before this call. The event this call appends is not included. It
		// takes precedence over SinceCursor.
		EventCount *int
		// SinceCursor is the ID of the last event the caller holds. When set
		// and EventCount is nil, Events holds the earlier events after it, on
		// the same terms. When both are unset, Events is empty.
		SinceCursor string
		ResolveData EventsResolveData
	}
	// EventsCreateBatchInput holds the arguments to EventsCreateBatch.
	EventsCreateBatchInput struct {
		RunID  RunID
		Events []EventsCreateEvent
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
		// EventID and EventType identify the appended or previously admitted event.
		EventID   EventID
		EventType EventType
		// Events holds the events selected by EventsCreateOptions.
		Events []Event
		// Step is the step created by a step_created event.
		Step *Step
		// StepCreated reports whether this call appended the step_created event.
		StepCreated bool
		// AllocatedEventCount is the number of events newly appended by this call.
		AllocatedEventCount int
	}
	// EventBatchResult is the result of EventsCreateBatch, with one
	// EventResult per event in order.
	EventBatchResult struct {
		Events []EventResult
	}
)

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
