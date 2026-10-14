package worlds

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type Storage interface {
	StorageRuns
	StorageSteps
	StorageEvents
	StorageHooks
}

type StorageRuns interface {
	// RunsGet returns the run identified by RunID. It returns ErrRunNotFound when
	// the run does not exist. It applies ResolveData to the returned run.
	RunsGet(context.Context, RunsGetInput) (Run, error)
	// RunsList returns runs that match the input filters. It applies Pagination
	// and ResolveData to the result.
	RunsList(context.Context, RunsListInput) (Page[Run], error)
	// RunsWaitForTerminalStatus waits for a terminal run or Timeout. It returns
	// the current run when Timeout expires.
	RunsWaitForTerminalStatus(context.Context, RunsWaitForTerminalStatusInput) (Run, error)
	// RunsSetAttributes applies Changes to the identified run as one durable
	// update.
	RunsSetAttributes(context.Context, RunsSetAttributesInput) error
}

type StorageSteps interface {
	// StepsGet returns the step identified by RunID and StepID. It returns
	// ErrStepNotFound when the step does not exist. It applies ResolveData to the
	// returned step.
	StepsGet(context.Context, StepsGetInput) (Step, error)
	// StepsList returns the steps in a run. It applies Pagination and ResolveData
	// to the result.
	StepsList(context.Context, StepsListInput) (Page[Step], error)
}

type StorageEvents interface {
	// EventsCreateRun creates the identified run and its initial run_created
	// event. It does not deduplicate calls. It returns ErrRunAlreadyExists when
	// the run already exists. Its result has AllocatedEventCount set to one.
	EventsCreateRun(context.Context, EventsCreateRunInput) (EventResult, error)
	// EventsCreate appends a valid lifecycle event to a run. It enforces the
	// transition rules and returns events selected by EventsCreateOptions. It
	// sets AllocatedEventCount to one when it appends an event and zero when it
	// returns a previously appended event. For a step_created event, it returns
	// the materialized Step and sets StepCreated only when it appended that step.
	EventsCreate(context.Context, EventsCreateInput) (EventResult, error)
	// EventsCreateBatch appends Events in order. It appends none of them when an
	// event is invalid or cannot be admitted.
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

type StorageHooks interface {
	// HooksGet returns the hook identified by HookID. It returns ErrHookNotFound
	// when the hook does not exist. It applies ResolveData to the returned hook.
	HooksGet(context.Context, HooksGetInput) (Hook, error)
	// HooksGetByToken returns the active hook for Token. It returns
	// ErrHookNotFound when the token is unclaimed or expired. It applies
	// ResolveData to the returned hook.
	HooksGetByToken(context.Context, HooksGetByTokenInput) (Hook, error)
	// HooksList returns hooks that match the input filters. It applies Pagination
	// and ResolveData to the result.
	HooksList(context.Context, HooksListInput) (Page[Hook], error)
	// HooksClaim atomically claims the hook token in Event and appends the event.
	// It returns ErrHookConflict when another active hook owns the token and
	// Force is false.
	HooksClaim(context.Context, HooksClaimInput) (HooksClaimResult, error)
}

type (
	RunsGetInput struct {
		RunID       RunID
		ResolveData ResolveData
	}
	RunsListInput struct {
		WorkflowName string
		Status       []Status
		Pagination   Pagination
		ResolveData  ResolveData
	}
	RunsWaitForTerminalStatusInput struct {
		RunID       RunID
		Timeout     time.Duration
		ResolveData ResolveData
	}
	RunsAttributeChange struct {
		Key   string
		Value *string
	}
	RunsSetAttributesInput struct {
		RunID   RunID
		Changes []RunsAttributeChange
	}
)

type Run struct {
	RunID        RunID
	WorkflowName string
	Status       Status
	Input        []byte
	Output       []byte
	Error        []byte
	Attributes   map[string]string
	CreatedAt    time.Time
	UpdatedAt    time.Time
	StartedAt    *time.Time
	CompletedAt  *time.Time
}

type (
	StepsGetInput struct {
		RunID       RunID
		StepID      StepID
		ResolveData ResolveData
	}

	StepsListInput struct {
		RunID       RunID
		Pagination  Pagination
		ResolveData ResolveData
	}
)

type Step struct {
	RunID       RunID
	StepID      StepID
	StepName    string
	Status      Status
	Input       []byte
	Output      []byte
	Error       []byte
	Attempt     int
	CreatedAt   time.Time
	UpdatedAt   time.Time
	StartedAt   *time.Time
	CompletedAt *time.Time
	RetryAfter  *time.Time
}

type (
	HooksGetInput struct {
		HookID      HookID
		ResolveData ResolveData
	}
	HooksGetByTokenInput struct {
		Token       string
		ResolveData ResolveData
	}
	HooksListInput struct {
		Pagination  Pagination
		RunID       RunID
		ResolveData ResolveData
	}
	HooksClaimInput struct {
		RunID   RunID
		Event   EventsCreateEvent
		Options EventsCreateOptions
		Force   bool
	}
	HooksClaimResult struct {
		EventResult
		ForcedRunID RunID
	}
	WebhookRequest struct {
		Method string              `json:"method"`
		URL    string              `json:"url"`
		Header map[string][]string `json:"header"`
		Body   []byte              `json:"body"`
	}
	WebhookResponse struct {
		Status int                 `json:"status"`
		Header map[string][]string `json:"header,omitempty"`
		Body   []byte              `json:"body,omitempty"`
	}
	WebhookReceivedEventData struct {
		Request    *WebhookRequest  `json:"request,omitempty"`
		Response   *WebhookResponse `json:"response,omitempty"`
		ResponseTo EventID          `json:"responseTo,omitempty"`
	}
)

type Webhook struct {
	Method   string           `json:"method,omitempty"`
	Manual   bool             `json:"manual,omitempty"`
	Response *WebhookResponse `json:"response,omitempty"`
}

type Hook struct {
	RunID           RunID
	HookID          HookID
	Token           string
	Metadata        []byte
	Webhook         *Webhook
	ReceivedEventID EventID
	CreatedAt       time.Time
	UpdatedAt       time.Time
	ReceivedAt      *time.Time
	MinRetention    time.Duration
	RetainUntil     *time.Time
}

type EventType string

const (
	EventTypeRunCreated       EventType = "run_created"
	EventTypeRunSpawned       EventType = "run_spawned"
	EventTypeRunStarted       EventType = "run_started"
	EventTypeRunCompleted     EventType = "run_completed"
	EventTypeRunFailed        EventType = "run_failed"
	EventTypeRunCancelled     EventType = "run_cancelled"
	EventTypeAttrSet          EventType = "attr_set"
	EventTypeStepCreated      EventType = "step_created"
	EventTypeStepStarted      EventType = "step_started"
	EventTypeStepCompleted    EventType = "step_completed"
	EventTypeStepFailed       EventType = "step_failed"
	EventTypeStepRetrying     EventType = "step_retrying"
	EventTypeStepCancelled    EventType = "step_cancelled"
	EventTypeHookCreated      EventType = "hook_created"
	EventTypeHookReceived     EventType = "hook_received"
	EventTypeHookDisposed     EventType = "hook_disposed"
	EventTypeHookConflict     EventType = "hook_conflict"
	EventTypeHookForceClaimed EventType = "hook_force_claimed"
	EventTypeWaitCreated      EventType = "wait_created"
	EventTypeWaitCompleted    EventType = "wait_completed"
)

func (t EventType) Valid() bool {
	switch t {
	case EventTypeRunCreated, EventTypeRunSpawned, EventTypeRunStarted, EventTypeRunCompleted, EventTypeRunFailed,
		EventTypeRunCancelled, EventTypeAttrSet, EventTypeStepCreated, EventTypeStepStarted,
		EventTypeStepCompleted, EventTypeStepFailed, EventTypeStepRetrying, EventTypeStepCancelled,
		EventTypeHookCreated, EventTypeHookReceived, EventTypeHookDisposed, EventTypeHookConflict, EventTypeHookForceClaimed,
		EventTypeWaitCreated, EventTypeWaitCompleted:
		return true
	default:
		return false
	}
}

type (
	EventsCreateRunInput struct {
		RunID        RunID
		WorkflowName string
		Input        []byte
		Attributes   map[string]string
	}
	EventsCreateInput struct {
		RunID   RunID
		Event   EventsCreateEvent
		Options EventsCreateOptions
	}
	EventsCreateOptions struct {
		EventCount  *int
		SinceCursor string
		ResolveData EventsResolveData
	}
	EventsCreateBatchInput struct {
		RunID  RunID
		Events []EventsCreateEvent
	}
	EventsGetInput struct {
		RunID       RunID
		EventID     EventID
		ResolveData EventsResolveData
	}
	EventsCreateEvent struct {
		EventType     EventType
		CorrelationID string
		Data          []byte
	}
	EventsListInput struct {
		RunID       RunID
		Pagination  Pagination
		ResolveData EventsResolveData
	}
	EventsListByCorrelationIDInput struct {
		RunID         RunID
		CorrelationID string
		Pagination    Pagination
		ResolveData   EventsResolveData
	}
	EventResult struct {
		// EventID and EventType identify the appended or previously admitted event.
		EventID   EventID
		EventType EventType
		// Events contains replay events selected by EventsCreateOptions.
		Events []Event
		// Step is set for a step_created event when its materialized step exists.
		Step *Step
		// StepCreated reports whether this call appended the step_created event.
		StepCreated bool
		// AllocatedEventCount is the number of events newly appended by this call.
		AllocatedEventCount int
	}
	EventBatchResult struct {
		Events []EventResult
	}
)

type Event struct {
	RunID         RunID
	EventID       EventID
	EventType     EventType
	CorrelationID string
	Data          []byte
	CreatedAt     time.Time
}

func EventIDForSlot(slot uint64) EventID {
	return EventID(fmt.Sprintf("evnt_%026d", slot))
}

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
