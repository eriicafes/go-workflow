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
	RunsGet(context.Context, RunsGetInput) (Run, error)
	RunsList(context.Context, RunsListInput) (Page[Run], error)
	RunsWaitForTerminalStatus(context.Context, RunsWaitForTerminalStatusInput) (Run, error)
	RunsSetAttributes(context.Context, RunsSetAttributesInput) error
}

type StorageSteps interface {
	StepsGet(context.Context, StepsGetInput) (Step, error)
	StepsList(context.Context, StepsListInput) (Page[Step], error)
}

type StorageEvents interface {
	EventsCreateRun(context.Context, EventsCreateRunInput) (EventResult, error)
	EventsCreate(context.Context, EventsCreateInput) (EventResult, error)
	EventsCreateBatch(context.Context, EventsCreateBatchInput) (EventBatchResult, error)
	EventsGet(context.Context, EventsGetInput) (Event, error)
	EventsList(context.Context, EventsListInput) (Page[Event], error)
	EventsListByCorrelationID(context.Context, EventsListByCorrelationIDInput) (Page[Event], error)
}

type StorageHooks interface {
	HooksGet(context.Context, HooksGetInput) (Hook, error)
	HooksGetByToken(context.Context, HooksGetByTokenInput) (Hook, error)
	HooksList(context.Context, HooksListInput) (Page[Hook], error)
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
		EventID             EventID
		EventType           EventType
		Events              []Event
		Step                *Step
		StepCreated         bool
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
