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
	EventsList(context.Context, EventsListInput) (Page[Event], error)
	EventsListByCorrelationID(context.Context, EventsListByCorrelationIDInput) (Page[Event], error)
}

type StorageHooks interface {
	HooksGet(context.Context, HooksGetInput) (Hook, error)
	HooksGetByToken(context.Context, HooksGetByTokenInput) (Hook, error)
	HooksList(context.Context, HooksListInput) (Page[Hook], error)
}

type (
	RunsGetInput struct {
		RunID RunID
	}
	RunsListInput struct {
		WorkflowName string
		Status       []Status
		Pagination   Pagination
	}
	RunsWaitForTerminalStatusInput struct {
		RunID   RunID
		Timeout time.Duration
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
		RunID  RunID
		StepID StepID
	}

	StepsListInput struct {
		RunID      RunID
		Pagination Pagination
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
		HookID HookID
	}
	HooksGetByTokenInput struct {
		Token string
	}
	HooksListInput struct {
		Pagination Pagination
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
}

type EventType string

const (
	EventTypeRunCreated    EventType = "run_created"
	EventTypeRunStarted    EventType = "run_started"
	EventTypeRunCompleted  EventType = "run_completed"
	EventTypeRunFailed     EventType = "run_failed"
	EventTypeRunCancelled  EventType = "run_cancelled"
	EventTypeAttrSet       EventType = "attr_set"
	EventTypeStepCreated   EventType = "step_created"
	EventTypeStepStarted   EventType = "step_started"
	EventTypeStepCompleted EventType = "step_completed"
	EventTypeStepFailed    EventType = "step_failed"
	EventTypeStepRetrying  EventType = "step_retrying"
	EventTypeStepCancelled EventType = "step_cancelled"
	EventTypeHookCreated   EventType = "hook_created"
	EventTypeHookReceived  EventType = "hook_received"
	EventTypeHookDisposed  EventType = "hook_disposed"
	EventTypeHookConflict  EventType = "hook_conflict"
	EventTypeWaitCreated   EventType = "wait_created"
	EventTypeWaitCompleted EventType = "wait_completed"
	EventTypeNoop          EventType = "noop"
)

func (t EventType) Valid() bool {
	switch t {
	case EventTypeRunCreated, EventTypeRunStarted, EventTypeRunCompleted, EventTypeRunFailed,
		EventTypeRunCancelled, EventTypeAttrSet, EventTypeStepCreated, EventTypeStepStarted,
		EventTypeStepCompleted, EventTypeStepFailed, EventTypeStepRetrying, EventTypeStepCancelled, EventTypeHookCreated,
		EventTypeHookReceived, EventTypeHookDisposed, EventTypeHookConflict, EventTypeWaitCreated,
		EventTypeWaitCompleted, EventTypeNoop:
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
	}
	EventsCreateBatchInput struct {
		RunID  RunID
		Events []EventsCreateEvent
	}
	EventsCreateEvent struct {
		EventType     EventType
		CorrelationID string
		Data          []byte
	}
	EventsListInput struct {
		RunID      RunID
		Pagination Pagination
	}
	EventsListByCorrelationIDInput struct {
		RunID         RunID
		CorrelationID string
		Pagination    Pagination
	}
	EventResult struct {
		EventID   EventID
		EventType EventType
		Events    []Event
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
