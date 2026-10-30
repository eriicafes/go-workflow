// Package worlds defines the World contract that go-workflow records runs in,
// the runtime that executes deliveries against it, and the HTTP server that
// exposes a World to clients.
//
// Each run is an append-only event log, and the runs, steps, and hooks returned
// by Storage are views derived from those events.
package worlds

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"
)

// SpecVersion is the RPC protocol version implemented by this package. A
// World returns it from its SpecVersion method.
const SpecVersion = 1

// World is the backend a run is recorded in. It provides event storage, a
// delivery queue, and streams. The runtime, the client, and HTTPHandler all
// operate on a World.
type World interface {
	Storage
	Queue
	Streamer
	// BaseURL returns the absolute public URL where HTTPHandler is mounted,
	// including its workflow path prefix and without a trailing slash.
	BaseURL() string
	// Analytics returns the Analytics interface when analytics are enabled,
	// otherwise nil.
	Analytics() Analytics
	// Capabilities reports the optional behavior this World implements.
	Capabilities() Capabilities
	// SpecVersion returns the RPC protocol version implemented by this World.
	SpecVersion() int
	// DeploymentID returns the deployment whose code this World's deliveries
	// execute, or "" when the World has no deployments. Runs created through
	// the World's Client record it, and the Runtime enforces it only when
	// Capabilities reports DeploymentAffinity.
	DeploymentID() string
}

// Capabilities reports optional World behavior. A capability being false does
// not remove a method from World. Unsupported attribute writes are successful
// no-ops, while Analytics returns nil.
//
// DeploymentAffinity reports that the World's deployments are atomic and its
// queue can address a delivery to one, as Vercel's can. Runs created through
// the World's Client are then pinned to World.DeploymentID, and the Runtime
// replays a run only on the deployment it is pinned to, re-routing other
// deliveries there.
//
// QueueRouting reports that the World chooses a delivery's queue from its
// QueueInput.WorkflowName. Code that queues a delivery for a run it has not
// loaded then reads the run for its workflow name.
type Capabilities struct {
	Analytics          bool `cbor:"analytics"`
	SetAttributes      bool `cbor:"setAttributes"`
	DeploymentAffinity bool `cbor:"deploymentAffinity"`
	QueueRouting       bool `cbor:"queueRouting"`
}

// NormalizeBaseURL validates an HTTP handler's public URL and removes its
// trailing slash.
func NormalizeBaseURL(value string) (string, error) {
	parsed, err := url.ParseRequestURI(value)
	if err != nil || parsed.Host == "" || parsed.User != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.ForceQuery || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", fmt.Errorf("workflow: base URL must be an absolute HTTP URL without a query or fragment")
	}
	return strings.TrimRight(value, "/"), nil
}

// Status is the lifecycle state of a run or step.
type Status string

const (
	// StatusPending means the run or step has been created but not started.
	StatusPending Status = "pending"
	// StatusRunning means a delivery has started the run or step.
	StatusRunning Status = "running"
	// StatusCompleted means the run or step finished with an output.
	StatusCompleted Status = "completed"
	// StatusFailed means the run or step finished with an error.
	StatusFailed Status = "failed"
	// StatusCancelled means the run or step was cancelled.
	StatusCancelled Status = "cancelled"
)

// Terminal reports whether s is completed, failed, or cancelled. A run or
// step in a terminal status accepts no further lifecycle events.
func (s Status) Terminal() bool {
	return s == StatusCompleted || s == StatusFailed || s == StatusCancelled
}

// ResolveData selects whether Storage returns the payloads of runs, steps,
// and hooks, which are run and step Input and Output, and hook Metadata. The
// zero value behaves like ResolveDataAll.
type ResolveData string

const (
	// ResolveDataAll returns payloads.
	ResolveDataAll ResolveData = "all"
	// ResolveDataNone omits payloads.
	ResolveDataNone ResolveData = "none"
)

// EventsResolveData selects which event payloads Storage returns. The zero
// value behaves like EventsResolveDataAll.
type EventsResolveData string

const (
	// EventsResolveDataAll returns every event's Data.
	EventsResolveDataAll EventsResolveData = "all"
	// EventsResolveDataNone omits payloads from event Data and keeps its
	// metadata, as ResolveEventData describes.
	EventsResolveDataNone EventsResolveData = "none"
	// EventsResolveDataSkipStepInputs requests event Data without the step
	// input recorded in step_created events. A World may treat it like
	// EventsResolveDataAll.
	EventsResolveDataSkipStepInputs EventsResolveData = "skip-step-inputs"
)

// SortOrder is the order in which a list returns its items. The zero value
// behaves like SortOrderAscending.
type SortOrder string

const (
	// SortOrderAscending returns the oldest items first.
	SortOrderAscending SortOrder = "asc"
	// SortOrderDescending returns the newest items first.
	SortOrderDescending SortOrder = "desc"
)

// RunID identifies a run. Generated run IDs have the prefix "wrun_".
type RunID string

// StepID identifies a step within its run. Generated step IDs have the
// prefix "step_".
type StepID string

// HookID identifies a hook. Generated hook IDs have the prefix "hook_".
type HookID string

// WaitID identifies a wait, the record of a timer. Generated wait IDs have
// the prefix "wait_".
type WaitID string

// StreamID identifies a stream writer session. Generated stream IDs have the
// prefix "strm_".
type StreamID string

// EventID identifies an event within its run's event log. It encodes the
// event's position in the log (see EventIDForSlot and EventSlot).
type EventID string

// NewChildRunID returns the ID of the run started by step stepID. It shares the
// step ID's ULID, so every attempt of the step derives the same run ID.
func NewChildRunID(stepID StepID) RunID {
	return RunID("wrun_" + strings.TrimPrefix(string(stepID), "step_"))
}

// NewRunID returns a new unique run ID.
func NewRunID() RunID { return RunID("wrun_" + ulid.Make().String()) }

// NewStepID returns a new unique step ID.
func NewStepID() StepID { return StepID("step_" + ulid.Make().String()) }

// NewHookID returns a new unique hook ID.
func NewHookID() HookID { return HookID("hook_" + ulid.Make().String()) }

// NewWaitID returns a new unique wait ID.
func NewWaitID() WaitID { return WaitID("wait_" + ulid.Make().String()) }

// NewStreamID returns a new unique stream writer session ID.
func NewStreamID() StreamID { return StreamID("strm_" + ulid.Make().String()) }

const hookTokenAlphabet = "useandom-26T19834_0v5b7qw9zxcgfkrljAIJPVYKhpGUCXW"

// NewRunHookToken returns a 21-character token derived from the run and hook
// declaration position. It stays the same across replays, including retries
// before the hook registration is committed.
func NewRunHookToken(run Run, position int) string {
	seed := sha256.Sum256(fmt.Appendf(nil, "%s:%s:%s:hook:%d", run.RunID, run.WorkflowName, run.DeploymentID, position))
	return base64.RawURLEncoding.EncodeToString(seed[:])[:21]
}

// NewHookToken returns a new random hook token, which is a 21-character Nano
// ID.
func NewHookToken() string {
	bytes := make([]byte, 21)
	limit := 256 - 256%len(hookTokenAlphabet)
	for index := 0; index < len(bytes); {
		var random [1]byte
		if _, err := rand.Read(random[:]); err != nil {
			panic(err)
		}
		if int(random[0]) >= limit {
			continue
		}
		bytes[index] = hookTokenAlphabet[int(random[0])%len(hookTokenAlphabet)]
		index++
	}
	return string(bytes)
}

type (
	// Pagination selects one page of a list.
	Pagination struct {
		// Cursor is the Cursor of the previous Page. Empty starts at the
		// first item.
		Cursor string
		// Limit is the maximum number of items to return. Zero or less
		// requests every remaining item, though a World may still return
		// fewer and set HasMore.
		Limit     int
		SortOrder SortOrder
	}
	// Page is one page of a list.
	Page[T any] struct {
		Data []T
		// Cursor continues the list after this page when passed back in
		// Pagination.
		Cursor string
		// HasMore reports whether items remain after this page.
		HasMore bool
	}
)

// WakeRun completes the selected pending waits of a run and queues a
// delivery that replays it. correlationIDs holds wait IDs, and an empty list
// selects every pending wait. It returns the number of waits it completed.
// It queues the delivery even when that number is zero while the run is not
// terminal, so retrying a call whose queueing failed still wakes the run.
func WakeRun(ctx context.Context, world World, runID RunID, correlationIDs []string) (int, error) {
	pending, err := PendingWaits(ctx, world, runID)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, waitID := range pending {
		if len(correlationIDs) != 0 && !slices.Contains(correlationIDs, waitID) {
			continue
		}
		if _, err := world.EventsCreate(ctx, EventsCreateInput{
			RunID: runID,
			Event: EventsCreateEvent{
				EventType:     EventTypeWaitCompleted,
				CorrelationID: waitID,
			},
		}); err != nil {
			return count, err
		}
		count++
	}
	if count == 0 {
		run, err := world.RunsGet(ctx, RunsGetInput{RunID: runID, ResolveData: ResolveDataNone})
		if err != nil || run.Status.Terminal() {
			return 0, err
		}
	}
	queue, err := RunDelivery(ctx, world, runID)
	if err != nil {
		return count, err
	}
	queue.IdempotencyKey = "wake:" + string(runID)
	_, err = world.Queue(ctx, queue)
	return count, err
}

// PendingWaits returns the IDs of run runID's waits that have not completed,
// in the order they were created.
func PendingWaits(ctx context.Context, world World, runID RunID) ([]string, error) {
	var created []string
	completed := make(map[string]struct{})
	var cursor Pagination
	for {
		page, err := world.EventsList(ctx, EventsListInput{
			RunID:      runID,
			Pagination: cursor,
		})
		if err != nil {
			return nil, err
		}
		for _, event := range page.Data {
			switch event.EventType {
			case EventTypeWaitCreated:
				created = append(created, event.CorrelationID)
			case EventTypeWaitCompleted:
				completed[event.CorrelationID] = struct{}{}
			}
		}
		if !page.HasMore {
			break
		}
		cursor = Pagination{Cursor: page.Cursor}
	}
	pending := created[:0]
	for _, waitID := range created {
		if _, ok := completed[waitID]; !ok {
			pending = append(pending, waitID)
		}
	}
	return pending, nil
}

// ResumeHook appends payload to hook as a hook_received event and queues a
// delivery so the hook's run resumes, addressed to the run's deployment. When
// the World refuses the payload with ErrHookNotActive, it still queues the
// delivery, because an earlier resume may have recorded its payload without
// queueing one, and returns the error.
//
// When queueing fails after the payload is recorded, it returns an error
// wrapping ErrResumeNotQueued.
func ResumeHook(ctx context.Context, world World, hook Hook, payload []byte) (EventResult, error) {
	data, err := EncodeHookReceived(payload)
	if err != nil {
		return EventResult{}, err
	}
	result, err := world.EventsCreate(ctx, EventsCreateInput{
		RunID: hook.RunID,
		Event: EventsCreateEvent{EventType: EventTypeHookReceived, CorrelationID: string(hook.HookID), Data: data},
	})
	key := string(result.EventID)
	switch {
	case errors.Is(err, ErrHookNotActive):
		key = "resume:" + string(hook.HookID)
	case err != nil:
		return result, err
	}
	// the hook carries its run's workflow name, so only a World with
	// deployment affinity reads the run
	queue := QueueInput{RunID: hook.RunID, WorkflowName: hook.WorkflowName}
	if world.Capabilities().DeploymentAffinity {
		var deliveryErr error
		if queue, deliveryErr = RunDelivery(ctx, world, hook.RunID); deliveryErr != nil {
			return result, deliveryErr
		}
	}
	queue.IdempotencyKey = key
	_, queueErr := world.Queue(ctx, queue)
retry:
	for attempt := 1; queueErr != nil && attempt < resumeQueueAttempts; attempt++ {
		timer := time.NewTimer(time.Duration(attempt) * 100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			queueErr = errors.Join(queueErr, ctx.Err())
			break retry
		case <-timer.C:
		}
		_, queueErr = world.Queue(ctx, queue)
	}
	if queueErr == nil {
		return result, err
	}
	if err == nil {
		return result, fmt.Errorf("%w: %w", ErrResumeNotQueued, queueErr)
	}
	return result, queueErr
}

// resumeQueueAttempts is how often ResumeHook tries to queue a recorded payload's run.
const resumeQueueAttempts = 3

// RunDeploymentID returns the deployment run runID is pinned to, so a delivery
// queued from outside the run can be addressed to it. It returns "" without
// reading the run when world does not report DeploymentAffinity.
func RunDeploymentID(ctx context.Context, world World, runID RunID) (string, error) {
	if !world.Capabilities().DeploymentAffinity {
		return "", nil
	}
	run, err := world.RunsGet(ctx, RunsGetInput{RunID: runID, ResolveData: ResolveDataNone})
	return run.DeploymentID, err
}

// RunDelivery returns a delivery for runID addressed as the Runtime addresses
// its own: to the run's deployment when the World reports DeploymentAffinity,
// and with the run's workflow name when it reports QueueRouting. It reads the
// run only when one of them needs it.
func RunDelivery(ctx context.Context, world World, runID RunID) (QueueInput, error) {
	input := QueueInput{RunID: runID}
	capabilities := world.Capabilities()
	if !capabilities.DeploymentAffinity && !capabilities.QueueRouting {
		return input, nil
	}
	run, err := world.RunsGet(ctx, RunsGetInput{RunID: runID, ResolveData: ResolveDataNone})
	if err != nil {
		return input, err
	}
	if capabilities.DeploymentAffinity {
		input.DeploymentID = run.DeploymentID
	}
	input.WorkflowName = run.WorkflowName
	return input, nil
}
