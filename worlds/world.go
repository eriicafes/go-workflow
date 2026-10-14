// Package worlds defines the durable storage, queue, and stream protocol used
// by go-workflow. State is represented by an append-only event log; the run,
// step, and hook records returned by Storage are materialized views.
package worlds

import (
	"context"
	"crypto/rand"

	"github.com/oklog/ulid/v2"
)

const SpecVersion = 1

type World interface {
	Storage
	Queue
	Streamer
	Analytics() Analytics
	Capabilities() Capabilities
	SpecVersion() int
}

// Capabilities reports optional World behavior. A capability being false does
// not remove a method from World: unsupported attribute writes are successful
// no-ops, while Analytics returns nil.
type Capabilities struct {
	Analytics     bool `cbor:"analytics"`
	SetAttributes bool `cbor:"setAttributes"`
}

type Status string

const (
	StatusPending   Status = "pending"
	StatusRunning   Status = "running"
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
	StatusCancelled Status = "cancelled"
)

func (s Status) Terminal() bool {
	return s == StatusCompleted || s == StatusFailed || s == StatusCancelled
}

type RunID string
type StepID string
type HookID string
type WaitID string
type StreamID string
type EventID string

func NewRunID() RunID       { return RunID("wrun_" + ulid.Make().String()) }
func NewStepID() StepID     { return StepID("step_" + ulid.Make().String()) }
func NewHookID() HookID     { return HookID("hook_" + ulid.Make().String()) }
func NewWaitID() WaitID     { return WaitID("wait_" + ulid.Make().String()) }
func NewStreamID() StreamID { return StreamID("strm_" + ulid.Make().String()) }

const hookTokenAlphabet = "useandom-26T19834_0v5b7qw9zxcgfkrljAIJPVYKhpGUCXW"

// NewHookToken returns a 21-character Nano ID.
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
	Pagination struct {
		Cursor string
		Limit  int
		Desc   bool
	}
	Page[T any] struct {
		Data    []T
		Cursor  string
		HasMore bool
	}
)

// WakeRun completes the selected pending waits and queues one root replay.
// An empty correlation ID list selects every pending wait.
func WakeRun(ctx context.Context, world World, runID RunID, correlationIDs []string) (int, error) {
	created := make(map[string]struct{})
	completed := make(map[string]struct{})
	page, err := world.EventsList(ctx, EventsListInput{RunID: runID})
	if err != nil {
		return 0, err
	}
	for {
		for _, event := range page.Data {
			switch event.EventType {
			case EventTypeWaitCreated:
				created[event.CorrelationID] = struct{}{}
			case EventTypeWaitCompleted:
				completed[event.CorrelationID] = struct{}{}
			}
		}
		if !page.HasMore {
			break
		}
		page, err = world.EventsList(ctx, EventsListInput{RunID: runID, Pagination: Pagination{Cursor: page.Cursor}})
		if err != nil {
			return 0, err
		}
	}
	count := 0
	for waitID := range created {
		if _, ok := completed[waitID]; ok {
			continue
		}
		if len(correlationIDs) != 0 {
			found := false
			for _, correlationID := range correlationIDs {
				if correlationID == waitID {
					found = true
					break
				}
			}
			if !found {
				continue
			}
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
		return 0, nil
	}
	_, err = world.Queue(ctx, QueueInput{RunID: runID, IdempotencyKey: "wake:" + string(runID)})
	return count, err
}
