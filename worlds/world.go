// Package worlds defines the durable storage, queue, and stream protocol used
// by go-workflow. State is represented by an append-only event log; the run,
// step, and hook records returned by Storage are materialized views.
package worlds

import (
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
