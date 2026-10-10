// Package workflowops defines the worker-side workflow execution protocol.
package workflowops

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// RootSequence identifies the root workflow task in a run.
const RootSequence = -1

// Runner is the erased, executable form of a workflow function.
type Runner struct {
	identity string
	execute  func(context.Context, json.RawMessage) (json.RawMessage, error)
}

// RunnerSet maps workflow identities to their executable runners.
type RunnerSet map[string]Runner

// NewRunner constructs a runner from its stable identity and decoder/executor.
func NewRunner(identity string, execute func(context.Context, json.RawMessage) (json.RawMessage, error)) Runner {
	return Runner{identity: identity, execute: execute}
}

// Identity is the workflow's stable type identity.
func (runner Runner) Identity() string { return runner.identity }

// Event is one replay-safe workflow output record.
type Event struct {
	ID        int64
	Data      []byte
	CreatedAt time.Time
}

// Client schedules and reads workflow runs.
type Client interface {
	// CreateRun creates a root run when lease is nil, or creates/verifies a
	// child run at the leased replay position otherwise.
	CreateRun(context.Context, *LeasedRun, RunRequest) (string, error)
	// WaitRun waits until the root run completes, then returns its encoded output
	// or its terminal failure.
	WaitRun(context.Context, string) (json.RawMessage, error)
	// Events returns replay-safe output records after the supplied event ID.
	Events(context.Context, string, int64) ([]Event, error)
}

// World is the storage and dispatch protocol consumed by Engine.
type World interface {
	Client

	// EnsureWait creates or verifies a durable dependency barrier. Worlds resolve
	// the barrier when its referenced tasks become terminal. The barrier itself
	// is not executable worker work.
	EnsureWait(context.Context, *LeasedRun, WaitRequest) (Operation, error)
	// SuspendRun releases a root-run lease after its replay records unresolved
	// work. The world must requeue the root when its wait becomes terminal.
	SuspendRun(context.Context, *LeasedRun) error
	// HeartbeatRun extends a running lease. A world must fence stale tokens.
	HeartbeatRun(context.Context, *LeasedRun) error
	// CompleteRun records successful output for the leased root run or task.
	CompleteRun(context.Context, *LeasedRun, json.RawMessage) error
	// FailRun records terminal failure for the leased root run or task.
	FailRun(context.Context, *LeasedRun, error) error
	// LeaseDuration returns the maximum interval between worker heartbeats.
	LeaseDuration() time.Duration
	// EnsureOperation creates or verifies one deterministic replay command.
	EnsureOperation(context.Context, *LeasedRun, OperationRequest) (Operation, error)
	// FinishOperation records output or failure for a leased operation. The
	// implementation must reject a stale lease token.
	FinishOperation(context.Context, Operation, json.RawMessage, error) error
	// WaitOperation waits for a durable operation to reach a terminal state.
	WaitOperation(context.Context, Task) (json.RawMessage, error)
	// AppendEvent persists one idempotent replay-safe output event.
	AppendEvent(context.Context, Task, []byte) error
}

// Task identifies a durable workflow command. RootSequence identifies a root
// run. Non-negative sequences identify operations within that run.
type Task struct {
	RunID    string `json:"runId"`
	Sequence int    `json:"sequence"`
}

// RunRequest creates a root workflow run or records a child workflow run at a
// parent replay position. Kind must be KindRun.
type RunRequest struct {
	RunID       string          `json:"runId,omitempty"`
	Identity    string          `json:"identity"`
	Fingerprint string          `json:"fingerprint,omitempty"`
	Input       json.RawMessage `json:"input"`
	Sequence    int             `json:"sequence"`
	Kind        Kind            `json:"kind,omitempty"`
}

// Kind identifies the durable behavior recorded for a replay position.
type Kind string

const (
	KindRun         Kind = "run"
	KindStep        Kind = "step"
	KindSleep       Kind = "sleep"
	KindLog         Kind = "log"
	KindAwait       Kind = "await"
	KindJoin        Kind = "join"
	KindJoinSettled Kind = "join-settled"
	KindRace        Kind = "race"
)

// IsWaitKind reports whether kind is a dependency-barrier kind that a world can
// resolve without executing it as worker work.
func IsWaitKind(kind Kind) bool {
	switch kind {
	case KindAwait, KindJoin, KindJoinSettled, KindRace:
		return true
	default:
		return false
	}
}

// Status identifies the persisted state of a durable operation.
type Status string

const (
	StatusQueued    Status = "queued"
	StatusRunning   Status = "running"
	StatusRetryWait Status = "retry_wait"
	StatusWaiting   Status = "waiting"
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
	StatusCancelled Status = "cancelled"
)

// LeasedRun is a World-issued work lease protected by Token.
type LeasedRun struct {
	ID       string
	Identity string
	Token    string
	Input    json.RawMessage
	Sequence int
}

// OperationRequest is one deterministic replay position.
type OperationRequest struct {
	RunID       string
	Sequence    int
	Kind        Kind
	Identity    string
	Fingerprint string
	Input       json.RawMessage
	DueAt       *time.Time
}

// Operation is the persisted state of one replay position.
type Operation struct {
	RunID          string
	Sequence       int
	Status         Status
	Token          string
	Error          string
	IdempotencyKey string
	DueAt          time.Time
	LeaseUntil     time.Time
}

// Dependency is the storage-neutral terminal state used to resolve a
// durable wait. Worlds load their own records and convert them to this type.
type Dependency struct {
	Task       Task
	Status     Status
	Output     json.RawMessage
	Error      string
	Completion uint64
	Found      bool
}

// RaceOutcome identifies the dependency selected by a race barrier.
type RaceOutcome struct {
	Index int `json:"index"`
}

// WaitRequest creates a durable barrier over Tasks.
type WaitRequest struct {
	RunID        string
	Sequence     int
	Kind         Kind
	Fingerprint  string
	Dependencies []Task
}

// WaitResolution describes a deterministic wait result. Pending means no
// terminal transition should be persisted yet.
type WaitResolution struct {
	Pending bool
	Output  json.RawMessage
	Err     error
}

// ResolveWait applies await, race, join, and join-settled semantics to durable
// dependency states without depending on a storage implementation.
func ResolveWait(kind Kind, dependencies []Dependency) WaitResolution {
	terminal := func(value Dependency) bool {
		return value.Found && (value.Status == StatusCompleted || value.Status == StatusFailed)
	}
	failure := func(value Dependency) error {
		if !value.Found {
			return ErrRunNotFound
		}
		if value.Error == "" {
			return errors.New("workflow: dependency failed")
		}
		return errors.New(value.Error)
	}
	if kind == KindAwait {
		if len(dependencies) != 1 {
			return WaitResolution{Err: errors.New("workflow: await requires one dependency")}
		}
		dependency := dependencies[0]
		if !dependency.Found {
			return WaitResolution{Err: ErrRunNotFound}
		}
		if !terminal(dependency) {
			return WaitResolution{Pending: true}
		}
		if dependency.Status == StatusFailed {
			return WaitResolution{Err: failure(dependency)}
		}
		return WaitResolution{Output: append(json.RawMessage(nil), dependency.Output...)}
	}
	if kind == KindRace {
		winner := -1
		for index, dependency := range dependencies {
			if !dependency.Found {
				return WaitResolution{Err: ErrRunNotFound}
			}
			if !terminal(dependency) || (winner >= 0 && dependency.Completion >= dependencies[winner].Completion) {
				continue
			}
			winner = index
		}
		if winner < 0 {
			return WaitResolution{Pending: true}
		}
		output, _ := json.Marshal(RaceOutcome{Index: winner})
		return WaitResolution{Output: output}
	}

	settled := kind == KindJoinSettled
	firstFailure := -1
	for index, dependency := range dependencies {
		if !dependency.Found {
			if settled {
				return WaitResolution{Err: ErrRunNotFound}
			}
			if firstFailure < 0 {
				firstFailure = index
			}
			continue
		}
		if !terminal(dependency) {
			if settled {
				return WaitResolution{Pending: true}
			}
			continue
		}
		if !settled && dependency.Status == StatusFailed && (firstFailure < 0 || dependency.Completion < dependencies[firstFailure].Completion) {
			firstFailure = index
		}
	}
	if firstFailure >= 0 {
		return WaitResolution{Err: failure(dependencies[firstFailure])}
	}
	if !settled {
		for _, dependency := range dependencies {
			if !terminal(dependency) {
				return WaitResolution{Pending: true}
			}
		}
	}
	output, _ := json.Marshal(struct{}{})
	return WaitResolution{Output: output}
}

// NewRunID creates a collision-resistant durable workflow ID.
func NewRunID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return "run_" + hex.EncodeToString(value[:]), nil
}

func HistoryMismatchError(kind string, sequence int) error {
	return fmt.Errorf("workflow: replay at %s %d no longer matches recorded history", kind, sequence)
}

func HistoryMissingError(kind string, sequence int) error {
	return fmt.Errorf("workflow: history mismatch: %s %d is absent", kind, sequence)
}

// CommandMetadata is the storage-neutral, deterministic portion of a command.
type CommandMetadata struct {
	Kind        Kind
	Identity    string
	Fingerprint string
	Input       json.RawMessage
	ChildRunID  string
}

func (command CommandMetadata) MatchesOperation(request OperationRequest) bool {
	return command.Kind == request.Kind && command.Identity == request.Identity && command.Fingerprint == request.Fingerprint && bytes.Equal(command.Input, request.Input)
}

func (command CommandMetadata) MatchesRun(request RunRequest) bool {
	return command.Kind == KindRun && command.Identity == request.Identity && command.Fingerprint == request.Fingerprint && bytes.Equal(command.Input, request.Input)
}

func (command CommandMetadata) MatchesWait(request WaitRequest, input json.RawMessage) bool {
	return command.Kind == request.Kind && command.Identity == string(request.Kind) && command.Fingerprint == request.Fingerprint && bytes.Equal(command.Input, input)
}
