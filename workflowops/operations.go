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
	CreateRun(context.Context, *Lease, RunRequest) (string, error)
	// CancelOperations durably cancels unfinished tasks. Cancelling an already
	// cancelled task is a successful no-op.
	CancelOperations(context.Context, ...Task) error
	// WaitRun waits until the root run completes, then returns its encoded output
	// or its terminal failure.
	WaitRun(context.Context, string) (json.RawMessage, error)
	// Events returns replay-safe output records after the supplied event ID.
	Events(context.Context, string, int64) ([]Event, error)
}

// World is the storage and dispatch protocol consumed by Engine.
type World interface {
	Client

	// CleanupTimer removes a provider-managed timer after its durable task has
	// been cancelled. Worlds without a removable timer resource return nil.
	CleanupTimer(context.Context, Task) error
	// EnsureOperation creates or verifies one deterministic replay command.
	EnsureOperation(context.Context, *Lease, OperationRequest) (Operation, error)
	// RecordOperation creates a completed replay command. On a repeated call,
	// it validates request against the stored command, does not overwrite the
	// persisted output, and returns it.
	RecordOperation(context.Context, *Lease, OperationRequest, json.RawMessage) (json.RawMessage, error)
	// EnsureWait creates or verifies a durable dependency barrier. Worlds resolve
	// the barrier when its referenced tasks become terminal. The barrier itself
	// is not executable worker work.
	EnsureWait(context.Context, *Lease, WaitRequest) (Operation, error)
	// SuspendRun releases a root-run lease after its replay records unresolved
	// work. The world must requeue the root when its wait becomes terminal.
	SuspendRun(context.Context, *Lease) error
	// LeaseDuration returns the maximum interval between worker heartbeats.
	LeaseDuration() time.Duration
	// HeartbeatRun extends a running lease. A world must fence stale tokens.
	HeartbeatRun(context.Context, *Lease) error
	// CompleteRun records successful output for the leased root run or task.
	CompleteRun(context.Context, *Lease, json.RawMessage) error
	// FailRun records terminal failure for the leased root run or task.
	FailRun(context.Context, *Lease, error) error
	// FinishOperation records output or failure for a leased operation. The
	// implementation must reject a stale lease token.
	FinishOperation(context.Context, Operation, json.RawMessage, error) error
	// RetryOperation records a failed attempt and releases it for a later
	// durable claim. The implementation must reject a stale lease token.
	RetryOperation(context.Context, Operation, error, time.Time) error
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
	KindNow         Kind = "now"
	KindSleep       Kind = "sleep"
	KindTimeout     Kind = "timeout"
	KindCancel      Kind = "cancel"
	KindLog         Kind = "log"
	KindAwait       Kind = "await"
	KindJoin        Kind = "join"
	KindJoinSettled Kind = "join-settled"
	KindSelect      Kind = "select"
	KindSelectOk    Kind = "select-ok"
)

// IsWaitKind reports whether kind is a dependency-barrier kind that a world can
// resolve without executing it as worker work.
func IsWaitKind(kind Kind) bool {
	switch kind {
	case KindAwait, KindJoin, KindJoinSettled, KindSelect, KindSelectOk:
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

// Lease is a World-issued work lease protected by Token.
type Lease struct {
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
	RunID      string
	StepID     string
	Sequence   int
	Status     Status
	Token      string
	Error      string
	DueAt      time.Time
	LeaseUntil time.Time
	Attempt    int
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

// SelectOutcome identifies the dependency selected by a select barrier.
type SelectOutcome struct {
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

// ResolveWait applies await, select, join, and join-settled semantics to durable
// dependency states without depending on a storage implementation.
func ResolveWait(kind Kind, dependencies []Dependency) WaitResolution {
	terminal := func(value Dependency) bool {
		return value.Found && (value.Status == StatusCompleted ||
			value.Status == StatusFailed ||
			value.Status == StatusCancelled)
	}
	failure := func(value Dependency) error {
		if !value.Found {
			return ErrRunNotFound
		}
		if value.Status == StatusCancelled {
			return ErrCancelled
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
		if dependency.Status != StatusCompleted {
			return WaitResolution{Err: failure(dependency)}
		}
		return WaitResolution{Output: append(json.RawMessage(nil), dependency.Output...)}
	}
	if kind == KindSelect {
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
		output, _ := json.Marshal(SelectOutcome{Index: winner})
		return WaitResolution{Output: output}
	}
	if kind == KindSelectOk {
		if len(dependencies) == 0 {
			return WaitResolution{Err: errors.New("workflow: select-ok requires a dependency")}
		}
		winner, firstFailure := -1, -1
		for index, dependency := range dependencies {
			if !dependency.Found {
				return WaitResolution{Err: ErrRunNotFound}
			}
			if dependency.Status == StatusCompleted &&
				(winner < 0 || dependency.Completion < dependencies[winner].Completion) {
				winner = index
				continue
			}
			if !terminal(dependency) {
				continue
			}
			if firstFailure < 0 || dependency.Completion < dependencies[firstFailure].Completion {
				firstFailure = index
			}
		}
		if winner >= 0 {
			output, _ := json.Marshal(SelectOutcome{Index: winner})
			return WaitResolution{Output: output}
		}
		for _, dependency := range dependencies {
			if !terminal(dependency) {
				return WaitResolution{Pending: true}
			}
		}
		return WaitResolution{Err: failure(dependencies[firstFailure])}
	}

	settled := kind == KindJoinSettled
	firstFailure := -1
	pending := false
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
			pending = true
			continue
		}
		if !settled && dependency.Status != StatusCompleted && (firstFailure < 0 || dependency.Completion < dependencies[firstFailure].Completion) {
			firstFailure = index
		}
	}
	if firstFailure >= 0 {
		return WaitResolution{Err: failure(dependencies[firstFailure])}
	}
	if pending {
		return WaitResolution{Pending: true}
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
	return fmt.Errorf("workflow: history mismatch: replay at %s %d no longer matches recorded history", kind, sequence)
}

func HistoryMissingError(kind string, sequence int) error {
	return fmt.Errorf("workflow: history mismatch: %s %d is absent", kind, sequence)
}

// MatchesOperation reports whether persisted command fields match an operation
// replay request.
func MatchesOperation(kind Kind, identity, fingerprint string, input json.RawMessage, request OperationRequest) bool {
	return kind == request.Kind &&
		identity == request.Identity &&
		fingerprint == request.Fingerprint &&
		bytes.Equal(input, request.Input)
}

// MatchesRun reports whether persisted command fields match a child-run replay
// request. childRunID is deliberately not part of the request comparison.
func MatchesRun(kind Kind, identity, fingerprint string, input json.RawMessage, request RunRequest) bool {
	return kind == KindRun &&
		identity == request.Identity &&
		fingerprint == request.Fingerprint &&
		bytes.Equal(input, request.Input)
}

// MatchesWait reports whether persisted command fields match a durable wait
// request. encodedDependencies is the storage encoding of request.Dependencies.
func MatchesWait(kind Kind, identity, fingerprint string, input json.RawMessage, request WaitRequest, encodedDependencies json.RawMessage) bool {
	return kind == request.Kind &&
		identity == string(request.Kind) &&
		fingerprint == request.Fingerprint &&
		bytes.Equal(input, encodedDependencies)
}
