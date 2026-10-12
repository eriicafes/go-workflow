// Package workflowtest provides an in-memory World for workflow tests.
package workflowtest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/workflowops"
)

// World is an in-memory workflow World intended for tests. It starts work in
// goroutines and uses time.Sleep to defer timer tasks. Tests therefore use the
// ordinary Go clock instead of a simulated clock.
type World struct {
	mu         sync.Mutex
	ctx        context.Context
	cancel     context.CancelFunc
	engine     *workflowops.Engine
	runs       map[string]*run
	commands   map[string]map[int]*command
	streams    map[workflowops.Stream]*stream
	hooks      map[string]*hook
	changes    chan struct{}
	nextID     uint64
	completion uint64
	closed     bool
}

var _ workflow.World = (*World)(nil)

// CleanupTimer is a no-op because test timers are goroutines that check the
// task's durable state before running.
func (*World) CleanupTimer(context.Context, workflowops.Task) error {
	return nil
}

type run struct {
	id         string
	identity   string
	input      json.RawMessage
	startedAt  time.Time
	status     workflowops.Status
	output     json.RawMessage
	err        string
	token      string
	completion uint64
}

type command struct {
	sequence     int
	kind         workflowops.Kind
	identity     string
	fingerprint  string
	input        json.RawMessage
	dependencies []workflowops.Task
	childID      string
	status       workflowops.Status
	output       json.RawMessage
	err          string
	dueAt        time.Time
	token        string
	completion   uint64
	attempt      int
}

type stream struct {
	data   []byte
	closed bool
	lock   string
}

type hook struct {
	task      workflowops.Task
	retention time.Duration
	expiresAt time.Time
	webhook   *workflowops.Webhook
	response  json.RawMessage
}

// New creates an immediately usable in-memory World. It executes runners in
// goroutines when root runs and durable operations are scheduled.
func New(runners ...workflowops.Runner) *World {
	set := workflow.Runners(runners...)
	ctx, cancel := context.WithCancel(context.Background())
	world := &World{
		ctx:      ctx,
		cancel:   cancel,
		runs:     make(map[string]*run),
		commands: make(map[string]map[int]*command),
		streams:  make(map[workflowops.Stream]*stream),
		hooks:    make(map[string]*hook),
		changes:  make(chan struct{}),
	}
	world.engine = workflowops.NewEngine(world, set)
	return world
}

// Close stops future work. Scheduled goroutines which are already sleeping
// return without executing when they wake.
func (world *World) Close() {
	world.mu.Lock()
	if world.closed {
		world.mu.Unlock()
		return
	}
	world.closed = true
	world.cancel()
	world.signalLocked()
	world.mu.Unlock()
}

func (world *World) createRoot(ctx context.Context, request workflowops.RunRequest) (string, error) {
	if request.Sequence != workflowops.RootSequence {
		return "", fmt.Errorf("workflowtest: invalid root sequence %d", request.Sequence)
	}
	if request.Kind != "" && request.Kind != workflowops.KindRun {
		return "", fmt.Errorf("workflowtest: invalid root kind %q", request.Kind)
	}
	if request.Fingerprint == "" {
		request.Fingerprint = request.Identity
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}

	world.mu.Lock()
	if world.closed {
		world.mu.Unlock()
		return "", errors.New("workflowtest: world is closed")
	}
	id := world.idLocked()
	world.runs[id] = &run{
		id:        id,
		identity:  request.Identity,
		input:     clone(request.Input),
		startedAt: time.Now(),
		status:    workflowops.StatusQueued,
	}
	world.signalLocked()
	world.mu.Unlock()

	world.schedule(workflowops.Task{RunID: id, Sequence: workflowops.RootSequence}, time.Time{})
	return id, nil
}

// GetRun returns durable metadata for one in-memory run.
func (world *World) GetRun(_ context.Context, id string) (workflowops.RunInfo, bool, error) {
	world.mu.Lock()
	defer world.mu.Unlock()
	run := world.runs[id]
	if run == nil {
		return workflowops.RunInfo{}, false, nil
	}
	return workflowops.RunInfo{Identity: run.identity, Status: run.status, StartedAt: run.startedAt}, true, nil
}

// Wake makes every pending sleep in a run immediately eligible to execute.
func (world *World) Wake(_ context.Context, id string) (int, error) {
	world.mu.Lock()
	if world.runs[id] == nil {
		world.mu.Unlock()
		return 0, workflow.ErrRunNotFound
	}
	now := time.Now()
	var tasks []workflowops.Task
	for sequence, command := range world.commands[id] {
		if command.kind != workflowops.KindSleep || command.status != workflowops.StatusQueued || !command.dueAt.After(now) {
			continue
		}
		command.dueAt = now
		tasks = append(tasks, workflowops.Task{RunID: id, Sequence: sequence})
	}
	if len(tasks) != 0 {
		world.signalLocked()
	}
	world.mu.Unlock()
	world.scheduleAll(tasks)
	return len(tasks), nil
}

// WaitRun waits for a root run to complete.
func (world *World) WaitRun(ctx context.Context, id string) (json.RawMessage, error) {
	for {
		world.mu.Lock()
		run, found := world.runs[id]
		if !found {
			world.mu.Unlock()
			return nil, workflow.ErrRunNotFound
		}
		switch run.status {
		case workflowops.StatusCompleted:
			output := clone(run.output)
			world.mu.Unlock()
			return output, nil
		case workflowops.StatusFailed:
			err := workflowops.DecodeFailure(run.err)
			world.mu.Unlock()
			return nil, err
		case workflowops.StatusCancelled:
			world.mu.Unlock()
			return nil, workflowops.ErrCancelled
		}
		changes := world.changes
		world.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changes:
		}
	}
}

func (world *World) ReadStream(_ context.Context, reference workflowops.Stream, offset int64) (workflowops.StreamRead, error) {
	world.mu.Lock()
	defer world.mu.Unlock()
	if _, found := world.runs[reference.RunID]; !found {
		return workflowops.StreamRead{}, workflow.ErrRunNotFound
	}
	value := world.streams[reference]
	if value == nil {
		return workflowops.StreamRead{Next: offset}, nil
	}
	if offset < 0 {
		offset = max(int64(len(value.data))+offset, 0)
	}
	if offset > int64(len(value.data)) {
		offset = int64(len(value.data))
	}
	return workflowops.StreamRead{
		Data:   clone(value.data[offset:]),
		Next:   int64(len(value.data)),
		Closed: value.closed,
	}, nil
}

func (world *World) LockStream(_ context.Context, reference workflowops.Stream, lock string) (bool, error) {
	world.mu.Lock()
	defer world.mu.Unlock()
	if _, found := world.runs[reference.RunID]; !found {
		return false, workflow.ErrRunNotFound
	}
	value := world.streams[reference]
	if value == nil {
		value = &stream{}
		world.streams[reference] = value
	}
	if value.closed {
		return false, workflowops.ErrCancelled
	}
	if value.lock != "" {
		return false, nil
	}
	value.lock = lock
	world.signalLocked()
	return true, nil
}

func (world *World) AppendStream(_ context.Context, reference workflowops.Stream, lock string, data []byte) error {
	world.mu.Lock()
	defer world.mu.Unlock()
	value := world.streams[reference]
	if _, found := world.runs[reference.RunID]; !found {
		return workflow.ErrRunNotFound
	}
	if value == nil {
		value = &stream{}
		world.streams[reference] = value
	}
	if value.closed || value.lock != lock {
		return workflowops.ErrCancelled
	}
	value.data = append(value.data, data...)
	world.signalLocked()
	return nil
}

func (world *World) ReleaseStream(_ context.Context, reference workflowops.Stream, lock string, closeStream bool) error {
	world.mu.Lock()
	defer world.mu.Unlock()
	value := world.streams[reference]
	if _, found := world.runs[reference.RunID]; !found {
		return workflow.ErrRunNotFound
	}
	if value == nil || value.lock != lock {
		return workflowops.ErrCancelled
	}
	value.closed = value.closed || closeStream
	value.lock = ""
	world.signalLocked()
	return nil
}

func (world *World) CloseStreams(_ context.Context, runID string) error {
	world.mu.Lock()
	defer world.mu.Unlock()
	for reference, value := range world.streams {
		if reference.RunID == runID {
			value.closed = true
			value.lock = ""
		}
	}
	world.signalLocked()
	return nil
}

// CreateRun records and schedules a root or nested workflow run.
func (world *World) CreateRun(ctx context.Context, lease *workflowops.Lease, request workflowops.RunRequest) (string, error) {
	if lease == nil {
		return world.createRoot(ctx, request)
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}

	world.mu.Lock()
	if err := world.openRunLocked(request.RunID); err != nil {
		world.mu.Unlock()
		return "", err
	}
	commands := world.commandsForLocked(request.RunID)
	if existing, found := commands[request.Sequence]; found {
		if !matchesRun(existing, request) || existing.childID == "" {
			world.mu.Unlock()
			return "", workflowops.HistoryMismatchError("nested run", request.Sequence)
		}
		id := existing.childID
		world.mu.Unlock()
		return id, nil
	}
	if !allowsCreate(lease, request.RunID) {
		world.mu.Unlock()
		return "", workflowops.HistoryMissingError("nested run command", request.Sequence)
	}

	childID := world.idLocked()
	commands[request.Sequence] = &command{
		sequence:    request.Sequence,
		kind:        workflowops.KindRun,
		identity:    request.Identity,
		fingerprint: request.Fingerprint,
		input:       clone(request.Input),
		childID:     childID,
		status:      workflowops.StatusCompleted,
		completion:  world.nextCompletionLocked(),
	}
	world.runs[childID] = &run{
		id:        childID,
		identity:  request.Identity,
		input:     clone(request.Input),
		startedAt: time.Now(),
		status:    workflowops.StatusQueued,
	}
	world.signalLocked()
	world.mu.Unlock()

	world.schedule(workflowops.Task{RunID: childID, Sequence: workflowops.RootSequence}, time.Time{})
	return childID, nil
}

// EnsureOperation creates or validates a durable operation command.
func (world *World) EnsureOperation(ctx context.Context, lease *workflowops.Lease, request workflowops.OperationRequest) (workflowops.Operation, error) {
	if err := ctx.Err(); err != nil {
		return workflowops.Operation{}, err
	}

	world.mu.Lock()
	if err := world.openRunLocked(request.RunID); err != nil {
		world.mu.Unlock()
		return workflowops.Operation{}, err
	}
	commands := world.commandsForLocked(request.RunID)
	if existing, found := commands[request.Sequence]; found {
		if !matchesOperation(existing, request) || existing.childID != "" {
			world.mu.Unlock()
			return workflowops.Operation{}, workflowops.HistoryMismatchError("operation", request.Sequence)
		}
		op := operation(request.RunID, existing)
		world.mu.Unlock()
		return op, nil
	}
	if !allowsCreate(lease, request.RunID) {
		world.mu.Unlock()
		return workflowops.Operation{}, workflowops.HistoryMissingError("command", request.Sequence)
	}

	dueAt := time.Time{}
	if request.DueAt != nil {
		dueAt = *request.DueAt
	}
	status := workflowops.StatusQueued
	if request.Kind == workflowops.KindRollback || request.Kind == workflowops.KindHook {
		status = workflowops.StatusWaiting
	}
	command := &command{
		sequence:    request.Sequence,
		kind:        request.Kind,
		identity:    request.Identity,
		fingerprint: request.Fingerprint,
		input:       clone(request.Input),
		status:      status,
		dueAt:       dueAt,
	}
	commands[request.Sequence] = command
	op := operation(request.RunID, command)
	world.signalLocked()
	world.mu.Unlock()

	if status == workflowops.StatusQueued {
		world.schedule(workflowops.Task{RunID: request.RunID, Sequence: request.Sequence}, dueAt)
	}
	return op, nil
}

// RecordOperation creates a completed replay command. On a repeated call, it
// validates request against the stored command, does not overwrite the
// persisted output, and returns it.
func (world *World) RecordOperation(
	ctx context.Context,
	lease *workflowops.Lease,
	request workflowops.OperationRequest,
	output json.RawMessage,
) (json.RawMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !allowsCreate(lease, request.RunID) {
		return nil, workflowops.ErrLeaseLost
	}

	world.mu.Lock()
	defer world.mu.Unlock()
	run, found := world.runs[request.RunID]
	if !found || run.status != workflowops.StatusRunning || run.token != lease.Token {
		return nil, workflowops.ErrLeaseLost
	}
	commands := world.commandsForLocked(request.RunID)
	if existing, found := commands[request.Sequence]; found {
		if !matchesOperation(existing, request) ||
			existing.childID != "" ||
			existing.status != workflowops.StatusCompleted {
			return nil, workflowops.HistoryMismatchError("native operation", request.Sequence)
		}
		return clone(existing.output), nil
	}
	commands[request.Sequence] = &command{
		sequence:    request.Sequence,
		kind:        request.Kind,
		identity:    request.Identity,
		fingerprint: request.Fingerprint,
		input:       clone(request.Input),
		status:      workflowops.StatusCompleted,
		output:      clone(output),
		completion:  world.nextCompletionLocked(),
	}
	world.signalLocked()
	return clone(output), nil
}

// EnsureWait creates or validates a durable dependency barrier.
func (world *World) EnsureWait(ctx context.Context, lease *workflowops.Lease, request workflowops.WaitRequest) (workflowops.Operation, error) {
	if err := ctx.Err(); err != nil {
		return workflowops.Operation{}, err
	}

	world.mu.Lock()
	if err := world.openRunLocked(request.RunID); err != nil {
		world.mu.Unlock()
		return workflowops.Operation{}, err
	}
	commands := world.commandsForLocked(request.RunID)
	if existing, found := commands[request.Sequence]; found {
		input, err := json.Marshal(request.Dependencies)
		if err != nil || !matchesWait(existing, request, input) {
			world.mu.Unlock()
			if err != nil {
				return workflowops.Operation{}, err
			}
			return workflowops.Operation{}, workflowops.HistoryMismatchError("wait", request.Sequence)
		}
		op := operation(request.RunID, existing)
		world.mu.Unlock()
		return op, nil
	}
	if !allowsCreate(lease, request.RunID) {
		world.mu.Unlock()
		return workflowops.Operation{}, workflowops.HistoryMissingError("wait command", request.Sequence)
	}

	input, err := json.Marshal(request.Dependencies)
	if err != nil {
		world.mu.Unlock()
		return workflowops.Operation{}, err
	}
	command := &command{
		sequence:     request.Sequence,
		kind:         request.Kind,
		identity:     string(request.Kind),
		fingerprint:  request.Fingerprint,
		input:        clone(input),
		dependencies: append([]workflowops.Task(nil), request.Dependencies...),
		status:       workflowops.StatusWaiting,
	}
	commands[request.Sequence] = command
	ready := world.resolveWaitsLocked()
	op := operation(request.RunID, command)
	world.signalLocked()
	world.mu.Unlock()

	world.scheduleAll(ready)
	return op, nil
}

// SuspendRun releases a root lease while it waits on a durable barrier.
func (world *World) SuspendRun(_ context.Context, leased *workflowops.Lease) error {
	if leased.Sequence != workflowops.RootSequence {
		return workflowops.ErrLeaseLost
	}

	world.mu.Lock()
	run, found := world.runs[leased.ID]
	if !found || run.status != workflowops.StatusRunning || run.token != leased.Token {
		world.mu.Unlock()
		return workflowops.ErrLeaseLost
	}
	run.status, run.token = workflowops.StatusWaiting, ""
	ready := world.resolveWaitsLocked()
	world.signalLocked()
	world.mu.Unlock()

	world.scheduleAll(ready)
	return nil
}

// HeartbeatRun verifies that leased remains current. Test worlds do not expire
// leases while their process is running.
func (world *World) HeartbeatRun(_ context.Context, leased *workflowops.Lease) error {
	world.mu.Lock()
	defer world.mu.Unlock()
	if leased.Sequence == workflowops.RootSequence {
		run, found := world.runs[leased.ID]
		if !found {
			return workflowops.ErrLeaseLost
		}
		if run.status == workflowops.StatusCancelled {
			return workflowops.ErrCancelled
		}
		if run.status != workflowops.StatusRunning || run.token != leased.Token {
			return workflowops.ErrLeaseLost
		}
		return nil
	}
	command, found := world.commands[leased.ID][leased.Sequence]
	if !found {
		return workflowops.ErrLeaseLost
	}
	if command.status == workflowops.StatusCancelled {
		return workflowops.ErrCancelled
	}
	if command.status != workflowops.StatusRunning || command.token != leased.Token {
		return workflowops.ErrLeaseLost
	}
	return nil
}

// CompleteRun records the successful root workflow output.
func (world *World) CompleteRun(_ context.Context, leased *workflowops.Lease, output json.RawMessage) error {
	if leased.Sequence != workflowops.RootSequence {
		return workflowops.ErrLeaseLost
	}

	world.mu.Lock()
	run, found := world.runs[leased.ID]
	if !found || run.status != workflowops.StatusRunning || run.token != leased.Token {
		world.mu.Unlock()
		return workflowops.ErrLeaseLost
	}
	run.status = workflowops.StatusCompleted
	run.output = clone(output)
	run.err = ""
	run.token = ""
	run.completion = world.nextCompletionLocked()
	world.dropHooksLocked(run.id)
	ready := world.resolveWaitsLocked()
	world.signalLocked()
	world.mu.Unlock()

	world.scheduleAll(ready)
	return world.CloseStreams(context.Background(), leased.ID)
}

// FailRun records a terminal root or operation failure.
func (world *World) FailRun(_ context.Context, leased *workflowops.Lease, cause error) error {
	world.mu.Lock()
	if leased.Sequence != workflowops.RootSequence {
		err, ready := world.finishCommandLocked(leased.ID, leased.Sequence, leased.Token, nil, cause)
		if err == nil {
			world.signalLocked()
		}
		world.mu.Unlock()
		world.scheduleAll(ready)
		return err
	}
	run, found := world.runs[leased.ID]
	if !found || run.status != workflowops.StatusRunning || run.token != leased.Token {
		world.mu.Unlock()
		return workflowops.ErrLeaseLost
	}
	run.output = nil
	run.err = workflowops.EncodeFailure(cause)
	run.token = ""
	if rollback, found := world.startRollbackLocked(run); found {
		world.signalLocked()
		world.mu.Unlock()
		world.schedule(rollback, time.Time{})
		return nil
	}
	run.status = workflowops.StatusFailed
	run.completion = world.nextCompletionLocked()
	world.dropHooksLocked(run.id)
	ready := world.resolveWaitsLocked()
	world.signalLocked()
	world.mu.Unlock()

	world.scheduleAll(ready)
	return nil
}

// LeaseDuration returns a value long enough that normal unit tests do not need
// to renew their in-memory work leases.
func (*World) LeaseDuration() time.Duration { return time.Minute }

// FinishOperation records the terminal result of a leased operation.
func (world *World) FinishOperation(_ context.Context, value workflowops.Operation, output json.RawMessage, cause error) error {
	world.mu.Lock()
	err, ready := world.finishCommandLocked(value.RunID, value.Sequence, value.Token, output, cause)
	if err == nil {
		world.signalLocked()
	}
	terminal := false
	if run := world.runs[value.RunID]; run != nil {
		terminal = run.status == workflowops.StatusCompleted || run.status == workflowops.StatusFailed || run.status == workflowops.StatusCancelled
	}
	world.mu.Unlock()

	world.scheduleAll(ready)
	if err == nil && terminal {
		return world.CloseStreams(context.Background(), value.RunID)
	}
	return err
}

func (world *World) RetryOperation(_ context.Context, value workflowops.Operation, cause error, due time.Time) error {
	world.mu.Lock()
	command, found := world.commands[value.RunID][value.Sequence]
	if !found || command.status != workflowops.StatusRunning || command.token != value.Token {
		world.mu.Unlock()
		return workflowops.ErrLeaseLost
	}
	command.status = workflowops.StatusRetryWait
	command.output = nil
	command.err = workflowops.EncodeFailure(cause)
	command.token = ""
	command.dueAt = due
	world.signalLocked()
	world.mu.Unlock()
	world.schedule(workflowops.Task{RunID: value.RunID, Sequence: value.Sequence}, due)
	return nil
}

// CancelOperations marks unfinished tasks as cancelled. Repeating a cancellation is
// a successful no-op.
func (world *World) CancelOperations(_ context.Context, tasks ...workflowops.Task) error {
	world.mu.Lock()
	visited := make(map[string]bool)
	for _, task := range tasks {
		if err := world.cancelTaskLocked(task, visited); err != nil {
			world.mu.Unlock()
			return err
		}
	}
	ready := world.resolveWaitsLocked()
	world.signalLocked()
	world.mu.Unlock()
	world.scheduleAll(ready)
	return nil
}

func (world *World) cancelTaskLocked(task workflowops.Task, visited map[string]bool) error {
	if task.Sequence == workflowops.RootSequence {
		if visited[task.RunID] {
			return nil
		}
		visited[task.RunID] = true
		run, found := world.runs[task.RunID]
		if !found {
			return workflowops.ErrRunNotFound
		}
		if run.status != workflowops.StatusCompleted &&
			run.status != workflowops.StatusFailed &&
			run.status != workflowops.StatusCancelled {
			run.status = workflowops.StatusCancelled
			run.err = workflowops.ErrCancelled.Error()
			run.token = ""
			run.completion = world.nextCompletionLocked()
		}
		for sequence, command := range world.commands[task.RunID] {
			if err := world.cancelTaskLocked(
				workflowops.Task{RunID: task.RunID, Sequence: sequence},
				visited,
			); err != nil {
				return err
			}
			if command.childID != "" {
				if err := world.cancelTaskLocked(
					workflowops.Task{RunID: command.childID, Sequence: workflowops.RootSequence},
					visited,
				); err != nil {
					return err
				}
			}
		}
		return nil
	}
	command, found := world.commands[task.RunID][task.Sequence]
	if !found {
		return workflowops.ErrRunNotFound
	}
	if command.status == workflowops.StatusCompleted ||
		command.status == workflowops.StatusFailed ||
		command.status == workflowops.StatusCancelled {
		return nil
	}
	command.status = workflowops.StatusCancelled
	command.err = workflowops.ErrCancelled.Error()
	command.output = nil
	command.token = ""
	command.completion = world.nextCompletionLocked()
	return nil
}

// WaitOperation waits for a durable operation to complete.
func (world *World) WaitOperation(ctx context.Context, task workflowops.Task) (json.RawMessage, error) {
	for {
		world.mu.Lock()
		command, found := world.commands[task.RunID][task.Sequence]
		if !found {
			world.mu.Unlock()
			return nil, workflow.ErrRunNotFound
		}
		switch command.status {
		case workflowops.StatusCompleted:
			output := clone(command.output)
			world.mu.Unlock()
			return output, nil
		case workflowops.StatusFailed:
			err := workflowops.DecodeFailure(command.err)
			world.mu.Unlock()
			return nil, err
		case workflowops.StatusCancelled:
			world.mu.Unlock()
			return nil, workflowops.ErrCancelled
		}
		changes := world.changes
		world.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changes:
		}
	}
}

func (world *World) scheduleAll(tasks []workflowops.Task) {
	for _, task := range tasks {
		world.schedule(task, time.Time{})
	}
}

func (world *World) schedule(task workflowops.Task, dueAt time.Time) {
	go func() {
		if delay := time.Until(dueAt); delay > 0 {
			time.Sleep(delay)
		}
		leased, ok := world.claim(task)
		if !ok {
			return
		}
		_ = world.engine.Execute(world.ctx, leased)
	}()
}

func (world *World) claim(task workflowops.Task) (*workflowops.Lease, bool) {
	world.mu.Lock()
	defer world.mu.Unlock()
	if world.closed {
		return nil, false
	}
	run, found := world.runs[task.RunID]
	if !found {
		return nil, false
	}
	token := world.idLocked()
	if task.Sequence == workflowops.RootSequence {
		if run.status != workflowops.StatusQueued {
			return nil, false
		}
		run.status, run.token = workflowops.StatusRunning, token
		return &workflowops.Lease{
			ID:        run.id,
			Identity:  run.identity,
			Input:     clone(run.input),
			Sequence:  workflowops.RootSequence,
			Token:     token,
			StartedAt: run.startedAt,
		}, true
	}
	command, found := world.commands[task.RunID][task.Sequence]
	if !found || (command.status != workflowops.StatusQueued &&
		(command.status != workflowops.StatusRetryWait || command.dueAt.After(time.Now()))) {
		return nil, false
	}
	command.status, command.token = workflowops.StatusRunning, token
	command.attempt++
	return &workflowops.Lease{
		ID:        run.id,
		Identity:  run.identity,
		Input:     clone(run.input),
		Sequence:  task.Sequence,
		Token:     token,
		StartedAt: run.startedAt,
	}, true
}

func (world *World) finishCommandLocked(runID string, sequence int, token string, output json.RawMessage, cause error) (error, []workflowops.Task) {
	command, found := world.commands[runID][sequence]
	if !found || command.status != workflowops.StatusRunning || command.token != token {
		return workflowops.ErrLeaseLost, nil
	}
	if cause == nil {
		command.status = workflowops.StatusCompleted
		command.output = clone(output)
		command.err = ""
	} else {
		command.status = workflowops.StatusFailed
		command.output = nil
		command.err = workflowops.EncodeFailure(cause)
	}
	command.token = ""
	command.completion = world.nextCompletionLocked()
	ready := world.resolveWaitsLocked()
	if command.kind == workflowops.KindRollback {
		if rollback, found := world.advanceRollbackLocked(runID); found {
			ready = append(ready, rollback)
		}
	}
	return nil, ready
}

func (world *World) startRollbackLocked(run *run) (workflowops.Task, bool) {
	commands := world.commands[run.id]
	for sequence := len(commands) - 1; sequence >= 0; sequence-- {
		command := commands[sequence]
		if command == nil || command.kind != workflowops.KindRollback || command.status != workflowops.StatusWaiting {
			continue
		}
		command.status = workflowops.StatusQueued
		run.status = workflowops.StatusRollingBack
		return workflowops.Task{RunID: run.id, Sequence: sequence}, true
	}
	return workflowops.Task{}, false
}

func (world *World) advanceRollbackLocked(runID string) (workflowops.Task, bool) {
	run := world.runs[runID]
	if run == nil || run.status != workflowops.StatusRollingBack {
		return workflowops.Task{}, false
	}
	if task, found := world.startRollbackLocked(run); found {
		return task, true
	}
	run.status = workflowops.StatusFailed
	run.completion = world.nextCompletionLocked()
	world.dropHooksLocked(run.id)
	return workflowops.Task{}, false
}

func (world *World) resolveWaitsLocked() []workflowops.Task {
	var ready []workflowops.Task
	for changed := true; changed; {
		changed = false
		for runID, commands := range world.commands {
			for _, command := range commands {
				if command.status != workflowops.StatusWaiting || !workflowops.IsWaitKind(command.kind) {
					continue
				}
				states := make([]workflowops.Dependency, len(command.dependencies))
				for index, dependency := range command.dependencies {
					states[index] = world.dependencyStateLocked(dependency)
				}
				resolution := workflowops.ResolveWait(command.kind, states)
				if resolution.Pending {
					continue
				}
				if resolution.Err == nil {
					command.status = workflowops.StatusCompleted
					command.output = resolution.Output
					command.err = ""
				} else {
					command.status = workflowops.StatusFailed
					command.output = nil
					command.err = workflowops.EncodeFailure(resolution.Err)
				}
				command.completion = world.nextCompletionLocked()
				if run := world.runs[runID]; run.status == workflowops.StatusWaiting {
					run.status = workflowops.StatusQueued
					ready = append(ready, workflowops.Task{RunID: runID, Sequence: workflowops.RootSequence})
				}
				changed = true
			}
		}
	}
	return ready
}

func (world *World) dependencyStateLocked(task workflowops.Task) workflowops.Dependency {
	if task.Sequence == workflowops.RootSequence {
		run, found := world.runs[task.RunID]
		if !found {
			return workflowops.Dependency{Task: task}
		}
		return workflowops.Dependency{
			Task: task, Found: true, Status: run.status, Output: clone(run.output), Error: run.err, Completion: run.completion,
		}
	}
	command, found := world.commands[task.RunID][task.Sequence]
	if !found {
		return workflowops.Dependency{Task: task}
	}
	return workflowops.Dependency{
		Task: task, Found: true, Status: command.status, Output: clone(command.output), Error: command.err, Completion: command.completion,
	}
}

func (world *World) openRunLocked(runID string) error {
	if world.closed {
		return errors.New("workflowtest: world is closed")
	}
	if _, found := world.runs[runID]; !found {
		return workflow.ErrRunNotFound
	}
	return nil
}

func (world *World) commandsForLocked(runID string) map[int]*command {
	commands := world.commands[runID]
	if commands == nil {
		commands = make(map[int]*command)
		world.commands[runID] = commands
	}
	return commands
}

func (world *World) idLocked() string {
	world.nextID++
	return strconv.FormatUint(world.nextID, 36)
}

func (world *World) nextCompletionLocked() uint64 {
	world.completion++
	return world.completion
}

func (world *World) signalLocked() {
	close(world.changes)
	world.changes = make(chan struct{})
}

func operation(runID string, command *command) workflowops.Operation {
	return workflowops.Operation{
		RunID:    runID,
		StepID:   runID + ":" + strconv.Itoa(command.sequence),
		Sequence: command.sequence,
		Status:   command.status,
		Token:    command.token,
		Error:    command.err,
		DueAt:    command.dueAt,
		Attempt:  command.attempt,
	}
}

func allowsCreate(lease *workflowops.Lease, runID string) bool {
	return lease != nil && lease.ID == runID && lease.Sequence == workflowops.RootSequence
}

func matchesOperation(command *command, request workflowops.OperationRequest) bool {
	return workflowops.MatchesOperation(
		command.kind,
		command.identity,
		command.fingerprint,
		command.input,
		request,
	)
}

func matchesRun(command *command, request workflowops.RunRequest) bool {
	return workflowops.MatchesRun(
		command.kind,
		command.identity,
		command.fingerprint,
		command.input,
		request,
	)
}

func matchesWait(command *command, request workflowops.WaitRequest, input json.RawMessage) bool {
	return workflowops.MatchesWait(
		command.kind,
		command.identity,
		command.fingerprint,
		command.input,
		request,
		input,
	)
}

func clone[T ~[]byte](value T) T {
	return append(T(nil), value...)
}
