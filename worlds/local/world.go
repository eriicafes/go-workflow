// Package localworld provides a durable, single-process World for local
// workflow development.
package localworld

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"sort"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/workflowops"
)

const storeVersion = 1

var (
	ErrNotStarted = errors.New("workflow/local: world is not started")
	ErrInUse      = errors.New("workflow/local: storage is already in use")
)

// World stores authoritative workflow state as JSON records under root. It
// supports one process per root. Open owns its root. New borrows a root
// supplied by its caller.
type World struct {
	mu        sync.RWMutex
	root      *os.Root
	ownedRoot bool
	lock      *os.File
	ctx       context.Context
	cancel    context.CancelFunc
	engine    *workflowops.Engine
	started   bool
	closed    bool
	changes   chan struct{}
	workers   sync.WaitGroup
	records   sync.Mutex // serializes read-modify-write transitions within this process
}

type versionRecord struct {
	Version    int    `json:"version"`
	Completion uint64 `json:"completion"`
}

type runRecord struct {
	ID         string             `json:"id"`
	Identity   string             `json:"identity"`
	Input      json.RawMessage    `json:"input"`
	Status     workflowops.Status `json:"status"`
	Output     json.RawMessage    `json:"output,omitempty"`
	Error      string             `json:"error,omitempty"`
	Token      string             `json:"token,omitempty"`
	LeaseUntil time.Time          `json:"leaseUntil,omitempty"`
	Completion uint64             `json:"completion,omitempty"`
}

type commandRecord struct {
	Sequence       int                `json:"sequence"`
	Kind           workflowops.Kind   `json:"kind"`
	Identity       string             `json:"identity"`
	Fingerprint    string             `json:"fingerprint"`
	Input          json.RawMessage    `json:"input"`
	Dependencies   []workflowops.Task `json:"dependencies,omitempty"`
	ChildID        string             `json:"childId,omitempty"`
	Status         workflowops.Status `json:"status"`
	Output         json.RawMessage    `json:"output,omitempty"`
	Error          string             `json:"error,omitempty"`
	DueAt          time.Time          `json:"dueAt,omitempty"`
	Token          string             `json:"token,omitempty"`
	LeaseUntil     time.Time          `json:"leaseUntil,omitempty"`
	Completion     uint64             `json:"completion,omitempty"`
	IdempotencyKey string             `json:"idempotencyKey"`
}

type eventRecord struct {
	ID        int64     `json:"id"`
	Data      []byte    `json:"data"`
	CreatedAt time.Time `json:"createdAt"`
}

// Open creates path with private permissions and returns a World which owns it.
// runners are executed by the world's engine.
func Open(dir string, runners workflowops.RunnerSet) (*World, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("workflow/local: create storage: %w", err)
	}
	if err := os.Chmod(dir, 0700); err != nil {
		return nil, fmt.Errorf("workflow/local: protect storage: %w", err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, fmt.Errorf("workflow/local: open storage: %w", err)
	}
	world, err := New(root, runners)
	if err != nil {
		_ = root.Close()
		return nil, err
	}
	world.ownedRoot = true
	return world, nil
}

// New creates a World which borrows root. The caller remains responsible for closing root.
// runners are executed by the world's engine.
func New(root *os.Root, runners workflowops.RunnerSet) (*World, error) {
	if root == nil {
		return nil, errors.New("workflow/local: nil storage root")
	}
	world := &World{root: root, changes: make(chan struct{})}
	world.engine = workflowops.NewEngine(world, runners)
	return world, nil
}

// Start acquires the exclusive storage lock, validates and recovers persisted work.
func (world *World) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	world.mu.Lock()
	if world.started {
		world.mu.Unlock()
		return nil
	}
	if world.closed {
		world.mu.Unlock()
		return errors.New("workflow/local: world is closed")
	}
	if err := world.acquireLock(); err != nil {
		world.mu.Unlock()
		return err
	}
	if err := world.validateVersion(); err != nil {
		world.releaseLock()
		world.mu.Unlock()
		return err
	}
	world.ctx, world.cancel = context.WithCancel(ctx)
	world.started = true
	world.mu.Unlock()
	if err := world.recover(); err != nil {
		_ = world.Close()
		return err
	}
	world.workers.Add(1)
	go world.scheduler()
	world.signal()
	return nil
}

// Close stops dispatch, releases the exclusive lock, and closes roots opened by Open.
func (world *World) Close() error {
	world.mu.Lock()
	if world.closed {
		world.mu.Unlock()
		return nil
	}
	world.closed = true
	if world.cancel != nil {
		world.cancel()
	}
	world.signalLocked()
	world.mu.Unlock()
	world.workers.Wait()
	world.mu.Lock()
	world.started = false
	world.releaseLock()
	owned, root := world.ownedRoot, world.root
	world.mu.Unlock()
	if owned {
		return root.Close()
	}
	return nil
}

// Clear stops dispatch, removes persisted state while this world holds its lock,
// and closes the world. Open a new World before submitting further work.
func (world *World) Clear(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	world.mu.Lock()
	if !world.started || world.lock == nil || world.closed {
		world.mu.Unlock()
		return ErrNotStarted
	}
	world.cancel()
	world.signalLocked()
	world.mu.Unlock()
	world.workers.Wait()
	world.records.Lock()
	defer world.records.Unlock()
	for _, name := range []string{"runs", "commands", "events", "version.json"} {
		if err := world.root.RemoveAll(name); err != nil {
			return err
		}
	}
	if err := writeJSON(world.root, "version.json", versionRecord{Version: storeVersion}); err != nil {
		return err
	}
	world.mu.Lock()
	world.started, world.closed = false, true
	world.releaseLock()
	owned, root := world.ownedRoot, world.root
	world.mu.Unlock()
	if owned {
		return root.Close()
	}
	return nil
}

func (world *World) mustStart() error {
	world.mu.RLock()
	defer world.mu.RUnlock()
	if !world.started || world.closed {
		return ErrNotStarted
	}
	return nil
}

func (world *World) createRoot(ctx context.Context, request workflowops.RunRequest) (string, error) {
	if request.Sequence != workflowops.RootSequence {
		return "", fmt.Errorf("workflow/local: invalid root sequence %d", request.Sequence)
	}
	if request.Kind != "" && request.Kind != workflowops.KindRun {
		return "", fmt.Errorf("workflow/local: invalid root kind %q", request.Kind)
	}
	if request.Fingerprint == "" {
		request.Fingerprint = request.Identity
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := world.mustStart(); err != nil {
		return "", err
	}
	world.records.Lock()
	defer world.records.Unlock()
	id, err := workflowops.NewRunID()
	if err != nil {
		return "", err
	}
	run := runRecord{
		ID:       id,
		Identity: request.Identity,
		Input:    clone(request.Input),
		Status:   workflowops.StatusQueued,
	}
	if err := writeJSON(world.root, runName(id), run); err != nil {
		return "", err
	}
	world.launch(id, workflowops.RootSequence)
	world.signal()
	return id, nil
}

// WaitRun always reads durable state. Notifications only avoid needless rereads.
func (world *World) WaitRun(ctx context.Context, id string) (json.RawMessage, error) {
	for {
		record, err := world.readRun(id)
		if err != nil {
			return nil, err
		}
		switch record.Status {
		case workflowops.StatusCompleted:
			return clone(record.Output), nil
		case workflowops.StatusFailed:
			return nil, errors.New(record.Error)
		}
		change := world.changeChannel()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-change:
		}
	}
}

func (world *World) Events(_ context.Context, id string, after int64) ([]workflow.Event, error) {
	if _, err := world.readRun(id); err != nil {
		return nil, err
	}
	names, err := listJSON(world.root, eventDir(id))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	events := make([]workflow.Event, 0, len(names))
	for _, name := range names {
		var event eventRecord
		if err := readJSON(world.root, path.Join(eventDir(id), name), &event); err != nil {
			return nil, err
		}
		if event.ID > after {
			events = append(events, workflow.Event{ID: event.ID, Data: clone(event.Data), CreatedAt: event.CreatedAt})
		}
	}
	sort.Slice(events, func(i, j int) bool { return events[i].ID < events[j].ID })
	return events, nil
}

// CreateRun writes a child before writing its parent command reference.
func (world *World) CreateRun(
	ctx context.Context,
	lease *workflowops.LeasedRun,
	request workflowops.RunRequest,
) (string, error) {
	if lease == nil {
		return world.createRoot(ctx, request)
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := world.mustStart(); err != nil {
		return "", err
	}
	world.records.Lock()
	defer world.records.Unlock()
	command, found, err := world.readCommand(request.RunID, request.Sequence)
	if err != nil {
		return "", err
	}
	if found {
		if !commandMetadata(command).MatchesRun(request) || command.ChildID == "" {
			return "", workflowops.HistoryMismatchError("nested run", request.Sequence)
		}
		return command.ChildID, nil
	}
	if !allowsCreate(lease, request.RunID) {
		return "", workflowops.HistoryMissingError("nested run command", request.Sequence)
	}
	child, err := workflowops.NewRunID()
	if err != nil {
		return "", err
	}
	run := runRecord{
		ID:       child,
		Identity: request.Identity,
		Input:    clone(request.Input),
		Status:   workflowops.StatusQueued,
	}
	if err := writeJSON(world.root, runName(child), run); err != nil {
		return "", err
	}
	command = commandRecord{
		Sequence:       request.Sequence,
		Kind:           workflowops.KindRun,
		Identity:       request.Identity,
		Fingerprint:    request.Fingerprint,
		Input:          clone(request.Input),
		ChildID:        child,
		Status:         workflowops.StatusCompleted,
		IdempotencyKey: request.RunID + ":" + strconv.Itoa(request.Sequence),
	}
	if err := writeJSON(world.root, commandName(request.RunID, request.Sequence), command); err != nil {
		return "", err
	}
	world.launch(child, workflowops.RootSequence)
	world.signal()
	return child, nil
}

func (*World) LeaseDuration() time.Duration { return 5 * time.Minute }

func (world *World) HeartbeatRun(_ context.Context, leased *workflowops.LeasedRun) error {
	world.records.Lock()
	defer world.records.Unlock()
	if leased.Sequence == workflowops.RootSequence {
		run, err := world.readRun(leased.ID)
		if err != nil {
			return err
		}
		if run.Status != workflowops.StatusRunning || run.Token != leased.Token {
			return workflow.ErrLeaseLost
		}
		run.LeaseUntil = time.Now().Add(world.LeaseDuration())
		return writeJSON(world.root, runName(run.ID), run)
	}
	command, found, err := world.readCommand(leased.ID, leased.Sequence)
	if err != nil {
		return err
	}
	if !found || command.Status != workflowops.StatusRunning || command.Token != leased.Token {
		return workflow.ErrLeaseLost
	}
	command.LeaseUntil = time.Now().Add(world.LeaseDuration())
	return writeJSON(world.root, commandName(leased.ID, leased.Sequence), command)
}

func (world *World) CompleteRun(_ context.Context, leased *workflowops.LeasedRun, output json.RawMessage) error {
	world.records.Lock()
	defer world.records.Unlock()
	if leased.Sequence != workflowops.RootSequence {
		return workflow.ErrLeaseLost
	}
	run, err := world.readRun(leased.ID)
	if err != nil {
		return err
	}
	if run.Status != workflowops.StatusRunning || run.Token != leased.Token {
		return workflow.ErrLeaseLost
	}
	completion, err := world.nextCompletion()
	if err != nil {
		return err
	}
	run.Status = workflowops.StatusCompleted
	run.Output = clone(output)
	run.Error = ""
	run.Completion = completion
	run.Token = ""
	run.LeaseUntil = time.Time{}
	if err := writeJSON(world.root, runName(run.ID), run); err != nil {
		return err
	}
	world.resolveWaits()
	world.signal()
	return nil
}

func (world *World) FailRun(_ context.Context, leased *workflowops.LeasedRun, cause error) error {
	world.records.Lock()
	defer world.records.Unlock()
	if leased.Sequence != workflowops.RootSequence {
		return world.finishCommand(leased.ID, leased.Sequence, leased.Token, nil, cause)
	}
	run, err := world.readRun(leased.ID)
	if err != nil {
		return err
	}
	if run.Status != workflowops.StatusRunning || run.Token != leased.Token {
		return workflow.ErrLeaseLost
	}
	completion, err := world.nextCompletion()
	if err != nil {
		return err
	}
	run.Status = workflowops.StatusFailed
	run.Error = cause.Error()
	run.Output = nil
	run.Completion = completion
	run.Token = ""
	run.LeaseUntil = time.Time{}
	if err := writeJSON(world.root, runName(run.ID), run); err != nil {
		return err
	}
	world.resolveWaits()
	world.signal()
	return nil
}

func (world *World) EnsureOperation(
	ctx context.Context,
	lease *workflowops.LeasedRun,
	request workflowops.OperationRequest,
) (workflowops.Operation, error) {
	if err := ctx.Err(); err != nil {
		return workflowops.Operation{}, err
	}
	if err := world.mustStart(); err != nil {
		return workflowops.Operation{}, err
	}
	if _, err := world.readRun(request.RunID); err != nil {
		return workflowops.Operation{}, err
	}
	world.records.Lock()
	defer world.records.Unlock()
	command, found, err := world.readCommand(request.RunID, request.Sequence)
	if err != nil {
		return workflowops.Operation{}, err
	}
	if found {
		if !commandMetadata(command).MatchesOperation(request) || command.ChildID != "" {
			return workflowops.Operation{}, workflowops.HistoryMismatchError("operation", request.Sequence)
		}
		return commandOperation(request.RunID, command), nil
	}
	if !allowsCreate(lease, request.RunID) {
		return workflowops.Operation{}, workflowops.HistoryMissingError("command", request.Sequence)
	}
	due := time.Time{}
	if request.DueAt != nil {
		due = *request.DueAt
	}
	command = commandRecord{
		Sequence:       request.Sequence,
		Kind:           request.Kind,
		Identity:       request.Identity,
		Fingerprint:    request.Fingerprint,
		Input:          clone(request.Input),
		Status:         workflowops.StatusQueued,
		DueAt:          due,
		IdempotencyKey: request.RunID + ":" + strconv.Itoa(request.Sequence),
	}
	if err := writeJSON(world.root, commandName(request.RunID, request.Sequence), command); err != nil {
		return workflowops.Operation{}, err
	}
	world.signal()
	return commandOperation(request.RunID, command), nil
}

func (world *World) EnsureWait(
	ctx context.Context,
	lease *workflowops.LeasedRun,
	request workflowops.WaitRequest,
) (workflowops.Operation, error) {
	if err := ctx.Err(); err != nil {
		return workflowops.Operation{}, err
	}
	if err := world.mustStart(); err != nil {
		return workflowops.Operation{}, err
	}
	if _, err := world.readRun(request.RunID); err != nil {
		return workflowops.Operation{}, err
	}
	world.records.Lock()
	defer world.records.Unlock()
	input, err := json.Marshal(request.Dependencies)
	if err != nil {
		return workflowops.Operation{}, err
	}
	command, found, err := world.readCommand(request.RunID, request.Sequence)
	if err != nil {
		return workflowops.Operation{}, err
	}
	if found {
		if !commandMetadata(command).MatchesWait(request, input) || command.ChildID != "" {
			return workflowops.Operation{}, workflowops.HistoryMismatchError("wait", request.Sequence)
		}
		return commandOperation(request.RunID, command), nil
	}
	if !allowsCreate(lease, request.RunID) {
		return workflowops.Operation{}, workflowops.HistoryMissingError("wait command", request.Sequence)
	}
	command = commandRecord{
		Sequence:       request.Sequence,
		Kind:           request.Kind,
		Identity:       string(request.Kind),
		Fingerprint:    request.Fingerprint,
		Input:          clone(input),
		Dependencies:   append([]workflowops.Task(nil), request.Dependencies...),
		Status:         workflowops.StatusWaiting,
		IdempotencyKey: request.RunID + ":" + strconv.Itoa(request.Sequence),
	}
	if err := writeJSON(world.root, commandName(request.RunID, request.Sequence), command); err != nil {
		return workflowops.Operation{}, err
	}
	world.resolveWaits()
	command, found, err = world.readCommand(request.RunID, request.Sequence)
	if err != nil {
		return workflowops.Operation{}, err
	}
	if !found {
		return workflowops.Operation{}, workflowops.HistoryMissingError("wait command", request.Sequence)
	}
	return commandOperation(request.RunID, command), nil
}

func (world *World) SuspendRun(_ context.Context, leased *workflowops.LeasedRun) error {
	world.records.Lock()
	defer world.records.Unlock()
	if leased.Sequence != workflowops.RootSequence {
		return workflow.ErrLeaseLost
	}
	run, err := world.readRun(leased.ID)
	if err != nil {
		return err
	}
	if run.Status != workflowops.StatusRunning || run.Token != leased.Token {
		return workflow.ErrLeaseLost
	}
	run.Status, run.Token, run.LeaseUntil = workflowops.StatusWaiting, "", time.Time{}
	if err := writeJSON(world.root, runName(run.ID), run); err != nil {
		return err
	}
	world.resolveWaits()
	return nil
}

func (world *World) FinishOperation(
	_ context.Context,
	value workflowops.Operation,
	output json.RawMessage,
	cause error,
) error {
	world.records.Lock()
	defer world.records.Unlock()
	return world.finishCommand(value.RunID, value.Sequence, value.Token, output, cause)
}

func (world *World) finishCommand(runID string, sequence int, token string, output json.RawMessage, cause error) error {
	command, found, err := world.readCommand(runID, sequence)
	if err != nil {
		return err
	}
	if !found || command.Status != workflowops.StatusRunning || command.Token != token {
		return workflow.ErrLeaseLost
	}
	completion, err := world.nextCompletion()
	if err != nil {
		return err
	}
	if cause != nil {
		command.Status, command.Error, command.Output = workflowops.StatusFailed, cause.Error(), nil
	} else {
		command.Status, command.Error, command.Output = workflowops.StatusCompleted, "", clone(output)
	}
	command.Completion, command.Token, command.LeaseUntil = completion, "", time.Time{}
	if err := writeJSON(world.root, commandName(runID, sequence), command); err != nil {
		return err
	}
	world.resolveWaits()
	world.signal()
	return nil
}

func (world *World) WaitOperation(ctx context.Context, task workflowops.Task) (json.RawMessage, error) {
	for {
		command, found, err := world.readCommand(task.RunID, task.Sequence)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("workflow: operation %d not found", task.Sequence)
		}
		switch command.Status {
		case workflowops.StatusCompleted:
			return clone(command.Output), nil
		case workflowops.StatusFailed:
			return nil, errors.New(command.Error)
		}
		change := world.changeChannel()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-change:
		}
	}
}

func (world *World) AppendEvent(_ context.Context, task workflowops.Task, data []byte) error {
	world.records.Lock()
	defer world.records.Unlock()
	if _, err := world.readRun(task.RunID); err != nil {
		return err
	}
	name := path.Join(eventDir(task.RunID), strconv.Itoa(task.Sequence)+".json")
	if _, err := world.root.Stat(name); err == nil {
		return nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	event := eventRecord{
		ID:        int64(task.Sequence),
		Data:      clone(data),
		CreatedAt: time.Now(),
	}
	if err := writeJSON(world.root, name, event); err != nil {
		return err
	}
	world.signal()
	return nil
}

func (world *World) scheduler() {
	defer world.workers.Done()
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()
	for {
		// Capture the notification before scanning: a write that races with the
		// scan either appears in this scan or closes this channel, never vanishes.
		change := world.changeChannel()
		due, err := world.dispatchReady()
		if err != nil {
			world.cancel()
			return
		}
		var timerC <-chan time.Time
		if !due.IsZero() {
			duration := time.Until(due)
			if duration < 0 {
				duration = 0
			}
			timer.Reset(duration)
			timerC = timer.C
		}
		select {
		case <-world.ctx.Done():
			return
		case <-change:
			if timerC != nil && !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-timerC:
		}
	}
}

func (world *World) dispatchReady() (time.Time, error) {
	runs, err := world.allRuns()
	if err != nil {
		return time.Time{}, err
	}
	now, next := time.Now(), time.Time{}
	for _, run := range runs {
		if run.Status == workflowops.StatusQueued {
			world.launch(run.ID, workflowops.RootSequence)
		}
		// A workflow records its full batch of commands before suspending. Do
		// not run a command while its parent is still constructing that batch.
		if run.Status != workflowops.StatusWaiting {
			continue
		}
		commands, err := world.commands(run.ID)
		if err != nil {
			return time.Time{}, err
		}
		for _, command := range commands {
			if command.Status != workflowops.StatusQueued {
				continue
			}
			if command.Kind == workflowops.KindSleep && command.DueAt.After(now) {
				if next.IsZero() || command.DueAt.Before(next) {
					next = command.DueAt
				}
				continue
			}
			world.launch(run.ID, command.Sequence)
		}
	}
	return next, nil
}

func (world *World) launch(runID string, sequence int) {
	world.workers.Add(1)
	go func() {
		defer world.workers.Done()
		leased, ok := world.claim(runID, sequence)
		if !ok {
			return
		}
		_ = world.engine.Execute(world.ctx, leased)
	}()
}

func (world *World) claim(runID string, sequence int) (*workflowops.LeasedRun, bool) {
	if world.mustStart() != nil {
		return nil, false
	}
	world.records.Lock()
	defer world.records.Unlock()
	token, err := workflowops.NewRunID()
	if err != nil {
		return nil, false
	}
	if sequence == workflowops.RootSequence {
		run, err := world.readRun(runID)
		if err != nil || run.Status != workflowops.StatusQueued {
			return nil, false
		}
		run.Status, run.Token, run.LeaseUntil = workflowops.StatusRunning, token, time.Now().Add(world.LeaseDuration())
		if writeJSON(world.root, runName(runID), run) != nil {
			return nil, false
		}
		return &workflowops.LeasedRun{
			ID:       runID,
			Identity: run.Identity,
			Input:    clone(run.Input),
			Sequence: workflowops.RootSequence,
			Token:    token,
		}, true
	}
	command, found, err := world.readCommand(runID, sequence)
	if err != nil || !found || command.Status != workflowops.StatusQueued {
		return nil, false
	}
	command.Status = workflowops.StatusRunning
	command.Token = token
	command.LeaseUntil = time.Now().Add(world.LeaseDuration())
	if writeJSON(world.root, commandName(runID, sequence), command) != nil {
		return nil, false
	}
	run, err := world.readRun(runID)
	if err != nil {
		return nil, false
	}
	return &workflowops.LeasedRun{
		ID:       runID,
		Identity: run.Identity,
		Input:    clone(run.Input),
		Sequence: sequence,
		Token:    token,
	}, true
}

func (world *World) recover() error {
	world.records.Lock()
	defer world.records.Unlock()
	runs, err := world.allRuns()
	if err != nil {
		return err
	}
	for _, run := range runs {
		commands, err := world.commands(run.ID)
		if err != nil {
			return err
		}
		for _, command := range commands {
			if command.Status == workflowops.StatusRunning {
				command.Status, command.Token, command.LeaseUntil = workflowops.StatusQueued, "", time.Time{}
				if err := writeJSON(world.root, commandName(run.ID, command.Sequence), command); err != nil {
					return err
				}
			}
		}
		if run.Status == workflowops.StatusRunning {
			run.Status, run.Token, run.LeaseUntil = workflowops.StatusQueued, "", time.Time{}
			if err := writeJSON(world.root, runName(run.ID), run); err != nil {
				return err
			}
		}
	}
	world.resolveWaits()
	return nil
}

func (world *World) resolveWaits() {
	for changed := true; changed; {
		changed = false
		runs, err := world.allRuns()
		if err != nil {
			return
		}
		for _, run := range runs {
			commands, err := world.commands(run.ID)
			if err != nil {
				return
			}
			for _, command := range commands {
				if command.Status != workflowops.StatusWaiting || !workflowops.IsWaitKind(command.Kind) {
					continue
				}
				states := make([]workflowops.Dependency, len(command.Dependencies))
				for index, dependency := range command.Dependencies {
					states[index] = world.dependencyState(dependency)
				}
				resolution := workflowops.ResolveWait(command.Kind, states)
				if resolution.Pending {
					continue
				}
				completion, err := world.nextCompletion()
				if err != nil {
					return
				}
				if resolution.Err != nil {
					command.Status, command.Error, command.Output = workflowops.StatusFailed, resolution.Err.Error(), nil
				} else {
					command.Status, command.Error, command.Output = workflowops.StatusCompleted, "", resolution.Output
				}
				command.Completion = completion
				if writeJSON(world.root, commandName(run.ID, command.Sequence), command) != nil {
					return
				}
				if run.Status == workflowops.StatusWaiting {
					run.Status = workflowops.StatusQueued
					if writeJSON(world.root, runName(run.ID), run) != nil {
						return
					}
				}
				changed = true
			}
		}
	}
	world.signal()
}

func (world *World) dependencyState(task workflowops.Task) workflowops.Dependency {
	if task.Sequence == workflowops.RootSequence {
		run, err := world.readRun(task.RunID)
		if err != nil {
			return workflowops.Dependency{Task: task}
		}
		return workflowops.Dependency{
			Task:       task,
			Found:      true,
			Status:     workflowops.Status(run.Status),
			Output:     clone(run.Output),
			Error:      run.Error,
			Completion: run.Completion,
		}
	}
	command, found, err := world.readCommand(task.RunID, task.Sequence)
	if err != nil || !found {
		return workflowops.Dependency{Task: task}
	}
	return workflowops.Dependency{
		Task:       task,
		Found:      true,
		Status:     workflowops.Status(command.Status),
		Output:     clone(command.Output),
		Error:      command.Error,
		Completion: command.Completion,
	}
}

func (world *World) nextCompletion() (uint64, error) {
	var version versionRecord
	if err := readJSON(world.root, "version.json", &version); err != nil {
		return 0, err
	}
	version.Completion++
	if err := writeJSON(world.root, "version.json", version); err != nil {
		return 0, err
	}
	return version.Completion, nil
}

func (world *World) validateVersion() error {
	var version versionRecord
	err := readJSON(world.root, "version.json", &version)
	if errors.Is(err, fs.ErrNotExist) {
		return writeJSON(world.root, "version.json", versionRecord{Version: storeVersion})
	}
	if err != nil {
		return err
	}
	if version.Version != storeVersion {
		return fmt.Errorf("workflow/local: incompatible storage version %d", version.Version)
	}
	return nil
}

func (world *World) readRun(id string) (runRecord, error) {
	var record runRecord
	err := readJSON(world.root, runName(id), &record)
	if errors.Is(err, fs.ErrNotExist) {
		return record, workflow.ErrRunNotFound
	}
	return record, err
}

func (world *World) readCommand(runID string, sequence int) (commandRecord, bool, error) {
	var record commandRecord
	err := readJSON(world.root, commandName(runID, sequence), &record)
	if errors.Is(err, fs.ErrNotExist) {
		return record, false, nil
	}
	return record, err == nil, err
}

func (world *World) allRuns() ([]runRecord, error) {
	names, err := listJSON(world.root, "runs")
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	result := make([]runRecord, 0, len(names))
	for _, name := range names {
		var record runRecord
		if err := readJSON(world.root, path.Join("runs", name), &record); err != nil {
			return nil, err
		}
		result = append(result, record)
	}
	return result, nil
}

func (world *World) commands(runID string) ([]commandRecord, error) {
	names, err := listJSON(world.root, path.Join("commands", runID))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	result := make([]commandRecord, 0, len(names))
	for _, name := range names {
		var record commandRecord
		if err := readJSON(world.root, path.Join("commands", runID, name), &record); err != nil {
			return nil, err
		}
		result = append(result, record)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Sequence < result[j].Sequence })
	return result, nil
}

func (world *World) acquireLock() error {
	if err := world.root.MkdirAll("locks", 0700); err != nil {
		return err
	}
	file, err := world.root.OpenFile("locks/world.lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return ErrInUse
		}
		return err
	}
	world.lock = file
	return nil
}

func (world *World) releaseLock() {
	if world.lock != nil {
		_ = syscall.Flock(int(world.lock.Fd()), syscall.LOCK_UN)
		_ = world.lock.Close()
		world.lock = nil
	}
}

func (world *World) changeChannel() <-chan struct{} {
	world.mu.RLock()
	defer world.mu.RUnlock()
	return world.changes
}

func (world *World) signal() {
	world.mu.Lock()
	world.signalLocked()
	world.mu.Unlock()
}

func (world *World) signalLocked() {
	close(world.changes)
	world.changes = make(chan struct{})
}

func runName(id string) string {
	return path.Join("runs", id+".json")
}

func commandName(id string, sequence int) string {
	return path.Join("commands", id, strconv.Itoa(sequence)+".json")
}

func eventDir(id string) string {
	return path.Join("events", id)
}

func commandOperation(runID string, command commandRecord) workflowops.Operation {
	return workflowops.Operation{
		RunID:          runID,
		Sequence:       command.Sequence,
		Status:         workflowops.Status(command.Status),
		Token:          command.Token,
		Error:          command.Error,
		IdempotencyKey: command.IdempotencyKey,
		DueAt:          command.DueAt,
		LeaseUntil:     command.LeaseUntil,
	}
}

func allowsCreate(lease *workflowops.LeasedRun, runID string) bool {
	return lease != nil && lease.ID == runID && lease.Sequence == workflowops.RootSequence
}

func commandMetadata(command commandRecord) workflowops.CommandMetadata {
	return workflowops.CommandMetadata{
		Kind:        command.Kind,
		Identity:    command.Identity,
		Fingerprint: command.Fingerprint,
		Input:       command.Input,
		ChildRunID:  command.ChildID,
	}
}

func clone[T ~[]byte](value T) T {
	return append(T(nil), value...)
}
