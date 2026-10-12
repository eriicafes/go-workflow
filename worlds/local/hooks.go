package localworld

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io/fs"
	"path"
	"time"

	"github.com/eriicafes/go-workflow/workflowops"
)

type hookRecord struct {
	Token        string               `json:"token"`
	Task         workflowops.Task     `json:"task"`
	MinRetention time.Duration        `json:"minRetention"`
	ExpiresAt    time.Time            `json:"expiresAt,omitempty"`
	Webhook      *workflowops.Webhook `json:"webhook,omitempty"`
	Response     json.RawMessage      `json:"response,omitempty"`
}

// GetHook returns a hook until its retention period ends.
func (world *World) GetHook(_ context.Context, token string) (workflowops.Hook, bool, error) {
	world.records.Lock()
	defer world.records.Unlock()
	record, found, err := world.readHook(token)
	if err != nil || !found {
		return workflowops.Hook{}, found, err
	}
	if !record.ExpiresAt.IsZero() && !time.Now().Before(record.ExpiresAt) {
		if err := remove(world.root, hookName(token)); err != nil {
			return workflowops.Hook{}, false, err
		}
		return workflowops.Hook{}, false, nil
	}
	return workflowops.Hook{Token: token, Task: record.Task, ExpiresAt: record.ExpiresAt, Registered: true, Webhook: record.Webhook}, true, nil
}

// RegisterHook claims a globally unique token for request.Task.
func (world *World) RegisterHook(_ context.Context, lease *workflowops.Lease, request workflowops.HookRequest) (workflowops.Hook, error) {
	if lease == nil || request.Token == "" || request.Task.RunID != lease.ID {
		return workflowops.Hook{}, workflowops.ErrLeaseLost
	}
	world.records.Lock()
	defer world.records.Unlock()
	run, err := world.readRun(lease.ID)
	if err != nil {
		return workflowops.Hook{}, err
	}
	if run.Status != workflowops.StatusRunning || run.Token != lease.Token {
		return workflowops.Hook{}, workflowops.ErrLeaseLost
	}
	command, found, err := world.readCommand(request.Task.RunID, request.Task.Sequence)
	if err != nil || !found || command.Kind != workflowops.KindHook {
		return workflowops.Hook{}, workflowops.ErrRunNotFound
	}
	existing, found, err := world.readHook(request.Token)
	if err != nil {
		return workflowops.Hook{}, err
	}
	if found && existing.Task == request.Task {
		return workflowops.Hook{Token: request.Token, Task: request.Task, ExpiresAt: existing.ExpiresAt, Registered: true, Webhook: existing.Webhook}, nil
	}
	if found && existing.Task.RunID == request.Task.RunID {
		prior, priorFound, err := world.readCommand(existing.Task.RunID, existing.Task.Sequence)
		if err != nil {
			return workflowops.Hook{}, err
		}
		if priorFound && (prior.Status == workflowops.StatusCompleted ||
			prior.Status == workflowops.StatusFailed ||
			prior.Status == workflowops.StatusCancelled) {
			if err := remove(world.root, hookName(request.Token)); err != nil {
				return workflowops.Hook{}, err
			}
			found = false
		}
	}
	if found && !existing.ExpiresAt.IsZero() && !time.Now().Before(existing.ExpiresAt) {
		if err := remove(world.root, hookName(request.Token)); err != nil {
			return workflowops.Hook{}, err
		}
		found = false
	}
	if found && !request.Force {
		return workflowops.Hook{Token: request.Token, Task: request.Task, Conflict: &existing.Task, Registered: true}, nil
	}
	if found {
		if err := world.failHookLocked(existing.Task); err != nil {
			return workflowops.Hook{}, err
		}
	}
	record := hookRecord{Token: request.Token, Task: request.Task, MinRetention: request.MinRetention, Webhook: request.Webhook}
	if err := writeJSON(world.root, hookName(request.Token), record); err != nil {
		return workflowops.Hook{}, err
	}
	world.signalLocked()
	return workflowops.Hook{Token: request.Token, Task: request.Task, Registered: true}, nil
}

// ReplyWebhook records a response for a manual webhook.
func (world *World) ReplyWebhook(_ context.Context, token string, response json.RawMessage) error {
	world.records.Lock()
	defer world.records.Unlock()
	record, found, err := world.readHook(token)
	if err != nil || !found || record.Webhook == nil || !record.Webhook.Manual {
		return workflowops.ErrHookNotFound
	}
	if len(record.Response) != 0 {
		return nil
	}
	record.Response = clone(response)
	if err := writeJSON(world.root, hookName(token), record); err != nil {
		return err
	}
	world.signal()
	return nil
}

// WaitWebhookResponse waits for a response recorded by ReplyWebhook.
func (world *World) WaitWebhookResponse(ctx context.Context, token string) (json.RawMessage, error) {
	for {
		world.records.Lock()
		record, found, err := world.readHook(token)
		if err != nil || !found || record.Webhook == nil || !record.Webhook.Manual {
			world.records.Unlock()
			if err != nil {
				return nil, err
			}
			return nil, workflowops.ErrHookNotFound
		}
		if len(record.Response) != 0 {
			response := clone(record.Response)
			world.records.Unlock()
			return response, nil
		}
		world.records.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-world.changeChannel():
		}
	}
}

// ResumeWebhook persists a payload and requeues the hook owner.
func (world *World) ResumeWebhook(_ context.Context, token string, payload json.RawMessage) error {
	world.records.Lock()
	defer world.records.Unlock()
	record, found, err := world.readHook(token)
	if err != nil || !found {
		return workflowops.ErrHookNotFound
	}
	if !record.ExpiresAt.IsZero() && !time.Now().Before(record.ExpiresAt) {
		_ = remove(world.root, hookName(token))
		return workflowops.ErrHookNotFound
	}
	command, found, err := world.readCommand(record.Task.RunID, record.Task.Sequence)
	if err != nil || !found {
		return workflowops.ErrHookNotFound
	}
	if command.Status == workflowops.StatusCompleted {
		return nil
	}
	if command.Status != workflowops.StatusWaiting {
		return workflowops.ErrHookNotFound
	}
	completion, err := world.nextCompletion()
	if err != nil {
		return err
	}
	command.Status = workflowops.StatusCompleted
	command.Output = clone(payload)
	command.Error = ""
	command.Completion = completion
	if err := writeJSON(world.root, commandName(record.Task.RunID, record.Task.Sequence), command); err != nil {
		return err
	}
	if record.MinRetention > 0 {
		record.ExpiresAt = time.Now().Add(record.MinRetention)
		if err := writeJSON(world.root, hookName(token), record); err != nil {
			return err
		}
		world.expireHook(token, record.ExpiresAt)
	}
	run, err := world.readRun(record.Task.RunID)
	if err != nil {
		return err
	}
	if run.Status == workflowops.StatusWaiting {
		run.Status = workflowops.StatusQueued
		if err := writeJSON(world.root, runName(run.ID), run); err != nil {
			return err
		}
		world.launch(run.ID, workflowops.RootSequence)
	}
	world.signalLocked()
	return nil
}

func (world *World) expireHook(token string, expiresAt time.Time) {
	go func() {
		timer := time.NewTimer(time.Until(expiresAt))
		defer timer.Stop()
		select {
		case <-world.ctx.Done():
			return
		case <-timer.C:
		}
		world.records.Lock()
		record, found, err := world.readHook(token)
		if err == nil && found && record.ExpiresAt.Equal(expiresAt) {
			err = remove(world.root, hookName(token))
		}
		world.records.Unlock()
		if err == nil && found {
			world.signal()
		}
	}()
}

func (world *World) failHookLocked(task workflowops.Task) error {
	command, found, err := world.readCommand(task.RunID, task.Sequence)
	if err != nil || !found || command.Status != workflowops.StatusWaiting {
		return err
	}
	completion, err := world.nextCompletion()
	if err != nil {
		return err
	}
	command.Status = workflowops.StatusFailed
	command.Output = nil
	command.Error = workflowops.EncodeFailure(workflowops.ErrHookForceClaimed)
	command.Completion = completion
	if err := writeJSON(world.root, commandName(task.RunID, task.Sequence), command); err != nil {
		return err
	}
	run, err := world.readRun(task.RunID)
	if err != nil {
		return err
	}
	if run.Status == workflowops.StatusWaiting {
		run.Status = workflowops.StatusQueued
		if err := writeJSON(world.root, runName(run.ID), run); err != nil {
			return err
		}
		world.launch(run.ID, workflowops.RootSequence)
	}
	return nil
}

func (world *World) readHook(token string) (hookRecord, bool, error) {
	var record hookRecord
	err := readJSON(world.root, hookName(token), &record)
	if errors.Is(err, fs.ErrNotExist) {
		return hookRecord{}, false, nil
	}
	if err != nil {
		return hookRecord{}, false, err
	}
	return record, true, nil
}

func hookName(token string) string {
	return path.Join("hooks", hex.EncodeToString([]byte(token))+".json")
}
