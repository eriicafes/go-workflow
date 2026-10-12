package workflowtest

import (
	"context"
	"encoding/json"
	"time"

	"github.com/eriicafes/go-workflow/workflowops"
)

// GetHook returns the token's current owner until its retention expires.
func (world *World) GetHook(_ context.Context, token string) (workflowops.Hook, bool, error) {
	world.mu.Lock()
	defer world.mu.Unlock()
	value, found := world.hooks[token]
	if !found {
		return workflowops.Hook{}, false, nil
	}
	if !value.expiresAt.IsZero() && !time.Now().Before(value.expiresAt) {
		delete(world.hooks, token)
		return workflowops.Hook{}, false, nil
	}
	return workflowops.Hook{Token: token, Task: value.task, ExpiresAt: value.expiresAt, Registered: true, Webhook: value.webhook}, true, nil
}

// RegisterHook claims request.Token for the waiting hook task.
func (world *World) RegisterHook(_ context.Context, lease *workflowops.Lease, request workflowops.HookRequest) (workflowops.Hook, error) {
	if request.Token == "" || lease == nil || request.Task.RunID != lease.ID {
		return workflowops.Hook{}, workflowops.ErrLeaseLost
	}
	world.mu.Lock()
	run := world.runs[lease.ID]
	if run == nil || run.status != workflowops.StatusRunning || run.token != lease.Token {
		world.mu.Unlock()
		return workflowops.Hook{}, workflowops.ErrLeaseLost
	}
	command := world.commands[request.Task.RunID][request.Task.Sequence]
	if command == nil || command.kind != workflowops.KindHook {
		world.mu.Unlock()
		return workflowops.Hook{}, workflowops.ErrRunNotFound
	}
	if existing := world.hooks[request.Token]; existing != nil {
		if existing.task == request.Task {
			world.mu.Unlock()
			return workflowops.Hook{Token: request.Token, Task: request.Task, ExpiresAt: existing.expiresAt, Registered: true, Webhook: existing.webhook}, nil
		}
		if existing.task.RunID == request.Task.RunID {
			command := world.commands[existing.task.RunID][existing.task.Sequence]
			if command != nil && (command.status == workflowops.StatusCompleted ||
				command.status == workflowops.StatusFailed ||
				command.status == workflowops.StatusCancelled) {
				delete(world.hooks, request.Token)
				existing = nil
			}
		}
		if existing != nil {
			if !existing.expiresAt.IsZero() && !time.Now().Before(existing.expiresAt) {
				delete(world.hooks, request.Token)
			} else if !request.Force {
				world.mu.Unlock()
				return workflowops.Hook{Token: request.Token, Task: request.Task, Conflict: &existing.task, Registered: true}, nil
			} else {
				ready := world.failHookLocked(existing.task, workflowops.ErrHookForceClaimed)
				world.signalLocked()
				world.mu.Unlock()
				world.scheduleAll(ready)
				world.mu.Lock()
				if run = world.runs[lease.ID]; run == nil || run.status != workflowops.StatusRunning || run.token != lease.Token {
					world.mu.Unlock()
					return workflowops.Hook{}, workflowops.ErrLeaseLost
				}
			}
		}
	}
	world.hooks[request.Token] = &hook{task: request.Task, retention: request.MinRetention, webhook: request.Webhook}
	world.signalLocked()
	world.mu.Unlock()
	return workflowops.Hook{Token: request.Token, Task: request.Task, Registered: true}, nil
}

// ReplyWebhook records one response for a manual webhook.
func (world *World) ReplyWebhook(_ context.Context, token string, response json.RawMessage) error {
	world.mu.Lock()
	defer world.mu.Unlock()
	value := world.hooks[token]
	if value == nil || value.webhook == nil || !value.webhook.Manual {
		return workflowops.ErrHookNotFound
	}
	if len(value.response) != 0 {
		return nil
	}
	value.response = clone(response)
	world.signalLocked()
	return nil
}

// WaitWebhookResponse waits for the response recorded by ReplyWebhook.
func (world *World) WaitWebhookResponse(ctx context.Context, token string) (json.RawMessage, error) {
	for {
		world.mu.Lock()
		value := world.hooks[token]
		if value == nil || value.webhook == nil || !value.webhook.Manual {
			world.mu.Unlock()
			return nil, workflowops.ErrHookNotFound
		}
		if len(value.response) != 0 {
			response := clone(value.response)
			world.mu.Unlock()
			return response, nil
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

// ResumeWebhook persists a payload and requeues the suspended owner run.
func (world *World) ResumeWebhook(_ context.Context, token string, payload json.RawMessage) error {
	world.mu.Lock()
	value := world.hooks[token]
	if value == nil || (!value.expiresAt.IsZero() && !time.Now().Before(value.expiresAt)) {
		delete(world.hooks, token)
		world.mu.Unlock()
		return workflowops.ErrHookNotFound
	}
	command := world.commands[value.task.RunID][value.task.Sequence]
	if command == nil {
		world.mu.Unlock()
		return workflowops.ErrHookNotFound
	}
	if command.status == workflowops.StatusCompleted {
		world.mu.Unlock()
		return nil
	}
	if command.status != workflowops.StatusWaiting {
		world.mu.Unlock()
		return workflowops.ErrHookNotFound
	}
	command.status = workflowops.StatusCompleted
	command.output = clone(payload)
	command.err = ""
	command.completion = world.nextCompletionLocked()
	if value.retention > 0 {
		value.expiresAt = time.Now().Add(value.retention)
		world.expireHook(token, value.expiresAt)
	}
	run := world.runs[value.task.RunID]
	ready := world.resolveWaitsLocked()
	if run != nil && run.status == workflowops.StatusWaiting {
		run.status = workflowops.StatusQueued
		ready = append(ready, workflowops.Task{RunID: run.id, Sequence: workflowops.RootSequence})
	}
	world.signalLocked()
	world.mu.Unlock()
	world.scheduleAll(ready)
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
		world.mu.Lock()
		value := world.hooks[token]
		if value != nil && value.expiresAt.Equal(expiresAt) {
			delete(world.hooks, token)
			world.signalLocked()
		}
		world.mu.Unlock()
	}()
}

func (world *World) failHookLocked(task workflowops.Task, cause error) []workflowops.Task {
	command := world.commands[task.RunID][task.Sequence]
	if command == nil || command.status != workflowops.StatusWaiting {
		return nil
	}
	command.status = workflowops.StatusFailed
	command.output = nil
	command.err = workflowops.EncodeFailure(cause)
	command.completion = world.nextCompletionLocked()
	ready := world.resolveWaitsLocked()
	if run := world.runs[task.RunID]; run != nil && run.status == workflowops.StatusWaiting {
		run.status = workflowops.StatusQueued
		ready = append(ready, workflowops.Task{RunID: run.id, Sequence: workflowops.RootSequence})
	}
	return ready
}

func (world *World) dropHooksLocked(runID string) {
	for token, value := range world.hooks {
		if value.task.RunID == runID && value.expiresAt.IsZero() &&
			(value.webhook == nil || !value.webhook.Manual || value.response == nil) {
			delete(world.hooks, token)
		}
	}
}
