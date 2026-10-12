package vercelworld

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/eriicafes/go-workflow/workflowops"
)

// GetHook reads a retained hook token from Redis.
func (world *World) GetHook(ctx context.Context, token string) (workflowops.Hook, bool, error) {
	record, err := world.hash(ctx, world.hookKey(token))
	if err != nil {
		return workflowops.Hook{}, false, err
	}
	if len(record) == 0 {
		return workflowops.Hook{}, false, nil
	}
	expires := parseStamp(record["expires"])
	if !expires.IsZero() && !time.Now().Before(expires) {
		if err := world.redis.HDel(ctx, world.hookKey(token), "run", "sequence", "retention", "expires"); err != nil {
			return workflowops.Hook{}, false, err
		}
		return workflowops.Hook{}, false, nil
	}
	hook := workflowops.Hook{
		Token:      token,
		Task:       workflowops.Task{RunID: record["run"], Sequence: parseSequence(record["sequence"])},
		ExpiresAt:  expires,
		Registered: true,
	}
	if record["webhook"] != "" {
		if err := json.Unmarshal([]byte(record["webhook"]), &hook.Webhook); err != nil {
			return workflowops.Hook{}, false, err
		}
	}
	return hook, true, nil
}

// RegisterHook claims a token for a hook operation. Redis serializes each hash
// update, while the root lease prevents a stale replay from becoming an owner.
func (world *World) RegisterHook(ctx context.Context, lease *workflowops.Lease, request workflowops.HookRequest) (workflowops.Hook, error) {
	if lease == nil || request.Token == "" || request.Task.RunID != lease.ID {
		return workflowops.Hook{}, workflowops.ErrLeaseLost
	}
	if err := world.validateLease(ctx, lease); err != nil {
		return workflowops.Hook{}, err
	}
	existing, found, err := world.GetHook(ctx, request.Token)
	if err != nil {
		return workflowops.Hook{}, err
	}
	if found && existing.Task == request.Task {
		return existing, nil
	}
	if found && existing.Task.RunID == request.Task.RunID {
		prior, err := world.hash(ctx, world.taskKey(existing.Task.RunID, existing.Task.Sequence))
		if err != nil {
			return workflowops.Hook{}, err
		}
		if terminalStatus(workflowops.Status(prior["status"])) {
			found = false
		}
	}
	if found && !request.Force {
		return workflowops.Hook{Token: request.Token, Task: request.Task, Conflict: &existing.Task, Registered: true}, nil
	}
	if found {
		if err := world.forceHook(ctx, existing.Task); err != nil {
			return workflowops.Hook{}, err
		}
	}
	if err := world.redis.HSet(
		ctx,
		world.hookKey(request.Token),
		"run", request.Task.RunID,
		"sequence", strconv.Itoa(request.Task.Sequence),
		"retention", strconv.FormatInt(request.MinRetention.Nanoseconds(), 10),
		"expires", "",
		"webhook", mustJSON(request.Webhook),
		"response", "",
	); err != nil {
		return workflowops.Hook{}, err
	}
	return workflowops.Hook{Token: request.Token, Task: request.Task, Registered: true}, nil
}

func terminalStatus(status workflowops.Status) bool {
	return status == workflowops.StatusCompleted ||
		status == workflowops.StatusFailed ||
		status == workflowops.StatusCancelled
}

// ReplyWebhook stores the first manual webhook response.
func (world *World) ReplyWebhook(ctx context.Context, token string, response json.RawMessage) error {
	hook, found, err := world.GetHook(ctx, token)
	if err != nil {
		return err
	}
	if !found || hook.Webhook == nil || !hook.Webhook.Manual {
		return workflowops.ErrHookNotFound
	}
	record, err := world.hash(ctx, world.hookKey(token))
	if err != nil {
		return err
	}
	if record["response"] != "" {
		return nil
	}
	return world.redis.HSet(ctx, world.hookKey(token), "response", string(response))
}

// WaitWebhookResponse waits for the response stored by ReplyWebhook.
func (world *World) WaitWebhookResponse(ctx context.Context, token string) (json.RawMessage, error) {
	for {
		hook, found, err := world.GetHook(ctx, token)
		if err != nil {
			return nil, err
		}
		if !found || hook.Webhook == nil || !hook.Webhook.Manual {
			return nil, workflowops.ErrHookNotFound
		}
		record, err := world.hash(ctx, world.hookKey(token))
		if err != nil {
			return nil, err
		}
		if record["response"] != "" {
			return json.RawMessage(record["response"]), nil
		}
		timer := time.NewTimer(25 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

// ResumeWebhook completes the waiting operation and queues its owner root.
func (world *World) ResumeWebhook(ctx context.Context, token string, payload json.RawMessage) error {
	hook, found, err := world.GetHook(ctx, token)
	if err != nil {
		return err
	}
	if !found {
		return workflowops.ErrHookNotFound
	}
	task, err := world.hash(ctx, world.taskKey(hook.Task.RunID, hook.Task.Sequence))
	if err != nil {
		return err
	}
	if workflowops.Status(task["status"]) == workflowops.StatusCompleted {
		return nil
	}
	if workflowops.Status(task["status"]) != workflowops.StatusWaiting {
		return workflowops.ErrHookNotFound
	}
	completion, err := world.redis.Incr(ctx, world.completionKey())
	if err != nil {
		return err
	}
	if err := world.redis.HSet(
		ctx,
		world.taskKey(hook.Task.RunID, hook.Task.Sequence),
		"status", string(workflowops.StatusCompleted),
		"output", string(payload),
		"error", "",
		"completed", strconv.FormatInt(completion, 10),
		"updated", stamp(time.Now()),
	); err != nil {
		return err
	}
	record, err := world.hash(ctx, world.hookKey(token))
	if err != nil {
		return err
	}
	retention, _ := strconv.ParseInt(record["retention"], 10, 64)
	if retention > 0 {
		if err := world.redis.HSet(ctx, world.hookKey(token), "expires", stamp(time.Now().Add(time.Duration(retention)))); err != nil {
			return err
		}
	}
	rootTask := workflowops.Task{RunID: hook.Task.RunID, Sequence: workflowops.RootSequence}
	root, err := world.hash(ctx, world.taskKey(rootTask.RunID, rootTask.Sequence))
	if err != nil {
		return err
	}
	if workflowops.Status(root["status"]) != workflowops.StatusWaiting {
		return nil
	}
	if err := world.redis.HSet(ctx, world.taskKey(rootTask.RunID, rootTask.Sequence), "status", string(workflowops.StatusQueued), "updated", stamp(time.Now())); err != nil {
		return err
	}
	if err := world.redis.HSet(ctx, world.runKey(rootTask.RunID), "status", string(workflowops.StatusQueued), "updated", stamp(time.Now())); err != nil {
		return err
	}
	return world.enqueueTask(ctx, rootTask, time.Time{})
}

func (world *World) forceHook(ctx context.Context, task workflowops.Task) error {
	if err := world.redis.HSet(
		ctx,
		world.taskKey(task.RunID, task.Sequence),
		"status", string(workflowops.StatusFailed),
		"error", workflowops.EncodeFailure(workflowops.ErrHookForceClaimed),
		"output", "",
		"updated", stamp(time.Now()),
	); err != nil {
		return err
	}
	root := workflowops.Task{RunID: task.RunID, Sequence: workflowops.RootSequence}
	if err := world.redis.HSet(ctx, world.taskKey(root.RunID, root.Sequence), "status", string(workflowops.StatusQueued), "updated", stamp(time.Now())); err != nil {
		return err
	}
	if err := world.redis.HSet(ctx, world.runKey(root.RunID), "status", string(workflowops.StatusQueued), "updated", stamp(time.Now())); err != nil {
		return err
	}
	return world.enqueueTask(ctx, root, time.Time{})
}

func (world *World) hookKey(token string) string {
	return world.config.Prefix + ":hook:" + token
}

func mustJSON(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return string(encoded)
}
