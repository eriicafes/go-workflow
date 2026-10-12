package awsworld

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/eriicafes/go-workflow/workflowops"
)

// GetHook reads a hook until its retention period ends.
func (world *World) GetHook(ctx context.Context, token string) (workflowops.Hook, bool, error) {
	item, found, err := world.item(ctx, hookKey(token))
	if err != nil || !found {
		return workflowops.Hook{}, found, err
	}
	expires := parseStamp(item["expires"])
	if !expires.IsZero() && !time.Now().Before(expires) {
		_, err = world.dynamo.DeleteItem(ctx, &dynamodb.DeleteItemInput{TableName: awsv2.String(world.config.TableName), Key: hookKey(token)})
		return workflowops.Hook{}, false, err
	}
	hook := workflowops.Hook{
		Token:      token,
		Task:       workflowops.Task{RunID: item["run"], Sequence: parseSequence(item["sequence"])},
		ExpiresAt:  expires,
		Registered: true,
	}
	if item["webhook"] != "" {
		if err := json.Unmarshal([]byte(item["webhook"]), &hook.Webhook); err != nil {
			return workflowops.Hook{}, false, err
		}
	}
	return hook, true, nil
}

// RegisterHook claims a globally unique hook token for the current replay.
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
		prior, priorFound, err := world.item(ctx, taskKey(existing.Task.RunID, existing.Task.Sequence))
		if err != nil {
			return workflowops.Hook{}, err
		}
		if priorFound && terminalStatus(workflowops.Status(prior["status"])) {
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
	webhook, err := json.Marshal(request.Webhook)
	if err != nil {
		return workflowops.Hook{}, err
	}
	_, err = world.dynamo.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: awsv2.String(world.config.TableName),
		Item: merge(hookKey(request.Token), map[string]string{
			"run":       request.Task.RunID,
			"sequence":  strconv.Itoa(request.Task.Sequence),
			"retention": strconv.FormatInt(request.MinRetention.Nanoseconds(), 10),
			"expires":   "",
			"webhook":   string(webhook),
			"response":  "",
		}),
	})
	if err != nil {
		return workflowops.Hook{}, err
	}
	return workflowops.Hook{Token: request.Token, Task: request.Task, Registered: true}, nil
}

func terminalStatus(status workflowops.Status) bool {
	return status == workflowops.StatusCompleted ||
		status == workflowops.StatusFailed ||
		status == workflowops.StatusCancelled
}

// ReplyWebhook stores the first response for a manual webhook.
func (world *World) ReplyWebhook(ctx context.Context, token string, response json.RawMessage) error {
	hook, found, err := world.GetHook(ctx, token)
	if err != nil {
		return err
	}
	if !found || hook.Webhook == nil || !hook.Webhook.Manual {
		return workflowops.ErrHookNotFound
	}
	_, err = world.dynamo.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:        awsv2.String(world.config.TableName),
		Key:              hookKey(token),
		UpdateExpression: awsv2.String("SET #response = :response"),
		ExpressionAttributeNames: map[string]string{
			"#response": "response",
		},
		ExpressionAttributeValues: map[string]ddb.AttributeValue{
			":response": avs(string(response)),
		},
	})
	return err
}

// WaitWebhookResponse waits for a response stored by ReplyWebhook.
func (world *World) WaitWebhookResponse(ctx context.Context, token string) (json.RawMessage, error) {
	for {
		hook, found, err := world.GetHook(ctx, token)
		if err != nil {
			return nil, err
		}
		if !found || hook.Webhook == nil || !hook.Webhook.Manual {
			return nil, workflowops.ErrHookNotFound
		}
		item, found, err := world.item(ctx, hookKey(token))
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, workflowops.ErrHookNotFound
		}
		if item["response"] != "" {
			return json.RawMessage(item["response"]), nil
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

// ResumeWebhook writes a payload to the waiting operation and queues its root.
func (world *World) ResumeWebhook(ctx context.Context, token string, payload json.RawMessage) error {
	hook, found, err := world.GetHook(ctx, token)
	if err != nil {
		return err
	}
	if !found {
		return workflowops.ErrHookNotFound
	}
	task, found, err := world.item(ctx, taskKey(hook.Task.RunID, hook.Task.Sequence))
	if err != nil {
		return err
	}
	if !found {
		return workflowops.ErrHookNotFound
	}
	if workflowops.Status(task["status"]) == workflowops.StatusCompleted {
		return nil
	}
	if workflowops.Status(task["status"]) != workflowops.StatusWaiting {
		return workflowops.ErrHookNotFound
	}
	_, err = world.dynamo.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:           awsv2.String(world.config.TableName),
		Key:                 taskKey(hook.Task.RunID, hook.Task.Sequence),
		UpdateExpression:    awsv2.String("SET #status=:completed, #output=:output, #error=:empty, #updated=:updated"),
		ConditionExpression: awsv2.String("#status=:waiting"),
		ExpressionAttributeNames: map[string]string{
			"#status": "status", "#output": "output", "#error": "error", "#updated": "updated",
		},
		ExpressionAttributeValues: map[string]ddb.AttributeValue{
			":completed": avs(string(workflowops.StatusCompleted)), ":output": avs(string(payload)), ":empty": avs(""),
			":updated": avs(stamp(time.Now())), ":waiting": avs(string(workflowops.StatusWaiting)),
		},
	})
	if conditional(err) {
		return workflowops.ErrHookNotFound
	}
	if err != nil {
		return err
	}
	hookRecord, _, err := world.item(ctx, hookKey(token))
	if err != nil {
		return err
	}
	retention, _ := strconv.ParseInt(hookRecord["retention"], 10, 64)
	if retention > 0 {
		if _, err = world.dynamo.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName: awsv2.String(world.config.TableName), Key: hookKey(token),
			UpdateExpression:          awsv2.String("SET #expires=:expires"),
			ExpressionAttributeNames:  map[string]string{"#expires": "expires"},
			ExpressionAttributeValues: map[string]ddb.AttributeValue{":expires": avs(stamp(time.Now().Add(time.Duration(retention))))},
		}); err != nil {
			return err
		}
	}
	root := workflowops.Task{RunID: hook.Task.RunID, Sequence: workflowops.RootSequence}
	_, err = world.dynamo.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: awsv2.String(world.config.TableName), Key: taskKey(root.RunID, root.Sequence),
		UpdateExpression:          awsv2.String("SET #status=:queued, #updated=:updated"),
		ConditionExpression:       awsv2.String("#status=:waiting"),
		ExpressionAttributeNames:  map[string]string{"#status": "status", "#updated": "updated"},
		ExpressionAttributeValues: map[string]ddb.AttributeValue{":queued": avs(string(workflowops.StatusQueued)), ":waiting": avs(string(workflowops.StatusWaiting)), ":updated": avs(stamp(time.Now()))},
	})
	if conditional(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if _, err = world.dynamo.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: awsv2.String(world.config.TableName), Key: runKey(root.RunID),
		UpdateExpression:          awsv2.String("SET #status=:queued, #updated=:updated"),
		ExpressionAttributeNames:  map[string]string{"#status": "status", "#updated": "updated"},
		ExpressionAttributeValues: map[string]ddb.AttributeValue{":queued": avs(string(workflowops.StatusQueued)), ":updated": avs(stamp(time.Now()))},
	}); err != nil {
		return err
	}
	return world.enqueueTask(ctx, root, time.Time{})
}

func (world *World) forceHook(ctx context.Context, task workflowops.Task) error {
	_, err := world.dynamo.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: awsv2.String(world.config.TableName), Key: taskKey(task.RunID, task.Sequence),
		UpdateExpression:          awsv2.String("SET #status=:failed, #error=:error, #updated=:updated"),
		ExpressionAttributeNames:  map[string]string{"#status": "status", "#error": "error", "#updated": "updated"},
		ExpressionAttributeValues: map[string]ddb.AttributeValue{":failed": avs(string(workflowops.StatusFailed)), ":error": avs(workflowops.EncodeFailure(workflowops.ErrHookForceClaimed)), ":updated": avs(stamp(time.Now()))},
	})
	if err != nil {
		return err
	}
	root := workflowops.Task{RunID: task.RunID, Sequence: workflowops.RootSequence}
	if _, err = world.dynamo.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: awsv2.String(world.config.TableName), Key: taskKey(root.RunID, root.Sequence),
		UpdateExpression:          awsv2.String("SET #status=:queued, #updated=:updated"),
		ExpressionAttributeNames:  map[string]string{"#status": "status", "#updated": "updated"},
		ExpressionAttributeValues: map[string]ddb.AttributeValue{":queued": avs(string(workflowops.StatusQueued)), ":updated": avs(stamp(time.Now()))},
	}); err != nil {
		return err
	}
	if _, err = world.dynamo.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: awsv2.String(world.config.TableName), Key: runKey(root.RunID),
		UpdateExpression:          awsv2.String("SET #status=:queued, #updated=:updated"),
		ExpressionAttributeNames:  map[string]string{"#status": "status", "#updated": "updated"},
		ExpressionAttributeValues: map[string]ddb.AttributeValue{":queued": avs(string(workflowops.StatusQueued)), ":updated": avs(stamp(time.Now()))},
	}); err != nil {
		return err
	}
	return world.enqueueTask(ctx, root, time.Time{})
}

func parseSequence(value string) int {
	sequence, _ := strconv.Atoi(value)
	return sequence
}
