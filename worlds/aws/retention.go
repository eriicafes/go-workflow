package awsworld

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	sch "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	"github.com/eriicafes/go-workflow/worlds"
)

// maxBatchWrite is DynamoDB's BatchWriteItem request limit.
const maxBatchWrite = 25

// finishes reports whether an event of type t makes its run terminal.
func finishes(t worlds.EventType) bool {
	return t == worlds.EventTypeRunCompleted || t == worlds.EventTypeRunFailed || t == worlds.EventTypeRunCancelled
}

// scheduleExpiry queues the deletion of a finished run's data for its
// workflow's retention after the run completed.
func (w *World) scheduleExpiry(ctx context.Context, run worlds.Run) error {
	retention := worlds.RetentionFor(w.config.Retention, w.config.Retentions, run.WorkflowName)
	if retention <= 0 || run.CompletedAt == nil {
		return nil
	}
	return w.queueExpiry(ctx, run.RunID, run.WorkflowName, run.CompletedAt.Add(retention))
}

// queueExpiry stores the expiry delivery of runID, due at due, and publishes
// it like any delivery: through SQS within 15 minutes and through Scheduler
// beyond. Its ID is fixed per run, so queueing it again replaces the record,
// and a schedule that already exists counts as published.
func (w *World) queueExpiry(ctx context.Context, runID worlds.RunID, workflowName string, due time.Time) error {
	id := "expire-" + string(runID)
	input := worlds.QueueInput{RunID: runID, WorkflowName: workflowName}
	queue := w.target(w.config.Routes[workflowName])
	delay := time.Until(due)
	if scheduled(queue, delay) && w.scheduler == nil {
		return errors.New("workflow/aws: run retention over 15 minutes requires Config.Scheduler")
	}
	inputJSON, err := json.Marshal(input)
	if err != nil {
		return err
	}
	item := merge(queueKey(id), map[string]ddb.AttributeValue{
		"input":     avs(string(inputJSON)),
		"createdAt": avs(stamp(time.Now())),
		"expire":    avbool(true),
	})
	setTime(item, "notBefore", &due)
	if scheduled(queue, delay) {
		item["schedule"] = avs(scheduleName(id))
	}
	if queue.name != "" {
		item["queue"] = avs(queue.name)
	}
	if _, err := w.dynamo.PutItem(ctx, &dynamodb.PutItemInput{TableName: awsv2.String(w.config.TableName), Item: item}); err != nil {
		return err
	}
	err = w.publish(ctx, queue, id, input, delay, scheduleName(id))
	var exists *sch.ConflictException
	if errors.As(err, &exists) {
		return nil
	}
	return err
}

// expireRun deletes a finished run's hooks, the tokens they still own, and
// its run partition, the run's view last, so a deletion cut short is retried
// while the run still exists. When one of its hooks still retains its token,
// it queues the expiry again for when that retention ends instead and
// reports that it rescheduled.
func (w *World) expireRun(ctx context.Context, input worlds.QueueInput) (rescheduled bool, err error) {
	runID := string(input.RunID)
	var hookKeys []map[string]ddb.AttributeValue
	err = w.query(ctx, &dynamodb.QueryInput{
		IndexName:                 awsv2.String(listGroupIndex),
		KeyConditionExpression:    awsv2.String("list_group_pk = :pk"),
		ExpressionAttributeValues: map[string]ddb.AttributeValue{":pk": avs("HOOKS#" + runID)},
	}, func(item map[string]ddb.AttributeValue) bool {
		hookKeys = append(hookKeys, key(read(item, "pk"), read(item, "sk")))
		return true
	})
	if err != nil {
		return false, err
	}
	hooks, err := w.items(ctx, hookKeys...)
	if err != nil {
		return false, err
	}
	for _, item := range hooks {
		hook, err := hookFromItem(item)
		if err != nil {
			return false, err
		}
		if until := hook.TokenRetentionUntil; until != nil && until.After(time.Now()) {
			return true, w.queueExpiry(ctx, input.RunID, input.WorkflowName, *until)
		}
		if hook.Token != "" {
			if err := w.releaseToken(ctx, hook.Token, hook.HookID); err != nil {
				return false, err
			}
		}
	}

	var keys []map[string]ddb.AttributeValue
	err = w.query(ctx, &dynamodb.QueryInput{
		KeyConditionExpression:    awsv2.String("pk = :pk"),
		ExpressionAttributeValues: map[string]ddb.AttributeValue{":pk": avs(runPartitionKey(runID))},
		ProjectionExpression:      awsv2.String("pk, sk"),
	}, func(item map[string]ddb.AttributeValue) bool {
		if read(item, "sk") != "META" {
			keys = append(keys, key(read(item, "pk"), read(item, "sk")))
		}
		return true
	})
	if err != nil {
		return false, err
	}
	keys = append(append(keys, hookKeys...), runKey(runID))
	return false, w.deleteItems(ctx, keys)
}

// releaseToken deletes the token item of token while hookID still owns it.
func (w *World) releaseToken(ctx context.Context, token string, hookID worlds.HookID) error {
	_, err := w.dynamo.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName:                 awsv2.String(w.config.TableName),
		Key:                       tokenKey(token),
		ConditionExpression:       awsv2.String("hookId = :hook"),
		ExpressionAttributeValues: map[string]ddb.AttributeValue{":hook": avs(string(hookID))},
	})
	if conditional(err) {
		return nil
	}
	return err
}

// deleteItems deletes keys in order, in batches of maxBatchWrite, retrying
// items DynamoDB leaves unprocessed.
func (w *World) deleteItems(ctx context.Context, keys []map[string]ddb.AttributeValue) error {
	for start := 0; start < len(keys); start += maxBatchWrite {
		requests := make([]ddb.WriteRequest, 0, maxBatchWrite)
		for _, k := range keys[start:min(start+maxBatchWrite, len(keys))] {
			requests = append(requests, ddb.WriteRequest{DeleteRequest: &ddb.DeleteRequest{Key: k}})
		}
		pending := map[string][]ddb.WriteRequest{w.config.TableName: requests}
		for attempt := 0; len(pending[w.config.TableName]) > 0; attempt++ {
			if attempt == 10 {
				return fmt.Errorf("workflow/aws: %d deletes left unprocessed", len(pending[w.config.TableName]))
			}
			if attempt > 0 {
				if err := sleep(ctx, backoff(attempt, 50*time.Millisecond, 2*time.Second)); err != nil {
					return err
				}
			}
			result, err := w.dynamo.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{RequestItems: pending})
			if err != nil {
				return err
			}
			pending = result.UnprocessedItems
		}
	}
	return nil
}
