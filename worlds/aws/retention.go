package awsworld

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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

// scheduleExpiry deletes a finished run's data when its ExpiredAt has
// passed, as for a run with $retention "0", and otherwise queues the deletion
// for its ExpiredAt, which the commit finishing the run set from its
// workflow's retention. A deletion that fails is queued to run again.
func (w *World) scheduleExpiry(ctx context.Context, run worlds.Run) error {
	if run.ExpiredAt == nil {
		return nil
	}
	if worlds.RunDataExpired(run, time.Now()) {
		if _, err := w.purgeRun(ctx, run.RunID); err == nil {
			return nil
		}
	}
	return w.queueExpiry(ctx, run.RunID, run.WorkflowName, *run.ExpiredAt)
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

// When a run's data is deleted, the run, step, event, hook, and token items
// stay, the payloads in the run's event items go, and its streams are left
// empty and closed. The run's expiredAt is set first, so its payloads are
// unreadable before they are deleted, and dataPurged is set last, so a
// deletion cut short is finished by its expiry delivery or the next
// RunsDeleteDataMany.

// expireRun deletes the data of the run of an expiry delivery.
func (w *World) expireRun(ctx context.Context, input worlds.QueueInput) error {
	_, err := w.purgeRun(ctx, input.RunID)
	return err
}

// purgeRun deletes a finished run's data and reports whether its expiredAt
// had already passed. A run that does not exist has nothing to delete.
func (w *World) purgeRun(ctx context.Context, runID worlds.RunID) (alreadyExpired bool, err error) {
	item, found, err := w.item(ctx, runKey(string(runID)))
	if err != nil || !found {
		return false, err
	}
	run := runFromItem(item)
	now := time.Now().UTC()
	alreadyExpired = worlds.RunDataExpired(run.Run, now)
	if !alreadyExpired {
		if _, err := w.dynamo.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName:                 awsv2.String(w.config.TableName),
			Key:                       runKey(string(runID)),
			UpdateExpression:          awsv2.String("SET expiredAt = :now"),
			ConditionExpression:       awsv2.String("attribute_exists(pk)"),
			ExpressionAttributeValues: map[string]ddb.AttributeValue{":now": avs(stamp(now))},
		}); err != nil {
			return false, err
		}
	}
	if readBool(item, "dataPurged") {
		return alreadyExpired, nil
	}
	var events []map[string]ddb.AttributeValue
	var streams []map[string]ddb.AttributeValue
	err = w.query(ctx, &dynamodb.QueryInput{
		KeyConditionExpression:    awsv2.String("pk = :pk"),
		ExpressionAttributeValues: map[string]ddb.AttributeValue{":pk": avs(runPartitionKey(string(runID)))},
		ConsistentRead:            awsv2.Bool(true),
	}, func(item map[string]ddb.AttributeValue) bool {
		switch sk := read(item, "sk"); {
		case strings.HasPrefix(sk, "EVENT#"):
			events = append(events, item)
		case strings.HasPrefix(sk, "STREAM#"):
			streams = append(streams, item)
		}
		return true
	})
	if err != nil {
		return alreadyExpired, err
	}
	for _, item := range events {
		event := eventFromItem(item)
		scrubbed := worlds.ScrubEventData(event.EventType, event.Data)
		if bytes.Equal(scrubbed, event.Data) {
			continue
		}
		update := &dynamodb.UpdateItemInput{
			TableName:                awsv2.String(w.config.TableName),
			Key:                      key(read(item, "pk"), read(item, "sk")),
			UpdateExpression:         awsv2.String("REMOVE #data"),
			ExpressionAttributeNames: map[string]string{"#data": "data"},
		}
		if len(scrubbed) > 0 {
			update.UpdateExpression = awsv2.String("SET #data = :data")
			update.ExpressionAttributeValues = map[string]ddb.AttributeValue{":data": avb(scrubbed)}
		}
		if _, err := w.dynamo.UpdateItem(ctx, update); err != nil {
			return alreadyExpired, err
		}
	}
	for _, item := range streams {
		name, count := read(item, "name"), int(readInt(item, "count"))
		// The stream is empty and closed before its chunks go.
		if _, err := w.dynamo.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName:                 awsv2.String(w.config.TableName),
			Key:                       key(read(item, "pk"), read(item, "sk")),
			UpdateExpression:          awsv2.String("SET #count = :empty, #closed = :true"),
			ExpressionAttributeNames:  map[string]string{"#count": "count", "#closed": "closed"},
			ExpressionAttributeValues: map[string]ddb.AttributeValue{":empty": avn(0), ":true": avbool(true)},
		}); err != nil {
			return alreadyExpired, err
		}
		chunks := make([]map[string]ddb.AttributeValue, count)
		for index := range count {
			chunks[index] = chunkKey(string(runID), name, index)
		}
		if err := w.deleteItems(ctx, chunks); err != nil {
			return alreadyExpired, err
		}
	}
	_, err = w.dynamo.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 awsv2.String(w.config.TableName),
		Key:                       runKey(string(runID)),
		UpdateExpression:          awsv2.String("SET dataPurged = :true"),
		ExpressionAttributeValues: map[string]ddb.AttributeValue{":true": avbool(true)},
	})
	return alreadyExpired, err
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
