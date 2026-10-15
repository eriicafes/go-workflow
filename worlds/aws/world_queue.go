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
	"github.com/eriicafes/go-workflow/worlds"
	"github.com/oklog/ulid/v2"
)

type queueRecord struct {
	input     worlds.QueueInput
	notBefore *time.Time
	schedule  string
}

// Queue stores a QUEUE# record and publishes its ID: through SQS with a
// delay of up to 15 minutes, otherwise through a one-time Scheduler schedule
// whose name is kept on the record. When publishing fails the record is
// removed. SQS standard queues do not deduplicate, so IdempotencyKey is not
// used.
func (w *World) Queue(ctx context.Context, input worlds.QueueInput) (worlds.QueueResult, error) {
	if err := ctx.Err(); err != nil {
		return worlds.QueueResult{}, err
	}
	var delay time.Duration
	if input.NotBefore != nil {
		delay = time.Until(*input.NotBefore)
	}
	scheduled := delay > maxSQSDelay
	if scheduled && w.scheduler == nil {
		return worlds.QueueResult{}, fmt.Errorf("workflow/aws: delivery %s from now exceeds SQS's 15-minute delay and requires Config.Scheduler", delay.Round(time.Second))
	}
	id := ulid.Make().String()
	body, err := json.Marshal(queueMessage{ID: id, Input: input})
	if err != nil {
		return worlds.QueueResult{}, err
	}
	inputJSON, err := json.Marshal(input)
	if err != nil {
		return worlds.QueueResult{}, err
	}
	item := merge(queueKey(id), map[string]ddb.AttributeValue{
		"input":     avs(string(inputJSON)),
		"createdAt": avs(stamp(time.Now())),
	})
	setTime(item, "notBefore", input.NotBefore)
	if scheduled {
		item["schedule"] = avs(scheduleName(id))
	}
	if err := checkItemSize(item); err != nil {
		return worlds.QueueResult{}, err
	}
	if _, err := w.dynamo.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           awsv2.String(w.config.TableName),
		Item:                item,
		ConditionExpression: awsv2.String("attribute_not_exists(pk)"),
	}); err != nil {
		return worlds.QueueResult{}, err
	}
	if scheduled {
		err = w.createSchedule(ctx, scheduleName(id), *input.NotBefore, string(body))
	} else {
		err = w.sendMessage(ctx, string(body), delay)
	}
	if err != nil {
		if cleanup := w.deleteQueueRecord(context.WithoutCancel(ctx), id); cleanup != nil {
			return worlds.QueueResult{}, errors.Join(err, cleanup)
		}
		return worlds.QueueResult{}, err
	}
	return worlds.QueueResult{MessageID: id}, nil
}

func (w *World) QueueBatch(ctx context.Context, input worlds.QueueBatchInput) ([]worlds.QueueResult, error) {
	results := make([]worlds.QueueResult, 0, len(input.Items))
	for _, item := range input.Items {
		result, err := w.Queue(ctx, item)
		if err != nil {
			return results, err
		}
		results = append(results, result)
	}
	return results, nil
}

// QueueCancel deletes the record, so a delivery already in flight is
// acknowledged without running, and deletes any schedule.
func (w *World) QueueCancel(ctx context.Context, input worlds.QueueCancelInput) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	result, err := w.dynamo.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName:           awsv2.String(w.config.TableName),
		Key:                 queueKey(input.MessageID),
		ConditionExpression: awsv2.String("attribute_exists(pk)"),
		ReturnValues:        ddb.ReturnValueAllOld,
	})
	if conditional(err) {
		return worlds.ErrQueueMessageNotFound
	}
	if err != nil {
		return err
	}
	// A World without Scheduler cannot delete the schedule; when it fires,
	// the consumer finds no record and acknowledges it.
	if schedule := read(result.Attributes, "schedule"); schedule != "" && w.scheduler != nil {
		return w.deleteSchedule(ctx, schedule)
	}
	return nil
}

func (w *World) queueRecord(ctx context.Context, id string) (queueRecord, bool, error) {
	item, found, err := w.item(ctx, queueKey(id))
	if err != nil || !found {
		return queueRecord{}, false, err
	}
	record := queueRecord{notBefore: readTimePtr(item, "notBefore"), schedule: read(item, "schedule")}
	if err := json.Unmarshal([]byte(read(item, "input")), &record.input); err != nil {
		return queueRecord{}, false, fmt.Errorf("workflow/aws: decode queue record %s: %w", id, err)
	}
	return record, true, nil
}

func (w *World) deleteQueueRecord(ctx context.Context, id string) error {
	_, err := w.dynamo.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: awsv2.String(w.config.TableName),
		Key:       queueKey(id),
	})
	return err
}
