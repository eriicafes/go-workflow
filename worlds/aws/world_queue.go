package awsworld

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
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
	// queue is the name of the queue in Config.Queues the delivery was
	// routed to, or empty for the default queue.
	queue string
	// expire marks the delivery that deletes a finished run's data.
	expire bool
	// expiresAt is when the message stops living, from its Retention. Nil
	// keeps it until it is acknowledged.
	expiresAt *time.Time
}

// Queue sends a delivery due more than 15 minutes ahead through
// Config.Scheduler when it is set, and otherwise sends it again on each early
// arrival until it is due. It routes the
// delivery to the queue Config.Routes names for its workflow. A delivery with
// an IdempotencyKey is stored once while its message lives, for
// worlds.MinDeduplicationWindow from its first send: a repeat returns the
// first message's ID.
func (w *World) Queue(ctx context.Context, input worlds.QueueInput) (worlds.QueueResult, error) {
	// Store a QUEUE# record, claim the idempotency key for it, and publish its
	// ID through SQS with a delay of up to 15 minutes, or otherwise through a
	// one-time Scheduler schedule whose name is kept on the record. When
	// publishing fails the record and the claim are removed.
	if err := ctx.Err(); err != nil {
		return worlds.QueueResult{}, err
	}
	input.MessageID, input.Attempt, input.DeliveryCount = "", 0, 0
	// The message is created now, whatever the caller set.
	input.CreatedAt = time.Now().UTC().Truncate(time.Millisecond)
	queue := w.target(w.config.Routes[input.WorkflowName])
	var delay time.Duration
	if input.NotBefore != nil {
		delay = time.Until(*input.NotBefore)
	}
	publishDelay := delay
	if w.scheduler == nil {
		// Without Scheduler a delivery due later than SQS's longest delay
		// arrives after it, early, and deliver sends it again for the rest.
		publishDelay = min(delay, maxSQSDelay)
	}
	id := ulid.Make().String()
	inputJSON, err := json.Marshal(input)
	if err != nil {
		return worlds.QueueResult{}, err
	}
	now := time.Now()
	item := merge(queueKey(id), map[string]ddb.AttributeValue{
		"input":     avs(string(inputJSON)),
		"createdAt": avs(stamp(now)),
	})
	setTime(item, "notBefore", input.NotBefore)
	if input.Retention > 0 {
		// A message lives for its Retention from when it is first due.
		expiresAt := now.Add(max(delay, 0) + input.Retention)
		setTime(item, "expiresAt", &expiresAt)
	}
	if scheduled(publishDelay) {
		item["schedule"] = avs(scheduleName(id))
	}
	if queue.name != "" {
		item["queue"] = avs(queue.name)
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
	if input.IdempotencyKey != "" {
		existing, err := w.claimKey(ctx, input.RunID, input.IdempotencyKey, id, now)
		if err == nil && existing != "" {
			err = w.deleteQueueRecord(ctx, id)
			return worlds.QueueResult{MessageID: existing}, err
		}
		if err != nil {
			if cleanup := w.deleteQueueRecord(context.WithoutCancel(ctx), id); cleanup != nil {
				return worlds.QueueResult{}, errors.Join(err, cleanup)
			}
			return worlds.QueueResult{}, err
		}
	}
	if err := w.publish(ctx, queue, id, input, publishDelay, scheduleName(id)); err != nil {
		cleanup := w.deleteQueueRecord(context.WithoutCancel(ctx), id)
		if input.IdempotencyKey != "" {
			cleanup = errors.Join(cleanup, w.releaseKey(context.WithoutCancel(ctx), input.RunID, input.IdempotencyKey, id))
		}
		if cleanup != nil {
			return worlds.QueueResult{}, errors.Join(err, cleanup)
		}
		return worlds.QueueResult{}, err
	}
	return worlds.QueueResult{MessageID: id}, nil
}

// QueueBatch queues the items one after another, and reports an item that
// failed in its result.
func (w *World) QueueBatch(ctx context.Context, input worlds.QueueBatchInput) ([]worlds.QueueResult, error) {
	return worlds.QueueEach(ctx, w.Queue, input.Items), nil
}

// maxKeyAttempts bounds how often claimKey retries a claim that races with
// another Queue of the same key.
const maxKeyAttempts = 5

// claimKey claims an idempotency key of runID for message id with a DEDUPE#
// item that lasts worlds.MinDeduplicationWindow. It returns the ID of the
// message that holds the key instead, when that message still lives. A claim
// whose window has passed, or whose message was acknowledged or never
// published, is taken over.
func (w *World) claimKey(ctx context.Context, runID worlds.RunID, key, id string, now time.Time) (string, error) {
	until := now.Add(worlds.MinDeduplicationWindow)
	item := merge(dedupeKey(runID, key), map[string]ddb.AttributeValue{
		"messageId":   avs(id),
		"dedupeUntil": avn(until.UnixMilli()),
		"ttl":         avn(until.Unix()),
	})
	for range maxKeyAttempts {
		_, err := w.dynamo.PutItem(ctx, &dynamodb.PutItemInput{
			TableName:                 awsv2.String(w.config.TableName),
			Item:                      item,
			ConditionExpression:       awsv2.String("attribute_not_exists(pk) OR dedupeUntil < :now"),
			ExpressionAttributeValues: map[string]ddb.AttributeValue{":now": avn(time.Now().UnixMilli())},
		})
		if err == nil || !conditional(err) {
			return "", err
		}
		claim, found, err := w.item(ctx, dedupeKey(runID, key))
		if err != nil {
			return "", err
		}
		if !found {
			continue
		}
		holder := read(claim, "messageId")
		if holder == id {
			return "", nil
		}
		if _, lives, err := w.queueRecord(ctx, holder); err != nil {
			return "", err
		} else if lives {
			return holder, nil
		}
		// The holder is gone, so the key is free: take it over unless
		// another Queue did first.
		_, err = w.dynamo.PutItem(ctx, &dynamodb.PutItemInput{
			TableName:                 awsv2.String(w.config.TableName),
			Item:                      item,
			ConditionExpression:       awsv2.String("messageId = :holder"),
			ExpressionAttributeValues: map[string]ddb.AttributeValue{":holder": avs(holder)},
		})
		if err == nil || !conditional(err) {
			return "", err
		}
	}
	return "", fmt.Errorf("workflow/aws: idempotency key %q of run %s is contended", key, runID)
}

// releaseKey removes message id's claim on an idempotency key of runID.
func (w *World) releaseKey(ctx context.Context, runID worlds.RunID, key, id string) error {
	_, err := w.dynamo.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName:                 awsv2.String(w.config.TableName),
		Key:                       dedupeKey(runID, key),
		ConditionExpression:       awsv2.String("messageId = :id"),
		ExpressionAttributeValues: map[string]ddb.AttributeValue{":id": avs(id)},
	})
	if conditional(err) {
		return nil
	}
	return err
}

// dedupeKey is the key of the claim on an idempotency key of runID. The key
// is hashed, so any key fits DynamoDB's key size.
func dedupeKey(runID worlds.RunID, idempotencyKey string) map[string]ddb.AttributeValue {
	sum := sha256.Sum256([]byte(idempotencyKey))
	return key("DEDUPE#"+string(runID)+"#"+base64.RawURLEncoding.EncodeToString(sum[:]), "META")
}

func (w *World) queueRecord(ctx context.Context, id string) (queueRecord, bool, error) {
	item, found, err := w.item(ctx, queueKey(id))
	if err != nil || !found {
		return queueRecord{}, false, err
	}
	record := queueRecord{notBefore: readTimePtr(item, "notBefore"), schedule: read(item, "schedule"), queue: read(item, "queue"), expire: readBool(item, "expire"), expiresAt: readTimePtr(item, "expiresAt")}
	if err := json.Unmarshal([]byte(read(item, "input")), &record.input); err != nil {
		return queueRecord{}, false, fmt.Errorf("workflow/aws: decode queue record %s: %w", id, err)
	}
	return record, true, nil
}

// countDelivery counts a delivery of message id that is about to execute and
// returns its DeliveryCount and Attempt: every delivery counts, and Attempt
// leaves out the handoffs. found is false when the record is gone.
func (w *World) countDelivery(ctx context.Context, id string) (deliveryCount, attempt int, found bool, err error) {
	output, err := w.dynamo.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 awsv2.String(w.config.TableName),
		Key:                       queueKey(id),
		UpdateExpression:          awsv2.String("ADD deliveries :one"),
		ConditionExpression:       awsv2.String("attribute_exists(pk)"),
		ExpressionAttributeValues: map[string]ddb.AttributeValue{":one": avn(1)},
		ReturnValues:              ddb.ReturnValueAllNew,
	})
	if conditional(err) {
		return 0, 0, false, nil
	}
	if err != nil {
		return 0, 0, false, err
	}
	deliveries := int(readInt(output.Attributes, "deliveries"))
	handoffs := int(readInt(output.Attributes, "handoffs"))
	return deliveries, max(deliveries-handoffs, 1), true, nil
}

// markRedelivery records on message id's record that it is delivered again
// after after: a handoff (zero) is counted, so the next delivery's Attempt
// leaves it out, and a later redelivery is due at its time, so an early
// arrival waits for it. found is false when the record is gone.
func (w *World) markRedelivery(ctx context.Context, id string, after time.Duration) (found bool, err error) {
	update := &dynamodb.UpdateItemInput{
		TableName:           awsv2.String(w.config.TableName),
		Key:                 queueKey(id),
		ConditionExpression: awsv2.String("attribute_exists(pk)"),
	}
	if after == 0 {
		update.UpdateExpression = awsv2.String("ADD handoffs :one")
		update.ExpressionAttributeValues = map[string]ddb.AttributeValue{":one": avn(1)}
	} else {
		update.UpdateExpression = awsv2.String("SET notBefore = :due")
		update.ExpressionAttributeValues = map[string]ddb.AttributeValue{":due": avs(stamp(time.Now().Add(after)))}
	}
	_, err = w.dynamo.UpdateItem(ctx, update)
	if conditional(err) {
		return false, nil
	}
	return err == nil, err
}

func (w *World) deleteQueueRecord(ctx context.Context, id string) error {
	_, err := w.dynamo.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: awsv2.String(w.config.TableName),
		Key:       queueKey(id),
	})
	return err
}
