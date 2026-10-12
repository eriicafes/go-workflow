package awsworld

import (
	"context"
	"encoding/base64"

	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/workflowops"
)

// ReadStream reads a byte range from the stream item. Stream contents are
// base64 encoded because DynamoDB string attributes must be valid UTF-8.
func (world *World) ReadStream(ctx context.Context, reference workflowops.Stream, offset int64) (workflowops.StreamRead, error) {
	if _, found, err := world.item(ctx, runKey(reference.RunID)); err != nil {
		return workflowops.StreamRead{}, err
	} else if !found {
		return workflowops.StreamRead{}, workflow.ErrRunNotFound
	}
	item, found, err := world.item(ctx, streamKey(reference.RunID, reference.Namespace))
	if err != nil {
		return workflowops.StreamRead{}, err
	}
	if !found {
		return workflowops.StreamRead{Next: offset}, nil
	}
	data, err := base64.RawStdEncoding.DecodeString(item["data"])
	if err != nil {
		return workflowops.StreamRead{}, err
	}
	if offset < 0 {
		offset = max(int64(len(data))+offset, 0)
	}
	if offset > int64(len(data)) {
		offset = int64(len(data))
	}
	return workflowops.StreamRead{
		Data:   append([]byte(nil), data[offset:]...),
		Next:   int64(len(data)),
		Closed: item["closed"] == "1",
	}, nil
}

// LockStream acquires the stream's exclusive writer lock.
func (world *World) LockStream(ctx context.Context, reference workflowops.Stream, lock string) (bool, error) {
	if _, found, err := world.item(ctx, runKey(reference.RunID)); err != nil {
		return false, err
	} else if !found {
		return false, workflow.ErrRunNotFound
	}
	key := streamKey(reference.RunID, reference.Namespace)
	_, err := world.dynamo.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:           awsv2.String(world.config.TableName),
		Key:                 key,
		UpdateExpression:    awsv2.String("SET #lock=:lock, #data=if_not_exists(#data,:empty), #closed=if_not_exists(#closed,:open)"),
		ConditionExpression: awsv2.String("(attribute_not_exists(#lock) OR #lock=:empty) AND (attribute_not_exists(#closed) OR #closed=:open)"),
		ExpressionAttributeNames: map[string]string{
			"#lock": "lock", "#data": "data", "#closed": "closed",
		},
		ExpressionAttributeValues: map[string]ddb.AttributeValue{
			":lock": avs(lock), ":empty": avs(""), ":open": avs("0"),
		},
	})
	if err == nil {
		return true, nil
	}
	if conditional(err) {
		return false, nil
	}
	return false, err
}

// AppendStream appends bytes held by lock with an optimistic compare-and-swap.
func (world *World) AppendStream(ctx context.Context, reference workflowops.Stream, lock string, data []byte) error {
	if _, found, err := world.item(ctx, runKey(reference.RunID)); err != nil {
		return err
	} else if !found {
		return workflow.ErrRunNotFound
	}

	key := streamKey(reference.RunID, reference.Namespace)
	for {
		item, found, err := world.item(ctx, key)
		if err != nil {
			return err
		}
		if !found {
			encoded := base64.RawStdEncoding.EncodeToString(data)
			_, err = world.dynamo.PutItem(ctx, &dynamodb.PutItemInput{
				TableName:           awsv2.String(world.config.TableName),
				Item:                merge(key, map[string]string{"data": encoded, "closed": "0", "lock": lock}),
				ConditionExpression: awsv2.String("attribute_not_exists(pk)"),
			})
			if err == nil {
				return nil
			}
			if conditional(err) {
				continue
			}
			return err
		}
		if item["closed"] == "1" || item["lock"] != lock {
			return workflowops.ErrCancelled
		}
		previous, err := base64.RawStdEncoding.DecodeString(item["data"])
		if err != nil {
			return err
		}
		next := base64.RawStdEncoding.EncodeToString(append(previous, data...))
		_, err = world.dynamo.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName:           awsv2.String(world.config.TableName),
			Key:                 key,
			UpdateExpression:    awsv2.String("SET #data=:next"),
			ConditionExpression: awsv2.String("#closed=:open AND #data=:previous AND #lock=:lock"),
			ExpressionAttributeNames: map[string]string{
				"#closed": "closed",
				"#data":   "data",
				"#lock":   "lock",
			},
			ExpressionAttributeValues: map[string]ddb.AttributeValue{
				":next":     avs(next),
				":open":     avs("0"),
				":previous": avs(item["data"]),
				":lock":     avs(lock),
			},
		})
		if err == nil {
			return nil
		}
		if conditional(err) {
			continue
		}
		return err
	}
}

// ReleaseStream releases token's writer lock and can close the stream.
func (world *World) ReleaseStream(ctx context.Context, reference workflowops.Stream, lock string, closeStream bool) error {
	if _, found, err := world.item(ctx, runKey(reference.RunID)); err != nil {
		return err
	} else if !found {
		return workflow.ErrRunNotFound
	}
	key := streamKey(reference.RunID, reference.Namespace)
	closed := "0"
	if closeStream {
		closed = "1"
	}
	_, err := world.dynamo.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:           awsv2.String(world.config.TableName),
		Key:                 key,
		UpdateExpression:    awsv2.String("SET #closed=:closed REMOVE #lock"),
		ConditionExpression: awsv2.String("#lock=:lock"),
		ExpressionAttributeNames: map[string]string{
			"#closed": "closed",
			"#lock":   "lock",
		},
		ExpressionAttributeValues: map[string]ddb.AttributeValue{
			":closed": avs(closed),
			":lock":   avs(lock),
		},
	})
	if conditional(err) {
		return workflowops.ErrCancelled
	}
	return err
}

// CloseStreams closes every stream in a terminal run partition.
func (world *World) CloseStreams(ctx context.Context, runID string) error {
	var start map[string]ddb.AttributeValue
	for {
		result, err := world.dynamo.Query(ctx, &dynamodb.QueryInput{
			TableName:              awsv2.String(world.config.TableName),
			ExclusiveStartKey:      start,
			KeyConditionExpression: awsv2.String("pk=:pk AND begins_with(sk,:stream)"),
			ExpressionAttributeValues: map[string]ddb.AttributeValue{
				":pk": avs(runPartitionKey(runID)), ":stream": avs("STREAM#"),
			},
		})
		if err != nil {
			return err
		}
		for _, item := range result.Items {
			_, err = world.dynamo.UpdateItem(ctx, &dynamodb.UpdateItemInput{
				TableName:        awsv2.String(world.config.TableName),
				Key:              map[string]ddb.AttributeValue{"pk": item["pk"], "sk": item["sk"]},
				UpdateExpression: awsv2.String("SET #closed=:closed REMOVE #lock"),
				ExpressionAttributeNames: map[string]string{
					"#closed": "closed",
				},
				ExpressionAttributeValues: map[string]ddb.AttributeValue{":closed": avs("1")},
			})
			if err != nil {
				return err
			}
		}
		if len(result.LastEvaluatedKey) == 0 {
			return nil
		}
		start = result.LastEvaluatedKey
	}
}
