// Package awsworld provides the event-sourced AWS World.
package awsworld

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/aws/aws-lambda-go/events"
	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/testworld"
)

type Config struct {
	TableName, QueueURL string
	Scheduler           *SchedulerConfig
}
type SchedulerConfig struct {
	Client                    *scheduler.Client
	Group, TargetARN, RoleARN string
}
type World struct {
	*testworld.World
	config Config
	dynamo *dynamodb.Client
	sqs    *sqs.Client
}

func New(config Config, dynamoClient *dynamodb.Client, queueClient *sqs.Client, runners ...worlds.Runner) (*World, error) {
	if config.TableName == "" || config.QueueURL == "" || dynamoClient == nil || queueClient == nil {
		return nil, errors.New("workflow/aws: table, queue, DynamoDB, and SQS are required")
	}
	snapshot, err := loadSnapshot(context.Background(), config.TableName, dynamoClient)
	if err != nil {
		return nil, err
	}
	w := &World{config: config, dynamo: dynamoClient, sqs: queueClient}
	w.World = testworld.NewWithSnapshotAndQueue(snapshot, w.save, runners...)
	return w, nil
}

const statePK = "WORLD#STATE"

func loadSnapshot(ctx context.Context, table string, client *dynamodb.Client) (testworld.Snapshot, error) {
	result, err := client.GetItem(ctx, &dynamodb.GetItemInput{TableName: awsv2.String(table), Key: stateKey()})
	if err != nil {
		return testworld.Snapshot{}, err
	}
	if len(result.Item) == 0 {
		return testworld.Snapshot{}, nil
	}
	data, ok := result.Item["state"].(*ddbtypes.AttributeValueMemberB)
	if !ok {
		return testworld.Snapshot{}, errors.New("workflow/aws: invalid world state")
	}
	var snapshot testworld.Snapshot
	if err := json.Unmarshal(data.Value, &snapshot); err != nil {
		return testworld.Snapshot{}, errors.New("workflow/aws: invalid world state: " + err.Error())
	}
	return snapshot, nil
}

func stateKey() map[string]ddbtypes.AttributeValue {
	return map[string]ddbtypes.AttributeValue{
		"pk": &ddbtypes.AttributeValueMemberS{Value: statePK},
		"sk": &ddbtypes.AttributeValueMemberS{Value: statePK},
	}
}

func (w *World) save(snapshot testworld.Snapshot) {
	data, err := json.Marshal(snapshot)
	if err != nil {
		return
	}
	item := stateKey()
	item["state"] = &ddbtypes.AttributeValueMemberB{Value: data}
	_, _ = w.dynamo.PutItem(context.Background(), &dynamodb.PutItemInput{TableName: awsv2.String(w.config.TableName), Item: item})
}

type queueMessage struct {
	ID    string            `json:"id"`
	Input worlds.QueueInput `json:"input"`
}

// Queue persists the intent first, then publishes its message to SQS. The
// persisted record lets a worker acknowledge exactly the work it completed.
func (w *World) Queue(ctx context.Context, input worlds.QueueInput) (worlds.QueueResult, error) {
	result, err := w.World.Queue(ctx, input)
	if err != nil {
		return worlds.QueueResult{}, err
	}
	body, err := json.Marshal(queueMessage{ID: result.MessageID, Input: input})
	if err != nil {
		return worlds.QueueResult{}, err
	}
	message := &sqs.SendMessageInput{QueueUrl: awsv2.String(w.config.QueueURL), MessageBody: awsv2.String(string(body))}
	if input.NotBefore != nil {
		delay := time.Until(*input.NotBefore)
		if delay > 15*time.Minute {
			return worlds.QueueResult{}, errors.New("workflow/aws: timers over 15 minutes require scheduler delivery")
		}
		if delay > 0 {
			message.DelaySeconds = int32(delay.Round(time.Second) / time.Second)
		}
	}
	if _, err := w.sqs.SendMessage(ctx, message); err != nil {
		return worlds.QueueResult{}, err
	}
	return result, nil
}

func (w *World) QueueBatch(ctx context.Context, input worlds.QueueBatchInput) ([]worlds.QueueResult, error) {
	result := make([]worlds.QueueResult, 0, len(input.Items))
	for _, item := range input.Items {
		value, err := w.Queue(ctx, item)
		if err != nil {
			return result, err
		}
		result = append(result, value)
	}
	return result, nil
}

func HTTP(world *World) http.Handler { return worlds.HTTPHandler(world, nil, "") }

// Queue returns an SQS Lambda handler for the provider's durable queue.
func Queue(world *World, runners worlds.RunnerSet) func(context.Context, events.SQSEvent) (events.SQSEventResponse, error) {
	return func(ctx context.Context, event events.SQSEvent) (events.SQSEventResponse, error) {
		runtime := worlds.NewRuntime(world, runners)
		response := events.SQSEventResponse{}
		for _, record := range event.Records {
			var message queueMessage
			if err := json.Unmarshal([]byte(record.Body), &message); err != nil || message.ID == "" {
				response.BatchItemFailures = append(response.BatchItemFailures, events.SQSBatchItemFailure{ItemIdentifier: record.MessageId})
				continue
			}
			if err := runtime.Execute(ctx, message.Input); err != nil {
				response.BatchItemFailures = append(response.BatchItemFailures, events.SQSBatchItemFailure{ItemIdentifier: record.MessageId})
				continue
			}
			if err := world.QueueCancel(ctx, worlds.QueueCancelInput{MessageID: message.ID}); err != nil && !errors.Is(err, worlds.ErrQueueMessageNotFound) {
				response.BatchItemFailures = append(response.BatchItemFailures, events.SQSBatchItemFailure{ItemIdentifier: record.MessageId})
			}
		}
		return response, nil
	}
}
