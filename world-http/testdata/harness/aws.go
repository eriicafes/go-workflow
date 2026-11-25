package main

import (
	"cmp"
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-lambda-go/events"
	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/eriicafes/go-workflow/worlds"
	awsworld "github.com/eriicafes/go-workflow/worlds/aws"
)

// awsPollers is how many messages the AWS World's queue is read for at once,
// as concurrent Lambda invocations of its queue handler would.
const awsPollers = 16

// openAWS returns the AWS World on a fresh DynamoDB table and SQS queue, at
// DYNAMODB_ENDPOINT and SQS_ENDPOINT, by default DynamoDB Local and ElasticMQ
// on their usual ports. Pollers hand the queue's messages to the World's
// queue handler, as Lambda's SQS event source does, and delete the ones it
// does not report as failed. Closing it deletes the table and the queue.
func openAWS(ctx context.Context, baseURL string, executor worlds.Executor) (worlds.World, func(), error) {
	credentials := awsv2.CredentialsProviderFunc(func(context.Context) (awsv2.Credentials, error) {
		return awsv2.Credentials{AccessKeyID: "test", SecretAccessKey: "test"}, nil
	})
	dynamo := dynamodb.New(dynamodb.Options{
		Region:       "us-east-1",
		BaseEndpoint: awsv2.String(cmp.Or(os.Getenv("DYNAMODB_ENDPOINT"), "http://localhost:8000")),
		Credentials:  credentials,
		// DynamoDB Local occasionally closes a connection mid-response, which
		// the SDK retries as a transient error.
		RetryMaxAttempts: 5,
	})
	queues := sqs.New(sqs.Options{
		Region:       "us-east-1",
		BaseEndpoint: awsv2.String(cmp.Or(os.Getenv("SQS_ENDPOINT"), "http://localhost:9324")),
		Credentials:  credentials,
	})
	name := fmt.Sprintf("harness_%d_%d", os.Getpid(), time.Now().UnixNano())
	if err := awsworld.CreateTable(ctx, dynamo, name); err != nil {
		return nil, nil, err
	}
	created, err := queues.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: awsv2.String(name)})
	if err != nil {
		deleteTable(dynamo, name)
		return nil, nil, err
	}
	queueURL := awsv2.ToString(created.QueueUrl)
	world, err := awsworld.New(awsworld.Config{BaseURL: baseURL, TableName: name, QueueURL: queueURL}, dynamo, queues)
	if err != nil {
		deleteQueue(queues, queueURL)
		deleteTable(dynamo, name)
		return nil, nil, err
	}
	handler := awsworld.Queue(world, executor)
	polling, stop := context.WithCancel(ctx)
	var pollers sync.WaitGroup
	for range awsPollers {
		pollers.Go(func() { pollSQS(polling, queues, queueURL, handler) })
	}
	return world, func() {
		stop()
		pollers.Wait()
		deleteQueue(queues, queueURL)
		deleteTable(dynamo, name)
	}, nil
}

// pollSQS receives queueURL's messages one at a time until ctx is done, hands
// each to handler as an SQS event, and deletes it unless handler reports it
// as failed, which leaves it for its next delivery.
func pollSQS(ctx context.Context, client *sqs.Client, queueURL string, handler func(context.Context, events.SQSEvent) (events.SQSEventResponse, error)) {
	for ctx.Err() == nil {
		received, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:                    awsv2.String(queueURL),
			MaxNumberOfMessages:         1,
			WaitTimeSeconds:             1,
			MessageSystemAttributeNames: []sqstypes.MessageSystemAttributeName{sqstypes.MessageSystemAttributeNameAll},
		})
		if err != nil {
			if ctx.Err() == nil {
				log.Printf("harness: receive from SQS: %v", err)
				time.Sleep(100 * time.Millisecond)
			}
			continue
		}
		var event events.SQSEvent
		for _, message := range received.Messages {
			event.Records = append(event.Records, events.SQSMessage{
				MessageId:     awsv2.ToString(message.MessageId),
				ReceiptHandle: awsv2.ToString(message.ReceiptHandle),
				Body:          awsv2.ToString(message.Body),
				Attributes:    message.Attributes,
			})
		}
		if len(event.Records) == 0 {
			continue
		}
		response, err := handler(ctx, event)
		if err != nil {
			log.Printf("harness: SQS handler: %v", err)
			continue
		}
		failed := make(map[string]bool, len(response.BatchItemFailures))
		for _, failure := range response.BatchItemFailures {
			failed[failure.ItemIdentifier] = true
		}
		for _, record := range event.Records {
			if failed[record.MessageId] {
				continue
			}
			if _, err := client.DeleteMessage(context.WithoutCancel(ctx), &sqs.DeleteMessageInput{QueueUrl: awsv2.String(queueURL), ReceiptHandle: awsv2.String(record.ReceiptHandle)}); err != nil {
				log.Printf("harness: delete SQS message %s: %v", record.MessageId, err)
			}
		}
	}
}

func deleteTable(client *dynamodb.Client, name string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := client.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: awsv2.String(name)}); err != nil {
		log.Printf("harness: delete table %s: %v", name, err)
	}
}

func deleteQueue(client *sqs.Client, queueURL string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := client.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: awsv2.String(queueURL)}); err != nil {
		log.Printf("harness: delete queue %s: %v", queueURL, err)
	}
}
