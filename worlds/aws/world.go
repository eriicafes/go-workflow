// Package awsworld provides a World backed by DynamoDB, SQS, and EventBridge
// Scheduler.
//
// All state lives in one DynamoDB table. Each run partition (RUN#<id>) holds
// the run view (META, which also stores the event count), its events
// (EVENT#<slot>), steps (STEP#<id>), completed-wait markers (WAIT#<id>), and
// streams (STREAM#<name> and CHUNK#<name>#<index>). Hooks (HOOK#<id>), hook
// tokens (TOKEN#<token>), and queue records (QUEUE#<id>) have their own
// partitions. Appends use optimistic concurrency: a TransactWriteItems that
// requires the event count read beforehand, so any number of processes can
// share one table.
package awsworld

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/eriicafes/go-workflow/worlds"
)

// Config identifies the AWS resources used by the World.
type Config struct {
	// BaseURL is the absolute public URL of HTTP(world), including
	// /v1/workflow. Webhooks created by this World use it as their URL base.
	BaseURL string
	// TableName is the DynamoDB table created as described by CreateTable.
	TableName string
	// QueueURL is the SQS queue that delivers queue messages.
	QueueURL string
	// Scheduler delivers messages due more than 15 minutes in the future,
	// beyond SQS's delay limit. Without it such messages are rejected.
	Scheduler *SchedulerConfig
}

// SchedulerConfig configures EventBridge Scheduler for long delays.
type SchedulerConfig struct {
	// Client creates and deletes one-time schedules.
	Client *scheduler.Client
	// Group is the schedule group. An empty value uses "default".
	Group string
	// TargetARN is the ARN of the SQS queue at QueueURL.
	TargetARN string
	// RoleARN is the IAM role Scheduler assumes to send to TargetARN.
	RoleARN string
}

// World is a multi-process World stored in DynamoDB.
type World struct {
	config    Config
	dynamo    dynamoAPI
	sqs       sqsAPI
	scheduler schedulerAPI
}

// New returns a World using the supplied clients, which carry the region,
// credentials, retry policy, and endpoints.
func New(config Config, dynamoClient *dynamodb.Client, sqsClient *sqs.Client) (*World, error) {
	if dynamoClient == nil || sqsClient == nil {
		return nil, errors.New("workflow/aws: DynamoDB and SQS clients are required")
	}
	var schedulerClient schedulerAPI
	if config.Scheduler != nil && config.Scheduler.Client != nil {
		schedulerClient = config.Scheduler.Client
	}
	return newWorld(config, dynamoClient, sqsClient, schedulerClient)
}

// Close is a no-op; the World holds no resources beyond the supplied clients.
func (*World) Close() {}

func (w *World) BaseURL() string             { return w.config.BaseURL }
func (*World) SpecVersion() int              { return worlds.SpecVersion }
func (w *World) Analytics() worlds.Analytics { return analytics{world: w} }
func (*World) Capabilities() worlds.Capabilities {
	return worlds.Capabilities{Analytics: true, SetAttributes: true}
}

// HTTP returns the RPC and webhook endpoints under /v1/workflow.
func HTTP(world *World) http.Handler { return worlds.HTTPHandler(world, nil, "") }

// RPC returns only the CBOR RPC endpoint.
func RPC(world *World) http.Handler { return worlds.RPCHandler(world) }

// Webhook returns only the token webhook endpoint.
func Webhook(world *World) http.Handler { return worlds.WebhookHandler(world) }

// Queue returns a Lambda handler for an SQS event source mapping configured
// with ReportBatchItemFailures. Failed records are reported individually so
// SQS redelivers only those.
func Queue(world *World, runners worlds.RunnerSet) func(context.Context, events.SQSEvent) (events.SQSEventResponse, error) {
	runtime := worlds.NewRuntime(world, runners)
	return func(ctx context.Context, event events.SQSEvent) (events.SQSEventResponse, error) {
		return world.consume(ctx, runtime, event), nil
	}
}

func newWorld(config Config, dynamoClient dynamoAPI, sqsClient sqsAPI, schedulerClient schedulerAPI) (*World, error) {
	if config.BaseURL == "" || config.TableName == "" || config.QueueURL == "" {
		return nil, errors.New("workflow/aws: base URL, table name, and queue URL are required")
	}
	baseURL, err := worlds.NormalizeBaseURL(config.BaseURL)
	if err != nil {
		return nil, fmt.Errorf("workflow/aws: invalid base URL: %w", err)
	}
	config.BaseURL = baseURL
	if config.Scheduler != nil && (schedulerClient == nil || config.Scheduler.TargetARN == "" || config.Scheduler.RoleARN == "") {
		return nil, errors.New("workflow/aws: scheduler requires a client, target ARN, and role ARN")
	}
	return &World{config: config, dynamo: dynamoClient, sqs: sqsClient, scheduler: schedulerClient}, nil
}
