// Package awsworld implements a World on DynamoDB, SQS, and EventBridge
// Scheduler, for deployments on AWS Lambda or other AWS compute where any
// number of functions and processes share one table.
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

// Config configures the AWS World.
type Config struct {
	// BaseURL is the absolute public URL of HTTP(world), including
	// /v1/workflow. Webhooks created by this World use it as their URL base.
	// It is required.
	BaseURL string
	// TableName is the DynamoDB table, laid out as CreateTable creates it. It
	// is required.
	TableName string
	// QueueURL is the URL of the SQS standard queue that carries deliveries. It
	// is required.
	QueueURL string
	// Scheduler, when set, delivers work due more than 15 minutes ahead, beyond
	// the SQS delay limit. Without it, queueing such a delivery returns an
	// error.
	Scheduler *SchedulerConfig
}

// SchedulerConfig configures EventBridge Scheduler to deliver work due more
// than 15 minutes ahead. Each such delivery gets a one-time schedule that
// sends it to the SQS queue when it is due.
type SchedulerConfig struct {
	// Client creates and deletes schedules. It is required.
	Client *scheduler.Client
	// Group is the schedule group. Empty uses "default".
	Group string
	// TargetARN is the ARN of the SQS queue at Config.QueueURL. It is required.
	TargetARN string
	// RoleARN is the IAM role Scheduler assumes to send to TargetARN. It is
	// required.
	RoleARN string
}

// World is the AWS World. It is safe for concurrent use, and any number of
// Worlds in any number of processes may share one table.
type World struct {
	config    Config
	dynamo    dynamoAPI
	sqs       sqsAPI
	scheduler schedulerAPI
}

// New returns a World that uses the supplied clients, which carry the region,
// credentials, retry policy, and endpoints. It does not contact AWS. It returns
// an error when a client, BaseURL, TableName, or QueueURL is missing, or when
// Scheduler lacks its client, target ARN, or role ARN.
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

// Close does nothing, because the World holds no resources beyond the
// supplied clients, which the caller keeps owning.
func (*World) Close()                        {}
func (w *World) BaseURL() string             { return w.config.BaseURL }
func (*World) SpecVersion() int              { return worlds.SpecVersion }
func (w *World) Analytics() worlds.Analytics { return analytics{world: w} }
func (*World) Capabilities() worlds.Capabilities {
	return worlds.Capabilities{Analytics: true, SetAttributes: true}
}

// HTTP returns a handler serving the RPC and webhook endpoints under
// /v1/workflow. It does not authenticate requests.
func HTTP(world *World) http.Handler { return worlds.HTTPHandler(world, "") }

// RPC returns a handler serving only the RPC endpoint.
func RPC(world *World) http.Handler { return worlds.RPCHandler(world) }

// Webhook returns a handler serving only the webhook endpoint, authorized by
// the hook token in its URL.
func Webhook(world *World) http.Handler { return worlds.WebhookHandler(world) }

// Queue returns the SQS Lambda handler, which executes deliveries for the
// workflows of runners. Attach it through an SQS event source mapping with
// ReportBatchItemFailures enabled. It reports each failed delivery
// individually, so SQS redelivers only those.
func Queue(world *World, runners worlds.RunnerSet) func(context.Context, events.SQSEvent) (events.SQSEventResponse, error) {
	runtime := worlds.NewRuntime(world, runners)
	return func(ctx context.Context, event events.SQSEvent) (events.SQSEventResponse, error) {
		return world.consume(ctx, runtime, event), nil
	}
}
