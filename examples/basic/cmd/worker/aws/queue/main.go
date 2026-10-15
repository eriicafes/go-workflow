// queue runs the basic workflow worker from an SQS-triggered Lambda.
package main

import (
	"context"
	"log"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/examples/basic/flows"
	"github.com/eriicafes/go-workflow/worlds"
	awsworld "github.com/eriicafes/go-workflow/worlds/aws"
)

func main() {
	ctx := context.Background()
	config, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatal(err)
	}

	world, err := awsworld.New(
		awsworld.Config{
			BaseURL:   os.Getenv("WORKFLOW_BASE_URL"),
			TableName: os.Getenv("WORKFLOW_TABLE"),
			QueueURL:  os.Getenv("WORKFLOW_QUEUE_URL"),
			Scheduler: &awsworld.SchedulerConfig{
				Client:    scheduler.NewFromConfig(config),
				Group:     os.Getenv("WORKFLOW_SCHEDULER_GROUP"),
				TargetARN: os.Getenv("WORKFLOW_QUEUE_ARN"),
				RoleARN:   os.Getenv("WORKFLOW_SCHEDULER_ROLE_ARN"),
			},
		},
		dynamodb.NewFromConfig(config),
		sqs.NewFromConfig(config),
	)
	if err != nil {
		log.Fatal(err)
	}

	runners := worlds.Runners(
		workflow.Use(flows.GenerateReport),
		workflow.Use(flows.PublishReport),
	)

	lambda.Start(awsworld.Queue(world, runners))
}
