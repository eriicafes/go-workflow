// api runs the app-facing workflow API as an AWS Lambda Function URL.
package main

import (
	"context"
	"log"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-lambda-go/lambdaurl"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/eriicafes/go-workflow/worlds/aws"
)

func main() {
	ctx := context.Background()
	awsConfig, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatal(err)
	}

	world, err := awsworld.New(
		awsworld.Config{
			BaseURL:   os.Getenv("WORKFLOW_BASE_URL"),
			TableName: os.Getenv("WORKFLOW_TABLE"),
			QueueURL:  os.Getenv("WORKFLOW_QUEUE_URL"),
		},
		dynamodb.NewFromConfig(awsConfig),
		sqs.NewFromConfig(awsConfig),
	)
	if err != nil {
		log.Fatal(err)
	}

	lambda.Start(lambdaurl.Wrap(awsworld.HTTP(world)))
}
