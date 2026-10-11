// Package awsworld contains the AWS deployment integration for workflow worlds.
package awsworld

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/aws/aws-lambda-go/events"
	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/workflowops"
)

// HTTP returns the worker HTTP API for an API Gateway or Lambda Function URL.
func HTTP(world *World) http.Handler {
	return workflow.NewHTTPHandler(world)
}

// Queue returns a Lambda SQS handler. It reports only failed records so Lambda
// retries them while successfully processed records are acknowledged.
func Queue(world *World, runners workflowops.RunnerSet) func(context.Context, events.SQSEvent) (events.SQSEventResponse, error) {
	engine := workflowops.NewEngine(world, runners)
	return func(ctx context.Context, event events.SQSEvent) (events.SQSEventResponse, error) {
		response := events.SQSEventResponse{}
		for _, message := range event.Records {
			var task workflowops.Task
			if err := json.Unmarshal([]byte(message.Body), &task); err != nil || task.RunID == "" {
				response.BatchItemFailures = append(response.BatchItemFailures, events.SQSBatchItemFailure{ItemIdentifier: message.MessageId})
				continue
			}
			lease, err := world.ClaimTask(ctx, task)
			if err == nil && lease != nil {
				err = engine.Execute(ctx, lease)
			}
			if err != nil && !errors.Is(err, workflowops.ErrCancelled) {
				response.BatchItemFailures = append(response.BatchItemFailures, events.SQSBatchItemFailure{ItemIdentifier: message.MessageId})
			}
		}
		return response, nil
	}
}
