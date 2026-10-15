package awsworld

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-lambda-go/events"
	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/eriicafes/go-workflow/worlds"
)

// sqsAPI is the subset of the SQS client used by World.
type sqsAPI interface {
	SendMessage(context.Context, *sqs.SendMessageInput, ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
}

// maxSQSDelay is the longest DelaySeconds SQS accepts.
const maxSQSDelay = 15 * time.Minute

// queueMessage is the SQS and Scheduler message body. The queue item stays
// authoritative; the body only identifies it.
type queueMessage struct {
	ID    string            `json:"id"`
	Input worlds.QueueInput `json:"input"`
}

func (w *World) sendMessage(ctx context.Context, body string, delay time.Duration) error {
	_, err := w.sqs.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:     awsv2.String(w.config.QueueURL),
		MessageBody:  awsv2.String(body),
		DelaySeconds: delaySeconds(delay),
	})
	return err
}

// consume handles one SQS batch. Records whose queue item is gone were
// cancelled or already completed and are acknowledged. Records that arrive
// before NotBefore are sent again with the remaining delay.
func (w *World) consume(ctx context.Context, runtime *worlds.Runtime, event events.SQSEvent) events.SQSEventResponse {
	response := events.SQSEventResponse{}
	for _, record := range event.Records {
		if err := w.deliver(ctx, runtime, record.Body); err != nil {
			response.BatchItemFailures = append(response.BatchItemFailures, events.SQSBatchItemFailure{ItemIdentifier: record.MessageId})
		}
	}
	return response
}

func (w *World) deliver(ctx context.Context, runtime *worlds.Runtime, body string) error {
	var message queueMessage
	if err := json.Unmarshal([]byte(body), &message); err != nil || message.ID == "" {
		return fmt.Errorf("workflow/aws: invalid queue message: %q", body)
	}
	record, found, err := w.queueRecord(ctx, message.ID)
	if err != nil || !found {
		return err
	}
	if record.notBefore != nil {
		if delay := time.Until(*record.notBefore); delay > 0 {
			return w.sendMessage(ctx, body, delay)
		}
	}
	if err := runtime.Execute(ctx, record.input); err != nil {
		return err
	}
	return w.deleteQueueRecord(ctx, message.ID)
}

// delaySeconds rounds delay up to whole seconds within SQS's range.
func delaySeconds(delay time.Duration) int32 {
	if delay <= 0 {
		return 0
	}
	delay = min(delay, maxSQSDelay)
	return int32((delay + time.Second - 1) / time.Second)
}
