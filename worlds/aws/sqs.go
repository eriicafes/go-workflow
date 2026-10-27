package awsworld

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"time"

	"github.com/aws/aws-lambda-go/events"
	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/eriicafes/go-workflow/worlds"
)

// sqsAPI is the subset of the SQS client used by World.
type sqsAPI interface {
	SendMessage(context.Context, *sqs.SendMessageInput, ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
	ChangeMessageVisibility(context.Context, *sqs.ChangeMessageVisibilityInput, ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
}

const (
	// maxSQSDelay is the longest DelaySeconds SQS accepts.
	maxSQSDelay = 15 * time.Minute
	// visibilityHeartbeat is how often a running delivery extends its
	// message's visibility, by visibilityExtension each time, so the message
	// is not redelivered to another consumer while it runs.
	visibilityHeartbeat = 30 * time.Second
	visibilityExtension = 90 * time.Second
	// maxRetryBackoff caps how long a failed delivery waits before SQS
	// redelivers it.
	maxRetryBackoff = 15 * time.Minute
)

// queueMessage is the SQS and Scheduler message body. The queue item stays
// authoritative, and the body only identifies it.
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
// cancelled or already completed, and records that are not queue messages,
// are acknowledged. Records that arrive before NotBefore are sent again with
// the remaining delay. A failed record is retried by SQS after a backoff that
// grows with its receive count, so a redrive policy still moves it to a
// dead-letter queue.
func (w *World) consume(ctx context.Context, runtime *worlds.Runtime, event events.SQSEvent) events.SQSEventResponse {
	response := events.SQSEventResponse{}
	for _, record := range event.Records {
		stop := w.holdVisibility(ctx, record.ReceiptHandle)
		err := w.deliver(ctx, runtime, record.Body)
		stop()
		if err != nil {
			log.Printf("workflow/aws: delivery %s failed: %v", record.MessageId, err)
			w.setVisibility(ctx, record.ReceiptHandle, retryBackoff(record.Attributes["ApproximateReceiveCount"]))
			response.BatchItemFailures = append(response.BatchItemFailures, events.SQSBatchItemFailure{ItemIdentifier: record.MessageId})
		}
	}
	return response
}

// holdVisibility extends the message's visibility until stop is called.
func (w *World) holdVisibility(ctx context.Context, receiptHandle string) (stop func()) {
	if receiptHandle == "" {
		return func() {}
	}
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(visibilityHeartbeat)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				w.setVisibility(ctx, receiptHandle, visibilityExtension)
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

func (w *World) setVisibility(ctx context.Context, receiptHandle string, timeout time.Duration) {
	if receiptHandle == "" {
		return
	}
	_, err := w.sqs.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          awsv2.String(w.config.QueueURL),
		ReceiptHandle:     awsv2.String(receiptHandle),
		VisibilityTimeout: int32(timeout / time.Second),
	})
	if err != nil && ctx.Err() == nil {
		log.Printf("workflow/aws: change message visibility: %v", err)
	}
}

// retryBackoff doubles from one second with each receive, up to
// maxRetryBackoff.
func retryBackoff(receiveCount string) time.Duration {
	count, _ := strconv.Atoi(receiveCount)
	backoff := time.Second << min(max(count-1, 0), 10)
	return min(backoff, maxRetryBackoff)
}

func (w *World) deliver(ctx context.Context, runtime *worlds.Runtime, body string) error {
	var message queueMessage
	if err := json.Unmarshal([]byte(body), &message); err != nil || message.ID == "" {
		// Redelivering a body that is not a queue message cannot succeed.
		log.Printf("workflow/aws: dropping invalid queue message: %q", body)
		return nil
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
	input := record.input
	input.MessageID = message.ID
	if err := runtime.Execute(ctx, input); err != nil {
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
