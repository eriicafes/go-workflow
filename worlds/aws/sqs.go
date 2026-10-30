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
	"github.com/oklog/ulid/v2"
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
// authoritative, and the body only identifies it. Publish is set on a
// Scheduler publish to a FIFO queue, so its content-based deduplication ID
// differs from an earlier publish of the same delivery.
type queueMessage struct {
	ID      string            `json:"id"`
	Input   worlds.QueueInput `json:"input"`
	Publish string            `json:"publish,omitempty"`
}

// target is a queue deliveries are published to: the default queue, with an
// empty name, or a queue in Config.Queues.
type target struct {
	name string
	url  string
	arn  string
	fifo bool
}

// target returns the queue named name, or the default queue for "".
func (w *World) target(name string) target {
	if queue, ok := w.config.Queues[name]; ok && name != "" {
		return target{name: name, url: queue.URL, arn: queue.ARN, fifo: queue.FIFO}
	}
	defaultTarget := target{url: w.config.QueueURL}
	if w.config.Scheduler != nil {
		defaultTarget.arn = w.config.Scheduler.TargetARN
	}
	return defaultTarget
}

// sourceURL returns the URL of the queue whose ARN is eventSourceARN, or the
// default queue's.
func (w *World) sourceURL(eventSourceARN string) string {
	for _, queue := range w.config.Queues {
		if eventSourceARN != "" && queue.ARN == eventSourceARN {
			return queue.URL
		}
	}
	return w.config.QueueURL
}

// publish sends the delivery id to queue: through SQS when it is due within
// 15 minutes on a standard queue or due now on a FIFO queue, and otherwise
// through a one-time schedule named schedule.
func (w *World) publish(ctx context.Context, queue target, id string, input worlds.QueueInput, delay time.Duration, schedule string) error {
	if !scheduled(queue, delay) {
		body, err := json.Marshal(queueMessage{ID: id, Input: input})
		if err != nil {
			return err
		}
		send := &sqs.SendMessageInput{
			QueueUrl:    awsv2.String(queue.url),
			MessageBody: awsv2.String(string(body)),
		}
		if queue.fifo {
			send.MessageGroupId = awsv2.String(string(input.RunID))
			send.MessageDeduplicationId = awsv2.String(id)
		} else {
			send.DelaySeconds = delaySeconds(delay)
		}
		_, err = w.sqs.SendMessage(ctx, send)
		return err
	}
	message := queueMessage{ID: id, Input: input}
	if queue.fifo {
		message.Publish = ulid.Make().String()
	}
	body, err := json.Marshal(message)
	if err != nil {
		return err
	}
	return w.createSchedule(ctx, schedule, time.Now().Add(delay), string(body), queue, input.RunID)
}

// scheduled reports whether a delivery due after delay is published through
// Scheduler: beyond the SQS delay limit, or delayed at all on a FIFO queue.
func scheduled(queue target, delay time.Duration) bool {
	return delay > maxSQSDelay || (queue.fifo && delay > 0)
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
		queueURL := w.sourceURL(record.EventSourceARN)
		stop := w.holdVisibility(ctx, queueURL, record.ReceiptHandle)
		err := w.deliver(ctx, runtime, record.Body)
		stop()
		if err != nil {
			log.Printf("workflow/aws: delivery %s failed: %v", record.MessageId, err)
			w.setVisibility(ctx, queueURL, record.ReceiptHandle, retryBackoff(record.Attributes["ApproximateReceiveCount"]))
			response.BatchItemFailures = append(response.BatchItemFailures, events.SQSBatchItemFailure{ItemIdentifier: record.MessageId})
		}
	}
	return response
}

// holdVisibility extends the message's visibility until stop is called.
func (w *World) holdVisibility(ctx context.Context, queueURL, receiptHandle string) (stop func()) {
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
				w.setVisibility(ctx, queueURL, receiptHandle, visibilityExtension)
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

func (w *World) setVisibility(ctx context.Context, queueURL, receiptHandle string, timeout time.Duration) {
	if receiptHandle == "" {
		return
	}
	_, err := w.sqs.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          awsv2.String(queueURL),
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
			// Sent again to the queue it was routed to, at most 15 minutes
			// ahead on a standard queue and through Scheduler on a FIFO
			// queue. A new schedule name keeps it apart from a schedule
			// that already fired.
			queue := w.target(record.queue)
			if !queue.fifo {
				delay = min(delay, maxSQSDelay)
			}
			return w.publish(ctx, queue, message.ID, record.input, delay, scheduleName(ulid.Make().String()))
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
