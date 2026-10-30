package vercelworld

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/eriicafes/go-workflow/worlds"
)

// queueRecord is the durable delivery stored at p:queue:<id>. Vercel Queues
// and QStash messages carry only the ID.
type queueRecord struct {
	QueueName string `json:"queueName,omitempty"`
	// Topic is the Vercel Queues topic the delivery's workflow is routed to;
	// empty is Config.Topic.
	Topic           string         `json:"topic,omitempty"`
	RunID           worlds.RunID   `json:"runId"`
	StepID          *worlds.StepID `json:"stepId,omitempty"`
	NotBefore       *time.Time     `json:"notBefore,omitempty"`
	IdempotencyKey  string         `json:"idempotencyKey,omitempty"`
	QStashMessageID string         `json:"qstashMessageId,omitempty"`
	// DeploymentID is the deployment the message is published to, kept so a
	// QStash timer is handed back to the same deployment.
	DeploymentID                 string `json:"deploymentId,omitempty"`
	DeploymentMismatchRetryCount int    `json:"deploymentMismatchRetryCount,omitempty"`
	ReplayDivergenceCount        int    `json:"replayDivergenceCount,omitempty"`
}

func (record queueRecord) input() worlds.QueueInput {
	return worlds.QueueInput{QueueName: record.QueueName, RunID: record.RunID, StepID: record.StepID, NotBefore: record.NotBefore, IdempotencyKey: record.IdempotencyKey, DeploymentID: record.DeploymentID, DeploymentMismatchRetryCount: record.DeploymentMismatchRetryCount, ReplayDivergenceCount: record.ReplayDivergenceCount}
}

func (w *World) putQueueRecord(ctx context.Context, id string, record queueRecord, mode string) (bool, error) {
	data, err := json.Marshal(record)
	if err != nil {
		return false, err
	}
	result, err := w.redis.Run(ctx, scriptQueuePut, w.prefix, id, string(data), mode)
	return replyInt(result) == 1, err
}

func (w *World) getQueueRecord(ctx context.Context, id string) (queueRecord, bool, error) {
	raw, ok, err := w.redis.Get(ctx, w.key("queue", id))
	if err != nil || !ok {
		return queueRecord{}, false, err
	}
	var record queueRecord
	if err := json.Unmarshal([]byte(raw), &record); err != nil {
		return queueRecord{}, false, fmt.Errorf("workflow/vercel: decode queue record %s: %w", id, err)
	}
	return record, true, nil
}

func (w *World) takeQueueRecord(ctx context.Context, id string) (queueRecord, bool, error) {
	result, err := w.redis.Run(ctx, scriptQueueTake, w.prefix, id)
	if err != nil || result == nil {
		return queueRecord{}, false, err
	}
	var record queueRecord
	if err := json.Unmarshal([]byte(replyString(result)), &record); err != nil {
		return queueRecord{}, false, fmt.Errorf("workflow/vercel: decode queue record %s: %w", id, err)
	}
	return record, true, nil
}

// Queue returns an error for a delivery due more than six days ahead unless
// Config.QStash is set. It does not deduplicate by IdempotencyKey. It
// publishes to input.DeploymentID, or to Config.DeploymentID when that is
// empty, and returns an error wrapping worlds.ErrDeploymentUnavailable when
// Vercel cannot find that deployment's consumer.
func (w *World) Queue(ctx context.Context, input worlds.QueueInput) (worlds.QueueResult, error) {
	// Store the delivery record, then publish its ID to Vercel Queues (delays
	// up to six days) or QStash (longer delays). A failed publish removes the
	// record.
	if err := ctx.Err(); err != nil {
		return worlds.QueueResult{}, err
	}
	var delay time.Duration
	if input.NotBefore != nil {
		delay = time.Until(*input.NotBefore)
	}
	long := delaySeconds(delay) > int64(vqsMaxQueueDelay/time.Second)
	if long && w.config.QStash == nil {
		return worlds.QueueResult{}, errors.New("workflow/vercel: delays over six days require QStash configuration")
	}
	id := ulid.Make().String()
	deploymentID := input.DeploymentID
	if deploymentID == "" {
		deploymentID = w.config.DeploymentID
	}
	record := queueRecord{QueueName: input.QueueName, Topic: w.config.Topics[w.config.Routes[input.WorkflowName]], RunID: input.RunID, StepID: input.StepID, NotBefore: input.NotBefore, IdempotencyKey: input.IdempotencyKey, DeploymentID: deploymentID, DeploymentMismatchRetryCount: input.DeploymentMismatchRetryCount, ReplayDivergenceCount: input.ReplayDivergenceCount}
	if _, err := w.putQueueRecord(ctx, id, record, "NX"); err != nil {
		return worlds.QueueResult{}, err
	}
	if err := w.schedule(ctx, id, record, delay, long); err != nil {
		if _, _, cleanup := w.takeQueueRecord(context.WithoutCancel(ctx), id); cleanup != nil {
			err = errors.Join(err, cleanup)
		}
		return worlds.QueueResult{}, err
	}
	return worlds.QueueResult{MessageID: id}, nil
}

func (w *World) schedule(ctx context.Context, id string, record queueRecord, delay time.Duration, long bool) error {
	if !long {
		return w.publishVQS(ctx, id, record.Topic, delay, record.DeploymentID)
	}
	// A delay beyond QStash's limit fires early and is scheduled again by
	// releaseTimer.
	due := *record.NotBefore
	if limit := time.Now().Add(w.config.QStash.maxDelay()); due.After(limit) {
		due = limit
	}
	qstashID, err := w.publishQStash(ctx, id, due)
	if err != nil {
		return err
	}
	record.QStashMessageID = qstashID
	stored, err := w.putQueueRecord(ctx, id, record, "XX")
	if err == nil && !stored {
		// Cancelled while publishing.
		err = w.cancelQStash(ctx, qstashID)
	}
	if err != nil {
		return errors.Join(err, w.cancelQStash(context.WithoutCancel(ctx), qstashID))
	}
	return nil
}

// releaseTimer moves a QStash-delivered record onto Vercel Queues. When the
// deployment the delivery is addressed to is gone, it is published to this
// deployment instead, whose runtime re-routes it and fails the run when its
// deployment stays unavailable.
func (w *World) releaseTimer(ctx context.Context, id string) error {
	record, ok, err := w.getQueueRecord(ctx, id)
	if err != nil || !ok {
		return err
	}
	var delay time.Duration
	if record.NotBefore != nil {
		delay = time.Until(*record.NotBefore)
	}
	if delaySeconds(delay) > int64(vqsMaxQueueDelay/time.Second) {
		return w.schedule(ctx, id, record, delay, true)
	}
	err = w.publishVQS(ctx, id, record.Topic, delay, record.DeploymentID)
	if errors.Is(err, worlds.ErrDeploymentUnavailable) && record.DeploymentID != w.config.DeploymentID {
		err = w.publishVQS(ctx, id, record.Topic, delay, w.config.DeploymentID)
	}
	if err != nil {
		return err
	}
	record.QStashMessageID = ""
	_, err = w.putQueueRecord(ctx, id, record, "XX")
	return err
}

func (w *World) QueueBatch(ctx context.Context, input worlds.QueueBatchInput) ([]worlds.QueueResult, error) {
	results := make([]worlds.QueueResult, 0, len(input.Items))
	for _, item := range input.Items {
		result, err := w.Queue(ctx, item)
		if err != nil {
			return results, err
		}
		results = append(results, result)
	}
	return results, nil
}

func (w *World) QueueCancel(ctx context.Context, input worlds.QueueCancelInput) error {
	// Removing the record makes a later delivery acknowledge without running.
	// A pending QStash timer is deleted too.
	if err := ctx.Err(); err != nil {
		return err
	}
	record, ok, err := w.takeQueueRecord(ctx, input.MessageID)
	if err != nil {
		return err
	}
	if !ok {
		return worlds.ErrQueueMessageNotFound
	}
	if record.QStashMessageID != "" {
		return w.cancelQStash(ctx, record.QStashMessageID)
	}
	return nil
}

type consumer struct {
	world   *World
	runtime *worlds.Runtime
}

func (c *consumer) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		writer.Header().Set("Allow", http.MethodPost)
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	defer request.Body.Close()
	ctx := request.Context()
	delivery, err := c.world.parseDelivery(request)
	if errors.Is(err, errDeliveryGone) {
		writer.WriteHeader(http.StatusOK)
		return
	}
	if err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	status, err := c.handle(ctx, delivery)
	if err != nil {
		log.Printf("workflow/vercel: queue message %s: %v", delivery.MessageID, err)
		http.Error(writer, "workflow queue delivery failed", status)
		return
	}
	writer.WriteHeader(status)
}

func (c *consumer) handle(ctx context.Context, delivery vqsDelivery) (int, error) {
	var message vqsMessage
	if err := json.Unmarshal(delivery.Body, &message); err != nil || message.ID == "" {
		// This is not a message this World published, so drop it rather than
		// retry.
		return http.StatusOK, c.world.acknowledge(ctx, delivery)
	}
	record, ok, err := c.world.getQueueRecord(ctx, message.ID)
	if err != nil {
		return http.StatusInternalServerError, err
	}
	if !ok {
		// Cancelled or already executed.
		return http.StatusOK, c.world.acknowledge(ctx, delivery)
	}
	if record.NotBefore != nil {
		if early := time.Until(*record.NotBefore); early > 0 {
			// Hide the message until it is due instead of failing it.
			if err := c.world.setVisibility(ctx, delivery, early); err != nil {
				return http.StatusInternalServerError, err
			}
			return http.StatusAccepted, nil
		}
	}
	stop := c.renewLease(ctx, delivery)
	input := record.input()
	input.MessageID = message.ID
	err = c.runtime.Execute(ctx, input)
	stop()
	if errors.Is(err, worlds.ErrRunNotFound) {
		// Retrying cannot create the run, so drop the delivery.
		log.Printf("workflow/vercel: queue message %s: %v", message.ID, err)
		err = nil
	}
	if err != nil {
		if visibilityErr := c.world.setVisibility(context.WithoutCancel(ctx), delivery, retryDelay(delivery.DeliveryCount)); visibilityErr != nil {
			err = errors.Join(err, visibilityErr)
		}
		return http.StatusInternalServerError, err
	}
	if _, _, err := c.world.takeQueueRecord(ctx, message.ID); err != nil {
		return http.StatusInternalServerError, err
	}
	if err := c.world.acknowledge(ctx, delivery); err != nil {
		return http.StatusInternalServerError, err
	}
	return http.StatusOK, nil
}

// renewLease keeps the delivery invisible while it executes.
func (c *consumer) renewLease(ctx context.Context, delivery vqsDelivery) func() {
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		next := vqsLeaseRenewal
		if !delivery.VisibilityDeadline.IsZero() {
			next = max(min(next, time.Until(delivery.VisibilityDeadline)/2), 0)
		}
		timer := time.NewTimer(next)
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-timer.C:
				if err := c.world.setVisibility(ctx, delivery, vqsLease); err != nil {
					log.Printf("workflow/vercel: renew queue lease %s: %v", delivery.MessageID, err)
				}
				timer.Reset(vqsLeaseRenewal)
			}
		}
	}()
	return func() {
		close(done)
		<-stopped
	}
}
