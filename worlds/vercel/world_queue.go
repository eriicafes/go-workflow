package vercelworld

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/eriicafes/go-workflow/worlds"
)

// requestIDKey holds, in a delivery's context, the Vercel request ID of its
// invocation from the x-vercel-id header.
type requestIDKey struct{}

// setRequestID records in an event's frame meta the Vercel request ID of the
// delivery writing it, so the dashboard links the event to the invocation's
// logs.
func setRequestID(ctx context.Context, meta map[string]any) {
	if id, ok := ctx.Value(requestIDKey{}).(string); ok {
		meta["vercelId"] = id
	}
}

// queueMessage is the Vercel Queues payload: the whole delivery, so the
// World keeps no record of queued work.
type queueMessage struct {
	// ID is the delivery's message ID, which stays the same across hops and
	// redeliveries.
	ID                           string         `json:"id"`
	QueueName                    string         `json:"queueName,omitempty"`
	RunID                        worlds.RunID   `json:"runId"`
	StepID                       *worlds.StepID `json:"stepId,omitempty"`
	NotBefore                    *time.Time     `json:"notBefore,omitempty"`
	IdempotencyKey               string         `json:"idempotencyKey,omitempty"`
	DeploymentID                 string         `json:"deploymentId,omitempty"`
	DeploymentMismatchRetryCount int            `json:"deploymentMismatchRetryCount,omitempty"`
	ReplayDivergenceCount        int            `json:"replayDivergenceCount,omitempty"`
	// Topic is the Vercel Queues topic the delivery's workflow is routed to;
	// empty is Config.Topic.
	Topic string `json:"topic,omitempty"`
}

func (message queueMessage) input() worlds.QueueInput {
	return worlds.QueueInput{QueueName: message.QueueName, RunID: message.RunID, StepID: message.StepID, NotBefore: message.NotBefore, IdempotencyKey: message.IdempotencyKey, DeploymentID: message.DeploymentID, DeploymentMismatchRetryCount: message.DeploymentMismatchRetryCount, ReplayDivergenceCount: message.ReplayDivergenceCount, MessageID: message.ID}
}

// Queue publishes the delivery to Vercel Queues. It does not deduplicate by
// input.IdempotencyKey: Vercel Queues deduplicates only retried publishes of
// the same delivery, by its message ID. It publishes to input.DeploymentID,
// or to Config.DeploymentID when that is empty, and returns an error wrapping
// worlds.ErrDeploymentUnavailable when Vercel cannot find that deployment's
// consumer. A delivery due more than 23 hours ahead is delivered in hops of up
// to 23 hours.
func (w *World) Queue(ctx context.Context, input worlds.QueueInput) (worlds.QueueResult, error) {
	if err := ctx.Err(); err != nil {
		return worlds.QueueResult{}, err
	}
	deploymentID := input.DeploymentID
	if deploymentID == "" {
		deploymentID = w.config.DeploymentID
	}
	message := queueMessage{ID: ulid.Make().String(), QueueName: input.QueueName, Topic: w.config.Topics[w.config.Routes[input.WorkflowName]], RunID: input.RunID, StepID: input.StepID, NotBefore: input.NotBefore, IdempotencyKey: input.IdempotencyKey, DeploymentID: deploymentID, DeploymentMismatchRetryCount: input.DeploymentMismatchRetryCount, ReplayDivergenceCount: input.ReplayDivergenceCount}
	var delay time.Duration
	if input.NotBefore != nil {
		delay = time.Until(*input.NotBefore)
	}
	if err := w.publishVQS(ctx, message, message.ID, min(delay, vqsMaxQueueDelay), deploymentID); err != nil {
		return worlds.QueueResult{}, err
	}
	return worlds.QueueResult{MessageID: message.ID}, nil
}

// republish publishes message again under key, with delay capped at the
// Vercel Queues limit. When the deployment the delivery is addressed to is
// gone, it is published to this deployment instead, whose runtime re-routes
// it and fails the run when its deployment stays unavailable.
func (w *World) republish(ctx context.Context, message queueMessage, key string, delay time.Duration) error {
	delay = min(delay, vqsMaxQueueDelay)
	err := w.publishVQS(ctx, message, key, delay, message.DeploymentID)
	if errors.Is(err, worlds.ErrDeploymentUnavailable) && message.DeploymentID != w.config.DeploymentID {
		err = w.publishVQS(ctx, message, key, delay, w.config.DeploymentID)
	}
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
	if id := strings.TrimSpace(request.Header.Get("x-vercel-id")); id != "" {
		ctx = context.WithValue(ctx, requestIDKey{}, id)
	}
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
	var message queueMessage
	if err := json.Unmarshal(delivery.Body, &message); err != nil || message.ID == "" || message.RunID == "" {
		// This is not a message this World published, so drop it rather than
		// retry.
		return http.StatusOK, c.world.acknowledge(ctx, delivery)
	}
	if message.NotBefore != nil {
		early := time.Until(*message.NotBefore)
		if early > vqsMaxVisibility {
			// A delay beyond Vercel Queues' limit continues on the next hop,
			// published before this one is acknowledged so a crash in between
			// only redelivers this one, whose retried publish Vercel drops by
			// its idempotency key.
			if err := c.world.republish(ctx, message, message.ID+":"+delivery.MessageID, early); err != nil {
				return http.StatusInternalServerError, err
			}
			return http.StatusOK, c.world.acknowledge(ctx, delivery)
		}
		if early > 0 {
			// Hide the message until it is due instead of failing it.
			if err := c.world.setVisibility(ctx, delivery, early); err != nil {
				return http.StatusInternalServerError, err
			}
			return http.StatusAccepted, nil
		}
	}
	stop := c.renewLease(ctx, delivery)
	input := message.input()
	input.Attempt = delivery.DeliveryCount
	err := c.runtime.Execute(ctx, input)
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
