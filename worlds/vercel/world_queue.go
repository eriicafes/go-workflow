package vercelworld

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"slices"
	"strings"
	"sync"
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

// stepIDsHeader is the response header that names, as a JSON array, the steps
// a flow's invocation ran inline, by which Vercel links the invocation's logs
// to them. It names at most maxStepIDs.
const (
	stepIDsHeader = "X-Vercel-Internal-Workflow-Step-Ids"
	maxStepIDs    = 10
)

// stepRecorderKey holds, in a flow delivery's context, its *stepRecorder.
type stepRecorderKey struct{}

// stepRecorder collects the IDs of the steps whose functions a flow delivery
// runs.
type stepRecorder struct {
	mu  sync.Mutex
	off bool
	ids []string
}

// RecordStepExecution notes that the delivery of ctx is about to run the
// function of step stepID, when it is a flow's delivery, for the
// stepIDsHeader of the delivery's response.
func (w *World) RecordStepExecution(ctx context.Context, stepID worlds.StepID) {
	recorder, ok := ctx.Value(stepRecorderKey{}).(*stepRecorder)
	id := string(stepID)
	if !ok || id == "" {
		return
	}
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if !recorder.off && len(recorder.ids) < maxStepIDs && !slices.Contains(recorder.ids, id) {
		recorder.ids = append(recorder.ids, id)
	}
}

// header returns the stepIDsHeader value, or "" when no step was recorded.
func (r *stepRecorder) header() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.ids) == 0 {
		return ""
	}
	encoded, _ := json.Marshal(r.ids)
	return string(encoded)
}

// queueMessage is the Vercel Queues payload: the whole delivery, so the
// World keeps no record of queued work.
type queueMessage struct {
	// ID marks the payload as one this World published. A delivery's
	// MessageID is Vercel Queues' own message ID, not this.
	ID                           string                 `json:"id"`
	QueueName                    string                 `json:"queueName,omitempty"`
	RunID                        worlds.RunID           `json:"runId"`
	WorkflowName                 string                 `json:"workflowName,omitempty"`
	StepID                       *worlds.StepID         `json:"stepId,omitempty"`
	NotBefore                    *time.Time             `json:"notBefore,omitempty"`
	IdempotencyKey               string                 `json:"idempotencyKey,omitempty"`
	DeploymentID                 string                 `json:"deploymentId,omitempty"`
	DeploymentMismatchRetryCount int                    `json:"deploymentMismatchRetryCount,omitempty"`
	ReplayDivergenceCount        int                    `json:"replayDivergenceCount,omitempty"`
	StepName                     string                 `json:"stepName,omitempty"`
	StepAttempt                  int                    `json:"stepAttempt,omitempty"`
	StepResent                   bool                   `json:"stepResent,omitempty"`
	RunSpecVersion               int                    `json:"runSpecVersion,omitempty"`
	RunInput                     *queueRunInput         `json:"runInput,omitempty"`
	CompleteWaits                []string               `json:"completeWaits,omitempty"`
	Recovery                     bool                   `json:"recovery,omitempty"`
	WaitContinuation             *queueWaitContinuation `json:"waitContinuation,omitempty"`
	// RetentionMs is the delivery's Retention in milliseconds.
	RetentionMs int64 `json:"retentionMs,omitempty"`
	// StepCreatedAt is the delivery's StepCreatedAt in milliseconds since
	// the epoch.
	StepCreatedAt int64 `json:"stepCreatedAt,omitempty"`
	// SDKMessage is the delivery's SDKMessage.
	SDKMessage []byte `json:"sdkMessage,omitempty"`
}

// queueRunInput is a first delivery's RunInput in a queueMessage: the run's
// creation.
type queueRunInput struct {
	WorkflowName        string            `json:"workflowName"`
	DeploymentID        string            `json:"deploymentId,omitempty"`
	Input               []byte            `json:"input,omitempty"`
	Attributes          map[string]string `json:"attributes,omitempty"`
	ExecutionContext    map[string]any    `json:"executionContext,omitempty"`
	EncryptionPublicKey string            `json:"encryptionPublicKey,omitempty"`
	StartHook           *worlds.StartHook `json:"startHook,omitempty"`
	// AllowReservedAttributes is EventsCreateRunInput.AllowReservedAttributes.
	AllowReservedAttributes bool `json:"allowReservedAttributes,omitempty"`
	SpecVersion             int  `json:"specVersion"`
}

// queueWaitContinuation is worlds.WaitContinuation in a queueMessage.
type queueWaitContinuation struct {
	CorrelationID string     `json:"correlationId"`
	Attempt       int        `json:"attempt,omitempty"`
	ResumeAt      *time.Time `json:"resumeAt,omitempty"`
	HopAt         *time.Time `json:"hopAt,omitempty"`
}

// newQueueMessage returns the message of input, whose ID is id.
func newQueueMessage(id string, input worlds.QueueInput) queueMessage {
	message := queueMessage{
		ID:                           id,
		QueueName:                    input.QueueName,
		RunID:                        input.RunID,
		WorkflowName:                 input.WorkflowName,
		StepID:                       input.StepID,
		NotBefore:                    input.NotBefore,
		IdempotencyKey:               input.IdempotencyKey,
		DeploymentID:                 input.DeploymentID,
		DeploymentMismatchRetryCount: input.DeploymentMismatchRetryCount,
		ReplayDivergenceCount:        input.ReplayDivergenceCount,
		StepName:                     input.StepName,
		StepAttempt:                  input.StepAttempt,
		StepResent:                   input.StepResent,
		RunSpecVersion:               input.RunSpecVersion,
		CompleteWaits:                input.CompleteWaits,
		Recovery:                     input.Recovery,
		RunInput:                     toQueueRunInput(input.RunInput),
		RetentionMs:                  input.Retention.Milliseconds(),
		SDKMessage:                   input.SDKMessage,
	}
	if wait := input.WaitContinuation; wait != nil {
		message.WaitContinuation = &queueWaitContinuation{CorrelationID: wait.CorrelationID, Attempt: wait.Attempt, ResumeAt: wait.ResumeAt, HopAt: wait.HopAt}
	}
	if !input.StepCreatedAt.IsZero() {
		message.StepCreatedAt = input.StepCreatedAt.UnixMilli()
	}
	return message
}

func (message queueMessage) input() worlds.QueueInput {
	input := worlds.QueueInput{
		QueueName:                    message.QueueName,
		RunID:                        message.RunID,
		WorkflowName:                 message.WorkflowName,
		StepID:                       message.StepID,
		NotBefore:                    message.NotBefore,
		IdempotencyKey:               message.IdempotencyKey,
		DeploymentID:                 message.DeploymentID,
		DeploymentMismatchRetryCount: message.DeploymentMismatchRetryCount,
		ReplayDivergenceCount:        message.ReplayDivergenceCount,
		StepName:                     message.StepName,
		StepAttempt:                  message.StepAttempt,
		StepResent:                   message.StepResent,
		RunSpecVersion:               message.RunSpecVersion,
		CompleteWaits:                message.CompleteWaits,
		Recovery:                     message.Recovery,
		RunInput:                     message.RunInput.input(message.RunID),
		Retention:                    time.Duration(message.RetentionMs) * time.Millisecond,
		SDKMessage:                   message.SDKMessage,
	}
	if wait := message.WaitContinuation; wait != nil {
		input.WaitContinuation = &worlds.WaitContinuation{CorrelationID: wait.CorrelationID, Attempt: wait.Attempt, ResumeAt: wait.ResumeAt, HopAt: wait.HopAt}
	}
	if message.StepCreatedAt > 0 {
		input.StepCreatedAt = time.UnixMilli(message.StepCreatedAt).UTC()
	}
	return input
}

// Queue publishes the delivery to Vercel Queues, with input.IdempotencyKey as
// its idempotency key, so Vercel Queues stores one message per key while it
// lives. A flow other than an intermediate hop of a wait's timer carries its
// run ID as its concurrency key, so Vercel Queues delivers one flow of a run
// at a time and coalesces the others into it. The message lives for
// input.Retention, within the 1 minute to 7 days Vercel Queues accepts, or
// Vercel's default 24 hours, and its delay is capped an hour short of that.
// It publishes to input.DeploymentID, or to Config.DeploymentID when that is
// empty, and returns an error wrapping worlds.ErrDeploymentUnavailable when
// Vercel cannot find that deployment's consumer. Its result carries the
// message ID Vercel Queues gave the message, which a delivery of it carries
// too, and is empty when Vercel Queues failed over to another region that
// cannot name it.
func (w *World) Queue(ctx context.Context, input worlds.QueueInput) (worlds.QueueResult, error) {
	if err := ctx.Err(); err != nil {
		return worlds.QueueResult{}, err
	}
	message, send := w.prepareSend(input)
	id, err := w.publishVQS(ctx, message, send)
	if err != nil {
		return worlds.QueueResult{}, err
	}
	return worlds.QueueResult{MessageID: id}, nil
}

// prepareSend returns the message of a delivery and how it is published.
func (w *World) prepareSend(input worlds.QueueInput) (queueMessage, vqsSend) {
	if input.DeploymentID == "" {
		input.DeploymentID = w.config.DeploymentID
	}
	send := vqsSend{
		Topic:          w.config.Topics[w.config.Routes[input.WorkflowName]],
		DeploymentID:   input.DeploymentID,
		IdempotencyKey: input.IdempotencyKey,
		Retention:      input.Retention,
	}
	if input.SerializedFlow() {
		send.ConcurrencyKey = string(input.RunID)
	}
	if input.NotBefore != nil {
		send.Delay = time.Until(*input.NotBefore)
	}
	return newQueueMessage(ulid.Make().String(), input), send
}

// QueueBatch publishes the deliveries with Vercel Queues' batch send: one
// request for up to 100 messages that share a topic, a region and a
// deployment, each message published as Queue would publish it. Requests for
// different destinations are sent at the same time, and a request that fails
// transiently is sent again as a single send is. The results are in the order
// of the items, with an empty message ID for a message Vercel Queues accepted
// without naming it. A message Vercel Queues refused has its result's Err
// set, and Retryable as Vercel Queues says whether sending it again may
// succeed. When a request fails, the outcome of its messages is not known and
// QueueBatch returns the failure as its error.
func (w *World) QueueBatch(ctx context.Context, input worlds.QueueBatchInput) ([]worlds.QueueResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	type destination struct {
		region              worlds.Region
		topic, deploymentID string
	}
	var order []destination
	groups := map[destination][]vqsBatchItem{}
	for index, item := range input.Items {
		message, send := w.prepareSend(item)
		body, err := json.Marshal(message)
		if err != nil {
			return nil, err
		}
		header := messageHeaders(message, send)
		header.Set("Content-Type", "application/json")
		to := destination{messageRegion(message.RunID), w.topicOf(send), send.DeploymentID}
		if _, ok := groups[to]; !ok {
			order = append(order, to)
		}
		groups[to] = append(groups[to], vqsBatchItem{index: index, body: body, header: header})
	}
	results := make([]worlds.QueueResult, len(input.Items))
	failures := make([]error, len(order))
	var sending sync.WaitGroup
	for group, to := range order {
		sending.Go(func() {
			items := groups[to]
			for start := 0; start < len(items) && failures[group] == nil; start += vqsBatchMax {
				chunk := items[start:min(start+vqsBatchMax, len(items))]
				var outcomes []vqsBatchOutcome
				err := retrySend(ctx, to.topic, func() error {
					var err error
					outcomes, err = w.sendBatchVQS(ctx, to.region, to.topic, to.deploymentID, chunk)
					return err
				})
				if err != nil {
					failures[group] = err
					return
				}
				for position, outcome := range outcomes {
					result := &results[chunk[position].index]
					switch {
					case outcome.Status == "failed":
						result.Err, result.Retryable = fmt.Errorf("workflow/vercel: publish queue message: %d: %s", outcome.StatusCode, outcome.Error), outcome.Retryable
					case outcome.MessageID != nil:
						result.MessageID = *outcome.MessageID
					}
				}
			}
		})
	}
	sending.Wait()
	return results, errors.Join(failures...)
}

type consumer struct {
	world *World
	// execute executes a delivery with the consumer's executor.
	execute func(context.Context, worlds.QueueInput) error
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
	steps := &stepRecorder{}
	status, err := c.handle(context.WithValue(ctx, stepRecorderKey{}, steps), delivery)
	if ids := steps.header(); ids != "" {
		writer.Header().Set(stepIDsHeader, ids)
	}
	if err != nil {
		log.Printf("workflow/vercel: queue message %s: %v", delivery.MessageID, err)
		http.Error(writer, "workflow queue delivery failed", status)
		return
	}
	writer.WriteHeader(status)
}

// handle executes a delivery and settles its message: acknowledged when it
// succeeds, hidden for a Redelivery's delay or handed off for a Redelivery of
// zero, and hidden for a backoff when it fails. It answers 200 for an
// acknowledged message, 202 for one left for its next delivery, and 500 for a
// failure.
func (c *consumer) handle(ctx context.Context, delivery vqsDelivery) (int, error) {
	var message queueMessage
	if err := json.Unmarshal(delivery.Body, &message); err != nil || message.ID == "" || (message.RunID == "" && len(message.SDKMessage) == 0) {
		// This is not a message this World published, so drop it rather than
		// retry. A Workflow SDK message without a run, such as a health
		// check, is delivered.
		return http.StatusOK, c.world.acknowledge(ctx, delivery, 0)
	}
	if message.NotBefore != nil {
		if early := time.Until(*message.NotBefore); early > 0 {
			// Hide the message until it is due instead of failing it.
			if err := c.world.setVisibility(ctx, delivery, early); err != nil {
				return http.StatusInternalServerError, err
			}
			return http.StatusAccepted, nil
		}
	}
	if recorder, ok := ctx.Value(stepRecorderKey{}).(*stepRecorder); ok && message.StepID != nil {
		// A step's own delivery is linked to its step by the message.
		recorder.mu.Lock()
		recorder.off = true
		recorder.mu.Unlock()
	}
	input := message.input()
	// The delivery carries Vercel Queues' message ID, which stays the same
	// across its redeliveries.
	input.MessageID = delivery.MessageID
	// When the message was queued, as Vercel Queues reports it.
	input.CreatedAt = delivery.CreatedAt
	input.DeliveryCount = delivery.DeliveryCount
	if delivery.DeliveryCount > 0 {
		// A handoff is not an attempt.
		input.Attempt = max(delivery.DeliveryCount-delivery.HandoffCount, 1)
	}
	coalesced := 0
	for rerun := 0; ; rerun++ {
		stop := c.renewLease(ctx, delivery)
		err := c.execute(ctx, input)
		stop()
		if errors.Is(err, worlds.ErrRunNotFound) {
			// Retrying cannot create the run, so drop the delivery.
			log.Printf("workflow/vercel: queue message %s: %v", delivery.MessageID, err)
			err = nil
		}
		if err != nil {
			// A handoff of a flow message close to its expiry, or to its
			// handoff bound, continues on a new message instead, and this
			// one is acknowledged.
			if redelivery, ok := worlds.RedeliveryOf(err); ok && redelivery.After == 0 {
				rotated, rotateErr := c.rotate(ctx, delivery, message)
				if rotateErr != nil {
					return http.StatusInternalServerError, rotateErr
				}
				if rotated {
					if err := c.world.acknowledge(ctx, delivery, coalesced); err != nil && !errors.As(err, new(*errCoalescedPending)) {
						return http.StatusInternalServerError, err
					}
					return http.StatusOK, nil
				}
			}
			return c.redeliver(ctx, delivery, err)
		}
		if rerun >= vqsMaxCoalescedReruns {
			// The last re-run Vercel Queues makes: if more messages are
			// coalesced into the delivery meanwhile, it refuses the
			// acknowledgment and the message is handed off, keeping its
			// expiry and handoff count. A message due for rotation is
			// continued on a new one first; if the acknowledgment then
			// succeeds, the continuation is one extra wake.
			if _, err := c.rotate(ctx, delivery, message); err != nil {
				return http.StatusInternalServerError, err
			}
		}
		err = c.world.acknowledge(ctx, delivery, coalesced)
		var pending *errCoalescedPending
		if !errors.As(err, &pending) {
			if err != nil {
				return http.StatusInternalServerError, err
			}
			return http.StatusOK, nil
		}
		if rerun >= vqsMaxCoalescedReruns {
			// Messages keep being coalesced into this delivery: a busy run,
			// not a failing one. Its next delivery replays from the log what
			// they were sent for.
			if err := c.world.handOff(ctx, delivery); err != nil {
				return http.StatusInternalServerError, err
			}
			return http.StatusAccepted, nil
		}
		// Messages of the run were coalesced into this delivery while it
		// executed, so it executes again, as a redelivery would, before its
		// acknowledgment covers them.
		coalesced = pending.Count
		if input.DeliveryCount > 0 {
			input.DeliveryCount++
		}
		delivery.VisibilityDeadline = time.Time{}
	}
}

// redeliver leaves the delivery's message for its next delivery after err:
// after a Redelivery's delay, cut to what Vercel Queues honors, or at once
// for a Redelivery of zero, a handoff; or after a backoff for a failure.
func (c *consumer) redeliver(ctx context.Context, delivery vqsDelivery, err error) (int, error) {
	ctx = context.WithoutCancel(ctx)
	if redelivery, ok := worlds.RedeliveryOf(err); ok {
		var settleErr error
		if redelivery.After == 0 {
			settleErr = c.world.handOff(ctx, delivery)
		} else {
			settleErr = c.world.setVisibility(ctx, delivery, redelivery.After)
		}
		if settleErr != nil {
			return http.StatusInternalServerError, settleErr
		}
		return http.StatusAccepted, nil
	}
	// The backoff is never shorter than the wait the failure itself asks for.
	delay := worlds.DeliveryRetryDelay(err, retryDelay(delivery.DeliveryCount))
	if api, ok := errors.AsType[*apiError](err); ok {
		delay = max(delay, api.RetryAfter)
	}
	if early, ok := errors.AsType[*worlds.TooEarlyError](err); ok {
		delay = max(delay, early.RetryAfter)
	}
	if visibilityErr := c.world.setVisibility(ctx, delivery, delay); visibilityErr != nil {
		err = errors.Join(err, visibilityErr)
	}
	return http.StatusInternalServerError, err
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

// rotation returns why a flow message asked to hand off continues on a new
// message instead, or "" to hand it off: it expires within
// vqsRotationExpiryMargin, by Vercel Queues' expiry or else its first send
// and retention, or it is within vqsRotationHandoffMargin handoffs of
// vqsMaxHandoffs.
func rotation(delivery vqsDelivery, message queueMessage, now time.Time) string {
	expiresAt := delivery.ExpiresAt
	if expiresAt.IsZero() && !delivery.CreatedAt.IsZero() {
		retention := vqsDefaultRetention
		if message.RetentionMs > 0 {
			retention = time.Duration(message.RetentionMs) * time.Millisecond
		}
		expiresAt = delivery.CreatedAt.Add(retention)
	}
	if !expiresAt.IsZero() && !now.Before(expiresAt.Add(-vqsRotationExpiryMargin)) {
		return "expiry"
	}
	if delivery.HandoffCount >= vqsMaxHandoffs-vqsRotationHandoffMargin {
		return "handoff bound"
	}
	return ""
}

// rotate continues the run of a flow message due for rotation on a new
// message, due after this one is acknowledged, and reports whether it did.
// The new message carries what the old one is a wake for, its wait's timer
// and the waits it completes, and none of what marks a recovered flow or
// counts the old one's retries. A step message keeps its own, since
// its key would drop a copy.
func (c *consumer) rotate(ctx context.Context, delivery vqsDelivery, message queueMessage) (bool, error) {
	if message.StepID != nil {
		return false, nil
	}
	why := rotation(delivery, message, time.Now())
	if why == "" {
		return false, nil
	}
	input := message.input()
	due := time.Now().Add(vqsRotationDelay)
	continuation := worlds.QueueInput{
		QueueName: input.QueueName, RunID: input.RunID, WorkflowName: input.WorkflowName, DeploymentID: input.DeploymentID,
		RunSpecVersion: input.RunSpecVersion, CompleteWaits: input.CompleteWaits, WaitContinuation: input.WaitContinuation,
		NotBefore: &due,
	}
	if _, err := c.world.Queue(context.WithoutCancel(ctx), continuation); err != nil {
		return false, err
	}
	log.Printf("workflow/vercel: continuing run %s on a new message instead of handing off queue message %s (%s; handoffs: %d)", input.RunID, delivery.MessageID, why, delivery.HandoffCount)
	return true, nil
}

func toQueueRunInput(input *worlds.EventsCreateRunInput) *queueRunInput {
	if input == nil {
		return nil
	}
	return &queueRunInput{WorkflowName: input.WorkflowName, DeploymentID: input.DeploymentID, Input: input.Input, Attributes: input.Attributes, ExecutionContext: input.ExecutionContext, EncryptionPublicKey: input.EncryptionPublicKey, StartHook: input.StartHook, AllowReservedAttributes: input.AllowReservedAttributes, SpecVersion: worlds.SpecVersion}
}

func (run *queueRunInput) input(runID worlds.RunID) *worlds.EventsCreateRunInput {
	if run == nil {
		return nil
	}
	return &worlds.EventsCreateRunInput{RunID: runID, WorkflowName: run.WorkflowName, DeploymentID: run.DeploymentID, Input: run.Input, Attributes: run.Attributes, ExecutionContext: run.ExecutionContext, EncryptionPublicKey: run.EncryptionPublicKey, StartHook: run.StartHook, AllowReservedAttributes: run.AllowReservedAttributes}
}
