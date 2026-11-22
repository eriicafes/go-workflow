package vercelworld

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/eriicafes/go-workflow/worlds"
)

var errDeliveryGone = errors.New("workflow/vercel: queue message is no longer available")

const (
	// vqsMaxQueueDelay is the longest delay of a message published without a
	// retention: it keeps Vercel's default retention of 24 hours, so it is
	// still retained for an hour after it becomes visible. It is the World's
	// MaxQueueDelay, and the runtime reaches later timers in hops.
	vqsMaxQueueDelay = 23 * time.Hour
	// vqsDefaultRetention is the retention of a message published without
	// Vqs-Retention-Seconds, and vqsMinRetention and vqsMaxRetention the
	// bounds Vercel Queues accepts for it.
	vqsDefaultRetention = 24 * time.Hour
	vqsMinRetention     = time.Minute
	vqsMaxRetention     = 7 * 24 * time.Hour
	// vqsRetentionBuffer is kept between a message's delay and its expiry,
	// so it is not delivered just as it expires.
	vqsRetentionBuffer = time.Hour
	// vqsMinRetry is the shortest visibility timeout Vercel Queues accepts.
	vqsMinRetry = 5 * time.Second
	// vqsMaxRetry is the longest backoff of a failed delivery.
	vqsMaxRetry = 15 * time.Minute
	// vqsMaxVisibility is the longest visibility timeout Vercel Queues
	// honors for one redelivery. A longer Redelivery is cut to it, and the
	// runtime asks again when the message arrives early.
	vqsMaxVisibility = 15 * time.Minute
	// vqsMaxCoalescedReruns is how often a delivery runs again because
	// messages were coalesced into it before it hands its message off.
	vqsMaxCoalescedReruns = 32
	// vqsRotationExpiryMargin is how long before its message expires a
	// flow continues on a new message instead of handing it off, and
	// vqsRotationHandoffMargin how many handoffs short of
	// vqsMaxHandoffs, the most Vercel Queues makes of one message, past
	// which a handoff counts as an attempt.
	vqsRotationExpiryMargin  = 4 * time.Hour
	vqsMaxHandoffs           = 10_000
	vqsRotationHandoffMargin = 100
	// vqsRotationDelay delays the new message past the acknowledgment of
	// the one it replaces, so it is not coalesced into it.
	vqsRotationDelay = time.Second
	// vqsLease is the visibility timeout requested and renewed while a
	// delivery executes.
	vqsLease          = 5 * time.Minute
	vqsLeaseRenewal   = time.Minute
	vqsPayloadMaxSize = 1 << 20
	// vqsRequestTimeout is the default deadline of one Vercel Queues
	// request (see queueRequestTimeout).
	vqsRequestTimeout = 30 * time.Second
	// vqsSendMaxAttempts is how many times a send is made, the first
	// included, vqsSendRetryJitter the most random spread added to each
	// backoff, vqsSendRetryBudget how long after the first attempt a retry
	// may still start, and vqsSendMaxRetryAfter the longest Retry-After of a
	// 429 waited out in-process.
	vqsSendMaxAttempts   = 3
	vqsSendRetryJitter   = 100 * time.Millisecond
	vqsSendRetryBudget   = 35 * time.Second
	vqsSendMaxRetryAfter = 5 * time.Second
)

// vqsSendRetryDelays is the backoff before each retry of a send, the last
// repeated.
var vqsSendRetryDelays = []time.Duration{100 * time.Millisecond, 500 * time.Millisecond}

// queueURL returns the Vercel Queues API root of region: Config.QueueURL when
// it was set, and region's own host otherwise. Through Vercel's API proxy,
// the x-vercel-queue-region header names the region the proxy forwards to.
func (w *World) queueURL(region worlds.Region) (string, http.Header) {
	var header http.Header
	if w.api.proxy {
		header = http.Header{"X-Vercel-Queue-Region": {string(region)}}
	}
	if w.config.QueueURL != "" {
		return w.config.QueueURL, header
	}
	return "https://" + string(region) + ".vercel-queue.com/api/v3", header
}

// messageRegion returns the region a message of runID is sent to: the region
// its run ID carries, else that of VERCEL_REGION, else iad1.
func messageRegion(runID worlds.RunID) worlds.Region {
	rest, _ := strings.CutPrefix(string(runID), runIDPrefix)
	if value, err := ulid.ParseStrict(rest); err == nil && value[0]&runIDTagBit != 0 {
		if region, ok := runRegion(runID); ok {
			return region
		}
	}
	return envRegion()
}

func (w *World) vqsRequest(ctx context.Context, region worlds.Region, method, path string, body []byte, header http.Header) (*http.Response, error) {
	token, err := w.config.OIDCToken(ctx)
	if err != nil {
		return nil, err
	}
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	base, routing := w.queueURL(region)
	// Every request has a deadline of its own, so a hung queue does not hold
	// a lease past its renewal.
	timeout := queueRequestTimeout()
	requestCtx, cancel := context.WithTimeoutCause(ctx, timeout, errQueueRequestDeadline)
	request, err := http.NewRequestWithContext(requestCtx, method, base+path, reader)
	if err != nil {
		cancel()
		return nil, err
	}
	for _, values := range []http.Header{routing, header} {
		for key, value := range values {
			request.Header[key] = value
		}
	}
	request.Header.Set("Authorization", "Bearer "+token)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if request.Header.Get("Vqs-Deployment-Id") == "" && w.config.DeploymentID != "" {
		request.Header.Set("Vqs-Deployment-Id", w.config.DeploymentID)
	}
	response, err := w.http.Do(request)
	if err != nil {
		if ctx.Err() == nil && context.Cause(requestCtx) == errQueueRequestDeadline {
			err = fmt.Errorf("%w after %s: %w", errQueueRequestDeadline, timeout, err)
		}
		cancel()
		return nil, err
	}
	response.Body = cancelOnClose{response.Body, cancel}
	return response, nil
}

// cancelOnClose is a response body that releases its request's deadline when
// it is closed.
type cancelOnClose struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (body cancelOnClose) Close() error {
	defer body.cancel()
	return body.ReadCloser.Close()
}

// vqsSend is how one message is published: its topic, deployment, and the
// Vercel Queues options of the delivery it carries.
type vqsSend struct {
	Topic        string
	DeploymentID string
	// IdempotencyKey is sent as Vqs-Idempotency-Key when it is set: Vercel
	// Queues stores one message per key while it lives.
	IdempotencyKey string
	// ConcurrencyKey is sent as Vqs-Experimental-Concurrency-Key when it is
	// set: Vercel Queues keeps one delivery per key active and coalesces the
	// key's other messages into it.
	ConcurrencyKey string
	Delay          time.Duration
	Retention      time.Duration
}

// messageLifetime returns the Vqs-Delay-Seconds and Vqs-Retention-Seconds of a
// message, zero for a header not sent. A retention is clamped to what Vercel
// Queues accepts, and a delay to the message's retention less an hour: 23
// hours with the default retention.
func messageLifetime(delay, retention time.Duration) (delaySecs, retentionSecs int64) {
	maxDelay := vqsDefaultRetention - vqsRetentionBuffer
	if retention > 0 {
		retentionSecs = delaySeconds(min(max(retention, vqsMinRetention), vqsMaxRetention))
		maxDelay = max(time.Duration(retentionSecs)*time.Second-vqsRetentionBuffer, 0)
	}
	return min(delaySeconds(delay), int64(maxDelay/time.Second)), retentionSecs
}

// publishVQS sends message to send.Topic, or to Config.Topic when it is
// empty, addressed to send.DeploymentID when it is set, and returns the
// message ID Vercel Queues gave it: empty when it answers 202, as it does when
// it fails over to another region and cannot name the message. A send that
// fails transiently is sent again (see sendRetryDelay). A send Vercel Queues
// refuses because it cannot find the deployment's consumer returns an error
// wrapping worlds.ErrDeploymentUnavailable once its retries run out.
func (w *World) publishVQS(ctx context.Context, message queueMessage, send vqsSend) (string, error) {
	topic := send.Topic
	if topic == "" {
		topic = w.config.Topic
	}
	body, err := json.Marshal(message)
	if err != nil {
		return "", err
	}
	header := http.Header{}
	if send.IdempotencyKey != "" {
		header.Set("Vqs-Idempotency-Key", send.IdempotencyKey)
	}
	if send.ConcurrencyKey != "" {
		header.Set("Vqs-Experimental-Concurrency-Key", send.ConcurrencyKey)
	}
	delay, retention := messageLifetime(send.Delay, send.Retention)
	if delay > 0 {
		header.Set("Vqs-Delay-Seconds", strconv.FormatInt(delay, 10))
	}
	if retention > 0 {
		header.Set("Vqs-Retention-Seconds", strconv.FormatInt(retention, 10))
	}
	if send.DeploymentID != "" {
		header.Set("Vqs-Deployment-Id", send.DeploymentID)
	}
	region, path := messageRegion(message.RunID), "/topic/"+url.PathEscape(topic)
	started := time.Now()
	for attempt := 1; ; attempt++ {
		id, err := w.sendVQS(ctx, region, path, body, header, send.DeploymentID)
		if err == nil {
			return id, nil
		}
		wait, retry := time.Duration(0), false
		if attempt < vqsSendMaxAttempts {
			wait, retry = sendRetryDelay(ctx, err, attempt-1)
		}
		if !retry || time.Since(started)+wait > vqsSendRetryBudget {
			if attempt > 1 {
				log.Printf("workflow/vercel: queue send to %s failed after %d attempts: %v", topic, attempt, err)
			}
			return "", err
		}
		log.Printf("workflow/vercel: queue send to %s failed (attempt %d), retrying in %s: %v", topic, attempt, wait, err)
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return "", errors.Join(err, ctx.Err())
		case <-timer.C:
		}
	}
}

// sendVQS makes one attempt at a send, and returns the message ID Vercel
// Queues answered with.
func (w *World) sendVQS(ctx context.Context, region worlds.Region, path string, body []byte, header http.Header, deploymentID string) (string, error) {
	response, err := w.vqsRequest(ctx, region, http.MethodPost, path, body, header)
	if err != nil {
		return "", fmt.Errorf("workflow/vercel: publish queue message: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		data, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return "", &vqsSendError{
			Status:       response.StatusCode,
			Code:         response.Header.Get("Vqs-Error-Code"),
			RetryAfter:   vqsRetryAfter(response.Header.Get("Retry-After")),
			DeploymentID: deploymentID,
			Message:      strings.TrimSpace(string(data)),
		}
	}
	if response.StatusCode == http.StatusAccepted {
		// Vercel Queues failed over to another region, which took the
		// message but cannot name it.
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		return "", nil
	}
	var answer struct {
		MessageID string `json:"messageId"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&answer); err != nil {
		return "", fmt.Errorf("workflow/vercel: publish queue message: invalid answer: %v", err)
	}
	return answer.MessageID, nil
}

// vqsSendError is a send Vercel Queues answered with a non-2xx status.
type vqsSendError struct {
	Status int
	// Code is the Vqs-Error-Code header.
	Code string
	// RetryAfter is the wait the Retry-After header asks for, zero for none.
	RetryAfter   time.Duration
	DeploymentID string
	Message      string
}

// consumerDiscovery reports whether Vercel Queues could not find the consumer
// of the deployment the message is addressed to.
func (e *vqsSendError) consumerDiscovery() bool {
	return e.Status == http.StatusBadGateway && e.Code == "consumer_discovery_failed"
}

func (e *vqsSendError) Error() string {
	if e.consumerDiscovery() {
		return fmt.Sprintf("workflow/vercel: publish queue message to deployment %q: %v", e.DeploymentID, worlds.ErrDeploymentUnavailable)
	}
	return fmt.Sprintf("workflow/vercel: publish queue message: %d %s: %s", e.Status, http.StatusText(e.Status), e.Message)
}

func (e *vqsSendError) Unwrap() error {
	if e.consumerDiscovery() {
		return worlds.ErrDeploymentUnavailable
	}
	return nil
}

// sendRetryDelay returns how long to wait before sending again after err, the
// retry-th retry's backoff of vqsSendRetryDelays plus up to vqsSendRetryJitter,
// and false when err is not worth a re-send. A transport failure, the queue
// request deadline, a 5xx, and the 502 of a failed consumer discovery are
// sent again after the backoff. A 429 is sent again after the backoff or its
// Retry-After, whichever is longer, unless it asks for more than
// vqsSendMaxRetryAfter, which is left to the caller. Any other 4xx means
// Vercel Queues decided, as does the 503 of a consumer registry that is not
// configured.
func sendRetryDelay(ctx context.Context, err error, retry int) (time.Duration, bool) {
	backoff := vqsSendRetryDelays[min(retry, len(vqsSendRetryDelays)-1)] + rand.N(vqsSendRetryJitter)
	var sendErr *vqsSendError
	if errors.As(err, &sendErr) {
		switch {
		case sendErr.Status == http.StatusTooManyRequests:
			if sendErr.RetryAfter > vqsSendMaxRetryAfter {
				return 0, false
			}
			return max(backoff, sendErr.RetryAfter), true
		case sendErr.Status == http.StatusServiceUnavailable && sendErr.Code == "consumer_registry_not_configured":
			return 0, false
		case sendErr.Status >= 500:
			return backoff, true
		}
		return 0, false
	}
	if transientTransport(ctx, err) {
		return backoff, true
	}
	return 0, false
}

// vqsRetryAfter returns the wait a Retry-After header asks for, in seconds or
// as an HTTP date, as @vercel/queue reads it, or zero when it has none.
func vqsRetryAfter(value string) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if seconds, err := strconv.ParseFloat(value, 64); err == nil {
		return time.Duration(max(seconds, 0) * float64(time.Second))
	}
	if at, err := http.ParseTime(value); err == nil {
		return max(time.Until(at), 0)
	}
	return 0
}

// errQueueRequestDeadline is the failure of a Vercel Queues request that ran
// past its deadline, vqsRequestTimeout.
var errQueueRequestDeadline = errors.New("workflow/vercel: queue request deadline exceeded")

// queueRequestTimeout is the deadline of one Vercel Queues request:
// WORKFLOW_VERCEL_QUEUE_TIMEOUT_MS when it is set, within 5 seconds to 2
// minutes, and vqsRequestTimeout otherwise.
func queueRequestTimeout() time.Duration {
	value, err := strconv.Atoi(strings.TrimSpace(os.Getenv("WORKFLOW_VERCEL_QUEUE_TIMEOUT_MS")))
	if err != nil {
		return vqsRequestTimeout
	}
	return min(max(time.Duration(value)*time.Millisecond, 5*time.Second), 2*time.Minute)
}

// transientTransport reports whether err is a transport failure worth
// another attempt: a connection refused, reset or closed, a failed lookup,
// a timeout, or the queue request deadline, but not the end of ctx.
func transientTransport(ctx context.Context, err error) bool {
	if err == nil || ctx.Err() != nil {
		return false
	}
	if errors.Is(err, errQueueRequestDeadline) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ETIMEDOUT) {
		return true
	}
	var opErr *net.OpError
	var dnsErr *net.DNSError
	if errors.As(err, &opErr) || errors.As(err, &dnsErr) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

// vqsDelivery is one push delivery resolved to its payload and lease.
type vqsDelivery struct {
	Topic         string
	ConsumerGroup string
	MessageID     string
	ReceiptHandle string
	// DeliveryCount counts the message's deliveries, handoffs included, and
	// HandoffCount its handoffs. Zero DeliveryCount is unknown.
	DeliveryCount int
	HandoffCount  int
	// VisibilityDeadline is when the current lease expires, when known.
	VisibilityDeadline time.Time
	// CreatedAt is when the message was first sent, and ExpiresAt when
	// Vercel Queues drops it, when known.
	CreatedAt, ExpiresAt time.Time
	Body                 []byte
	// Region is the region of the queue that delivered the message, which
	// its lease is renewed and released in.
	Region worlds.Region
}

// parseDelivery reads a push delivery. Binary-mode deliveries (ce-type
// com.vercel.queue.v2beta) carry the payload and lease in the request.
// Deliveries without a receipt handle and structured v1beta CloudEvents are
// claimed with ReceiveMessageById.
func (w *World) parseDelivery(request *http.Request) (vqsDelivery, error) {
	body, err := io.ReadAll(io.LimitReader(request.Body, vqsPayloadMaxSize+1))
	if err != nil {
		return vqsDelivery{}, err
	}
	if len(body) > vqsPayloadMaxSize {
		return vqsDelivery{}, errors.New("workflow/vercel: queue delivery is too large")
	}
	var delivery vqsDelivery
	switch request.Header.Get("Ce-Type") {
	case "com.vercel.queue.v2beta":
		delivery = vqsDelivery{
			Topic:         request.Header.Get("Ce-Vqsqueuename"),
			ConsumerGroup: request.Header.Get("Ce-Vqsconsumergroup"),
			MessageID:     request.Header.Get("Ce-Vqsmessageid"),
			ReceiptHandle: request.Header.Get("Ce-Vqsreceipthandle"),
		}
		delivery.DeliveryCount, _ = strconv.Atoi(request.Header.Get("Ce-Vqsdeliverycount"))
		delivery.HandoffCount, _ = strconv.Atoi(request.Header.Get("Ce-Vqsexperimentalhandoffcount"))
		delivery.VisibilityDeadline, _ = time.Parse(time.RFC3339Nano, request.Header.Get("Ce-Vqsvisibilitydeadline"))
		delivery.CreatedAt, _ = time.Parse(time.RFC3339Nano, request.Header.Get("Ce-Vqscreatedat"))
		delivery.ExpiresAt, _ = time.Parse(time.RFC3339Nano, request.Header.Get("Ce-Vqsexpiresat"))
		if delivery.ReceiptHandle != "" {
			delivery.Body = body
		}
	default:
		var event struct {
			Type string `json:"type"`
			Data struct {
				QueueName     string `json:"queueName"`
				ConsumerGroup string `json:"consumerGroup"`
				MessageID     string `json:"messageId"`
			} `json:"data"`
		}
		if !strings.Contains(request.Header.Get("Content-Type"), "application/cloudevents+json") || json.Unmarshal(body, &event) != nil {
			return vqsDelivery{}, errors.New("workflow/vercel: invalid queue delivery")
		}
		delivery = vqsDelivery{Topic: event.Data.QueueName, ConsumerGroup: event.Data.ConsumerGroup, MessageID: event.Data.MessageID}
	}
	if delivery.Topic == "" || delivery.ConsumerGroup == "" || delivery.MessageID == "" {
		return vqsDelivery{}, errors.New("workflow/vercel: invalid queue delivery")
	}
	// Leases live in the region whose queue delivered the message, which
	// Vercel names in ce-vqsregion, as @vercel/queue routes its acks.
	var ok bool
	if delivery.Region, ok = knownRegion(worlds.Region(request.Header.Get("Ce-Vqsregion"))); !ok {
		delivery.Region = envRegion()
	}
	if delivery.ReceiptHandle == "" {
		return w.receiveByID(request.Context(), delivery)
	}
	return delivery, nil
}

func (w *World) consumerPath(delivery vqsDelivery) string {
	return "/topic/" + url.PathEscape(delivery.Topic) + "/consumer/" + url.PathEscape(delivery.ConsumerGroup)
}

func (w *World) receiveByID(ctx context.Context, delivery vqsDelivery) (vqsDelivery, error) {
	header := http.Header{}
	header.Set("Accept", "application/x-ndjson")
	header.Set("Vqs-Visibility-Timeout-Seconds", strconv.Itoa(int(vqsLease/time.Second)))
	response, err := w.vqsRequest(ctx, delivery.Region, http.MethodPost, w.consumerPath(delivery)+"/id/"+url.PathEscape(delivery.MessageID), nil, header)
	if err != nil {
		return vqsDelivery{}, fmt.Errorf("workflow/vercel: receive queue message: %w", err)
	}
	defer response.Body.Close()
	switch response.StatusCode {
	case http.StatusOK:
	case http.StatusNotFound, http.StatusConflict, http.StatusGone:
		return vqsDelivery{}, errDeliveryGone
	default:
		return vqsDelivery{}, responseError("receive queue message", response)
	}
	line, err := bufio.NewReaderSize(io.LimitReader(response.Body, 2*vqsPayloadMaxSize), 64<<10).ReadBytes('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return vqsDelivery{}, err
	}
	var message struct {
		ReceiptHandle string    `json:"receiptHandle"`
		DeliveryCount int       `json:"deliveryCount"`
		Body          string    `json:"body"`
		CreatedAt     time.Time `json:"createdAt"`
		ExpiresAt     time.Time `json:"expiresAt"`
	}
	if err := json.Unmarshal(line, &message); err != nil || message.ReceiptHandle == "" {
		return vqsDelivery{}, errors.New("workflow/vercel: invalid queue message")
	}
	if delivery.Body, err = base64.StdEncoding.DecodeString(message.Body); err != nil {
		return vqsDelivery{}, fmt.Errorf("workflow/vercel: decode queue message: %w", err)
	}
	delivery.ReceiptHandle, delivery.DeliveryCount = message.ReceiptHandle, message.DeliveryCount
	delivery.CreatedAt, delivery.ExpiresAt = message.CreatedAt, message.ExpiresAt
	if delivery.CreatedAt.IsZero() {
		delivery.CreatedAt, _ = time.Parse(time.RFC3339Nano, response.Header.Get("Vqs-Timestamp"))
	}
	if delivery.ExpiresAt.IsZero() {
		delivery.ExpiresAt, _ = time.Parse(time.RFC3339Nano, response.Header.Get("Vqs-Expires-At"))
	}
	// The counts may come as headers, as on a multipart receive.
	if count, err := strconv.Atoi(response.Header.Get("Vqs-Delivery-Count")); err == nil && delivery.DeliveryCount == 0 {
		delivery.DeliveryCount = count
	}
	delivery.HandoffCount, _ = strconv.Atoi(response.Header.Get("Vqs-Experimental-Handoff-Count"))
	delivery.VisibilityDeadline = time.Now().Add(vqsLease)
	return delivery, nil
}

// errCoalescedPending is the refusal of an acknowledgment that does not cover
// every message Vercel Queues coalesced into the delivery.
type errCoalescedPending struct {
	// Count is how many messages were coalesced into the delivery.
	Count int
}

func (e *errCoalescedPending) Error() string {
	return fmt.Sprintf("workflow/vercel: %d queue messages were coalesced into the delivery", e.Count)
}

// acknowledge removes the delivery from its consumer group. coalesced is how
// many coalesced messages the delivery's last execution covers, sent as
// Vqs-Experimental-If-Coalesced when it is above zero. When more were
// coalesced into it, Vercel Queues refuses, and acknowledge returns an
// *errCoalescedPending.
func (w *World) acknowledge(ctx context.Context, delivery vqsDelivery, coalesced int) error {
	var header http.Header
	if coalesced > 0 {
		header = http.Header{"Vqs-Experimental-If-Coalesced": {strconv.Itoa(coalesced)}}
	}
	response, err := w.vqsRequest(ctx, delivery.Region, http.MethodDelete, w.consumerPath(delivery)+"/lease/"+url.PathEscape(delivery.ReceiptHandle), nil, header)
	if err != nil {
		return fmt.Errorf("workflow/vercel: acknowledge queue message: %w", err)
	}
	defer response.Body.Close()
	if (response.StatusCode >= 200 && response.StatusCode < 300) || response.StatusCode == http.StatusNotFound {
		return nil
	}
	if response.StatusCode == http.StatusConflict && response.Header.Get("Vqs-Error-Code") == "concurrency_coalesced_pending" {
		count, err := strconv.Atoi(response.Header.Get("Vqs-Experimental-Coalesced"))
		if err != nil || count <= 0 {
			var body struct {
				CoalescedCount int `json:"coalescedCount"`
			}
			_ = json.NewDecoder(io.LimitReader(response.Body, 4096)).Decode(&body)
			count = body.CoalescedCount
		}
		if count > 0 {
			return &errCoalescedPending{Count: count}
		}
	}
	return responseError("acknowledge queue message", response)
}

// setVisibility hides the delivery for timeout, up to vqsMaxVisibility, which
// renews the lease or schedules the next delivery.
func (w *World) setVisibility(ctx context.Context, delivery vqsDelivery, timeout time.Duration) error {
	return w.changeVisibility(ctx, delivery, min(delaySeconds(timeout), int64(vqsMaxVisibility/time.Second)), false)
}

// handOff releases the delivery for its next delivery at once, which Vercel
// Queues does not count toward its retries and reports in the next
// delivery's handoff count.
func (w *World) handOff(ctx context.Context, delivery vqsDelivery) error {
	return w.changeVisibility(ctx, delivery, 0, true)
}

func (w *World) changeVisibility(ctx context.Context, delivery vqsDelivery, seconds int64, handoff bool) error {
	body, err := json.Marshal(map[string]int64{"visibilityTimeoutSeconds": seconds})
	if err != nil {
		return err
	}
	var header http.Header
	if handoff {
		header = http.Header{"Vqs-Experimental-Handoff": {"1"}}
	}
	response, err := w.vqsRequest(ctx, delivery.Region, http.MethodPatch, w.consumerPath(delivery)+"/lease/"+url.PathEscape(delivery.ReceiptHandle), body, header)
	if err != nil {
		return fmt.Errorf("workflow/vercel: change queue message visibility: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return responseError("change queue message visibility", response)
	}
	if outcome := response.Header.Get("Vqs-Experimental-Handoff"); handoff && outcome != "" && outcome != "accepted" {
		// not_immediate or limit_reached: Vercel Queues redelivers it as an
		// ordinary retry, which counts as an attempt.
		log.Printf("workflow/vercel: handoff of queue message %s: %s", delivery.MessageID, outcome)
	}
	return nil
}

func responseError(action string, response *http.Response) error {
	data, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
	return fmt.Errorf("workflow/vercel: %s: %s: %s", action, response.Status, strings.TrimSpace(string(data)))
}

// delaySeconds rounds a delay up so a message is never delivered early.
func delaySeconds(delay time.Duration) int64 {
	if delay <= 0 {
		return 0
	}
	return int64((delay + time.Second - 1) / time.Second)
}

// retryDelay backs off failed deliveries: a second doubling with each
// delivery up to 15 minutes, with up to 25% jitter either way, and never less
// than the 5 seconds Vercel Queues accepts.
func retryDelay(deliveryCount int) time.Duration {
	delay := time.Second
	for range min(max(deliveryCount-1, 0), 10) {
		delay *= 2
	}
	delay = min(delay, vqsMaxRetry)
	jitter := time.Duration((rand.Float64()*2 - 1) * 0.25 * float64(delay))
	return max(delay+jitter, vqsMinRetry)
}
