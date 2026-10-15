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
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

var errDeliveryGone = errors.New("workflow/vercel: queue message is no longer available")

const (
	// vqsMaxDelay is the longest Vqs-Delay-Seconds and Vqs-Retention-Seconds.
	vqsMaxDelay = 7 * 24 * time.Hour
	// vqsLease is the visibility timeout requested and renewed while a
	// delivery executes.
	vqsLease          = 5 * time.Minute
	vqsLeaseRenewal   = time.Minute
	vqsMaxVisibility  = time.Hour
	vqsPayloadMaxSize = 1 << 20
)

// vqsMessage is the Vercel Queue payload. The delivery itself is the Redis
// queue record it names.
type vqsMessage struct {
	ID string `json:"id"`
}

func (w *World) vqsRequest(ctx context.Context, method, path string, body []byte, header http.Header) (*http.Response, error) {
	token, err := w.config.OIDCToken(ctx)
	if err != nil {
		return nil, err
	}
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	request, err := http.NewRequestWithContext(ctx, method, w.config.QueueURL+path, reader)
	if err != nil {
		return nil, err
	}
	for key, values := range header {
		request.Header[key] = values
	}
	request.Header.Set("Authorization", "Bearer "+token)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if deployment := os.Getenv("VERCEL_DEPLOYMENT_ID"); deployment != "" {
		request.Header.Set("Vqs-Deployment-Id", deployment)
	}
	return w.http.Do(request)
}

// publishVQS sends messageID to the topic. The message ID doubles as the
// idempotency key so only a retried publish of the same delivery is dropped.
// Runtime idempotency keys are reused across distinct deliveries and Vercel
// would silently drop those for up to 24 hours.
func (w *World) publishVQS(ctx context.Context, messageID string, delay time.Duration) error {
	body, err := json.Marshal(vqsMessage{ID: messageID})
	if err != nil {
		return err
	}
	header := http.Header{}
	header.Set("Vqs-Idempotency-Key", messageID)
	// Retain every message for the maximum so a delayed or failing delivery
	// is not dropped before its record is processed.
	header.Set("Vqs-Retention-Seconds", strconv.Itoa(int(vqsMaxDelay/time.Second)))
	if seconds := delaySeconds(delay); seconds > 0 {
		header.Set("Vqs-Delay-Seconds", strconv.FormatInt(seconds, 10))
	}
	response, err := w.vqsRequest(ctx, http.MethodPost, "/topic/"+url.PathEscape(w.config.Topic), body, header)
	if err != nil {
		return fmt.Errorf("workflow/vercel: publish queue message: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return responseError("publish queue message", response)
	}
	return nil
}

// vqsDelivery is one push delivery resolved to its payload and lease.
type vqsDelivery struct {
	Topic         string
	ConsumerGroup string
	MessageID     string
	ReceiptHandle string
	DeliveryCount int
	// VisibilityDeadline is when the current lease expires, when known.
	VisibilityDeadline time.Time
	Body               []byte
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
		delivery.VisibilityDeadline, _ = time.Parse(time.RFC3339Nano, request.Header.Get("Ce-Vqsvisibilitydeadline"))
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
	response, err := w.vqsRequest(ctx, http.MethodPost, w.consumerPath(delivery)+"/id/"+url.PathEscape(delivery.MessageID), nil, header)
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
		ReceiptHandle string `json:"receiptHandle"`
		DeliveryCount int    `json:"deliveryCount"`
		Body          string `json:"body"`
	}
	if err := json.Unmarshal(line, &message); err != nil || message.ReceiptHandle == "" {
		return vqsDelivery{}, errors.New("workflow/vercel: invalid queue message")
	}
	if delivery.Body, err = base64.StdEncoding.DecodeString(message.Body); err != nil {
		return vqsDelivery{}, fmt.Errorf("workflow/vercel: decode queue message: %w", err)
	}
	delivery.ReceiptHandle, delivery.DeliveryCount = message.ReceiptHandle, message.DeliveryCount
	delivery.VisibilityDeadline = time.Now().Add(vqsLease)
	return delivery, nil
}

// acknowledge removes the delivery from its consumer group.
func (w *World) acknowledge(ctx context.Context, delivery vqsDelivery) error {
	response, err := w.vqsRequest(ctx, http.MethodDelete, w.consumerPath(delivery)+"/lease/"+url.PathEscape(delivery.ReceiptHandle), nil, nil)
	if err != nil {
		return fmt.Errorf("workflow/vercel: acknowledge queue message: %w", err)
	}
	defer response.Body.Close()
	if (response.StatusCode >= 200 && response.StatusCode < 300) || response.StatusCode == http.StatusNotFound {
		return nil
	}
	return responseError("acknowledge queue message", response)
}

// setVisibility hides the delivery for timeout, which renews the lease or,
// after a failure, schedules the next attempt.
func (w *World) setVisibility(ctx context.Context, delivery vqsDelivery, timeout time.Duration) error {
	body, err := json.Marshal(map[string]int64{"visibilityTimeoutSeconds": min(delaySeconds(timeout), int64(vqsMaxVisibility/time.Second))})
	if err != nil {
		return err
	}
	response, err := w.vqsRequest(ctx, http.MethodPatch, w.consumerPath(delivery)+"/lease/"+url.PathEscape(delivery.ReceiptHandle), body, nil)
	if err != nil {
		return fmt.Errorf("workflow/vercel: extend queue lease: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return responseError("extend queue lease", response)
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

// retryDelay backs off failed deliveries from 5 seconds to 5 minutes.
func retryDelay(deliveryCount int) time.Duration {
	delay := 5 * time.Second
	for range min(max(deliveryCount-1, 0), 6) {
		delay *= 2
	}
	return min(delay, 5*time.Minute)
}
