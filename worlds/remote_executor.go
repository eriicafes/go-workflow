package worlds

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"time"

	"github.com/fxamacker/cbor/v2"
)

// MaxRemoteRedelivery is the longest redelivery delay a RemoteExecutor asks
// for. A Workflow SDK app that asks for a longer or non-finite delay gets
// this one, the longest delay Vercel Queues supports.
const MaxRemoteRedelivery = 15 * time.Minute

// sdkWorkflowQueuePrefix is the Workflow SDK's queue name prefix for a
// workflow's flow and step messages.
const sdkWorkflowQueuePrefix = "__wkf_workflow_"

// ErrRemoteUnauthorized is the error of a delivery the Workflow SDK app
// refused with 401 Unauthorized, which means the RemoteExecutor's Token is
// not the one the app's World holds.
var ErrRemoteUnauthorized = errors.New("workflow: the Workflow SDK app refused the delivery's token")

// RemoteExecutor executes each delivery by sending it to a Workflow SDK app
// whose World is world-http, at the app's flow route, which serves both flow
// and step deliveries. It lets a Go World back a JavaScript app.
//
// A delivery is a POST of a CBOR map holding the SDK's queue name and
// message, and the delivery's messageId, attempt, deliveryCount and
// createdAt. The app answers with the CBOR result of the SDK's queue
// handler: {timeoutSeconds: n} asks for the message again after n seconds,
// where 0 is a handoff, and anything else acknowledges it. A response that is
// not 2xx, or a request that fails, is an error the World retries; the SDK
// runtime makes a repeated delivery safe.
type RemoteExecutor struct {
	// URL is the app's flow route, such as
	// https://app.example.com/.well-known/workflow/v1/flow.
	URL string
	// Token is sent as a bearer token. It is the token world-http
	// authenticates RPC requests with, the one passed to WithClientToken.
	// Empty sends no Authorization header.
	Token string
	// Client sends the requests. Nil uses http.DefaultClient.
	Client *http.Client
}

// remoteDelivery is the body of a RemoteExecutor's request.
type remoteDelivery struct {
	RPCVersion    int             `cbor:"rpcVersion"`
	QueueName     string          `cbor:"queueName"`
	Message       cbor.RawMessage `cbor:"message"`
	MessageID     string          `cbor:"messageId,omitempty"`
	Attempt       int             `cbor:"attempt,omitempty"`
	DeliveryCount int             `cbor:"deliveryCount,omitempty"`
	CreatedAt     *time.Time      `cbor:"createdAt,omitempty"`
}

// sdkMessage is QueueInput.SDKMessage decoded.
type sdkMessage struct {
	QueueName string          `cbor:"queueName"`
	Message   cbor.RawMessage `cbor:"message"`
}

// remoteResult is the part of the SDK queue handler's result a
// RemoteExecutor reads.
type remoteResult struct {
	TimeoutSeconds *float64 `cbor:"timeoutSeconds"`
}

// Execute sends input to the app and returns its outcome. It ignores
// runtime.
func (e RemoteExecutor) Execute(ctx context.Context, _ Runtime, input QueueInput) error {
	body, err := remoteDeliveryBody(input)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, e.URL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", RPCContentType)
	request.Header.Set("Accept", RPCContentType)
	if e.Token != "" {
		request.Header.Set("Authorization", "Bearer "+e.Token)
	}
	client := e.Client
	if client == nil {
		client = http.DefaultClient
	}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("workflow: deliver to %s: %w", e.URL, err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("workflow: read the delivery response from %s: %w", e.URL, err)
	}
	switch {
	case response.StatusCode == http.StatusUnauthorized:
		return ErrRemoteUnauthorized
	case response.StatusCode < 200 || response.StatusCode > 299:
		return fmt.Errorf("workflow: deliver to %s: %s: %s", e.URL, response.Status, bytes.TrimSpace(data[:min(len(data), 512)]))
	}
	return remoteOutcome(data)
}

// remoteDeliveryBody returns the request body of input.
func remoteDeliveryBody(input QueueInput) ([]byte, error) {
	delivery := remoteDelivery{
		RPCVersion:    RPCVersion,
		MessageID:     input.MessageID,
		Attempt:       input.Attempt,
		DeliveryCount: input.DeliveryCount,
	}
	if !input.CreatedAt.IsZero() {
		delivery.CreatedAt = &input.CreatedAt
	}
	if len(input.SDKMessage) > 0 {
		var message sdkMessage
		if err := cbor.Unmarshal(input.SDKMessage, &message); err != nil {
			return nil, fmt.Errorf("workflow: decode the SDK message of run %s: %w", input.RunID, err)
		}
		delivery.QueueName, delivery.Message = message.QueueName, message.Message
	} else {
		// A delivery the Go World queued itself, such as a recovery flow,
		// carries no SDK message, so it gets one built from its fields.
		message, err := rpcEncoding.Marshal(sdkMessageOf(input))
		if err != nil {
			return nil, err
		}
		delivery.QueueName, delivery.Message = sdkWorkflowQueuePrefix+input.WorkflowName, message
	}
	return rpcEncoding.Marshal(delivery)
}

// sdkMessageOf returns the Workflow SDK message of a delivery that has none.
func sdkMessageOf(input QueueInput) map[string]any {
	message := map[string]any{"runId": string(input.RunID)}
	if input.StepID != nil {
		message["stepId"] = string(*input.StepID)
	}
	if input.StepName != "" {
		message["stepName"] = input.StepName
	}
	if input.StepAttempt > 1 {
		message["stepAttempt"] = input.StepAttempt
	}
	if input.StepResent {
		message["stepResent"] = true
	}
	if input.RunSpecVersion != 0 {
		message["runSpecVersion"] = input.RunSpecVersion
	}
	if input.Recovery {
		message["recovery"] = true
	}
	if len(input.CompleteWaits) > 0 {
		message["completeWaits"] = input.CompleteWaits
	}
	if input.DeploymentMismatchRetryCount > 0 {
		message["deploymentMismatchRetryCount"] = input.DeploymentMismatchRetryCount
	}
	return message
}

// remoteOutcome returns the delivery outcome of the SDK queue handler's
// result data.
func remoteOutcome(data []byte) error {
	if len(bytes.TrimSpace(data)) == 0 {
		return nil
	}
	var result remoteResult
	if err := cbor.Unmarshal(data, &result); err != nil {
		// A result that is not a map, such as null, acknowledges.
		var value any
		if cbor.Unmarshal(data, &value) == nil {
			return nil
		}
		return fmt.Errorf("workflow: decode the delivery response: %w", err)
	}
	if result.TimeoutSeconds == nil {
		return nil
	}
	seconds := *result.TimeoutSeconds
	if math.IsNaN(seconds) || math.IsInf(seconds, 0) || seconds*float64(time.Second) > float64(MaxRemoteRedelivery) {
		return Redeliver(MaxRemoteRedelivery)
	}
	return Redeliver(time.Duration(seconds * float64(time.Second)))
}
