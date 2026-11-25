package worlds

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"slices"
	"strconv"
	"time"
)

// MaxRemoteRedelivery caps the redelivery delay a Workflow SDK app asks for
// with timeoutSeconds. A longer delay, including Infinity, is shortened to it.
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
// A delivery is sent as the Workflow SDK's local queue sends one: a POST of
// the SDK message as JSON, with Uint8Array values as
// {"__type":"Uint8Array","data":"<base64>"}, and its queue name, message ID,
// attempt, delivery count and creation time in the x-vqs-queue-name,
// x-vqs-message-id, x-vqs-message-attempt, x-vqs-message-delivery-count and
// x-vqs-message-created-at headers. The app answers with JSON:
// {"timeoutSeconds": n} asks for the message again after n seconds, where 0 is
// a handoff, and anything else acknowledges it. A response that is not 2xx,
// or a request that fails, is an error the World retries; the SDK runtime
// makes a repeated delivery safe.
type RemoteExecutor struct {
	// URL is the app's flow route, such as
	// https://app.example.com/.well-known/workflow/v1/flow.
	URL string
	// Token is sent as a bearer token. It is the token world-http
	// authenticates RPC requests with, the one passed to WithClientToken.
	// Empty sends no Authorization header.
	Token string
	// Header is sent with every request, such as the headers a proxy in
	// front of the app needs. The delivery's own headers take precedence.
	Header http.Header
	// Client sends the requests. Nil uses http.DefaultClient.
	Client *http.Client
}

// sdkMessage is QueueInput.SDKMessage decoded.
type sdkMessage struct {
	QueueName string          `json:"queueName"`
	Message   json.RawMessage `json:"message"`
}

// remoteResult is the part of the SDK queue handler's result a
// RemoteExecutor reads.
type remoteResult struct {
	TimeoutSeconds *float64 `json:"timeoutSeconds"`
}

// Execute sends input to the app and returns its outcome. It ignores
// runtime.
func (e RemoteExecutor) Execute(ctx context.Context, _ Runtime, input QueueInput) error {
	queueName, body, err := remoteDelivery(input)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, e.URL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	for name, values := range e.Header {
		request.Header[http.CanonicalHeaderKey(name)] = slices.Clone(values)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("x-vqs-queue-name", queueName)
	request.Header.Set("x-vqs-message-id", input.MessageID)
	request.Header.Set("x-vqs-message-attempt", strconv.Itoa(max(input.Attempt, 1)))
	if input.DeliveryCount > 0 {
		request.Header.Set("x-vqs-message-delivery-count", strconv.Itoa(input.DeliveryCount))
	}
	if !input.CreatedAt.IsZero() {
		request.Header.Set("x-vqs-message-created-at", input.CreatedAt.UTC().Format("2006-01-02T15:04:05.000Z"))
	}
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

// remoteDelivery returns the queue name and request body of input.
func remoteDelivery(input QueueInput) (string, []byte, error) {
	if len(input.SDKMessage) > 0 {
		var message sdkMessage
		if err := json.Unmarshal(input.SDKMessage, &message); err != nil {
			return "", nil, fmt.Errorf("workflow: decode the SDK message of run %s: %w", input.RunID, err)
		}
		return message.QueueName, message.Message, nil
	}
	// A delivery the Go World queued itself, such as a recovery flow,
	// carries no SDK message, so it gets one built from its fields.
	body, err := json.Marshal(sdkMessageOf(input))
	return sdkWorkflowQueuePrefix + input.WorkflowName, body, err
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
// JSON result.
func remoteOutcome(data []byte) error {
	var result remoteResult
	if json.Unmarshal(data, &result) != nil || result.TimeoutSeconds == nil {
		// A result that is not an object with timeoutSeconds acknowledges.
		return nil
	}
	seconds := *result.TimeoutSeconds
	if math.IsNaN(seconds) || seconds*float64(time.Second) > float64(MaxRemoteRedelivery) {
		return Redeliver(MaxRemoteRedelivery)
	}
	return Redeliver(time.Duration(seconds * float64(time.Second)))
}
