# Webhooks

A webhook lets a workflow wait for an HTTP request. Create it, share its URL
with the producer, and wait for the recorded request:

```go
webhook, err := workflow.CreateWebhook[Payment](ctx)
if err != nil {
	return Output{}, err
}
defer webhook.Dispose(ctx)

if _, err := workflow.Step(ctx, registerCallback, webhook.URL()); err != nil {
	return Output{}, err
}

request, err := webhook.Request(ctx)
if err != nil {
	return Output{}, err
}

payment, err := request.JSON()
if err != nil {
	return Output{}, err
}

return Output{PaymentID: payment.ID}, nil
```

`Request` returns a `*WebhookRequest[T]` that embeds `*http.Request`. The
request survives step serialization, including its raw body and response
origin. Call `JSON` to decode the recorded body without consuming
`Request.Body`.

Reads of the recorded request are deterministic. Put signature verification
and other external work in a step.

The reconstructed HTTP request uses `context.Background()`. Use the step handler's `ctx` for cancellation and step operations. If you need the HTTP request itself to carry that context, bind it explicitly inside the step with `request.Request = request.Request.WithContext(ctx)`.

## URLs and responses

The URL is `<BaseURL>/webhooks/<random-token>`. Set the World's `Config.BaseURL` to the public location of its HTTP handler. A webhook always gets a random token.

Set `WebhookOption.Method` to restrict the HTTP method. An empty method
accepts any request method.

| Configuration | Response |
| --- | --- |
| default | `202 Accepted` |
| `Response` | Static response, then endpoint deactivation. |
| `Manual: true` | Waits for the receiving step's recorded response. |

Request and static response bodies are limited to 128 KiB. See
[HTTP Endpoints](/reference/http#webhooks) for response codes and failure cases.

## Responding inside a step

Pass the serialized request as the step input. `RespondWith` is callable only inside a step:

```go
webhook, err := workflow.CreateWebhook[Command](ctx, workflow.WebhookOption{
	Method: http.MethodPost,
	Manual: true,
})
if err != nil {
	return Output{}, err
}
defer webhook.Dispose(ctx)

request, err := webhook.Request(ctx)
if err != nil {
	return Output{}, err
}

command, err := workflow.Step(ctx, respondToCommand, request)
return Output{Command: command}, err
```

The step decodes the command and records the response:

```go
func respondToCommand(ctx context.Context, request *workflow.WebhookRequest[Command]) (Command, error) {
	command, err := request.JSON()
	if err != nil {
		return Command{}, workflow.FatalError(err)
	}

	response := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": {"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"ok":true}`)),
	}

	return command, request.RespondWith(ctx, response)
}
```

A retry that records the same response succeeds without another response
event. Recording a different response fails with a fatal error.

The responding step must belong to the receiving run, and the webhook must
use manual responses. Invalid or oversized responses fail without retrying.
Transient storage failures remain retryable.

A webhook can participate in [selection](/guide/concurrency#racing-operations).
Dispose it when the endpoint is no longer needed.

## Security

::: warning The token is the only authorization
Anyone with a webhook URL can resume it. Treat the URL as a bearer capability and verify the provider's signature, timestamp, and replay protection before trusting the payload.
:::

Every World exports `Webhook(world)` to expose only the webhook endpoint while keeping RPC private.
