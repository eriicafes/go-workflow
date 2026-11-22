# Webhooks

A webhook lets a workflow wait for HTTP requests. Create it, share its URL with
the producer, and wait for a request:

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

payment, err := workflow.Step(ctx, readPayment, request)
if err != nil {
	return Output{}, err
}

return Output{PaymentID: payment.ID}, nil
```

`Request` returns the next request as a `WebhookRequest`: its `Method`, `URL`,
and `Header` are recorded with it, and its body is a stream of the run. Reading the body is I/O, so `Body` and `JSON` are
step-only. Pass the request to a step, which can also verify a signature:

```go
func readPayment(ctx context.Context, request *workflow.WebhookRequest[Payment]) (Payment, error) {
	payment, err := request.JSON(ctx)
	if err != nil {
		return Payment{}, workflow.FatalError(err)
	}
	return payment, nil
}
```

A webhook takes any number of requests: call `Request` again for the next
one, or dispose of the webhook when it is no longer needed.

## URLs and responses

The URL is `<BaseURL>/webhooks/<token>`. Set the World's
`Config.BaseURL` to the public location of its HTTP handler. A webhook always
gets a generated token: 21 characters of Nano ID's alphabet drawn from the
run's seed, so every replay draws the same one. It accepts any HTTP method; check `request.Method` in a
step when only some should be accepted.

| Configuration | Response |
| --- | --- |
| default | `202 Accepted` |
| `Response` | The static response, sent to every request. |
| `Manual: true` | Held until a step responds with `RespondWith`. |

The settings are the webhook's metadata, `{respondWith}`, as the SDK records
them. Request and static response bodies are limited to 128 KiB. See
[HTTP Endpoints](/reference/http#webhooks) for response codes and failure
cases.

## Responding inside a step

With `Manual`, each request waits for a response. `RespondWith` is step-only:

```go
webhook, err := workflow.CreateWebhook[Command](ctx, workflow.WebhookOption{Manual: true})
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

The step decodes the command and sends the response:

```go
func respondToCommand(ctx context.Context, request *workflow.WebhookRequest[Command]) (Command, error) {
	command, err := request.JSON(ctx)
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

The response is written to the request's response stream, and the request
answers with the first one written, so a retried attempt's response is
ignored. `RespondWith` fails without retrying for a webhook without manual
responses or an oversized body.

A webhook can participate in [selection](/guide/concurrency#racing-operations).

## Security

::: warning The token is the only authorization
Anyone with a webhook URL can resume it. Treat the URL as a bearer capability and verify the provider's signature, timestamp, and replay protection before trusting the payload.
:::

Every World except the test World exports `Webhook(world)` to expose only the webhook endpoint while keeping RPC private.
