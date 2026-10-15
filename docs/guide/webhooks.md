# Webhooks

A webhook is a [hook](/guide/hooks) resumed by an HTTP request to its URL. The
JSON request body is decoded as the payload. Use webhooks to wait for callbacks
from payment providers, CI systems, or anything that can call a URL.

```go
func Checkout(ctx context.Context, input workflows.Checkout) (workflows.CheckoutResult, error) {
	webhook, err := workflow.CreateWebhook[Payment](ctx)
	if err != nil {
		return workflows.CheckoutResult{}, err
	}

	if _, err := workflow.Step(ctx, registerCallback, webhook.URL()); err != nil {
		return workflows.CheckoutResult{}, err
	}

	payment, err := webhook.Await(ctx)
	if err != nil {
		return workflows.CheckoutResult{}, err
	}

	return workflows.CheckoutResult{PaymentID: payment.ID}, nil
}
```

## URLs

A webhook always gets a random token, since its URL is public. `webhook.URL()`
returns the absolute URL, built from the World's base URL:

```
<BaseURL>/webhooks/<token>
```

For example `https://workflows.example.com/v1/workflow/webhooks/V1StGXR8_Z5jdHi6B-myT`.
Set `Config.BaseURL` on your World to the public URL where its
[HTTP handler](/worlds/#http-handlers) is mounted, and serve the webhook
endpoint there.

## Options

```go
type WebhookOption struct {
	Method   string         // accepted method; empty accepts any
	Response *http.Response // static response; body up to 128 KiB
	Manual   bool           // hold the request until RespondWith
}
```

| Configuration | Response to the caller |
| --- | --- |
| default | `202 Accepted` |
| `Response` set | that response; the webhook is disposed after the request |
| `Manual: true` | held open until the runner calls `RespondWith` |

A request with the wrong method gets `405 Method Not Allowed`. A token that no
active webhook owns gets `404 Not Found`. Request bodies are limited to
128 KiB (`413` above that).

### Responding from the runner

With `Manual`, the runner reads the request and decides the response:

```go
webhook, err := workflow.CreateWebhook[Command](ctx, workflow.WebhookOption{Method: http.MethodPost, Manual: true})
if err != nil {
	return Result{}, err
}

command, err := webhook.Await(ctx)
if err != nil {
	return Result{}, err
}

response := &http.Response{
	StatusCode: http.StatusOK,
	Header:     http.Header{"Content-Type": {"application/json"}},
	Body:       io.NopCloser(strings.NewReader(`{"ok":true}`)),
}
if err := webhook.RespondWith(ctx, response); err != nil {
	return Result{}, err
}
```

In workflow context `RespondWith` records the response in a step, so replay does
not send it again.

## Reading the request

`webhook.Request(ctx)` returns the request that resumed the webhook, rebuilt
from the recorded method, URL, header, and body. Like `Await`, it suspends the
run until the webhook receives a request. The body is read from the event log,
so reading it in the runner is deterministic. Verify signatures in a step:

```go
request, err := webhook.Request(ctx)
if err != nil {
	return Result{}, err
}

var body bytes.Buffer
if _, err := body.ReadFrom(request.Body); err != nil {
	return Result{}, err
}

if _, err := workflow.Step(ctx, verifySignature, signedRequest{
	Signature: request.Header.Get("Stripe-Signature"),
	Body:      body.Bytes(),
}); err != nil {
	return Result{}, err
}
```

## Passing webhooks to steps

`*Webhook[T]` implements `json.Marshaler` and `json.Unmarshaler`, so it can be
passed to a step as input or returned from one.

## Security

::: warning The token is the only authorization
Anyone with a webhook URL can resume it. Treat the URL as a bearer capability,
and verify the provider's signature, timestamp, and replay protection before
trusting the payload.
:::

Every World exports `Webhook(world)`, which serves only the webhook endpoint, so
you can expose webhooks publicly while keeping the RPC endpoint private.
