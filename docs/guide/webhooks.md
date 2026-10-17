# Webhooks

A webhook is a [hook](/guide/hooks) resumed once by an HTTP request. `Request(ctx)` waits for a `*WebhookRequest[T]`, which embeds `*http.Request` and preserves method, URL, headers, raw body, and response origin across step serialization.

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

`JSON() (T, error)` decodes the recorded raw body without consuming `Request.Body`. You can call it repeatedly. Headers and body reads from the recorded request are deterministic; signature verification or other external work belongs in a step.

The reconstructed HTTP request uses `context.Background()`. Use the step handler's `ctx` for cancellation and step operations. If you need the HTTP request itself to carry that context, bind it explicitly inside the step with `request.Request = request.Request.WithContext(ctx)`.

## URLs and responses

The URL is `<BaseURL>/webhooks/<random-token>`. Set the World's `Config.BaseURL` to the public location of its HTTP handler. A webhook always gets a random token.

```go
type WebhookOption struct {
    Method string          // empty accepts any method
    Response *http.Response // static response; body at most 128 KiB
    Manual bool            // wait for a step to respond
}
```

| Configuration | Response |
| --- | --- |
| default | `202 Accepted` |
| `Response` | Static response, then endpoint deactivation. |
| `Manual: true` | Waits for the receiving step's recorded response. |

Wrong methods return `405`; unknown or disposed tokens return `404`; bodies over 128 KiB return `413`. A manual response wait that times out returns `504`, disposal or run termination while waiting returns `410`, and a storage failure returns `500`. A takeover race returns retryable `503` without forwarding the request body to the new owner.

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

// Step function:
func respondToCommand(ctx context.Context, request *workflow.WebhookRequest[Command]) (Command, error) {
    command, err := request.JSON()
    if err != nil {
        return Command{}, workflow.FatalError(err)
    }
    response := &http.Response{
        StatusCode: http.StatusOK,
        Header: http.Header{"Content-Type": {"application/json"}},
        Body: io.NopCloser(strings.NewReader(`{"ok":true}`)),
    }
    return command, request.RespondWith(ctx, response)
}
```

A retry that records the same response succeeds without another response event. A different response for that request fails with a fatal error. Invalid origin, non-manual responses, and oversized response bodies fail without retrying; transient storage failures remain retryable. A request must be answered by a step in the receiving run.

`Webhook` supports selection and durable `Dispose`, while its received `WebhookRequest` owns `JSON` and `RespondWith`. Token conflict inspection is a `Hook` method.

## Security

::: warning The token is the only authorization
Anyone with a webhook URL can resume it. Treat the URL as a bearer capability and verify the provider's signature, timestamp, and replay protection before trusting the payload.
:::

Every World exports `Webhook(world)` to expose only the webhook endpoint while keeping RPC private.
