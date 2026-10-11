# Vercel world

The Vercel workflow world requires:

- An Upstash Redis database. Configure either its TLS TCP connection string or
  its REST endpoint and token.
- A Vercel Queue topic for workflow task notifications.
- A Queue consumer route running `vercelworld.Queue(world, runners)`.
- An optional HTTP route running `vercelworld.HTTP(world, runners)` when another service,
  a UI, or a proxy uses `workflow.NewHTTPClient`.

Configure the queue endpoint, OIDC token, and topic as
`VERCEL_QUEUE_URL`, `VERCEL_OIDC_TOKEN`, and `VERCEL_WORKFLOW_QUEUE_TOPIC`.
The queue endpoint includes `/api/v3`, for example
`https://iad1.vercel-queue.com/api/v3`.

```go
world, err := vercelworld.New(vercelworld.Config{
    RedisURL: os.Getenv("REDIS_URL"), // rediss://default:password@host:6379
    QueueURL: os.Getenv("VERCEL_QUEUE_URL"),
    OIDCToken: os.Getenv("VERCEL_OIDC_TOKEN"),
    Topic: os.Getenv("VERCEL_WORKFLOW_QUEUE_TOPIC"),
})
if err != nil { return err }

runners := workflow.Runners(workflow.Use(flows.GenerateReport))
http.ListenAndServe(":8081", vercelworld.Queue(world, runners))
```

`RedisURL` selects a TCP connection. When it is empty, configure the REST
transport instead:

```go
RedisRESTURL:   os.Getenv("UPSTASH_REDIS_REST_URL"),
RedisRESTToken: os.Getenv("UPSTASH_REDIS_REST_TOKEN"),
```

Deploy `vercelworld.HTTP(world, runners)` as a separate app-facing API function
only when another service, a UI, or a proxy needs remote access. It shares the
Redis and Queue configuration. Pass the workflow runners so it can execute a
QStash timer if long timers are enabled. The basic example keeps the two
entrypoints in `cmd/worker/vercel/queue` and `cmd/worker/vercel/api`.

When the code that starts a workflow already has the same world in process,
pass it directly to `workflow.NewContext`. No HTTP function is needed.

The consumer is at-least-once. Redis task claims are lease-fenced and
idempotent. Vercel Queue delays are limited to seven days, so this world
uses QStash for longer durable timers when configured.

## Long timers with QStash

Vercel Queue remains the transport for immediate work and timers of seven days
or less. To run longer timers, configure QStash with the public base URL of
the HTTP handler:

```go
QStash: &vercelworld.QStashConfig{
    Token:             os.Getenv("QSTASH_TOKEN"),
    WebhookEndpoint:    "https://worker.example.com",
    CurrentSigningKey: os.Getenv("QSTASH_CURRENT_SIGNING_KEY"),
    NextSigningKey:    os.Getenv("QSTASH_NEXT_SIGNING_KEY"),
},
```

`WebhookEndpoint` must be the HTTPS application origin with no path. The world
publishes QStash messages to, and `vercelworld.HTTP(world, runners)` mounts a
private consumer at,
`/.well-known/workflow/v1/webhook/qstash`. QStash signs each request and the
handler verifies it before claiming the task. The world stores QStash's message
ID only for those long timers, so cancellation removes the pending QStash
delivery without attempting to cancel ordinary Vercel Queue messages.

## Security

Deploy vercelworld.Queue(world, runners) as a Vercel Queue push consumer, not
as an ordinary public route. Vercel queue consumer functions are not externally
reachable. Only Queue delivery can invoke them, so the consumer handler assumes
trusted delivery. The QStash path on `vercelworld.HTTP(world, runners)` must be
public, but it rejects requests without a valid QStash signature.

`vercelworld.HTTP(world, runners)` is a separate app-facing API. It performs no
authentication or per-run authorization. Prefer an internal application-to-
workflow route. If it is exposed across a trust boundary, put your
authentication and authorization middleware in front of it and enforce which
callers may start each workflow or read each run.
